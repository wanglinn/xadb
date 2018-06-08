#include "postgres.h"

#include "access/xact.h"

#include "catalog/pgxc_node.h"
#include "executor/nodeEmptyResult.h"
#include "executor/execdesc.h"
#include "executor/executor.h"
#include "intercomm/inter-comm.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "lib/stringinfo.h"
#include "libpq/libpq.h"
#include "libpq/libpq-node.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "parser/parsetree.h"
#include "storage/lmgr.h"
#include "storage/mem_toc.h"
#include "tcop/dest.h"
#include "utils/combocid.h"
#include "utils/lsyscache.h"
#include "utils/ps_status.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "executor/clusterReceiver.h"
#include "executor/execCluster.h"

#include "libpq/libpq-fe.h"

#include "reduce/adb_reduce.h"

#define REMOTE_KEY_END						0xFFFFFF00
#define REMOTE_KEY_TRANSACTION_STATE		0xFFFFFF01
#define REMOTE_KEY_GLOBAL_SNAPSHOT			0xFFFFFF02
#define REMOTE_KEY_ACTIVE_SNAPSHOT			0xFFFFFF03
/*#define REMOTE_KEY_COMBO_CID				0xFFFFFF04*/
#define REMOTE_KEY_PLAN_STMT				0xFFFFFF05
#define REMOTE_KEY_PARAM					0xFFFFFF06
#define REMOTE_KEY_NODE_OID					0xFFFFFF07
#define REMOTE_KEY_RTE_LIST					0xFFFFFF08
#define REMOTE_KEY_ES_INSTRUMENT			0xFFFFFF09
#define REMOTE_KEY_HAS_REDUCE				0xFFFFFF0A
#define REMOTE_KEY_REDUCE_GROUP				0xFFFFFF0B

typedef struct ClusterPlanContext
{
	bool transaction_read_only;		/* is read only plan */
	bool have_temp;					/* have temporary object */
	bool have_reduce;				/* does this cluster plan have reduce node? */
	bool start_self_reduce;			/* does this cluster plan need start self-reduce? */
}ClusterPlanContext;

typedef struct ClusterErrorHookContext
{
	ErrorContextCallback	callback;
	Oid						saved_node_oid;
	bool					saved_enable_cluster_plan;
}ClusterErrorHookContext;

extern bool enable_cluster_plan;

static void restore_cluster_plan_info(StringInfo buf);
static QueryDesc *create_cluster_query_desc(StringInfo buf, DestReceiver *r);

static void SerializePlanInfo(StringInfo msg, PlannedStmt *stmt, ParamListInfo param, ClusterPlanContext *context);
static bool SerializePlanHook(StringInfo buf, Node *node, void *context);
static void *LoadPlanHook(StringInfo buf, NodeTag tag, void *context);
static bool HaveModifyPlanWalker(Plan *plan, Node *GlobOrStmt, void *context);
static void SerializeRelationOid(StringInfo buf, Oid relid);
static Oid RestoreRelationOid(StringInfo buf, bool missok);
static void send_rdc_listend_port(int port);
static void wait_rdc_group_message(void);
static bool get_rdc_listen_port_hook(void *context, struct pg_conn *conn, PQNHookFuncType type, ...);
static void StartRemoteReduceGroup(List *conns, RdcMask *rdc_masks, int rdc_cnt);
static void StartRemotePlan(StringInfo msg, List *rnodes, ClusterPlanContext *context);
static bool InstrumentEndLoop_walker(PlanState *ps, Bitmapset **called);
static void InstrumentEndLoop_cluster(PlanState *ps);
static bool RelationIsCoordOnly(Oid relid);

static void ExecClusterErrorHookMaster(void *arg);
static void ExecClusterErrorHookNode(void *arg);
static void SetupClusterErrorHook(ClusterErrorHookContext *context);
static void RestoreClusterHook(ClusterErrorHookContext *context);

void exec_cluster_plan(const void *splan, int length)
{
	QueryDesc *query_desc;
	DestReceiver *receiver;
	StringInfoData buf;
	StringInfoData msg;
	ClusterErrorHookContext error_context_hook;
	int eflags;
	bool need_instrument;
	bool has_reduce;

	buf.data = (char*)splan;
	buf.cursor = 0;
	buf.len = buf.maxlen = length;

	SetupClusterErrorHook(&error_context_hook);
	enable_cluster_plan = true;

	restore_cluster_plan_info(&buf);
	receiver = CreateDestReceiver(DestClusterOut);
	query_desc = create_cluster_query_desc(&buf, receiver);
	if(mem_toc_lookup(&buf, REMOTE_KEY_ES_INSTRUMENT, NULL) != NULL)
		need_instrument = true;
	else
		need_instrument = false;

	if (mem_toc_lookup(&buf, REMOTE_KEY_HAS_REDUCE, NULL) != NULL)
		has_reduce = true;
	else
		has_reduce = false;

	/* Send a message
	 * 'H' for copy out, 'W' for copy both */
	initStringInfo(&msg);
	pq_sendbyte(&msg, 0);
	pq_sendint(&msg, 0, 2);
	pq_putmessage('W', msg.data, msg.len);
	pq_flush();

	if (has_reduce)
	{
		int rdc_listen_port;

		/* Start self Reduce with rdc_id */
		set_ps_display("<cluster start self reduce>", false);
		rdc_listen_port = StartSelfReduceLauncher(PGXCNodeOid);

		/* Tell coordinator self own listen port */
		send_rdc_listend_port(rdc_listen_port);

		/* Wait for the whole Reduce connect OK */
		set_ps_display("<cluster start group reduce>", false);
		wait_rdc_group_message();
	}

	eflags = 0;
	if (query_desc->plannedstmt->hasModifyingCTE ||
		query_desc->plannedstmt->rowMarks != NIL ||
		HaveModifyPlanWalker(query_desc->plannedstmt->planTree, NULL, NULL))
		eflags |= EXEC_FLAG_UPDATE_CMD_ID;

	ExecutorStart(query_desc, eflags);

	set_ps_display("<cluster query>", false);

	/* run plan */
	ExecutorRun(query_desc, ForwardScanDirection, 0L);
	/* and finish */
	ExecutorFinish(query_desc);

	if(need_instrument)
		InstrumentEndLoop_cluster(query_desc->planstate);

	/* send processed message */
	resetStringInfo(&msg);
	serialize_processed_message(&msg, query_desc->estate->es_processed);
	pq_putmessage('d', msg.data, msg.len);

	/* send Instrumentation info */
	if(need_instrument)
	{
		resetStringInfo(&msg);
		serialize_instrument_message(query_desc->planstate, &msg);
		pq_putmessage('d', msg.data, msg.len);
	}

	/* and clean up */
	ExecutorEnd(query_desc);
	FreeQueryDesc(query_desc);
	RestoreClusterHook(&error_context_hook);
	PopActiveSnapshot();

	pfree(msg.data);

	/* Send Copy Done message */
	pq_putemptymessage('c');
	EndClusterTransaction();
}

static void restore_cluster_plan_info(StringInfo buf)
{
	char *ptr;
	StringInfoData msg;

	ptr = mem_toc_lookup(buf, REMOTE_KEY_TRANSACTION_STATE, NULL);
	if(ptr == NULL)
		ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION)
			,errmsg("Can not find transaction state")));
	StartClusterTransaction(ptr);

	ptr = mem_toc_lookup(buf, REMOTE_KEY_NODE_OID, NULL);
	if(ptr == NULL)
		ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION)
			, errmsg("Can not find Node Oid")));
	memcpy(&PGXCNodeOid, ptr, sizeof(PGXCNodeOid));

	msg.data = mem_toc_lookup(buf, REMOTE_KEY_GLOBAL_SNAPSHOT, &msg.len);
	if(msg.data == NULL)
		ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION)
			, errmsg("Can not find global transaction snapshot")));
	msg.maxlen = msg.len;
	msg.cursor = 0;
	SetGlobalSnapshot(&msg);

	ptr = mem_toc_lookup(buf, REMOTE_KEY_ACTIVE_SNAPSHOT, NULL);
	if(ptr == NULL)
		ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION)
			, errmsg("Can not find active snapshot")));
	PushActiveSnapshot(RestoreSnapshot(ptr));

	/* ADBQ: need active snapshot? */
}

static QueryDesc *create_cluster_query_desc(StringInfo info, DestReceiver *r)
{
	ListCell *lc;
	List *rte_list;
	Relation *base_rels;
	PlannedStmt *stmt;
	ParamListInfo paramLI;
	StringInfoData buf;
	int es_instrument;
	int i,n;

	buf.data = mem_toc_lookup(info, REMOTE_KEY_RTE_LIST, &buf.len);
	if(buf.data == NULL)
		ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION)
			, errmsg("can not find range table list")));
	buf.maxlen = buf.len;
	buf.cursor = 0;
	rte_list = (List*)loadNodeAndHook(&buf, LoadPlanHook, NULL);

	n = list_length(rte_list);
	base_rels = palloc(sizeof(Relation) * n);
	for(i=0,lc=list_head(rte_list);lc!=NULL;lc=lnext(lc),++i)
	{
		RangeTblEntry *rte = lfirst(lc);
		if(rte->rtekind == RTE_RELATION)
			base_rels[i] = heap_open(rte->relid, NoLock);
		else
			base_rels[i] = NULL;
	}

	buf.data = mem_toc_lookup(info, REMOTE_KEY_PLAN_STMT, &buf.len);
	if(buf.data == NULL)
		ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION)
			, errmsg("Can not find PlannedStmt")));
	buf.maxlen = buf.len;
	buf.cursor = 0;
	stmt = (PlannedStmt*)loadNodeAndHook(&buf, LoadPlanHook, (void*)base_rels);
	stmt->rtable = rte_list;
	foreach(lc, stmt->planTree->targetlist)
		((TargetEntry*)lfirst(lc))->resjunk = false;
	for(i=0;i<n;++i)
	{
		if(base_rels[i])
			heap_close(base_rels[i], NoLock);
	}
	pfree(base_rels);

	/* modify RowMarks */
	foreach(lc, stmt->rowMarks)
	{
		PlanRowMark *rc = (PlanRowMark *) lfirst(lc);
		Oid			relid;

		/* ignore "parent" rowmarks; they are irrelevant at runtime */
		if (rc->isParent)
			continue;

		/* get relation's OID (will produce InvalidOid if subquery) */
		relid = getrelid(rc->rti, rte_list);

		/* let function InitPlan ignore this */
		if (!OidIsValid(relid))
			rc->isParent = true;
	}

	buf.data = mem_toc_lookup(info, REMOTE_KEY_PARAM, &buf.len);
	if(buf.data)
	{
		buf.cursor = 0;
		buf.maxlen = buf.len;
		paramLI = LoadParamList(&buf);
	}else
	{
		paramLI = NULL;
	}

	buf.data = mem_toc_lookup(info, REMOTE_KEY_ES_INSTRUMENT, 0);
	if(buf.data)
		memcpy(&es_instrument, buf.data, sizeof(es_instrument));
	else
		es_instrument = 0;
	return CreateQueryDesc(stmt,
						   "<cluster query>",
						   GetTransactionSnapshot()/*GetActiveSnapshot()*/, InvalidSnapshot,
						   r, paramLI, es_instrument);
}

/************************************************************************/
static bool
ReducePlanWalker(Node *node, PlannedStmt *stmt)
{
	if (node == NULL)
		return false;

	if (IsA(node, ClusterReduce))
	{
		return true;
	}else if(IsA(node, SubPlan))
	{
		SubPlan *subPlan = (SubPlan*)node;
		return ReducePlanWalker(list_nth(stmt->subplans, subPlan->plan_id-1), stmt);
	}else if(IsA(node, CteScan))
	{
		CteScan *cte = (CteScan*)node;
		if(ReducePlanWalker(list_nth(stmt->subplans, cte->ctePlanId-1), stmt))
			return true;
	}

	return node_tree_walker(node, ReducePlanWalker, stmt);
}

PlanState* ExecStartClusterPlan(Plan *plan, EState *estate, int eflags, List *rnodes)
{
	PlannedStmt *stmt;
	StringInfoData msg;
	ClusterPlanContext context;
	bool have_reduce;
	bool start_self_reduce;

	AssertArg(plan);
	AssertArg(rnodes != NIL);

	if(eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return ExecInitNode(plan, estate, eflags);

	stmt = palloc(sizeof(*stmt));
	memcpy(stmt, estate->es_plannedstmt, sizeof(*stmt));
	stmt->planTree = plan;
	/* make sure remote send tuple(s) */
	stmt->commandType = CMD_SELECT;

	initStringInfo(&msg);
	SerializePlanInfo(&msg, stmt, estate->es_param_list_info, &context);

	if(estate->es_instrument)
	{
		begin_mem_toc_insert(&msg, REMOTE_KEY_ES_INSTRUMENT);
		appendBinaryStringInfo(&msg,
							   (char*)&(estate->es_instrument),
							   sizeof(estate->es_instrument));
		end_mem_toc_insert(&msg, REMOTE_KEY_ES_INSTRUMENT);
	}

	/* TODO: judge whether start self reduce or not */
	start_self_reduce = true;
	have_reduce = ReducePlanWalker((Node *) plan, stmt);
	pfree(stmt);
	if (have_reduce)
	{
		begin_mem_toc_insert(&msg, REMOTE_KEY_HAS_REDUCE);
		appendBinaryStringInfo(&msg, (const char *) &have_reduce, sizeof(have_reduce));
		end_mem_toc_insert(&msg, REMOTE_KEY_HAS_REDUCE);
	}

	context.have_reduce = have_reduce;
	context.start_self_reduce = start_self_reduce;

	StartRemotePlan(&msg, rnodes, &context);
	pfree(msg.data);

	return ExecInitNode(plan, estate, eflags);
}

static void SerializePlanInfo(StringInfo msg, PlannedStmt *stmt,
							  ParamListInfo param, ClusterPlanContext *context)
{
	ListCell *lc;
	List *rte_list;
	PlannedStmt *new_stmt;
	Size size;

	new_stmt = palloc(sizeof(*new_stmt));
	memcpy(new_stmt, stmt, sizeof(*new_stmt));
	new_stmt->rtable = NIL;

	if (context)
	{
		context->have_temp = false;
		context->transaction_read_only = true;
	}

	rte_list = NIL;
	foreach(lc, stmt->rtable)
	{
		RangeTblEntry *rte = lfirst(lc);
		if(rte->rtekind == RTE_RELATION &&
			(rte->relkind == RELKIND_VIEW ||
			 rte->relkind == RELKIND_FOREIGN_TABLE ||
			 rte->relkind == RELKIND_MATVIEW))
		{
			RangeTblEntry *new_rte = palloc(sizeof(*rte));
			memcpy(new_rte, rte, sizeof(*rte));
			new_rte->rtekind = RTE_REMOTE_DUMMY;
			new_rte->relid = InvalidOid;
			rte_list = lappend(rte_list, new_rte);
		}else
		{
			rte_list = lappend(rte_list, rte);
		}
	}
	begin_mem_toc_insert(msg, REMOTE_KEY_RTE_LIST);
	saveNodeAndHook(msg, (Node*)rte_list, SerializePlanHook, context);
	end_mem_toc_insert(msg, REMOTE_KEY_RTE_LIST);

	begin_mem_toc_insert(msg, REMOTE_KEY_PLAN_STMT);
	saveNodeAndHook(msg, (Node*)new_stmt, SerializePlanHook, context);
	end_mem_toc_insert(msg, REMOTE_KEY_PLAN_STMT);

	begin_mem_toc_insert(msg, REMOTE_KEY_PARAM);
	SaveParamList(msg, param);
	end_mem_toc_insert(msg, REMOTE_KEY_PARAM);

	begin_mem_toc_insert(msg, REMOTE_KEY_ACTIVE_SNAPSHOT);
	size = EstimateSnapshotSpace(GetActiveSnapshot());
	enlargeStringInfo(msg, size);
	SerializeSnapshot(GetActiveSnapshot(), &(msg->data[msg->len]));
	msg->len += size;
	end_mem_toc_insert(msg, REMOTE_KEY_ACTIVE_SNAPSHOT);

	begin_mem_toc_insert(msg, REMOTE_KEY_GLOBAL_SNAPSHOT);
	InterXactSerializeSnapshot(msg, GetActiveSnapshot());
	end_mem_toc_insert(msg, REMOTE_KEY_GLOBAL_SNAPSHOT);

	begin_mem_toc_insert(msg, REMOTE_KEY_TRANSACTION_STATE);
	SerializeClusterTransaction(msg);
	end_mem_toc_insert(msg, REMOTE_KEY_TRANSACTION_STATE);

	foreach(lc, rte_list)
	{
		/* pfree we palloced memory */
		if (list_member_ptr(stmt->rtable, lfirst(lc)) == false)
			pfree(lfirst(lc));
	}
	list_free(rte_list);
	pfree(new_stmt);
}

static bool SerializePlanHook(StringInfo buf, Node *node, void *context)
{
	AssertArg(buf && node);

#define SAVE_NODE_INVALID_OID(type_, member_)							\
	do{																	\
		type_ tmp_;														\
		memcpy(&tmp_, node, sizeof(type_));								\
		tmp_.member_ = InvalidOid;										\
		saveNodeAndHook(buf, (Node*)&tmp_, SerializePlanHook, context);	\
	}while(0)
#define IS_RELOID_COORD_ONLY(type_, member_)							\
		OidIsValid(((type_*)node)->member_)	&&							\
		RelationIsCoordOnly(((type_*)node)->member_)

	switch(nodeTag(node))
	{
	case T_ValuesScan:
	case T_FunctionScan:
	case T_ForeignScan:
		{
			/* always run at coordinator */
			Plan *empty = MakeEmptyResultPlan((Plan*)node);
			saveNodeAndHook(buf, (Node*)empty, SerializePlanHook, context);
			pfree(empty);
			return true;
		}
	case T_RangeTblEntry:
		if(((RangeTblEntry*)node)->rtekind == RTE_RELATION)
		{
			RangeTblEntry *rte = (RangeTblEntry*)node;
			Relation rel = heap_open(rte->relid, NoLock);
			LOCKMODE locked_mode = GetLocalLockedRelationOidMode(rte->relid);
			RangeTblEntry tmp;
			Assert(locked_mode != NoLock);

			memcpy(&tmp, rte, sizeof(RangeTblEntry));
			tmp.relid = InvalidOid;
			saveNodeAndHook(buf, (Node*)&tmp, SerializePlanHook, context);

			/* save node Oids */
			if(RelationGetLocInfo(rel))
			{
				List *oids = RelationGetLocInfo(rel)->nodeids;
				saveNode(buf, (Node*)oids);

				if(RelationUsesLocalBuffers(rel))
					((ClusterPlanContext*)context)->have_temp = true;
			}else
			{
				List *oids = list_make1_oid(PGXCNodeOid);
				saveNode(buf, (Node*)oids);
				list_free(oids);
			}
			/* save relation Oid */
			SerializeRelationOid(buf, RelationGetRelid(rel));
			heap_close(rel, NoLock);
			appendBinaryStringInfo(buf, (char*)&locked_mode, sizeof(locked_mode));
			return true;
		}
		break;
	case T_TargetEntry:
		if (IS_RELOID_COORD_ONLY(TargetEntry, resorigtbl))
		{
			SAVE_NODE_INVALID_OID(TargetEntry, resorigtbl);
			return true;
		}
		break;
	case T_Hash:
		if (IS_RELOID_COORD_ONLY(Hash, skewTable))
		{
			SAVE_NODE_INVALID_OID(Hash, skewTable);
			return true;
		}
		break;
	case T_IndexScan:
		SAVE_NODE_INVALID_OID(IndexScan, indexid);
		SerializeRelationOid(buf, ((IndexScan*)node)->indexid);
		return true;
	case T_IndexOnlyScan:
		SAVE_NODE_INVALID_OID(IndexOnlyScan, indexid);
		SerializeRelationOid(buf, ((IndexOnlyScan*)node)->indexid);
		return true;
	case T_BitmapIndexScan:
		SAVE_NODE_INVALID_OID(BitmapIndexScan, indexid);
		SerializeRelationOid(buf, ((BitmapIndexScan*)node)->indexid);
		return true;
	case T_ModifyTable:
		((ClusterPlanContext*)context)->transaction_read_only = false;
		break;
	case T_CustomScan:
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Cluster plan not support CustomScan yet!")));
		break;	/* keep compiler quiet */
	default:
		break;
	}

	saveNodeAndHook(buf, node, SerializePlanHook, context);

	return true;
}

static void *LoadPlanHook(StringInfo buf, NodeTag tag, void *context)
{
	Node *node = loadNodeAndHookWithTag(buf, LoadPlanHook, context, tag);
	Assert(node != NULL);

#define LOAD_SCAN_REL_INFO(type_, member_)												\
	do{																					\
		bool missing_ok = !list_member_oid(((Scan*)node)->execute_nodes, PGXCNodeOid);	\
		((type_*)node)->member_ = RestoreRelationOid(buf, missing_ok); /* eat buffer */	\
		if (missing_ok)																	\
			node = (Node*)MakeEmptyResultPlan((Plan*)node);								\
	}while(0)

	switch(nodeTag(node))
	{
	case T_RangeTblEntry:
		{
			RangeTblEntry *rte = (RangeTblEntry*)node;
			if(rte->rtekind == RTE_RELATION)
			{
				/* get relation oid */
				List *oids;
				LOCKMODE lock_mode;
				bool missok;

				/* load nodes */
				oids = (List*)loadNode(buf);
				Assert(oids && IsA(oids, OidList));
				missok = !list_member_oid(oids, PGXCNodeOid);

				/* load reation Oid */
				rte->relid = RestoreRelationOid(buf, missok);
				if (missok)
					rte->relid = InvalidOid;

				/* we must lock relation now */
				pq_copymsgbytes(buf, (char*)&lock_mode, sizeof(lock_mode));
				if(OidIsValid(rte->relid))
					LockRelationOid(rte->relid, lock_mode);
				else
					rte->rtekind = RTE_REMOTE_DUMMY;
			}
		}
		break;
	case T_CustomScan:
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Cluster plan not support CustomScan yet!")));
		break;	/* keep compiler quiet */
	case T_Agg:
		if (((Agg*)node)->aggsplit != AGGSPLIT_INITIAL_SERIAL &&
			list_member_oid(((Agg*)node)->exec_nodes, PGXCNodeOid) == false)
			node = (Node*)MakeEmptyResultPlan((Plan*)node);
		break;
	case T_SeqScan:
	case T_TidScan:
		if (!list_member_oid(((Scan*)node)->execute_nodes, PGXCNodeOid))
			node = (Node*)MakeEmptyResultPlan((Plan*)node);
		break;
	case T_IndexScan:
		LOAD_SCAN_REL_INFO(IndexScan, indexid);
		break;
	case T_IndexOnlyScan:
		LOAD_SCAN_REL_INFO(IndexOnlyScan, indexid);
		break;
	case T_BitmapIndexScan:
		LOAD_SCAN_REL_INFO(BitmapIndexScan, indexid);
		break;
#if 0
	/* in cte and subquery we can not get right Relation yet, so disable it for now */
	case T_Var:
		if (!IS_SPECIAL_VARNO(((Var*)node)->varno)
			&& ((Var*)node)->varattno > 0)
		{
			/* check column attribute */
			Form_pg_attribute attr;
			Var *var = (Var*)node;
			Relation rel = ((Relation*)context)[var->varno-1];
			if(rel != NULL)
			{
				if(var->varattno > RelationGetNumberOfAttributes(rel))
				{
					ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
						,err_generic_string(PG_DIAG_TABLE_NAME, RelationGetRelationName(rel))
						,errmsg("invalid column index %d", var->varattno)));
				}
				attr = RelationGetDescr(rel)->attrs[var->varattno-1];
				if(var->vartype != attr->atttypid)
				{
					ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_DEFINITION)
						,err_generic_string(PG_DIAG_TABLE_NAME, RelationGetRelationName(rel))
						,err_generic_string(PG_DIAG_COLUMN_NAME, NameStr(attr->attname))
						,errmsg("column \"%s\" type is diffent", NameStr(attr->attname))));
				}
			}
		}
		break;
#endif
	default:
		break;
	}

	return node;
}

static bool HaveModifyPlanWalker(Plan *plan, Node *GlobOrStmt, void *context)
{
	if (plan == NULL)
		return false;
	if (IsA(plan, ModifyTable))
		return true;
	return plan_tree_walker(plan, GlobOrStmt, HaveModifyPlanWalker, NULL);
}

static void SerializeRelationOid(StringInfo buf, Oid relid)
{
	Relation rel;
	rel = relation_open(relid, NoLock);
	save_namespace(buf, RelationGetNamespace(rel));
	save_node_string(buf, RelationGetRelationName(rel));
	relation_close(rel, NoLock);
}

static Oid RestoreRelationOid(StringInfo buf, bool missok)
{
	Oid oid = load_namespace_extend(buf, missok);
	char *rel_name = load_node_string(buf, true);
	if(OidIsValid(oid) && rel_name[0])
		oid = get_relname_relid(rel_name, oid);
	else
		oid = InvalidOid;
	if(!OidIsValid(oid) && !missok)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				errmsg("relation \"%s\" not exists", rel_name)));
	return oid;
}

static void send_rdc_listend_port(int port)
{
	StringInfoData buf;

	serialize_rdc_listen_port_message(&buf, port);
	pq_putmessage('d', buf.data, buf.len);
	pq_flush();
	pfree(buf.data);
}

static void wait_rdc_group_message(void)
{
	int				i;
	int				type;
	int				rdc_cnt;
	RdcMask		   *rdc_masks;
	StringInfoData	buf;
	StringInfoData	msg;

	pq_startmsgread();
	type = pq_getbyte();
	if (type != 'd')
	{
		if (type != EOF)
			ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("fail to receive reduce group message"),
				 errdetail("unexpected message type '%c' on client connection", type)));

		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("fail to receive reduce group message"),
				 errdetail("unexpected EOF on client connection")));
	}

	initStringInfo(&msg);
	if (pq_getmessage(&msg, 0))
	{
		pfree(msg.data);
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("fail to receive reduce group message"),
				 errdetail("unexpected EOF on client connection")));
	}

	buf.data = mem_toc_lookup(&msg, REMOTE_KEY_REDUCE_GROUP, &(buf.len));
	if (buf.data == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("Can not find reduce group message")));
	buf.cursor = 0;
	buf.maxlen = buf.len;

	pq_copymsgbytes(&buf, (char *) &rdc_cnt, sizeof(rdc_cnt));
	Assert(rdc_cnt > 0);
	rdc_masks = (RdcMask *) palloc0(rdc_cnt * sizeof(RdcMask));
	for (i = 0; i < rdc_cnt; i++)
	{
		rdc_masks[i].rdc_host = (char *) pq_getmsgrawstring(&buf);
		pq_copymsgbytes(&buf, (char *) &(rdc_masks[i].rdc_port),
						sizeof(rdc_masks[i].rdc_port));
		pq_copymsgbytes(&buf, (char *) &(rdc_masks[i].rdc_rpid),
						sizeof(rdc_masks[i].rdc_rpid));
	}
	/*pq_getmsgend(&buf);*/

	StartSelfReduceGroup(rdc_masks, rdc_cnt);

	EndSelfReduceGroup();

	pfree(msg.data);
}

static bool
get_rdc_listen_port_hook(void *context, struct pg_conn *conn, PQNHookFuncType type, ...)
{
	RdcMask		   *rdc_mask = (RdcMask *) context;
	va_list			args;
	const char	   *buf;
	int				len;

	AssertArg(context);
	switch (type)
	{
		case PQNHFT_ERROR:
			ereport(ERROR, (errmsg("%m")));
		case PQNHFT_COPY_OUT_DATA:
			va_start(args, type);
			buf = va_arg(args, const char*);
			len = va_arg(args, int);

			if(clusterRecvRdcListenPort(conn, buf, len, &(rdc_mask->rdc_port)))
			{
				va_end(args);
				return true;
			}
			va_end(args);
			break;
		case PQNHFT_RESULT:
			{
				PGresult	   *res;
				ExecStatusType	status;

				va_start(args, type);
				res = va_arg(args, PGresult*);
				if(res)
				{
					status = PQresultStatus(res);
					if(status == PGRES_FATAL_ERROR)
						PQNReportResultError(res, conn, ERROR, true);
				}
				va_end(args);
			}
			break;
		default:
			ereport(ERROR, (errmsg("unexpected PQNHookFuncType %d", type)));
			break;
	}
	return false;
}

static void
StartRemoteReduceGroup(List *conns, RdcMask *rdc_masks, int rdc_cnt)
{
	StringInfoData	msg;
	int				i;
	ListCell	   *lc;
	PGconn		   *conn;
	char		   *host;
	int				port;
	RdcPortId		rpid;
	int				len;

	AssertArg(conns && rdc_masks);
	AssertArg(rdc_cnt > 0);

	initStringInfo(&msg);
	begin_mem_toc_insert(&msg, REMOTE_KEY_REDUCE_GROUP);
	appendBinaryStringInfo(&msg, (char *) &rdc_cnt, sizeof(rdc_cnt));
	for (i = 0; i < rdc_cnt; i++)
	{
		host = rdc_masks[i].rdc_host;
		port = rdc_masks[i].rdc_port;
		rpid = rdc_masks[i].rdc_rpid;
		Assert(host && host[0]);

		/* including the terminating null byte ('\0') */
		len = strlen(host) + 1;
		appendBinaryStringInfo(&msg, host, len);
		len = sizeof(port);
		appendBinaryStringInfo(&msg, (char *) &port, len);
		len = sizeof(rpid);
		appendBinaryStringInfo(&msg, (char *) &rpid, len);
	}
	end_mem_toc_insert(&msg, REMOTE_KEY_REDUCE_GROUP);

	foreach (lc, conns)
	{
		conn = lfirst(lc);
		if (PQputCopyData(conn, msg.data, msg.len) <= 0 ||
			PQflush(conn))
		{
			const char *node_name = PQNConnectName(conn);
			pfree(msg.data);
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("%s", PQerrorMessage(conn)),
					 node_name ? errnode(node_name) : 0));
		}
	}
	pfree(msg.data);
}

static void StartRemotePlan(StringInfo msg, List *rnodes, ClusterPlanContext *context)
{
	ListCell *lc,*lc2;
	List *list_conn;
	PGconn *conn;
	PGresult * volatile res;
	int save_len;

	int				rdc_id;
	int				rdc_cnt;
	RdcMask		   *rdc_masks = NULL;
	ErrorContextCallback error_context_hook;
	InterXactState	state;

	Assert(rnodes);
	/* try to start transaction */
	state = GetCurrentInterXactState();
	if (!context->transaction_read_only)
		state->need_xact_block = true;
	InterXactBegin(state, rnodes);
	Assert(state->cur_handle);

	error_context_hook.arg = NULL;
	error_context_hook.callback = ExecClusterErrorHookMaster;
	error_context_hook.previous = error_context_stack;
	error_context_stack = &error_context_hook;

	rdc_id = 0;
	if (context->have_reduce)
	{
		rdc_cnt = list_length(rnodes) + (context->start_self_reduce ? 1 : 0);
		rdc_masks = (RdcMask *) palloc0(rdc_cnt * sizeof(RdcMask));
		if (context->start_self_reduce)
		{
			rdc_masks[rdc_id].rdc_rpid = PGXCNodeOid;
			rdc_masks[rdc_id].rdc_port = 0;	/* fill later */
			rdc_masks[rdc_id].rdc_host = get_pgxc_nodehost(PGXCNodeOid);
			rdc_id++;
		}
	}

	list_conn = GetPGconnFromHandleList(state->cur_handle->handles);
	/*foreach(lc, list_conn)*/
	Assert(list_length(list_conn) == list_length(rnodes));
	save_len = msg->len;
	forboth(lc, list_conn, lc2, rnodes)
	{
		/* send node oid to remote */
		msg->len = save_len;
		begin_mem_toc_insert(msg, REMOTE_KEY_NODE_OID);
		appendBinaryStringInfo(msg, (char*)&(lfirst_oid(lc2)), sizeof(lfirst_oid(lc2)));
		end_mem_toc_insert(msg, REMOTE_KEY_NODE_OID);

		/* send reduce group map and reduce ID to remote */
		if (context->have_reduce)
		{
			rdc_masks[rdc_id].rdc_rpid = lfirst_oid(lc2);
			rdc_masks[rdc_id].rdc_port = 0;	/* fill later */
			rdc_masks[rdc_id].rdc_host = get_pgxc_nodehost(lfirst_oid(lc2));
			rdc_id++;
		}

		/* send plan info */
		conn = lfirst(lc);
		if(PQsendPlan(conn, msg->data, msg->len) == false)
		{
			const char *node_name = PQNConnectName(conn);
			safe_pfree(rdc_masks);
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
				errmsg("%s", PQerrorMessage(conn)),
				node_name ? errnode(node_name) : 0));
		}
	}
	msg->len = save_len;

	res = NULL;
	rdc_id = 0;
	PG_TRY();
	{
		/* start self reduce */
		if (context->have_reduce && context->start_self_reduce)
		{
			rdc_masks[rdc_id].rdc_port = StartSelfReduceLauncher(PGXCNodeOid);
			rdc_id++;
		}

		foreach(lc, list_conn)
		{
			conn = lfirst(lc);
			res = PQgetResult(conn);
			if(res == NULL)
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("remote node not result any status")));
			switch(PQresultStatus(res))
			{
			case PGRES_COPY_BOTH:
			case PGRES_COPY_OUT:
				break;
			case PGRES_FATAL_ERROR:
				PQNReportResultError(res, conn, ERROR, false);
				break;
			default:
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("%s", "remote status error, expect COPY BOTH"),
					errhint("%s %s", "remote result is", PQresStatus(PQresultStatus(res)))));
				break;
			}
			PQclear(res);
			res = NULL;
		}
	}PG_CATCH();
	{
		if(res)
			PQclear(res);
		safe_pfree(rdc_masks);
		PG_RE_THROW();
	}PG_END_TRY();

	/* Start makeup reduce group */
	if (context->have_reduce)
	{
		/* wait for listen port of other reduce */
		foreach(lc, list_conn)
		{
			conn = lfirst(lc);
			PQNOneExecFinish(conn, get_rdc_listen_port_hook, &rdc_masks[rdc_id], true);
			rdc_id++;
		}

		/* have already got all listen port of other reduces */
		if (context->start_self_reduce)
			StartSelfReduceGroup(rdc_masks, rdc_id);

		/* tell other reduce infomation about reduce group */
		StartRemoteReduceGroup(list_conn, rdc_masks, rdc_id);

		/* wait for self reduce start reduce group OK */
		EndSelfReduceGroup();

		safe_pfree(rdc_masks);
	}

	error_context_stack = error_context_hook.previous;
}

static bool InstrumentEndLoop_walker(PlanState *ps, Bitmapset **called)
{
	if(ps == NULL)
		return false;
	if (ps->instrument &&
		bms_is_member(ps->plan->plan_node_id, *called) == false)
	{
		InstrEndLoop(ps->instrument);
		*called = bms_add_member(*called, ps->plan->plan_node_id );
	}
	return planstate_tree_walker(ps, InstrumentEndLoop_walker, called);
}

static void InstrumentEndLoop_cluster(PlanState *ps)
{
	Bitmapset *called = NULL;
	InstrumentEndLoop_walker(ps, &called);
	bms_free(called);
}

static void ExecClusterErrorHookMaster(void *arg)
{
	const ErrorData *err_data;

	if ((err_data = err_current_data()) != NULL &&
		err_data->elevel >= ERROR)
	{
		EndSelfReduce(0, 0);
	}
}

static void ExecClusterErrorHookNode(void *arg)
{
	const ErrorData *err_data;

	if ((err_data = err_current_data()) != NULL &&
		err_data->elevel >= ERROR)
	{
		RestoreClusterHook(arg);
	}
}

static void SetupClusterErrorHook(ClusterErrorHookContext *context)
{
	AssertArg(context);

	context->saved_node_oid = PGXCNodeOid;
	context->saved_enable_cluster_plan = enable_cluster_plan;
	context->callback.arg = context;
	context->callback.callback = ExecClusterErrorHookNode;
	context->callback.previous = error_context_stack;
	error_context_stack = &context->callback;
}

static void RestoreClusterHook(ClusterErrorHookContext *context)
{
	PGXCNodeOid = context->saved_node_oid;
	enable_cluster_plan = context->saved_enable_cluster_plan;
	error_context_stack = context->callback.previous;
}

static bool RelationIsCoordOnly(Oid relid)
{
	Relation rel = heap_open(relid, NoLock);
	Form_pg_class form = RelationGetForm(rel);
	bool result = false;
	if (form->relkind == RELKIND_VIEW ||
		form->relkind == RELKIND_FOREIGN_TABLE ||
		form->relkind == RELKIND_MATVIEW ||
		form->relpersistence == RELPERSISTENCE_TEMP)
		result = true;

	heap_close(rel, NoLock);
	return result;
}
