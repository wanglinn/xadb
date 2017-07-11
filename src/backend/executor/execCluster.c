#include "postgres.h"

#include "access/xact.h"

#include "executor/execdesc.h"
#include "executor/executor.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "lib/stringinfo.h"
#include "libpq/libpq.h"
#include "libpq/libpq-node.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
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
#define REMOTE_KEY_COMBO_CID				0xFFFFFF04
#define REMOTE_KEY_PLAN_STMT				0xFFFFFF05
#define REMOTE_KEY_PARAM					0xFFFFFF06
#define REMOTE_KEY_NODE_OID					0xFFFFFF07
#define REMOTE_KEY_RTE_LIST					0xFFFFFF08
#define REMOTE_KEY_ES_INSTRUMENT			0xFFFFFF09
#define REMOTE_KEY_HAS_REDUCE				0xFFFFFF0A
#define REMOTE_KEY_REDUCE_GROUP				0xFFFFFF0B

static void restore_cluster_plan_info(StringInfo buf);
static QueryDesc *create_cluster_query_desc(StringInfo buf, DestReceiver *r);

static void SerializePlanInfo(StringInfo msg, PlannedStmt *stmt, ParamListInfo param);
static bool SerializePlanHook(StringInfo buf, Node *node, void *context);
static void *LoadPlanHook(StringInfo buf, NodeTag tag, void *context);
static void send_rdc_listend_port(int port);
static void wait_rdc_group_message(void);
static bool get_rdc_listen_port_hook(void *context, struct pg_conn *conn, PQNHookFuncType type, ...);
static void StartRemoteReduceGroup(List *conns, RdcListenMask *rdc_masks, int rdc_cnt);
static void StartRemotePlan(StringInfo msg, List *rnodes, bool has_reduce, bool self_start_reduce);
static bool InstrumentEndLoop_walker(PlanState *ps, void *);
static void ExecClusterErrorHook(void *arg);

void exec_cluster_plan(const void *splan, int length)
{
	QueryDesc *query_desc;
	DestReceiver *receiver;
	StringInfoData buf;
	StringInfoData msg;
	ErrorContextCallback error_context_hook;
	bool need_instrument;

	bool has_reduce;

	buf.data = (char*)splan;
	buf.cursor = 0;
	buf.len = buf.maxlen = length;

	error_context_hook.arg = (void*)(Size)PGXCNodeOid;
	error_context_hook.callback = ExecClusterErrorHook;
	error_context_hook.previous = error_context_stack;
	error_context_stack = &error_context_hook;

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

	ExecutorStart(query_desc, 0);

	set_ps_display("<cluster query>", false);

	/* run plan */
	ExecutorRun(query_desc, ForwardScanDirection, 0L);
	/* and finish */
	ExecutorFinish(query_desc);

	if(need_instrument)
		InstrumentEndLoop_walker(query_desc->planstate, NULL);

	/* send processed message */
	resetStringInfo(&buf);
	serialize_processed_message(&buf, query_desc->estate->es_processed);
	pq_putmessage('d', buf.data, buf.len);

	/* send Instrumentation info */
	if(need_instrument)
	{
		resetStringInfo(&buf);
		serialize_instrument_message(query_desc->planstate, &buf);
		pq_putmessage('d', buf.data, buf.len);
	}

	/* and clean up */
	ExecutorEnd(query_desc);
	FreeQueryDesc(query_desc);
	error_context_stack = error_context_hook.previous;
	PGXCNodeOid = (Oid)(Size)(error_context_hook.arg);

	pfree(buf.data);

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

	ptr = mem_toc_lookup(buf, REMOTE_KEY_COMBO_CID, NULL);
	if(ptr == NULL)
		ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION)
			, errmsg("Can not find CID")));
	RestoreComboCIDState(ptr);

	ptr = mem_toc_lookup(buf, REMOTE_KEY_NODE_OID, NULL);
	if(ptr == NULL)
		ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION)
			, errmsg("Can not find Node Oid")));
	memcpy(&PGXCNodeOid, ptr, sizeof(PGXCNodeOid));

	/*ptr = mem_toc_lookup(buf, REMOTE_KEY_TRANSACTION_SNAPSHOT, NULL);
	if(ptr == NULL)
		ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION)
			, errmsg("Can not find transaction snapshot")));*/
	/* ADBQ: RestoreSnapshot restore lsn, we need change it ? */
	/*RestoreTransactionSnapshot(RestoreSnapshot(ptr), NULL);*/

	msg.data = mem_toc_lookup(buf, REMOTE_KEY_GLOBAL_SNAPSHOT, &msg.len);
	if(msg.data == NULL)
		ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION)
			, errmsg("Can not find global transaction snapshot")));
	msg.maxlen = msg.len;
	msg.cursor = 0;
	SetGlobalSnapshot(&msg);

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
						   "<claster query>",
						   GetTransactionSnapshot()/*GetActiveSnapshot()*/, InvalidSnapshot,
						   r, paramLI, es_instrument);
}

/************************************************************************/
static bool
ReducePlanWalker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, ClusterReduce))
		return true;

	return node_tree_walker(node, ReducePlanWalker, context);
}

PlanState* ExecStartClusterPlan(Plan *plan, EState *estate, int eflags, List *rnodes)
{
	PlannedStmt *stmt;
	StringInfoData msg;
	bool has_reduce;
	bool self_start_reduce;

	AssertArg(plan);
	AssertArg(rnodes != NIL);

	if(eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return ExecInitNode(plan, estate, eflags);

	stmt = palloc(sizeof(*stmt));
	memcpy(stmt, estate->es_plannedstmt, sizeof(*stmt));
	stmt->planTree = plan;

	initStringInfo(&msg);
	SerializePlanInfo(&msg, stmt, estate->es_param_list_info);
	pfree(stmt);

	if(estate->es_instrument)
	{
		begin_mem_toc_insert(&msg, REMOTE_KEY_ES_INSTRUMENT);
		appendBinaryStringInfo(&msg,
							   (char*)&(estate->es_instrument),
							   sizeof(estate->es_instrument));
		end_mem_toc_insert(&msg, REMOTE_KEY_ES_INSTRUMENT);
	}

	/* TODO: judge whether start self reduce or not */
	self_start_reduce = true;
	has_reduce = ReducePlanWalker((Node *) plan, NULL);
	if (has_reduce)
	{
		begin_mem_toc_insert(&msg, REMOTE_KEY_HAS_REDUCE);
		appendBinaryStringInfo(&msg, (const char *) &has_reduce, sizeof(has_reduce));
		end_mem_toc_insert(&msg, REMOTE_KEY_HAS_REDUCE);
	}

	StartRemotePlan(&msg, rnodes, has_reduce, self_start_reduce);
	pfree(msg.data);

	return ExecInitNode(plan, estate, eflags);
}

static void SerializePlanInfo(StringInfo msg, PlannedStmt *stmt, ParamListInfo param)
{
	List *rte_list;
	Size size;

	rte_list = stmt->rtable;
	begin_mem_toc_insert(msg, REMOTE_KEY_RTE_LIST);
	saveNodeAndHook(msg, (Node*)rte_list, SerializePlanHook, NULL);
	end_mem_toc_insert(msg, REMOTE_KEY_RTE_LIST);

	stmt->rtable = NIL;
	begin_mem_toc_insert(msg, REMOTE_KEY_PLAN_STMT);
	saveNodeAndHook(msg, (Node*)stmt, SerializePlanHook, NULL);
	end_mem_toc_insert(msg, REMOTE_KEY_PLAN_STMT);
	stmt->rtable = rte_list;

	begin_mem_toc_insert(msg, REMOTE_KEY_PARAM);
	SaveParamList(msg, param);
	end_mem_toc_insert(msg, REMOTE_KEY_PARAM);

	begin_mem_toc_insert(msg, REMOTE_KEY_COMBO_CID);
	size = EstimateComboCIDStateSpace();
	enlargeStringInfo(msg, size);
	SerializeComboCIDState(size, &(msg->data[msg->len]));
	msg->len += size;
	end_mem_toc_insert(msg, REMOTE_KEY_COMBO_CID);

	/*begin_mem_toc_insert(msg, REMOTE_KEY_TRANSACTION_SNAPSHOT);
	size = EstimateSnapshotSpace(GetTransactionSnapshot());
	enlargeStringInfo(msg, size);
	SerializeSnapshot(GetTransactionSnapshot(), &(msg->data[msg->len]));
	msg->len += size;
	end_mem_toc_insert(msg, REMOTE_KEY_TRANSACTION_SNAPSHOT);*/

	begin_mem_toc_insert(msg, REMOTE_KEY_GLOBAL_SNAPSHOT);
	pgxc_serialize_snapshot(msg, GetActiveSnapshot());
	end_mem_toc_insert(msg, REMOTE_KEY_GLOBAL_SNAPSHOT);

	begin_mem_toc_insert(msg, REMOTE_KEY_TRANSACTION_STATE);
	SerializeClusterTransaction(msg);
	end_mem_toc_insert(msg, REMOTE_KEY_TRANSACTION_STATE);
}

/*
 * save RangeTblEntry::relid to string
 * save IndexScan::indexid to string
 */
static bool SerializePlanHook(StringInfo buf, Node *node, void *context)
{
	AssertArg(buf && node);
	saveNodeAndHook(buf, node, SerializePlanHook, context);
	if(IsA(node, RangeTblEntry)
		&& ((RangeTblEntry*)node)->rtekind == RTE_RELATION)
	{
		RangeTblEntry *rte = (RangeTblEntry*)node;
		Relation rel = heap_open(rte->relid, NoLock);
		LOCKMODE locked_mode = GetLocalLockedRelationOidMode(rte->relid);
		Assert(locked_mode != NoLock);
		/* save is temp table ? */
		appendStringInfoChar(buf, RelationUsesLocalBuffers(rel) ? '\1':'\0');
		/* save namespace */
		save_namespace(buf, RelationGetNamespace(rel));
		/* save relation name */
		save_node_string(buf, RelationGetRelationName(rel));
		heap_close(rel, NoLock);
		appendBinaryStringInfo(buf, (char*)&locked_mode, sizeof(locked_mode));
	}else if(IsA(node, IndexScan) ||
			 IsA(node, IndexOnlyScan) ||
			 IsA(node, BitmapIndexScan))
	{
		Relation rel;
		Oid indexid;
		StaticAssertExpr(offsetof(IndexScan, indexid) == offsetof(IndexOnlyScan, indexid), "");
		StaticAssertExpr(offsetof(IndexScan, indexid) == offsetof(BitmapIndexScan, indexid), "");
		indexid = ((IndexScan*)node)->indexid;
		rel = relation_open(indexid, NoLock);
		save_namespace(buf, RelationGetNamespace(rel));
		save_node_string(buf, RelationGetRelationName(rel));
		heap_close(rel, NoLock);
	}
	return true;
}

static void *LoadPlanHook(StringInfo buf, NodeTag tag, void *context)
{
	Node *node = loadNodeAndHookWithTag(buf, LoadPlanHook, context, tag);
	Assert(node != NULL);
	if(IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rte = (RangeTblEntry*)node;
		if(rte->rtekind == RTE_RELATION)
		{
			/* get relation oid */
			char *rel_name;
			Oid nsp_oid;
			LOCKMODE lock_mode;

			++(buf->cursor);	/* skip is temp table */
			nsp_oid = load_namespace(buf);
			rel_name = load_node_string(buf, false);

			rte->relid = get_relname_relid(rel_name, nsp_oid);
			if(!OidIsValid(rte->relid))
			{
				ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_TABLE),
					 errmsg("relation \"%s.%s\" does not exist",
							get_namespace_name(nsp_oid), rel_name)));
			}
			/* we must lock relation now */
			pq_copymsgbytes(buf, (char*)&lock_mode, sizeof(lock_mode));
			LockRelationOid(rte->relid, lock_mode);
		}
	}else if(IsA(node, IndexScan) ||
			 IsA(node, IndexOnlyScan) ||
			 IsA(node, BitmapIndexScan))
	{
		IndexScan *scan = (IndexScan*)node;
		char *rel_name;
		Oid nsp_oid;
		StaticAssertExpr(offsetof(IndexScan, indexid) == offsetof(IndexOnlyScan, indexid), "");
		StaticAssertExpr(offsetof(IndexScan, indexid) == offsetof(BitmapIndexScan, indexid), "");

		nsp_oid = load_namespace(buf);
		rel_name = load_node_string(buf, false);
		scan->indexid = get_relname_relid(rel_name, nsp_oid);
		if(!OidIsValid(scan->indexid))
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("index \"%s.%s\" does not exist",
							get_namespace_name(nsp_oid), rel_name)));
		}
	}else if(IsA(node, Var)
		&& !IS_SPECIAL_VARNO(((Var*)node)->varno)
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

	return node;
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
	RdcListenMask  *rdc_masks;
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
	rdc_masks = (RdcListenMask *) palloc0(rdc_cnt * sizeof(RdcListenMask));
	for (i = 0; i < rdc_cnt; i++)
	{
		rdc_masks[i].rdc_host = (char *) pq_getmsgrawstring(&buf);
		pq_copymsgbytes(&buf, (char *) &(rdc_masks[i].rdc_port),
						sizeof(rdc_masks[i].rdc_port));
		pq_copymsgbytes(&buf, (char *) &(rdc_masks[i].rdc_roid),
						sizeof(rdc_masks[i].rdc_roid));
	}
	pq_getmsgend(&buf);

	StartSelfReduceGroup(rdc_masks, rdc_cnt);

	EndSelfReduceGroup();

	pfree(msg.data);
}

static bool
get_rdc_listen_port_hook(void *context, struct pg_conn *conn, PQNHookFuncType type, ...)
{
	RdcListenMask  *rdc_mask = (RdcListenMask *) context;
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
		default:
			ereport(ERROR, (errmsg("unexpected PQNHookFuncType %d", type)));
			break;
	}
	return false;
}

static void
StartRemoteReduceGroup(List *conns, RdcListenMask *rdc_masks, int rdc_cnt)
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
		rpid = rdc_masks[i].rdc_roid;
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
			pfree(msg.data);
			const char *node_name = PQNConnectName(conn);
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("%s", PQerrorMessage(conn)),
					 node_name ? errnode(node_name) : 0));
		}
	}
	pfree(msg.data);
}

static void StartRemotePlan(StringInfo msg, List *rnodes, bool has_reduce, bool self_start_reduce)
{
	ListCell *lc,*lc2;
	List *list_conn;
	PGconn *conn;
	PGresult * volatile res;
	int save_len;

	int				rdc_id;
	int				rdc_cnt;
	RdcListenMask  *rdc_masks = NULL;
	ErrorContextCallback error_context_hook;

	error_context_hook.arg = NULL;
	error_context_hook.callback = ExecClusterErrorHook;
	error_context_hook.previous = error_context_stack;
	error_context_stack = &error_context_hook;

	rdc_id = 0;
	if (has_reduce)
	{
		rdc_cnt = list_length(rnodes) + (self_start_reduce ? 1 : 0);
		rdc_masks = (RdcListenMask *) palloc0(rdc_cnt * sizeof(RdcListenMask));
		if (self_start_reduce)
		{
			rdc_masks[rdc_id].rdc_roid = PGXCNodeOid;
			rdc_masks[rdc_id].rdc_port = 0;	/* fill later */
			rdc_masks[rdc_id].rdc_host = get_pgxc_nodehost(PGXCNodeOid);
			rdc_id++;
		}
	}

	list_conn = PQNGetConnUseOidList(rnodes);
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
		if (has_reduce)
		{
			rdc_masks[rdc_id].rdc_roid = lfirst_oid(lc2);
			rdc_masks[rdc_id].rdc_port = 0;	/* fill later */
			rdc_masks[rdc_id].rdc_host = get_pgxc_nodehost(lfirst_oid(lc2));
			rdc_id++;
		}

		/* send plan info */
		conn = lfirst(lc);
		if(PQsendPlan(conn, msg->data, msg->len) == false)
		{
			safe_pfree(rdc_masks);
			const char *node_name = PQNConnectName(conn);
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
		if (has_reduce && self_start_reduce)
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
	if (has_reduce)
	{
		/* wait for listen port of other reduce */
		foreach(lc, list_conn)
		{
			conn = lfirst(lc);
			PQNOneExecFinish(conn, get_rdc_listen_port_hook, &rdc_masks[rdc_id], true);
			rdc_id++;
		}

		/* have already got all listen port of other reduces */
		if (self_start_reduce)
			StartSelfReduceGroup(rdc_masks, rdc_id);

		/* tell other reduce infomation about reduce group */
		StartRemoteReduceGroup(list_conn, rdc_masks, rdc_id);

		/* wait for self reduce start reduce group OK */
		EndSelfReduceGroup();

		safe_pfree(rdc_masks);
	}

	error_context_stack = error_context_hook.previous;
}

static bool InstrumentEndLoop_walker(PlanState *ps, void *context)
{
	if(ps == NULL)
		return false;
	if(ps->instrument)
		InstrEndLoop(ps->instrument);
	return planstate_tree_walker(ps, InstrumentEndLoop_walker, NULL);
}

static void ExecClusterErrorHook(void *arg)
{
	const ErrorData *err_data;

	if ((err_data = err_current_data()) != NULL &&
		err_data->elevel >= ERROR)
	{
		PGXCNodeOid = (Oid)(Size)(arg);
		EndSelfReduce(0, 0);
	}
}
