#include "postgres.h"

#include "access/xact.h"

#include "executor/execdesc.h"
#include "executor/executor.h"
#include "pgxc/pgxcnode.h"
#include "lib/stringinfo.h"
#include "libpq/libpq.h"
#include "libpq/libpq-node.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "storage/mem_toc.h"
#include "tcop/dest.h"
#include "utils/combocid.h"
#include "utils/lsyscache.h"
#include "utils/ps_status.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "executor/execCluster.h"

#include "libpq/libpq-fe.h"

#define REMOTE_KEY_END						0xFFFFFF00
#define REMOTE_KEY_TRANSACTION_STATE		0xFFFFFF01
#define REMOTE_KEY_GLOBAL_SNAPSHOT			0xFFFFFF02
#define REMOTE_KEY_ACTIVE_SNAPSHOT			0xFFFFFF03
#define REMOTE_KEY_COMBO_CID				0xFFFFFF04
#define REMOTE_KEY_PLAN_STMT				0xFFFFFF05
#define REMOTE_KEY_PARAM					0xFFFFFF06
#define REMOTE_KEY_NODE_OID					0xFFFFFF07
#define REMOTE_KEY_RTE_LIST					0xFFFFFF08

static Oid cluster_node_oid = InvalidOid;

static void restore_cluster_plan_info(StringInfo buf);
static QueryDesc *create_cluster_query_desc(StringInfo buf, DestReceiver *r);

static void SerializePlanInfo(StringInfo msg, PlannedStmt *stmt, ParamListInfo param);
static bool SerializePlanHook(StringInfo buf, Node *node, void *context);
static void *LoadPlanHook(StringInfo buf, NodeTag tag, void *context);
static void StartRemotePlan(StringInfo msg, List *rnodes);

void exec_cluster_plan(const void *splan, int length)
{
	QueryDesc *query_desc;
	DestReceiver *receiver;
	StringInfoData buf;

	buf.data = (char*)splan;
	buf.cursor = 0;
	buf.len = buf.maxlen = length;

	restore_cluster_plan_info(&buf);
	receiver = CreateDestReceiver(DestClusterOut);
	query_desc = create_cluster_query_desc(&buf, receiver);

	ExecutorStart(query_desc, 0);

	set_ps_display("<claster query>", false);

	/* Send a message
	 * 'H' for copy out, 'W' for copy both */
	pq_beginmessage(&buf, 'W');
	pq_sendbyte(&buf, 0);
	pq_sendint(&buf, 0, 2);
	pq_endmessage(&buf);
	pq_flush();

	/* run plan */
	ExecutorRun(query_desc, ForwardScanDirection, 0L);

	/* and clean up */
	ExecutorFinish(query_desc);
	ExecutorEnd(query_desc);
	FreeQueryDesc(query_desc);
	cluster_node_oid = InvalidOid;

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
	memcpy(&cluster_node_oid, ptr, sizeof(cluster_node_oid));

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
	return CreateQueryDesc(stmt,
						   "<claster query>",
						   GetTransactionSnapshot()/*GetActiveSnapshot()*/, InvalidSnapshot,
						   r, paramLI, 0);
}

/************************************************************************/

PlanState* ExecStartClusterPlan(Plan *plan, EState *estate, int eflags, List *rnodes)
{
	PlannedStmt *stmt;
	StringInfoData msg;
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

	StartRemotePlan(&msg, rnodes);
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
	saveNode(msg, (Node*)stmt);
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
		/* save is temp table ? */
		appendStringInfoChar(buf, RelationUsesLocalBuffers(rel) ? '\1':'\0');
		/* save namespace */
		save_namespace(buf, RelationGetNamespace(rel));
		/* save relation name */
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
		}
	}else if(IsA(node, Var)
		&& !IS_SPECIAL_VARNO(((Var*)node)->varno)
		&& ((Var*)node)->varattno > 0)
	{
		/* check column attribute */
		Form_pg_attribute attr;
		Var *var = (Var*)node;
		Relation rel = ((Relation*)context)[var->varno-1];
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

	return node;
}

static void StartRemotePlan(StringInfo msg, List *rnodes)
{
	ListCell *lc,*lc2;
	List *list_conn;
	PGconn *conn;
	PGresult * volatile res;
	int save_len;

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

		/* send plan info */
		conn = lfirst(lc);
		if(PQsendPlan(conn, msg->data, msg->len) == false)
		{
			const char *node_name = PQNConnectName(conn);
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
				errmsg("%s", PQerrorMessage(conn)),
				node_name ? errnode(node_name) : 0));
		}
	}
	msg->len = save_len;

	res = NULL;
	PG_TRY();
	{
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
		PG_RE_THROW();
	}PG_END_TRY();
}

Oid get_cluster_node_oid(void)
{
	return cluster_node_oid;
}