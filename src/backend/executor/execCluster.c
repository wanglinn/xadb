#include "postgres.h"

#include "access/transam.h"
#include "access/xact.h"
#include "catalog/pgxc_node.h"
#include "catalog/ora_convert.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "commands/matview.h"
#include "commands/vacuum.h"
#include "commands/cluster.h"
#include "executor/clusterHeapScan.h"
#include "executor/execdesc.h"
#include "executor/executor.h"
#include "executor/nodeEmptyResult.h"
#include "intercomm/inter-comm.h"
#include "pgxc/groupmgr.h"
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
#include "pgstat.h"
#include "pgxc/nodemgr.h"
#include "storage/lmgr.h"
#include "storage/mem_toc.h"
#include "tcop/dest.h"
#include "utils/builtins.h"
#include "utils/combocid.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "executor/clusterReceiver.h"
#include "executor/execCluster.h"

#include "libpq/libpq-fe.h"
#include "pgxc/redistrib.h"

#include "utils/dynamicreduce.h"
#include "pgxc/slot.h"


#define REMOTE_KEY_END						0xFFFFFF00
#define REMOTE_KEY_TRANSACTION_STATE		0xFFFFFF01
#define REMOTE_KEY_GLOBAL_SNAPSHOT			0xFFFFFF02
#define REMOTE_KEY_ACTIVE_SNAPSHOT			0xFFFFFF03
#define REMOTE_KEY_PLAN_STMT				0xFFFFFF04
#define REMOTE_KEY_PARAM					0xFFFFFF05
#define REMOTE_KEY_NODE_OID					0xFFFFFF06
#define REMOTE_KEY_RTE_LIST					0xFFFFFF07
#define REMOTE_KEY_ES_INSTRUMENT			0xFFFFFF08
#define REMOTE_KEY_REDUCE_INFO				0xFFFFFF09
#define REMOTE_KEY_REDUCE_GROUP				0xFFFFFF0A
#define REMOTE_KEY_CUSTOM_FUNCTION			0xFFFFFF0B
#define REMOTE_KEY_COORD_INFO				0xFFFFFF0C
#define REMOTE_KEY_QUERY_STRING_INFO		0xFFFFFF0D

#define CLUSTER_CUSTOM_NEED_SEND_STAT			1
#define CLUSTER_CUSTOM_NO_NEED_SEND_STAT		2

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
	uint32					saved_node_id;
	bool					saved_enable_cluster_plan;
	bool					saved_in_cluster_mode;
}ClusterErrorHookContext;

typedef struct ClusterCustomExecInfo
{
	ClusterCustom_function func;
	const char *FuncString;
	const int flag;
}ClusterCustomExecInfo;
#define CLUSTER_CUSTOM_EXEC_FUNC(fun_, flag) fun_, #fun_, flag

typedef struct ClusterCoordInfo
{
	const char *name;
	int			pid;
	Oid			oid;
}ClusterCoordInfo;

typedef struct GetRDCListenPortHook
{
	PQNHookFunctions		pub;
	DynamicReduceNodeInfo  *rdc_info;
}GetRDCListenPortHook;

extern bool enable_cluster_plan;
bool in_cluster_mode = false;

static void ExecClusterPlanStmt(StringInfo buf, ClusterCoordInfo *info);
static void ExecClusterCopyStmt(StringInfo buf, ClusterCoordInfo *info);
static void ExecClusterAuxPadding(StringInfo buf, ClusterCoordInfo *info);
static NodeTag GetClusterPlanType(StringInfo buf);

static void restore_cluster_plan_info(StringInfo buf);
static QueryDesc *create_cluster_query_desc(StringInfo buf, DestReceiver *r);

static void SerializePlanInfo(StringInfo msg, PlannedStmt *stmt, ParamListInfo param, ClusterPlanContext *context);
static void SerializeTransactionInfo(StringInfo msg);
static bool SerializePlanHook(StringInfo buf, Node *node, void *context);
static void *LoadPlanHook(StringInfo buf, NodeTag tag, void *context);
static void* loadNodeType(StringInfo buf, NodeTag tag, NodeTag *ptag);
static bool HaveModifyPlanWalker(Plan *plan, Node *GlobOrStmt, void *context);
static void SerializeRelationOid(StringInfo buf, Oid relid);
static Oid RestoreRelationOid(StringInfo buf, bool missok);
static void SerializeCoordinatorInfo(StringInfo buf);
static void SerializeDebugQueryString(StringInfo buf);
static ClusterCoordInfo* RestoreCoordinatorInfo(StringInfo buf);
static const char* RestoreDebugQueryString(StringInfo buf);
static void send_rdc_listend_port(int port);
static void wait_rdc_group_message(void);
static bool get_rdc_listen_port_hook(PQNHookFunctions *pub, struct pg_conn *conn, const char *buf, int len);
static void StartRemoteReduceGroup(List *conns, DynamicReduceNodeInfo *rdc_info, uint32 rdc_cnt);
static List* StartRemotePlan(StringInfo msg, List *rnodes, ClusterPlanContext *context, bool start_trans);
static bool InstrumentEndLoop_walker(PlanState *ps, Bitmapset **called);
static void InstrumentEndLoop_cluster(PlanState *ps);
static bool RelationIsCoordOnly(Oid relid);

static void ExecClusterErrorHookMaster(void *arg);
static void ExecClusterErrorHookNode(void *arg);
static void SetupClusterErrorHook(ClusterErrorHookContext *context);
static void RestoreClusterHook(ClusterErrorHookContext *context);

static const ClusterCustomExecInfo* find_custom_func_info(StringInfo mem_toc, bool noError);

static const ClusterCustomExecInfo cluster_custom_execute[] =
	{
		{CLUSTER_CUSTOM_EXEC_FUNC(DoClusterHeapScan, CLUSTER_CUSTOM_NEED_SEND_STAT)}
		,{CLUSTER_CUSTOM_EXEC_FUNC(ClusterRefreshMatView, CLUSTER_CUSTOM_NEED_SEND_STAT)}
		,{CLUSTER_CUSTOM_EXEC_FUNC(cluster_vacuum, CLUSTER_CUSTOM_NEED_SEND_STAT)}
		,{CLUSTER_CUSTOM_EXEC_FUNC(ClusterNodeAlter, CLUSTER_CUSTOM_NEED_SEND_STAT)}
		,{CLUSTER_CUSTOM_EXEC_FUNC(ClusterNodeRemove, CLUSTER_CUSTOM_NEED_SEND_STAT)}
		,{CLUSTER_CUSTOM_EXEC_FUNC(ClusterNodeCreate, CLUSTER_CUSTOM_NEED_SEND_STAT)}
		,{CLUSTER_CUSTOM_EXEC_FUNC(ClusterExecImplicitConvert, CLUSTER_CUSTOM_NEED_SEND_STAT)}
		,{CLUSTER_CUSTOM_EXEC_FUNC(ClusterPgxcGroupCreate, CLUSTER_CUSTOM_NEED_SEND_STAT)}
		,{CLUSTER_CUSTOM_EXEC_FUNC(ClusterPgxcGroupRemove, CLUSTER_CUSTOM_NEED_SEND_STAT)}
		,{CLUSTER_CUSTOM_EXEC_FUNC(cluster_cluster, CLUSTER_CUSTOM_NO_NEED_SEND_STAT)}
		,{CLUSTER_CUSTOM_EXEC_FUNC(ClusterRedistributeRelation, CLUSTER_CUSTOM_NO_NEED_SEND_STAT)}
	};

static void set_cluster_display(const char *activity, bool force, ClusterCoordInfo *info);

static DynamicReduceNodeInfo   *CnRdcInfo = NULL;
static uint32					CnRdcCnt = 0;

Oid
GetCurrentCnRdcID(const char *rdc_name)
{
	int i;

	for (i = 0; i < CnRdcCnt; i++)
	{
		DynamicReduceNodeInfo *info = &CnRdcInfo[i];
		if (pg_strcasecmp(NameStr(info->name), rdc_name) == 0)
			return info->node_oid;
	}

	return InvalidOid;
}

void exec_cluster_plan(const void *splan, int length)
{
	char *tmp;
	const ClusterCustomExecInfo *custom_fun;
	ClusterCoordInfo *info;
	StringInfoData msg;
	NodeTag tag;
	ClusterErrorHookContext error_context_hook;
	static const char copy_msg[] = {1,		/* format, ignore */
									0, 0	/* natts, ignore */
											/* no more attr send */};

	msg.data = (char*)splan;
	msg.len = msg.maxlen = length;
	msg.cursor = 0;

	custom_fun = find_custom_func_info(&msg, true);
	if (custom_fun == NULL)
		tag = GetClusterPlanType(&msg);
	else
		tag = T_Invalid;

	SetupClusterErrorHook(&error_context_hook);
	restore_cluster_plan_info(&msg);
	info = RestoreCoordinatorInfo(&msg);
	debug_query_string = RestoreDebugQueryString(&msg);
	pgstat_report_activity(STATE_RUNNING,
						   debug_query_string ? debug_query_string : "<cluster query>");

	/* Send a message
	 * 'H' for copy out, 'W' for copy both */
	pq_putmessage('W', copy_msg, sizeof(copy_msg));
	pq_flush();

	SaveTableStatSnapshot();

	if ((tmp=mem_toc_lookup(&msg, REMOTE_KEY_REDUCE_INFO, NULL)) != NULL)
	{
		/* need reduce */
		uint32 rdc_listen_port;

		/* Start self Reduce with rdc_id */
		set_cluster_display("<cluster start self reduce>", false, info);
		rdc_listen_port = StartDynamicReduceWorker();

		/* Tell coordinator self own listen port */
		send_rdc_listend_port(rdc_listen_port);

		/* Wait for the whole Reduce connect OK */
		set_cluster_display("<cluster start group reduce>", false, info);
		wait_rdc_group_message();
		DatanodeInClusterPlan = true;
	}

	switch(tag)
	{
	case T_Invalid:
		if (custom_fun == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("Can not found valid plan info")));
		set_cluster_display(custom_fun->FuncString, false, info);
		(*custom_fun->func)(&msg);
		break;
	case T_PlannedStmt:
		ExecClusterPlanStmt(&msg, info);
		break;
	case T_CopyStmt:
		ExecClusterCopyStmt(&msg, info);
		break;
	case T_PaddingAuxDataStmt:
		ExecClusterAuxPadding(&msg, info);
		break;
	default:
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("unknown cluster plan type %d", tag)));
	}

	/* send stat */
	if ((!custom_fun) || (custom_fun->flag != CLUSTER_CUSTOM_NO_NEED_SEND_STAT))
	{
		initStringInfo(&msg);
		if(SerializeTableStat(&msg))
			pq_putmessage('d', msg.data, msg.len);
	}
	pfree(msg.data);

	if (ActiveSnapshotSet())
	{
		PopActiveSnapshot();
		Assert(ActiveSnapshotSet() == false);
	}
	EndClusterTransaction();

	RestoreClusterHook(&error_context_hook);

	/* Send Copy Done message */
	pq_putemptymessage('c');
	DatanodeInClusterPlan = false;

	DestroyTableStateSnapshot();
}

static void ExecClusterPlanStmt(StringInfo buf, ClusterCoordInfo *info)
{
	QueryDesc *query_desc;
	DestReceiver *receiver;
	StringInfoData msg;
	int eflags;
	bool need_instrument;

	enable_cluster_plan = true;

	receiver = CreateDestReceiver(DestClusterOut);
	query_desc = create_cluster_query_desc(buf, receiver);
	if(mem_toc_lookup(buf, REMOTE_KEY_ES_INSTRUMENT, NULL) != NULL)
		need_instrument = true;
	else
		need_instrument = false;

	eflags = 0;
	if (query_desc->plannedstmt->hasModifyingCTE ||
		query_desc->plannedstmt->rowMarks != NIL ||
		HaveModifyPlanWalker(query_desc->plannedstmt->planTree, NULL, NULL))
		eflags |= EXEC_FLAG_UPDATE_CMD_ID;

	ExecutorStart(query_desc, eflags);
	clusterRecvSetTopPlanState(receiver, query_desc->planstate);

	set_cluster_display("<advance reduce>", false, info);
	if (query_desc->totaltime)
		InstrStartNode(query_desc->totaltime);
	if (query_desc->totaltime)
		InstrStopNode(query_desc->totaltime, 0);

	set_cluster_display("<cluster query>", false, info);

	/* run plan */
	ExecutorRun(query_desc, ForwardScanDirection, 0L, true);

	/* send processed message */
	initStringInfo(&msg);
	serialize_processed_message(&msg, query_desc->estate->es_processed);
	pq_putmessage('d', msg.data, msg.len);

	/* and send function ExecutorRun finish */
	resetStringInfo(&msg);
	appendStringInfoChar(&msg, CLUSTER_MSG_EXECUTOR_RUN_END);
	pq_putmessage('d', msg.data, msg.len);
	pq_flush();

	/* and finish */
	ExecutorFinish(query_desc);

	if(need_instrument)
		InstrumentEndLoop_cluster(query_desc->planstate);

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

	pfree(msg.data);
}

static void ExecClusterCopyStmt(StringInfo buf, ClusterCoordInfo *info)
{
	CopyStmt *stmt;
	StringInfoData msg;

	msg.data = mem_toc_lookup(buf, REMOTE_KEY_PLAN_STMT, &msg.len);
	Assert(msg.data != NULL && msg.len > 0);
	msg.maxlen = msg.len;
	msg.cursor = 0;

	stmt = (CopyStmt*)loadNode(&msg);
	Assert(IsA(stmt, CopyStmt));
	set_cluster_display("<CLUSTER COPY FROM>", false, info);

	DoClusterCopy(stmt, buf);
}

static void
ExecClusterAuxPadding(StringInfo buf, ClusterCoordInfo *info)
{
	PaddingAuxDataStmt *stmt;
	StringInfoData		msg;

	msg.data = mem_toc_lookup(buf, REMOTE_KEY_PLAN_STMT, &msg.len);
	Assert(msg.data != NULL && msg.len > 0);
	msg.maxlen = msg.len;
	msg.cursor = 0;

	stmt = (PaddingAuxDataStmt *) loadNode(&msg);
	Assert(IsA(stmt, PaddingAuxDataStmt));
	set_cluster_display("<PADDING AUXILIARY DATA>", false, info);

	ExecPaddingAuxDataStmt(stmt, buf);
}

static NodeTag GetClusterPlanType(StringInfo buf)
{
	NodeTag tag;
	StringInfoData plan;

	plan.data = mem_toc_lookup(buf, REMOTE_KEY_PLAN_STMT, &plan.len);
	if (plan.data == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("can not found cluster plan info")));
	}
	plan.maxlen = plan.len;
	plan.cursor = 0;

	tag = T_Invalid;
	loadNodeAndHook(&plan, loadNodeType, &tag);

	return tag;
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
	memcpy(&PGXCNodeIdentifier, ptr+sizeof(PGXCNodeOid), sizeof(PGXCNodeIdentifier));

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
	RangeTblEntry *rte;
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
		rte = lfirst(lc);
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
						   "<cluster query>",
						   GetTransactionSnapshot()/*GetActiveSnapshot()*/, InvalidSnapshot,
						   r, paramLI, NULL, es_instrument);
}

/************************************************************************/
PlanState* ExecStartClusterPlan(Plan *plan, EState *estate, int eflags, List *rnodes)
{
	PlannedStmt *stmt;
	StringInfoData msg;
	ClusterPlanContext context;

	AssertArg(plan);
	AssertArg(rnodes != NIL);

	if(eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return ExecInitNode(plan, estate, eflags);

	stmt = palloc(sizeof(*stmt));
	memcpy(stmt, estate->es_plannedstmt, sizeof(*stmt));
	stmt->planTree = plan;
	/* make sure remote send tuple(s) */
	stmt->commandType = CMD_SELECT;

	/* this is a fake PlannedStmt */
	stmt->stmt_location = -1;
	stmt->stmt_len = 0;

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

	pfree(stmt);
	if (context.have_reduce)
	{
		begin_mem_toc_insert(&msg, REMOTE_KEY_REDUCE_INFO);
		appendStringInfoChar(&msg, (char)false);	/* not memory module */
		end_mem_toc_insert(&msg, REMOTE_KEY_REDUCE_INFO);
	}

	SerializeCoordinatorInfo(&msg);

	StartRemotePlan(&msg, rnodes, &context, true);
	pfree(msg.data);

	return ExecInitNode(plan, estate, eflags);
}

List* ExecStartClusterCopy(List *rnodes, struct CopyStmt *stmt, StringInfo mem_toc, uint32 flag)
{
	ClusterPlanContext context;
	StringInfoData msg;
	List *conn_list;
	initStringInfo(&msg);

	if(mem_toc)
		appendBinaryStringInfo(&msg, mem_toc->data, mem_toc->len);

	begin_mem_toc_insert(&msg, REMOTE_KEY_PLAN_STMT);
	saveNode(&msg, (Node*)stmt);
	end_mem_toc_insert(&msg, REMOTE_KEY_PLAN_STMT);

	SerializeTransactionInfo(&msg);

	MemSet(&context, 0, sizeof(context));
	context.transaction_read_only = false;
	context.have_temp = false;
	if (flag & EXEC_CLUSTER_FLAG_NEED_REDUCE)
	{
		context.have_reduce = true;
		begin_mem_toc_insert(&msg, REMOTE_KEY_REDUCE_INFO);
		appendStringInfoChar(&msg, (char)((flag & EXEC_CLUSTER_FLAG_USE_MEM_REDUCE) ? true:false));
		end_mem_toc_insert(&msg, REMOTE_KEY_REDUCE_INFO);
	}
	if (flag & EXEC_CLUSTER_FLAG_NEED_SELF_REDUCE)
	{
		Assert(context.have_reduce);
		context.start_self_reduce = true;
	}

	SerializeCoordinatorInfo(&msg);

	conn_list = StartRemotePlan(&msg, rnodes, &context, true);
	Assert(list_length(conn_list) == list_length(rnodes));

	pfree(msg.data);

	return conn_list;
}

List *
ExecStartClusterAuxPadding(List *rnodes, Node *stmt, StringInfo mem_toc, uint32 flag)
{
	ClusterPlanContext context;
	StringInfoData msg;
	List *conn_list;
	initStringInfo(&msg);

	if(mem_toc)
		appendBinaryStringInfo(&msg, mem_toc->data, mem_toc->len);

	begin_mem_toc_insert(&msg, REMOTE_KEY_PLAN_STMT);
	saveNode(&msg, stmt);
	end_mem_toc_insert(&msg, REMOTE_KEY_PLAN_STMT);

	SerializeTransactionInfo(&msg);

	MemSet(&context, 0, sizeof(context));
	context.transaction_read_only = false;
	context.have_temp = false;
	if (flag & EXEC_CLUSTER_FLAG_NEED_REDUCE)
	{
		context.have_reduce = true;
		begin_mem_toc_insert(&msg, REMOTE_KEY_REDUCE_INFO);
		appendStringInfoChar(&msg, (char)((flag & EXEC_CLUSTER_FLAG_USE_MEM_REDUCE) ? true:false));
		end_mem_toc_insert(&msg, REMOTE_KEY_REDUCE_INFO);
	}
	if (flag & EXEC_CLUSTER_FLAG_NEED_SELF_REDUCE)
	{
		Assert(context.have_reduce);
		context.start_self_reduce = true;
	}

	SerializeCoordinatorInfo(&msg);

	conn_list = StartRemotePlan(&msg, rnodes, &context, true);
	Assert(list_length(conn_list) == list_length(rnodes));

	pfree(msg.data);

	return conn_list;
}

void ClusterTocSetCustomFunStr(StringInfo mem_toc, const char *proc)
{
	begin_mem_toc_insert(mem_toc, REMOTE_KEY_CUSTOM_FUNCTION);
	appendStringInfoString(mem_toc, proc);
	appendStringInfoChar(mem_toc, '\0');
	end_mem_toc_insert(mem_toc, REMOTE_KEY_CUSTOM_FUNCTION);
}

static const ClusterCustomExecInfo* find_custom_func_info(StringInfo mem_toc, bool noError)
{
	const char *str = mem_toc_lookup(mem_toc, REMOTE_KEY_CUSTOM_FUNCTION, NULL);
	Size i;
	if (str == NULL)
	{
		if (noError)
			return NULL;
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("Can not found custom cluster function info")));
	}

	for(i=0;i<lengthof(cluster_custom_execute);++i)
	{
		if (strcmp(cluster_custom_execute[i].FuncString, str) == 0)
			return &cluster_custom_execute[i];
	}

	if (!noError)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("Can not found custom cluster function \"%s\"", str)));

	return NULL;
}

List* ExecClusterCustomFunction(List *rnodes, StringInfo mem_toc, uint32 flag)
{
	ClusterPlanContext context;

	/* check custom function */
	find_custom_func_info(mem_toc, false);

	SerializeTransactionInfo(mem_toc);

	SerializeCoordinatorInfo(mem_toc);

	SerializeDebugQueryString(mem_toc);

	MemSet(&context, 0, sizeof(context));
	context.transaction_read_only = ((flag & EXEC_CLUSTER_FLAG_READ_ONLY) ? true:false);
	if (flag & EXEC_CLUSTER_FLAG_NEED_REDUCE)
	{
		context.have_reduce = true;
		begin_mem_toc_insert(mem_toc, REMOTE_KEY_REDUCE_INFO);
		appendStringInfoChar(mem_toc, (char)((flag & EXEC_CLUSTER_FLAG_USE_MEM_REDUCE) ? true:false));
		end_mem_toc_insert(mem_toc, REMOTE_KEY_REDUCE_INFO);
	}

	if (flag & EXEC_CLUSTER_FLAG_NEED_SELF_REDUCE)
	{
		Assert(context.have_reduce);
		context.start_self_reduce = true;
	}

	return StartRemotePlan(mem_toc, rnodes, &context, (flag & EXEC_CLUSTER_FLAG_NOT_START_TRANS) ? false:true);
}

static void SerializePlanInfo(StringInfo msg, PlannedStmt *stmt,
							  ParamListInfo param, ClusterPlanContext *context)
{
	ListCell *lc;
	List *rte_list;
	PlannedStmt *new_stmt;
	Bitmapset *coord_only_rti = NULL;
	Index rti;

	new_stmt = palloc(sizeof(*new_stmt));
	memcpy(new_stmt, stmt, sizeof(*new_stmt));
	new_stmt->rtable = NIL;

	if (context)
	{
		context->have_temp = false;
		context->transaction_read_only = true;
		context->have_reduce = false;
		context->start_self_reduce = false;
	}

	/* modify range table if relation is in coordinator only */
	rti = 1;
	rte_list = NIL;
	foreach(lc, stmt->rtable)
	{
		RangeTblEntry *rte = lfirst(lc);
		if (rte->rtekind == RTE_RELATION)
		{
			bool coord_only = false;
			if (rte->relkind == RELKIND_VIEW ||
				rte->relkind == RELKIND_MATVIEW ||
				rte->relkind == RELKIND_FOREIGN_TABLE)
			{
				coord_only = true;
			}else if((rte->relkind == RELKIND_RELATION ||
					  rte->relkind == RELKIND_PARTITIONED_TABLE) &&
					 is_relid_remote(rte->relid) == false)
			{
				coord_only = true;
			}

			if (coord_only)
			{
				RangeTblEntry *new_rte = palloc(sizeof(*rte));
				memcpy(new_rte, rte, sizeof(*rte));
				new_rte->rtekind = RTE_REMOTE_DUMMY;
				new_rte->relid = InvalidOid;
				rte = new_rte;

				coord_only_rti = bms_add_member(coord_only_rti, rti);
			}
		}
		rte_list = lappend(rte_list, rte);
		++rti;
	}

	/* serialize range table */
	begin_mem_toc_insert(msg, REMOTE_KEY_RTE_LIST);
	saveNodeAndHook(msg, (Node*)rte_list, SerializePlanHook, context);
	end_mem_toc_insert(msg, REMOTE_KEY_RTE_LIST);

	/* modify RowMarks if relation is in coordinator only */
	if (stmt->rowMarks != NIL)
	{
		/* do not lock view, foreign table, matview and temporary table,
		   because it only in coordinator
		 */
		PlanRowMark *rowmark;
		new_stmt->rowMarks = NIL;
		foreach (lc, stmt->rowMarks)
		{
			rowmark = copyObject(lfirst(lc));

			/* let mark type is copy(don't lock relation) when relation only in coordinator */
			if (bms_is_member(rowmark->rti, coord_only_rti))
				rowmark->markType = ROW_MARK_COPY;

			new_stmt->rowMarks = lappend(new_stmt->rowMarks, rowmark);
		}
	}

	begin_mem_toc_insert(msg, REMOTE_KEY_PLAN_STMT);
	saveNodeAndHook(msg, (Node*)new_stmt, SerializePlanHook, context);
	end_mem_toc_insert(msg, REMOTE_KEY_PLAN_STMT);

	begin_mem_toc_insert(msg, REMOTE_KEY_PARAM);
	SaveParamList(msg, param);
	end_mem_toc_insert(msg, REMOTE_KEY_PARAM);

	SerializeTransactionInfo(msg);

	foreach(lc, rte_list)
	{
		/* pfree we palloced memory */
		if (list_member_ptr(stmt->rtable, lfirst(lc)) == false)
			pfree(lfirst(lc));
	}
	list_free(rte_list);
	bms_free(coord_only_rti);
	if (new_stmt->rowMarks)
		list_free_deep(new_stmt->rowMarks);
	pfree(new_stmt);
}

static void SerializeTransactionInfo(StringInfo msg)
{
	Size size;

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
	case T_ClusterGather:
	case T_ClusterMergeGather:
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Invalid plan tree, because have one more cluster gather")));
		break;	/* keep compiler quiet */
	case T_ClusterReduce:
		if (context)
		{
			ClusterPlanContext *cpc = (ClusterPlanContext*)context;
			cpc->have_reduce = true;
			if (cpc->start_self_reduce == false &&
				list_member_oid(((ClusterReduce*)node)->reduce_oids, PGXCNodeOid))
			{
				cpc->start_self_reduce = true;
			}
		}
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
	case T_ClusterGather:
	case T_ClusterMergeGather:
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Invalid plan tree, because have one more cluster gather")));
		break;	/* keep compiler quiet */
	case T_Agg:
		if (((Agg*)node)->aggsplit != AGGSPLIT_INITIAL_SERIAL &&
			list_member_oid(((Agg*)node)->exec_nodes, PGXCNodeOid) == false)
			node = (Node*)MakeEmptyResultPlan((Plan*)node);
		break;
	/* if make empty plan using execute_nodes, must modify is_cluster_base_relation_scan_plan function */ 
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

static void* loadNodeType(StringInfo buf, NodeTag tag, NodeTag *ptag)
{
	*ptag = tag;
	return ptag;
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

static void SerializeCoordinatorInfo(StringInfo buf)
{
	begin_mem_toc_insert(buf, REMOTE_KEY_COORD_INFO);
	appendBinaryStringInfo(buf, (char*)&MyProcPid, sizeof(MyProcPid));
	appendBinaryStringInfo(buf, (char*)&PGXCNodeOid, sizeof(PGXCNodeOid));
	appendBinaryStringInfo(buf, PGXCNodeName, strlen(PGXCNodeName)+1);
	end_mem_toc_insert(buf, REMOTE_KEY_COORD_INFO);
}

static void SerializeDebugQueryString(StringInfo buf)
{
	if (debug_query_string)
	{
		begin_mem_toc_insert(buf, REMOTE_KEY_QUERY_STRING_INFO);
		appendBinaryStringInfo(buf, debug_query_string, strlen(debug_query_string)+1);
		end_mem_toc_insert(buf, REMOTE_KEY_QUERY_STRING_INFO);
	}
}

static const char* RestoreDebugQueryString(StringInfo buf)
{
	StringInfoData msg;

	msg.data = mem_toc_lookup(buf, REMOTE_KEY_QUERY_STRING_INFO, &msg.maxlen);
	if (msg.data == NULL)
		return NULL;
	msg.cursor = 0;
	msg.len = msg.maxlen;

	return pq_getmsgrawstring(&msg);
}

static ClusterCoordInfo* RestoreCoordinatorInfo(StringInfo buf)
{
	ClusterCoordInfo *info;
	StringInfoData msg;

	msg.data = mem_toc_lookup(buf, REMOTE_KEY_COORD_INFO, &msg.maxlen);
	if (msg.data == NULL)
		return NULL;
	msg.cursor = 0;
	msg.len = msg.maxlen;

	info = palloc0(sizeof(*info));
	pq_copymsgbytes(&msg, (char*)&(info->pid), sizeof(info->pid));
	pq_copymsgbytes(&msg, (char*)&(info->oid), sizeof(info->oid));
	info->name = pq_getmsgrawstring(&msg);

	return info;
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
	int				type;
	StringInfoData	buf;
	StringInfoData	msg;
	MemoryContext	oldcontext;

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

	/* pfree old CnRdcInfo */
	if (CnRdcInfo != NULL)
	{
		pfree(CnRdcInfo);
		CnRdcInfo = NULL;
		CnRdcCnt = 0;
	}

	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	CnRdcCnt = RestoreDynamicReduceNodeInfo(&buf, &CnRdcInfo);
	Assert(CnRdcCnt > 0);
	(void) MemoryContextSwitchTo(oldcontext);
	/*pq_getmsgend(&buf);*/

	DynamicReduceConnectNet(CnRdcInfo, CnRdcCnt);

	pfree(msg.data);
}

static bool
get_rdc_listen_port_hook(PQNHookFunctions *pub, struct pg_conn *conn, const char *buf, int len)
{
	DynamicReduceNodeInfo *info = ((GetRDCListenPortHook*)pub)->rdc_info;

	if(clusterRecvRdcListenPort(conn, buf, len, &(info->port)))
		return true;

	return false;
}

static void
StartRemoteReduceGroup(List *conns, DynamicReduceNodeInfo *rdc_info, uint32 rdc_cnt)
{
	StringInfoData	msg;
	ListCell	   *lc;
	PGconn		   *conn;

	AssertArg(conns && rdc_info);
	AssertArg(rdc_cnt > 0);

	initStringInfo(&msg);
	begin_mem_toc_insert(&msg, REMOTE_KEY_REDUCE_GROUP);
	SerializeDynamicReduceNodeInfo(&msg, rdc_info, rdc_cnt);
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

static void
PrepareReduceInfo(DynamicReduceNodeInfo *info, Oid nodeid)
{
	info->node_oid = nodeid;
	info->port = 0;
	get_pgxc_node_name_and_host(nodeid, &info->name, &info->host);
}

static List* StartRemotePlan(StringInfo msg, List *rnodes, ClusterPlanContext *context, bool start_trans)
{
	ListCell *lc;
	List *list_conn;
	PGconn *conn;
	PGresult * volatile res;
	int save_len;

	uint32					rdc_id;
	uint32					rdc_cnt;
	DynamicReduceNodeInfo  *rdc_info = NULL;

	ErrorContextCallback	error_context_hook;
	InterXactState			state;
	NodeHandle			   *handle;

	Assert(rnodes);
	/* try to start transaction */
	state = GetCurrentInterXactState();
	if (start_trans &&
		!context->transaction_read_only)
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
		rdc_info = (DynamicReduceNodeInfo *) palloc0(rdc_cnt * sizeof(DynamicReduceNodeInfo));
		if (context->start_self_reduce)
		{
			PrepareReduceInfo(&rdc_info[rdc_id], PGXCNodeOid);
			rdc_id++;
		}
	}

	save_len = msg->len;
	foreach(lc, state->cur_handle->handles)
	{
		handle = (NodeHandle *) lfirst(lc);

		/* send node oid to remote */
		msg->len = save_len;
		begin_mem_toc_insert(msg, REMOTE_KEY_NODE_OID);
		appendBinaryStringInfoNT(msg, (char*)&(handle->node_id), sizeof(handle->node_id));
		StaticAssertStmt(sizeof(handle->hashvalue) == sizeof(PGXCNodeIdentifier), "need change code");
		appendBinaryStringInfoNT(msg, (char*)&(handle->hashvalue), sizeof(handle->hashvalue));
		end_mem_toc_insert(msg, REMOTE_KEY_NODE_OID);

		/* send reduce group map and reduce ID to remote */
		if (context->have_reduce)
		{
			PrepareReduceInfo(&rdc_info[rdc_id], handle->node_id);
			rdc_id++;
		}

		/* send plan info */
		conn = handle->node_conn;
		if(PQsendPlan(conn, msg->data, msg->len) == false)
		{
			const char *node_name = PQNConnectName(conn);
			safe_pfree(rdc_info);
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("%s", PQerrorMessage(conn)),
					 node_name ? errnode(node_name) : 0));
		}
	}
	msg->len = save_len;
	list_conn = GetPGconnFromHandleList(state->cur_handle->handles);
	PQNFlush(list_conn, true);

	res = NULL;
	rdc_id = 0;
	PG_TRY();
	{
		/* start self reduce */
		if (context->have_reduce && context->start_self_reduce)
		{
			rdc_info[rdc_id].port = StartDynamicReduceWorker();
			rdc_info[rdc_id].pid = MyProcPid;
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
			case PGRES_COPY_IN:
				break;
			case PGRES_FATAL_ERROR:
				PQNReportResultError(res, conn, ERROR, false);
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
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
		safe_pfree(rdc_info);
		PG_RE_THROW();
	}PG_END_TRY();

	/* Start makeup reduce group */
	if (context->have_reduce)
	{
		GetRDCListenPortHook *hook = PQNMakeDefHookFunctions(sizeof(GetRDCListenPortHook));
		hook->pub.HookCopyOut = get_rdc_listen_port_hook;
		/* wait for listen port of other reduce */
		foreach(lc, list_conn)
		{
			conn = lfirst(lc);
			hook->rdc_info = &rdc_info[rdc_id];
			hook->rdc_info->pid = PQbackendPID(conn);
			PQNOneExecFinish(conn, &hook->pub, true);
			rdc_id++;
		}

		/* tell other reduce infomation about reduce group */
		StartRemoteReduceGroup(list_conn, rdc_info, rdc_id);
		if (context->start_self_reduce)
			DynamicReduceConnectNet(rdc_info, rdc_id);

		pfree(hook);
		safe_pfree(rdc_info);
	}

	error_context_stack = error_context_hook.previous;

	return list_conn;
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
		/* no need to do this, it will be done @AtEOXact_Reduce */
		//EndSelfReduce(0, 0);
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
	context->saved_node_id = PGXCNodeIdentifier;
	context->saved_enable_cluster_plan = enable_cluster_plan;
	context->saved_in_cluster_mode = in_cluster_mode;
	in_cluster_mode = true;
	context->callback.arg = context;
	context->callback.callback = ExecClusterErrorHookNode;
	context->callback.previous = error_context_stack;
	error_context_stack = &context->callback;
}

static void RestoreClusterHook(ClusterErrorHookContext *context)
{
	PGXCNodeOid = context->saved_node_oid;
	PGXCNodeIdentifier = context->saved_node_id;
	enable_cluster_plan = context->saved_enable_cluster_plan;
	error_context_stack = context->callback.previous;
	in_cluster_mode = context->saved_in_cluster_mode;
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

/******************** cluster stat ***********************/

#define CLUSTER_TAB_STAT_INSERTED			(1<<0)
#define CLSUTER_TAB_STAT_UPDATED			(1<<1)
#define CLSUTER_TAB_STAT_DELETED			(1<<2)
#define CLUSTER_TAB_STAT_INSERTED_PRE_TRUNC	(1<<3)
#define CLSUTER_TAB_STAT_UPDATED_PRE_TRUNC	(1<<4)
#define CLSUTER_TAB_STAT_DELETED_PRE_TRUNC	(1<<5)
#define CLUSTER_TAB_STAT_PRE_TRUNC			(CLUSTER_TAB_STAT_INSERTED_PRE_TRUNC | \
											 CLSUTER_TAB_STAT_UPDATED_PRE_TRUNC | \
											 CLSUTER_TAB_STAT_DELETED_PRE_TRUNC)
typedef uint8 cluster_tab_stat_mark_type;

typedef struct ClusterTabStatKey
{
	Oid			oid;					/* table's OID */
	int			nest_level;				/* subtransaction nest level */
}ClusterTabStatKey;

typedef struct ClusterTabStatValue
{
	PgStat_Counter tuples_inserted;		/* tuples inserted in (sub)xact */
	PgStat_Counter tuples_updated;		/* tuples updated in (sub)xact */
	PgStat_Counter tuples_deleted;		/* tuples deleted in (sub)xact */
	PgStat_Counter inserted_pre_trunc;	/* tuples inserted prior to truncate */
	PgStat_Counter updated_pre_trunc;	/* tuples updated prior to truncate */
	PgStat_Counter deleted_pre_trunc;	/* tuples deleted prior to truncate */
}ClusterTabStatValue;

typedef struct ClusterTabStatInfo
{
	ClusterTabStatKey	key;
	ClusterTabStatValue	value;
}ClusterTabStatInfo;

static HTAB	   *clusterStatSnapshot = NULL;

static bool count_table_pgstat(PgStat_TableStatus *status, long *nelem)
{
	PgStat_TableXactStatus *trans = status->trans;
	int count = 0;
	if (status->t_system ||	/* ignore system relation, include toast relation */
		trans == NULL)
		return false;

	/* get first trans */
	while (trans->upper)
		trans = trans->upper;

	while(trans)
	{
		++count;
		trans = trans->next;
	}

	*nelem += count;
	return false;
}

static void create_cluster_table_stat_htab(long nelem)
{
	HASHCTL		ctl;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(ClusterTabStatKey);
	ctl.entrysize = sizeof(ClusterTabStatInfo);
	ctl.hcxt = MessageContext;
	clusterStatSnapshot = hash_create("cluster table stat snapshot",
									  100,
									  &ctl,
									  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static void copy_table_pgstat(ClusterTabStatValue *val, const PgStat_TableXactStatus *trans)
{
	val->tuples_inserted = trans->tuples_inserted;
	val->tuples_updated = trans->tuples_updated;
	val->tuples_deleted = trans->tuples_deleted;
	if (trans->truncated)
	{
		val->inserted_pre_trunc = trans->inserted_pre_trunc;
		val->updated_pre_trunc = trans->updated_pre_trunc;
		val->deleted_pre_trunc = trans->deleted_pre_trunc;
	}else
	{
		val->inserted_pre_trunc =
			val->updated_pre_trunc =
			val->deleted_pre_trunc = 0;
	}
}

static bool save_table_pgstat(PgStat_TableStatus *status, void *context)
{
	PgStat_TableXactStatus *trans;
	ClusterTabStatKey key;
	ClusterTabStatInfo *info;
	key.oid = status->t_id;

	trans = status->trans;
	if (status->t_system || /* ignore system relation, include toast relation */
		trans == NULL)
		return false;

	/* get first trans */
	while(trans->upper)
		trans = trans->upper;

	while(trans)
	{
		key.nest_level = trans->nest_level;
		info = hash_search(clusterStatSnapshot, &key, HASH_ENTER, NULL);

		copy_table_pgstat(&info->value, trans);

		trans = trans->next;
	}

	return false;
}

static bool serialize_table_pgstate(PgStat_TableStatus *status, StringInfo buf)
{
	PgStat_TableXactStatus *trans;
	ClusterTabStatInfo *saved;
	ClusterTabStatInfo info;
	int saved_len;
	cluster_tab_stat_mark_type mark;

	trans = status->trans;
	if (status->t_system || /* ignore system relation, include toast relation */
		trans == NULL)
		return false;

	/* get first trans */
	while(trans->upper)
		trans = trans->upper;

	info.key.oid = status->t_id;
	while(trans)
	{
		info.key.nest_level = trans->nest_level;
		if (clusterStatSnapshot)
			saved = hash_search(clusterStatSnapshot, &info.key, HASH_FIND, NULL);
		else
			saved = NULL;

		copy_table_pgstat(&info.value, trans);
		if (saved)
		{
			info.value.tuples_inserted -= saved->value.tuples_inserted;
			info.value.tuples_updated -= saved->value.tuples_updated;
			info.value.tuples_deleted -= saved->value.tuples_deleted;
			info.value.inserted_pre_trunc -= saved->value.inserted_pre_trunc;
			info.value.updated_pre_trunc -= saved->value.updated_pre_trunc;
			info.value.deleted_pre_trunc -= saved->value.deleted_pre_trunc;
		}

		/* save last len for mark */
		saved_len = buf->len;
		mark = 0;
		appendBinaryStringInfo(buf, (char*)&mark, sizeof(mark));

#define SERIALIZE(field, macro)				\
		do{if (info.value.field)			\
		{									\
			mark |= macro;					\
			appendBinaryStringInfo(buf,		\
				(char*)&info.value.field,	\
				sizeof(info.value.field));	\
		}}while(0)

		SERIALIZE(tuples_inserted,		CLUSTER_TAB_STAT_INSERTED);
		SERIALIZE(tuples_updated,		CLSUTER_TAB_STAT_UPDATED);
		SERIALIZE(tuples_deleted,		CLSUTER_TAB_STAT_DELETED);
		SERIALIZE(inserted_pre_trunc,	CLUSTER_TAB_STAT_INSERTED_PRE_TRUNC);
		SERIALIZE(updated_pre_trunc,	CLSUTER_TAB_STAT_UPDATED_PRE_TRUNC);
		SERIALIZE(deleted_pre_trunc,	CLSUTER_TAB_STAT_DELETED_PRE_TRUNC);
#undef SERIALIZE

		if (mark == 0)
		{
			/* no stat saved, reset buffer */
			buf->len = saved_len;
		}else
		{
			/* rewrite mark */
			memcpy(buf->data + saved_len, (char*)&mark, sizeof(mark));
			/* save relation oid and nest_level */
			save_oid_class(buf, info.key.oid);
			appendBinaryStringInfo(buf, (char*)&info.key.nest_level, sizeof(info.key.nest_level));
		}

		trans = trans->next;
	}

	return false;
}

void ClusterRecvTableStat(const char *data, int length)
{
	Relation					rel = NULL;
	PgStat_TableStatus		   *status;
	PgStat_TableXactStatus	   *trans;
	ClusterTabStatInfo			info;
	StringInfoData				buf;
	cluster_tab_stat_mark_type	mark;

	buf.data = (char*)data;
	buf.cursor = 0;
	buf.len = buf.maxlen = length;

	while(buf.cursor < buf.len)
	{
		pq_copymsgbytes(&buf, (char*)&mark, sizeof(mark));
#define RESTORE(field, macro)							\
		do{if(mark & macro)								\
		{												\
			pq_copymsgbytes(&buf,						\
				(char*)&info.value.field,				\
				sizeof(info.value.field));				\
		}}while(0)
		MemSet(&info.value, 0, sizeof(info.value));
		RESTORE(tuples_inserted,	CLUSTER_TAB_STAT_INSERTED);
		RESTORE(tuples_updated,		CLSUTER_TAB_STAT_UPDATED);
		RESTORE(tuples_deleted,		CLSUTER_TAB_STAT_DELETED);
		RESTORE(inserted_pre_trunc,	CLUSTER_TAB_STAT_INSERTED_PRE_TRUNC);
		RESTORE(updated_pre_trunc,	CLSUTER_TAB_STAT_UPDATED_PRE_TRUNC);
		RESTORE(deleted_pre_trunc,	CLSUTER_TAB_STAT_DELETED_PRE_TRUNC);
#undef RESTORE

		info.key.oid = load_oid_class(&buf);
		pq_copymsgbytes(&buf, (char*)&info.key.nest_level, sizeof(info.key.nest_level));

		if (rel == NULL ||
			RelationGetRelid(rel) != info.key.oid)
		{
			if(rel)
				relation_close(rel, NoLock);
			rel = relation_open(info.key.oid, NoLock);
		}

		status = pgstat_get_status(rel);
		if (status && status->trans)
		{
			trans = status->trans;
			trans->tuples_inserted += info.value.tuples_inserted;
			trans->tuples_updated += info.value.tuples_updated;
			trans->tuples_deleted += info.value.tuples_deleted;
			if (mark & CLUSTER_TAB_STAT_PRE_TRUNC)
			{
				if (trans->truncated)
				{
					trans->inserted_pre_trunc += info.value.inserted_pre_trunc;
					trans->updated_pre_trunc += info.value.updated_pre_trunc;
					trans->deleted_pre_trunc += info.value.deleted_pre_trunc;
				}else
				{
					trans->inserted_pre_trunc = info.value.inserted_pre_trunc;
					trans->updated_pre_trunc = info.value.updated_pre_trunc;
					trans->deleted_pre_trunc = info.value.deleted_pre_trunc;
					trans->truncated = true;
				}
			}
		}
	}

	if (rel)
		relation_close(rel, NoLock);
}

void SaveTableStatSnapshot(void)
{
	long nelem = 0L;
	walker_table_stat(count_table_pgstat, &nelem);
	if (nelem == 0L)
	{
		clusterStatSnapshot = NULL;
	}else
	{
		create_cluster_table_stat_htab(nelem);
		Assert(clusterStatSnapshot != NULL);
		walker_table_stat(save_table_pgstat, NULL);
	}
}

bool SerializeTableStat(StringInfo buf)
{
	int saved_len1;
	int saved_len2 = buf->len;
	appendStringInfoChar(buf, CLUSTER_MSG_TABLE_STAT);
	saved_len1 = buf->len;
	walker_table_stat(serialize_table_pgstate, buf);
	if (buf->len == saved_len1)
	{
		/* not have any serialized, reset buffer */
		buf->len = saved_len2;
		return false;
	}

	return true;
}

void DestroyTableStateSnapshot(void)
{
	if (clusterStatSnapshot)
	{
		hash_destroy(clusterStatSnapshot);
		clusterStatSnapshot = NULL;
	}
}

static void set_cluster_display(const char *activity, bool force, ClusterCoordInfo *info)
{
	if (info == NULL)
	{
		set_ps_display(activity, force);
	}else
	{
		StringInfoData buf;
		initStringInfo(&buf);
		appendStringInfo(&buf, "%s for PID %d from %s", activity, info->pid, info->name);
		set_ps_display(buf.data, force);
		pfree(buf.data);
	}
}
