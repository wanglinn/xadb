#include "postgres.h"

#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/htup_details.h"
#include "access/relscan.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/adb_clean.h"
#include "catalog/indexing.h"
#include "catalog/pg_database.h"
#include "catalog/pg_operator_d.h"
#include "catalog/pg_type_d.h"
#include "catalog/pgxc_class.h"
#include "commands/copy.h"
#include "commands/dbcommands.h"
#include "executor/clusterReceiver.h"
#include "executor/execCluster.h"
#include "executor/executor.h"
#include "libpq-fe.h"
#include "libpq/libpq-node.h"
#include "libpq/pqformat.h"
#include "libpq/pqmq.h"
#include "lib/oidbuffer.h"
#include "lib/stringinfo.h"
#include "intercomm/inter-node.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/reduceinfo.h"
#include "optimizer/var.h"
#include "parser/parse_coerce.h"
#include "postmaster/bgworker.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "storage/bufmgr.h"
#include "storage/mem_toc.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/combocid.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/resowner.h"
#include "utils/resowner_private.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#define EXPANSION_QUEUE_SIZE	(16*1024)

#define EW_TOC_MAGIC				UINT64CONST(0xaf93442bbc367cfd)
#define EW_KEY_LIBRARY				UINT64CONST(0xFFFFFFFFFFFF0001)
#define EW_KEY_GUC					UINT64CONST(0xFFFFFFFFFFFF0002)
#define EW_KEY_TRANSACTION_SNAPSHOT	UINT64CONST(0xFFFFFFFFFFFF0003)
#define EW_KEY_ACTIVE_SNAPSHOT		UINT64CONST(0xFFFFFFFFFFFF0004)
#define EW_KEY_TRANSACTION_STATE	UINT64CONST(0xFFFFFFFFFFFF0005)
#define EW_KEY_COMBO_CID			UINT64CONST(0xFFFFFFFFFFFF0006)
#define EW_KEY_EXPANSION_LIST		UINT64CONST(0xFFFFFFFFFFFF0007)
#define EW_KEY_COMMAND				1
#define EW_KEY_CLASS_RELATION		2
#define EW_KEY_SQL					3
#define EW_KEY_DATABASE				4
#define EW_KEY_END_DATABASE			5

typedef struct ExpansionWorkerExtera
{
	Oid				dboid;
	int				encoding;
	TransactionId	xid;
}ExpansionWorkerExtera;

typedef struct ClusterExpansionContext
{
	dsm_segment			   *seg;
	shm_mq_handle		   *mq_sender;
	shm_mq_handle		   *mq_receiver;
	BackgroundWorkerHandle *handle;
	List				   *expansion;
}ClusterExpansionContext;

typedef struct ClusterCleanContext
{
	Relation	rel_clean;
	Snapshot	snapshot;
}ClusterCleanContext;

typedef struct ExpansionClean
{
	MemoryContext	mcontext;
	Expr		   *expr;
	ExprState	   *state;
	ExprContext	   *econtext;
	TupleTableSlot *slot;
	BlockNumber		max_block;
	bool			limit_insert;
}ExpansionClean;

static void CreateSHMQPipe(dsm_segment *seg, shm_mq_handle** mqh_sender, shm_mq_handle **mqh_receiver, bool is_worker)
{
	shm_mq			   *mq_sender;
	shm_mq			   *mq_receiver;
	char			   *addr = dsm_segment_address(seg);

	if (is_worker)
	{
		mq_receiver = (shm_mq*)(addr);
		mq_sender = (shm_mq*)(addr+EXPANSION_QUEUE_SIZE);
	}else
	{
		mq_sender = shm_mq_create(addr, EXPANSION_QUEUE_SIZE);
		mq_receiver = shm_mq_create(addr+EXPANSION_QUEUE_SIZE,
									EXPANSION_QUEUE_SIZE);
	}
	shm_mq_set_sender(mq_sender, MyProc);
	*mqh_sender = shm_mq_attach(mq_sender, seg, NULL);
	shm_mq_set_receiver(mq_receiver, MyProc);
	*mqh_receiver = shm_mq_attach(mq_receiver, seg, NULL);
}

static void CheckExistDatanode(List *list, Oid nodeoid, DefElem *def, ParseState *pstate)
{
	ListCell   *lc,*lc2;

	foreach (lc, list)
	{
		Assert(IsA(lfirst(lc), OidList));
		foreach (lc2, lfirst(lc))
		{
			if (lfirst_oid(lc2) == nodeoid)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("duplicate node specify \"%s\"", def->defname),
						 parser_errposition(pstate, def->location)));
			}
		}
	}
}

static inline void SerializeDatabaseName(StringInfo buf, const char *dbname)
{
	save_node_string(buf, dbname);
}

static inline Oid RestoreDatabaseOid(StringInfo buf)
{
	char	   *dbname = load_node_string(buf, false);
	return get_database_oid(dbname, false);
}

static List* MakeExpansionArg(AlterNodeStmt *stmt, ParseState *pstate)
{
	DefElem	   *def_from;
	DefElem	   *def_to;
	List	   *list_expan;
	ListCell   *lc,*lc2;
	Oid			oid;

	list_expan = NIL;
	foreach (lc, stmt->options)
	{
		def_from = lfirst_node(DefElem, lc);
		oid = get_pgxc_nodeoid(def_from->defname);
		if (!OidIsValid(oid))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("node \"%s\" not exist", def_from->defname),
					 parser_errposition(pstate, def_from->location)));
		CheckExistDatanode(list_expan, oid, def_from, pstate);
		list_expan = lappend(list_expan, list_make1_oid(oid));

		foreach(lc2, castNode(List, def_from->arg))
		{
			def_to = lfirst_node(DefElem, lc2);
			oid = get_pgxc_nodeoid(def_to->defname);
			if (!OidIsValid(oid))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("node \"%s\" not exist", def_to->defname),
						 parser_errposition(pstate, def_to->location)));
			CheckExistDatanode(list_expan, oid, def_to, pstate);
			lappend_oid(llast(list_expan), oid);
		}
	}

	return list_expan;
}

static List* ConnectAlterNodes(AlterNodeStmt *stmt, List *expansion, const char *sql)
{
	List		   *oids = GetAllCnIDL(false);
	List		   *remote_list;
	ListCell	   *lc,*lc2;
	StringInfoData	msg;

	foreach (lc, expansion)
	{
		foreach (lc2, lfirst(lc))
			oids = lappend_oid(oids, lfirst_oid(lc2));
	}

	initStringInfo(&msg);
	ClusterTocSetCustomFun(&msg, ClusterExpansion);
	begin_mem_toc_insert(&msg, EW_KEY_COMMAND);
	saveNode(&msg, (Node*)stmt);
	end_mem_toc_insert(&msg, EW_KEY_COMMAND);

	begin_mem_toc_insert(&msg, EW_KEY_SQL);
	save_node_string(&msg, sql);
	end_mem_toc_insert(&msg, EW_KEY_SQL);

	remote_list = ExecClusterCustomFunction(oids, &msg, 0);
	list_free(oids);
	pfree(msg.data);

	return remote_list;
}

static BackgroundWorkerHandle* StartExpansionWorker(dsm_segment *dsm_seg, shm_mq_handle** mqh_sender, shm_mq_handle **mqh_receiver,
													List *expansion_node, Oid dboid)
{
	BackgroundWorker		bg;
	BackgroundWorkerHandle *handle;
	ExpansionWorkerExtera  *extra;
	char				   *ptr;
	Size					library_len = 0;
	Size					guc_len = 0;
	Size					tsnaplen = 0;
	Size					asnaplen = 0;
	Size					tstatelen = 0;
	Size					combocidlen = 0;
	Size					segsize = 0;
	Snapshot				transaction_snapshot = GetTransactionSnapshot();
	Snapshot				active_snapshot = GetActiveSnapshot();
	shm_toc_estimator		estimator;
	shm_toc				   *toc;
	StringInfoData			arg;

	CreateSHMQPipe(dsm_seg, mqh_sender, mqh_receiver, false);

	bg.bgw_flags = BGWORKER_SHMEM_ACCESS|BGWORKER_BACKEND_DATABASE_CONNECTION;
	bg.bgw_start_time = BgWorkerStart_ConsistentState;
	bg.bgw_restart_time = BGW_NEVER_RESTART;
	strcpy(bg.bgw_library_name, "postgres");
	strcpy(bg.bgw_function_name, "ExpansionWorkerMain");
	strcpy(bg.bgw_type, "expansion worker");
	snprintf(bg.bgw_name, BGW_MAXLEN, "expansion worker for PID %d", MyProcPid);
	bg.bgw_notify_pid = MyProcPid;

	bg.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(dsm_seg));
	extra = (ExpansionWorkerExtera*)bg.bgw_extra;
	extra->dboid = dboid;
	extra->encoding = pg_get_client_encoding();
	extra->xid = GetCurrentTransactionId();

	if (!RegisterDynamicBackgroundWorker(&bg, &handle))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not register background process"),
				 errhint("You may need to increase max_worker_processes.")));
	}
	shm_mq_set_handle(*mqh_sender, handle);
	shm_mq_set_handle(*mqh_receiver, handle);

	shm_toc_initialize_estimator(&estimator);

	if (expansion_node == NIL)
	{
		Assert(IsDnNode());
		arg.data = NULL;
		arg.len = 0;
	}else
	{
		Assert(IsCnNode());
		initStringInfo(&arg);
		saveNode(&arg, (Node*)expansion_node);
	}

	/* Estimate space for various kinds of state sharing. */
	library_len = EstimateLibraryStateSpace();
	shm_toc_estimate_chunk(&estimator, library_len);
	guc_len = EstimateGUCStateSpace();
	shm_toc_estimate_chunk(&estimator, guc_len);
	tsnaplen = EstimateSnapshotSpace(transaction_snapshot);
	shm_toc_estimate_chunk(&estimator, tsnaplen);
	asnaplen = EstimateSnapshotSpace(active_snapshot);
	shm_toc_estimate_chunk(&estimator, asnaplen);
	tstatelen = EstimateTransactionStateSpace();
	shm_toc_estimate_chunk(&estimator, tstatelen);
	combocidlen = EstimateComboCIDStateSpace();
	shm_toc_estimate_chunk(&estimator, combocidlen);
	/* If you add more chunks here, you probably need to add keys. */
	shm_toc_estimate_keys(&estimator, 6);

	if (expansion_node != NIL)
	{
		shm_toc_estimate_chunk(&estimator, arg.len+sizeof(Size));
		shm_toc_estimate_keys(&estimator, 1);
	}

	segsize = shm_toc_estimate(&estimator);
	toc = shm_toc_create(EW_TOC_MAGIC, palloc(segsize), segsize);

	/* Serialize shared libraries we have loaded. */
	ptr = shm_toc_allocate(toc, library_len);
	SerializeLibraryState(library_len, ptr);
	shm_toc_insert(toc, EW_KEY_LIBRARY, ptr);

	/* Serialize GUC settings. */
	ptr = shm_toc_allocate(toc, guc_len);
	SerializeGUCState(guc_len, ptr);
	shm_toc_insert(toc, EW_KEY_GUC, ptr);

	/* Serialize transaction snapshot and active snapshot. */
	ptr = shm_toc_allocate(toc, tsnaplen);
	SerializeSnapshot(transaction_snapshot, ptr);
	shm_toc_insert(toc, EW_KEY_TRANSACTION_SNAPSHOT, ptr);
	ptr = shm_toc_allocate(toc, asnaplen);
	SerializeSnapshot(active_snapshot, ptr);
	shm_toc_insert(toc, EW_KEY_ACTIVE_SNAPSHOT, ptr);

	/* Serialize transaction state. */
	ptr = shm_toc_allocate(toc, tstatelen);
	SerializeTransactionState(tstatelen, ptr);
	shm_toc_insert(toc, EW_KEY_TRANSACTION_STATE, ptr);

	ptr = shm_toc_allocate(toc, combocidlen);
	SerializeComboCIDState(combocidlen, ptr);
	shm_toc_insert(toc, EW_KEY_COMBO_CID, ptr);

	if (expansion_node != NIL)
	{
		ptr = shm_toc_allocate(toc, arg.len+sizeof(Size));
		*(Size*)ptr = arg.len;
		memcpy(ptr+sizeof(Size), arg.data, arg.len);
		shm_toc_insert(toc, EW_KEY_EXPANSION_LIST, ptr);
	}

	if (shm_mq_send(*mqh_sender, segsize, toc, false) != SHM_MQ_SUCCESS)
		ereport(ERROR,
				(errmsg("send startup message to expansion worker result detached")));
	pfree(toc);
	if (arg.data)
		pfree(arg.data);

	return handle;
}

/* Parse ErrorResponse or NoticeResponse. */
static void ProcessExpansionWorkerNotice(StringInfo msg)
{
	ErrorData	edata;
	pq_parse_errornotice(msg, &edata);

	/* Death of a worker isn't enough justification for suicide. */
	edata.elevel = Min(edata.elevel, ERROR);

	if (edata.context)
		edata.context = psprintf("%s\n%s", edata.context, _("expansion worker"));
	else
		edata.context = pstrdup(_("expansion worker"));

	ThrowErrorData(&edata);
}

static void LoopExpansionWorkerMessage(shm_mq_handle *mq)
{
	PGconn		   *conn;
	List		   *list;
	Size			size;
	StringInfoData	msg;
	Oid				oid;
	char			msgtype;

	for (;;)
	{
		if (shm_mq_receive(mq, &size, (void**)&msg.data, false) != SHM_MQ_SUCCESS)
			ereport(ERROR,
					(errmsg("receive message from expansion worker got MQ detached")));
		msg.maxlen = msg.len = (int)size;
		msg.cursor = 0;

		msgtype = pq_getmsgbyte(&msg);
		switch (msgtype)
		{
		case 'E':	/* ErrorResponse */
		case 'N':	/* NoticeResponse */
			ProcessExpansionWorkerNotice(&msg);
			break;
		case 'c':
			return;
		case EW_KEY_CLASS_RELATION:
			{
				pq_copymsgbytes(&msg, (char*)&oid, sizeof(Oid));
				Assert(msg.data[msg.cursor] == EW_KEY_CLASS_RELATION);	/* send to datanode */
				conn = PQNFindConnUseOid(oid);
				if (conn == NULL)
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Connection for node %u not connected", oid)));
				if (!PQisCopyOutState(conn) || !PQisCopyInState(conn))
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Connection for node %u is not in copy both mode", oid)));
				list = list_make1(conn);
				PQNputCopyData(list, msg.data + msg.cursor, msg.len-msg.cursor);
				list_free(list);
			}
			break;
		default:
			break;
		}
	}
}

void AlterNodeExpansionWork(AlterNodeStmt *stmt, ParseState *pstate)
{
	dsm_segment	   *dsm_seg = dsm_create(EXPANSION_QUEUE_SIZE*2, 0);
	Relation		rel = relation_open(DatabaseRelationId, LW_SHARED);
	HeapScanDesc	scan = heap_beginscan_catalog(rel, 0, NULL);
	Relation		rel_clean = relation_open(AdbCleanRelationId, AccessShareLock);
	HeapScanDesc	scan_clean = heap_beginscan_catalog(rel_clean, 0, NULL);
	HeapTuple		tuple;
	shm_mq_handle  *mqh_sender;
	shm_mq_handle  *mqh_receiver;
	List		   *expansion_list;
	List		   *remote_list;
	BackgroundWorkerHandle
				   *handle;
	ListCell	   *lc;
	StringInfoData	msg;

	/* Confirm that the adb_clean table is empty. */
	if ((tuple = heap_getnext(scan_clean, ForwardScanDirection)) != NULL)
	{
		heap_endscan(scan_clean);
		relation_close(rel_clean, AccessShareLock);
		ereport(ERROR, 
				(errmsg("The capacity expansion work is not allowed to expand again when the cleaning task is not completed.")));
	}
	heap_endscan(scan_clean);
	relation_close(rel_clean, AccessShareLock);
	
	initStringInfo(&msg);
	expansion_list = MakeExpansionArg(stmt, pstate);
	remote_list = ConnectAlterNodes(stmt, expansion_list, pstate->p_sourcetext);
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Form_pg_database form = (Form_pg_database) GETSTRUCT(tuple);
		if (form->datistemplate ||
			form->datallowconn == false)
			continue;

		resetStringInfo(&msg);
		appendStringInfoChar(&msg, EW_KEY_DATABASE);
		SerializeDatabaseName(&msg, NameStr(form->datname));
		PQNputCopyData(remote_list, msg.data, msg.len);
		handle = StartExpansionWorker(dsm_seg, &mqh_sender, &mqh_receiver, expansion_list, HeapTupleGetOid(tuple));

		LoopExpansionWorkerMessage(mqh_receiver);
		resetStringInfo(&msg);
		appendStringInfoChar(&msg, EW_KEY_END_DATABASE);
		PQNputCopyData(remote_list, msg.data, msg.len);

		WaitForBackgroundWorkerShutdown(handle);
		pfree(handle);
		shm_mq_detach(mqh_sender);
		shm_mq_detach(mqh_receiver);
		GetCurrentCommandId(true);

		foreach (lc, remote_list)
			wait_executor_end_msg(lfirst(lc));
	}
	heap_endscan(scan);
	relation_close(rel, LW_SHARED);
	dsm_detach(dsm_seg);
	pfree(msg.data);
	list_free(expansion_list);
	PQNPutCopyEnd(remote_list);
	PQNListExecFinish(remote_list, NULL, &PQNDefaultHookFunctions, true);
	list_free(remote_list);

	CacheInvalidateRelcacheAll();
	InvalidateSystemCaches();
}

static List* RestoreWorkerInfo(shm_mq_handle *mq)
{
	Size			size;
	char		   *ptr,*toc_mem;
	shm_toc		   *toc;
	List		   *expansion_node;
	StringInfoData	arg;

	if (shm_mq_receive(mq, &size, (void**)&ptr, false) != SHM_MQ_SUCCESS)
	{
		ereport(ERROR,
				(errmsg("expansion worker receive startup message result MQ detached")));
	}
	toc_mem = MemoryContextAlloc(TopMemoryContext, size);
	memcpy(toc_mem, ptr, size);
	toc = shm_toc_attach(EW_TOC_MAGIC, toc_mem);
	if (toc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("invalid magic number in expansion startup message")));

	/*
	 * Load libraries that were loaded by original backend.  We want to do
	 * this before restoring GUCs, because the libraries might define custom
	 * variables.
	 */
	StartTransactionCommand();
	RestoreLibraryState(shm_toc_lookup(toc, EW_KEY_LIBRARY, false));

	/* Restore GUC values from launching backend. */
	RestoreGUCState(shm_toc_lookup(toc, EW_KEY_GUC, false));
	CommitTransactionCommand();

	StartParallelWorkerTransaction(shm_toc_lookup(toc, EW_KEY_TRANSACTION_STATE, false));

	/* Restore combo CID state. */
	RestoreComboCIDState(shm_toc_lookup(toc, EW_KEY_COMBO_CID, false));

	/* Restore transaction snapshot. */
	ptr = shm_toc_lookup(toc, EW_KEY_TRANSACTION_SNAPSHOT, false);
	RestoreTransactionSnapshot(RestoreSnapshot(ptr),
							   shm_mq_get_sender(shm_mq_get_queue(mq)));

	/* Restore active snapshot. */
	PushActiveSnapshot(RestoreSnapshot(shm_toc_lookup(toc, EW_KEY_ACTIVE_SNAPSHOT, false)));

	if (IS_PGXC_DATANODE)
	{
		expansion_node = NIL;
	}else
	{
		ptr = shm_toc_lookup(toc, EW_KEY_EXPANSION_LIST, false);
		arg.len = arg.maxlen = (int)(*(Size*)ptr);
		arg.data = ptr + sizeof(Size);
		arg.cursor = 0;
		expansion_node = (List*)loadNode(&arg);
		Assert(IsA(expansion_node, List));
	}

	/*
	 * We've changed which tuples we can see, and must therefore invalidate
	 * system caches.
	 */
	InvalidateSystemCaches();

	return expansion_node;
}

static void combin_nodeoid(OidBuffer boids, const oidvector *voids, List *expansion_list)
{
	ListCell *lc,*lc2;

	appendOidBufferArray(boids, voids->values, voids->dim1);
	foreach (lc, expansion_list)
	{
		Assert(IsA(lfirst(lc), OidList));
		foreach(lc2, lfirst(lc))
			appendOidBufferUniqueOid(boids, lfirst_oid(lc2));
	}
}

static int cmp_nodeoid_ptr(const void *a, const void *b)
{
	return cmp_pgxc_nodename(*(Oid*)a, *(Oid*)b);
}

static List* GetExpansionList(List *list, Oid oid)
{
	ListCell *lc;
	foreach (lc, list)
	{
		if (list_member_oid(lfirst(lc), oid))
			return lfirst(lc);
	}

	return NIL;
}

static HeapTuple UpdateClassNodeoidsValues(Oid *oids, uint32 count, List *values, HeapTuple tup,
									  Relation rel_class, CatalogIndexState indstate)
{
	HeapTuple		new_tup;
	char		   *str;
	text		   *txt;
	Datum			datums[Natts_pgxc_class];
	bool			nulls[Natts_pgxc_class];
	bool			reps[Natts_pgxc_class];

	MemSet(reps, false, sizeof(reps));

	datums[Anum_pgxc_class_nodeoids-1] = PointerGetDatum(buildoidvector(oids, count));
	nulls[Anum_pgxc_class_nodeoids-1] = false;
	reps[Anum_pgxc_class_nodeoids-1] = true;

	if (values != NIL)
	{
		str = nodeToString(values);
		txt = cstring_to_text(str);
		datums[Anum_pgxc_class_pcvalues-1] = PointerGetDatum(txt);
		nulls[Anum_pgxc_class_pcvalues-1] = false;
	}else
	{
		nulls[Anum_pgxc_class_pcvalues-1] = true;
	}
	reps[Anum_pgxc_class_pcvalues-1] = true;

	new_tup = heap_modify_tuple(tup, RelationGetDescr(rel_class), datums, nulls, reps);
	CatalogTupleUpdateWithInfo(rel_class, &tup->t_self, new_tup, indstate);

	if (values)
	{
		pfree(txt);
		pfree(str);
	}
	pfree(DatumGetPointer(datums[Anum_pgxc_class_nodeoids-1]));

	return new_tup;
}

static void InsertAdbClean(Oid relid, BlockNumber max_block, Expr *expr,
						   Relation rel_clean, CatalogIndexState state)
{
	HeapTuple	tup;
	char	   *str;
	text	   *txt;
	Datum		datum[Natts_adb_clean];
	bool		nulls[Natts_adb_clean];

	memset(datum, 0, sizeof(datum));
	memset(nulls, false, sizeof(nulls));

	datum[Anum_adb_clean_clndb - 1] = ObjectIdGetDatum(MyDatabaseId);
	datum[Anum_adb_clean_clnrel - 1] = ObjectIdGetDatum(relid);
	datum[Anum_adb_clean_clnblocks - 1] = Int32GetDatum(max_block);
	str = nodeToString(expr);
	txt = cstring_to_text(str);
	datum[Anum_adb_clean_clnexpr - 1] = PointerGetDatum(txt);

	tup = heap_form_tuple(RelationGetDescr(rel_clean), datum, nulls);
	CatalogTupleInsertWithInfo(rel_clean, tup, state);
	heap_freetuple(tup);
	pfree(txt);
	pfree(str);
}

static void InsertGTMClean(Oid relid, List *expansion,
						   Relation rel_clean, CatalogIndexState state)
{
	ListCell   *lc,*lc2;
	List	   *data_nodes = NIL;

	foreach (lc, expansion)
	{
		foreach(lc2, lfirst(lc))
			data_nodes = lappend_oid(data_nodes, lfirst_oid(lc2));
	}

	InsertAdbClean(relid, 0, (Expr*)data_nodes, rel_clean, state);
	list_free(data_nodes);
}

static CoalesceExpr* makeCoalesceBool(Expr *expr, bool b)
{
	CoalesceExpr *coalesce = makeNode(CoalesceExpr);
	Const *c = makeConst(BOOLOID, -1, InvalidOid, sizeof(bool), BoolGetDatum(b), false, true);
	Assert(exprType((Node*)expr) == BOOLOID);
	coalesce->coalescetype = BOOLOID;
	coalesce->coalescecollid = InvalidOid;
	coalesce->args = list_make2(expr, c);
	coalesce->location = -1;

	return coalesce;
}

static void ExpansionReplicated(Form_pgxc_class form_class, HeapTuple tup, List *expansion,
								Relation rel_class, CatalogIndexState indstate)
{
	OidBufferData	oids;
	HeapTuple		new_tup;

	initOidBuffer(&oids);
	combin_nodeoid(&oids, &form_class->nodeoids, expansion);
	qsort(oids.oids, oids.len, sizeof(Oid), cmp_nodeoid_ptr);

	new_tup = UpdateClassNodeoidsValues(oids.oids, oids.len, NIL, tup, rel_class, indstate);
	heap_freetuple(new_tup);
}

static List* GetHashNodesValues(oidvector *oids, HeapTuple tup, TupleDesc desc)
{
	List   *result = NIL;
	Datum	datum;
	int		i;
	bool	isnull;

	datum = fastgetattr(tup, Anum_pgxc_class_pcvalues, desc, &isnull);
	if (isnull)
	{
		for (i=0;i<oids->dim1;++i)
			result = lappend(result, list_make1_int(i));
	}else
	{
		result = stringToNode(TextDatumGetCString(datum));
	}

	return result;
}

static uint32 GetOldModulus(List *old_values, oidvector *old_nodeoids, Oid nodeoid)
{
	List	   *list;
	int			n;

	Assert(list_length(old_values) == old_nodeoids->dim1);
	n = old_nodeoids->dim1;
	while (n>0)
	{
		if (old_nodeoids->values[--n] == nodeoid)
		{
			list = list_nth(old_values, n);
			Assert(IsA(list, IntList));
			return list_length(list);
		}
	}

	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("can not found old node info for %u", nodeoid)));
	return 0;	/* never run, keep compiler quiet */
}

static uint32 GetBestMultiple(List *old_values, oidvector *old_nodeoids, List *expansion)
{
	ListCell   *lc;
	List	   *list;
	List	   *multiple = NIL;
	uint32		new_modulus;
	uint32		old_modulus;
	uint32		least_common;
	uint32		max,min;

	foreach (lc, expansion)
	{
		list = lfirst(lc);
		Assert(IsA(list, OidList));
		new_modulus = list_length(list);
		old_modulus = GetOldModulus(old_values, old_nodeoids, linitial_oid(list));
		if (new_modulus >= old_modulus)
		{
			max = new_modulus;
			min = old_modulus;
		}else
		{
			max = old_modulus;
			min = new_modulus;
		}
		least_common = max;
		while (least_common % min != 0)
			least_common += max;
		Assert(least_common % old_modulus == 0);
		multiple = list_append_unique_int(multiple, least_common/old_modulus);
	}

	/* find max value */
	lc = list_head(multiple);
	least_common = lfirst_int(lc);
	for_each_cell (lc, lnext(lc))
	{
		if (least_common < lfirst_int(lc))
			least_common = lfirst_int(lc);
	}
	max = least_common;
	multiple = list_delete_int(multiple, least_common);

re_loop_:
	foreach (lc, multiple)
	{
		if (least_common % lfirst_int(lc) != 0)
		{
			least_common += max;
			goto re_loop_;
		}
	}

	list_free(multiple);
	return least_common;
}

static void ReplaceHashExpansionNode(Oid *oids, uint32 count, List *expansion)
{
	ListCell	   *lc;
	Bitmapset	   *bms;
	Oid				oid;
	uint32			n;
	Assert(count > 0 && list_length(expansion) > 0);

	bms = NULL;
	oid = linitial_oid(expansion);
	for (n=count;n>0;)
	{
		--n;
		if (oids[n] == oid)
			bms = bms_add_member(bms, n);
	}
	Assert(bms_membership(bms) == BMS_MULTIPLE);
	Assert(bms_num_members(bms) % list_length(expansion) == 0);

	while (bms_is_empty(bms) == false)
	{
		/* skip keep node */
		n = bms_first_member(bms);
		lc = list_head(expansion);

		/* replace */
		while ((lc=lnext(lc)) != NULL)
		{
			n = bms_first_member(bms);
			oids[n] = lfirst_oid(lc);
		}
	}
	bms_free(bms);
}

static HeapTuple ExpansionHashUpdate(Form_pgxc_class form_class, HeapTuple tup, List *expansion,
								Relation rel_class, CatalogIndexState indstate, List **new_values)
{
	ListCell	   *lc;
	List		   *old_values;
	Oid			   *new_oid_remainder;
	Oid			   *new_oids;
	uint32			old_modulus;
	uint32			new_modulus;
	uint32			n;

	old_values = GetHashNodesValues(&form_class->nodeoids, tup, RelationGetDescr(rel_class));
	Assert(list_length(old_values) == form_class->nodeoids.dim1);

	/* get old modulu */
	old_modulus = 0;
	foreach (lc, old_values)
	{
		Assert(IsA(lfirst(lc), IntList));
		old_modulus += list_length(lfirst(lc));
	}

	/* get best new modulus */
	new_modulus = GetBestMultiple(old_values, &form_class->nodeoids, expansion) * old_modulus;

	/* expansion old modulus to new modulus */
	new_oid_remainder = palloc(sizeof(Oid)*new_modulus);
	n = 0;
	foreach (lc, old_values)
	{
		ListCell *lc2;
		foreach(lc2, lfirst(lc))
			new_oid_remainder[lfirst_int(lc2)] = form_class->nodeoids.values[n];
		++n;
	}
	for (n=old_modulus;n<new_modulus;n+=old_modulus)
		memmove(&new_oid_remainder[n], new_oid_remainder, sizeof(Oid)*old_modulus);

	/* replace each node to new nodes */
	foreach (lc, expansion)
		ReplaceHashExpansionNode(new_oid_remainder, new_modulus, lfirst(lc));

	/* update pgxc_class */
	n = MakeHashNodesAndValues(new_oid_remainder, new_modulus, &new_oids, new_values);
	return UpdateClassNodeoidsValues(new_oids, n, *new_values, tup, rel_class, indstate);
}

static ScalarArrayOpExpr* makeExprInAnyInt(Expr *expr, List *list)
{
	ListCell		   *lc;
	Datum			   *datums;
	ArrayType		   *arr;
	ScalarArrayOpExpr  *sao;
	Const			   *c;
	uint32				i;

	Assert(exprType((Node*)expr) == INT4OID);
	Assert(IsA(list, IntList));

	datums = palloc(sizeof(Datum)*list_length(list));
	i = 0;
	foreach (lc, list)
		datums[i++] = Int32GetDatum(lfirst_int(lc));
	arr = construct_array(datums, list_length(list), INT4OID, sizeof(int32), true, 'i');
	pfree(datums);

	c = makeConst(INT4ARRAYOID, -1, InvalidOid, -1, PointerGetDatum(arr), false, false);
	sao = makeNode(ScalarArrayOpExpr);
	sao->opno = Int4EqualOperator;
	sao->opfuncid = F_INT4EQ;
	sao->useOr = true;
	sao->inputcollid = InvalidOid;
	sao->args = list_make2(expr, c);
	sao->location = -1;

	return sao;
}

static OpExpr* makeInt4Equal(Expr *l, int32 n)
{
	Const *c = makeConst(INT4OID, -1, InvalidOid, sizeof(int32), Int32GetDatum(n), false, true);
	OpExpr *op = makeNode(OpExpr);
	Assert(exprType((Node*)l) == INT4OID);
	op->opno = Int4EqualOperator;
	op->opfuncid = F_INT4EQ;
	op->opresulttype = BOOLOID;
	op->opretset = false;
	op->opcollid = InvalidOid;
	op->inputcollid = InvalidOid;
	op->args = list_make2(l, c);
	op->location = -1;
	return op;
}

static void SendCleanExprMsg(shm_mq_handle *mq, StringInfo buf, Expr *clean_expr, Oid reloid, Oid dboid)
{
	Assert(OidIsValid(reloid) && OidIsValid(dboid));
	Assert(clean_expr != NULL);

	resetStringInfo(buf);
	appendStringInfoChar(buf, EW_KEY_CLASS_RELATION);
	appendBinaryStringInfoNT(buf, (char*)&dboid, sizeof(Oid));
	appendStringInfoChar(buf, EW_KEY_CLASS_RELATION);
	save_oid_class(buf, reloid);

	saveNode(buf, (Node*)clean_expr);
	if (shm_mq_send(mq, buf->len, buf->data, false) != SHM_MQ_SUCCESS)
		ereport(ERROR, (errmsg("send clean message to main worker result detached")));
}

static void ExpansionHashMakeClean(Form_pgxc_class new_class, List *new_values, List *expansion, shm_mq_handle *mq)
{
	Relation		rel;
	ListCell	   *lc,*lc2;
	Expr		   *mod_expr;
	Expr		   *clean_expr;
	StringInfoData	msg;
	uint32			modulus;
	uint32			i;
	Oid				oid;
	bool			null_res;

	rel = relation_open(new_class->pcrelid, NoLock);
	initStringInfo(&msg);

	if (new_values == NIL)
	{
		modulus = new_class->nodeoids.dim1;
	}else
	{
		modulus = 0;
		foreach (lc, new_values)
			modulus += list_length(lfirst(lc));
	}
	mod_expr = CreateReduceModuloExpr(rel, rel->rd_locator_info, modulus, 1);

	foreach (lc, expansion)
	{
		foreach (lc2, lfirst(lc))
		{
			oid = lfirst_oid(lc2);

			clean_expr = NULL;
			for (i=0;i<new_class->nodeoids.dim1;++i)
			{
				if (new_class->nodeoids.values[i] != oid)
					continue;

				if (new_values)
				{
					List *list = list_nth(new_values, i);
					clean_expr = (Expr*)makeExprInAnyInt(mod_expr, list);
					null_res = list_member_int(list, 0);
				}else
				{
					clean_expr = (Expr*)makeInt4Equal(mod_expr, i);
					null_res = (i == 0);
				}
				clean_expr = (Expr*)makeCoalesceBool(clean_expr, null_res);
				break;
			}
			Assert(i<new_class->nodeoids.dim1);
			Assert(clean_expr != NULL);
			SendCleanExprMsg(mq, &msg, clean_expr, new_class->pcrelid, oid);
		}
	}

	relation_close(rel, NoLock);
}

static void ExpansionHash(Form_pgxc_class form_class, HeapTuple tup, List *expansion,
						  Relation rel_class, CatalogIndexState indstate, shm_mq_handle *mq)
{
	HeapTuple	new_tup;
	List	   *new_values;

	new_tup = ExpansionHashUpdate(form_class, tup, expansion, rel_class, indstate, &new_values);
	if (IsGTMNode())
		ExpansionHashMakeClean((Form_pgxc_class)GETSTRUCT(new_tup), new_values, expansion, mq);
	heap_freetuple(new_tup);
}

static void ExpansionRandom(Form_pgxc_class form_class, HeapTuple tup, List *expansion,
							Relation rel_class, CatalogIndexState indstate, shm_mq_handle *mq)
{
	Expr		   *clean_expr;
	Const		   *modulo_expr;
	Const		   *equal_value;
	ListCell	   *lc,*lc2;
	List		   *list;
	StringInfoData	msg;
	int				i;

	ExpansionReplicated(form_class, tup, expansion, rel_class, indstate);
	if (!IsGTMNode())
		return;

	/* hashtext(ctid::text) */
	clean_expr = (Expr*)makeVar(1, SelfItemPointerAttributeNumber, TIDOID, -1, InvalidOid, 0);
	clean_expr = (Expr*)coerce_to_target_type(NULL,
											  (Node*)clean_expr,
											  TIDOID,
											  TEXTOID,
											  -1,
											  COERCION_EXPLICIT,
											  COERCE_EXPLICIT_CAST,
											  -1);
	clean_expr = (Expr*)makeFuncExpr(F_HASHTEXT,
									 INT4OID,
									 list_make1(clean_expr),
									 InvalidOid,
									 InvalidOid,
									 COERCE_EXPLICIT_CALL);

	/* modulo value, for now set 0, change when using */
	modulo_expr = makeConst(INT4OID, -1, InvalidOid, sizeof(int32), Int32GetDatum(0), false, true);

	/* hash_combin_mod(0, hash(ctid::text)) */
	clean_expr = (Expr*)makeFuncExpr(F_HASH_COMBIN_MOD,
									 INT4OID,
									 list_make2(modulo_expr, clean_expr),
									 InvalidOid,
									 InvalidOid,
									 COERCE_EXPLICIT_CALL);

	/* hash_combin_mod(0, hash(ctid::text)) = 0 */
	clean_expr = (Expr*)makeInt4Equal(clean_expr, 0);
	equal_value = castNode(Const, llast(castNode(OpExpr, clean_expr)->args));

	initStringInfo(&msg);
	foreach (lc, expansion)
	{
		list = lfirst(lc);
		Assert(IsA(list, OidList));
		modulo_expr->constvalue = Int32GetDatum(list_length(list));
		i = 0;
		foreach (lc2, list)
		{
			resetStringInfo(&msg);
			equal_value->constvalue = Int32GetDatum(i);
			++i;
			SendCleanExprMsg(mq, &msg, clean_expr, form_class->pcrelid, lfirst_oid(lc2));
		}
	}
}

static void ExpansionList(Form_pgxc_class form_class, HeapTuple tup, List *expansion, shm_mq_handle *mq)
{

}

static void ExpansionRange(Form_pgxc_class form_class, HeapTuple tup, List *expansion, shm_mq_handle *mq)
{

}

static void ExpansionHashmap(Form_pgxc_class form_class, HeapTuple tup, List *expansion, shm_mq_handle *mq)
{

}

static void ExpansionWorkerCoord(List *expansion_node, shm_mq_handle *mq, MemoryContext loop_context)
{
	Relation			rel_class;
	Relation			rel_clean;
	CatalogIndexState	class_index_state;
	CatalogIndexState	clean_index_state;
	Form_pgxc_class		form_class;
	HeapScanDesc		scan;
	MemoryContext		main_context;
	List			   *expansion_rel_node;
	HeapTuple			tup;
	int					i;

	main_context = CurrentMemoryContext;

	if (IsGTMCnNode())
	{
		rel_clean = heap_open(AdbCleanRelationId, RowExclusiveLock);
		clean_index_state = CatalogOpenIndexes(rel_clean);
	}else
	{
		rel_clean = NULL;
		clean_index_state = NULL;
	}
	rel_class = heap_open(PgxcClassRelationId, RowExclusiveLock);
	class_index_state = CatalogOpenIndexes(rel_class);
	scan = heap_beginscan_catalog(rel_class, 0, NULL);
	while ((tup=heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		MemoryContextSwitchTo(loop_context);
		MemoryContextResetAndDeleteChildren(loop_context);

		form_class = (Form_pgxc_class) GETSTRUCT(tup);
		if (!SearchSysCacheExists1(RELOID, form_class->pcrelid))
		{
			/* should not happen */
			ereport(WARNING,
					(errmsg("relation %u not found in pg_class", form_class->pcrelid)));
			continue;
		}
		
		expansion_rel_node = NIL;
		for (i=0;i<form_class->nodeoids.dim1;++i)
		{
			List *list = GetExpansionList(expansion_node, form_class->nodeoids.values[i]);
			if (list != NIL)
				expansion_rel_node = lappend(expansion_rel_node, list);
		}
		if (expansion_rel_node == NIL)
			continue;

		switch(form_class->pclocatortype)
		{
		case LOCATOR_TYPE_REPLICATED:
			ExpansionReplicated(form_class, tup, expansion_rel_node, rel_class, class_index_state);
			break;
		case LOCATOR_TYPE_HASH:
			ExpansionHash(form_class, tup, expansion_rel_node, rel_class, class_index_state, mq);
			break;
		case LOCATOR_TYPE_LIST:
			ExpansionList(form_class, tup, expansion_rel_node, mq);
			break;
		case LOCATOR_TYPE_RANGE:
			ExpansionRange(form_class, tup, expansion_rel_node, mq);
			break;
		case LOCATOR_TYPE_RANDOM:
			ExpansionRandom(form_class, tup, expansion_rel_node, rel_class, class_index_state, mq);
			break;
		case LOCATOR_TYPE_HASHMAP:
			ExpansionHashmap(form_class, tup, expansion_rel_node, mq);
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("unknow locator type %d", form_class->pclocatortype)));
		}
		if (rel_clean &&
			form_class->pclocatortype != LOCATOR_TYPE_REPLICATED)
			InsertGTMClean(form_class->pcrelid, expansion_rel_node, rel_clean, clean_index_state);

		CHECK_FOR_INTERRUPTS();
	}
	MemoryContextSwitchTo(main_context);

	heap_endscan(scan);
	CatalogCloseIndexes(class_index_state);
	heap_close(rel_class, RowExclusiveLock);
	if (rel_clean)
	{
		CatalogCloseIndexes(clean_index_state);
		heap_close(rel_clean, RowExclusiveLock);
	}
}

static void ExpansionWorkerDatanode(shm_mq_handle *mqh_receiver, shm_mq_handle *mqh_sender, MemoryContext loop_context)
{
	Relation			rel_clean;
	Relation			rel;
	CatalogIndexState	clean_index_state;
	MemoryContext		main_context = CurrentMemoryContext;
	Size				size;
	StringInfoData		msg;
	shm_mq_result		result;
	int					msgtype;
	BlockNumber			num_blocks;

	rel_clean = heap_open(AdbCleanRelationId, RowExclusiveLock);
	clean_index_state = CatalogOpenIndexes(rel_clean);

loop_:
	MemoryContextSwitchTo(loop_context);
	MemoryContextReset(loop_context);
	result = shm_mq_receive(mqh_receiver, &size, (void**)&msg.data, false);
	if (result == SHM_MQ_DETACHED)
		ereport(ERROR,
				(errmsg("expansion worker can not receive expansion message: MQ detached")));
	Assert(result == SHM_MQ_SUCCESS);

	msg.len = msg.maxlen = (int)size;
	msg.cursor = 0;
	msgtype = pq_getmsgbyte(&msg);
	if (msgtype == EW_KEY_CLASS_RELATION)
	{
		rel = heap_open(load_oid_class(&msg), NoLock);
		if (rel->rd_rel->relkind == RELKIND_RELATION ||
			rel->rd_rel->relkind == RELPERSISTENCE_UNLOGGED)
			num_blocks = RelationGetNumberOfBlocks(rel);
		else
			num_blocks = 0;
		if (num_blocks > 0)
			InsertAdbClean(RelationGetRelid(rel),
						   RelationGetNumberOfBlocks(rel),
						   (Expr*)loadNode(&msg),
						   rel_clean,
						   clean_index_state);
		heap_close(rel, NoLock);
		goto loop_;
	}else if (msgtype != EW_KEY_END_DATABASE)
	{
		ereport(ERROR,
				(errmsg("expansion worker got unknown message type %d", msgtype),
				 errcode(ERRCODE_PROTOCOL_VIOLATION)));
	}

	CatalogCloseIndexes(clean_index_state);
	heap_close(rel_clean, RowExclusiveLock);
	MemoryContextSwitchTo(main_context);
}

void ExpansionWorkerMain(Datum arg)
{
	shm_mq_handle	   *mqh_sender;
	shm_mq_handle	   *mqh_receiver;
	dsm_segment		   *seg;
	List			   *expansion_node;
	MemoryContext		loop_context;
	ExpansionWorkerExtera *extra = (ExpansionWorkerExtera*)MyBgworkerEntry->bgw_extra;

	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	/* Set up a memory context and resource owner. */
	Assert(CurrentResourceOwner == NULL);
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "expansion toplevel");
	CurrentMemoryContext = AllocSetContextCreate(TopMemoryContext,
												 "expansion worker",
												 ALLOCSET_DEFAULT_SIZES);
	loop_context = AllocSetContextCreate(CurrentMemoryContext,
										 "expansion loop",
										 ALLOCSET_DEFAULT_SIZES);

	seg = dsm_attach(DatumGetUInt32(arg));
	CreateSHMQPipe(seg, &mqh_sender, &mqh_receiver, true);
	pq_redirect_to_shm_mq(seg, mqh_sender);

	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
	MyPgXact->vacuumFlags |= PROC_IS_EXPANSION_WORKER;
	LWLockRelease(ProcArrayLock);

	BackgroundWorkerInitializeConnectionByOid(extra->dboid, InvalidOid, 0);

	SetClientEncoding(extra->encoding);
	expansion_node = RestoreWorkerInfo(mqh_receiver);

	if (IsCnNode())
		ExpansionWorkerCoord(expansion_node, mqh_sender, loop_context);
	else
		ExpansionWorkerDatanode(mqh_receiver, mqh_sender, loop_context);
	MemoryContextDelete(loop_context);

	{
		static const char c = 'c';
		shm_mq_send(mqh_sender, sizeof(c), &c, false);
	}
}

static void TryExpansionWorkerMessage(ClusterExpansionContext *context, bool nowait)
{
	Size			size;
	StringInfoData	buf;
	int				msgtype;
	shm_mq_result	mq_result;

re_check_notice_:
	mq_result = shm_mq_receive(context->mq_receiver, &size, (void**)&buf.data, nowait);
	if (mq_result == SHM_MQ_SUCCESS)
	{
		buf.maxlen = buf.len = (int)size;
		buf.cursor = 0;
		msgtype = pq_getmsgbyte(&buf);
		switch (msgtype)
		{
		case 'E':	/* ErrorResponse */
		case 'N':	/* NoticeResponse */
			ProcessExpansionWorkerNotice(&buf);
			goto re_check_notice_;
		case 'c':
			WaitForBackgroundWorkerShutdown(context->handle);
			pfree(context->handle);
			context->handle = NULL;
			shm_mq_detach(context->mq_sender);
			shm_mq_detach(context->mq_receiver);
			context->mq_sender = context->mq_receiver = NULL;
			break;
		default:
			ereport(ERROR,
					(errmsg("unknown message type %d from expansion worker", msgtype),
					 errcode(ERRCODE_INTERNAL_ERROR)));
		}
	}else if (mq_result == SHM_MQ_DETACHED)
	{
		ereport(ERROR,
				(errmsg("get message from expansion worker failed")));
	}
}

static int ProcessClusterExpansionCommand(ClusterExpansionContext *context, const char *data, int len)
{
	StringInfoData	buf;
	int				msgtype;

	if (context->mq_receiver)
		TryExpansionWorkerMessage(context, true);

	buf.data = (char*)data;
	buf.len = buf.maxlen = len;
	buf.cursor = 0;

	msgtype = pq_getmsgbyte(&buf);
	switch(msgtype)
	{
	case EW_KEY_DATABASE:
		if (context->seg == NULL)
			context->seg = dsm_create(EXPANSION_QUEUE_SIZE*2, 0);
		if (context->handle)
			ereport(ERROR,
					(errmsg("last database not run end")));
			context->handle = StartExpansionWorker(context->seg,
												   &context->mq_sender,
												   &context->mq_receiver,
												   context->expansion,
												   RestoreDatabaseOid(&buf));
		break;
	case EW_KEY_CLASS_RELATION:
		if (IsCnNode())
			break;	/* shuld not happen, just skip data */
		if (context->seg == NULL || context->mq_sender == NULL)
			ereport(ERROR,
					(errmsg("no expansion worker for any database"),
					 errcode(ERRCODE_PROTOCOL_VIOLATION)));
		if (shm_mq_send(context->mq_sender, len, data, false) != SHM_MQ_SUCCESS)
			ereport(ERROR,
					(errmsg("send message to expansion worker failed")));
		break;
	case EW_KEY_END_DATABASE:
		if (context->seg == NULL || context->mq_sender == NULL)
			ereport(ERROR,
					(errmsg("no expansion worker for any database"),
					 errcode(ERRCODE_PROTOCOL_VIOLATION)));
		if (IsDnNode() &&
			shm_mq_send(context->mq_sender, len, data, false) != SHM_MQ_SUCCESS)
			ereport(ERROR,
					(errmsg("send message to expansion worker failed")));
		while (context->handle)
			TryExpansionWorkerMessage(context, false);
		put_executor_end_msg(true);
		break;
	default:
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("unexpected cluster command 0x%02X during COPY from coordinator", msgtype)));
	}
	return 0;
}

void ClusterExpansion(StringInfo mem_toc)
{
	ClusterExpansionContext context;
	MemSet(&context, 0, sizeof(context));

	if (IsCnNode())
	{
		AlterNodeStmt  *stmt;
		ParseState	   *pstate;
		StringInfoData	buf;

		buf.data = mem_toc_lookup(mem_toc, EW_KEY_COMMAND, &buf.len);
		if (buf.data == NULL)
			ereport(ERROR,
					(errmsg("Can not found AlterNodeStmt in cluster message"),
					 errcode(ERRCODE_PROTOCOL_VIOLATION)));
		buf.maxlen = buf.len;
		buf.cursor = 0;

		stmt = (AlterNodeStmt*)loadNode(&buf);
		if (stmt == NULL || !IsA(stmt, AlterNodeStmt))
			ereport(ERROR,
					(errmsg("Invalid AlterNodeStmt in cluster message"),
					 errcode(ERRCODE_PROTOCOL_VIOLATION)));

		buf.data = mem_toc_lookup(mem_toc, EW_KEY_SQL, &buf.len);
		if (buf.data == NULL)
			ereport(ERROR,
					(errmsg("Can not found sql string in cluster message"),
					 errcode(ERRCODE_PROTOCOL_VIOLATION)));
		pstate = make_parsestate(NULL);
		pstate->p_sourcetext = buf.data;
		context.expansion = MakeExpansionArg(stmt, pstate);
		free_parsestate(pstate);
	}

	SimpleNextCopyFromNewFE((SimpleCopyDataFunction)ProcessClusterExpansionCommand, &context);
	if (context.handle)
		WaitForBackgroundWorkerShutdown(context.handle);	/* should not run to here */
	if (context.seg)
		dsm_detach(context.seg);

	CacheInvalidateRelcacheAll();
	InvalidateSystemCaches();
}

static bool HaveItemPointerVar(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Var) &&
		((Var*)node)->varattno == SelfItemPointerAttributeNumber)
		return true;

	return expression_tree_walker(node, HaveItemPointerVar, NULL);
}

void RelationBuildExpansionClean(Relation rel)
{
	MemoryContext volatile context;
	MemoryContext volatile oldcontext;
	HeapTuple		tuple;
	ExpansionClean *clean;
	Form_adb_clean	form_clean;
	text		   *txt;
	char		   *str;
	TupleDesc		desc;
	if (RelationGetRelid(rel) < FirstNormalObjectId ||
		!IsDnNode())
		return;

	tuple = SearchSysCache2(ADBCLEANOID, ObjectIdGetDatum(MyDatabaseId), ObjectIdGetDatum(RelationGetRelid(rel)));
	if (!HeapTupleIsValid(tuple))
		return;

	context = AllocSetContextCreate(CacheMemoryContext, "expansion clean", ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(context);
	PG_TRY();
	{
		form_clean = (Form_adb_clean) GETSTRUCT(tuple);
		clean = palloc0(sizeof(*clean));
		clean->mcontext = CurrentMemoryContext;
		clean->max_block = form_clean->clnblocks;
		txt = pg_detoast_datum_packed(&form_clean->clnexpr);
		str = text_to_cstring(txt);
		clean->expr = stringToNode(str);
		pfree(str);
		if (txt != &form_clean->clnexpr)
			pfree(txt);
		clean->state = ExecInitExpr(clean->expr, NULL);
		clean->econtext = CreateStandaloneExprContext();
		desc = RelationGetDescr(rel);
		clean->slot = MakeSingleTupleTableSlot(desc);
		ResourceOwnerForgetTupleDesc(CurrentResourceOwner, desc);
		clean->econtext->ecxt_scantuple = clean->slot;

		/*
		 * for now, only clean "distribute by random" relation using "ctid" system column,
		 * so if have ctid column, mark need limit insert blocks
		 */
		clean->limit_insert = HaveItemPointerVar((Node*)clean->expr, NULL);
	}PG_CATCH();
	{
		MemoryContextSwitchTo(oldcontext);
		MemoryContextDelete(context);
		PG_RE_THROW();
	}PG_END_TRY();

	ReleaseSysCache(tuple);
	MemoryContextSwitchTo(oldcontext);
	rel->rd_clean = clean;
}

void DestroyExpansionClean(struct ExpansionClean *clean)
{
	if (clean == NULL)
		return;
	FreeExprContext(clean->econtext, true);
	ExecClearTuple(clean->slot);
	--(clean->slot->tts_tupleDescriptor);
	MemoryContextDelete(clean->mcontext);
}

bool IsExpansionCleanEqual(struct ExpansionClean *a, struct ExpansionClean *b)
{
	if (a == b)
		return true;
	if (a == NULL ||
		b == NULL)
		return false;

	if (equal(a->expr, b->expr) == false)
		return false;
	if (a->slot->tts_tupleDescriptor == b->slot->tts_tupleDescriptor ||
		equalTupleDescs(a->slot->tts_tupleDescriptor, b->slot->tts_tupleDescriptor))
		return true;

	return false;
}

bool ExecTestExpansionClean(struct ExpansionClean *clean, void *tup)
{
	Datum			datum;
	bool			isnull;

	if (ItemPointerGetBlockNumberNoCheck(&((HeapTuple)tup)->t_self) > clean->max_block)
	{
		datum = BoolGetDatum(true);
	}else
	{
		ExecStoreTuple(tup, clean->slot, InvalidBuffer, false);
		datum = ExecEvalExprSwitchContext(clean->state, clean->econtext, &isnull);
		Assert(!isnull);
		ResetExprContext(clean->econtext);
	}

	return DatumGetBool(datum);
}

BlockNumber GetExpansionInsertLimitBlock(struct ExpansionClean *clean)
{
	if (clean && clean->limit_insert)
		return clean->max_block;
	return InvalidBlockNumber;
}

bool CanInsertIntoExpansionRel(struct ExpansionClean *clean, BlockNumber blk)
{
	if (clean &&
		clean->limit_insert &&
		blk < clean->max_block)
		return false;
	return true;
}

static void finish_clean_rel(Relation rel_clean, ItemPointer tid, Buffer buffer)
{
	bool		need_release;
	Page		page;
	XLogRecPtr	recptr;
	ItemId		lpp;

	if (BufferIsValid(buffer))
	{
		need_release = false;
		Assert(BufferGetBlockNumber(buffer) == ItemPointerGetBlockNumber(tid));
	}else
	{
		need_release = true;
		buffer = ReadBuffer(rel_clean, ItemPointerGetBlockNumber(tid));
		LockBufferForCleanup(buffer);
	}
	page = BufferGetPage(buffer);

	lpp = PageGetItemId(page, tid->ip_posid);
	if (ItemIdIsNormal(lpp))
	{
		heap_page_prune_execute(buffer, NULL, 0, &tid->ip_posid, 1, NULL, 0);
		PageClearFull(page);
		MarkBufferDirty(buffer);
		recptr = log_heap_clean(rel_clean, buffer, NULL, 0, &tid->ip_posid, 1, NULL, 0, InvalidTransactionId);
		PageSetLSN(page, recptr);
	}
	if (need_release)
		UnlockReleaseBuffer(buffer);
}

void AlterNodeExpansionClean(AlterNodeStmt *stmt, struct ParseState *pstate)
{
	HeapTuple		tuple;
	Form_adb_clean	form_clean;
	List		   *all_connect;
	List		   *remote_list;
	List		   *remote_oids;
	List		   *list;
	ListCell	   *lc;
	text		   *txt;
	char		   *str;
	PGconn		   *conn;
	StringInfoData	msg;
	Relation		rel_clean = relation_open(AdbCleanRelationId, AccessExclusiveLock);
	HeapScanDesc	scan = heap_beginscan_catalog(rel_clean, 0, NULL);

#if 0
	(void)GetTransactionSnapshot();	/* update RecentGlobalDataXmin */
#endif

	/* find all datanodes */
	remote_oids = NIL;
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		form_clean = (Form_adb_clean) GETSTRUCT(tuple);
		if (form_clean->clndb != MyDatabaseId)
			continue;
#if 0
		if (HeapTupleHeaderGetRawXmax(tuple->t_data) < /* SnapSendGetGlobalXmin() */RecentGlobalDataXmin)
		{
			ereport(ERROR,
					(errmsg("expansion transaction id %u still in snapshot, please try again later",
							HeapTupleHeaderGetRawXmin(tuple->t_data)),
					 errhint("maybe \"vacuum_defer_cleanup_age\" too large")));
		}
#endif
		txt = pg_detoast_datum_packed(&form_clean->clnexpr);
		str = text_to_cstring(txt);
		list = stringToNode(str);
		pfree(str);
		if (txt != &form_clean->clnexpr)
			pfree(txt);

		foreach (lc, list)
			remote_oids = list_append_unique_oid(remote_oids, lfirst_oid(lc));
		list_free(list);
	}
	all_connect = NIL;
	if (remote_oids == NIL)
		goto clean_end_;

	/* start cluster function */
	initStringInfo(&msg);
	ClusterTocSetCustomFun(&msg, ClusterExpansionClean);
	all_connect = ExecClusterCustomFunction(remote_oids, &msg, 0);

	/* clean each relation data */
	heap_rescan(scan, NULL);
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		CHECK_FOR_INTERRUPTS();
		form_clean = (Form_adb_clean) GETSTRUCT(tuple);
		if (form_clean->clndb != MyDatabaseId)
			continue;

		if (SearchSysCacheExists1(RELOID, ObjectIdGetDatum(form_clean->clnrel)) == false)
			goto clean_rel_end_;	/* should not happen */

		txt = pg_detoast_datum_packed(&form_clean->clnexpr);
		str = text_to_cstring(txt);
		list = stringToNode(str);
		pfree(str);
		if (txt != &form_clean->clnexpr)
			pfree(txt);

		remote_list = NIL;
		foreach (lc, list)
		{
			conn = PQNFindConnUseOid(lfirst_oid(lc));
			if (conn == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Connection for node %u not connected", lfirst_oid(lc))));
			if (!PQisCopyOutState(conn) || !PQisCopyInState(conn))
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Connection for node %u is not in copy both mode", lfirst_oid(lc))));
			remote_list = lappend(remote_list, conn);
		}
		list_free(list);

		resetStringInfo(&msg);
		appendStringInfoChar(&msg, EW_KEY_CLASS_RELATION);
		save_oid_class(&msg, form_clean->clnrel);
		PQNputCopyData(remote_list, msg.data, msg.len);
		PQNFlush(remote_list, true);
		
		while(remote_list)
		{
			conn = linitial(remote_list);
			wait_executor_end_msg(conn);
			remote_list = list_delete_first(remote_list);
		}
clean_rel_end_:
		LockBufferForCleanup(scan->rs_cbuf);
		finish_clean_rel(rel_clean, &tuple->t_self, scan->rs_cbuf);
		LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);
	}
	pfree(msg.data);

clean_end_:
	heap_endscan(scan);
	relation_close(rel_clean, AccessExclusiveLock);
	if (all_connect)
	{
		PQNPutCopyEnd(all_connect);
		PQNListExecFinish(all_connect, NULL, &PQNDefaultHookFunctions, true);
		list_free(all_connect);
	}
}

static int ProcessClusterCleanCommand(ClusterCleanContext *context, const char *data, int len)
{
	Relation		rel;
	ExpansionClean *clean;
	StringInfoData	buf;
	int				msgtype;
	Oid				relid;
	BlockNumber		block;
	XLogRecPtr		recptr;
	OffsetNumber	clean_items[MaxHeapTuplesPerPage];
	int				clean_nitem;

	buf.data = (char*)data;
	buf.len = buf.maxlen = len;
	buf.cursor = 0;

	msgtype = pq_getmsgbyte(&buf);
	if (msgtype != EW_KEY_CLASS_RELATION)
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("unexpected cluster command 0x%02X during COPY from coordinator", msgtype)));
	}
	recptr = InvalidXLogRecPtr;
	rel = NULL;
	relid = load_oid_class_extend(&buf, true);
	if (!OidIsValid(relid))
		goto end_clean_rel_;

	rel = heap_open(relid, AccessShareLock);
	if (rel->rd_clean == NULL)
	{
		Assert(SearchSysCacheExists2(ADBCLEANOID, ObjectIdGetDatum(MyDatabaseId), ObjectIdGetDatum(relid)) == false);
		goto end_clean_rel_;
	}
	clean = rel->rd_clean;

	for (block=0;block<clean->max_block;++block)
	{
		bool			all_visible;
		bool			valid;
		int				lines;
		OffsetNumber	lineoff;
		ItemId			lpp;
		HeapTupleData	loctup;
		Buffer			buffer = ReadBuffer(rel, block);
		Page			page = BufferGetPage(buffer);

		LockBufferForCleanup(buffer);
		TestForOldSnapshot(context->snapshot, rel, page);
		lines = PageGetMaxOffsetNumber(page);
		all_visible = PageIsAllVisible(page);
		clean_nitem = 0;
		for (lineoff = FirstOffsetNumber, lpp = PageGetItemId(page, lineoff);
			 lineoff <= lines;
			 lineoff++,lpp++)
		{
			if (!ItemIdIsNormal(lpp))
				continue;
			loctup.t_tableOid = relid;
			loctup.t_data = (HeapTupleHeader) PageGetItem(page, lpp);
			loctup.t_len = ItemIdGetLength(lpp);
			ItemPointerSet(&(loctup.t_self), block, lineoff);

			if (all_visible)
				valid = true;
			else
				valid = HeapTupleSatisfiesVisibility(&loctup, context->snapshot, buffer);

			if (valid && ExecTestExpansionClean(rel->rd_clean, &loctup) == false)
				clean_items[clean_nitem++] = lineoff;
		}

		if (clean_nitem > 0)
		{
			heap_page_prune_execute(buffer, NULL, 0, clean_items, clean_nitem, NULL, 0);
			PageClearFull(page);
			MarkBufferDirty(buffer);
			if (RelationNeedsWAL(rel))
			{
				recptr = log_heap_clean(rel, buffer, NULL, 0, clean_items, clean_nitem, NULL, 0, InvalidTransactionId);
				PageSetLSN(page, recptr);
			}
		}
		UnlockReleaseBuffer(buffer);
		CHECK_FOR_INTERRUPTS();
	}

end_clean_rel_:
	if (rel != NULL)
	{
		HeapTuple tup = SearchSysCache2(ADBCLEANOID, ObjectIdGetDatum(MyDatabaseId), relid);
		if (HeapTupleIsValid(tup))
		{
			if (recptr != InvalidXLogRecPtr)
				XLogFlush(recptr);	/* before clean rel info, flush last XLOG */
			finish_clean_rel(context->rel_clean, &tup->t_self, InvalidBuffer);
			ReleaseSysCache(tup);
		}
		relation_close(rel, AccessShareLock);
	}
	put_executor_end_msg(true);
	return 0;
}

void ClusterExpansionClean(StringInfo mem_toc)
{
	ClusterCleanContext context;
	MemSet(&context, 0, sizeof(context));
	context.rel_clean = relation_open(AdbCleanRelationId, AccessExclusiveLock);
	context.snapshot = GetActiveSnapshot();
	SimpleNextCopyFromNewFE((SimpleCopyDataFunction)ProcessClusterCleanCommand, &context);
	relation_close(context.rel_clean, AccessExclusiveLock);
	CacheInvalidateRelcacheAll();
	InvalidateSystemCaches();
}

/* Delete the expansion cleanup information of the specified relation. */
void RemoveCleanInfoFromExpansionClean(Oid relOid)
{
	HeapTuple		tuple;
	Relation		rel_clean;
	Form_adb_clean	form_clean;

	tuple = SearchSysCache2(ADBCLEANOID, ObjectIdGetDatum(MyDatabaseId), ObjectIdGetDatum(relOid));
	if (tuple)
	{
		form_clean = (Form_adb_clean) GETSTRUCT(tuple);
		Assert(form_clean->clndb == MyDatabaseId && 
			   form_clean->clnrel == relOid);

		rel_clean = relation_open(AdbCleanRelationId, RowExclusiveLock);
		simple_heap_delete(rel_clean, &(tuple->t_self));
		relation_close(rel_clean, RowExclusiveLock);
		ReleaseSysCache(tuple);
	}
}