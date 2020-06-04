/*-------------------------------------------------------------------------
 *
 * matview.c
 *	  materialized view support
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/matview.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "commands/cluster.h"
#include "commands/matview.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "parser/parse_relation.h"
#include "pgstat.h"
#include "rewrite/rewriteHandler.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#ifdef ADB
#include "access/tuptypeconvert.h"
#include "commands/event_trigger.h"
#include "executor/clusterReceiver.h"
#include "executor/execCluster.h"
#include "executor/tstoreReceiver.h"
#include "libpq/libpq-fe.h"
#include "libpq/pqformat.h"
#include "libpq/libpq-node.h"
#include "libpq/libpq.h"
#include "pgxc/nodemgr.h"
#include "storage/mem_toc.h"
#include "utils/memutils.h"
#include "catalog/pgxc_node.h"
#include "commands/copy.h"
#include "commands/createas.h"
#include "intercomm/inter-comm.h"
#include "pgxc/pgxc.h"
#include "pgxc/execRemote.h"
#include "pgxc/remotecopy.h"
#include "pgxc/copyops.h"
#include "catalog/pg_type.h"
#include "funcapi.h"

#define REMOTE_KEY_CMD_INFO		0x1
#endif


typedef struct
{
	DestReceiver pub;			/* publicly-known function pointers */
	Oid			transientoid;	/* OID of new heap into which to store */
	/* These fields are filled by transientrel_startup: */
	Relation	transientrel;	/* relation to write to */
	CommandId	output_cid;		/* cmin to insert in output tuples */
	int			ti_options;		/* table_tuple_insert performance options */
	BulkInsertState bistate;	/* bulk insert state */
} DR_transientrel;

static int	matview_maintenance_depth = 0;

static void transientrel_startup(DestReceiver *self, int operation, TupleDesc typeinfo);
static bool transientrel_receive(TupleTableSlot *slot, DestReceiver *self);
static void transientrel_shutdown(DestReceiver *self);
static void transientrel_destroy(DestReceiver *self);
static uint64 refresh_matview_datafill(DestReceiver *dest, Query *query,
									   const char *queryString);
static char *make_temptable_name_n(char *tempname, int n);
static void refresh_by_match_merge(Oid matviewOid, Oid tempOid, Oid relowner,
								   int save_sec_context ADB_ONLY_COMMA_ARG(Tuplestorestate *tstore));
static void refresh_by_heap_swap(Oid matviewOid, Oid OIDNewHeap, char relpersistence);
static bool is_usable_unique_index(Relation indexRel);
static void OpenMatViewIncrementalMaintenance(void);
static void CloseMatViewIncrementalMaintenance(void);
#ifdef ADB
static ObjectAddress ExecRefreshMatView_adb(RefreshMatViewStmt *stmt, const char *queryString,
											ParamListInfo params, char *completionTag, bool master);
static void adb_send_relation_data(List *connList, Oid reloid, LOCKMODE lockmode);
static void adb_send_copy_end(List *connList);
static uint64 adb_recv_relation_data(DestReceiver *self, TupleDesc desc, int operator);
#endif /* ADB */

/*
 * SetMatViewPopulatedState
 *		Mark a materialized view as populated, or not.
 *
 * NOTE: caller must be holding an appropriate lock on the relation.
 */
void
SetMatViewPopulatedState(Relation relation, bool newstate)
{
	Relation	pgrel;
	HeapTuple	tuple;

	Assert(relation->rd_rel->relkind == RELKIND_MATVIEW);

	/*
	 * Update relation's pg_class entry.  Crucial side-effect: other backends
	 * (and this one too!) are sent SI message to make them rebuild relcache
	 * entries.
	 */
	pgrel = table_open(RelationRelationId, RowExclusiveLock);
	tuple = SearchSysCacheCopy1(RELOID,
								ObjectIdGetDatum(RelationGetRelid(relation)));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u",
			 RelationGetRelid(relation));

	((Form_pg_class) GETSTRUCT(tuple))->relispopulated = newstate;

	CatalogTupleUpdate(pgrel, &tuple->t_self, tuple);

	heap_freetuple(tuple);
	table_close(pgrel, RowExclusiveLock);

	/*
	 * Advance command counter to make the updated pg_class row locally
	 * visible.
	 */
	CommandCounterIncrement();
}

#ifdef ADB
void ClusterRefreshMatView(StringInfo mem_toc)
{
	RefreshMatViewStmt *stmt;
	const char		   *queryString;
	ParamListInfo		params;
	char			   *completionTag;

	StringInfoData		buf;
	buf.data = mem_toc_lookup(mem_toc, REMOTE_KEY_CMD_INFO, &buf.maxlen);
	if (buf.data == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("Can not found cluster refresh materialized view message")));
	}
	buf.len = buf.maxlen;
	buf.cursor = 0;

	stmt = (RefreshMatViewStmt*)loadNode(&buf);
	if (stmt == NULL ||
		!IsA(stmt, RefreshMatViewStmt))
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("Can not found RefreshMatViewStmt struct")));
	}
	queryString = load_node_string(&buf, false);
	completionTag = load_node_string(&buf, true);
	params = LoadParamList(&buf);

	/*
	 * REFRESH CONCURRENTLY executes some DDL commands internally.
	 * Inhibit DDL command collection here to avoid those commands
	 * from showing up in the deparsed command queue.  The refresh
	 * command itself is queued, which is enough.
	 */
	EventTriggerInhibitCommandCollection();
	PG_TRY();
	{
		ExecRefreshMatView_adb(stmt, queryString, params, completionTag, false);
	}
	PG_CATCH();
	{
		EventTriggerUndoInhibitCommandCollection();
		PG_RE_THROW();
	}
	PG_END_TRY();
	EventTriggerUndoInhibitCommandCollection();

	pfree(completionTag);
}
#endif /* ADB */

/*
 * ExecRefreshMatView -- execute a REFRESH MATERIALIZED VIEW command
 *
 * This refreshes the materialized view by creating a new table and swapping
 * the relfilenodes of the new table and the old materialized view, so the OID
 * of the original materialized view is preserved. Thus we do not lose GRANT
 * nor references to this materialized view.
 *
 * If WITH NO DATA was specified, this is effectively like a TRUNCATE;
 * otherwise it is like a TRUNCATE followed by an INSERT using the SELECT
 * statement associated with the materialized view.  The statement node's
 * skipData field shows whether the clause was used.
 *
 * Indexes are rebuilt too, via REINDEX. Since we are effectively bulk-loading
 * the new heap, it's better to create the indexes afterwards than to fill them
 * incrementally while we load.
 *
 * The matview's "populated" state is changed based on whether the contents
 * reflect the result set of the materialized view's query.
 */
ObjectAddress
ExecRefreshMatView(RefreshMatViewStmt *stmt, const char *queryString,
				   ParamListInfo params, char *completionTag)
{
#ifdef ADB
	return ExecRefreshMatView_adb(stmt, queryString, params, completionTag, true);
}

static ObjectAddress
ExecRefreshMatView_adb(RefreshMatViewStmt *stmt, const char *queryString,
					   ParamListInfo params, char *completionTag, bool master)
{
	Tuplestorestate *tstore = NULL;
	List		   *connList = NIL;
#endif /* ADB */
	Oid			matviewOid;
	Relation	matviewRel;
	RewriteRule *rule;
	List	   *actions;
	Query	   *dataQuery;
	Oid			tableSpace;
	Oid			relowner;
	Oid			OIDNewHeap;
	DestReceiver *dest;
	uint64		processed = 0;
	bool		concurrent;
	LOCKMODE	lockmode;
	char		relpersistence = RELPERSISTENCE_TEMP;
	Oid			save_userid;
	int			save_sec_context;
	int			save_nestlevel;
	ObjectAddress address;

	/* Determine strength of lock needed. */
	concurrent = stmt->concurrent;
	lockmode = concurrent ? ExclusiveLock : AccessExclusiveLock;

	/*
	 * Get a lock until end of transaction.
	 */
	matviewOid = RangeVarGetRelidExtended(stmt->relation,
										  lockmode, 0,
										  RangeVarCallbackOwnsTable, NULL);
	matviewRel = table_open(matviewOid, NoLock);

	/* Make sure it is a materialized view. */
	if (matviewRel->rd_rel->relkind != RELKIND_MATVIEW)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("\"%s\" is not a materialized view",
						RelationGetRelationName(matviewRel))));

	/* Check that CONCURRENTLY is not specified if not populated. */
	if (concurrent && !RelationIsPopulated(matviewRel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("CONCURRENTLY cannot be used when the materialized view is not populated")));

	/* Check that conflicting options have not been specified. */
	if (concurrent && stmt->skipData)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("CONCURRENTLY and WITH NO DATA options cannot be used together")));

	/*
	 * Check that everything is correct for a refresh. Problems at this point
	 * are internal errors, so elog is sufficient.
	 */
	if (matviewRel->rd_rel->relhasrules == false ||
		matviewRel->rd_rules->numLocks < 1)
		elog(ERROR,
			 "materialized view \"%s\" is missing rewrite information",
			 RelationGetRelationName(matviewRel));

	if (matviewRel->rd_rules->numLocks > 1)
		elog(ERROR,
			 "materialized view \"%s\" has too many rules",
			 RelationGetRelationName(matviewRel));

	rule = matviewRel->rd_rules->rules[0];
	if (rule->event != CMD_SELECT || !(rule->isInstead))
		elog(ERROR,
			 "the rule for materialized view \"%s\" is not a SELECT INSTEAD OF rule",
			 RelationGetRelationName(matviewRel));

	actions = rule->actions;
	if (list_length(actions) != 1)
		elog(ERROR,
			 "the rule for materialized view \"%s\" is not a single action",
			 RelationGetRelationName(matviewRel));

	/*
	 * Check that there is a unique index with no WHERE clause on one or more
	 * columns of the materialized view if CONCURRENTLY is specified.
	 */
	if (concurrent)
	{
		List	   *indexoidlist = RelationGetIndexList(matviewRel);
		ListCell   *indexoidscan;
		bool		hasUniqueIndex = false;

		foreach(indexoidscan, indexoidlist)
		{
			Oid			indexoid = lfirst_oid(indexoidscan);
			Relation	indexRel;

			indexRel = index_open(indexoid, AccessShareLock);
			hasUniqueIndex = is_usable_unique_index(indexRel);
			index_close(indexRel, AccessShareLock);
			if (hasUniqueIndex)
				break;
		}

		list_free(indexoidlist);

		if (!hasUniqueIndex)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("cannot refresh materialized view \"%s\" concurrently",
							quote_qualified_identifier(get_namespace_name(RelationGetNamespace(matviewRel)),
													   RelationGetRelationName(matviewRel))),
					 errhint("Create a unique index with no WHERE clause on one or more columns of the materialized view.")));
	}

	/*
	 * The stored query was rewritten at the time of the MV definition, but
	 * has not been scribbled on by the planner.
	 */
	dataQuery = linitial_node(Query, actions);

	/*
	 * Check for active uses of the relation in the current transaction, such
	 * as open scans.
	 *
	 * NB: We count on this to protect us against problems with refreshing the
	 * data using TABLE_INSERT_FROZEN.
	 */
	CheckTableNotInUse(matviewRel, "REFRESH MATERIALIZED VIEW");

	/*
	 * Tentatively mark the matview as populated or not (this will roll back
	 * if we fail later).
	 */
	SetMatViewPopulatedState(matviewRel, !stmt->skipData);

#ifdef ADB
	if (master)
	{
		List		   *oidList = adb_get_all_coord_oid_list(false);
		StringInfoData	toc;

		/* not include me */
		oidList = list_delete_oid(oidList, PGXCNodeOid);
		if (oidList != NIL)
		{
			initStringInfo(&toc);
			
			ClusterTocSetCustomFun(&toc, ClusterRefreshMatView);

			begin_mem_toc_insert(&toc, REMOTE_KEY_CMD_INFO);
			saveNode(&toc, (Node*)stmt);
			save_node_string(&toc, queryString);
			save_node_string(&toc, completionTag);
			SaveParamList(&toc, params);
			end_mem_toc_insert(&toc, REMOTE_KEY_CMD_INFO);

			connList = ExecClusterCustomFunction(oidList, &toc, 0);

			pfree(toc.data);
			list_free(oidList);
		}
	}
#endif /* ADB */

	relowner = matviewRel->rd_rel->relowner;

	/*
	 * Switch to the owner's userid, so that any functions are run as that
	 * user.  Also arrange to make GUC variable changes local to this command.
	 * Don't lock it down too tight to create a temporary table just yet.  We
	 * will switch modes when we are about to execute user code.
	 */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(relowner,
						   save_sec_context | SECURITY_LOCAL_USERID_CHANGE);
	save_nestlevel = NewGUCNestLevel();

	/* Concurrent refresh builds new data in temp tablespace, and does diff. */
#ifdef ADB
	/* if we using a temporary table not in master coordinator, we can not using 2PC */
	if (master == false && concurrent)
	{
		tstore = tuplestore_begin_heap(true, false, work_mem);
		dest = CreateTuplestoreDestReceiver();
		SetTuplestoreDestReceiverParams(dest, tstore, CurrentMemoryContext, false);
		OIDNewHeap = InvalidOid;
	}else
	{
#endif /* ADB */
	if (concurrent)
	{
		tableSpace = GetDefaultTablespace(RELPERSISTENCE_TEMP, false);
		relpersistence = RELPERSISTENCE_TEMP;
	}
	else
	{
		tableSpace = matviewRel->rd_rel->reltablespace;
		relpersistence = matviewRel->rd_rel->relpersistence;
	}

	/*
	 * Create the transient table that will receive the regenerated data. Lock
	 * it against access by any other process until commit (by which time it
	 * will be gone).
	 */
	OIDNewHeap = make_new_heap(matviewOid, tableSpace, relpersistence,
							   ExclusiveLock);
	LockRelationOid(OIDNewHeap, AccessExclusiveLock);
	dest = CreateTransientRelDestReceiver(OIDNewHeap);
#ifdef ADB
	}
#endif /* ADB */

	/*
	 * Now lock down security-restricted operations.
	 */
	SetUserIdAndSecContext(relowner,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);

	/* Generate the data, if wanted. */
	if (!stmt->skipData)
	{
#ifdef ADB
		if (master)
		{
#endif /* ADB */
		processed = refresh_matview_datafill(dest, dataQuery, queryString);
#ifdef ADB
			adb_send_relation_data(connList, OIDNewHeap, NoLock);
			adb_send_copy_end(connList);
		}else
		{
			processed = adb_recv_relation_data(dest,
											   RelationGetDescr(matviewRel),
											   dataQuery->commandType);
		}
#endif /* ADB */
	}

	/* Make the matview match the newly generated data. */
	if (concurrent)
	{
		int			old_depth = matview_maintenance_depth;

		PG_TRY();
		{
			refresh_by_match_merge(matviewOid, OIDNewHeap, relowner,
								   save_sec_context ADB_ONLY_COMMA_ARG(tstore));
		}
		PG_CATCH();
		{
			matview_maintenance_depth = old_depth;
			PG_RE_THROW();
		}
		PG_END_TRY();
		Assert(matview_maintenance_depth == old_depth);
	}
	else
	{
		refresh_by_heap_swap(matviewOid, OIDNewHeap, relpersistence);

		/*
		 * Inform stats collector about our activity: basically, we truncated
		 * the matview and inserted some new data.  (The concurrent code path
		 * above doesn't need to worry about this because the inserts and
		 * deletes it issues get counted by lower-level code.)
		 */
		pgstat_count_truncate(matviewRel);
		if (!stmt->skipData)
			pgstat_count_heap_insert(matviewRel, processed);
	}

	table_close(matviewRel, NoLock);

	/* Roll back any GUC changes */
	AtEOXact_GUC(false, save_nestlevel);

	/* Restore userid and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	ObjectAddressSet(address, RelationRelationId, matviewOid);

#ifdef ADB
	if (tstore)
		tuplestore_end(tstore);
	if (connList)
	{
		PQNListExecFinish(connList, NULL, &PQNFalseHookFunctions, true);
		list_free(connList);
	}
#endif /* ADB */

	return address;
}

/*
 * refresh_matview_datafill
 *
 * Execute the given query, sending result rows to "dest" (which will
 * insert them into the target matview).
 *
 * Returns number of rows inserted.
 */
static uint64
refresh_matview_datafill(DestReceiver *dest, Query *query,
						 const char *queryString)
{
	List	   *rewritten;
	PlannedStmt *plan;
	QueryDesc  *queryDesc;
	Query	   *copied_query;
	uint64		processed;

	/* Lock and rewrite, using a copy to preserve the original query. */
	copied_query = copyObject(query);
	AcquireRewriteLocks(copied_query, true, false);
	rewritten = QueryRewrite(copied_query);

	/* SELECT should never rewrite to more or less than one SELECT query */
	if (list_length(rewritten) != 1)
		elog(ERROR, "unexpected rewrite result for REFRESH MATERIALIZED VIEW");
	query = (Query *) linitial(rewritten);

	/* Check for user-requested abort. */
	CHECK_FOR_INTERRUPTS();

	/* Plan the query which will generate data for the refresh. */
	plan = pg_plan_query(query, 0, NULL);

	/*
	 * Use a snapshot with an updated command ID to ensure this query sees
	 * results of any previously executed queries.  (This could only matter if
	 * the planner executed an allegedly-stable function that changed the
	 * database contents, but let's do it anyway to be safe.)
	 */
	PushCopiedSnapshot(GetActiveSnapshot());
	UpdateActiveSnapshotCommandId();

	/* Create a QueryDesc, redirecting output to our tuple receiver */
	queryDesc = CreateQueryDesc(plan, queryString,
								GetActiveSnapshot(), InvalidSnapshot,
								dest, NULL, NULL, 0);

	/* call ExecutorStart to prepare the plan for execution */
	ExecutorStart(queryDesc, 0);

	/* run the plan */
	ExecutorRun(queryDesc, ForwardScanDirection, 0L, true);

	processed = queryDesc->estate->es_processed;

	/* and clean up */
	ExecutorFinish(queryDesc);
	ExecutorEnd(queryDesc);

	FreeQueryDesc(queryDesc);

	PopActiveSnapshot();

	return processed;
}

DestReceiver *
CreateTransientRelDestReceiver(Oid transientoid)
{
	DR_transientrel *self = (DR_transientrel *) palloc0(sizeof(DR_transientrel));

	self->pub.receiveSlot = transientrel_receive;
	self->pub.rStartup = transientrel_startup;
	self->pub.rShutdown = transientrel_shutdown;
	self->pub.rDestroy = transientrel_destroy;
	self->pub.mydest = DestTransientRel;
	self->transientoid = transientoid;

	return (DestReceiver *) self;
}

/*
 * transientrel_startup --- executor startup
 */
static void
transientrel_startup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
	DR_transientrel *myState = (DR_transientrel *) self;
	Relation	transientrel;

	transientrel = table_open(myState->transientoid, NoLock);

	/*
	 * Fill private fields of myState for use by later routines
	 */
	myState->transientrel = transientrel;
	myState->output_cid = GetCurrentCommandId(true);

	/*
	 * We can skip WAL-logging the insertions, unless PITR or streaming
	 * replication is in use. We can skip the FSM in any case.
	 */
	myState->ti_options = TABLE_INSERT_SKIP_FSM | TABLE_INSERT_FROZEN;
	if (!XLogIsNeeded())
		myState->ti_options |= TABLE_INSERT_SKIP_WAL;
	myState->bistate = GetBulkInsertState();

	/* Not using WAL requires smgr_targblock be initially invalid */
	Assert(RelationGetTargetBlock(transientrel) == InvalidBlockNumber);
}

/*
 * transientrel_receive --- receive one tuple
 */
static bool
transientrel_receive(TupleTableSlot *slot, DestReceiver *self)
{
	DR_transientrel *myState = (DR_transientrel *) self;

	/*
	 * Note that the input slot might not be of the type of the target
	 * relation. That's supported by table_tuple_insert(), but slightly less
	 * efficient than inserting with the right slot - but the alternative
	 * would be to copy into a slot of the right type, which would not be
	 * cheap either. This also doesn't allow accessing per-AM data (say a
	 * tuple's xmin), but since we don't do that here...
	 */

	table_tuple_insert(myState->transientrel,
					   slot,
					   myState->output_cid,
					   myState->ti_options,
					   myState->bistate);

	/* We know this is a newly created relation, so there are no indexes */

	return true;
}

/*
 * transientrel_shutdown --- executor end
 */
static void
transientrel_shutdown(DestReceiver *self)
{
	DR_transientrel *myState = (DR_transientrel *) self;

	FreeBulkInsertState(myState->bistate);

	table_finish_bulk_insert(myState->transientrel, myState->ti_options);

	/* close transientrel, but keep lock until commit */
	table_close(myState->transientrel, NoLock);
	myState->transientrel = NULL;
}

/*
 * transientrel_destroy --- release DestReceiver object
 */
static void
transientrel_destroy(DestReceiver *self)
{
	pfree(self);
}


/*
 * Given a qualified temporary table name, append an underscore followed by
 * the given integer, to make a new table name based on the old one.
 *
 * This leaks memory through palloc(), which won't be cleaned up until the
 * current memory context is freed.
 */
static char *
make_temptable_name_n(char *tempname, int n)
{
	StringInfoData namebuf;

	initStringInfo(&namebuf);
	appendStringInfoString(&namebuf, tempname);
	appendStringInfo(&namebuf, "_%d", n);
	return namebuf.data;
}

/*
 * refresh_by_match_merge
 *
 * Refresh a materialized view with transactional semantics, while allowing
 * concurrent reads.
 *
 * This is called after a new version of the data has been created in a
 * temporary table.  It performs a full outer join against the old version of
 * the data, producing "diff" results.  This join cannot work if there are any
 * duplicated rows in either the old or new versions, in the sense that every
 * column would compare as equal between the two rows.  It does work correctly
 * in the face of rows which have at least one NULL value, with all non-NULL
 * columns equal.  The behavior of NULLs on equality tests and on UNIQUE
 * indexes turns out to be quite convenient here; the tests we need to make
 * are consistent with default behavior.  If there is at least one UNIQUE
 * index on the materialized view, we have exactly the guarantee we need.
 *
 * The temporary table used to hold the diff results contains just the TID of
 * the old record (if matched) and the ROW from the new table as a single
 * column of complex record type (if matched).
 *
 * Once we have the diff table, we perform set-based DELETE and INSERT
 * operations against the materialized view, and discard both temporary
 * tables.
 *
 * Everything from the generation of the new data to applying the differences
 * takes place under cover of an ExclusiveLock, since it seems as though we
 * would want to prohibit not only concurrent REFRESH operations, but also
 * incremental maintenance.  It also doesn't seem reasonable or safe to allow
 * SELECT FOR UPDATE or SELECT FOR SHARE on rows being updated or deleted by
 * this command.
 */
static void
refresh_by_match_merge(Oid matviewOid, Oid tempOid, Oid relowner,
					   int save_sec_context ADB_ONLY_COMMA_ARG(Tuplestorestate *tstore))
{
	StringInfoData querybuf;
	Relation	matviewRel;
	Relation	tempRel;
	char	   *matviewname;
	char	   *tempname;
#ifdef ADB
	char	   *difftempname;
	EphemeralNamedRelation difftblenr = NULL;
	TupleDesc difftbldesc = NULL;
	Tuplestorestate *difftbltupstore = NULL;
	int difftblrc;
#endif
	char	   *diffname;
	TupleDesc	tupdesc;
	bool		foundUniqueIndex;
	List	   *indexoidlist;
	ListCell   *indexoidscan;
	int16		relnatts;
	Oid		   *opUsedForQual;

	initStringInfo(&querybuf);
	matviewRel = table_open(matviewOid, NoLock);
	matviewname = quote_qualified_identifier(get_namespace_name(RelationGetNamespace(matviewRel)),
											 RelationGetRelationName(matviewRel));
#ifdef ADB
	if (tstore)
	{
		tempRel = NULL;
		tempname = "named_ts_for_refresh_mv";
		difftempname = "named_ts_for_diff_mv";
	}else
	{
#endif /* ADB */
	tempRel = table_open(tempOid, NoLock);
	tempname = quote_qualified_identifier(get_namespace_name(RelationGetNamespace(tempRel)),
										  RelationGetRelationName(tempRel));
#ifdef ADB
	}
#endif /* ADB */
	diffname = make_temptable_name_n(tempname, 2);

	relnatts = RelationGetNumberOfAttributes(matviewRel);

	/* Open SPI context. */
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

#ifdef ADB
	if (tstore)
	{
		EphemeralNamedRelation enr = palloc0(sizeof(*enr));
		int rc;
		
		enr->md.name = tempname;
		enr->md.reliddesc = InvalidOid;
		enr->md.tupdesc = RelationGetDescr(matviewRel);
		enr->md.enrtype = ENR_NAMED_TUPLESTORE;
		enr->md.enrtuples = tuplestore_tuple_count(tstore);
		enr->reldata = tstore;

		rc = SPI_register_relation(enr);
		if (rc != SPI_OK_REL_REGISTER)
			elog(ERROR, "SPI register relation failed:%d", rc);
	}else
	{
#endif /* ADB */
	/* Analyze the temp table with the new contents. */
	appendStringInfo(&querybuf, "ANALYZE %s", tempname);
	if (SPI_exec(querybuf.data, 0) != SPI_OK_UTILITY)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);

	/*
	 * We need to ensure that there are not duplicate rows without NULLs in
	 * the new data set before we can count on the "diff" results.  Check for
	 * that in a way that allows showing the first duplicated row found.  Even
	 * after we pass this test, a unique index on the materialized view may
	 * find a duplicate key problem.
	 */
	resetStringInfo(&querybuf);
	appendStringInfo(&querybuf,
					 "SELECT newdata FROM %s newdata "
					 "WHERE newdata IS NOT NULL AND EXISTS "
					 "(SELECT 1 FROM %s newdata2 WHERE newdata2 IS NOT NULL "
					 "AND newdata2 OPERATOR(pg_catalog.*=) newdata "
					 "AND newdata2.ctid OPERATOR(pg_catalog.<>) "
					 "newdata.ctid)",
					 tempname, tempname);
	if (SPI_execute(querybuf.data, false, 1) != SPI_OK_SELECT)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);
	if (SPI_processed > 0)
	{
		/*
		 * Note that this ereport() is returning data to the user.  Generally,
		 * we would want to make sure that the user has been granted access to
		 * this data.  However, REFRESH MAT VIEW is only able to be run by the
		 * owner of the mat view (or a superuser) and therefore there is no
		 * need to check for access to data in the mat view.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_CARDINALITY_VIOLATION),
				 errmsg("new data for materialized view \"%s\" contains duplicate rows without any null columns",
						RelationGetRelationName(matviewRel)),
				 errdetail("Row: %s",
						   SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1))));
	}
#ifdef ADB
	}
#endif /* ADB */

	SetUserIdAndSecContext(relowner,
						   save_sec_context | SECURITY_LOCAL_USERID_CHANGE);

	/* Start building the query for creating the diff table. */
	resetStringInfo(&querybuf);
#ifdef ADB
	if (!tstore)
#endif /* ADB */
		appendStringInfo(&querybuf, "CREATE TEMP TABLE %s AS ", diffname);
	appendStringInfo(&querybuf,
					 "SELECT mv.ctid AS tid, newdata "
					 "FROM %s mv FULL JOIN %s newdata ON (",
					 matviewname, tempname);

	/*
	 * Get the list of index OIDs for the table from the relcache, and look up
	 * each one in the pg_index syscache.  We will test for equality on all
	 * columns present in all unique indexes which only reference columns and
	 * include all rows.
	 */
	tupdesc = matviewRel->rd_att;
	opUsedForQual = (Oid *) palloc0(sizeof(Oid) * relnatts);
	foundUniqueIndex = false;

	indexoidlist = RelationGetIndexList(matviewRel);

	foreach(indexoidscan, indexoidlist)
	{
		Oid			indexoid = lfirst_oid(indexoidscan);
		Relation	indexRel;

		indexRel = index_open(indexoid, RowExclusiveLock);
		if (is_usable_unique_index(indexRel))
		{
			Form_pg_index indexStruct = indexRel->rd_index;
			int			indnkeyatts = indexStruct->indnkeyatts;
			oidvector  *indclass;
			Datum		indclassDatum;
			bool		isnull;
			int			i;

			/* Must get indclass the hard way. */
			indclassDatum = SysCacheGetAttr(INDEXRELID,
											indexRel->rd_indextuple,
											Anum_pg_index_indclass,
											&isnull);
			Assert(!isnull);
			indclass = (oidvector *) DatumGetPointer(indclassDatum);

			/* Add quals for all columns from this index. */
			for (i = 0; i < indnkeyatts; i++)
			{
				int			attnum = indexStruct->indkey.values[i];
				Oid			opclass = indclass->values[i];
				Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);
				Oid			attrtype = attr->atttypid;
				HeapTuple	cla_ht;
				Form_pg_opclass cla_tup;
				Oid			opfamily;
				Oid			opcintype;
				Oid			op;
				const char *leftop;
				const char *rightop;

				/*
				 * Identify the equality operator associated with this index
				 * column.  First we need to look up the column's opclass.
				 */
				cla_ht = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));
				if (!HeapTupleIsValid(cla_ht))
					elog(ERROR, "cache lookup failed for opclass %u", opclass);
				cla_tup = (Form_pg_opclass) GETSTRUCT(cla_ht);
				Assert(cla_tup->opcmethod == BTREE_AM_OID);
				opfamily = cla_tup->opcfamily;
				opcintype = cla_tup->opcintype;
				ReleaseSysCache(cla_ht);

				op = get_opfamily_member(opfamily, opcintype, opcintype,
										 BTEqualStrategyNumber);
				if (!OidIsValid(op))
					elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
						 BTEqualStrategyNumber, opcintype, opcintype, opfamily);

				/*
				 * If we find the same column with the same equality semantics
				 * in more than one index, we only need to emit the equality
				 * clause once.
				 *
				 * Since we only remember the last equality operator, this
				 * code could be fooled into emitting duplicate clauses given
				 * multiple indexes with several different opclasses ... but
				 * that's so unlikely it doesn't seem worth spending extra
				 * code to avoid.
				 */
				if (opUsedForQual[attnum - 1] == op)
					continue;
				opUsedForQual[attnum - 1] = op;

				/*
				 * Actually add the qual, ANDed with any others.
				 */
				if (foundUniqueIndex)
					appendStringInfoString(&querybuf, " AND ");

				leftop = quote_qualified_identifier("newdata",
													NameStr(attr->attname));
				rightop = quote_qualified_identifier("mv",
													 NameStr(attr->attname));

				generate_operator_clause(&querybuf,
										 leftop, attrtype,
										 op,
										 rightop, attrtype);

				foundUniqueIndex = true;
			}
		}

		/* Keep the locks, since we're about to run DML which needs them. */
		index_close(indexRel, NoLock);
	}

	list_free(indexoidlist);

	/*
	 * There must be at least one usable unique index on the matview.
	 *
	 * ExecRefreshMatView() checks that after taking the exclusive lock on the
	 * matview. So at least one unique index is guaranteed to exist here
	 * because the lock is still being held; so an Assert seems sufficient.
	 */
	Assert(foundUniqueIndex);

	appendStringInfoString(&querybuf,
						   " AND newdata OPERATOR(pg_catalog.*=) mv) "
						   "WHERE newdata IS NULL OR mv IS NULL "
						   "ORDER BY tid");
#ifdef ADB
	if (tstore == NULL)
	{
#endif /* ADB */
	/* Create the temporary "diff" table. */
	if (SPI_exec(querybuf.data, 0) != SPI_OK_UTILITY)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);
#ifdef ADB
	}
	else
	{
		HeapTuple spi_tuple;
		int i = 0;

		/* use the tuplestore difftbltupstore to store the diff tuple content */
		difftblenr = palloc0(sizeof(*difftblenr));
		difftblenr->md.name = difftempname;
		difftblenr->md.reliddesc = InvalidOid;
		difftblenr->md.enrtype = ENR_NAMED_TUPLESTORE;
		difftbltupstore = tuplestore_begin_heap(true, false, work_mem);

		if ( SPI_exec(querybuf.data, 0) != SPI_OK_SELECT)
			elog(ERROR, "SPI_exec failed: %s", querybuf.data);

		difftbldesc = CreateTemplateTupleDesc(2);
		TupleDescInitEntry(difftbldesc, (AttrNumber) 1, "tid",
						   TIDOID, -1, 0);
		TupleDescInitEntry(difftbldesc, (AttrNumber) 2, "newdata",
						   matviewRel->rd_att->tdtypeid, -1, 0);
		difftblenr->md.tupdesc = BlessTupleDesc(difftbldesc);

		for (i = 0; i < SPI_processed; i++)
		{
			spi_tuple = SPI_tuptable->vals[i];
			tuplestore_puttuple(difftbltupstore, spi_tuple);
		}

		difftblenr->md.enrtuples = tuplestore_tuple_count(difftbltupstore);
		tuplestore_donestoring(difftbltupstore);
		difftblenr->reldata = difftbltupstore;
		difftblrc = SPI_register_relation(difftblenr);

		if (difftblrc != SPI_OK_REL_REGISTER)
			elog(ERROR, "SPI register relation failed:%d", difftblrc);

	}
#endif
	SetUserIdAndSecContext(relowner,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);

	/*
	 * We have no further use for data from the "full-data" temp table, but we
	 * must keep it around because its type is referenced from the diff table.
	 */

#ifdef ADB
	if (tstore == NULL)
	{
#endif /* ADB */
	/* Analyze the diff table. */
	resetStringInfo(&querybuf);
	appendStringInfo(&querybuf, "ANALYZE %s", diffname);
	if (SPI_exec(querybuf.data, 0) != SPI_OK_UTILITY)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);

	OpenMatViewIncrementalMaintenance();

	/* Deletes must come before inserts; do them first. */
	resetStringInfo(&querybuf);
#ifdef ADB
	}else
	{
		/* Deletes must come before inserts; do them first. */
		resetStringInfo(&querybuf);
		OpenMatViewIncrementalMaintenance();
	}
#endif /* ADB */

#ifdef ADB
	if (tstore)
		appendStringInfo(&querybuf,
					 "DELETE FROM %s mv WHERE ctid OPERATOR(pg_catalog.=) ANY "
					 "(SELECT diff.tid FROM %s diff "
					 "WHERE diff.tid IS NOT NULL "
					 "AND diff.newdata IS NULL)",
					 matviewname, difftempname);
	else
#endif
	appendStringInfo(&querybuf,
					 "DELETE FROM %s mv WHERE ctid OPERATOR(pg_catalog.=) ANY "
					 "(SELECT diff.tid FROM %s diff "
					 "WHERE diff.tid IS NOT NULL "
					 "AND diff.newdata IS NULL)",
					 matviewname, diffname);
#ifdef ADB
	if (tstore == NULL)
	{
#endif /* ADB */
	if (SPI_exec(querybuf.data, 0) != SPI_OK_DELETE)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);

	/* Inserts go last. */
	resetStringInfo(&querybuf);
#ifdef ADB
	}else
	{
		if (SPI_exec(querybuf.data, 0) != SPI_OK_DELETE)
			elog(ERROR, "SPI_exec failed: %s", querybuf.data);

		/* Inserts go last. */
		resetStringInfo(&querybuf);
	}
#endif /* ADB */

#ifdef ADB
	if (tstore)
		appendStringInfo(&querybuf,
					 "INSERT INTO %s SELECT (diff.newdata).* "
					 "FROM %s diff WHERE tid IS NULL",
					 matviewname, difftempname);
	else
#endif
	appendStringInfo(&querybuf,
					 "INSERT INTO %s SELECT (diff.newdata).* "
					 "FROM %s diff WHERE tid IS NULL",
					 matviewname, diffname);
	if (SPI_exec(querybuf.data, 0) != SPI_OK_INSERT)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);

	/* We're done maintaining the materialized view. */
	CloseMatViewIncrementalMaintenance();
#ifdef ADB
	if (tempRel != NULL)
#endif /* ADB */
	table_close(tempRel, NoLock);
	table_close(matviewRel, NoLock);

	/* Clean up temp tables. */
	resetStringInfo(&querybuf);
#ifdef ADB
	if (tstore == NULL)
	{
#endif /* ADB */
	appendStringInfo(&querybuf, "DROP TABLE %s, %s", diffname, tempname);
	if (SPI_exec(querybuf.data, 0) != SPI_OK_UTILITY)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);
#ifdef ADB
	}
	else
	{
		SPI_unregister_relation(difftempname);
		if (difftbltupstore)
			tuplestore_end(difftbltupstore);
		if (difftbldesc)
			FreeTupleDesc(difftbldesc);
		pfree(difftblenr);

	}
#endif /* ADB */

	/* Close SPI context. */
	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");
}

/*
 * Swap the physical files of the target and transient tables, then rebuild
 * the target's indexes and throw away the transient table.  Security context
 * swapping is handled by the called function, so it is not needed here.
 */
static void
refresh_by_heap_swap(Oid matviewOid, Oid OIDNewHeap, char relpersistence)
{
	finish_heap_swap(matviewOid, OIDNewHeap, false, false, true, true,
					 RecentXmin, ReadNextMultiXactId(), relpersistence);
}

/*
 * Check whether specified index is usable for match merge.
 */
static bool
is_usable_unique_index(Relation indexRel)
{
	Form_pg_index indexStruct = indexRel->rd_index;

	/*
	 * Must be unique, valid, immediate, non-partial, and be defined over
	 * plain user columns (not expressions).  We also require it to be a
	 * btree.  Even if we had any other unique index kinds, we'd not know how
	 * to identify the corresponding equality operator, nor could we be sure
	 * that the planner could implement the required FULL JOIN with non-btree
	 * operators.
	 */
	if (indexStruct->indisunique &&
		indexStruct->indimmediate &&
		indexRel->rd_rel->relam == BTREE_AM_OID &&
		indexStruct->indisvalid &&
		RelationGetIndexPredicate(indexRel) == NIL &&
		indexStruct->indnatts > 0)
	{
		/*
		 * The point of groveling through the index columns individually is to
		 * reject both index expressions and system columns.  Currently,
		 * matviews couldn't have OID columns so there's no way to create an
		 * index on a system column; but maybe someday that wouldn't be true,
		 * so let's be safe.
		 */
		int			numatts = indexStruct->indnatts;
		int			i;

		for (i = 0; i < numatts; i++)
		{
			int			attnum = indexStruct->indkey.values[i];

			if (attnum <= 0)
				return false;
		}
		return true;
	}
	return false;
}


/*
 * This should be used to test whether the backend is in a context where it is
 * OK to allow DML statements to modify materialized views.  We only want to
 * allow that for internal code driven by the materialized view definition,
 * not for arbitrary user-supplied code.
 *
 * While the function names reflect the fact that their main intended use is
 * incremental maintenance of materialized views (in response to changes to
 * the data in referenced relations), they are initially used to allow REFRESH
 * without blocking concurrent reads.
 */
bool
MatViewIncrementalMaintenanceIsEnabled(void)
{
	return matview_maintenance_depth > 0;
}

static void
OpenMatViewIncrementalMaintenance(void)
{
	matview_maintenance_depth++;
}

static void
CloseMatViewIncrementalMaintenance(void)
{
	matview_maintenance_depth--;
	Assert(matview_maintenance_depth >= 0);
}

#ifdef ADB
static void flush_copy_data(PGconn *conn)
{
	int res;
	for(;;)
	{
		res = PQflush(conn);
		if (res < 0)
			ereport(ERROR,
					(errmsg("%s", PQerrorMessage(conn)),
					 errnode(PQNConnectName(conn))));
		else if (res == 0)
			break;
		
		WaitLatchOrSocket(MyLatch,
						  WL_LATCH_SET | WL_SOCKET_WRITEABLE,
						  PQsocket(conn),
						  -1L,
						  PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
	}
}
static void send_copy_data(List *list, const char *buf, int len)
{
	ListCell *lc;
	int res;
	foreach (lc, list)
	{
		res = PQputCopyData(lfirst(lc), buf, len);
		if (res < 0)
			ereport(ERROR,
					(errmsg("%s", PQerrorMessage(lfirst(lc))),
					 errnode(PQNConnectName(lfirst(lc)))));
		else if (res > 0)
			flush_copy_data(lfirst(lc));
	}
}

static void adb_send_copy_end(List *connList)
{
	ListCell *lc;
	int res;
	foreach (lc, connList)
	{
		res = PQputCopyEnd(lfirst(lc), NULL);
		if (res < 0)
			ereport(ERROR,
					(errmsg("%s", PQerrorMessage(lfirst(lc))),
					 errnode(PQNConnectName(lfirst(lc)))));
		else if (res > 0)
			flush_copy_data(lfirst(lc));
	}
}

static void adb_send_relation_data(List *connList, Oid reloid, LOCKMODE lockmode)
{
	Relation			rel = heap_open(reloid, lockmode);
	TableScanDesc		scandesc = table_beginscan(rel, GetActiveSnapshot(), 0, NULL);
	TupleTypeConvert   *convert = create_type_convert(RelationGetDescr(rel), true, false);
	TupleTableSlot	   *base_slot = table_slot_create(rel, NULL);
	MemoryContext		context = AllocSetContextCreate(CurrentMemoryContext,
														"SEND RELATION",
														ALLOCSET_DEFAULT_SIZES);
	MemoryContext		oldcontext;
	TupleTableSlot	   *out_slot;
	StringInfoData		buf;
	char				msg_type;

	initStringInfo(&buf);
	serialize_slot_head_message(&buf, RelationGetDescr(rel));
	send_copy_data(connList, buf.data, buf.len);
	if (convert)
	{
		out_slot = MakeSingleTupleTableSlot(convert->out_desc, &TTSOpsVirtual);
		msg_type = CLUSTER_MSG_CONVERT_TUPLE;
		resetStringInfo(&buf);
		serialize_slot_convert_head(&buf, convert->out_desc);
		send_copy_data(connList, buf.data, buf.len);
	}else
	{
		out_slot = base_slot;
		msg_type = CLUSTER_MSG_TUPLE_DATA;
	}

	oldcontext = MemoryContextSwitchTo(context);
	while (table_scan_getnextslot(scandesc, ForwardScanDirection, base_slot))
	{
		if (convert)
			do_type_convert_slot_out(convert, base_slot, out_slot, false);

		resetStringInfo(&buf);
		serialize_slot_message(&buf, out_slot, msg_type);
		send_copy_data(connList, buf.data, buf.len);

		MemoryContextReset(context);
	}

	pfree(buf.data);
	free_type_convert(convert);
	if (out_slot != base_slot)
		ExecDropSingleTupleTableSlot(out_slot);
	ExecDropSingleTupleTableSlot(base_slot);
	table_endscan(scandesc);
	table_close(rel, NoLock);
	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(context);
}

static bool recv_copy_data(StringInfo buf, char msg_type)
{
	int			mtype;

readmessage:
	HOLD_CANCEL_INTERRUPTS();
	pq_startmsgread();
	mtype = pq_getbyte();
	if (mtype == EOF ||
		pq_getmessage(buf, 0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("unexpected EOF on client connection with an open transaction")));
	}
	RESUME_CANCEL_INTERRUPTS();

	switch (mtype)
	{
		case 'd':	/* CopyData */
			break;
		case 'c':	/* CopyDone */
			/* COPY IN correctly terminated by frontend */
			return false;
		case 'f':	/* CopyFail */
			ereport(ERROR,
					(errcode(ERRCODE_QUERY_CANCELED),
					 errmsg("COPY from stdin failed: %s",
							pq_getmsgstring(buf))));
			break;
		case 'H':	/* Flush */
		case 'S':	/* Sync */

			/*
			 * Ignore Flush/Sync for the convenience of client
			 * libraries (such as libpq) that may send those
			 * without noticing that the command they just
			 * sent was COPY.
			 */
			goto readmessage;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("unexpected message type 0x%02X during COPY from stdin",
							mtype)));
			break;
	}

	if (buf->data[0] != msg_type)
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("Invalid message type 0x%02X", buf->data[0]),
				 errdetail("Expect 0x%02X", msg_type)));
	}
	++(buf->cursor);

	return true;
}

static uint64 adb_recv_relation_data(DestReceiver *self, TupleDesc desc, int operator)
{
	TupleTypeConvert   *convert = create_type_convert(desc, false, true);
	TupleTableSlot	   *base_slot = MakeSingleTupleTableSlot(desc, convert ? &TTSOpsVirtual : &TTSOpsMinimalTuple);
	MemoryContext		context = AllocSetContextCreate(CurrentMemoryContext,
														"RECV RELATION",
														ALLOCSET_DEFAULT_SIZES);
	MemoryContext		oldcontext;
	TupleTableSlot	   *in_slot;
	uint64				processed;
	StringInfoData		buf;
	char				msg_type;

	initStringInfo(&buf);
	if (recv_copy_data(&buf, CLUSTER_MSG_TUPLE_DESC) == false)
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("Can not found datatype message")));
	}
	compare_slot_head_message(&buf.data[buf.cursor], buf.len-buf.cursor, desc);

	if (convert)
	{
		in_slot = MakeSingleTupleTableSlot(convert->out_desc, &TTSOpsMinimalTuple);
		msg_type = CLUSTER_MSG_CONVERT_TUPLE;
		if (recv_copy_data(&buf, CLUSTER_MSG_CONVERT_DESC) == false)
		{
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					errmsg("Can not found datatype message")));
		}
		compare_slot_head_message(&buf.data[buf.cursor], buf.len-buf.cursor, convert->out_desc);
	}else
	{
		in_slot = base_slot;
		msg_type = CLUSTER_MSG_TUPLE_DATA;
	}

	(*self->rStartup)(self, operator, desc);
	processed = 0;

	oldcontext = MemoryContextSwitchTo(context);
	while (recv_copy_data(&buf, msg_type))
	{
		MemoryContextReset(context);

		restore_slot_message(&buf.data[buf.cursor], buf.len - buf.cursor, in_slot);
		if (convert)
			do_type_convert_slot_in(convert, in_slot, base_slot, false);
		
		if (((*self->receiveSlot)(base_slot, self)) == false)
			break;
		++processed;
	}

	(*self->rShutdown)(self);

	pfree(buf.data);
	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(context);
	if (convert)
	{
		Assert(in_slot != base_slot);
		free_type_convert(convert);
		ExecDropSingleTupleTableSlot(in_slot);
	}
	ExecDropSingleTupleTableSlot(base_slot);

	return processed;
}

/*
 * This function accepts the data from the coordinator which initiated the
 * REFRESH MV command and inserts it into the transient relation created for the
 * materialized view.
 * The caller provides the materialized view receiver and the argument values to
 * be passed to the startup function of this receiver.
 */
extern void
pgxc_fill_matview_by_copy(DestReceiver *mv_dest, bool skipdata, int operation,
							TupleDesc tupdesc)
{
	CopyState	cstate;
	Relation	mv_rel = NULL;
	TupleTableSlot	*slot = MakeTupleTableSlot(NULL, &TTSOpsVirtual);

	Assert(IsCnCandidate());

	/*
	 * We need to provide the Relation where to copy to BeginCopyFrom. The
	 * Relation is populated by startup method of the DestReceiver. The startup
	 * method also creates the relation (in this case the materialized view
	 * itself or heap thereof), so irrespective of whether we are going to
	 * populate the MV or not, always fire the starup function.
	 */
	mv_dest->rStartup(mv_dest, operation, tupdesc);

	/*
	 * If we are to populate the the materialized view, only then start copy
	 * protocol and accept the data. The initiating coordinator too, will send
	 * the data only when the MV is to be populated.
	 */
	if (!skipdata)
	{
		if (mv_dest->mydest == DestTransientRel)
			mv_rel = ((DR_transientrel *)mv_dest)->transientrel;
		else if (mv_dest->mydest == DestIntoRel)
			mv_rel = get_dest_into_rel(mv_dest);

		AssertArg(mv_rel);
		ExecSetSlotDescriptor(slot, RelationGetDescr(mv_rel));
		/*
		 * Prepare structures to start receiving the data sent by the other
		 * coordinator through COPY protocol.
		 */
		cstate = BeginCopyFrom(NULL, mv_rel, NULL, false, NULL, NULL, NULL);
		/* Read the rows one by one and insert into the materialized view */
		for(;;)
		{
			ExecClearTuple(slot);
			/*
			 * Pull next row. The expression context is not used here, since there
			 * are no default expressions expected. Tuple OID too is not expected
			 * for materialized view.
			 */
			if (!NextCopyFrom(cstate, NULL, slot->tts_values, slot->tts_isnull))
				break;

			/* Create the tuple and slot out of the values read */
			ExecStoreVirtualTuple(slot);

			/* Insert the row in the materialized view */
			mv_dest->receiveSlot(slot, mv_dest);
		}
		EndCopyFrom(cstate);
	}

	/* Done, close the receiver and flag the end of COPY FROM */
	mv_dest->rShutdown(mv_dest);

}

/*
 * The function scans the recently refreshed materialized view and send the data
 * to the other coordinators to "refresh" materialized views at those
 * coordinators.
 * The query_string is expected to contain the DDL which requires this data
 * transfer e.g. CREATE MV or REFRSH MV. The DDL is sent to the other
 * coordinators, which in turn start receiving the data to populate the
 * materialized view.
 */
extern void
pgxc_send_matview_data(RangeVar *matview_rv, const char *query_string)
{
	Oid				matviewOid;
	Relation		matviewRel;
	RemoteCopyState*copyState;
	TupleDesc 		tupdesc;
	TableScanDesc	scandesc;
	TupleTableSlot *slot;
	StringInfoData	line_buf;

	/*
	 * This function should be called only from the coordinator where the
	 * REFRESH MV command is fired.
	 */
	Assert (IsCnMaster());
	/*
	 * The other coordinator will start accepting data through COPY protocol in
	 * response to the DDL. So, start sending the data with COPY
	 * protocol to the other coordinators.
	 */

	/* Prepare the RemoteCopyState for the COPYing data to the other coordinators */
	copyState = (RemoteCopyState *) palloc0(sizeof(RemoteCopyState));
	copyState->exec_nodes = makeNode(ExecNodes);
	/* We are copying the data from the materialized view */
	copyState->is_from = false;
	/* Materialized views are available on all the coordinators. */
	copyState->exec_nodes->nodeids = GetAllCnIDL(false);
	initStringInfo(&(copyState->query_buf));
	appendStringInfoString(&(copyState->query_buf), query_string);
	/* Begin redistribution on remote nodes */
	StartRemoteCopy(copyState);

	/*
	 * Open the relation for reading.
	 * Get a lock until end of transaction.
	 */
	matviewOid = RangeVarGetRelidExtended(matview_rv,
										  AccessShareLock, 0,
										  RangeVarCallbackOwnsTable, NULL);
	matviewRel = table_open(matviewOid, NoLock);
	tupdesc = RelationGetDescr(matviewRel);
	slot = table_slot_create(matviewRel, NULL);

	initStringInfo(&line_buf);
	scandesc = table_beginscan(matviewRel, SnapshotAny, 0, NULL);
	/* Send each tuple to the other coordinators in COPY format */
	while (table_scan_getnextslot(scandesc, ForwardScanDirection, slot))
	{
		CHECK_FOR_INTERRUPTS();
		/* Deconstruct to get the values for the attributes */
		slot_getallattrs(slot);

		/* Format and send the data */
		CopyOps_BuildOneRowTo(tupdesc, slot->tts_values, slot->tts_isnull, &line_buf);

		DoRemoteCopyFrom(copyState, &line_buf, copyState->exec_nodes->nodeids);
	}
	table_endscan(scandesc);
	ExecDropSingleTupleTableSlot(slot);
	pfree(line_buf.data);

	/*
	 * Finish the redistribution process. There is no primary node for
	 * Materialized view and it's replicated on all the coordinators
	 */
	EndRemoteCopy(copyState);

	/* Lock is maintained until transaction commits */
	table_close(matviewRel, NoLock);
	return;
}
#endif /* ADB */
