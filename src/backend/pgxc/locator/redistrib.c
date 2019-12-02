/*-------------------------------------------------------------------------
 *
 * redistrib.c
 *	  Routines related to online data redistribution
 *
 * Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/pgxc/locator/redistrib.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "access/hash.h"
#include "access/htup.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_node.h"
#include "commands/tablecmds.h"
#include "pgxc/copyops.h"
#include "pgxc/execRemote.h"
#include "pgxc/pgxc.h"
#include "pgxc/redistrib.h"
#include "pgxc/remotecopy.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#ifdef ADB
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "commands/cluster.h"
#include "commands/copy.h"
#include "executor/clusterReceiver.h"
#include "executor/execCluster.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "intercomm/inter-comm.h"
#include "libpq/pqformat.h"
#include "nodes/makefuncs.h"
#include "optimizer/reduceinfo.h"
#include "pgxc/locator.h"
#include "storage/mem_toc.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "../../src/interfaces/libpq/libpq-fe.h"
#include "utils/syscache.h"
#endif

#define IsCommandTypePreUpdate(x) (x == CATALOG_UPDATE_BEFORE || \
								   x == CATALOG_UPDATE_BOTH)
#define IsCommandTypePostUpdate(x) (x == CATALOG_UPDATE_AFTER || \
									x == CATALOG_UPDATE_BOTH)
#ifdef ADB
extern bool enable_cluster_plan;

#define SHADOW_RELATION_PREFIX		"pg_temp_"
#define REMOTE_KEY_CREATE_SHADOW_TABLE		1
#define REMOTE_KEY_REDIST_SHADOW_DATA		2
#define REMOTE_KEY_SWAP_SHADOW_SOURCE_TABLE	3
#define REMOTE_KEY_REWRITE_CATALOG_TABLE	4

typedef struct ShadowReduceState
{
	Relation		redist_currentRelation;
	HeapScanDesc	redist_currentScanDesc;
	TupleTableSlot *redist_ScanTupleSlot;
	TupleTableSlot *redist_ResultTupleSlot;
	ExprContext	   *redist_ExprContext;
	ProjectionInfo *redist_ProjInfo;
} ShadowReduceState;

#endif


/* Functions used for the execution of redistribution commands */
static void distrib_execute_query(char *sql, bool is_temp, ExecNodes *exec_nodes);
static void distrib_execute_command(RedistribState *distribState, RedistribCommand *command);
static void distrib_copy_to(RedistribState *distribState);
static void distrib_copy_from(RedistribState *distribState, ExecNodes *exec_nodes);
static void distrib_truncate(RedistribState *distribState, ExecNodes *exec_nodes);
void distrib_reindex(RedistribState *distribState, ExecNodes *exec_nodes);
static void distrib_delete_hash(RedistribState *distribState, ExecNodes *exec_nodes);

/* Functions used to build the command list */
static void pgxc_redist_build_entry(RedistribState *distribState,
								RelationLocInfo *oldLocInfo,
								RelationLocInfo *newLocInfo);
static void pgxc_redist_build_replicate(RedistribState *distribState,
								RelationLocInfo *oldLocInfo,
								RelationLocInfo *newLocInfo);
static void pgxc_redist_build_replicate_to_distrib(RedistribState *distribState,
								RelationLocInfo *oldLocInfo,
								RelationLocInfo *newLocInfo);

static void pgxc_redist_build_default(RedistribState *distribState);
static void pgxc_redist_add_reindex(RedistribState *distribState);

#ifdef ADB
static void DoReduceDataForShadowRel(Relation master,
					   Relation shadow,
					   List *rnodes,
					   AuxiliaryRelCopy *redistcopy,
					   char masterLocatorType,
					   char shadowLocatorType,
					   Oid preferred);
static List *distrib_get_remote_reduce_nodelist(RelationLocInfo *oldLocInfo, RelationLocInfo *newLocInfo
	, RedistribOperation commandType);
static TupleTableSlot *NextRowForDistribScanNone(CopyState cstate, ExprContext *context, void *data);
static List *MakeMainRelTargetForShadow(Relation mainRel, Index relid, bool targetEntry);
static int process_distrib_cmd(void *context, const char *data, int len);
static AuxiliaryRelCopy *MakeShadowRelCopyInfoFromMaster(Relation masterRel, RelationLocInfo *newLocInfo, int shadowId);
static bool distrib_local_node_need_reduce(char masterLocatorType, char shadowLocatorType
				, Oid preferred, Oid PGXCNodeOid);
static void WaitExecutorEndMessage(List *remotes);
#endif

/*
 * PGXCRedistribTable
 * Execute redistribution operations after catalog update
 */
void
PGXCRedistribTable(RedistribState *distribState, RedistribCatalog type)
{
	ListCell *item;

	/* Nothing to do if no redistribution operation */
	if (!distribState)
		return;

	/* Nothing to do if on remote node */
	if (!IsCnMaster())
		return;

	/* Execute each command if necessary */
	foreach(item, distribState->commands)
	{
		RedistribCommand *command = (RedistribCommand *)lfirst(item);

		/* Check if command can be run */
		if (!IsCommandTypePostUpdate(type) &&
			IsCommandTypePostUpdate(command->updateState))
			continue;
		if (!IsCommandTypePreUpdate(type) &&
			IsCommandTypePreUpdate(command->updateState))
			continue;

		/* Now enter in execution list */
		distrib_execute_command(distribState, command);
	}
}


/*
 * PGXCRedistribCreateCommandList
 * Look for the list of necessary commands to perform table redistribution.
 */
void
PGXCRedistribCreateCommandList(RedistribState *distribState, RelationLocInfo *newLocInfo)
{
	Relation rel;
	RelationLocInfo *oldLocInfo;

	rel = relation_open(distribState->relid, NoLock);
	oldLocInfo = RelationGetLocInfo(rel);

	/* Build redistribution command list */
	pgxc_redist_build_entry(distribState, oldLocInfo, newLocInfo);

	relation_close(rel, NoLock);
}


/*
 * pgxc_redist_build_entry
 * Entry point for command list building
 */
static void
pgxc_redist_build_entry(RedistribState *distribState,
						RelationLocInfo *oldLocInfo,
						RelationLocInfo *newLocInfo)
{
	/* If distribution has not changed at all, nothing to do */
	if (IsLocatorInfoEqual(oldLocInfo, newLocInfo))
		return;

	/* Evaluate cases for replicated tables */
	pgxc_redist_build_replicate(distribState, oldLocInfo, newLocInfo);

	/* Evaluate cases for replicated to distributed tables */
	pgxc_redist_build_replicate_to_distrib(distribState, oldLocInfo, newLocInfo);

	/* PGXCTODO: perform more complex builds of command list */

	/* Fallback to default */
	pgxc_redist_build_default(distribState);
}


/*
 * pgxc_redist_build_replicate_to_distrib
 * Build redistribution command list from replicated to distributed
 * table.
 */
static void
pgxc_redist_build_replicate_to_distrib(RedistribState *distribState,
							RelationLocInfo *oldLocInfo,
							RelationLocInfo *newLocInfo)
{
	List *removedNodeIds;
	List *newNodeIds;

	/* If a command list has already been built, nothing to do */
	if (list_length(distribState->commands) != 0)
		return;

	/* Redistribution is done from replication to distributed (with value) */
	if (!IsRelationReplicated(oldLocInfo) ||
		!IsRelationDistributedByValue(newLocInfo))
		return;

	/* Get the list of nodes that are added to the relation */
	removedNodeIds = list_difference_oid(oldLocInfo->nodeids, newLocInfo->nodeids);

	/* Get the list of nodes that are removed from relation */
	newNodeIds = list_difference_oid(newLocInfo->nodeids, oldLocInfo->nodeids);

	/*
	 * If some nodes are added, turn back to default, we need to fetch data
	 * and then redistribute it properly.
	 */
	if (newNodeIds != NIL)
		return;

	/* Nodes removed have to be truncated, so add a TRUNCATE commands to removed nodes */
	if (removedNodeIds != NIL)
	{
		ExecNodes *execNodes = makeNode(ExecNodes);
		execNodes->nodeids = removedNodeIds;
		/* Add TRUNCATE command */
		distribState->commands = lappend(distribState->commands,
					 makeRedistribCommand(DISTRIB_TRUNCATE, CATALOG_UPDATE_BEFORE, execNodes));
	}

	/*
	 * If the table is redistributed to a single node, a TRUNCATE on removed nodes
	 * is sufficient so leave here.
	 */
	if (list_length(newLocInfo->nodeids) == 1)
	{
		/* Add REINDEX command if necessary */
		pgxc_redist_add_reindex(distribState);
		return;
	}

	/*
	 * If we are here we are sure that redistribution only requires to delete data on remote
	 * nodes on the new subset of nodes. So launch to remote nodes a DELETE command that only
	 * eliminates the data not verifying the new hashing condition.
	 */
	if (newLocInfo->locatorType == LOCATOR_TYPE_HASH)
	{
		ExecNodes *execNodes = makeNode(ExecNodes);
		execNodes->nodeids = list_copy(newLocInfo->nodeids);
		distribState->commands = lappend(distribState->commands,
					 makeRedistribCommand(DISTRIB_DELETE_HASH, CATALOG_UPDATE_AFTER, execNodes));
	}
	else if (newLocInfo->locatorType == LOCATOR_TYPE_MODULO)
	{
		ExecNodes *execNodes = makeNode(ExecNodes);
		execNodes->nodeids = list_copy(newLocInfo->nodeids);
		distribState->commands = lappend(distribState->commands,
					 makeRedistribCommand(DISTRIB_DELETE_MODULO, CATALOG_UPDATE_AFTER, execNodes));
	}
	else if (newLocInfo->locatorType == LOCATOR_TYPE_HASHMAP)
	{
		ExecNodes *execNodes = makeNode(ExecNodes);
		execNodes->nodeids = list_copy(newLocInfo->nodeids);
		distribState->commands = lappend(distribState->commands,
					 makeRedistribCommand(DISTRIB_DELETE_HASHMAP, CATALOG_UPDATE_AFTER, execNodes));
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("Incorrect redistribution operation")));

	/* Add REINDEX command if necessary */
	pgxc_redist_add_reindex(distribState);
}


/*
 * pgxc_redist_build_replicate
 * Build redistribution command list for replicated tables
 */
static void
pgxc_redist_build_replicate(RedistribState *distribState,
							RelationLocInfo *oldLocInfo,
							RelationLocInfo *newLocInfo)
{
	List *removedNodeIds;
	List *newNodeIds;

	/* If a command list has already been built, nothing to do */
	if (list_length(distribState->commands) != 0)
		return;

	/* Case of a replicated table whose set of nodes is changed */
	if (!IsRelationReplicated(newLocInfo) ||
		!IsRelationReplicated(oldLocInfo))
		return;

	/* Get the list of nodes that are added to the relation */
	removedNodeIds = list_difference_oid(oldLocInfo->nodeids, newLocInfo->nodeids);

	/* Get the list of nodes that are removed from relation */
	newNodeIds = list_difference_oid(newLocInfo->nodeids, oldLocInfo->nodeids);

	/*
	 * If nodes have to be added, we need to fetch data for redistribution first.
	 * So add a COPY TO command to fetch data.
	 */
	if (newNodeIds != NIL)
	{
		/* Add COPY TO command */
		distribState->commands = lappend(distribState->commands,
					 makeRedistribCommand(DISTRIB_COPY_TO, CATALOG_UPDATE_BEFORE, NULL));
	}

	/* Nodes removed have to be truncated, so add a TRUNCATE commands to removed nodes */
	if (removedNodeIds != NIL)
	{
		ExecNodes *execNodes = makeNode(ExecNodes);
		execNodes->nodeids = removedNodeIds;
		/* Add TRUNCATE command */
		distribState->commands = lappend(distribState->commands,
					 makeRedistribCommand(DISTRIB_TRUNCATE, CATALOG_UPDATE_BEFORE, execNodes));
	}

	/* If necessary, COPY the data obtained at first step to the new nodes. */
	if (newNodeIds != NIL)
	{
		ExecNodes *execNodes = makeNode(ExecNodes);
		execNodes->nodeids = newNodeIds;
		/* Add COPY FROM command */
		distribState->commands = lappend(distribState->commands,
					 makeRedistribCommand(DISTRIB_COPY_FROM, CATALOG_UPDATE_AFTER, execNodes));
	}

	/* Add REINDEX command if necessary */
	pgxc_redist_add_reindex(distribState);
}


/*
 * pgxc_redist_build_default
 * Build a default list consisting of
 * COPY TO -> TRUNCATE -> COPY FROM ( -> REINDEX )
 */
static void
pgxc_redist_build_default(RedistribState *distribState)
{
	/* If a command list has already been built, nothing to do */
	if (list_length(distribState->commands) != 0)
		return;

	/* COPY TO command */
	distribState->commands = lappend(distribState->commands,
					 makeRedistribCommand(DISTRIB_COPY_TO, CATALOG_UPDATE_BEFORE, NULL));
	/* TRUNCATE command */
	distribState->commands = lappend(distribState->commands,
					 makeRedistribCommand(DISTRIB_TRUNCATE, CATALOG_UPDATE_BEFORE, NULL));
	/* COPY FROM command */
	distribState->commands = lappend(distribState->commands,
					 makeRedistribCommand(DISTRIB_COPY_FROM, CATALOG_UPDATE_AFTER, NULL));

	/* REINDEX command */
	pgxc_redist_add_reindex(distribState);
}


/*
 * pgxc_redist_build_reindex
 * Add a reindex command if necessary
 */
static void
pgxc_redist_add_reindex(RedistribState *distribState)
{
	Relation rel;

	rel = relation_open(distribState->relid, NoLock);

	/* Build REINDEX command if necessary */
	if (RelationGetIndexList(rel) != NIL)
	{
		distribState->commands = lappend(distribState->commands,
			 makeRedistribCommand(DISTRIB_REINDEX, CATALOG_UPDATE_AFTER, NULL));
	}

	relation_close(rel, NoLock);
}


/*
 * distrib_execute_command
 * Execute a redistribution operation
 */
static void
distrib_execute_command(RedistribState *distribState, RedistribCommand *command)
{
	/* Execute redistribution command */
	switch (command->type)
	{
		case DISTRIB_COPY_TO:
			distrib_copy_to(distribState);
			break;
		case DISTRIB_COPY_FROM:
			distrib_copy_from(distribState, command->execNodes);
			break;
		case DISTRIB_TRUNCATE:
			distrib_truncate(distribState, command->execNodes);
			break;
		case DISTRIB_REINDEX:
			distrib_reindex(distribState, command->execNodes);
			break;
		case DISTRIB_DELETE_HASH:
		case DISTRIB_DELETE_HASHMAP:
		case DISTRIB_DELETE_MODULO:
			distrib_delete_hash(distribState, command->execNodes);
			break;
		case DISTRIB_NONE:
		default:
			Assert(0); /* Should not happen */
	}

	GetCurrentCommandId(true);
	CommandCounterIncrement();
}


/*
 * distrib_copy_to
 * Copy all the data of table to be distributed.
 * This data is saved in a tuplestore saved in distribution state.
 * a COPY FROM operation is always done on nodes determined by the locator data
 * in catalogs, explaining why this cannot be done on a subset of nodes. It also
 * insures that no read operations are done on nodes where data is not yet located.
 */
static void
distrib_copy_to(RedistribState *distribState)
{
	Oid			relOid = distribState->relid;
	Relation	rel;
	RemoteCopyOptions *options;
	RemoteCopyState *copyState;
	Tuplestorestate *store; /* Storage of redistributed data */
	TupleDesc		tupdesc;

	/* Fetch necessary data to prepare for the table data acquisition */
	options = makeRemoteCopyOptions();

	/* All the fields are separated by tabs in redistribution */
	options->rco_delim = palloc(2);
	options->rco_delim[0] = COPYOPS_DELIMITER;
	options->rco_delim[1] = '\0';

	copyState = (RemoteCopyState *) palloc0(sizeof(RemoteCopyState));
	copyState->is_from = false;

	/* A sufficient lock level needs to be taken at a higher level */
	rel = relation_open(relOid, NoLock);
	tupdesc = RelationGetDescr(rel);
	RemoteCopyGetRelationLoc(copyState, rel, NIL);
	RemoteCopyBuildStatement(copyState, rel, options, NIL, NIL);

	/* Inform client of operation being done */
	ereport(DEBUG1,
			(errmsg("Copying data for relation \"%s.%s\"",
					get_namespace_name(RelationGetNamespace(rel)),
					RelationGetRelationName(rel))));

	/* Begin the COPY process */
	StartRemoteCopy(copyState);

	/* Create tuplestore storage */
	store = tuplestore_begin_heap(true, false, work_mem);

	/* Then get rows and copy them to the tuplestore used for redistribution */
	copyState->tuplestorestate = store;
	copyState->tuple_desc = tupdesc;
	copyState->remoteCopyType = REMOTE_COPY_TUPLESTORE;
	RemoteCopyBuildExtra(copyState, tupdesc);
	DoRemoteCopyTo(copyState);

	/* Do necessary clean-up */
	FreeRemoteCopyOptions(options);

	/* Lock is maintained until transaction commits */
	relation_close(rel, NoLock);

	/* Save results */
	distribState->store = store;
}


/*
 * PGXCDistribTableCopyFrom
 * Execute commands related to COPY FROM
 * Redistribute all the data of table with a COPY FROM from given tuplestore.
 */
static void
distrib_copy_from(RedistribState *distribState, ExecNodes *exec_nodes)
{
	Oid					relOid = distribState->relid;
	Tuplestorestate	   *store = distribState->store;
	Relation			rel;
	RemoteCopyOptions  *options;
	RemoteCopyState	   *copyState;
	bool				contains_tuple = true;
	TupleTableSlot	   *slot;
	List			   *nodes;
	StringInfoData		line_buf;
	ReduceInfo		   *rinfo;
	Expr			   *reduce_expr;
	ReduceExprState	   *reduce_state;
	ExprContext		   *econtext;
	MemoryContext		old_context;
	ExprDoneCond		done;
	Datum				datum;
	Oid					nodeoid;
	bool				isnull;

	/* Nothing to do if on remote node */
	if (!IsCnMaster())
		return;

	/* Fetch necessary data to prepare for the table data acquisition */
	options = makeRemoteCopyOptions();
	/* All the fields are separated by tabs in redistribution */
	options->rco_delim = palloc(2);
	options->rco_delim[0] = COPYOPS_DELIMITER;
	options->rco_delim[1] = '\0';

	copyState = (RemoteCopyState *) palloc0(sizeof(RemoteCopyState));
	copyState->is_from = true;

	/* A sufficient lock level needs to be taken at a higher level */
	rel = relation_open(relOid, NoLock);
	RemoteCopyGetRelationLoc(copyState, rel, NIL);
	RemoteCopyBuildStatement(copyState, rel, options, NIL, NIL);

	/*
	 * When building COPY FROM command in redistribution list,
	 * use the list of nodes that has been calculated there.
	 * It might be possible that this COPY is done only on a portion of nodes.
	 */
	if (exec_nodes && exec_nodes->nodeids != NIL)
	{
		copyState->exec_nodes->nodeids = list_copy(exec_nodes->nodeids);
		copyState->rel_loc->nodeids = list_copy(exec_nodes->nodeids);
	}

	rinfo = MakeReduceInfoFromLocInfo(copyState->rel_loc, NIL, RelationGetRelid(rel), 1);
	reduce_expr = CreateExprUsingReduceInfo(rinfo);
	reduce_state = ExecInitReduceExpr(reduce_expr);
	econtext = CreateStandaloneExprContext();

	/* Build table slot for this relation */
	slot = MakeSingleTupleTableSlot(RelationGetDescr(rel));
	/* initinalize line buffer for each slot */
	initStringInfo(&line_buf);

	/* Inform client of operation being done */
	ereport(DEBUG1,
			(errmsg("Redistributing data for relation \"%s.%s\"",
					get_namespace_name(RelationGetNamespace(rel)),
					RelationGetRelationName(rel))));

	/* Begin redistribution on remote nodes */
	StartRemoteCopy(copyState);

	econtext->ecxt_scantuple = slot;
	/* Transform each tuple stored into a COPY message and send it to remote nodes */
	while (contains_tuple)
	{
		ExecClearTuple(slot);

		/* Get tuple slot from the tuplestore */
		contains_tuple = tuplestore_gettupleslot(store, true, false, slot);
		if (!contains_tuple)
			break;

		/* Make sure the tuple is fully deconstructed */
		slot_getallattrs(slot);

		/* Build message to be sent to Datanodes */
		CopyOps_BuildOneRowTo(slot->tts_tupleDescriptor, slot->tts_values, slot->tts_isnull, &line_buf);

		nodes = NIL;
		old_context = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
		MemoryContextReset(CurrentMemoryContext);
		for(;;)
		{
			datum = ExecEvalReduceExpr(reduce_state, econtext, &isnull, &done);
			if (done == ExprEndResult)
			{
				break;
			}else if(isnull)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("ReduceExpr return a null value")));
			}else
			{
				nodeoid = DatumGetObjectId(datum);
				if (nodeoid == PGXCNodeOid)
				{
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("ReduceExpr return a myself Oid")));
				}else
				{
					nodes = lappend_oid(nodes, nodeoid);
				}
			}
		}
		MemoryContextSwitchTo(old_context);
		if (nodes == NIL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("ReduceExpr return an empty Oid list")));
		}

		/* Process data to Datanodes */
		DoRemoteCopyFrom(copyState, &line_buf, nodes);
	}

	pfree(line_buf.data);
	ExecDropSingleTupleTableSlot(slot);
	FreeExprContext(econtext, true);

	/* Finish the redistribution process */
	EndRemoteCopy(copyState);

	/* Lock is maintained until transaction commits */
	relation_close(rel, NoLock);
}


/*
 * distrib_truncate
 * Truncate all the data of specified table.
 * This is used as a second step of online data redistribution.
 */
static void
distrib_truncate(RedistribState *distribState, ExecNodes *exec_nodes)
{
	Relation	rel;
	StringInfo	buf;
	Oid			relOid = distribState->relid;

	/* Nothing to do if on remote node */
	if (!IsCnMaster())
		return;

	/* A sufficient lock level needs to be taken at a higher level */
	rel = relation_open(relOid, NoLock);

	/* Inform client of operation being done */
	ereport(DEBUG1,
			(errmsg("Truncating data for relation \"%s.%s\"",
					get_namespace_name(RelationGetNamespace(rel)),
					RelationGetRelationName(rel))));

	/* Initialize buffer */
	buf = makeStringInfo();

	/* Build query to clean up table before redistribution */
	appendStringInfo(buf, "TRUNCATE %s.%s",
					 get_namespace_name(RelationGetNamespace(rel)),
					 RelationGetRelationName(rel));

	/*
	 * Lock is maintained until transaction commits,
	 * relation needs also to be closed before effectively launching the query.
	 */
	relation_close(rel, NoLock);

	/* Execute the query */
	distrib_execute_query(buf->data, IsTempTable(relOid), exec_nodes);

	/* Clean buffers */
	pfree(buf->data);
	pfree(buf);
}


/*
 * distrib_reindex
 * Reindex the table that has been redistributed
 */
void
distrib_reindex(RedistribState *distribState, ExecNodes *exec_nodes)
{
	Relation	rel;
	StringInfo	buf;
	Oid			relOid = distribState->relid;

	/* Nothing to do if on remote node */
	if (!IsCnMaster())
		return;

	/* A sufficient lock level needs to be taken at a higher level */
	rel = relation_open(relOid, NoLock);

	/* Inform client of operation being done */
	ereport(DEBUG1,
			(errmsg("Reindexing relation \"%s.%s\"",
					get_namespace_name(RelationGetNamespace(rel)),
					RelationGetRelationName(rel))));

	/* Initialize buffer */
	buf = makeStringInfo();

	/* Generate the query */
	appendStringInfo(buf, "REINDEX TABLE %s.%s",
					 get_namespace_name(RelationGetNamespace(rel)),
					 RelationGetRelationName(rel));

	/* Execute the query */
	distrib_execute_query(buf->data, IsTempTable(relOid), exec_nodes);

	/* Clean buffers */
	pfree(buf->data);
	pfree(buf);

	/* Lock is maintained until transaction commits */
	relation_close(rel, NoLock);
}


/*
 * distrib_delete_hash
 * Perform a partial tuple deletion of remote tuples not checking the correct hash
 * condition. The new distribution condition is set up in exec_nodes when building
 * the command list.
 */
static void
distrib_delete_hash(RedistribState *distribState, ExecNodes *exec_nodes)
{
	Relation	rel;
	StringInfo	buf;
	Oid			relOid = distribState->relid;
	ListCell   *item;

	/* Nothing to do if on remote node */
	if (!IsCnMaster())
		return;

	/* A sufficient lock level needs to be taken at a higher level */
	rel = relation_open(relOid, NoLock);

	/* Inform client of operation being done */
	ereport(DEBUG1,
			(errmsg("Deleting necessary tuples \"%s.%s\"",
					get_namespace_name(RelationGetNamespace(rel)),
					RelationGetRelationName(rel))));

	/* Initialize buffer */
	buf = makeStringInfo();

	/* Build query to clean up table before redistribution */
	appendStringInfo(buf, "DELETE FROM %s.%s",
					 get_namespace_name(RelationGetNamespace(rel)),
					 RelationGetRelationName(rel));

	/*
	 * Launch the DELETE query to each node as the DELETE depends on
	 * local conditions for each node.
	 */
	foreach(item, exec_nodes->nodeids)
	{
		StringInfo	buf2;
		char	   *hashfuncname, *colname = NULL;
		Oid			hashtype;
		RelationLocInfo *locinfo = RelationGetLocInfo(rel);
		int			nodeid = lfirst_oid(item);
		int			nodepos = 0;
		ExecNodes  *local_exec_nodes = makeNode(ExecNodes);
		TupleDesc	tupDesc = RelationGetDescr(rel);
		Form_pg_attribute attr = tupDesc->attrs;
		ListCell   *item2;

		/* Here the query is launched to a unique node */
		local_exec_nodes->nodeids = lappend_oid(NIL, nodeid);

		/* Get the hash type of relation */
		hashtype = attr[locinfo->partAttrNum - 1].atttypid;

		/* Get function hash name */
		if (locinfo->locatorType == LOCATOR_TYPE_HASH ||
			locinfo->locatorType == LOCATOR_TYPE_HASHMAP)
		{
			hashfuncname = get_compute_hash_function(hashtype);
		}else
		{
			Assert(locinfo->locatorType == LOCATOR_TYPE_MODULO);
			hashfuncname = NULL;
		}

		/* Get distribution column name */
		if (IsRelationDistributedByValue(locinfo))
			colname = GetRelationDistribColumn(locinfo);
		else
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("Incorrect redistribution operation")));

		/*
		 * Find the correct node position in node list of locator information.
		 * So scan the node list and fetch the position of node.
		 */
		foreach(item2, locinfo->nodeids)
		{
			if (lfirst_oid(item2) == nodeid)
				break;
			nodepos++;
		}

		/*
		 * Then build the WHERE clause for deletion.
		 * The condition that allows to keep the tuples on remote nodes
		 * is of the type "RemoteNodeNumber != abs(hash_func(dis_col)) % NumDatanodes".
		 * the remote Datanode has no knowledge of its position in cluster so this
		 * number needs to be compiled locally on Coordinator.
		 * Taking the absolute value is necessary as hash may return a negative value.
		 * For hash distributions a condition with correct hash function is used.
		 * For modulo distribution, well we might need a hash function call but not
		 * all the time, this is determined implicitely by get_compute_hash_function.
		 */
		buf2 = makeStringInfo();
		if (hashfuncname)
		{
			/* Lets leave NULLs on the first node and delete from the rest */
			if (nodepos != 0)
				appendStringInfo(buf2, "%s WHERE %s IS NULL OR abs(%s(%s)) %% %d != %d",
								buf->data, colname, hashfuncname, colname,
								list_length(locinfo->nodeids), nodepos);
			else
				appendStringInfo(buf2, "%s WHERE abs(%s(%s)) %% %d != %d",
								buf->data, hashfuncname, colname,
								list_length(locinfo->nodeids), nodepos);
		}
		else
		{
			/* Lets leave NULLs on the first node and delete from the rest */
			if (nodepos != 0)
				appendStringInfo(buf2, "%s WHERE %s IS NULL OR abs(%s) %% %d != %d",
								buf->data, colname, colname,
								list_length(locinfo->nodeids), nodepos);
			else
				appendStringInfo(buf2, "%s WHERE abs(%s) %% %d != %d",
								buf->data, colname,
								list_length(locinfo->nodeids), nodepos);
		}

		/* Then launch this single query */
		distrib_execute_query(buf2->data, IsTempTable(relOid), local_exec_nodes);

		FreeExecNodes(&local_exec_nodes);
		pfree(buf2->data);
		pfree(buf2);
	}

	relation_close(rel, NoLock);

	/* Clean buffers */
	pfree(buf->data);
	pfree(buf);
}


/*
 * makeRedistribState
 * Build a distribution state operator
 */
RedistribState *
makeRedistribState(Oid relOid)
{
	RedistribState *res = (RedistribState *) palloc(sizeof(RedistribState));
	res->relid = relOid;
	res->commands = NIL;
	res->store = NULL;
	res->shadowRelid = InvalidOid;
	res->createShadowRel = false;
	res->canReduce = false;
	res->oldLocInfo = NULL;
	res->newLocInfo = NULL;
	return res;
}


/*
 * FreeRedistribState
 * Free given distribution state
 */
void
FreeRedistribState(RedistribState *state)
{
	ListCell *item;

	/* Leave if nothing to do */
	if (!state)
		return;

	foreach(item, state->commands)
		FreeRedistribCommand((RedistribCommand *) lfirst(item));
	if (list_length(state->commands) > 0)
		list_free(state->commands);
	if (state->store)
		tuplestore_clear(state->store);

	FreeRelationLocInfo(state->oldLocInfo);
	FreeRelationLocInfo(state->newLocInfo);

}

/*
 * makeRedistribCommand
 * Build a distribution command
 */
RedistribCommand *
makeRedistribCommand(RedistribOperation type, RedistribCatalog updateState, ExecNodes *nodes)
{
	RedistribCommand *res = (RedistribCommand *) palloc0(sizeof(RedistribCommand));
	res->type = type;
	res->updateState = updateState;
	res->execNodes = nodes;
	return res;
}

/*
 * FreeRedistribCommand
 * Free given distribution command
 */
void
FreeRedistribCommand(RedistribCommand *command)
{
	ExecNodes *nodes;
	/* Leave if nothing to do */
	if (!command)
		return;
	nodes = command->execNodes;

	if (nodes)
		FreeExecNodes(&nodes);
	pfree(command);
}

/*
 * distrib_execute_query
 * Execute single raw query on given list of nodes
 */
static void
distrib_execute_query(char *sql, bool is_temp, ExecNodes *exec_nodes)
{
	RemoteQuery *step = makeNode(RemoteQuery);
	step->combine_type = COMBINE_TYPE_SAME;
	step->exec_nodes = exec_nodes;
	step->sql_statement = pstrdup(sql);
	step->force_autocommit = false;

	/* Redistribution operations only concern Datanodes */
	step->exec_type = EXEC_ON_DATANODES;
	step->is_temp = is_temp;
	(void) ExecInterXactUtility(step, GetCurrentInterXactState());
	pfree(step->sql_statement);
	pfree(step);

	/* Be sure to advance the command counter after the last command */
	CommandCounterIncrement();
}

#ifdef ADB
/*
 * create shadow table
 * reduce souce relation data to shadow relation
 *
 */
void
distrib_build_shadow_relation(RedistribState *distribState
							, RedistribOperation commandType)
{
	StringInfoData msg;
	List *remoteList = NIL;
	List *nodeOids = NIL;
	List *redistcopylist = NIL;
	List *tableOIDs = NIL;
	List *childOids = NIL;
	ListCell *lc;
	Relation childRel;
	Oid reltablespace;
	Oid relnamespace;
	Oid preferred;
	Oid childOID;
	int flag;
	char relpersistence = RELPERSISTENCE_TEMP;
	char masterLocatorType;
	char shadowLocatorType;
	char *childRelname;
	char *childNspname;
	AuxiliaryRelCopy *redistcopy;

	initStringInfo(&msg);
	nodeOids = distrib_get_remote_reduce_nodelist(distribState->oldLocInfo
							, distribState->newLocInfo, commandType);

	tableOIDs = find_all_inheritors(distribState->relid, NoLock, NULL);
	flag = EXEC_CLUSTER_FLAG_NEED_REDUCE | EXEC_CLUSTER_FLAG_NEED_SELF_REDUCE;

	foreach(lc, tableOIDs)
	{
		resetStringInfo(&msg);
		ClusterTocSetCustomFun(&msg, ClusterRedistributeRelation);
		remoteList = ExecClusterCustomFunction(nodeOids, &msg, flag);
		childOID = lfirst_oid(lc);
		childOids = find_all_inheritors(childOID, AccessExclusiveLock, NULL);
		childRel = heap_open(childOID, AccessExclusiveLock);
		relnamespace = childRel->rd_rel->relnamespace;
		reltablespace = childRel->rd_rel->reltablespace;
		relpersistence = childRel->rd_rel->relpersistence;

		Assert((relpersistence != RELPERSISTENCE_TEMP));

		if (list_length(childOids) > 1
			&& childRel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		{
			heap_close(childRel, AccessExclusiveLock);
			continue;
		}
		childRelname = get_rel_name(childOID);
		childNspname = get_namespace_name(relnamespace);
		ereport(DEBUG1,
			(errmsg("create shadow relation for relation \"%s.%s\""
					, childNspname, childRelname)));
		resetStringInfo(&msg);
		appendStringInfoChar(&msg, REMOTE_KEY_CREATE_SHADOW_TABLE);
		save_node_string(&msg, childRelname);
		save_node_string(&msg, childNspname);
		appendBinaryStringInfo(&msg, (char *)&reltablespace, sizeof(reltablespace));
		appendBinaryStringInfo(&msg, (char *)&relpersistence, sizeof(relpersistence));
		PQNputCopyData(remoteList, msg.data, msg.len);
		WaitExecutorEndMessage(remoteList);

		GetCurrentCommandId(true);
		CommandCounterIncrement();

		ereport(DEBUG1,
				(errmsg("reduce source relation \"%s.%s\" data to shadow relation",
						childNspname, childRelname)));

		resetStringInfo(&msg);
		masterLocatorType = distribState->oldLocInfo->locatorType;
		shadowLocatorType = distribState->newLocInfo->locatorType;
		preferred = get_preferred_nodeoid(distribState->oldLocInfo->nodeids);
		redistcopy = MakeShadowRelCopyInfoFromMaster(childRel, distribState->newLocInfo, 0);
		redistcopylist = lappend(redistcopylist, redistcopy);

		appendStringInfoChar(&msg, REMOTE_KEY_REDIST_SHADOW_DATA);
		saveNode(&msg, (const Node *)nodeOids);
		save_node_string(&msg, childRelname);
		save_node_string(&msg, childNspname);
		appendBinaryStringInfo(&msg, (char *)&masterLocatorType, sizeof(masterLocatorType));
		appendBinaryStringInfo(&msg, (char *)&shadowLocatorType, sizeof(shadowLocatorType));
		appendBinaryStringInfo(&msg, (char *)&preferred, sizeof(preferred));
		SerializeAuxRelCopyInfo(&msg, redistcopylist);
		PQNputCopyData(remoteList, msg.data, msg.len);
		WaitExecutorEndMessage(remoteList);

		heap_close(childRel, NoLock);

		GetCurrentCommandId(true);
		CommandCounterIncrement();

		if (remoteList)
		 {
			ListCell *lc;
			foreach(lc, remoteList)
				PQputCopyEnd(lfirst(lc), NULL);
			PQNListExecFinish(remoteList, NULL, &PQNDefaultHookFunctions, true);
			list_free(remoteList);
		 }

		pfree(childRelname);
		pfree(childNspname);
	}

	list_free(tableOIDs);
	distribState->createShadowRel = true;

	list_free(nodeOids);
	pfree(msg.data);
}

static List *
MakeMainRelTargetForShadow(Relation mainRel, Index relid, bool targetEntry)
{
	Form_pg_attribute	main_attr;
	TupleDesc			main_desc = RelationGetDescr(mainRel);
	Var				   *var;
	TargetEntry		   *te;
	List			   *result = NIL;
	int					anum = 0;
	int					i;
	//char			   *attname;

	for(i=0;i<main_desc->natts;++i)
	{
		main_attr = TupleDescAttr(main_desc, i);
		if (main_attr->attisdropped)
			continue;

		++anum;
		//attname = NameStr(main_attr->attname);

		var = makeVar(relid, main_attr->attnum, main_attr->atttypid, main_attr->atttypmod
						, main_attr->attcollation, 0);
		if (targetEntry)
		{
			te = makeTargetEntry((Expr*)var, (AttrNumber)anum
									, pstrdup(NameStr(main_attr->attname)), false);
			result = lappend(result, te);
		}else
		{
			result = lappend(result, var);
		}
	}

	return result;
}

static AuxiliaryRelCopy *
MakeShadowRelCopyInfoFromMaster(Relation masterRel
								, RelationLocInfo *newLocInfo
								, int shadowId)
{
	AuxiliaryRelCopy *redist_copy;
	ReduceInfo		 *rinfo;

	redist_copy = palloc0(sizeof(*redist_copy));
	redist_copy->schemaname = get_namespace_name(RelationGetNamespace(masterRel));
	redist_copy->relname = pstrdup(RelationGetRelationName(masterRel));
	redist_copy->targetList = MakeMainRelTargetForShadow(masterRel, 1, true);

	rinfo = MakeReduceInfoFromLocInfo(newLocInfo, NIL, RelationGetRelid(masterRel), 1);
	redist_copy->reduce = CreateExprUsingReduceInfo(rinfo);
	redist_copy->id = shadowId;

	return redist_copy;
}

static void
DoReduceDataForShadowRel(Relation masterRel,
						 Relation shadowRel,
						 List * rnodes,
						 AuxiliaryRelCopy * redistcopy,
						 char masterLocatorType,
						 char shadowLocatorType,
						 Oid preferred)
{
	MemoryContext	shadow_context;
	MemoryContext	old_context;
	TupleDesc		scan_desc;
	TupleDesc		result_desc;
	ShadowReduceState state;
	bool			needReduce = true;

	Assert(masterRel && shadowRel);
	Assert(redistcopy);
	Assert(list_length(rnodes) > 0);

	PushActiveSnapshot(GetTransactionSnapshot());

	shadow_context = AllocSetContextCreate(CurrentMemoryContext,
											"DoReduceDataForShadowRel",
											ALLOCSET_DEFAULT_SIZES);
	old_context = MemoryContextSwitchTo(shadow_context);

	scan_desc = RelationGetDescr(masterRel);
	state.redist_currentRelation = masterRel;
	state.redist_currentScanDesc = heap_beginscan(masterRel,
											   GetActiveSnapshot(),
											   0, NULL);
	state.redist_ScanTupleSlot = MakeSingleTupleTableSlot(scan_desc);
	result_desc = RelationGetDescr(shadowRel);

	state.redist_ResultTupleSlot = MakeSingleTupleTableSlot(result_desc);
	state.redist_ExprContext = CreateStandaloneExprContext();
	state.redist_ProjInfo = ExecBuildProjectionInfo(redistcopy->targetList,
													state.redist_ExprContext,
													state.redist_ResultTupleSlot,
													NULL,
													scan_desc);

	needReduce = distrib_local_node_need_reduce(masterLocatorType, shadowLocatorType
				, preferred, PGXCNodeOid);
	if (!needReduce)
	{
		ClusterCopyFromReduce(shadowRel,
							redistcopy->reduce,
							rnodes,
							redistcopy->id,
							false,
							NextRowForDistribScanNone,
							&state);
	}
	else
		ClusterCopyFromReduce(shadowRel,
							redistcopy->reduce,
							rnodes,
							redistcopy->id,
							false,
							NextRowForPadding,
							&state);

	ReScanExprContext(state.redist_ExprContext);

	FreeExprContext(state.redist_ExprContext, true);
	ExecDropSingleTupleTableSlot(state.redist_ScanTupleSlot);
	ExecDropSingleTupleTableSlot(state.redist_ResultTupleSlot);
	if (state.redist_currentScanDesc != NULL)
		heap_endscan(state.redist_currentScanDesc);

	MemoryContextSwitchTo(old_context);
	MemoryContextDelete(shadow_context);

	PopActiveSnapshot();
}

void distrib_rewrite_catalog_swap_file(RedistribState *distribState
							, RedistribOperation commandType
							, AlterTableCmd *cmd
							, LOCKMODE lockmode)
{
	StringInfoData msg;
	List *remoteList = NIL;
	List *dnNodeOids = NIL;
	List *cnNodeOids = NIL;
	List *nodeOids = NIL;
	List *tableOIDs = NIL;
	List *childOids = NIL;
	ListCell *lc;
	Relation childRel;
	Oid relnamespace;
	Oid reltablespace;
	Oid childOID;
	//int flag;
	char relpersistence = RELPERSISTENCE_TEMP;
	char *childRelname;
	char *childNspname;

	Assert(commandType == DISTRIB_COPY_FROM);

	initStringInfo(&msg);
	cnNodeOids = GetAllCnIDL(false);
	dnNodeOids = distrib_get_remote_reduce_nodelist(distribState->oldLocInfo
						, distribState->newLocInfo, commandType);
	nodeOids = list_copy(cnNodeOids);

	foreach(lc, dnNodeOids)
	{
		nodeOids = list_append_unique_oid(nodeOids, lfirst_oid(lc));
	}

	/* rewrite the catatlog */
	resetStringInfo(&msg);
	tableOIDs = find_all_inheritors(distribState->relid, AccessExclusiveLock, NULL);
	if (list_length(tableOIDs) > 1)
	{
		ClusterTocSetCustomFun(&msg, ClusterRedistributeRelation);
		//flag = EXEC_CLUSTER_FLAG_NEED_REDUCE;
		remoteList = ExecClusterCustomFunction(nodeOids, &msg, 0);

		foreach(lc, tableOIDs)
		{
			childOID = lfirst_oid(lc);
			childOids = find_all_inheritors(childOID, AccessExclusiveLock, NULL);
			childRel = heap_open(childOID, AccessExclusiveLock);
			childRelname = get_rel_name(childOID);

			relnamespace = RelationGetNamespace(childRel);
			reltablespace = childRel->rd_rel->reltablespace;
			relpersistence = childRel->rd_rel->relpersistence;
			childNspname = get_namespace_name(relnamespace);

			if (childOID != distribState->relid)
			{
				ereport(DEBUG1,
						(errmsg("rewrite system catalogs for relation \"%s.%s\""
						, childNspname, childRelname)));
				DistribRewriteCatalogs(cmd, childOID, lockmode);

				resetStringInfo(&msg);
				appendStringInfoChar(&msg, REMOTE_KEY_REWRITE_CATALOG_TABLE);
				appendBinaryStringInfo(&msg, (char *)&lockmode, sizeof(lockmode));
				save_node_string(&msg, childRelname);
				save_node_string(&msg, childNspname);
				saveNode(&msg, (Node *)cmd);
				PQNputCopyData(remoteList, msg.data, msg.len);
				WaitExecutorEndMessage(remoteList);

				GetCurrentCommandId(true);
				CommandCounterIncrement();
			}

			heap_close(childRel, NoLock);
			pfree(childRelname);
			pfree(childNspname);
		}

		if (remoteList)
		{
			ListCell *lc;

			foreach(lc, remoteList)
				PQputCopyEnd(lfirst(lc), NULL);

			PQNListExecFinish(remoteList, NULL, &PQNDefaultHookFunctions, true);
			list_free(remoteList);
		}
	}

	/* swap source relation file with shadow relation file */
	resetStringInfo(&msg);
	ClusterTocSetCustomFun(&msg, ClusterRedistributeRelation);
	//flag = EXEC_CLUSTER_FLAG_NEED_REDUCE;
	remoteList = ExecClusterCustomFunction(dnNodeOids, &msg, 0);

	tableOIDs = find_all_inheritors(distribState->relid, AccessExclusiveLock, NULL);
	foreach(lc, tableOIDs)
	{
		childOID = lfirst_oid(lc);
		childOids = find_all_inheritors(childOID, AccessExclusiveLock, NULL);
		childRel = heap_open(childOID, AccessExclusiveLock);
		childRelname = get_rel_name(childOID);

		relnamespace = RelationGetNamespace(childRel);
		reltablespace = childRel->rd_rel->reltablespace;
		relpersistence = childRel->rd_rel->relpersistence;
		childNspname = get_namespace_name(relnamespace);

		if (list_length(childOids) > 1
				&& childRel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		{
			heap_close(childRel, NoLock);
			pfree(childRelname);
			pfree(childNspname);
			continue;
		}

		ereport(DEBUG1,
			(errmsg("swap source relation \"%s.%s\" file with shadow relation file"
				, childNspname, childRelname)));
		resetStringInfo(&msg);
		appendStringInfoChar(&msg, REMOTE_KEY_SWAP_SHADOW_SOURCE_TABLE);
		save_node_string(&msg, childRelname);
		save_node_string(&msg, childNspname);
		appendBinaryStringInfo(&msg, (char *)&reltablespace, sizeof(reltablespace));
		appendBinaryStringInfo(&msg, (char *)&relpersistence, sizeof(relpersistence));
		PQNputCopyData(remoteList, msg.data, msg.len);
		WaitExecutorEndMessage(remoteList);

		heap_close(childRel, NoLock);
		pfree(childRelname);
		pfree(childNspname);

		GetCurrentCommandId(true);
		CommandCounterIncrement();
	}

	if (remoteList)
	{
		ListCell *lc;

		foreach(lc, remoteList)
			PQputCopyEnd(lfirst(lc), NULL);

		PQNListExecFinish(remoteList, NULL, &PQNDefaultHookFunctions, true);
		list_free(remoteList);
	}

	list_free(tableOIDs);
	list_free(cnNodeOids);
	list_free(dnNodeOids);
	list_free(nodeOids);
	pfree(msg.data);
}

void
ClusterRedistributeRelation(StringInfo msg)
{
	SimpleNextCopyFromNewFE((SimpleCopyDataFunction)process_distrib_cmd, NULL);
}

/*
* check the table can use reduce method to redistribute the data
*/
bool
distrib_can_use_reduce(Relation rel
					, RelationLocInfo *oldLocInfo
					, RelationLocInfo *newLocInfo
					, List *subCmds)
{
	ListCell   *item;

	if (!enable_cluster_plan)
		return false;

	if (!oldLocInfo || !newLocInfo)
		return false;

	/* ordinary table */
	if (rel->rd_rel->relkind != RELKIND_RELATION
		&& rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
		return false;

	/* regular table */
	if (rel->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT)
		return false;

	if (!subCmds)
		return false;

	foreach(item, subCmds)
	{
		AlterTableCmd *cmd = (AlterTableCmd *) lfirst(item);
		if (cmd->subtype != AT_DistributeBy
			&& cmd->subtype != AT_SubCluster
			&& cmd->subtype != AT_AddNodeList
			&& cmd->subtype != AT_DeleteNodeList)
			return false;
	}

	return true;
}

static List *
distrib_get_remote_reduce_nodelist(RelationLocInfo *oldLocInfo
							, RelationLocInfo *newLocInfo
							, RedistribOperation commandType)
{
	List *nodeOids = NIL;
	ListCell *lc;

	Assert(oldLocInfo && newLocInfo);

	nodeOids = list_copy(oldLocInfo->nodeids);

	foreach(lc, newLocInfo->nodeids)
	{
		nodeOids = list_append_unique_oid(nodeOids, lfirst_oid(lc));
	}

	return nodeOids;
}

static TupleTableSlot *
NextRowForDistribScanNone(CopyState cstate, ExprContext * context, void *data)
{
	return NULL;
}

static int process_distrib_cmd(void *context, const char *data, int len)
{
	char mtype;
	char masterLocatorType;
	char shadowLocatorType;
	char relpersistence;
	char *masterRelname;
	char *nspName;
	char shadowRelName[64];
	bool is_system_catalog = false;
	bool swap_toast_by_content = false;
	Oid preferred;
	Oid masterRelid;
	Oid shadowRelid;
	Oid OIDNewHeap;
	Oid relnamespace;
	Oid reltablespace;
	LOCKMODE lockmode;
	List *rnodes = NIL;
	List *redistcopylist = NIL;
	AuxiliaryRelCopy *redistcopy = NULL;
	StringInfoData buf;
	Relation masterRel;
	Relation shadowRel;
	Relation relRelation;
	AlterTableCmd *cmd;
	HeapTuple shadowTuple;

	buf.data = (char*)data;
	buf.maxlen = buf.len = len;
	buf.cursor = 0;

	mtype = pq_getmsgbyte(&buf);
	switch(mtype)
	{
		case REMOTE_KEY_CREATE_SHADOW_TABLE:
			/* create shadow relation */
			masterRelname = load_node_string(&buf, false);
			nspName = load_node_string(&buf, false);
			pq_copymsgbytes(&buf, (char *)&(reltablespace), sizeof(reltablespace));
			pq_copymsgbytes(&buf, (char *)&(relpersistence), sizeof(relpersistence));

			relnamespace = get_namespaceid(nspName);
			masterRelid = get_relname_relid(masterRelname, relnamespace);

			ereport(DEBUG1,
				(errmsg("create shadow relation \"%s.%s%d\" for relation \"%s.%s\""
						, nspName
						, SHADOW_RELATION_PREFIX, masterRelid
						, nspName
						, masterRelname)));
			Assert((relpersistence != RELPERSISTENCE_TEMP));
			OIDNewHeap = make_new_heap(masterRelid, reltablespace
										, relpersistence, AccessExclusiveLock);

			relRelation = heap_open(RelationRelationId, RowExclusiveLock);
			shadowTuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(OIDNewHeap));
			heap_freetuple(shadowTuple);
			heap_close(relRelation, RowExclusiveLock);

			break;
		case REMOTE_KEY_REDIST_SHADOW_DATA:
			/* reduce source data to shadow relation */
			rnodes = (List*)loadNode(&buf);
			masterRelname = load_node_string(&buf, false);
			nspName = load_node_string(&buf, false);
			pq_copymsgbytes(&buf, (char *)&(masterLocatorType), sizeof(masterLocatorType));
			pq_copymsgbytes(&buf, (char *)&(shadowLocatorType), sizeof(shadowLocatorType));
			pq_copymsgbytes(&buf, (char *)&(preferred), sizeof(preferred));
			redistcopylist = RestoreAuxRelCopyInfo(&buf);

			relnamespace = get_namespaceid(nspName);
			masterRelid = get_relname_relid(masterRelname, relnamespace);
			sprintf(shadowRelName, "%s%d", SHADOW_RELATION_PREFIX, masterRelid);
			shadowRelid = get_relname_relid(shadowRelName, relnamespace);
			redistcopy = (AuxiliaryRelCopy *)linitial(redistcopylist);
			masterRel = heap_open(masterRelid, AccessExclusiveLock);
			shadowRel = heap_open(shadowRelid, AccessExclusiveLock);

			ereport(DEBUG1,
				(errmsg("reduce source relation \"%s.%s\" data to shadow relation\"%s.%s\""
					, nspName
					, RelationGetRelationName(masterRel)
					, nspName
					, RelationGetRelationName(shadowRel))));

			PG_TRY();
			{
				DoReduceDataForShadowRel(masterRel,
									shadowRel,
									rnodes,
									redistcopy,
									masterLocatorType,
									shadowLocatorType,
									preferred);
			}PG_CATCH();
			{
				heap_close(masterRel, AccessExclusiveLock);
				heap_close(shadowRel, AccessExclusiveLock);

				PG_RE_THROW();
			}PG_END_TRY();

			heap_close(masterRel, AccessExclusiveLock);
			heap_close(shadowRel, AccessExclusiveLock);
			break;
		case REMOTE_KEY_SWAP_SHADOW_SOURCE_TABLE:
			/* swap shadow relation file with source relation file */
			masterRelname = load_node_string(&buf, false);
			nspName = load_node_string(&buf, false);
			pq_copymsgbytes(&buf, (char *)&(reltablespace), sizeof(reltablespace));
			pq_copymsgbytes(&buf, (char *)&(relpersistence), sizeof(relpersistence));

			relnamespace = get_namespaceid(nspName);
			masterRelid = get_relname_relid(masterRelname, relnamespace);
			sprintf(shadowRelName, "%s%d", SHADOW_RELATION_PREFIX, masterRelid);
			shadowRelid = get_relname_relid(shadowRelName, relnamespace);

			ereport(DEBUG1,
				(errmsg("sawp source relation \"%s.%s\" file with shadow relation \"%s.%s\" file"
						, nspName
						, masterRelname
						, nspName
						, shadowRelName)));

			finish_heap_swap(masterRelid, shadowRelid, is_system_catalog,
							swap_toast_by_content, false, true,
							RecentXmin, ReadNextMultiXactId(), relpersistence);
			break;
		case REMOTE_KEY_REWRITE_CATALOG_TABLE:
			/* rewrite the table catalog */
			pq_copymsgbytes(&buf, (char *)&(lockmode), sizeof(lockmode));
			masterRelname = load_node_string(&buf, false);
			nspName = load_node_string(&buf, false);
			relnamespace = get_namespaceid(nspName);
			cmd = (AlterTableCmd *)loadNode(&buf);

			masterRelid = get_relname_relid(masterRelname, relnamespace);
			masterRel = heap_open(masterRelid, AccessExclusiveLock);

			DistribRewriteCatalogs(cmd, masterRelid, lockmode);
			heap_close(masterRel, AccessExclusiveLock);
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					errmsg("unexpected cluster command 0x%02X during COPY from coordinator",
							mtype)));
			break;
	}
	GetCurrentCommandId(true);
	CommandCounterIncrement();
	put_executor_end_msg(true);

	return 0;
}

static bool
distrib_local_node_need_reduce(char masterLocatorType, char shadowLocatorType
				, Oid preferred, Oid PGXCNodeOid)
{
	if (masterLocatorType != LOCATOR_TYPE_REPLICATED)
		return true;
	if (preferred == PGXCNodeOid)
		return true;

	return false;
}

static void WaitExecutorEndMessage(List *remotes)
{
	ListCell *lc;
	PQNFlush(remotes, true);
	foreach (lc, remotes)
		wait_executor_end_msg(lfirst(lc));
}
#endif /* ADB*/
