/*-------------------------------------------------------------------------
 *
 * inter-comm.c
 *	  Internode query routines
 *
 *
 * Portions Copyright (c) 2016-2017, ADB Development Group
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/intercomm/inter-query.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/relscan.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "agtm/agtm.h"
#include "catalog/pgxc_node.h"
#include "commands/prepare.h"
#include "executor/clusterReceiver.h"
#include "executor/executor.h"
#include "intercomm/inter-comm.h"
#include "libpq/libpq-fe.h"
#include "libpq/libpq-int.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_coerce.h"
#include "pgxc/locator.h"
#include "pgxc/pgxcnode.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"

typedef struct RemoteQueryContext
{
	PlanState		   *ps;
	TupleTableSlot	   *slot;
} RemoteQueryContext;

static List *RewriteExecNodes(RemoteQueryState *planstate, ExecNodes *exec_nodes);
static TupleTableSlot *InterXactQuery(InterXactState state, RemoteQueryState *node, TupleTableSlot *slot);
static bool HandleStartRemoteQuery(NodeHandle *handle, RemoteQueryState *node);
static PGconn *HandleGetPGconn(void *handle);
static bool RemoteQueryFinishHook(void *context, struct pg_conn *conn, PQNHookFuncType type, ...);

static List *
RewriteExecNodes(RemoteQueryState *planstate, ExecNodes *exec_nodes)
{
	ExprState	   *estate;
	bool			isnull;
	Datum			partvalue;
	int				nelems, idx;
	ListCell	   *lc;
	Datum		   *en_expr_values;
	bool		   *en_expr_nulls;
	Oid			   *en_expr_types;
	Node		   *en_expr_node;
	List		   *result = NIL;
	RelationLocInfo*rel_loc = NULL;
	Oid			   *argtypes = NULL;
	int				nargs;

	if (!exec_nodes || !exec_nodes->en_expr)
		return NIL;

	rel_loc = GetRelationLocInfo(exec_nodes->en_relid);
	Assert(rel_loc);

	/*
	 * en_expr is set by pgxc_set_en_expr only for distributed
	 * relations while planning DMLs, hence a select for update
	 * on a replicated table here is an assertion
	 */
	Assert(!(exec_nodes->accesstype == RELATION_ACCESS_READ_FOR_UPDATE &&
			IsRelationReplicated(rel_loc)));

	nelems = list_length(exec_nodes->en_expr);
	en_expr_values = (Datum *) palloc0(sizeof(Datum) * nelems);
	en_expr_nulls = (bool *) palloc0(sizeof(bool) * nelems);
	en_expr_types = (Oid *) palloc0(sizeof(Oid) * nelems);

	if (IsRelationDistributedByUserDefined(rel_loc))
	{
		Assert(OidIsValid(rel_loc->funcid));
		Assert(rel_loc->funcAttrNums);
		(void) get_func_signature(rel_loc->funcid, &argtypes, &nargs);
		Assert(nelems == nargs);
	}

	idx = 0;
	foreach (lc, exec_nodes->en_expr)
	{
		en_expr_node = (Node *)lfirst(lc);
		if (IsRelationDistributedByUserDefined(rel_loc) && en_expr_node)
		{
			en_expr_node = coerce_to_target_type(NULL, en_expr_node,
												exprType(en_expr_node),
												argtypes[idx],
												-1,
												COERCION_IMPLICIT,
												COERCE_IMPLICIT_CAST,
												-1);
		}
		if (en_expr_node)
		{
			estate = ExecInitExpr((Expr*)en_expr_node, (PlanState *) planstate);
			partvalue = ExecEvalExpr(estate,
									 planstate->ss.ps.ps_ExprContext,
									 &isnull,
									 NULL);
			en_expr_values[idx] = isnull ? (Datum)0 : partvalue;
			en_expr_nulls[idx] = isnull;
			en_expr_types[idx] = exprType(en_expr_node);
		} else
		{
			en_expr_values[idx] = (Datum)0;
			en_expr_nulls[idx] = true;
			en_expr_types[idx] = InvalidOid;
		}
		idx++;
	}

	if (argtypes)
		pfree(argtypes);

	result = GetInvolvedNodes(rel_loc, nelems, en_expr_values, en_expr_nulls,
							  en_expr_types, exec_nodes->accesstype);
	pfree(en_expr_values);
	pfree(en_expr_nulls);
	pfree(en_expr_types);
	FreeRelationLocInfo(rel_loc);

	return result;
}

List *
GetRemoteNodeList(RemoteQueryState *planstate, ExecNodes *exec_nodes, RemoteQueryExecType exec_type)
{
	List   *node_list = NIL;

	if (exec_nodes)
	{
		if (exec_nodes->en_expr)
			node_list = RewriteExecNodes(planstate, exec_nodes);
		else
		if (OidIsValid(exec_nodes->en_relid))
		{
			RelationLocInfo	   *rel_loc = GetRelationLocInfo(exec_nodes->en_relid);
			Datum				value = (Datum)0;
			bool				null = true;
			Oid					type = InvalidOid;

			node_list = GetInvolvedNodes(rel_loc, 1, &value, &null, &type, exec_nodes->accesstype);

			/*
			 * en_relid is set only for DMLs, hence a select for update on a
			 * replicated table here is an assertion
			 */
			Assert(!(exec_nodes->accesstype == RELATION_ACCESS_READ_FOR_UPDATE &&
					IsRelationReplicated(rel_loc)));

			FreeRelationLocInfo(rel_loc);
		}
		else
		{
			node_list = list_copy(exec_nodes->nodeids);
		}
	} else
	{
		switch (exec_type)
		{
			case EXEC_ON_COORDS:
				node_list = GetAllCnIds(false);
				break;
			case EXEC_ON_DATANODES:
				node_list = GetAllDnIds(false);
				break;
			case EXEC_ON_ALL_NODES:
				node_list = GetAllNodeIds(false);
				break;
			default:
				Assert(false);
				break;
		}
	}

	return node_list;
}

TupleTableSlot *
StartRemoteQuery(RemoteQueryState *node, TupleTableSlot *slot)
{
	RemoteQuery	   *step;
	List		   *node_list;
	InterXactState	state;
	bool			need_xact_block;

	Assert(node);
	/*
	 * A Postgres-XC node cannot run transactions while in recovery as
	 * this operation needs transaction IDs. This is more a safety guard than anything else.
	 */
	if (RecoveryInProgress())
		elog(ERROR, "cannot run transaction to remote nodes during recovery");

	if (node->conn_count == 0)
		node->connections = NULL;

	state = GetTopInterXactState();
	step = (RemoteQuery *) node->ss.ps.plan;
	if (step->is_temp)
		state->hastmp = true;

	node_list = GetRemoteNodeList(node, step->exec_nodes, step->exec_type);
	state = MakeInterXactState2(state, node_list);

	if (step->force_autocommit || step->read_only)
		need_xact_block = false;
	else
		need_xact_block = true;
	if (need_xact_block)
		state->need_xact_block = true;

	/* save handle list for current RemoteQueryState */
	if (node->cur_handles)
		list_free(node->cur_handles);
	node->cur_handles = list_copy(state->mix_handle->handles);
	node->all_handles = list_concat_unique_ptr(node->all_handles, node->cur_handles);

	return InterXactQuery(state, node, slot);
}

static TupleTableSlot *
InterXactQuery(InterXactState state, RemoteQueryState *node, TupleTableSlot *slot)
{
	NodeMixHandle	   *mix_handle;
	NodeHandle		   *handle;
	NodeHandle		   *pr_handle;
	ListCell		   *lc_handle;
	bool				need_xact_block;
	bool				already_begin;
	GlobalTransactionId	gxid;
	TimestampTz			timestamp = GetCurrentTransactionStartTimestamp();

	Assert(state && node);
	mix_handle = state->mix_handle;
	need_xact_block = state->need_xact_block;
	pr_handle = mix_handle->pr_handle;

	if (need_xact_block)
	{
		gxid = GetCurrentTransactionId();
		agtm_BeginTransaction();
	} else
		gxid = GetCurrentTransactionIdIfAny();

	PG_TRY();
	{
		ExecClearTuple(slot);

		if (pr_handle)
		{
			if (!HandleBegin(state, pr_handle, gxid, timestamp, need_xact_block, &already_begin) ||
				!HandleStartRemoteQuery(pr_handle, node))
			{
				state->block_state |= IBLOCK_ABORT;
				InterXactSaveHandleError(state, pr_handle);
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("%s", state->error->data)));
			}

			/* try to get the first no-null slot */
			if (TupIsNull(slot))
				slot = HandleGetRemoteSlot(pr_handle, slot, node, true);
		}

		foreach (lc_handle, mix_handle->handles)
		{
			handle = (NodeHandle *) lfirst(lc_handle);
			if (handle == pr_handle)
				continue;

			if (!HandleBegin(state, handle, gxid, timestamp, need_xact_block, &already_begin) ||
				!HandleStartRemoteQuery(handle, node))
			{
				state->block_state |= IBLOCK_ABORT;
				InterXactSaveHandleError(state, handle);
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("%s", state->error->data)));
			}
		}
	} PG_CATCH();
	{
		InterXactGC(state);
		PG_RE_THROW();
	} PG_END_TRY();

	return slot;
}

static bool
HandleStartRemoteQuery(NodeHandle *handle, RemoteQueryState *node)
{
	CommandId	cid;
	Snapshot	snapshot;
	RemoteQuery*step;

	Assert(handle && node);
	snapshot = GetActiveSnapshot();
	step = (RemoteQuery *) node->ss.ps.plan;

	/*
	 * mark the current owner of the handle, if the node is another owner,
	 * it is best to cache data from the handle and make it clean for the
	 * new owner.
	 */
	if (handle->node_owner && handle->node_owner != node)
		HandleCache(handle);
	handle->node_owner = node;

	/*
	 * Scan descriptor would be valid and would contain a valid snapshot
	 * in cases when we need to send out of order command id to data node
	 * e.g. in case of a fetch
	 */

	if (node->cursor != NULL &&
		node->cursor[0] != '\0' &&
		node->ss.ss_currentScanDesc != NULL &&
		node->ss.ss_currentScanDesc->rs_snapshot != NULL)
		cid = node->ss.ss_currentScanDesc->rs_snapshot->curcid;
	else
	{
		/*
		 * An insert into a child by selecting form its parent gets translated
		 * into a multi-statement transaction in which first we select from parent
		 * and then insert into child, then select form child and insert into child.
		 * The select from child should not see the just inserted rows.
		 * The command id of the select from child is therefore set to
		 * the command id of the insert-select query saved earlier.
		 * Similarly a WITH query that updates a table in main query
		 * and inserts a row in the same table in the WITH query
		 * needs to make sure that the row inserted by the WITH query does
		 * not get updated by the main query.
		 */
		if (step->exec_nodes &&
			step->exec_nodes->accesstype == RELATION_ACCESS_READ &&
			step->rq_save_command_id)
			cid = node->rqs_cmd_id;
		else
			cid = GetCurrentCommandId(false);
	}

	if (step->statement || step->cursor || node->rqs_num_params)
	{
		/* need to use Extended Query Protocol */
		int		fetch = 0;
		bool	prepared = false;
		bool	send_desc = false;

		if (step->base_tlist != NULL ||
			step->exec_nodes->accesstype == RELATION_ACCESS_READ ||
			step->has_row_marks)
			send_desc = true;

		/* if prepared statement is referenced see if it is already exist */
		if (step->statement)
			prepared = ActivateDatanodeStatementOnNode(step->statement, handle->node_id);
		/*
		 * execute and fetch rows only if they will be consumed
		 * immediately by the sorter
		 */
		if (step->cursor)
			fetch = 1;

		if (!HandleSendQueryExtend(handle,
								   cid,
								   snapshot,
								   prepared ? NULL : step->sql_statement,
								   step->statement,
								   step->cursor,
								   send_desc,
								   fetch,
								   node->rqs_num_params,
								   node->rqs_param_types,
								   NULL,
								   node->paramval_data,
								   node->paramval_len,
								   0,
								   NULL))
			return false;

	} else
	{
		if (!HandleSendQueryTree(handle, cid, snapshot, step->sql_statement, step->sql_node))
			return false;
	}

	return true;
}

static PGconn *
HandleGetPGconn(void *handle)
{
	if (handle)
		return ((NodeHandle *) handle)->node_conn;

	return NULL;
}

TupleTableSlot *
FetchRemoteQuery(RemoteQueryState *node, TupleTableSlot *slot)
{
	RemoteQueryContext	context;
	List			   *handle_list = NIL;

	if (!node || !slot)
		return NULL;

	handle_list = node->cur_handles;
	context.ps = &(node->ss.ps);
	context.slot = slot;

	PQNListExecFinish(handle_list, HandleGetPGconn, RemoteQueryFinishHook, &context, true);

	return slot;
}

TupleTableSlot *
HandleGetRemoteSlot(NodeHandle *handle, TupleTableSlot *slot, RemoteQueryState *node, bool blocking)
{
	RemoteQueryContext context;

	if (!handle || !slot)
		return NULL;

	context.ps = &(node->ss.ps);
	context.slot = slot;
	ExecClearTuple(slot);

	PQNOneExecFinish(handle->node_conn, RemoteQueryFinishHook, &context, blocking);

	return slot;
}

static bool
RemoteQueryFinishHook(void *context, struct pg_conn *conn, PQNHookFuncType type, ...)
{
	va_list args;

	switch(type)
	{
		case PQNHFT_ERROR:
			return PQNEFHNormal(NULL, conn, type);
		case PQNHFT_COPY_OUT_DATA:
			{
				int			len;
				const char *buf;
				va_start(args, type);
				buf = va_arg(args, const char*);
				len = va_arg(args, int);
				if(clusterRecvTuple(((RemoteQueryContext*)context)->slot, buf, len,
									((RemoteQueryContext*)context)->ps, conn))
				{
					va_end(args);
					return true;
				}
				va_end(args);
			}
			break;
		case PQNHFT_COPY_IN_ONLY:
			PQputCopyEnd(conn, NULL);
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
					else if(status == PGRES_COPY_IN)
						PQputCopyEnd(conn, NULL);
				}
				va_end(args);
			}
			break;
	}
	return false;
}

void
CloseRemoteStatement(const char *stmt_name, Oid *nodes, int nnodes)
{
	NodeMixHandle  *mix_handle;
	List		   *oid_list;

	if (!stmt_name)
		return;
	oid_list = OidArraryToList(NULL, nodes, nnodes);
	if (!oid_list)
		return ;

	mix_handle = GetMixedHandles(oid_list, NULL);
	Assert(mix_handle);
	HandleListClose(mix_handle->handles, true, stmt_name);
}

