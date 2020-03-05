/*-------------------------------------------------------------------------
 *
 * execRemote.c
 *
 *	  Functions to execute commands on remote Datanodes
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012, Postgres-XC Development Group
 * Portions Copyright (c) 2014-2017, ADB Development Group
 *
 * IDENTIFICATION
 *	  src/backend/pgxc/pool/execRemote.c
 *
 *-------------------------------------------------------------------------
 */

#include <time.h>

#include "postgres.h"
#include "miscadmin.h"

#include "access/relscan.h"
#include "access/twophase.h"
#include "access/transam.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "agtm/agtm.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_node.h"
#include "commands/prepare.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "libpq/libpq.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_coerce.h"
#include "parser/parse_type.h"
#include "parser/parsetree.h"
#include "pgxc/copyops.h"
#include "pgxc/execRemote.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "pgxc/poolmgr.h"
#include "pgxc/xc_maintenance_mode.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/snapmgr.h"
#include "utils/tuplesort.h"
#ifdef ADB
#include "access/rxact_mgr.h"
#include "executor/clusterReceiver.h"
#include "intercomm/inter-comm.h"
#endif

/*
 * non-FQS UPDATE & DELETE to a replicated table without any primary key or
 * unique key should be prohibited (true) or allowed (false)
 */
bool RequirePKeyForRepTab = true;

typedef struct
{
	xact_callback function;
	void *fparams;
} abort_callback_type;

/*
 * List of PGXCNodeHandle to track readers and writers involved in the
 * current transaction
 */
static abort_callback_type dbcleanup_info = { NULL, NULL };
#if 0
static void close_node_cursors(PGXCNodeHandle **connections, int conn_count, char *cursor);
#endif
static TupleTableSlot * RemoteQueryNext(ScanState *node);
static bool RemoteQueryRecheck(RemoteQueryState *node, TupleTableSlot *slot);

static bool IsReturningDMLOnReplicatedTable(RemoteQuery *rq);
static void SetDataRowForIntParams(JunkFilter *junkfilter,
					   TupleTableSlot *sourceSlot, TupleTableSlot *newSlot,
					   RemoteQueryState *rq_state);
static void pgxc_append_param_val(StringInfo buf, Datum val, Oid valtype);
static void pgxc_append_param_junkval(TupleTableSlot *slot, AttrNumber attno, Oid valtype, StringInfo buf);
static void pgxc_rq_fire_bstriggers(RemoteQueryState *node);
static void pgxc_rq_fire_astriggers(RemoteQueryState *node);

/*
 * Create a structure to store parameters needed to combine responses from
 * multiple connections as well as state information
 */
RemoteQueryState *
CreateRemoteQueryState(int node_count, CombineType combine_type)
{
	RemoteQueryState *rqstate = makeNode(RemoteQueryState);

	rqstate->cur_handles = NIL;
	rqstate->all_handles = NIL;
	rqstate->combine_type = combine_type;
	rqstate->command_complete_count = 0;
	rqstate->command_error_count = 0;
	rqstate->tuple_desc = NULL;
	rqstate->description_count = 0;
	rqstate->query_Done = false;
	rqstate->currentRow.msg = NULL;
	rqstate->currentRow.msglen = 0;
	rqstate->currentRow.msgnode = 0;
	rqstate->tapenodes = NULL;
	rqstate->remoteCopyType = REMOTE_COPY_NONE;
	rqstate->copy_file = NULL;
	rqstate->rqs_cmd_id = FirstCommandId;
	rqstate->rqs_processed = 0;

	return rqstate;
}

/*
 * Parse out row count from the command status response and convert it to integer
 */
static int
parse_row_count(const char *message, size_t len, uint64 *rowcount)
{
	int			digits = 0;
	int			pos;

	*rowcount = 0;
	/* skip \0 string terminator */
	for (pos = 0; pos < len - 1; pos++)
	{
		if (message[pos] >= '0' && message[pos] <= '9')
		{
			*rowcount = *rowcount * 10 + message[pos] - '0';
			digits++;
		} else
		{
			*rowcount = 0;
			digits = 0;
		}
	}
	return digits;
}

/*
 * HandleCmdComplete -
 *	combine deparsed sql statements execution results
 *
 * Input parameters:
 *	commandType is dml command type
 *	combineTag is used to combine the completion result
 *	msg_body is execution result needed to combine
 *	len is msg_body size
 */
void
HandleCmdComplete(CmdType commandType, CombineTag *combine, const char *msg_body, size_t len)
{
	int	digits = 0;
	uint64	originrowcount = 0;
	uint64	rowcount = 0;
	uint64	total = 0;

	if (msg_body == NULL)
		return;

	/* if there's nothing in combine, just copy the msg_body */
	if (strlen(combine->data) == 0)
	{
		strcpy(combine->data, msg_body);
		combine->cmdType = commandType;
		return;
	}
	else
	{
		/* commandType is conflict */
		if (combine->cmdType != commandType)
			return;

		/* get the processed row number from msg_body */
		digits = parse_row_count(msg_body, len + 1, &rowcount);
		elog(DEBUG1, "digits is %d\n", digits);
		Assert(digits >= 0);

		/* no need to combine */
		if (digits == 0)
			return;

		/* combine the processed row number */
		parse_row_count(combine->data, strlen(combine->data) + 1, &originrowcount);
		elog(DEBUG1, "originrowcount is %lu, rowcount is %lu\n", originrowcount, rowcount);
		total = originrowcount + rowcount;

	}

	/* output command completion tag */
	switch (commandType)
	{
		case CMD_SELECT:
			strcpy(combine->data, "SELECT");
			break;
		case CMD_INSERT:
			snprintf(combine->data, COMPLETION_TAG_BUFSIZE,
			   "INSERT %u %lu", 0, total);
			break;
		case CMD_UPDATE:
			snprintf(combine->data, COMPLETION_TAG_BUFSIZE,
					 "UPDATE %lu", total);
			break;
		case CMD_DELETE:
			snprintf(combine->data, COMPLETION_TAG_BUFSIZE,
					 "DELETE %lu", total);
			break;
		default:
			strcpy(combine->data, "");
			break;
	}
}

/*
 * Close RemoteQueryState and free allocated memory, if it is not needed
 */
void
CloseRemoteQueryState(RemoteQueryState *rqstate)
{
	if (rqstate)
	{
		list_free(rqstate->cur_handles);
		list_free(rqstate->all_handles);
		rqstate->cur_handles = NIL;
		rqstate->all_handles = NIL;
		if (rqstate->tuple_desc)
		{
			/*
			 * In the case of a remote COPY with tuplestore, RemoteQueryState is not
			 * responsible from freeing the tuple store. This is done at an upper
			 * level once data redistribution is completed.
			 */
			if (rqstate->remoteCopyType != REMOTE_COPY_TUPLESTORE)
				FreeTupleDesc(rqstate->tuple_desc);
		}
#if 0
		if (rqstate->cursor_connections)
			pfree(rqstate->cursor_connections);
#endif
		if (rqstate->tapenodes)
			pfree(rqstate->tapenodes);
		pfree(rqstate);
	}
}

RemoteQueryState *
ExecInitRemoteQuery(RemoteQuery *node, EState *estate, int eflags)
{
	RemoteQueryState   *rqstate;
	TupleDesc			scan_type;

	/* RemoteQuery node is the leaf node in the plan tree, just like seqscan */
	Assert(innerPlan(node) == NULL);
	Assert(outerPlan(node) == NULL);

	rqstate = CreateRemoteQueryState(0, node->combine_type);
	rqstate->ss.ps.ExecProcNode = ExecRemoteQuery;
	rqstate->ss.ps.plan = (Plan *) node;
	rqstate->ss.ps.state = estate;

	/*
	 * Miscellaneous initialisation
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &rqstate->ss.ps);

	/* Initialise child expressions */
	rqstate->ss.ps.qual = ExecInitQual(node->scan.plan.qual, (PlanState *) rqstate);

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_MARK)));

	/* Extract the eflags bits that are relevant for tuplestorestate */
	rqstate->eflags = (eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD));

	/*
	 * We anyways have to support BACKWARD for cache tuples.
	 */
	rqstate->eflags |= (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD);

	/*
	 * tuplestorestate of RemoteQueryState is for two purposes,
	 * one is rescan (see ExecRemoteQueryReScan), the other is cache
	 * (see HandleCacheOrGC)
	 */
	rqstate->tuplestorestate = tuplestore_begin_remoteheap(false, false, work_mem);
	tuplestore_set_eflags(rqstate->tuplestorestate, rqstate->eflags);

	rqstate->eof_underlying = false;

	scan_type = ExecTypeFromTL(node->base_tlist);
	ExecInitScanTupleSlot(estate, &rqstate->ss, scan_type, &TTSOpsMinimalTuple);

	rqstate->iterSlot = ExecInitExtraTupleSlot(estate, scan_type, &TTSOpsMinimalTuple);

	/*
	 * convert slot maybe change descripor when need convert,
	 * so we can not create an fixed slot
	 */
	rqstate->convertSlot = ExecInitExtraTupleSlot(estate, NULL, &TTSOpsMinimalTuple);
	ExecSetSlotDescriptor(rqstate->convertSlot, scan_type);

	/*
	 * convert will be set while the tuple description
	 * is set correctly.
	 */
	rqstate->recvState = (ClusterRecvState *) palloc0(sizeof(ClusterRecvState));
	rqstate->recvState->ps = &rqstate->ss.ps;

	/*
	 * If there are parameters supplied, get them into a form to be sent to the
	 * Datanodes with bind message. We should not have had done this before.
	 */
	SetDataRowForExtParams(estate->es_param_list_info, rqstate);

	/*
	 * Initialize result tuple type and projection info.
	 */
	ExecInitResultTupleSlotTL(&rqstate->ss.ps, &TTSOpsMinimalTuple);
	ExecAssignScanProjectionInfo(&rqstate->ss);

	if (node->reduce_expr)
		rqstate->reduce_state = ExecInitReduceExpr(node->reduce_expr);

	if (node->rq_save_command_id)
	{
		/* Save command id to be used in some special cases */
		rqstate->rqs_cmd_id = GetCurrentCommandId(false);
	}

	return rqstate;
}

/*
 * IsReturningDMLOnReplicatedTable
 *
 * This function returns true if the passed RemoteQuery
 * 1. Operates on a table that is replicated
 * 2. Represents a DML
 * 3. Has a RETURNING clause in it
 *
 * If the passed RemoteQuery has a non null base_tlist
 * means that DML has a RETURNING clause.
 */

static bool
IsReturningDMLOnReplicatedTable(RemoteQuery *rq)
{
	if (IsExecNodesReplicated(rq->exec_nodes) &&
		rq->base_tlist != NULL &&	/* Means DML has RETURNING */
		(rq->exec_nodes->accesstype == RELATION_ACCESS_UPDATE ||
		rq->exec_nodes->accesstype == RELATION_ACCESS_INSERT))
		return true;

	return false;
}

/*
 * ExecRemoteQuery
 * Wrapper around the main RemoteQueryNext() function. This
 * wrapper provides materialization of the result returned by
 * RemoteQueryNext
 */

TupleTableSlot *
ExecRemoteQuery(PlanState *ps)
{
	RemoteQueryState *node = castNode(RemoteQueryState, ps);
	return ExecScan(&(node->ss),
					(ExecScanAccessMtd) RemoteQueryNext,
					(ExecScanRecheckMtd) RemoteQueryRecheck);
}

/*
 * RemoteQueryRecheck -- remote query routine to recheck a tuple in EvalPlanQual
 */
static bool
RemoteQueryRecheck(RemoteQueryState *node, TupleTableSlot *slot)
{
	/*
	 * Note that unlike IndexScan, RemoteQueryScan never use keys in heap_beginscan
	 * (and this is very bad) - so, here we do not check are keys ok or not.
	 */
	return true;
}
/*
 * Execute step of PGXC plan.
 * The step specifies a command to be executed on specified nodes.
 * On first invocation connections to the Datanodes are initialized and
 * command is executed. Further, as well as within subsequent invocations,
 * responses are received until step is completed or there is a tuple to emit.
 * If there is a tuple it is returned, otherwise returned NULL. The NULL result
 * from the function indicates completed step.
 * The function returns at most one tuple per invocation.
 */
static TupleTableSlot *
RemoteQueryNext(ScanState *scan_node)
{
	RemoteQueryState   *node = (RemoteQueryState *)scan_node;
	TupleTableSlot	   *scanslot = scan_node->ss_ScanTupleSlot;
	RemoteQuery		   *rq = (RemoteQuery*) node->ss.ps.plan;
	EState			   *estate = node->ss.ps.state;

	/*
	 * Initialize tuples processed to 0, to make sure we don't re-use the
	 * values from the earlier iteration of RemoteQueryNext(). For an FQS'ed
	 * DML returning query, it may not get updated for subsequent calls.
	 * because there won't be a HandleCommandComplete() call to update this
	 * field.
	 */
	node->rqs_processed = 0;

	if (!node->query_Done)
	{
		/* Fire BEFORE STATEMENT triggers just before the query execution */
		pgxc_rq_fire_bstriggers(node);
		scanslot = StartRemoteQuery(node, scanslot);
		node->query_Done = true;
	} else
		ExecClearTuple(scanslot);

	if (node->update_cursor)
	{
#ifdef ADB
		ereport(ERROR,
				(errmsg("The new version of ADB communication has not yet covered this use case.")));
#endif
#if 0
		PGXCNodeAllHandles *all_dn_handles = get_exec_connections(node, NULL, EXEC_ON_DATANODES);
		close_node_cursors(all_dn_handles->datanode_handles,
						   all_dn_handles->dn_conn_count,
						   node->update_cursor);
		pfree(node->update_cursor);
		node->update_cursor = NULL;
		pfree_pgxc_all_handles(all_dn_handles);
#endif
	} else
	if (TupIsNull(scanslot))
	{
		scanslot = FetchRemoteQuery(node, scanslot);
		node->eof_underlying = TupIsNull(scanslot);
	}

	/*
	 * Now we know the query is successful. Fire AFTER STATEMENT triggers. Make
	 * sure this is the last iteration of the query. If an FQS query has
	 * RETURNING clause, this function can be called multiple times until we
	 * return NULL.
	 */
	if (TupIsNull(scanslot))
		pgxc_rq_fire_astriggers(node);

	/*
	 * If it's an FQSed DML query for which command tag is to be set,
	 * then update estate->es_processed. For other queries, the standard
	 * executer takes care of it; namely, in ExecModifyTable for DML queries
	 * and ExecutePlan for SELECT queries.
	 */
	if (rq->remote_query &&
		rq->remote_query->canSetTag &&
		!rq->rq_params_internal &&
		(rq->remote_query->commandType == CMD_INSERT ||
		 rq->remote_query->commandType == CMD_UPDATE ||
		 rq->remote_query->commandType == CMD_DELETE))
		estate->es_processed += node->rqs_processed;

	return scanslot;
}

/*
 * End the remote query
 */
void
ExecEndRemoteQuery(RemoteQueryState *node)
{
#if 0
	node->current_conn = 0;
	while (node->conn_count > 0)
	{
		int res;
		PGXCNodeHandle *conn = node->connections[node->current_conn];

		/* throw away message */
		if (node->currentRow.msg)
		{
			pfree(node->currentRow.msg);
			node->currentRow.msg = NULL;
		}

		if (conn == NULL)
		{
			node->conn_count--;
			continue;
		}

		/* no data is expected */
		if (conn->state == DN_CONNECTION_STATE_IDLE ||
				conn->state == DN_CONNECTION_STATE_ERROR_FATAL)
		{
			if (node->current_conn < --node->conn_count)
				node->connections[node->current_conn] = node->connections[node->conn_count];
			continue;
		}
		res = handle_response(conn, node);
		if (res == RESPONSE_EOF)
		{
			struct timeval timeout;
			timeout.tv_sec = END_QUERY_TIMEOUT;
			timeout.tv_usec = 0;

			if (pgxc_node_receive(1, &conn, &timeout))
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to read response from Datanodes when ending query")));
		}
	}
#endif

	if (node->tuplestorestate != NULL)
		ExecClearTuple(node->ss.ss_ScanTupleSlot);

	ExecClearTuple(node->iterSlot);
	ExecClearTuple(node->convertSlot);
	freeClusterRecvState(node->recvState);

	/*
	 * Release tuplestore resources
	 */
	if (node->tuplestorestate != NULL)
		tuplestore_end(node->tuplestorestate);
	node->tuplestorestate = NULL;

	/*
	 * If there are active cursors close them
	 */
	if (node->cursor || node->update_cursor)
	{
#if 0
		PGXCNodeAllHandles *all_handles = NULL;
		PGXCNodeHandle    **cur_handles;
		bool bFree = false;
		int nCount;
		int i;

		cur_handles = node->cursor_connections;
		nCount = node->cursor_count;

		for(i=0;i<node->cursor_count;i++)
		{
			if (node->cursor_connections == NULL || node->cursor_connections[i]->sock == -1)
			{
				bFree = true;
				all_handles = get_exec_connections(node, NULL, EXEC_ON_DATANODES);
				cur_handles = all_handles->datanode_handles;
				nCount = all_handles->dn_conn_count;
				break;
			}
		}

		if (node->cursor)
		{
			close_node_cursors(cur_handles, nCount, node->cursor);
			pfree(node->cursor);
			node->cursor = NULL;
		}

		if (node->update_cursor)
		{
			close_node_cursors(cur_handles, nCount, node->update_cursor);
			pfree(node->update_cursor);
			node->update_cursor = NULL;
		}

		if (bFree)
			pfree_pgxc_all_handles(all_handles);
#endif
	}

	/*
	 * Clean up parameters if they were set
	 */
	if (node->paramval_data)
	{
		pfree(node->paramval_data);
		node->paramval_data = NULL;
		node->paramval_len = 0;
	}

	/* Free the param types if they are newly allocated */
	if (node->rqs_param_types &&
		node->rqs_param_types != ((RemoteQuery*)node->ss.ps.plan)->rq_param_types)
	{
		pfree(node->rqs_param_types);
		node->rqs_param_types = NULL;
		node->rqs_num_params = 0;
	}

	HandleListResetOwner(node->all_handles);

	CloseRemoteQueryState(node);
}

#if 0
static void
close_node_cursors(PGXCNodeHandle **connections, int conn_count, char *cursor)
{
	int i;
	RemoteQueryState *rqstate;

	for (i = 0; i < conn_count; i++)
	{
		if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
			BufferConnection(connections[i]);
		if (pgxc_node_send_close(connections[i], false, cursor) != 0)
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close Datanode cursor")));
		if (pgxc_node_send_sync(connections[i]) != 0)
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close Datanode cursor")));
	}

	rqstate = CreateRemoteQueryState(conn_count, COMBINE_TYPE_NONE);

	while (conn_count > 0)
	{
		/*
		 * No matter any connection has crash down, we still need to deal with
		 * other connections.
		 */
		pgxc_node_receive(conn_count, connections, NULL);
		i = 0;
		while (i < conn_count)
		{
			int res = handle_response(connections[i], rqstate);
			if (res == RESPONSE_EOF)
			{
				i++;
			}
			else if (res == RESPONSE_COMPLETE)
			{
				if (--conn_count > i)
					connections[i] = connections[conn_count];
			}
			else
			{
				// Unexpected response, ignore?
				if (connections[i]->error == NULL)
				{
					add_error_message(connections[i],
						"Unexpected response from node %s", NameStr(connections[i]->name));
				}
				if (rqstate->errorMessage.len == 0)
				{
					appendStringInfo(&(rqstate->errorMessage),
						"Unexpected response from node %s", NameStr(connections[i]->name));
				}
				/* Stop tracking and move last connection in place */
				conn_count--;
				if (i < conn_count)
					connections[i] = connections[conn_count];
			}
		}
	}
}
#endif

/*
 * Encode parameter values to format of DataRow message (the same format is
 * used in Bind) to prepare for sending down to Datanodes.
 * The data row is copied to RemoteQueryState.paramval_data.
 */
void
SetDataRowForExtParams(ParamListInfo paraminfo, RemoteQueryState *rq_state)
{
	StringInfoData buf;
	uint16 n16;
	int i;
	int real_num_params = 0;
	RemoteQuery *node = (RemoteQuery*) rq_state->ss.ps.plan;
	ParamExternData *params = NULL;

	/* If there are no parameters, there is no data to BIND. */
	if (!paraminfo)
		return;

	/*
	 * If this query has been generated internally as a part of two-step DML
	 * statement, it uses only the internal parameters for input values taken
	 * from the source data, and it never uses external parameters. So even if
	 * parameters were being set externally, they won't be present in this
	 * statement (they might be present in the source data query). In such
	 * case where parameters refer to the values returned by SELECT query, the
	 * parameter data and parameter types would be set in SetDataRowForIntParams().
	 */
	if (node->rq_params_internal)
		return;

	Assert(!rq_state->paramval_data);

	/*
	 * It is necessary to fetch parameters
	 * before looking at the output value.
	 */
	if (paraminfo->paramFetch)
		params = palloc0(sizeof(params[0]) * paraminfo->numParams);
	for (i = 0; i < paraminfo->numParams; i++)
	{
		ParamExternData *param;

		if (paraminfo->paramFetch)
			param = paraminfo->paramFetch(paraminfo, i + 1, false, &params[i]);
		else
			param = &paraminfo->params[i];

		/*
		 * This is the last parameter found as useful, so we need
		 * to include all the previous ones to keep silent the remote
		 * nodes. All the parameters prior to the last usable having no
		 * type available will be considered as NULL entries.
		 */
		if (OidIsValid(param->ptype))
			real_num_params = i + 1;
	}

	/*
	 * If there are no parameters available, simply leave.
	 * This is possible in the case of a query called through SPI
	 * and using no parameters.
	 */
	if (real_num_params == 0)
	{
		rq_state->paramval_data = NULL;
		rq_state->paramval_len = 0;
		if (params)
			pfree(params);
		return;
	}

	initStringInfo(&buf);

	/* Number of parameter values */
	n16 = htons(real_num_params);
	appendBinaryStringInfo(&buf, (char *) &n16, 2);

	/* Parameter values */
	for (i = 0; i < real_num_params; i++)
	{
		ParamExternData *param;
		uint32 n32;

		if (paraminfo->paramFetch)
			param = &params[i];
		else
			param = &paraminfo->params[i];

		/*
		 * Parameters with no types are considered as NULL and treated as integer
		 * The same trick is used for dropped columns for remote DML generation.
		 */
		if (param->isnull || !OidIsValid(param->ptype))
		{
			n32 = htonl(-1);
			appendBinaryStringInfo(&buf, (char *) &n32, 4);
		}
		else
		{
			Oid		typOutput;
			bool	typIsVarlena;
			Datum	pval;
			char   *pstring;
			int		len;

			/* Get info needed to output the value */
			getTypeOutputInfo(param->ptype, &typOutput, &typIsVarlena);

			/*
			 * If we have a toasted datum, forcibly detoast it here to avoid
			 * memory leakage inside the type's output routine.
			 */
			if (typIsVarlena)
				pval = PointerGetDatum(PG_DETOAST_DATUM(param->value));
			else
				pval = param->value;

			/* Convert Datum to string */
			pstring = OidOutputFunctionCall(typOutput, pval);

			/* copy data to the buffer */
			len = strlen(pstring);
			n32 = htonl(len);
			appendBinaryStringInfo(&buf, (char *) &n32, 4);
			appendBinaryStringInfo(&buf, pstring, len);
		}
	}


	/*
	 * If parameter types are not already set, infer them from
	 * the paraminfo.
	 */
	if (node->rq_num_params > 0)
	{
		/*
		 * Use the already known param types for BIND. Parameter types
		 * can be already known when the same plan is executed multiple
		 * times.
		 */
		if (node->rq_num_params != real_num_params)
			elog(ERROR, "Number of user-supplied parameters do not match "
						"the number of remote parameters");
		rq_state->rqs_num_params = node->rq_num_params;
		rq_state->rqs_param_types = node->rq_param_types;
	}
	else
	{
		rq_state->rqs_num_params = real_num_params;
		rq_state->rqs_param_types = (Oid *) palloc(sizeof(Oid) * real_num_params);
		if (params)
		{
			for (i = 0; i < real_num_params; i++)
				rq_state->rqs_param_types[i] = params[i].ptype;
		}else
		{
			for (i = 0; i < real_num_params; i++)
				rq_state->rqs_param_types[i] = paraminfo->params[i].ptype;
		}
	}

	/* Assign the newly allocated data row to paramval */
	rq_state->paramval_data = buf.data;
	rq_state->paramval_len = buf.len;
	if (params)
		pfree(params);
}


/* ----------------------------------------------------------------
 *		ExecRemoteQueryReScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecRemoteQueryReScan(RemoteQueryState *node, ExprContext *exprCtxt)
{
	/*
	 * If the materialized store is not empty, just rewind the stored output.
	 */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

	if (!node->tuplestorestate)
		return;

	tuplestore_rescan(node->tuplestorestate);
}

/*
 * Called when the backend is ending.
 */
void
PGXCNodeCleanAndRelease(int code, Datum arg)
{
	/* Clean up prepared transactions before releasing connections */
	DropAllPreparedStatements();

	/* Disconnect with rxact manager process */
	DisconnectRemoteXact();

	/*
	 * Make sure the old NodeHandle will never keep anything about the
	 * last SQL.
	 */
	ResetNodeExecutor();

	/* Make sure the old PGconn will dump the trash data */
	PQNReleaseAllConnect(-1);

	/* Disconnect from Pooler */
	PoolManagerDisconnect();
}

/*
 * ExecProcNodeDMLInXC
 *
 * This function is used by ExecInsert/Update/Delete to execute the
 * Insert/Update/Delete on the datanode using RemoteQuery plan.
 *
 * In XC, a non-FQSed UPDATE/DELETE is planned as a two step process
 * The first step selects the ctid & node id of the row to be modified and the
 * second step creates a parameterized query that is supposed to take the data
 * row returned by the lower plan node as the parameters to modify the affected
 * row. In case of an INSERT however the first step is used to get the new
 * column values to be inserted in the target table and the second step uses
 * those values as parameters of the INSERT query.
 *
 * We use extended query protocol to avoid repeated planning of the query and
 * pass the column values(in case of an INSERT) and ctid & xc_node_id
 * (in case of UPDATE/DELETE) as parameters while executing the query.
 *
 * Parameters:
 * resultRemoteRel:  The RemoteQueryState containing DML statement to be
 *					 executed
 * sourceDataSlot: The tuple returned by the first step (described above)
 *					 to be used as parameters in the second step.
 * newDataSlot: This has all the junk attributes stripped off from
 *				sourceDataSlot, plus BEFORE triggers may have modified the
 *				originally fetched data values. In other words, this has
 *				the final values that are to be sent to datanode through BIND.
 *
 * Returns the result of RETURNING clause if any
 */
TupleTableSlot *
ExecProcNodeDMLInXC(EState *estate,
					TupleTableSlot *sourceDataSlot,
					TupleTableSlot *newDataSlot)
{
	ResultRelInfo *resultRelInfo = estate->es_result_relation_info;
	RemoteQueryState *resultRemoteRel = (RemoteQueryState *) estate->es_result_remoterel;
	ExprContext	*econtext = resultRemoteRel->ss.ps.ps_ExprContext;
	TupleTableSlot	*returningResultSlot = NULL;	/* RETURNING clause result */
	TupleTableSlot	*temp_slot;
	bool			dml_returning_on_replicated = false;
	RemoteQuery		*step = (RemoteQuery *) resultRemoteRel->ss.ps.plan;
	uint32			save_rqs_processed = 0;

	/*
	 * If the tuple returned by the previous step was null,
	 * simply return null tuple, no need to execute the DML
	 */
	if (TupIsNull(sourceDataSlot))
		return NULL;

	/*
	 * The current implementation of DMLs with RETURNING when run on replicated
	 * tables returns row from one of the datanodes. In order to achieve this
	 * ExecProcNode is repeatedly called saving one tuple and rejecting the rest.
	 * Do we have a DML on replicated table with RETURNING?
	 */
	dml_returning_on_replicated = IsReturningDMLOnReplicatedTable(step);

	/*
	 * Use data row returned by the previous step as parameter for
	 * the DML to be executed in this step.
	 */
	SetDataRowForIntParams(resultRelInfo->ri_junkFilter,
						   sourceDataSlot, newDataSlot, resultRemoteRel);

	/*
	 * do_query calls get_exec_connections to determine target nodes
	 * at execution time. The function get_exec_connections can decide
	 * to evaluate en_expr to determine the target nodes. To evaluate en_expr,
	 * ExecEvalVar is called which picks up values from ecxt_scantuple if Var
	 * does not refer either OUTER or INNER varno. Hence we should copy the
	 * tuple returned by previous step in ecxt_scantuple if econtext is set.
	 * The econtext is set only when en_expr is set for execution time
	 * determination of the target nodes.
	 */

	if (econtext)
		econtext->ecxt_scantuple = newDataSlot;


	/*
	 * This loop would be required to reject tuples received from datanodes
	 * when a DML with RETURNING is run on a replicated table otherwise it
	 * would run once.
	 * PGXC_TODO: This approach is error prone if the DML statement constructed
	 * by the planner is such that it updates more than one row (even in case of
	 * non-replicated data). Fix it.
	 */
	do
	{
		temp_slot = ExecProcNode((PlanState *)resultRemoteRel);
		if (!TupIsNull(temp_slot))
		{
			/* Have we already copied the returned tuple? */
			if (returningResultSlot == NULL)
			{
				/* Copy the received tuple to be returned later */
				returningResultSlot = MakeSingleTupleTableSlot(temp_slot->tts_tupleDescriptor, temp_slot->tts_ops);
				returningResultSlot = ExecCopySlot(returningResultSlot, temp_slot);
			}
			/* Clear the received tuple, the copy required has already been saved */
			ExecClearTuple(temp_slot);
		}
		else
		{
			/* Null tuple received, so break the loop */
			ExecClearTuple(temp_slot);
			break;
		}

		/*
		 * If we don't save rqs_processed, it will be assigned to 0 when enter
		 * RemoteQueryNext(see more details) again, and when break the loop,
		 * the caller will nerver get correct rqs_processed. See ExecInsert.
		 *
		 * eg.
		 *		create table t1(id int, value int) distribute by replication.
		 *		insert into t1 values(1,1),(2,2),(3,3) returning id;
		 */
		if (dml_returning_on_replicated &&
			save_rqs_processed == 0 &&
			resultRemoteRel->rqs_processed > 0)
			save_rqs_processed = resultRemoteRel->rqs_processed;

	} while (dml_returning_on_replicated);

	if (dml_returning_on_replicated)
		resultRemoteRel->rqs_processed = save_rqs_processed;

	/*
	 * A DML can impact more than one row, e.g. an update without any where
	 * clause on a table with more than one row. We need to make sure that
	 * RemoteQueryNext calls do_query for each affected row, hence we reset
	 * the flag here and finish the DML being executed only when we return
	 * NULL from ExecModifyTable
	 */
	resultRemoteRel->query_Done = false;

	return returningResultSlot;
}

/*
 * set_dbcleanup_callback:
 * Register a callback function which does some non-critical cleanup tasks
 * on xact success or abort, such as tablespace/database directory cleanup.
 */
void set_dbcleanup_callback(xact_callback function, void *paraminfo, int paraminfo_size)
{
	void *fparams;

	fparams = MemoryContextAlloc(TopMemoryContext, paraminfo_size);
	memcpy(fparams, paraminfo, paraminfo_size);

	dbcleanup_info.function = function;
	dbcleanup_info.fparams = fparams;
}

/*
 * AtEOXact_DBCleanup: To be called at post-commit or pre-abort.
 * Calls the cleanup function registered during this transaction, if any.
 */
void AtEOXact_DBCleanup(bool isCommit)
{
	if (dbcleanup_info.function)
		(*dbcleanup_info.function)(isCommit, dbcleanup_info.fparams);

	/*
	 * Just reset the callbackinfo. We anyway don't want this to be called again,
	 * until explicitly set.
	 */
	dbcleanup_info.function = NULL;
	if (dbcleanup_info.fparams)
	{
		pfree(dbcleanup_info.fparams);
		dbcleanup_info.fparams = NULL;
	}
}

/*
 * SetDataRowForIntParams: Form a BIND data row for internal parameters.
 * This function is called when the data for the parameters of remote
 * statement resides in some plan slot of an internally generated remote
 * statement rather than from some extern params supplied by the caller of the
 * query. Currently DML is the only case where we generate a query with
 * internal parameters.
 * The parameter data is constructed from the slot data, and stored in
 * RemoteQueryState.paramval_data.
 * At the same time, remote parameter types are inferred from the slot
 * tuple descriptor, and stored in RemoteQueryState.rqs_param_types.
 * On subsequent calls, these param types are re-used.
 * The data to be BOUND consists of table column data to be inserted/updated
 * and the ctid/nodeid values to be supplied for the WHERE clause of the
 * query. The data values are present in dataSlot whereas the ctid/nodeid
 * are available in sourceSlot as junk attributes.
 * sourceSlot is used only to retrieve ctid/nodeid, so it does not get
 * used for INSERTs, although it will never be NULL.
 * The slots themselves are undisturbed.
 */
static void
SetDataRowForIntParams(JunkFilter *junkfilter,
					   TupleTableSlot *sourceSlot, TupleTableSlot *dataSlot,
					   RemoteQueryState *rq_state)
{
	StringInfoData	buf;
	uint16			numparams = 0;
	RemoteQuery		*step = (RemoteQuery *) rq_state->ss.ps.plan;

	Assert(sourceSlot);

	/* Calculate the total number of parameters */
	if (step->rq_max_param_num > 0)
		numparams = step->rq_max_param_num;
	else if (dataSlot)
		numparams = dataSlot->tts_tupleDescriptor->natts;
	/* Add number of junk attributes */
	if (junkfilter)
	{
		if (junkfilter->jf_junkAttNo)
			numparams++;
		if (junkfilter->jf_xc_node_id)
			numparams++;
	}

	/*
	 * Infer param types from the slot tupledesc and junk attributes. But we
	 * have to do it only the first time: the interal parameters remain the same
	 * while processing all the source data rows because the data slot tupdesc
	 * never changes. Even though we can determine the internal param types
	 * during planning, we want to do it here: we don't want to set the param
	 * types and param data at two different places. Doing them together here
	 * helps us to make sure that the param types are in sync with the param
	 * data.
	 */

	/*
	 * We know the numparams, now initialize the param types if not already
	 * done. Once set, this will be re-used for each source data row.
	 */
	if (rq_state->rqs_num_params == 0)
	{
		int	attindex = 0;

		rq_state->rqs_num_params = numparams;
		rq_state->rqs_param_types =
			(Oid *) palloc(sizeof(Oid) * rq_state->rqs_num_params);

		if (dataSlot) /* We have table attributes to bind */
		{
			TupleDesc tdesc = dataSlot->tts_tupleDescriptor;
			int numatts = tdesc->natts;

			if (step->rq_max_param_num > 0)
				numatts = step->rq_max_param_num;

			for (attindex = 0; attindex < numatts; attindex++)
			{
				rq_state->rqs_param_types[attindex] =
					TupleDescAttr(tdesc, attindex)->atttypid;
			}
		}
		if (junkfilter) /* Param types for specific junk attributes if present */
		{
			/* jf_junkAttNo always contains ctid */
			if (AttributeNumberIsValid(junkfilter->jf_junkAttNo))
				rq_state->rqs_param_types[attindex] = TIDOID;

			if (AttributeNumberIsValid(junkfilter->jf_xc_node_id))
				rq_state->rqs_param_types[attindex + 1] = INT4OID;
		}
	}
	else
	{
		Assert(rq_state->rqs_num_params == numparams);
	}

	initStringInfo(&buf);

	{
		uint16 params_nbo = htons(numparams); /* Network byte order */
		appendBinaryStringInfo(&buf, (char *) &params_nbo, sizeof(params_nbo));
	}

	if (dataSlot)
	{
		TupleDesc	 	tdesc = dataSlot->tts_tupleDescriptor;
		int				attindex;
		int				numatts = tdesc->natts;

		/* Append the data attributes */

		if (step->rq_max_param_num > 0)
			numatts = step->rq_max_param_num;

		/* ensure we have all values */
		slot_getallattrs(dataSlot);
		for (attindex = 0; attindex < numatts; attindex++)
		{
			uint32 n32;
			Assert(attindex < numparams);

			if (dataSlot->tts_isnull[attindex])
			{
				n32 = htonl(-1);
				appendBinaryStringInfo(&buf, (char *) &n32, 4);
			}
			else
				pgxc_append_param_val(&buf, dataSlot->tts_values[attindex], TupleDescAttr(tdesc, attindex)->atttypid);

		}
	}

	/*
	 * From the source data, fetch the junk attribute values to be appended in
	 * the end of the data buffer. The junk attribute vals like ctid and
	 * xc_node_id are used in the WHERE clause parameters.
	 * These attributes would not be present for INSERT.
	 */
	if (junkfilter)
	{
		/* First one - jf_junkAttNo - always reprsents ctid */
		pgxc_append_param_junkval(sourceSlot, junkfilter->jf_junkAttNo,
								  TIDOID, &buf);
		pgxc_append_param_junkval(sourceSlot, junkfilter->jf_xc_node_id,
								  INT4OID, &buf);
	}

	/* Assign the newly allocated data row to paramval */
	rq_state->paramval_data = buf.data;
	rq_state->paramval_len = buf.len;

}


/*
 * pgxc_append_param_junkval:
 * Append into the data row the parameter whose value cooresponds to the junk
 * attributes in the source slot, namely ctid or node_id.
 */
static void
pgxc_append_param_junkval(TupleTableSlot *slot, AttrNumber attno,
						  Oid valtype, StringInfo buf)
{
	bool isNull;

	if (slot && attno != InvalidAttrNumber)
	{
		/* Junk attribute positions are saved by ExecFindJunkAttribute() */
		Datum val = ExecGetJunkAttribute(slot, attno, &isNull);
		/* shouldn't ever get a null result... */
		if (isNull)
			elog(ERROR, "NULL junk attribute");

		pgxc_append_param_val(buf, val, valtype);
	}
}

/*
 * pgxc_append_param_val:
 * Append the parameter value for the SET clauses of the UPDATE statement.
 * These values are the table attribute values from the dataSlot.
 */
static void
pgxc_append_param_val(StringInfo buf, Datum val, Oid valtype)
{
	/* Convert Datum to string */
	char *pstring;
	int len;
	uint32 n32;
	Oid		typOutput;
	bool	typIsVarlena;

	/* Get info needed to output the value */
	getTypeOutputInfo(valtype, &typOutput, &typIsVarlena);
	/*
	 * If we have a toasted datum, forcibly detoast it here to avoid
	 * memory leakage inside the type's output routine.
	 */
	if (typIsVarlena)
		val = PointerGetDatum(PG_DETOAST_DATUM(val));

	pstring = OidOutputFunctionCall(typOutput, val);

	/* copy data to the buffer */
	len = strlen(pstring);
	n32 = htonl(len);
	appendBinaryStringInfo(buf, (char *) &n32, 4);
	appendBinaryStringInfo(buf, pstring, len);
}

/*
 * pgxc_rq_fire_bstriggers:
 * BEFORE STATEMENT triggers to be fired for a user-supplied DML query.
 * For non-FQS query, we internally generate remote DML query to be executed
 * for each row to be processed. But we do not want to explicitly fire triggers
 * for such a query; ExecModifyTable does that for us. It is the FQS DML query
 * where we need to explicitly fire statement triggers on coordinator. We
 * cannot run stmt triggers on datanode. While we can fire stmt trigger on
 * datanode versus coordinator based on the function shippability, we cannot
 * do the same for FQS query. The datanode has no knowledge that the trigger
 * being fired is due to a non-FQS query or an FQS query. Even though it can
 * find that all the triggers are shippable, it won't know whether the stmt
 * itself has been FQSed. Even though all triggers were shippable, the stmt
 * might have been planned on coordinator due to some other non-shippable
 * clauses. So the idea here is to *always* fire stmt triggers on coordinator.
 * Note that this does not prevent the query itself from being FQSed. This is
 * because we separately fire stmt triggers on coordinator.
 */
static void
pgxc_rq_fire_bstriggers(RemoteQueryState *node)
{
	RemoteQuery *rq = (RemoteQuery*) node->ss.ps.plan;
	EState *estate = node->ss.ps.state;

	/* If it's not an internally generated query, fire BS triggers */
	if (!rq->rq_params_internal && estate->es_result_relations)
	{
		Assert(rq->remote_query);
		switch (rq->remote_query->commandType)
		{
			case CMD_INSERT:
				ExecBSInsertTriggers(estate, estate->es_result_relations);
				break;
			case CMD_UPDATE:
				ExecBSUpdateTriggers(estate, estate->es_result_relations);
				break;
			case CMD_DELETE:
				ExecBSDeleteTriggers(estate, estate->es_result_relations);
				break;
			default:
				break;
		}
	}
}

/*
 * pgxc_rq_fire_astriggers:
 * AFTER STATEMENT triggers to be fired for a user-supplied DML query.
 * See comments in pgxc_rq_fire_astriggers()
 */
static void
pgxc_rq_fire_astriggers(RemoteQueryState *node)
{
	RemoteQuery *rq = (RemoteQuery*) node->ss.ps.plan;
	EState *estate = node->ss.ps.state;

	/* If it's not an internally generated query, fire AS triggers */
	if (!rq->rq_params_internal && estate->es_result_relations)
	{
		Assert(rq->remote_query);
		switch (rq->remote_query->commandType)
		{
			case CMD_INSERT:
				ExecASInsertTriggers(estate, estate->es_result_relations, NULL);
				break;
			case CMD_UPDATE:
				ExecASUpdateTriggers(estate, estate->es_result_relations, NULL);
				break;
			case CMD_DELETE:
				ExecASDeleteTriggers(estate, estate->es_result_relations, NULL);
				break;
			default:
				break;
		}
	}
}
