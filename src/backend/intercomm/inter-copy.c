/*-------------------------------------------------------------------------
 *
 * inter-copy.c
 *	  Internode copy routines by NodeHandle
 *
 *
 * Portions Copyright (c) 2016-2017, ADB Development Group
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/intercomm/inter-copy.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"

#include "access/xact.h"
#include "agtm/agtm.h"
#include "commands/prepare.h"
#include "executor/clusterReceiver.h"
#include "intercomm/inter-comm.h"
#include "libpq/libpq.h"
#include "libpq/libpq-int.h"
#include "libpq/libpq-node.h"
#include "nodes/execnodes.h"
#include "pgxc/pgxc.h"
#include "pgxc/copyops.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"

typedef struct RemoteCopyContext
{
	RemoteCopyState	   *node;
	TupleTableSlot	   *slot;
} RemoteCopyContext;

static void HandleCopyOutRow(RemoteCopyState *node, char *buf, int len);
static int HandleStartRemoteCopy(NodeHandle *handle, CommandId cmid, Snapshot snap, const char *copy_query);
static bool HandleRecvCopyOK(NodeHandle *handle);
static bool StartCopyOKHook(void *context, struct pg_conn *conn, PQNHookFuncType type, ...);
static void FetchRemoteCopyRow(RemoteCopyState *node, StringInfo row);
static bool FetchCopyRowHook(void *context, struct pg_conn *conn, PQNHookFuncType type, ...);

void
StartRemoteCopy(RemoteCopyState *node)
{
	InterXactState		state;
	NodeMixHandle	   *mix_handle;
	NodeHandle		   *handle;
	ListCell		   *lc_handle;
	const char		   *copy_query;
	const List		   *node_list;
	bool				is_from;
	bool				already_begin;
	Snapshot			snap;
	CommandId			cmid;
	TimestampTz			timestamp;
	GlobalTransactionId gxid;

	if (!node)
		return ;

	copy_query = node->query_buf.data;
	node_list = node->exec_nodes->nodeids;
	is_from = node->is_from;

	/* sanity check */
	if (!copy_query || !list_length(node_list))
		return ;

	cmid = GetCurrentCommandId(is_from);
	snap = GetActiveSnapshot();
	timestamp = GetCurrentTransactionStartTimestamp();
	state = MakeInterXactState2(GetTopInterXactState(), node_list);
	state->need_xact_block = true;
	mix_handle = state->mix_handle;

	agtm_BeginTransaction();
	gxid = GetCurrentTransactionId();

	PG_TRY();
	{
		foreach (lc_handle, mix_handle->handles)
		{
			handle = (NodeHandle *) lfirst(lc_handle);
			already_begin = false;

			if (!HandleBegin(state, handle, gxid, timestamp, true, &already_begin) ||
				!HandleStartRemoteCopy(handle, cmid, snap, copy_query))
			{
				state->block_state |= IBLOCK_ABORT;
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("%s", HandleGetError(handle, false))));
			}
		}
	} PG_CATCH();
	{
		InterXactGC(state);
		PG_RE_THROW();
	} PG_END_TRY();

	node->copy_handles = mix_handle->handles;
}

uint64
FinishRemoteCopyOut(RemoteCopyState *node)
{
	StringInfoData	row = {NULL, 0, 0, 0};

	if (!node)
		return 0L;

	node->processed = 0;
	for (;;)
	{
		FetchRemoteCopyRow(node, &row);
		if (!row.data)
			break;
		node->processed++;
		HandleCopyOutRow(node, row.data, row.len);
	}

	return node->processed;
}

static void
HandleCopyOutRow(RemoteCopyState *node, char *buf, int len)
{
	Assert(node);
	switch (node->remoteCopyType)
	{
		case REMOTE_COPY_FILE:
			/* Write data directly to file */
			fwrite(buf, 1, len, node->copy_file);
			break;
		case REMOTE_COPY_STDOUT:
			/* Send back data to client */
			pq_putmessage('d', buf, len);
			break;
		case REMOTE_COPY_TUPLESTORE:
			{
				Datum  *values;
				bool   *nulls;
				TupleDesc   tupdesc = node->tuple_desc;
				int i, dropped;
				Form_pg_attribute *attr = tupdesc->attrs;
				FmgrInfo *in_functions;
				Oid *typioparams;
				char **fields;

				values = (Datum *) palloc(tupdesc->natts * sizeof(Datum));
				nulls = (bool *) palloc(tupdesc->natts * sizeof(bool));
				in_functions = (FmgrInfo *) palloc(tupdesc->natts * sizeof(FmgrInfo));
				typioparams = (Oid *) palloc(tupdesc->natts * sizeof(Oid));

				/* Calculate the Oids of input functions */
				for (i = 0; i < tupdesc->natts; i++)
				{
					Oid         in_func_oid;

					/* Do not need any information for dropped attributes */
					if (attr[i]->attisdropped)
						continue;

					getTypeInputInfo(attr[i]->atttypid,
									 &in_func_oid, &typioparams[i]);
					fmgr_info(in_func_oid, &in_functions[i]);
				}

				/*
				 * Convert message into an array of fields.
				 * Last \n is not included in converted message.
				 */
				fields = CopyOps_RawDataToArrayField(tupdesc, buf, len - 1);

				/* Fill in the array values */
				dropped = 0;
				for (i = 0; i < tupdesc->natts; i++)
				{
					char	*string = fields[i - dropped];
					/* Do not need any information for dropped attributes */
					if (attr[i]->attisdropped)
					{
						dropped++;
						nulls[i] = true; /* Consider dropped parameter as NULL */
						continue;
					}

					/* Find value */
					values[i] = InputFunctionCall(&in_functions[i],
												  string,
												  typioparams[i],
												  attr[i]->atttypmod);
					/* Setup value with NULL flag if necessary */
					if (string == NULL)
						nulls[i] = true;
					else
						nulls[i] = false;
				}

				/* Then insert the values into tuplestore */
				tuplestore_putvalues(node->tuplestorestate,
									 node->tuple_desc,
									 values,
									 nulls);

				/* Clean up everything */
				if (*fields)
					pfree(*fields);
				pfree(fields);
				pfree(values);
				pfree(nulls);
				pfree(in_functions);
				pfree(typioparams);
			}
			break;
		case REMOTE_COPY_NONE:
		default:
			Assert(0); /* Should not happen */
	}
}

static int
HandleStartRemoteCopy(NodeHandle *handle, CommandId cmid, Snapshot snap, const char *copy_query)
{
	if (!HandleSendQueryTree(handle, cmid, snap, copy_query, NULL) ||
		!HandleRecvCopyOK(handle))
		return 0;

	return 1;
}

static bool
HandleRecvCopyOK(NodeHandle *handle)
{
	bool copy_start_ok = false;

	Assert(handle && handle->node_conn);

	(void) PQNOneExecFinish(handle->node_conn, StartCopyOKHook, &copy_start_ok, true);

	return copy_start_ok;
}

static bool
StartCopyOKHook(void *context, struct pg_conn *conn, PQNHookFuncType type, ...)
{
	va_list args;
	switch(type)
	{
		case PQNHFT_ERROR:
			return PQNEFHNormal(NULL, conn, type);
		case PQNHFT_ASYNC_STATUS:
			if (conn->asyncStatus == PGASYNC_COPY_IN ||
				conn->asyncStatus == PGASYNC_COPY_OUT ||
				conn->asyncStatus == PGASYNC_COPY_BOTH)
			{
				*(bool *) context = true;
				return true;
			}
			break;
		case PQNHFT_COPY_OUT_DATA:
		case PQNHFT_COPY_IN_ONLY:
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
		default:
			break;
	}
	return false;
}

static void
FetchRemoteCopyRow(RemoteCopyState *node, StringInfo row)
{
	List *handle_list = NIL;

	Assert(node && row);
	handle_list = node->copy_handles;
	row->data = NULL;
	row->len = 0;
	row->maxlen = 0;
	row->cursor = 0;

	PQNListExecFinish(handle_list, HandleGetPGconn, FetchCopyRowHook, row, true);
}

static bool
FetchCopyRowHook(void *context, struct pg_conn *conn, PQNHookFuncType type, ...)
{
	va_list args;

	switch(type)
	{
		case PQNHFT_ERROR:
			return PQNEFHNormal(NULL, conn, type);
		case PQNHFT_ASYNC_STATUS:
			break;
		case PQNHFT_COPY_OUT_DATA:
			{
				StringInfo	row = (StringInfo) context;

				va_start(args, type);
				row->data = (char *) va_arg(args, const char*);
				row->len = row->maxlen = va_arg(args, int);
				va_end(args);

				return true;
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
		default:
			break;
	}

	return false;
}
