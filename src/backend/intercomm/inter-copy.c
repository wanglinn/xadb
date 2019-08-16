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
#include "libpq-int.h"
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

typedef struct FetchRemoteCopyContext
{
	PQNHookFunctions	pub;
	StringInfo			row;
}FetchRemoteCopyContext;

static NodeHandle*LookupNodeHandle(List *handle_list, Oid node_id);
static void HandleCopyOutRow(RemoteCopyState *node, char *buf, int len);
static int HandleStartRemoteCopy(NodeHandle *handle, CommandId cmid, Snapshot snap, const char *copy_query);
static bool HandleRecvCopyResult(NodeHandle *handle);
static void FetchRemoteCopyRow(RemoteCopyState *node, StringInfo row);
static bool FetchCopyRowHook(PQNHookFunctions *pub, struct pg_conn *conn, const char *buf, int len);

/*
 * StartRemoteCopy
 *
 * Send begin and copy query to involved nodes.
 */
void
StartRemoteCopy(RemoteCopyState *node)
{
	InterXactState		state;
	NodeMixHandle	   *cur_handle;
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

	/* Must use the command ID to mark COPY FROM tuples */
	cmid = GetCurrentCommandId(is_from);
	snap = GetActiveSnapshot();
	timestamp = GetCurrentTransactionStartTimestamp();
	state = MakeInterXactState2(GetCurrentInterXactState(), node_list);
	/* It is no need to send BEGIN when COPY TO */
	state->need_xact_block = is_from;
	cur_handle = state->cur_handle;

	//agtm_BeginTransaction();
	gxid = GetCurrentTransactionId();

	PG_TRY();
	{
		foreach (lc_handle, cur_handle->handles)
		{
			handle = (NodeHandle *) lfirst(lc_handle);
			already_begin = false;

			if (!HandleBegin(state, handle, gxid, timestamp, is_from, &already_begin) ||
				!HandleStartRemoteCopy(handle, cmid, snap, copy_query))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Fail to start remote COPY %s", is_from ? "FROM" : "TO"),
						 errnode(NameStr(handle->node_name)),
						 errdetail("%s", HandleGetError(handle))));
			}
		}
	} PG_CATCH();
	{
		InterXactGCCurrent(state);
		PG_RE_THROW();
	} PG_END_TRY();

	node->copy_handles = cur_handle->handles;
}

/*
 * EndRemoteCopy
 *
 * Send copy end message to involved handles.
 */
void
EndRemoteCopy(RemoteCopyState *node)
{
	NodeHandle	   *handle;
	NodeHandle	   *prhandle;
	ListCell	   *lc_handle;

	Assert(node);
	PG_TRY();
	{
		prhandle = GetPrimaryNodeHandle();
		if (prhandle && list_member_ptr(node->copy_handles, prhandle))
		{
			if (PQputCopyEnd(prhandle->node_conn, NULL) <= 0 ||
				!HandleFinishCommand(prhandle, NULL))
				ereport(ERROR,
						(errmsg("Fail to end COPY %s", node->is_from ? "FROM" : "TO"),
						 errnode(NameStr(prhandle->node_name)),
						 errdetail("%s", HandleGetError(prhandle))));
		}

		foreach (lc_handle, node->copy_handles)
		{
			handle = (NodeHandle *) lfirst(lc_handle);
			/* Primary handle has been copy end already, see above */
			if (prhandle && handle == prhandle)
				continue;
			if (PQputCopyEnd(handle->node_conn, NULL) <= 0 ||
				!HandleFinishCommand(handle, NULL))
				ereport(ERROR,
						(errmsg("Fail to end COPY %s", node->is_from ? "FROM" : "TO"),
						 errnode(NameStr(handle->node_name)),
						 errdetail("%s", HandleGetError(handle))));
		}
	} PG_CATCH();
	{
		HandleListGC(node->copy_handles);
		PG_RE_THROW();
	} PG_END_TRY();
}

/*
 * SendCopyFromHeader
 *
 * Send PG_HEADER for a COPY FROM in binary mode to all involved nodes.
 */
void
SendCopyFromHeader(RemoteCopyState *node, const StringInfo header)
{
	ListCell	   *lc_handle;
	NodeHandle	   *handle;

	Assert(node && header);
	PG_TRY();
	{
		foreach (lc_handle, node->copy_handles)
		{
			handle = (NodeHandle *) lfirst(lc_handle);
			if (PQputCopyData(handle->node_conn, header->data, header->len) <= 0)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("Fail to send COPY FROM header in binary mode."),
						 errnode(NameStr(handle->node_name)),
						 errdetail("%s", HandleGetError(handle))));
		}
	} PG_CATCH();
	{
		HandleListGC(node->copy_handles);
		PG_RE_THROW();
	} PG_END_TRY();
}

/*
 * DoRemoteCopyFrom
 *
 * Send copy line buffer for a COPY FROM to the node list.
 */
void
DoRemoteCopyFrom(RemoteCopyState *node, const StringInfo line_buf, const List *node_list)
{
	NodeHandle	   *handle;
	NodeHandle	   *prhandle;
	ListCell	   *lc_node;
	Oid				node_id;

	Assert(node && line_buf);
	PG_TRY();
	{
		prhandle = GetPrimaryNodeHandle();
		/* Primary handle should be sent first */
		if (prhandle && list_member_oid(node_list, prhandle->node_id))
		{
			Assert(list_member_ptr(node->copy_handles, prhandle));
			if (PQputCopyData(prhandle->node_conn, line_buf->data, line_buf->len) <= 0)
				ereport(ERROR,
						(errmsg("Fail to send to COPY FROM data."),
						 errnode(NameStr(prhandle->node_name)),
						 errhint("%s", HandleGetError(prhandle))));
		}

		foreach (lc_node, node_list)
		{
			node_id = lfirst_oid(lc_node);
			/* Primary handle has been sent already, see above */
			if (prhandle && node_id == prhandle->node_id)
				continue;
			handle = LookupNodeHandle(node->copy_handles, node_id);
			if (!handle)
				ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					errmsg("Fail to lookup handle for node which oid is \"%d\"", node_id)));

			if (PQputCopyData(handle->node_conn, line_buf->data, line_buf->len) <= 0)
				ereport(ERROR,
						(errmsg("Fail to send to COPY FROM data."),
						 errnode(NameStr(handle->node_name)),
						 errhint("%s", HandleGetError(handle))));
		}
	} PG_CATCH();
	{
		HandleListGC(node->copy_handles);
		PG_RE_THROW();
	} PG_END_TRY();
}

/*
 * DoRemoteCopyTo
 *
 * Fetch each copy out row and handle them
 *
 * return the count of copy row
 */
uint64
DoRemoteCopyTo(RemoteCopyState *node)
{
	StringInfoData row = {NULL, 0, 0, 0};

	Assert(node);

	node->processed = 0;
	for (;;)
	{
		FetchRemoteCopyRow(node, &row);
		if (!row.data)
			break;
		HandleCopyOutRow(node, row.data, row.len);
		node->processed++;
	}

	return node->processed;
}

/*
 * HandleCopyOutRow
 *
 * handle the row buffer to the specified destination.
 */
static void
HandleCopyOutRow(RemoteCopyState *node, char *buf, int len)
{
	Assert(node);
	switch (node->remoteCopyType)
	{
		case REMOTE_COPY_FILE:
			Assert(node->copy_file);
			/* Write data directly to file */
			fwrite(buf, 1, len, node->copy_file);
			break;
		case REMOTE_COPY_STDOUT:
			/* Send back data to client */
			pq_putmessage('d', buf, len);
			break;
		case REMOTE_COPY_TUPLESTORE:
			{
				TupleDesc			tupdesc = node->tuple_desc;
				Form_pg_attribute	attr = tupdesc->attrs;
				Oid				   *typioparams = node->copy_extra->typioparams;
				FmgrInfo		   *in_functions = node->copy_extra->inflinfos;
				Datum			   *values = node->copy_extra->values;
				bool			   *nulls = node->copy_extra->nulls;
				char			   *field;
				char			  **fields;
				int					i, dropped;

				Assert(typioparams && in_functions && values && nulls);

				/*
				 * Convert message into an array of fields.
				 * Last \n is not included in converted message.
				 */
				fields = CopyOps_RawDataToArrayField(tupdesc, buf, len - 1);

				/* Fill in the array values */
				dropped = 0;
				for (i = 0; i < tupdesc->natts; i++)
				{
					nulls[i] = false;
					field = fields[i - dropped];
					/* Do not need any information for dropped attributes */
					if (attr[i].attisdropped)
					{
						dropped++;
						nulls[i] = true; /* Consider dropped parameter as NULL */
						continue;
					}

					/* Find value */
					values[i] = InputFunctionCall(&in_functions[i],
												  field,
												  typioparams[i],
												  attr[i].atttypmod);
					/* Setup value with NULL flag if necessary */
					if (field == NULL)
						nulls[i] = true;
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
			}
			break;
		case REMOTE_COPY_NONE:
		default:
			Assert(0); /* Should not happen */
	}
}

/*
 * HandleStartRemoteCopy
 *
 * Send copy query to remote and wait for receiving response.
 *
 * return 0 if any trouble.
 * return 1 if OK.
 */
static int
HandleStartRemoteCopy(NodeHandle *handle, CommandId cmid, Snapshot snap, const char *copy_query)
{
	if (!HandleSendQueryTree(handle, cmid, snap, copy_query, NULL) ||
		!HandleRecvCopyResult(handle))
		return 0;

	return 1;
}

/*
 * HandleRecvCopyResult
 *
 * Wait for receiving correct COPY response synchronously
 *
 * return true if OK
 * return false if trouble
 */
static bool
HandleRecvCopyResult(NodeHandle *handle)
{
	bool copy_start_ok = false;
	PGresult *res = NULL;

	Assert(handle && handle->node_conn);
	res = PQgetResult(handle->node_conn);
	switch (PQresultStatus(res))
	{
		case PGRES_COPY_OUT:
		case PGRES_COPY_IN:
		case PGRES_COPY_BOTH:
			copy_start_ok = true;
			break;
		default:
			break;
	}
	PQclear(res);

	return copy_start_ok;
}

/*
 * FetchRemoteCopyRow
 *
 * Fetch any copy row from remote handles. keep them save
 * in StringInfo row.
 */
static void
FetchRemoteCopyRow(RemoteCopyState *node, StringInfo row)
{
	FetchRemoteCopyContext hook;
	List *handle_list = NIL;

	Assert(node && row);

	handle_list = node->copy_handles;
	hook.pub = PQNDefaultHookFunctions;
	hook.pub.HookCopyOut = FetchCopyRowHook;

	MemSet(row, 0, sizeof(*row));
	hook.row = row;

	PQNListExecFinish(handle_list, HandleGetPGconn, &hook.pub, true);
}

static bool
FetchCopyRowHook(PQNHookFunctions *pub, struct pg_conn *conn, const char *buf, int len)
{
	StringInfo	row = ((FetchRemoteCopyContext*)pub)->row;

	row->data = (char *) buf;
	row->len = row->maxlen = len;

	return true;
}

/*
 * LookupNodeHandle
 *
 * Find hanlde from handle list by node ID.
 *
 * return NULL if not found
 * return handle if found
 */
static NodeHandle*
LookupNodeHandle(List *handle_list, Oid node_id)
{
	ListCell	   *lc_handle;
	NodeHandle	   *handle;

	if (!OidIsValid(node_id))
		return NULL;

	foreach (lc_handle, handle_list)
	{
		handle = (NodeHandle *) lfirst(lc_handle);
		if (handle->node_id == node_id)
			return handle;
	}

	return NULL;
}
