/*-------------------------------------------------------------------------
 *
 * inter-comm.c
 *	  Internode communication routines by NodeHandle
 *
 *
 * Portions Copyright (c) 2016-2017, ADB Development Group
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/intercomm/inter-comm.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"

#include "access/xact.h"
#include "intercomm/inter-comm.h"
#include "libpq/libpq-int.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "reduce/wait_event.h"

typedef struct CommandResult
{
	bool		command_ok;
	char		completionTag[COMPLETION_TAG_BUFSIZE];
} CommandResult;

static StringInfo begin_cmd = NULL;

static const char *GenerateBeginQuery(void);
static int HandleSendBegin(NodeHandle *handle,
						   GlobalTransactionId xid, TimestampTz timestamp,
						   bool need_xact_block, bool *already_begin);
static bool HandleFinishCommandHook(void *context, struct pg_conn *conn, PQNHookFuncType type, ...);

List *
OidArraryToList(MemoryContext context, Oid *oids, int noids)
{
	MemoryContext	old_context = NULL;
	List		   *l = NIL;
	int				i;

	context = context ? context : CurrentMemoryContext;
	old_context = MemoryContextSwitchTo(context);
	for (i = 0; i < noids; i++)
		l = lappend_oid(l, oids[i]);
	(void) MemoryContextSwitchTo(old_context);

	return l;
}

Oid *
OidListToArrary(MemoryContext context, List *oid_list, int *noids)
{
	MemoryContext	old_context = NULL;
	ListCell	   *lc = NULL;
	Oid			   *oids = NULL;
	int				i = 0;

	if (oid_list)
	{
		context = context ? context : CurrentMemoryContext;
		old_context = MemoryContextSwitchTo(context);
		oids = (Oid *) palloc(list_length(oid_list) * sizeof(Oid));
		foreach (lc, oid_list)
			oids[i++] = lfirst_oid(lc);
		(void) MemoryContextSwitchTo(old_context);
	}
	if (noids)
		*noids = i;

	return oids;
}

/*
 * Construct a BEGIN TRANSACTION command after taking into account the
 * current options. The returned string is not palloced and is valid only until
 * the next call to the function.
 */
static const char *
GenerateBeginQuery(void)
{
	const char *read_only;
	const char *isolation_level;

	if (!begin_cmd)
	{
		MemoryContext old_context;

		old_context = MemoryContextSwitchTo(TopMemoryContext);
		begin_cmd = makeStringInfo();
		(void) MemoryContextSwitchTo(old_context);
	} else
		resetStringInfo(begin_cmd);

	/*
	 * First get the READ ONLY status because the next call to GetConfigOption
	 * will overwrite the return buffer
	 */
	if (strcmp(GetConfigOption("transaction_read_only", false, false), "on") == 0)
		read_only = "READ ONLY";
	else
		read_only = "READ WRITE";

	/* Now get the isolation_level for the transaction */
	isolation_level = GetConfigOption("transaction_isolation", false, false);
	if (strcmp(isolation_level, "default") == 0)
		isolation_level = GetConfigOption("default_transaction_isolation", false, false);

	/* Finally build a START TRANSACTION command */
	appendStringInfo(begin_cmd, "START TRANSACTION ISOLATION LEVEL %s %s",
					 isolation_level, read_only);

	return begin_cmd->data;
}

void
HandleGC(NodeHandle *handle)
{
	if (handle)
		PQNExecFinsh_trouble(handle->node_conn);
}

void
HandleCache(NodeHandle *handle)
{
	RemoteQueryState   *node;
	TupleTableSlot	   *slot;

	if (!handle || !handle->node_owner)
		return ;

	node = (RemoteQueryState *) handle->node_owner;
	if (!node->tuplestorestate)
	{
		node->tuplestorestate = tuplestore_begin_heap(false, false, work_mem);
		tuplestore_set_eflags(node->tuplestorestate, node->eflags);
	}
	slot = node->ss.ss_ScanTupleSlot;
	for (;;)
	{
		slot = HandleGetRemoteSlot(handle, slot, node, true);
		if (TupIsNull(slot))
			break;
		tuplestore_puttupleslot(node->tuplestorestate, slot);
	}
}

/*
 * HandleBegin
 *
 * send BEGIN message and receive response
 *
 * return 0 if any trouble
 * return 1 if OK
 */
int
HandleBegin(InterXactState state, NodeHandle *handle,
			GlobalTransactionId xid, TimestampTz timestamp,
			bool need_xact_block, bool *already_begin)
{
	if (!HandleSendBegin(handle, xid, timestamp, need_xact_block, already_begin))
		return 0;

	if (*already_begin || !need_xact_block)
		return 1;

	if (HandleFinishCommand(handle, TRANS_START_TAG))
	{
		InterXactSaveBeginNodes(state, handle->node_id);
		return 1;
	}

	return 0;
}

/*
 * HandleSendBegin
 *
 * send BEGIN message and don't wait response
 *
 * return 0 if any trouble
 * return 1 if OK
 */
static int
HandleSendBegin(NodeHandle *handle,
				GlobalTransactionId xid, TimestampTz timestamp,
				bool need_xact_block, bool *already_begin)
{
	PGconn *conn;

	Assert(handle && already_begin);
	conn = handle->node_conn;
	*already_begin = false;

	/*
	 * return if within transaction block.
	 */
	if (PQtransactionStatus(conn) == PQTRANS_INTRANS)
	{
		*already_begin = true;
		return 1;
	}

	if (!PQsendQueryStart(conn))
		return 0;

	if (!HandleSendGXID(handle, xid) ||
		!HandleSendTimestamp(handle, timestamp))
		return 0;

	if (!need_xact_block)
		return 1;

	return PQsendQuery(conn, GenerateBeginQuery());
}

/*
 * HandleSendCID
 *
 * send command ID and don't wait response
 *
 * return 0 if any trouble
 * return 1 if OK
 */
int
HandleSendCID(NodeHandle *handle, CommandId cid)
{
	PGconn *conn;

	/* no need to send command id */
	if (!IsSendCommandId() ||
		cid == InvalidCommandId)
		return 1;

	Assert(handle && handle->node_conn);
	conn = handle->node_conn;

	if (!PQsendQueryStart(conn))
		return 0;

	/* construct the global transaction xid message */
	if (pqPutMsgStart('M', true, conn) < 0 ||
		pqPutInt((int) cid, sizeof(cid), conn) < 0 ||
		pqPutMsgEnd(conn) < 0)
	{
		pqHandleSendFailure(conn);
		return 0;
	}

	return 1;
}

/*
 * HandleSendGXID
 *
 * send global transaction ID and don't wait response
 *
 * return 0 if any trouble
 * return 1 if OK
 */
int
HandleSendGXID(NodeHandle *handle, GlobalTransactionId xid)
{
	PGconn *conn;

	if (!GlobalTransactionIdIsValid(xid))
		return 1;

	Assert(handle && handle->node_conn);
	conn = handle->node_conn;

	if (!PQsendQueryStart(conn))
		return 0;

	/* construct the global transaction xid message */
	if (pqPutMsgStart('g', true, conn) < 0 ||
		pqPutInt((int) xid, sizeof(xid), conn) < 0 ||
		pqPutMsgEnd(conn) < 0)
	{
		pqHandleSendFailure(conn);
		return 0;
	}

	return 1;
}

/*
 * HandleSendTimestamp
 *
 * send global timestamp and don't wait response
 *
 * return 0 if any trouble
 * return 1 if OK
 */
int
HandleSendTimestamp(NodeHandle *handle, TimestampTz timestamp)
{
	PGconn *conn;
	uint32	hi, lo;
	int64	i = (int64) timestamp;

	if (!GlobalTimestampIsValid(timestamp))
		return 1;

	Assert(handle && handle->node_conn);
	conn = handle->node_conn;

	if (!PQsendQueryStart(conn))
		return 0;

	/* High order half first */
#ifdef INT64_IS_BUSTED
	/* don't try a right shift of 32 on a 32-bit word */
	hi = (i < 0) ? -1 : 0;
#else
	hi = (uint32) (i >> 32);
#endif
	/* Now the low order half */
	lo = (uint32) i;

	/* construct the global timestamp message */
	if (pqPutMsgStart('t', true, conn) < 0 ||
		pqPutInt((int) hi, sizeof(hi), conn) < 0 ||
		pqPutInt((int) lo, sizeof(lo), conn) < 0 ||
		pqPutMsgEnd(conn) < 0)
	{
		pqHandleSendFailure(conn);
		return 0;
	}

	return 1;
}

/*
 * HandleSendSnapshot
 *
 * send global snapshot and don't wait response
 *
 * return 0 if any trouble
 * return 1 if OK
 */
int
HandleSendSnapshot(NodeHandle *handle, Snapshot snapshot)
{
	PGconn		   *conn;
	StringInfoData	buf;

	if (!snapshot)
		return 1;

	Assert(handle);
	conn = handle->node_conn;
	if (!PQsendQueryStart(conn))
		return 0;

	initStringInfo(&buf);
	InterXactSerializeSnapshot(&buf, snapshot);

	/* construct the global snapshot message */
	if (pqPutMsgStart('s', true, conn) < 0 ||
		pqPutnchar(buf.data, buf.len, conn) < 0 ||
		pqPutMsgEnd(conn) < 0)
	{
		pqHandleSendFailure(conn);
		return 0;
	}

	return 1;
}

/*
 * HandleSendQueryTree
 *
 * send global snapshot and query (and query_tree if any),
 * don't wait response
 *
 * return 0 if any trouble
 * return 1 if OK
 */
int
HandleSendQueryTree(NodeHandle *handle,
					CommandId cid,
					Snapshot snapshot,
					const char *query,
					StringInfo query_tree)
{
	const char *tree_data = NULL;
	size_t		tree_len = 0;

	Assert(handle);
	if (query_tree)
	{
		tree_data = query_tree->data;
		tree_len = query_tree->len;
	}

	return HandleSendCID(handle, cid) &&
		   HandleSendSnapshot(handle, snapshot) &&
		   PQsendQueryTree(handle->node_conn, query, tree_data, tree_len);
}

/*
 * HandleSendQueryExtend
 *
 * send global snapshot and query (and query_tree if any),
 * don't wait response
 *
 * return 0 if any trouble
 * return 1 if OK
 */
int
HandleSendQueryExtend(NodeHandle *handle,
					  CommandId cid,
					  Snapshot snapshot,
					  const char *command,
					  const char *stmtName,
					  const char *portalName,
					  bool sendDescribe,
					  int fetchSize,
					  int nParams,
					  const Oid *paramTypes,
					  const int *paramFormats,
					  const char *paramBinaryValue,
					  const int paramBinaryLength,
					  int nResultFormat,
					  const int *resultFormats)
{
	Assert(handle);

	return HandleSendCID(handle, cid) &&
		   HandleSendSnapshot(handle, snapshot) &&
		   PQsendQueryExtendBinary(handle->node_conn,
		   						   command,
		   						   stmtName,
		   						   portalName,
		   						   sendDescribe,
		   						   fetchSize,
		   						   nParams,
		   						   paramTypes,
		   						   paramFormats,
		   						   paramBinaryValue,
		   						   paramBinaryLength,
		   						   nResultFormat,
		   						   resultFormats);
}

/*
 * HandleSendClose
 *
 * send close message and don't wait response
 *
 * return 0 if any trouble
 * return 1 if OK
 */
int
HandleSendClose(NodeHandle *handle, bool isStatement, const char *name)
{
	Assert(handle);
	return PQsendClose(handle->node_conn, isStatement, name);
}

/*
 * HandleClose
 *
 * send close message and wait response
 *
 * return 0 if any trouble
 * return 1 if OK
 */
int
HandleClose(NodeHandle *handle, bool isStatement, const char *name)
{
	if (!HandleSendClose(handle, isStatement, name) ||
		!HandleFinishCommand(handle,
			isStatement ? CLOSE_STMT_TAG : CLOSE_PORTAL_TAG))
	{
		HandleGC(handle);
		return 0;
	}

	return 1;
}

/*
 * HandleListClose
 *
 * send close message and wait response
 *
 */
void
HandleListClose(List *handle_list, bool isStatement, const char *name)
{
	NodeHandle	   *handle;
	ListCell	   *lc_handle;
	StringInfoData	errbuf;

	initStringInfo(&errbuf);
	foreach(lc_handle, handle_list)
	{
		handle = (NodeHandle *) lfirst(lc_handle);
		if (!HandleClose(handle, isStatement, name))
			appendStringInfo(&errbuf, "%s\n", PQerrorMessage(handle->node_conn));
	}

	if (errbuf.len > 0)
		ereport(ERROR, (errmsg("%s", errbuf.data)));

	pfree(errbuf.data);
}

/*
 * HandleResetOwner
 *
 * collect garbage for PGconn of handle and reset
 * its owner (RemoteQuertState).
 */
void
HandleResetOwner(NodeHandle * handle)
{
	if (handle) {
		HandleGC(handle);
		handle->node_owner = NULL;
	}
}

/*
 * HandleListResetOwner
 *
 * collect garbage for PGconn of each handle of handle_list,
 * and reset its owner (RemoteQuertState).
 */
void
HandleListResetOwner(List * handle_list)
{
	ListCell   *lc_handle;
	NodeHandle *handle;

	foreach (lc_handle, handle_list)
	{
		handle = (NodeHandle *) lfirst(lc_handle);
		HandleGC(handle);
		handle->node_owner = NULL;
	}
}

/*
 * HandleListFinishCommand
 *
 * receive all reponse of "handle_list"
 *
 * return false if any handle in trouble
 * return true if all success
 */
bool
HandleListFinishCommand(const List *handle_list, const char *commandTag)
{
	NodeHandle	   *handle;
	ListCell	   *lc_handle;
	bool			all_success = true;

	foreach (lc_handle, handle_list)
	{
		handle = (NodeHandle *) lfirst(lc_handle);
		Assert(handle->node_conn);
		all_success &= HandleFinishCommand(handle, commandTag);
	}

	return all_success;
}

/*
 * HandleFinishCommand
 *
 * receive COMMAND response
 *
 * return false if any trouble
 * return true if COMMAND OK
 */
bool
HandleFinishCommand(NodeHandle *handle, const char *commandTag)
{
	CommandResult result;

	Assert(handle);
	result.command_ok = false;
	result.completionTag[0] = '\0';

	(void) PQNOneExecFinish(handle->node_conn, HandleFinishCommandHook, &result, true);

	if (result.command_ok && commandTag && commandTag[0])
	{
		/* TODO:
		 * Check whether the completionTag of result match
		 * the commandTag.
		 */
	}

	return result.command_ok;
}

static bool
HandleFinishCommandHook(void *context, struct pg_conn *conn, PQNHookFuncType type, ...)
{
	va_list args;

	switch(type)
	{
		case PQNHFT_ERROR:
			return PQNEFHNormal(NULL, conn, type);
		case PQNHFT_COPY_OUT_DATA:
			Assert(false);
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
					else if (status == PGRES_COMMAND_OK)
					{
						CommandResult *result = (CommandResult *) context;

						result->command_ok = true;
						strcpy(result->completionTag, res->cmdStatus);
					}
				}
				va_end(args);
			}
			break;
	}
	return false;
}