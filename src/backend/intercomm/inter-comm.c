#include "postgres.h"

#include "intercomm/inter-comm.h"
#include "libpq/libpq-int.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "reduce/wait_event.h"

static StringInfo begin_cmd = NULL;

static const char *GenerateBeginQuery(void);
static bool HandleRecvCommand(NodeHandle *handle);

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

bool
HandleFinishCommand(const List *handle_list)
{
	NodeHandle	   *handle;
	ListCell	   *lc_handle;
	bool			all_success = true;

	foreach (lc_handle, handle_list)
	{
		handle = (NodeHandle *) lfirst(lc_handle);
		Assert(handle->node_conn);
		all_success &= HandleRecvCommand(handle);
	}

	return all_success;
}

int
HandleSendBegin(NodeHandle *handle,
				GlobalTransactionId xid,
				TimestampTz timestamp,
				bool need_xact_block,
				bool *alreay_begin)
{
	PGconn *conn;

	Assert(handle);
	conn = handle->node_conn;
	*alreay_begin = false;

	/*
	 * return if within transaction block.
	 */
	if (PQtransactionStatus(conn) == PQTRANS_INTRANS)
	{
		*alreay_begin = true;
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

static bool
HandleRecvCommand(NodeHandle *handle)
{
	PGresult	   *last_res = NULL;
	PGresult	   *res = NULL;
	PGconn		   *conn = NULL;
	WaitEventElt   *wee;
	WaitEVSetData	set;
	InterXactState	state;

	Assert(handle);
	conn = handle->node_conn;
	state = (InterXactState) handle->node_context;
	Assert(state);
	initWaitEVSetExtend(&set, 1);
	addWaitEventBySock(&set, PQsocket(conn), WT_SOCK_READABLE);
	for (;;)
	{
		while (PQisBusy(conn))
		{
			(void) execWaitEVSet(&set, -1);
			wee = nthWaitEventElt(&set, 0);
			Assert(wee);
			if (WEECanRead(wee) && !PQconsumeInput(conn))
			{
				InterXactSaveError(state, "%s", PQerrorMessage(conn));
				freeWaitEVSet(&set);
				return false;
			}
		}

		res = PQgetResult(conn);
		if (res == NULL)
			break;				/* query is complete */

		PQclear(last_res);
		last_res = res;
	}
	freeWaitEVSet(&set);

	if (PQresultStatus(last_res) == PGRES_COMMAND_OK)
		return true;

	InterXactSaveError(state, "%s", PQresultErrorMessage(last_res));
	PQclear(last_res);
	return false;
}

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

int
HandleSendQueryTree(NodeHandle *handle, Snapshot snapshot,
					const char *query, StringInfo query_tree)
{
	const char *tree_data = NULL;
	size_t		tree_len = 0;

	Assert(handle);
	if (query_tree)
	{
		tree_data = query_tree->data;
		tree_len = query_tree->len;
	}

	return HandleSendSnapshot(handle, snapshot) &&
		PQsendQueryTree(handle->node_conn, query, tree_data, tree_len);
}
