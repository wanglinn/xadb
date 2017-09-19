#include "postgres.h"

#include "access/rxact_mgr.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "agtm/agtm.h"
#include "agtm/agtm_client.h"
#include "datatype/timestamp.h"
#include "intercomm/inter-comm.h"
#include "libpq/libpq-fe.h"
#include "pgxc/execRemote.h"
#include "pgxc/pgxc.h"
#include "storage/ipc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

typedef enum
{
	TP_PREPARE	= 1 << 1,
	TP_COMMIT	= 1 << 2,
	TP_ABORT	= 1 << 3,
} TwoPhaseState;

static InterXactStateData TopInterXactStateData = {
	NULL,						/* current MemoryContext */
	NULL,						/* error stringinfo */
	IBLOCK_DEFAULT,				/* inter transaction block state */
	COMBINE_TYPE_NONE,			/* inter transaction block combine type */
	NULL,						/* two-phase GID */
	false,						/* is GID missing ok in the second phase of two-phase commit? */
	false,						/* is there any temporary object in the inter transaction? */
	false,						/* is the inter transaction implicit two-phase commit? */
	false,						/* true if ignore any error(try best to finish)) */
	false,						/* is the inter transaction start any transaction block? */
	NULL,						/* NodeMixHandle for the current query in the inter transaction block */
	NULL						/* NodeMixHandle for the whole inter transaction block */
};

static void InterXactSaveHandleError(InterXactState state, NodeHandle *handle);
static void InterXactGC(InterXactState state);
static void InterXactTwoPhase(const char *gid, Oid *nodes, int nnodes, TwoPhaseState tp_state, bool ignore_error);
static void InterXactPrepareInternal(InterXactState state);
static void InterXactCommitInternal(InterXactState state);
static void InterXactAbortInternal(InterXactState state);
static List *OidArraryToList(MemoryContext context, Oid *oids, int noids);
static Oid  *OidListToArrary(MemoryContext context, List *oid_list, int *noids);

bool
IsTwoPhaseCommitNeeded(void)
{
	InterXactState state = &TopInterXactStateData;

	if (state->hastmp)
	{
		elog(DEBUG1,
			 "Transaction accessed temporary objects - two-phase commit will not be "
			 "used, as that can lead to data inconsistencies in case of failures");
		return false;
	}

	return state->need_xact_block;
}

const char *
GetTopInterXactGID(void)
{
	InterXactState state = &TopInterXactStateData;

	return state->gid;
}

bool
IsTopInterXactHastmp(void)
{
	InterXactState state = &TopInterXactStateData;
	return state->hastmp;
}

void
TopInterXactTmpSet(bool hastmp)
{
	InterXactState state = &TopInterXactStateData;
	state->hastmp = hastmp;
}

void
ResetInterXactState(InterXactState state)
{
	if (state)
	{
		if (state->error)
			resetStringInfo(state->error);
		if (state->gid)
			pfree(state->gid);
		FreeMixHandle(state->mix_handle);
		FreeMixHandle(state->all_handle);
		state->block_state = IBLOCK_DEFAULT;
		state->gid = NULL;
		state->missing_ok = false;
		state->hastmp = false;
		state->implicit = false;
		state->ignore_error = false;
		state->need_xact_block = false;
		state->mix_handle = NULL;
		state->all_handle = NULL;
	}
}

void
FreeInterXactState(InterXactState state)
{
	if (state)
	{
		if (state->error)
		{
			pfree(state->error->data);
			pfree(state->error);
		}
		if (state->gid)
			pfree(state->gid);
		FreeMixHandle(state->mix_handle);
		FreeMixHandle(state->all_handle);
		pfree(state);
	}
}

InterXactState
MakeTopInterXactState(void)
{
	InterXactState state = &TopInterXactStateData;
	ResetInterXactState(state);
	state->context = TopMemoryContext;

	return state;
}

InterXactState
MakeInterXactState(MemoryContext context, const List *node_list)
{
	MemoryContext	old_context;
	InterXactState	state;

	context = context ? context : CurrentMemoryContext;
	old_context = MemoryContextSwitchTo(context);

	state = (InterXactState) palloc0(sizeof(InterXactStateData));
	state->context = context;
	state->block_state = IBLOCK_DEFAULT;
	state->gid = NULL;
	state->missing_ok = false;
	state->hastmp = false;
	state->implicit = false;
	state->ignore_error = false;
	state->need_xact_block = false;
	if (node_list)
	{
		NodeMixHandle  *mix_handle;
		int				mix_num;

		mix_num = list_length(node_list);
		mix_handle = GetMixedHandles(node_list, state);
		Assert(mix_handle && list_length(mix_handle->handles) == mix_num);
		/*
		 * free previous "mix_handle" and keep the new one in state.
		 */
		FreeMixHandle(state->mix_handle);
		state->mix_handle = mix_handle;
		/*
		 * generate a new "all_handle"
		 */
		state->all_handle = ConcatMixHandle(state->all_handle, mix_handle);
	}

	(void) MemoryContextSwitchTo(old_context);

	return state;
}

InterXactState
MakeInterXactState2(InterXactState state, const List *node_list)
{
	MemoryContext	old_context;
	NodeMixHandle  *mix_handle;
	int				mix_num;

	if (state == NULL)
		return MakeInterXactState(NULL, node_list);

	if (!node_list)
		return state;

	Assert(state->context);
	old_context = MemoryContextSwitchTo(state->context);
	mix_num = list_length(node_list);
	mix_handle = GetMixedHandles(node_list, state);
	Assert(mix_handle && list_length(mix_handle->handles) == mix_num);
	/*
	 * free previous "mix_handle" and keep the new one in state.
	 */
	FreeMixHandle(state->mix_handle);
	state->mix_handle = mix_handle;
	/*
	 * generate a new "all_handle"
	 */
	state->all_handle = ConcatMixHandle(state->all_handle, mix_handle);

	(void) MemoryContextSwitchTo(old_context);

	return state;
}

InterXactState
ExecInterXactUtility(RemoteQuery *node, InterXactState state)
{
	ExecDirectType	exec_direct_type;
	ExecNodes	   *exec_nodes;
	Snapshot		snapshot;
	bool			force_autocommit;
	bool			need_xact_block;
	List		   *node_list;

	Assert(node);
	exec_direct_type = node->exec_direct_type;
	exec_nodes = node->exec_nodes;
	force_autocommit = node->force_autocommit;

	need_xact_block = !force_autocommit;
	if (exec_direct_type == EXEC_DIRECT_UTILITY)
	{
		need_xact_block = false;

		/* This check is not done when analyzing to limit dependencies */
		if (IsTransactionBlock())
			ereport(ERROR,
					(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
					 errmsg("cannot run EXECUTE DIRECT with utility inside a transaction block")));
	}

	/* TODO: when exec_nodes is not null */
	if (exec_nodes)
	{
		ExecRemoteUtility(node);
		return NULL;
	}

	/* Get involved node oids */
	switch (node->exec_type)
	{
		case EXEC_ON_DATANODES:
			node_list = GetAllDnNids(false);
			break;
		case EXEC_ON_COORDS:
			node_list = GetAllCnNids(false);
			break;
		case EXEC_ON_ALL_NODES:
			node_list = GetAllNodeIds(false);
			break;
		case EXEC_ON_NONE:
		default:
			Assert(false);
			break;
	}
	Assert(node_list);

	/* Make up InterXactStateData */
	state = MakeInterXactState2(state, node_list);
	state->need_xact_block = need_xact_block;
	state->hastmp = node->is_temp;
	state->combine_type = node->combine_type;
	pfree(node_list);

	/* BEGIN */
	InterXactBegin(state);

	/* Utility */
	snapshot = GetActiveSnapshot();
	InterXactQuery(state, snapshot, node->sql_statement, node->sql_node);

	return state;
}

void
InterXactSetGID(InterXactState state, const char *gid)
{
	Assert(state);
	if (gid)
		state->gid = MemoryContextStrdup(state->context, gid);
}

Oid *
InterXactBeginNodes(InterXactState state, bool include_self, int *node_num)
{
	NodeMixHandle	   *all_handle;
	NodeHandle		   *handle;
	ListCell		   *cell;
	List			   *node_list;
	PGconn			   *conn;
	Oid				   *res;

	if (!IsCoordMaster() || state == NULL)
	{
		if (node_num)
			*node_num = 0;
		return NULL;
	}

	all_handle = state->mix_handle;
	if (!all_handle)
	{
		if (node_num)
			*node_num = 0;
		return NULL;
	}

	node_list = NIL;
	if (include_self)
		node_list = lappend_oid(node_list, PGXCNodeOid);
	foreach (cell, all_handle->handles)
	{
		handle = (NodeHandle *) lfirst(cell);
		conn = handle->node_conn;
		switch (PQtransactionStatus(conn))
		{
			case PQTRANS_IDLE:
				break;
			case PQTRANS_INTRANS:
				node_list = lappend_oid(node_list, handle->node_id);
				break;
			default:
				Assert(false);
				break;
		}
	}

	res = OidListToArrary(NULL, node_list, node_num);
	list_free(node_list);

	return res;
}

void
InterXactSaveError(InterXactState state, const char *fmt, ...)
{
	va_list		args;

	Assert(state);
	if (!state->error)
	{
		MemoryContext old_context;

		old_context = MemoryContextSwitchTo(state->context);
		state->error = makeStringInfo();
		(void) MemoryContextSwitchTo(old_context);
	}
	va_start(args, fmt);
	appendStringInfoVA(state->error, fmt, args);
	va_end(args);
}

static void
InterXactSaveHandleError(InterXactState state, NodeHandle *handle)
{
	Assert(state && handle);

	InterXactSaveError(state, "error from \"%s\": %s",
		NameStr(handle->node_name), PQerrorMessage(handle->node_conn));
}

void
InterXactSerializeSnapshot(StringInfo buf, Snapshot snapshot)
{
	uint32			nval;
	int				i;
	AssertArg(buf && snapshot);

	/* RecentGlobalXmin */
	nval = htonl(RecentGlobalXmin);
	appendBinaryStringInfo(buf, (const char *) &nval, sizeof(TransactionId));
	/* xmin */
	nval = htonl(snapshot->xmin);
	appendBinaryStringInfo(buf, (const char *) &nval, sizeof(TransactionId));
	/* xmax */
	nval = htonl(snapshot->xmax);
	appendBinaryStringInfo(buf, (const char *) &nval, sizeof(TransactionId));
	/* curcid */
	nval = htonl(snapshot->curcid);
	appendBinaryStringInfo(buf, (const char *) &nval, sizeof(CommandId));
	/* xcnt */
	nval = htonl(snapshot->xcnt);
	appendBinaryStringInfo(buf, (const char *) &nval, sizeof(uint32));
	/* xip */
	for (i = 0; i < snapshot->xcnt; i++)
	{
		nval = htonl(snapshot->xip[i]);
		appendBinaryStringInfo(buf, (const char *) &nval, sizeof(TransactionId));
	}
	/* subxcnt */
	nval = htonl(snapshot->subxcnt);
	appendBinaryStringInfo(buf, (const char *) &nval, sizeof(int32));
	/* subxip */
	for (i = 0; i < snapshot->subxcnt; i++)
	{
		nval = htonl(snapshot->subxip[i]);
		appendBinaryStringInfo(buf, (const char *) &nval, sizeof(TransactionId));
	}
}

/*
 * garbage collection of InterXactState
 *
 * a state machine for PGconn->asyncStatus
 */
static void
InterXactGC(InterXactState state)
{
	NodeMixHandle	   *mix_handle;
	NodeHandle		   *handle;
	ListCell		   *lc_handle;
	PGconn			   *conn;
	WaitEVSetData		set;
	WaitEventElt	   *wee;
	PGresult		   *res;

	Assert(state && state->mix_handle);
	mix_handle = state->mix_handle;
	initWaitEVSetExtend(&set, 1);
	foreach (lc_handle, mix_handle->handles)
	{
		handle = (NodeHandle *) lfirst(lc_handle);
		conn = handle->node_conn;

		if (PQstatus(conn) == CONNECTION_BAD)
			continue;

		resetWaitEVSet(&set);
		addWaitEventBySock(&set, PQsocket(conn), WT_SOCK_READABLE);
		for (;;)
		{
			while (PQisBusy(conn))
			{
				(void) execWaitEVSet(&set, -1);
				wee = nthWaitEventElt(&set, 0);

				if (WEECanRead(wee))
					(void) PQconsumeInput(conn);
			}

			res = PQgetResult(conn);
			if (res == NULL)
				break;

			/*
			 * not care about the result, we just want PGASYNC_IDLE of
			 * the PGconn. before this, consumue them.
			 */
			PQclear(res);
		}
	}

	freeWaitEVSet(&set, false);
}

void
InterXactBegin(InterXactState state)
{
	GlobalTransactionId	gxid;
	TimestampTz			timestamp;
	NodeMixHandle	   *mix_handle;
	NodeHandle		   *handle;
	ListCell		   *lc_handle;
	List			   *involved_handles;
	bool				already_begin;
	bool				need_xact_block;

	Assert(state);
	mix_handle = state->mix_handle;
	if (!mix_handle)
		return ;

	need_xact_block = state->need_xact_block;
	if (need_xact_block)
	{
		agtm_BeginTransaction();
		gxid = GetCurrentTransactionId();
	} else
		gxid = GetCurrentTransactionIdIfAny();
	timestamp = GetCurrentTransactionStartTimestamp();

	involved_handles = NIL;
	foreach (lc_handle, mix_handle->handles)
	{
		handle = (NodeHandle *) lfirst(lc_handle);
		if (!HandleSendBegin(handle, gxid, timestamp, need_xact_block, &already_begin))
		{
			state->block_state |= IBLOCK_ABORT;
			InterXactSaveHandleError(state, handle);
			break;
		}
		if (!already_begin && need_xact_block)
			involved_handles = lappend(involved_handles, handle);
	}

	/* Not all nodes perform successfully */
	if (state->block_state & IBLOCK_ABORT ||
		!HandleFinishCommand(involved_handles))
	{
		list_free(involved_handles);
		InterXactGC(state);
		ereport(ERROR,
				(errmsg("Could not begin transaction on involved nodes"),
				 errdetail("%s", state->error->data)));
	}

	state->block_state |= IBLOCK_BEGIN;
}

void
InterXactQuery(InterXactState state, Snapshot snapshot,
			   const char *query, StringInfo query_tree)
{
	NodeMixHandle	   *mix_handle;
	NodeHandle		   *handle;
	ListCell		   *lc_handle;
	List			   *involved_handles;

	Assert(state);
	mix_handle = state->mix_handle;
	if (!mix_handle)
		return ;

	involved_handles = NIL;
	foreach (lc_handle, mix_handle->handles)
	{
		handle = (NodeHandle *) lfirst(lc_handle);
		if (!HandleSendQueryTree(handle, snapshot, query, query_tree))
		{
			state->block_state |= IBLOCK_ABORT;
			InterXactSaveHandleError(state, handle);
			break;
		}
		involved_handles = lappend(involved_handles, handle);
	}

	/* Not all nodes perform successfully */
	if (state->block_state & IBLOCK_ABORT ||
		!HandleFinishCommand(involved_handles))
	{
		list_free(involved_handles);
		InterXactGC(state);
		ereport(ERROR,
				(errmsg("Could not process query on involved nodes"),
				 errdetail("%s", state->error->data)));
	}

	list_free(involved_handles);
	state->block_state |= IBLOCK_INPROGRESS;
}

void
InterXactPrepare(const char *gid, Oid *nodes, int nnodes)
{
	InterXactTwoPhase(gid, nodes, nnodes, TP_PREPARE, false);
}

void
InterXactCommit(const char *gid, Oid *nodes, int nnodes)
{
	InterXactTwoPhase(gid, nodes, nnodes, TP_COMMIT, false);
}

void
InterXactAbort(const char *gid, Oid *nodes, int nnodes, bool ignore_error)
{
	InterXactTwoPhase(gid, nodes, nnodes, TP_ABORT, ignore_error);
}

static void
InterXactTwoPhase(const char *gid, Oid *nodes, int nnodes, TwoPhaseState tp_state, bool ignore_error)
{
	InterXactState	state;
	List		   *node_list = NIL;

	node_list = OidArraryToList(NULL, nodes, nnodes);
	if (!node_list)
		return ;

	node_list = list_delete_oid(node_list, PGXCNodeOid);

	state = MakeInterXactState(NULL, (const List *) node_list);
	InterXactSetGID(state, gid);
	state->ignore_error = ignore_error;
	switch (tp_state)
	{
		case TP_PREPARE:
			InterXactPrepareInternal(state);
			break;
		case TP_COMMIT:
			InterXactCommitInternal(state);
			break;
		case TP_ABORT:
			InterXactAbortInternal(state);
			break;
		default:
			Assert(false);
			break;
	}
	FreeInterXactState(state);
	list_free(node_list);
}

static void
InterXactPrepareInternal(InterXactState state)
{
	NodeMixHandle	   *all_handle;
	NodeHandle		   *handle;
	ListCell		   *lc_handle;
	List			   *involved_handles;
	PGconn			   *conn;
	const char		   *gid;
	char			   *prepare_cmd;
	bool				error_occured;

	Assert(state);
	all_handle = state->all_handle;
	gid = state->gid;
	if (!all_handle || !gid || !gid[0])
		return ;

	prepare_cmd = psprintf("PREPARE TRANSACTION '%s';", gid);
	involved_handles = NIL;
	error_occured = false;
	foreach (lc_handle, all_handle->handles)
	{
		handle = (NodeHandle *) lfirst(lc_handle);
		conn = handle->node_conn;
		if (PQtransactionStatus(conn) != PQTRANS_INTRANS)
			continue;
		if (!HandleSendQueryTree(handle, NULL, prepare_cmd, NULL))
		{
			error_occured = true;
			/* ignore any error and continue */
			if (state->ignore_error)
				continue;
			state->block_state |= IBLOCK_ABORT;
			InterXactSaveHandleError(state, handle);
			break;
		}
		involved_handles = lappend(involved_handles, handle);
	}
	pfree(prepare_cmd);

	/* Not all nodes perform successfully */
	if (state->block_state & IBLOCK_ABORT ||
		!HandleFinishCommand(involved_handles))
	{
		list_free(involved_handles);
		InterXactGC(state);
		ereport(ERROR,
				(errmsg("Could not prepare transaction '%s' on involved nodes", gid),
				 errdetail("%s", state->error->data)));
	}

	list_free(involved_handles);
	if (!error_occured)
		state->block_state |= IBLOCK_PREPARE;
}

static void
InterXactCommitInternal(InterXactState state)
{
	NodeMixHandle	   *all_handle;
	NodeHandle		   *handle;
	ListCell		   *lc_handle;
	List			   *involved_handles;
	const char		   *gid;
	char			   *commit_cmd;
	bool				error_occured;

	Assert(state);
	all_handle = state->all_handle;
	gid = state->gid;
	if (!all_handle)
		return ;

	if (gid && gid[0])
		commit_cmd = psprintf("COMMIT PREPARED%s '%s';", state->missing_ok ? " IF EXISTS" : "", gid);
	else
		commit_cmd = psprintf("COMMIT TRANSACTION;");
	involved_handles = NIL;
	error_occured = false;
	foreach (lc_handle, all_handle->handles)
	{
		handle = (NodeHandle *) lfirst(lc_handle);
		if (!HandleSendQueryTree(handle, NULL, commit_cmd, NULL))
		{
			error_occured = true;
			/* ignore any error and continue */
			if (state->ignore_error)
				continue;
			state->block_state |= IBLOCK_ABORT;
			InterXactSaveHandleError(state, handle);
			break;
		}
		involved_handles = lappend(involved_handles, handle);
	}
	pfree(commit_cmd);

	/* Not all nodes perform successfully */
	if (state->block_state & IBLOCK_ABORT ||
		!HandleFinishCommand(involved_handles))
	{
		list_free(involved_handles);
		InterXactGC(state);
		ereport(ERROR,
				(errmsg("Could not commit transaction on involved nodes"),
				 errdetail("%s", state->error->data)));
	}

	list_free(involved_handles);
	if (!error_occured)
		state->block_state |= IBLOCK_END;
}

static void
InterXactAbortInternal(InterXactState state)
{
	NodeMixHandle	   *all_handle;
	NodeHandle		   *handle;
	ListCell		   *lc_handle;
	List			   *involved_handles;
	const char		   *gid;
	char			   *abort_cmd;
	bool				error_occured;

	Assert(state);
	all_handle = state->all_handle;
	gid = state->gid;
	if (!all_handle)
		return ;

	if (gid && gid[0])
		abort_cmd = psprintf("ROLLBACK PREPARED%s '%s';", state->missing_ok ? " IF EXISTS" : "", gid);
	else
		abort_cmd = psprintf("ROLLBACK TRANSACTION;");
	involved_handles = NIL;
	error_occured = false;
	foreach (lc_handle, all_handle->handles)
	{
		handle = (NodeHandle *) lfirst(lc_handle);
		if (!HandleSendQueryTree(handle, NULL, abort_cmd, NULL))
		{
			error_occured = true;
			/* ignore any error and continue */
			if (state->ignore_error)
				continue;
			state->block_state |= IBLOCK_ABORT;
			InterXactSaveHandleError(state, handle);
			break;
		}
		involved_handles = lappend(involved_handles, handle);
	}
	pfree(abort_cmd);

	/* Not all nodes perform successfully */
	if (state->block_state & IBLOCK_ABORT ||
		!HandleFinishCommand(involved_handles))
	{
		list_free(involved_handles);
		InterXactGC(state);
		ereport(ERROR,
				(errmsg("Could not commit transaction on involved nodes"),
				 errdetail("%s", state->error->data)));
	}

	list_free(involved_handles);
	if (!error_occured)
		state->block_state |= IBLOCK_ABORT_END;
}

static List *
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

static Oid *
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
		if (noids)
			*noids = i;
		(void) MemoryContextSwitchTo(old_context);
	}

	return oids;
}

/*-------------remote xact include inter xact and agtm xact-------------------*/
static void CommitPreparedRxact(const char *gid, int nnodes, Oid *nodes, bool isMissingOK);
static void AbortPreparedRxact(const char *gid, int nnodes, Oid *nodes, bool missing_ok);

void
RemoteXactCommit(int nnodes, Oid *nodes)
{
	if (!IsCoordMaster())
		return ;

	InterXactCommit(NULL, nodes, nnodes);
	agtm_CommitTransaction(NULL, false);
}

void
RemoteXactAbort(int nnodes, Oid *nodes, bool normal)
{
	if (!IsCoordMaster())
		return ;

	InterXactAbort(NULL, nodes, nnodes, !normal);
	agtm_AbortTransaction(NULL, false, !normal);
}

/*
 * StartFinishPreparedRxact
 *
 * It is used to record log in remote xact manager.
 */
void
StartFinishPreparedRxact(const char *gid,
						 int nnodes,
						 Oid *nodes,
						 bool isImplicit,
						 bool isCommit)
{
	if (!IsCoordMaster())
		return ;

	if (IsConnFromRxactMgr())
		return ;

	AssertArg(gid && gid[0]);

	/*
	 * Record rollback prepared transaction log
	 */
	if (!isCommit)
	{
		RecordRemoteXact(gid, nodes, nnodes, RX_ROLLBACK);
		return ;
	}

	/*
	 * Remote xact manager has already recorded log
	 * if it is implicit commit prepared transaction.
	 *
	 * See EndRemoteXactPrepare.
	 */
	if (!isImplicit)
		RecordRemoteXact(gid, nodes, nnodes, RX_COMMIT);
}

/*
 * EndFinishPreparedRxact
 *
 * Here we trully commit/rollback prepare remote xact.
 */
void
EndFinishPreparedRxact(const char *gid,
					   int nnodes,
					   Oid *nodes,
					   bool isMissingOK,
					   bool isCommit)
{
	if (!IsCoordMaster())
		return ;

	if (IsConnFromRxactMgr())
		return ;

	AssertArg(gid && gid[0]);

	if (isCommit)
		CommitPreparedRxact(gid, nnodes, nodes, isMissingOK);
	else
		AbortPreparedRxact(gid, nnodes, nodes, isMissingOK);
}

static void
CommitPreparedRxact(const char *gid,
					int nnodes,
					Oid *nodes,
					bool isMissingOK)
{
	volatile bool fail_to_commit = false;

	PG_TRY_HOLD();
	{
		/* Commit prepared on remote nodes */
		InterXactCommit(gid, nodes, nnodes);

		/* Commit prepared on AGTM */
		agtm_CommitTransaction(gid, isMissingOK);
	} PG_CATCH_HOLD();
	{
		AtAbort_Twophase();
		/* Record failed log */
		RecordRemoteXactFailed(gid, RX_COMMIT);
		/* Discard error data */
		errdump();
		fail_to_commit = true;
	} PG_END_TRY_HOLD();

	/* Return if success */
	if (!fail_to_commit)
	{
		/* Record success log */
		RecordRemoteXactSuccess(gid, RX_COMMIT);
		return ;
	}

	PG_TRY();
	{
		/* Fail to commit then wait for rxact to finish it */
		RxactWaitGID(gid);
	} PG_CATCH();
	{
		/*
		 * Fail again and exit current process
		 *
		 * ADBQ:
		 *		Does it cause crash of other processes?
		 *		Are there some resources not be released?
		 */
		proc_exit(1);
	} PG_END_TRY();
}

static void
AbortPreparedRxact(const char *gid,
				   int nnodes,
				   Oid *nodes,
				   bool isMissingOK)
{
	PG_TRY();
	{
		/* rollback prepared on remote nodes */
		InterXactAbort(gid, nodes, nnodes, false);

		/* rollback prepared on AGTM */
		agtm_AbortTransaction(gid, isMissingOK, false);
	} PG_CATCH();
	{
		/* Record FAILED log */
		RecordRemoteXactFailed(gid, RX_ROLLBACK);
		PG_RE_THROW();
	} PG_END_TRY();

	RecordRemoteXactSuccess(gid, RX_ROLLBACK);
}
