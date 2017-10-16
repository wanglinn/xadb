/*-------------------------------------------------------------------------
 *
 * inter-comm.c
 *	  Internode transaction routines
 *
 *
 * Portions Copyright (c) 2016-2017, ADB Development Group
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/intercomm/inter-xact.c
 *
 *-------------------------------------------------------------------------
 */
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
	NIL,						/* list of nodes already start transaction */
	NULL,						/* NodeMixHandle for the current query in the inter transaction block */
	NULL						/* NodeMixHandle for the whole inter transaction block */
};

static void InterXactTwoPhase(const char *gid, Oid *nodes, int nnodes, TwoPhaseState tp_state, bool missing_ok, bool ignore_error);
static void InterXactPrepareInternal(InterXactState state);
static void InterXactCommitInternal(InterXactState state);
static void InterXactAbortInternal(InterXactState state);

/*
 * IsTwoPhaseCommitNeeded
 *
 * return true if current inter transaction need transaction block
 * return false if not
 */
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

/*
 * GetTopInterXactGID
 *
 * return prepared GID of top inter transaction
 */
const char *
GetTopInterXactGID(void)
{
	InterXactState state = &TopInterXactStateData;

	return state->gid;
}

/*
 * IsTopInterXactHastmp
 *
 * return true if top transaction has temporary obejects
 * return false if not
 */
bool
IsTopInterXactHastmp(void)
{
	InterXactState state = &TopInterXactStateData;

	return state->hastmp;
}

/*
 * TopInterXactTmpSet
 *
 * Mark top inter transaction whether it contains temporary objects
 */
void
TopInterXactTmpSet(bool hastmp)
{
	InterXactState state = &TopInterXactStateData;

	state->hastmp = hastmp;
}

/*
 * ResetInterXactState
 *
 * release resources and reset to initial values
 */
void
ResetInterXactState(InterXactState state)
{
	if (state)
	{
		if (state->error)
			resetStringInfo(state->error);
		if (state->gid)
			pfree(state->gid);
		list_free(state->trans_nodes);
		FreeMixHandle(state->mix_handle);
		FreeMixHandle(state->all_handle);
		state->block_state = IBLOCK_DEFAULT;
		state->gid = NULL;
		state->missing_ok = false;
		state->hastmp = false;
		state->implicit = false;
		state->ignore_error = false;
		state->need_xact_block = false;
		state->trans_nodes = NIL;
		state->mix_handle = NULL;
		state->all_handle = NULL;
	}
}

/*
 * FreeInterXactState
 *
 * release resources of InterXactState
 */
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
		list_free(state->trans_nodes);
		FreeMixHandle(state->mix_handle);
		FreeMixHandle(state->all_handle);
		pfree(state);
	}
}

/*
 * MakeTopInterXactState
 *
 * return a clear top inter transaction state
 */
InterXactState
MakeTopInterXactState(void)
{
	InterXactState state = &TopInterXactStateData;
	ResetInterXactState(state);
	state->context = TopMemoryContext;

	return state;
}

/*
 * MakeInterXactState
 *
 * return a new inter transaction state
 */
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
	state->trans_nodes = NIL;
	if (node_list)
	{
		NodeMixHandle  *mix_handle;
		int				mix_num;

		mix_num = list_length(node_list);
		mix_handle = GetMixedHandles(node_list, state);
		Assert(mix_handle && list_length(mix_handle->handles) == mix_num);

		state->mix_handle = mix_handle;
		/*
		 * generate a new "all_handle"
		 */
		state->all_handle = ConcatMixHandle(state->all_handle, mix_handle);
	}

	(void) MemoryContextSwitchTo(old_context);

	return state;
}

/*
 * MakeInterXactState2
 *
 * return InterXactState which "mix_handle" is filled by oid_list
 */
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

/*
 * ExecInterXactUtility
 *
 * execute remote utility by InterXactState.
 */
InterXactState
ExecInterXactUtility(RemoteQuery *node, InterXactState state)
{
	ExecDirectType	exec_direct_type;
	ExecNodes	   *exec_nodes;
	bool			force_autocommit;
	bool			read_only;
	bool			need_xact_block;
	List		   *node_list;

	Assert(node);
	exec_direct_type = node->exec_direct_type;
	exec_nodes = node->exec_nodes;
	force_autocommit = node->force_autocommit;
	read_only = node->read_only;

	if (force_autocommit || read_only)
		need_xact_block = false;
	else
		need_xact_block = true;
	if (exec_direct_type == EXEC_DIRECT_UTILITY)
	{
		need_xact_block = false;

		/* This check is not done when analyzing to limit dependencies */
		if (IsTransactionBlock())
			ereport(ERROR,
					(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
					 errmsg("cannot run EXECUTE DIRECT with utility inside a transaction block")));
	}

	node_list = GetRemoteNodeList(NULL, exec_nodes, node->exec_type);
	/* no need to do next */
	if (!node_list)
		return state;

	/* Make up InterXactStateData */
	state = MakeInterXactState2(state, node_list);
	state->need_xact_block = need_xact_block;
	state->hastmp = node->is_temp;
	state->combine_type = node->combine_type;
	pfree(node_list);

	PG_TRY();
	{
		/* Utility */
		InterXactUtility(state,
						 GetActiveSnapshot(),
						 node->sql_statement,
						 node->sql_node);
	} PG_CATCH();
	{
		InterXactGC(state);
		PG_RE_THROW();
	} PG_END_TRY();

	return state;
}

/*
 * InterXactSetGID
 *
 * set inter transaction state prepared GID
 */
void
InterXactSetGID(InterXactState state, const char *gid)
{
	Assert(state);
	if (gid)
		state->gid = MemoryContextStrdup(state->context, gid);
}

/*
 * InterXactSaveBeginNodes
 *
 * save nodes which start transaction
 */
void
InterXactSaveBeginNodes(InterXactState state, Oid node)
{
	MemoryContext	old_context;

	Assert(state);
	old_context = MemoryContextSwitchTo(state->context);
	state->trans_nodes = list_append_unique_oid(state->trans_nodes, node);
	(void) MemoryContextSwitchTo(old_context);
}

/*
 * InterXactBeginNodes
 *
 * return all node oids which has been started transaction of "state"
 */
Oid *
InterXactBeginNodes(InterXactState state, bool include_self, int *node_num)
{
	NodeMixHandle	   *all_handle;
	List			   *node_list;
	Oid				   *res;

	if (!IsCoordMaster() || state == NULL)
	{
		if (node_num)
			*node_num = 0;
		return NULL;
	}

	all_handle = state->all_handle;
	if (!all_handle)
	{
		if (node_num)
			*node_num = 0;
		return NULL;
	}

	node_list = NIL;
	if (include_self)
		node_list = lappend_oid(node_list, PGXCNodeOid);
	node_list = list_concat_unique_oid(node_list, state->trans_nodes);

	res = OidListToArrary(NULL, node_list, node_num);
	list_free(node_list);

	return res;
}

/*
 * InterXactSaveError
 *
 * keep error message in "state"
 */
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

/*
 * InterXactSaveHandleError
 *
 * keep error message of "handle" in "state"
 */
void
InterXactSaveHandleError(InterXactState state, NodeHandle *handle)
{
	Assert(state && handle);

	InterXactSaveError(state, "error from \"%s\": %s",
		NameStr(handle->node_name), PQerrorMessage(handle->node_conn));
}

/*
 * InterXactSerializeSnapshot
 *
 * serialize snapshort for inter transaction state
 */
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
 * InterXactGC
 *
 * garbage collection for InterXactState
 */
void
InterXactGC(InterXactState state)
{
	NodeMixHandle	   *mix_handle;
	NodeHandle		   *handle;
	ListCell		   *lc_handle;

	Assert(state && state->mix_handle);
	mix_handle = state->mix_handle;
	foreach (lc_handle, mix_handle->handles)
	{
		handle = (NodeHandle *) lfirst(lc_handle);
		HandleGC(handle);
	}
}

/*
 * InterXactUtility
 *
 * execute utility by InterXactState
 */
void
InterXactUtility(InterXactState state, Snapshot snapshot,
				 const char *utility, StringInfo utility_tree)
{
	GlobalTransactionId gxid;
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
		if (!HandleBegin(state, handle, gxid, timestamp, need_xact_block, &already_begin) ||
			!HandleSendQueryTree(handle, InvalidCommandId, snapshot, utility, utility_tree))
		{
			state->block_state |= IBLOCK_ABORT;
			InterXactSaveHandleError(state, handle);
			break;
		}
		involved_handles = lappend(involved_handles, handle);
	}
	/* Not all nodes perform successfully */
	if (state->block_state & IBLOCK_ABORT ||
		!HandleListFinishCommand(involved_handles, NULL_TAG))
	{
		list_free(involved_handles);
		InterXactGC(state);
		ereport(ERROR,
				(errmsg("Could not process query on involved nodes"),
				 errdetail("%s", state->error->data)));
	}

	list_free(involved_handles);
	state->block_state |= (IBLOCK_BEGIN | IBLOCK_INPROGRESS);
}

/*
 * InterXactPrepare
 *
 * prepare a transaction by InterXactState
 */
void
InterXactPrepare(const char *gid, Oid *nodes, int nnodes)
{
	InterXactTwoPhase(gid, nodes, nnodes, TP_PREPARE, false, false);
}

/*
 * InterXactCommit
 *
 * commit a transaction by InterXactState
 */
void
InterXactCommit(const char *gid, Oid *nodes, int nnodes, bool missing_ok)
{
	InterXactTwoPhase(gid, nodes, nnodes, TP_COMMIT, missing_ok, false);
}

/*
 * InterXactAbort
 *
 * rollback a transaction by InterXactState
 */
void
InterXactAbort(const char *gid, Oid *nodes, int nnodes, bool missing_ok, bool ignore_error)
{
	InterXactTwoPhase(gid, nodes, nnodes, TP_ABORT, missing_ok, ignore_error);
}

/*
 * InterXactTwoPhase
 */
static void
InterXactTwoPhase(const char *gid, Oid *nodes, int nnodes, TwoPhaseState tp_state, bool missing_ok, bool ignore_error)
{
	InterXactState	state;
	List		   *node_list;

	node_list = OidArraryToList(NULL, nodes, nnodes);
	if (!node_list)
		return ;

	node_list = list_delete_oid(node_list, PGXCNodeOid);

	state = MakeInterXactState(NULL, (const List *) node_list);
	InterXactSetGID(state, gid);
	state->ignore_error = ignore_error;
	state->missing_ok = missing_ok;
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
		if (!HandleSendQueryTree(handle, InvalidCommandId, NULL, prepare_cmd, NULL))
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
		!HandleListFinishCommand(involved_handles, TRANS_PREPARE_TAG))
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
	const char		   *cmdTag;
	char			   *commit_cmd;
	bool				error_occured;

	Assert(state);
	all_handle = state->all_handle;
	gid = state->gid;
	if (!all_handle)
		return ;

	if (gid && gid[0])
	{
		commit_cmd = psprintf("COMMIT PREPARED%s '%s';", state->missing_ok ? " IF EXISTS" : "", gid);
		cmdTag = TRANS_COMMIT_PREPARED_TAG;
	} else
	{
		commit_cmd = psprintf("COMMIT TRANSACTION;");
		cmdTag = TRANS_COMMIT_TAG;
	}
	involved_handles = NIL;
	error_occured = false;
	foreach (lc_handle, all_handle->handles)
	{
		handle = (NodeHandle *) lfirst(lc_handle);
		if (!HandleSendQueryTree(handle, InvalidCommandId, NULL, commit_cmd, NULL))
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
		!HandleListFinishCommand(involved_handles, cmdTag))
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
	const char		   *cmdTag;
	char			   *abort_cmd;
	bool				error_occured;

	Assert(state);
	all_handle = state->all_handle;
	gid = state->gid;
	if (!all_handle)
		return ;

	if (gid && gid[0])
	{
		abort_cmd = psprintf("ROLLBACK PREPARED%s '%s';", state->missing_ok ? " IF EXISTS" : "", gid);
		cmdTag = TRANS_ROLLBACK_PREPARED_TAG;
	} else
	{
		abort_cmd = psprintf("ROLLBACK TRANSACTION;");
		cmdTag = TRANS_ROLLBACK_TAG;
	}
	involved_handles = NIL;
	error_occured = false;
	foreach (lc_handle, all_handle->handles)
	{
		handle = (NodeHandle *) lfirst(lc_handle);
		if (!HandleSendQueryTree(handle, -1, NULL, abort_cmd, NULL))
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
		!HandleListFinishCommand(involved_handles, cmdTag))
	{
		list_free(involved_handles);
		InterXactGC(state);
		/* ignore any error and continue */
		if (!state->ignore_error)
			ereport(ERROR,
					(errmsg("Could not abort transaction on involved nodes"),
					 errdetail("%s", state->error->data)));
		return ;
	}

	list_free(involved_handles);
	if (!error_occured)
		state->block_state |= IBLOCK_ABORT_END;
}

/*-------------remote xact include inter xact and agtm xact-------------------*/
static void CommitPreparedRxact(const char *gid, int nnodes, Oid *nodes, bool isMissingOK);
static void AbortPreparedRxact(const char *gid, int nnodes, Oid *nodes, bool missing_ok);

void
RemoteXactCommit(int nnodes, Oid *nodes)
{
	if (!IsCoordMaster())
		return ;

	InterXactCommit(NULL, nodes, nnodes, false);
	agtm_CommitTransaction(NULL, false);
}

void
RemoteXactAbort(int nnodes, Oid *nodes, bool normal)
{
	if (!IsCoordMaster())
		return ;

	InterXactAbort(NULL, nodes, nnodes, false, !normal);
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
		InterXactCommit(gid, nodes, nnodes, isMissingOK);

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
		InterXactAbort(gid, nodes, nnodes, isMissingOK, false);

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
