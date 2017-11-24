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
#include "access/transam.h"
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
	COMBINE_TYPE_NONE,			/* inter transaction block combine type */
	NULL,						/* two-phase GID */
	false,						/* is GID missing ok in the second phase of two-phase commit? */
	false,						/* is there any temporary object in the inter transaction? */
	false,						/* is the inter transaction implicit two-phase commit? */
	false,						/* true if ignore any error(try best to finish)) */
	false,						/* is the inter transaction start any transaction block? */
	NULL,						/* array of remote nodes already start transaction */
	0,							/* count of remote nodes already start transaction */
	0,							/* max count of remote nodes already malloc */
	NULL,						/* NodeMixHandle for the current query in the inter transaction block */
	NULL						/* NodeMixHandle for the whole inter transaction block */
};

static void InterXactTwoPhase(const char *gid, Oid *nodes, int nnodes, TwoPhaseState tp_state, bool missing_ok, bool ignore_error);
static void InterXactPrepareInternal(InterXactState state);
static void InterXactCommitInternal(InterXactState state);
static void InterXactAbortInternal(InterXactState state);

/*
 * GetPGconnAttatchTopInterXact
 *
 * Get all involved PGconn by node_list and attatch them
 * to the top inter transaction state.
 *
 * return a list of PGconn
 */
List *
GetPGconnAttatchTopInterXact(const List *node_list)
{
	InterXactState	state = GetTopInterXactState();

	state = MakeInterXactState2(state, node_list);

	return GetPGconnFromHandleList(state->mix_handle->handles);
}

/*
 * GetPGconnFromHandleList
 *
 * Get list of PGconn by NodeHandle list.
 *
 * return a list of PGconn
 */
List *
GetPGconnFromHandleList(List *handle_list)
{
	NodeHandle	   *handle;
	ListCell	   *lc_handle;
	List		   *conn_list = NIL;

	foreach (lc_handle, handle_list)
	{
		handle = (NodeHandle *) lfirst(lc_handle);
		conn_list = lappend(conn_list, handle->node_conn);
	}

	return conn_list;
}

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
	InterXactState state = GetTopInterXactState();

	if (hastmp)
		state->hastmp = true;
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
		if (state->trans_nodes)
			MemSet(state->trans_nodes, 0, sizeof(Oid) * state->trans_max);
		FreeMixHandle(state->mix_handle);
		FreeMixHandle(state->all_handle);
		state->gid = NULL;
		state->missing_ok = false;
		state->hastmp = false;
		state->implicit = false;
		state->ignore_error = false;
		state->need_xact_block = false;
		state->trans_count = 0;
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
		if (state->trans_nodes)
			pfree(state->trans_nodes);
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
	state->gid = NULL;
	state->missing_ok = false;
	state->hastmp = false;
	state->implicit = false;
	state->ignore_error = false;
	state->need_xact_block = false;
	state->trans_nodes = NULL;
	state->trans_count = 0;
	state->trans_max = 0;
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

	/*
	 * Check current "mix_handle" of InterXactState is just for
	 * the "node_list". if so, just return the "state" directly.
	 */
	if (state->mix_handle &&
		list_length(state->mix_handle->handles) == list_length(node_list))
	{
		NodeHandle *handle;
		ListCell   *lc_handle;
		bool		equal = true;

		foreach (lc_handle, state->mix_handle->handles)
		{
			handle = (NodeHandle *) lfirst(lc_handle);
			if (!list_member_oid(node_list, handle->node_id))
			{
				equal = false;
				break;
			}
		}

		if (equal)
			return state;
	}

	/*
	 * It is necessary to construct current mix handle for
	 * the "node_list".
	 */
	Assert(state->context);
	old_context = MemoryContextSwitchTo(state->context);
	mix_num = list_length(node_list);
	mix_handle = GetMixedHandles(node_list, state);
	Assert(mix_handle && list_length(mix_handle->handles) == mix_num);

	/*
	 * free previous "mix_handle" and keep the new one in "state".
	 */
	FreeMixHandle(state->mix_handle);
	state->mix_handle = mix_handle;

	/*
	 * generate a new "all_handle" every time.
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
	if (node->is_temp)
		state->hastmp = true;
	state->combine_type = node->combine_type;
	pfree(node_list);

	/* Utility */
	InterXactUtility(state,
					 GetActiveSnapshot(),
					 node->sql_statement,
					 node->sql_node);

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
 * InterXactSetXID
 *
 * set inter transaction state prepared GID by transaction ID
 */
void
InterXactSetXID(InterXactState state, TransactionId xid)
{
	Assert(state);

	if (TransactionIdIsValid(xid))
	{
		MemoryContext old_context;
		old_context = MemoryContextSwitchTo(state->context);
		state->gid = psprintf("T%u", xid);
		(void) MemoryContextSwitchTo(old_context);
	}
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
	int				i, new_max;

	Assert(state);
	for (i = 0; i < state->trans_count; i++)
	{
		/* return if already exists */
		if (state->trans_nodes[i] == node)
			return ;
	}
	/* a new node will be saved */
	old_context = MemoryContextSwitchTo(state->context);
	if (state->trans_max == 0)
	{
		Assert(state->trans_count == 0);
		new_max = 16;
		state->trans_nodes = (Oid *) palloc(sizeof(Oid) * new_max);
		state->trans_max = new_max;
	} else
	if (state->trans_count >= state->trans_max)
	{
		new_max = state->trans_max + 16;
		state->trans_nodes = (Oid *) repalloc(state->trans_nodes, sizeof(Oid) * new_max);
		state->trans_max = new_max;
	}
	state->trans_nodes[state->trans_count++] = node;
	Assert(state->trans_count <= state->trans_max);
	(void) MemoryContextSwitchTo(old_context);
}

/*
 * InterXactBeginNodes
 *
 * return all node oids of "state" which has been started transaction
 */
Oid *
InterXactBeginNodes(InterXactState state, bool include_self, int *node_num)
{
	NodeMixHandle	   *all_handle;
	Oid				   *res;
	int					node_cnt;

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

	if (include_self)
	{
		node_cnt = state->trans_count + 1;
		res = palloc(sizeof(Oid) * node_cnt);
		memcpy(res, state->trans_nodes, sizeof(Oid) * state->trans_count);
		res[state->trans_count] = PGXCNodeOid;
	} else
	{
		node_cnt = state->trans_count;
		res = state->trans_nodes;
	}

	if (node_num)
		*node_num = node_cnt;

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

	Assert(state && state->mix_handle);
	mix_handle = state->mix_handle;
	HandleListGC(mix_handle->handles);
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
	bool				already_begin;
	bool				need_xact_block;

	Assert(state);
	mix_handle = state->mix_handle;
	if (!mix_handle)
		return ;

	PG_TRY();
	{
		need_xact_block = state->need_xact_block;
		if (need_xact_block)
		{
			agtm_BeginTransaction();
			gxid = GetCurrentTransactionId();
		} else
			gxid = GetCurrentTransactionIdIfAny();
		timestamp = GetCurrentTransactionStartTimestamp();

		foreach (lc_handle, mix_handle->handles)
		{
			handle = (NodeHandle *) lfirst(lc_handle);
			if (!HandleBegin(state, handle, gxid, timestamp, need_xact_block, &already_begin) ||
				!HandleSendQueryTree(handle, InvalidCommandId, snapshot, utility, utility_tree) ||
				!HandleFinishCommand(handle, NULL_TAG))
			{
				ereport(ERROR,
						(errmsg("Fail to process utility query on remote node."),
						 errnode(NameStr(handle->node_name)),
						 errdetail("%s", HandleGetError(handle, false))));
			}
		}
	} PG_CATCH();
	{
		InterXactGC(state);
		PG_RE_THROW();
	} PG_END_TRY();
}

/*
 * InterXactBegin
 *
 * Begin transaction by InterXactState and node_list
 */
void
InterXactBegin(InterXactState state, const List *node_list)
{
	GlobalTransactionId gxid;
	TimestampTz			timestamp;
	InterXactState		new_state;
	NodeMixHandle	   *mix_handle;
	NodeHandle		   *handle;
	ListCell		   *lc_handle;
	bool				already_begin;
	bool				need_xact_block;

	new_state = MakeInterXactState2(state, node_list);
	if (!new_state || !new_state->mix_handle)
		return ;
	mix_handle = new_state->mix_handle;
	need_xact_block = state->need_xact_block;

	PG_TRY();
	{
		if (need_xact_block)
		{
			agtm_BeginTransaction();
			gxid = GetCurrentTransactionId();
		} else
			gxid = GetCurrentTransactionIdIfAny();
		timestamp = GetCurrentTransactionStartTimestamp();

		foreach (lc_handle, mix_handle->handles)
		{
			handle = (NodeHandle *) lfirst(lc_handle);
			if (!HandleBegin(state, handle, gxid, timestamp, need_xact_block, &already_begin))
				ereport(ERROR,
						(errmsg("Fail to begin transaction"),
						 errnode(NameStr(handle->node_name)),
						 errdetail("%s:", HandleGetError(handle, false))));
		}
	} PG_CATCH();
	{
		InterXactGC(new_state);
		PG_RE_THROW();
	} PG_END_TRY();
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
	list_free(node_list);
	InterXactSetGID(state, gid);
	state->ignore_error = ignore_error;
	state->missing_ok = missing_ok;

	PG_TRY();
	{
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
	} PG_CATCH();
	{
		InterXactGC(state);
		FreeInterXactState(state);
		PG_RE_THROW();
	} PG_END_TRY();
}

static void
InterXactPrepareInternal(InterXactState state)
{
	NodeMixHandle	   *all_handle;
	NodeHandle		   *handle;
	ListCell		   *lc_handle;
	PGconn			   *conn;
	const char		   *gid;
	char			   *prepare_cmd;

	Assert(state);
	all_handle = state->all_handle;
	gid = state->gid;
	if (!all_handle || !gid || !gid[0])
		return ;

	prepare_cmd = psprintf("PREPARE TRANSACTION '%s';", gid);
	foreach (lc_handle, all_handle->handles)
	{
		handle = (NodeHandle *) lfirst(lc_handle);
		conn = handle->node_conn;
		/* TODO: check invalid PQ transaction status  */
		if (PQtransactionStatus(conn) != PQTRANS_INTRANS)
			continue;
		if (!HandleSendQueryTree(handle, InvalidCommandId, NULL, prepare_cmd, NULL) ||
			!HandleFinishCommand(handle, TRANS_PREPARE_TAG))
		{
			/* ignore any error and continue */
			if (state->ignore_error)
				continue;

			pfree(prepare_cmd);
			ereport(ERROR,
					(errmsg("Fail to prepare transaction on remote node."),
					 errnode(NameStr(handle->node_name)),
					 errdetail("%s", HandleGetError(handle, false))));
		}
	}
	pfree(prepare_cmd);
}

static void
InterXactCommitInternal(InterXactState state)
{
	NodeMixHandle	   *all_handle;
	NodeHandle		   *handle;
	ListCell		   *lc_handle;
	const char		   *gid;
	const char		   *cmdTag;
	char			   *commit_cmd;

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
	foreach (lc_handle, all_handle->handles)
	{
		handle = (NodeHandle *) lfirst(lc_handle);
		if (!HandleSendQueryTree(handle, InvalidCommandId, NULL, commit_cmd, NULL) ||
			!HandleFinishCommand(handle, cmdTag))
		{
			/* ignore any error and continue */
			if (state->ignore_error)
				continue;

			pfree(commit_cmd);
			ereport(ERROR,
					(errmsg("Fail to commit%s transaction on remote node.",
							(gid && gid[0]) ? " prepared" : ""),
					 errnode(NameStr(handle->node_name)),
					 errdetail("%s", HandleGetError(handle, false))));
		}
	}
	pfree(commit_cmd);
}

static void
InterXactAbortInternal(InterXactState state)
{
	NodeMixHandle	   *all_handle;
	NodeHandle		   *handle;
	ListCell		   *lc_handle;
	const char		   *gid;
	const char		   *cmdTag;
	char			   *abort_cmd;

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
	foreach (lc_handle, all_handle->handles)
	{
		handle = (NodeHandle *) lfirst(lc_handle);
		if (!HandleSendQueryTree(handle, -1, NULL, abort_cmd, NULL) ||
			!HandleFinishCommand(handle, cmdTag))
		{
			/* ignore any error and continue */
			if (state->ignore_error)
				continue;

			pfree(abort_cmd);
			ereport(ERROR,
					(errmsg("Fail to rollback%s transaction on remote node.",
							(gid && gid[0]) ? " prepared" : ""),
					 errnode(NameStr(handle->node_name)),
					 errdetail("%s", HandleGetError(handle, false))));
		}
	}
	pfree(abort_cmd);
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
#if 0
		/* disconnect with rxact */
		DisconnectRemoteXact();
#endif
		/* record failed log */
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
#if 0
		/* disconnect with rxact */
		DisconnectRemoteXact();
#endif
		/* record failed log */
		RecordRemoteXactFailed(gid, RX_ROLLBACK);
		PG_RE_THROW();
	} PG_END_TRY();

	RecordRemoteXactSuccess(gid, RX_ROLLBACK);
}
