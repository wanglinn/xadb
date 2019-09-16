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

#include <time.h>

#include "access/rxact_mgr.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "agtm/agtm.h"
#include "agtm/agtm_client.h"
#include "datatype/timestamp.h"
#include "intercomm/inter-comm.h"
#include "libpq-fe.h"
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

#define INTER_TWO_PHASE_SEND		0x1		/* send SQL command */
#define INTER_TWO_PHASE_RECV		0x2		/* recv SQL command result */
#define INTER_TWO_PHASE_NO_ERROR	0x4		/* don't report send and recv error */
#define INTER_TWO_PHASE_MISS_OK		0x8		/* add "if exist" to SQL command */
#define INTER_TWO_PHASE_SEND_RECV	(INTER_TWO_PHASE_SEND|INTER_TWO_PHASE_RECV)

static InterXactStateData TopInterXactStateData = {
	NULL,						/* current MemoryContext */
	NULL,						/* two-phase GID */
	false,						/* is GID missing ok in the second phase of two-phase commit? */
	false,						/* is the inter transaction implicit two-phase commit? */
	false,						/* is the inter transaction start any transaction block? */
	NULL,						/* array of remote nodes already start transaction */
	0,							/* count of remote nodes already start transaction */
	0,							/* max count of remote nodes already malloc */
	NULL,						/* NodeMixHandle for the current query in the inter transaction block */
	NULL						/* NodeMixHandle for the whole inter transaction block */
};

static void ResetInterXactState(InterXactState state);
static const char* InterXactGetTransactionSQL(TwoPhaseState state, const char *gid, bool missing_ok, char **sql);
static void InterXactTwoPhase(const char *gid, Oid *nodes, int nnodes, TwoPhaseState tp_state, int tp_flags);
static void InterXactTwoPhaseGtm(const char *gid, Oid *nodes, int nnodes, TwoPhaseState tp_state, int tp_flags);
static void InterXactTwoPhaseInternal(List *handle_list, char *command, const char *command_tag, int tp_flags);

/*
 * GetPGconnAttatchCurrentInterXact
 *
 * Get all involved PGconn by node_list and attatch them
 * to the top inter transaction state.
 *
 * return a list of PGconn
 */
List *
GetPGconnAttatchCurrentInterXact(const List *node_list)
{
	InterXactState	state = GetCurrentInterXactState();

	state = MakeInterXactState2(state, node_list);

	return GetPGconnFromHandleList(state->cur_handle->handles);
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
 * ResetInterXactState
 *
 * release resources and reset to initial values
 */
static void
ResetInterXactState(InterXactState state)
{
	if (state)
	{
		if (state->gid)
			pfree(state->gid);
		if (state->trans_nodes)
			MemSet(state->trans_nodes, 0, sizeof(Oid) * state->trans_max);
		FreeMixHandle(state->cur_handle);
		FreeMixHandle(state->all_handle);
		state->gid = NULL;
		state->missing_ok = false;
		state->implicit = false;
		state->need_xact_block = false;
		state->trans_count = 0;
		state->cur_handle = NULL;
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
		if (state->gid)
			pfree(state->gid);
		if (state->trans_nodes)
			pfree(state->trans_nodes);
		FreeMixHandle(state->cur_handle);
		FreeMixHandle(state->all_handle);
		if (state != &TopInterXactStateData)
			pfree(state);
	}
}

InterXactState
MakeNewInterXactState(void)
{
	InterXactState state = (InterXactState)
		MemoryContextAllocZero(TopMemoryContext, sizeof(InterXactStateData));
	state->context = TopMemoryContext;

	return state;
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
	state->implicit = false;
	state->need_xact_block = false;
	state->trans_nodes = NULL;
	state->trans_count = 0;
	state->trans_max = 0;
	if (node_list)
	{
		NodeMixHandle  *cur_handle;
		int				mix_num;

		mix_num = list_length(node_list);
		cur_handle = GetMixedHandles(node_list, state);
		Assert(cur_handle && list_length(cur_handle->handles) == mix_num);

		state->cur_handle = cur_handle;

		/*
		 * generate a new "all_handle"
		 */
		state->all_handle = ConcatMixHandle(state->all_handle, cur_handle);
	}

	(void) MemoryContextSwitchTo(old_context);

	return state;
}

/*
 * MakeInterXactState2
 *
 * return InterXactState which "cur_handle" is filled by oid_list
 */
InterXactState
MakeInterXactState2(InterXactState state, const List *node_list)
{
	MemoryContext	old_context;
	NodeMixHandle  *cur_handle;
	int				mix_num;

	if (state == NULL)
		return MakeInterXactState(NULL, node_list);

	if (!node_list)
		return state;

	/*
	 * Check current "cur_handle" of InterXactState is just for
	 * the "node_list". if so, just return the "state" directly.
	 */
	if (state->cur_handle &&
		list_length(state->cur_handle->handles) == list_length(node_list))
	{
		NodeHandle *handle;
		ListCell   *lc_handle,
				   *lc_node;
		bool		equal = true;

		forboth(lc_handle, state->cur_handle->handles,
				lc_node, node_list)
		{
			handle = (NodeHandle *) lfirst(lc_handle);
			if (handle->node_id != lfirst_oid(lc_node))
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
	cur_handle = GetMixedHandles(node_list, state);
	Assert(cur_handle && list_length(cur_handle->handles) == mix_num);

	/*
	 * free previous "cur_handle" and keep the new one in "state".
	 */
	FreeMixHandle(state->cur_handle);
	state->cur_handle = cur_handle;

	/*
	 * generate a new "all_handle" every time.
	 */
	state->all_handle = ConcatMixHandle(state->all_handle, cur_handle);

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

	if (!IsCnMaster() || state == NULL)
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
 * InterXactGCCurrent
 *
 * garbage collection for current handles of InterXactState
 */
void
InterXactGCCurrent(InterXactState state)
{
	if (state)
	{
		NodeMixHandle	   *cur_handle;

		cur_handle = state->cur_handle;
		if (cur_handle)
			HandleListGC(cur_handle->handles);
	}
}

/*
 * InterXactGCAll
 *
 * garbage collection for all handles of InterXactState
 */
void
InterXactGCAll(InterXactState state)
{
	if (state)
	{
		NodeMixHandle	   *all_handle;
		all_handle = state->all_handle;
		if (all_handle)
			HandleListGC(all_handle->handles);
	}
}

/*
 * InterXactCacheCurrent
 *
 * Cache or GC for current handles of InterXactState
 */
void
InterXactCacheCurrent(InterXactState state)
{
	if (state)
	{
		NodeMixHandle	   *cur_handle;

		cur_handle = state->cur_handle;
		if (cur_handle)
			HandleListCacheOrGC(cur_handle->handles);
	}
}

/*
 * InterXactCacheAll
 *
 * Cache or GC for all handles of InterXactState
 */
void
InterXactCacheAll(InterXactState state)
{
	if (state)
	{
		NodeMixHandle	   *all_handle;

		all_handle = state->all_handle;
		if (all_handle)
			HandleListCacheOrGC(all_handle->handles);
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
	NodeMixHandle	   *cur_handle;
	NodeHandle		   *handle;
	ListCell		   *lc_handle;
	bool				already_begin;
	bool				need_xact_block;

	Assert(state);
	cur_handle = state->cur_handle;
	if (!cur_handle)
		return ;

	PG_TRY();
	{
		need_xact_block = state->need_xact_block;
		if (need_xact_block)
		{
			//agtm_BeginTransaction();
			gxid = GetCurrentTransactionId();
		} else
			gxid = GetCurrentTransactionIdIfAny();
		timestamp = GetCurrentTransactionStartTimestamp();

		/* Send utility query to remote nodes */
		foreach (lc_handle, cur_handle->handles)
		{
			handle = (NodeHandle *) lfirst(lc_handle);
			if (!HandleBegin(state, handle, gxid, timestamp, need_xact_block, &already_begin) ||
				!HandleSendQueryTree(handle, InvalidCommandId, snapshot, utility, utility_tree))
			{
				ereport(ERROR,
						(errmsg("Fail to send utility query to remote node."),
						 errnode(NameStr(handle->node_name)),
						 errdetail("%s", HandleGetError(handle))));
			}
		}

		/* Receive result of utility from remote nodes */
		foreach (lc_handle, cur_handle->handles)
		{
			handle = (NodeHandle *) lfirst(lc_handle);
			if (!HandleFinishCommand(handle, NULL_TAG))
			{
				ereport(ERROR,
						(errmsg("Fail to process utility query on remote node."),
						 errnode(NameStr(handle->node_name)),
						 errdetail("%s", HandleGetError(handle))));
			}
		}
	} PG_CATCH();
	{
		InterXactGCCurrent(state);
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
	NodeMixHandle	   *cur_handle;
	NodeHandle		   *handle;
	ListCell		   *lc_handle;
	bool				already_begin;
	bool				need_xact_block;

	new_state = MakeInterXactState2(state, node_list);
	if (!new_state || !new_state->cur_handle)
		return ;
	cur_handle = new_state->cur_handle;
	need_xact_block = state->need_xact_block;

	PG_TRY();
	{
		if (need_xact_block)
		{
			//agtm_BeginTransaction();
			gxid = GetCurrentTransactionId();
		} else
			gxid = GetCurrentTransactionIdIfAny();
		timestamp = GetCurrentTransactionStartTimestamp();

		foreach (lc_handle, cur_handle->handles)
		{
			handle = (NodeHandle *) lfirst(lc_handle);
			if (!HandleBegin(state, handle, gxid, timestamp, need_xact_block, &already_begin))
				ereport(ERROR,
						(errmsg("Fail to begin transaction"),
						 errnode(NameStr(handle->node_name)),
						 errdetail("%s:", HandleGetError(handle))));
		}
	} PG_CATCH();
	{
		InterXactGCCurrent(new_state);
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
	InterXactTwoPhase(gid, nodes, nnodes, TP_PREPARE, INTER_TWO_PHASE_SEND_RECV);
}

/*
 * InterXactPrepareGtm
 *
 * prepare a transaction by InterXactState
 */
void
InterXactPrepareGtm(const char *gid, Oid *nodes, int nnodes)
{
	InterXactTwoPhaseGtm(gid, nodes, nnodes, TP_PREPARE, INTER_TWO_PHASE_SEND_RECV);
}

/*
 * InterXactCommit
 *
 * commit a transaction by InterXactState
 */
void
InterXactCommit(const char *gid, Oid *nodes, int nnodes, bool missing_ok)
{
	int flags = INTER_TWO_PHASE_SEND_RECV;
	if (missing_ok)
		flags |= INTER_TWO_PHASE_MISS_OK;
	InterXactTwoPhase(gid, nodes, nnodes, TP_COMMIT, flags);
}

/*
InterXactCommitGtm(const char *gid, Oid *nodes, int nnodes, bool missing_ok)
 * InterXactCommitGtm
 *
 * commit a transaction by InterXactState
 */
void
InterXactCommitGtm(const char *gid, Oid *nodes, int nnodes, bool missing_ok)
{
	int flags = INTER_TWO_PHASE_SEND_RECV;
	if (missing_ok)
		flags |= INTER_TWO_PHASE_MISS_OK;
	InterXactTwoPhaseGtm(gid, nodes, nnodes, TP_COMMIT, flags);
}

void InterXactSendCommit(const char *gid, Oid *nodes, int nnodes, bool missing_ok, bool ignore_error)
{
	int flags = INTER_TWO_PHASE_SEND;
	if (missing_ok)
		flags |= INTER_TWO_PHASE_MISS_OK;
	if (ignore_error)
		flags |= INTER_TWO_PHASE_NO_ERROR;
	InterXactTwoPhase(gid, nodes, nnodes, TP_COMMIT, flags);
}

void InterXactRecvCommit(const char *gid, Oid *nodes, int nnodes, bool missing_ok, bool ignore_error)
{
	int flags = INTER_TWO_PHASE_RECV;
	if (missing_ok)
		flags |= INTER_TWO_PHASE_MISS_OK;
	if (ignore_error)
		flags |= INTER_TWO_PHASE_NO_ERROR;
	InterXactTwoPhase(gid, nodes, nnodes, TP_COMMIT, flags);
}

/*
 * InterXactAbort
 *
 * rollback a transaction by InterXactState
 */
void
InterXactAbort(const char *gid, Oid *nodes, int nnodes, bool missing_ok, bool normal)
{
	int flags = INTER_TWO_PHASE_SEND_RECV;
	if (!normal)
		flags |= INTER_TWO_PHASE_NO_ERROR;
	if (missing_ok)
		flags |= INTER_TWO_PHASE_MISS_OK;

	InterXactTwoPhase(gid, nodes, nnodes, TP_ABORT, flags);
}

/*
 * InterXactAbort
 *
 * rollback a transaction by InterXactState
 */
void
InterXactAbortGtm(const char *gid, Oid *nodes, int nnodes, bool missing_ok, bool normal)
{
	int flags = INTER_TWO_PHASE_SEND_RECV;
	if (!normal)
		flags |= INTER_TWO_PHASE_NO_ERROR;
	if (missing_ok)
		flags |= INTER_TWO_PHASE_MISS_OK;

	InterXactTwoPhaseGtm(gid, nodes, nnodes, TP_ABORT, flags);
}

/*
 * InterXactGetSQL
 * return command tag
 */
static const char*
InterXactGetTransactionSQL(TwoPhaseState state, const char *gid, bool missing_ok, char **command)
{
	const char *command_tag = NULL;

	switch (state)
	{
		case TP_PREPARE:
			if (gid && gid[0])
			{
				if (command)
					*command = psprintf("PREPARE TRANSACTION '%s';", gid);
				command_tag = TRANS_PREPARE_TAG;
			}else
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("prepare remote transaction not give id")));
			}
			break;
		case TP_COMMIT:
			if (gid && gid[0])
			{
				if (command)
					*command = psprintf("COMMIT PREPARED%s '%s';",
										missing_ok ? " IF EXISTS" : "", gid);
				command_tag = TRANS_COMMIT_PREPARED_TAG;
			} else
			{
				if (command)
					*command = pstrdup("COMMIT TRANSACTION;");
				command_tag = TRANS_COMMIT_TAG;
			}
			break;
		case TP_ABORT:
			if (gid && gid[0])
			{
				if (command)
					*command = psprintf("ROLLBACK PREPARED%s '%s';",
									missing_ok ? " IF EXISTS" : "", gid);
				command_tag = TRANS_ROLLBACK_PREPARED_TAG;
			} else
			{
				if (command)
					*command = pstrdup("ROLLBACK TRANSACTION;");
				command_tag = TRANS_ROLLBACK_TAG;
			}
			break;
		default:
			Assert(false);
			break;
	}

	return command_tag;
}

/*
 * InterXactTwoPhase
 */
static void
InterXactTwoPhaseGtm(const char *gid, Oid *nodes, int nnodes, TwoPhaseState tp_state, int tp_flags)
{
	List * volatile handle_list;
	char * volatile command = NULL;
	const char	   *command_tag;

	handle_list = GetGtmHandleList(nodes, nnodes, false, false, true, NULL);
	if (!handle_list)
		return ;

	PG_TRY();
	{
		command_tag = InterXactGetTransactionSQL(tp_state,
												 gid,
												 (tp_flags & INTER_TWO_PHASE_MISS_OK) ? true:false,
												 (char**)&command);
		Assert(command_tag != NULL && command != NULL);
		InterXactTwoPhaseInternal(handle_list, command, command_tag, tp_flags);
		pfree(command);
		list_free(handle_list);
	} PG_CATCH();
	{
		safe_pfree(command);
		HandleListGC(handle_list);
		list_free(handle_list);
		PG_RE_THROW();
	} PG_END_TRY();
}

/*
 * InterXactTwoPhase
 */
static void
InterXactTwoPhase(const char *gid, Oid *nodes, int nnodes, TwoPhaseState tp_state, int tp_flags)
{
	List * volatile handle_list;
	char * volatile command = NULL;
	const char	   *command_tag;

	handle_list = GetNodeHandleList(nodes, nnodes, false, false, true, NULL, true, tp_flags & INTER_TWO_PHASE_NO_ERROR ? false : true);
	if (!handle_list)
		return;

	PG_TRY();
	{
		command_tag = InterXactGetTransactionSQL(tp_state,
												 gid,
												 (tp_flags & INTER_TWO_PHASE_MISS_OK) ? true:false,
												 (char**)&command);
		Assert(command_tag != NULL && command != NULL);
		InterXactTwoPhaseInternal(handle_list, command, command_tag, tp_flags);
		pfree(command);
		list_free(handle_list);
	} PG_CATCH();
	{
		safe_pfree(command);
		HandleListGC(handle_list);
		list_free(handle_list);
		PG_RE_THROW();
	} PG_END_TRY();
}

static void
InterXactTwoPhaseInternal(List *handle_list, char *command, const char *command_tag, int tp_flags)
{
	NodeHandle	   *handle;
	ListCell	   *lc_handle;
	Assert((tp_flags & INTER_TWO_PHASE_SEND_RECV) != 0);

	if (tp_flags & INTER_TWO_PHASE_SEND)
	{
		foreach (lc_handle, handle_list)
		{
			handle = (NodeHandle *) lfirst(lc_handle);
			if (!HandleSendQueryTree(handle, InvalidCommandId, NULL, command, NULL))
			{
				if (tp_flags & INTER_TWO_PHASE_NO_ERROR)
					continue;

				ereport(ERROR,
						(errmsg("Fail send \"%s\" to remote node \"%s\".", command, NameStr(handle->node_name)),
						 errnode(NameStr(handle->node_name)),
						 errdetail("%s", HandleGetError(handle))));
			}
		}
	}
	if (tp_flags & INTER_TWO_PHASE_RECV)
	{
		foreach (lc_handle, handle_list)
		{
			handle = (NodeHandle *) lfirst(lc_handle);
			if (!HandleFinishCommand(handle, command_tag))
			{
				if (tp_flags & INTER_TWO_PHASE_NO_ERROR)
					continue;

				ereport(ERROR,
						(errmsg("Fail recv \"%s\" from remote node \"%s\".", command, NameStr(handle->node_name)),
						 errnode(NameStr(handle->node_name)),
						 errdetail("%s", HandleGetError(handle))));
			}
		}
	}
}

/*-------------remote xact include inter xact and agtm xact-------------------*/
static void CommitPreparedRxact(const char *gid, int nnodes, Oid *nodes, bool isMissingOK);
static void AbortPreparedRxact(const char *gid, int nnodes, Oid *nodes, bool missing_ok);

void
RemoteXactCommit(int nnodes, Oid *nodes)
{
	if (!IsCnMaster())
		return ;

	InterXactCommit(NULL, nodes, nnodes, false);
	//InterXactCommitGtm(NULL, nodes, nnodes, false);
	//agtm_CommitTransaction(NULL, false);
}

void
RemoteXactAbort(int nnodes, Oid *nodes, bool normal)
{
	if (!IsCnMaster())
		return ;

	InterXactAbort(NULL, nodes, nnodes, false, normal);
	//(NULL, nodes, nnodes, false, normal);
	//agtm_AbortTransaction(NULL, false, !normal);
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
						 bool isCommit)
{
	if (!IsCnMaster())
		return ;

	if (IsConnFromRxactMgr())
		return ;

	AssertArg(gid && gid[0]);

	/*
	 * Record commit/rollback prepared transaction log
	 */
	if (isCommit)
		RecordRemoteXact(gid, nodes, nnodes, RX_COMMIT, false);
	else
		RecordRemoteXact(gid, nodes, nnodes, RX_ROLLBACK, false);
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
	if (!IsCnMaster())
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
		//(gid, nodes, nnodes, isMissingOK);
		//agtm_CommitTransaction(gid, isMissingOK);
	} PG_CATCH_HOLD();
	{
		AtAbort_Twophase();

		/* record failed log */
		if (RecordRemoteXactFailed(gid, RX_COMMIT, true) == false)
			DisconnectRemoteXact();
		/* Discard error data */
		errdump();
		fail_to_commit = true;
	} PG_END_TRY_HOLD();

	/* Return if success */
	if (!fail_to_commit)
	{
		/* Record success log */
		if (RecordRemoteXactSuccess(gid, RX_COMMIT, true) == false)
			fail_to_commit = true;
	}
	if(fail_to_commit)
	{
		time_t end_time = time(NULL) + 5;	/* wait 5 seconds */
		for(;;)
		{
			pg_usleep(1000*100);	/* sleep 0.1 second */
			if (RxactWaitGID(gid, true))
				return;
			if (time(NULL) > end_time)
				proc_exit(EXIT_FAILURE);
		}
	}
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
		InterXactAbort(gid, nodes, nnodes, isMissingOK, true);

		/* rollback prepared on AGTM */
		agtm_AbortTransaction(gid, isMissingOK, false);
	} PG_CATCH();
	{
		/* record failed log */
		if (RecordRemoteXactFailed(gid, RX_ROLLBACK, true) == false)
			DisconnectRemoteXact();
		PG_RE_THROW();
	} PG_END_TRY();

	RecordRemoteXactSuccess(gid, RX_ROLLBACK, false);
}
