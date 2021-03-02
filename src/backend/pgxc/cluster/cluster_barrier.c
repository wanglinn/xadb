/*-------------------------------------------------------------------------
 *
 * cluster_barrier.c
 *
 *	  Cluster Barrier handling for PITR
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Portions Copyright (c) 2017-2018 ADB Development Group
 *
 * IDENTIFICATION
 *	  src/backend/pgxc/cluster/cluster_barrier.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "intercomm/inter-comm.h"
#include "intercomm/inter-node.h"
#include "pgxc/cluster_barrier.h"
#include "pgxc/pgxc.h"
#include "storage/lwlock.h"

static void CreateClusterBarrierPrepare(const char *barrierID);
static void CreateClusterBarrierExecute(const char *barrierID);
static void CreateClusterBarrierEnd(const char *barrierID);
static const char *generate_cluster_barrier_id(const char *barrierID);
static void DoRemoteClusterBarrier(NodeMixHandle *mix_handle,
								   NodeType node_type,
								   char cmd_type,
								   const char *barrierID);


/*
 * Prepare ourselves for an incoming BARRIER. We must disable all new 2PC
 * commits and let the ongoing commits to finish. We then remember the
 * barrier id (so that it can be matched with the final END message) and
 * tell the driving Coordinator to proceed with the next step.
 *
 * A simple way to implement this is to grab a lock in an exclusive mode
 * while all other backend starting a 2PC will grab the lock in shared
 * mode. So as long as we hold the exclusive lock, no other backend start a
 * new 2PC and there can not be any 2PC in-progress. This technique would
 * rely on assumption that an exclusive lock requester is not starved by
 * share lock requesters.
 *
 * Note: To ensure that the 2PC are not blocked for a long time, we should
 * set a timeout. The lock should be release after the timeout and the
 * barrier should be canceled.
 */
static void
CreateClusterBarrierPrepare(const char *barrierID)
{
	if (!IsCnCandidate())
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("The CREATE CLUSTER BARRIER PREPARE message is expected to "
						"do at a Coordinator candidate")));

	LWLockAcquire(BarrierLock, LW_EXCLUSIVE);

	/*
	 * TODO Start a timer to terminate the pending barrier after a specified
	 * timeout
	 */
}

/*
 * Execute the CREATE BARRIER command. Write a BARRIER WAL record and flush the
 * WAL buffers to disk before returning to the caller. Writing the WAL record
 * does not guarantee successful completion of the barrier command.
 */
static void
CreateClusterBarrierExecute(const char *barrierID)
{
	XLogRecPtr recptr;

	if (!IsConnFromCoord())
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("The CREATE CLUSTER BARRIER EXECUTE message is expected to "
						"do at a Non-Coordinator")));

	XLogBeginInsert();
	XLogRegisterData((char *) barrierID, strlen(barrierID) + 1);
	recptr = XLogInsert(RM_BARRIER_ID, XLOG_CLUSTER_BARRIER_CREATE);
	XLogFlush(recptr);
}


/*
 * Mark the completion of an on-going barrier. We must have remembered the
 * barrier ID when we received the CREATE BARRIER PREPARE command
 */
static void
CreateClusterBarrierEnd(const char *barrierID)
{
	if (!IsCnCandidate())
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("The CREATE CLUSTER BARRIER END message is expected to "
						"do at a Coordinator candidate")));

	LWLockRelease(BarrierLock);

	/*
	 * TODO Stop the timer
	 */
}

static const char *
generate_cluster_barrier_id(const char *barrierID)
{
	/*
	 * If the caller can passed a NULL value, generate an id which is
	 * guaranteed to be unique across the cluster. We use a combination of
	 * the Coordinator node id and current timestamp.
	 */
	if (barrierID)
		return barrierID;
	else
	{
		TimestampTz ts = GetCurrentTimestamp();

#ifdef HAVE_INT64_TIMESTAMP
		return psprintf("%s_"INT64_FORMAT, PGXCNodeName, ts);
#else
		return psprintf("%s_%.0f", PGXCNodeName, ts);
#endif
	}
}

static void
DoRemoteClusterBarrier(NodeMixHandle *mix_handle,
					   NodeType node_type,
					   char cmd_type,
					   const char *barrierID)
{
	ListCell   *lc_handle = NULL;
	NodeHandle *handle = NULL;
	const char *cmd_str = NULL;

	Assert(mix_handle && mix_handle->handles);

	switch (cmd_type)
	{
		case CLUSTER_BARRIER_PREPARE:
			cmd_str = "PREPARE";
			break;
		case CLUSTER_BARRIER_EXECUTE:
			cmd_str = "EXECUTE";
			break;
		case CLUSTER_BARRIER_END:
			cmd_str = "END";
			break;
		default:
			Assert(false);
			break;
	}

	foreach (lc_handle, mix_handle->handles)
	{
		handle = (NodeHandle *) lfirst(lc_handle);
		if (!(handle->node_type & node_type))
			continue;

		if (!HandleClusterBarrier(handle, cmd_type, barrierID))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to do remote CLUSTER BARRIER %s \"%s\"",
					 		cmd_str, barrierID),
					 errnode(NameStr(handle->node_name)),
					 errdetail("%s", HandleGetError(handle))));
	}
}

void
InterCreateClusterBarrier(char cmd_type, const char *barrierID, CommandDest dest)
{
	QueryCompletion	qc;
	switch (cmd_type)
	{
		case CLUSTER_BARRIER_PREPARE:
			CreateClusterBarrierPrepare(barrierID);
			break;
		case CLUSTER_BARRIER_EXECUTE:
			CreateClusterBarrierExecute(barrierID);
			break;
		case CLUSTER_BARRIER_END:
			CreateClusterBarrierEnd(barrierID);
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Invalid CLUSTER BARRIER command \"%c\"", cmd_type)));
			break;
	}

	InitializeQueryCompletion(&qc);
	qc.commandTag = CMDTAG_BARRIER;
	qc.datum = CStringGetDatum(barrierID);
	EndCommand(&qc, dest, false);
}

void
ExecCreateClusterBarrier(const char *barrierID, QueryCompletion *qc)
{
	const char	   *barrier_id = NULL;
	List		   *node_list = NIL;
	NodeMixHandle  *mix_handle = NULL;
	XLogRecPtr		recptr;

	/*
	 * Ensure that cluster barrier will be done at a Master-coordinator.
	 */
	if (!IsCnMaster())
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("CREATE CLUSTER BARRIER command must be "
						"done at a Master-coordinator")));

	/*
	 * Get a barrier id if the user has not supplied it
	 */
	barrier_id = generate_cluster_barrier_id(barrierID);

	elog(DEBUG1, "start CREATE CLUSTER BARRIER \"%s\"", barrier_id);

	node_list = GetAllNodeIDL(false);
	mix_handle = GetMixedHandles(node_list, NULL);
	Assert(node_list && mix_handle);
	list_free(node_list);

	/*
	 * Step One. Prepare all Coordinators for upcoming barrier request
	 */
	DoRemoteClusterBarrier(mix_handle,
						   TYPE_CN_NODE,
						   CLUSTER_BARRIER_PREPARE,
						   barrier_id);

	/*
	 * Step two. Issue BARRIER command to all involved components, including
	 * Coordinators and Datanodes
	 */
	DoRemoteClusterBarrier(mix_handle,
						   TYPE_CN_NODE | TYPE_DN_NODE,
						   CLUSTER_BARRIER_EXECUTE,
						   barrier_id);

	LWLockAcquire(BarrierLock, LW_EXCLUSIVE);

	XLogBeginInsert();
	XLogRegisterData((char *) barrier_id, strlen(barrier_id) + 1);
	recptr = XLogInsert(RM_BARRIER_ID, XLOG_CLUSTER_BARRIER_CREATE);
	XLogFlush(recptr);

	LWLockRelease(BarrierLock);

	/*
	 * Step three. Inform Coordinators about a successfully completed barrier
	 */
	DoRemoteClusterBarrier(mix_handle,
						   TYPE_CN_NODE,
						   CLUSTER_BARRIER_END,
						   barrier_id);

	FreeMixHandle(mix_handle);

	if (qc)
		qc->datum = CStringGetDatum(barrier_id);
}
