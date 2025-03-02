/*-------------------------------------------------------------------------
 *
 * pause.c
 *
 *	 Cluster Pause/Unpause handling
 *
 * IDENTIFICATION
 *	  $$
 *
 *-------------------------------------------------------------------------
 */

#ifdef ADB

#include "postgres.h"
#include "access/table.h"
#include "pgxc/execRemote.h"
#include "pgxc/pause.h"
#include "pgxc/pgxc.h"
#include "storage/spin.h"
#include "miscadmin.h"
#include "pgxc/execRemote.h"
#include "pgxc/poolmgr.h"
#include "pgxc/nodemgr.h"
#include "nodes/makefuncs.h"
#include "catalog/pgxc_node.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/builtins.h"
#include "access/htup_details.h"
#include "utils/rel.h"
#include "catalog/indexing.h"
#include "intercomm/inter-node.h"
#include "intercomm/inter-comm.h"
#include "storage/shmem.h"

/* globals */
bool cluster_lock_held;
bool cluster_ex_lock_held;
char *pause_cluster_str = "SELECT PG_PAUSE_CLUSTER()";
char *unpause_cluster_str = "SELECT PG_UNPAUSE_CLUSTER()";

static void HandleClusterPause(bool pause, bool initiator);
static void ProcessClusterPauseRequest(bool pause);

ClusterLockInfo *ClustLinfo = NULL;

/*
 * ProcessClusterPauseRequest:
 *
 * Carry out select pg_pause_cluster()/select pg_unpause_cluster() request on a coordinator node
 */
static void
ProcessClusterPauseRequest(bool pause)
{
	char *action = pause? pause_cluster_str:unpause_cluster_str;

	if (!IsCnCandidate())
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("The \"%s\" message is expected to "
						"arrive at a coordinator from another coordinator",
						action)));

	elog(DEBUG2, "Received \"%s\" from a coordinator", action);

	/*
	 * If calling unpause, ensure that the cluster lock has already been held
	 * in exclusive mode
	 */
	if (!pause && !cluster_ex_lock_held)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Received an \"%s\" request when cluster not \"%s\"!", unpause_cluster_str, pause_cluster_str)));

	/*
	 * Enable/Disable local queries. We need to release the lock first
	 *
	 * TODO: Think of some timeout mechanism here, if the locking takes too
	 * much time...
	 */
	ReleaseClusterLock(pause? false:true);
	AcquireClusterLock(pause? true:false);

	if (pause)
		cluster_ex_lock_held = true;
	else
		cluster_ex_lock_held = false;

	elog(DEBUG2, "%s queries at the coordinator", pause? "Paused":"Resumed");

	return;
}

/*
 * HandleClusterPause:
 *
 * Any errors will be reported via ereport.
 */
static void
HandleClusterPause(bool pause, bool initiator)
{
	NodeHandle *handle;
	List *node_list;
	ListCell *lc_handle;
	NodeMixHandle *cur_handle;
	char *action = pause? pause_cluster_str:unpause_cluster_str;

	elog(DEBUG2, "Preparing coordinators for \"%s\"", action);

	if (pause && cluster_ex_lock_held)
	{
		ereport(NOTICE, (errmsg("cluster already paused")));

		/* Nothing to do */
		return;
	}

	if (!pause && !cluster_ex_lock_held)
	{
		ereport(NOTICE, (errmsg("Issue \"%s\" before calling \"%s\"", pause_cluster_str, unpause_cluster_str)));

		/* Nothing to do */
		return;
	}

	/*
	 * If we are one of the participating coordinators, just do the action
	 * locally and return
	 */
	if (!initiator)
	{
		ProcessClusterPauseRequest(pause);
		return;
	}
	if(pause)
		PoolManagerSetCommand(POOL_CMD_TEMP, NULL);

	/*
	 * Send SELECT PG_PAUSE_CLUSTER()/SELECT PG_UNPAUSE_CLUSTER() message to all the coordinators. We should send an
	 * asyncronous request, update the local ClusterLock and then wait for the remote
	 * coordinators to respond back
	 */
	node_list = GetAllCnIDL(false);
	cur_handle = GetMixedHandles(node_list, NULL);
	list_free(node_list);

	PG_TRY();
	{
		if (cur_handle)
		{
			foreach (lc_handle, cur_handle->handles)
			{
				handle = (NodeHandle *) lfirst(lc_handle);
				if (!HandleSendQueryTree(handle, InvalidCommandId, InvalidSnapshot
					, pause ? pause_cluster_str : unpause_cluster_str, NULL)
					|| !HandleFinishCommand(handle, NULL_TAG))
				{
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Fail to send query: \"%s\"", pause ? pause_cluster_str : unpause_cluster_str),
							 errnode(NameStr(handle->node_name)),
							 errdetail("%s", HandleGetError(handle))));
				}
			}

			HandleListGC(cur_handle->handles);
		}
		/*
		 * Disable/Enable local queries. We need to release the SHARED mode first
		 *
		 * TODO: Start a timer to cancel the request in case of a timeout
		 */
		ReleaseClusterLock(pause? false:true);
		AcquireClusterLock(pause? true:false);

		if (pause)
			cluster_ex_lock_held = true;
		else
			cluster_ex_lock_held = false;


		elog(DEBUG2, "%s queries at the driving coordinator", pause? "Paused":"Resumed");
	} PG_CATCH();
	{
		/*
		 * If "SELECT PG_PAUSE_CLUSTER()", issue "SELECT PG_UNPAUSE_CLUSTER()" on the reachable nodes. For failure
		 * in cases of unpause, might need manual intervention at the offending
		 * coordinator node (maybe do a pg_cancel_backend() on the backend
		 * that's holding the exclusive lock or something..)
		 */
		if (!pause)
			ereport(WARNING,
				 (errmsg("\"%s\" command failed on one or more coordinator nodes."
						" Manual intervention may be required!", unpause_cluster_str)));
		else
			ereport(WARNING,
				 (errmsg("\"%s\" command failed on one or more coordinator nodes."
						" Trying to \"%s\" reachable nodes now", pause_cluster_str, unpause_cluster_str)));

		if (cur_handle)
		{
			foreach (lc_handle, cur_handle->handles)
			{
				handle = (NodeHandle *) lfirst(lc_handle);
				if (!HandleSendQueryTree(handle, InvalidCommandId, InvalidSnapshot
					, unpause_cluster_str, NULL)
					|| !HandleFinishCommand(handle, NULL_TAG))
				{
						ereport(WARNING,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Fail to send query: \"%s\"", unpause_cluster_str),
							 errnode(NameStr(handle->node_name)),
							 errdetail("%s", HandleGetError(handle))));
				}
			}
		}
		/* cleanup locally.. */
		ReleaseClusterLock(true);
		AcquireClusterLock(false);
		cluster_ex_lock_held = false;
		if (cur_handle)
			HandleListGC(cur_handle->handles);
		PG_RE_THROW();
	} PG_END_TRY();

	elog(DEBUG2, "Successfully completed \"%s\" command on "
				 "all coordinator nodes", action);

	return;
}

Datum
pg_pause_cluster(PG_FUNCTION_ARGS)
{
	bool pause = true;
	char	*action = pause ? pause_cluster_str : unpause_cluster_str;
	bool	 initiator = true;

	elog(DEBUG2, "\"%s\" request received", action);
	/* Only a superuser can perform this activity on a cluster */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("\"%s\" command: must be a superuser", action)));

	/* Ensure that we are a coordinator */
	if (!IS_PGXC_COORDINATOR)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("\"%s\" command must be sent to a coordinator", action)));

	/*
	 * Did the command come directly to this coordinator or via another
	 * coordinator?
	 */
	if (IsConnFromCoord())
		initiator = false;
	HandleClusterPause(pause, initiator);

	PG_RETURN_BOOL(true);
}

Datum
pg_unpause_cluster(PG_FUNCTION_ARGS)
{
	bool pause = false;
	char	*action = pause ? pause_cluster_str : unpause_cluster_str;
	bool	 initiator = true;

	elog(DEBUG2, "\"%s\" request received", action);

	/* Only a superuser can perform this activity on a cluster */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("\"%s\" command: must be a superuser", action)));

	/* Ensure that we are a coordinator */
	if (!IS_PGXC_COORDINATOR)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("\"%s\" command must be sent to a coordinator", action)));

	/*
	 * Did the command come directly to this coordinator or via another
	 * coordinator?
	 */
	if (IsConnFromCoord())
		initiator = false;

	HandleClusterPause(pause, initiator);

	PG_RETURN_BOOL(true);
}

/*
 * If the backend is shutting down, cleanup the PAUSE cluster lock
 * appropriately. We do this before shutting down shmem, because this needs
 * LWLock and stuff
 */
void
PGXCCleanClusterLock(int code, Datum arg)
{
	cluster_lock_held = false;

	/* Do nothing if cluster lock not held */
	if (!cluster_ex_lock_held)
		return;
	/* Do nothing if we are not the initiator */
	if (IsConnFromCoord())
	{
		if (cluster_ex_lock_held)
			ReleaseClusterLock(true);
		return;
	}

	/* Release locally too. We do not want a dangling value in cl_holder_pid! */
	ReleaseClusterLock(true);
	cluster_ex_lock_held = false;
}

/* Report shared memory space needed by ClusterLockShmemInit */
Size
ClusterLockShmemSize(void)
{
	Size		size = 0;

	size = add_size(size, sizeof(ClusterLockInfo));

	return size;
}

/* Allocate and initialize cluster locking related shared memory */
void
ClusterLockShmemInit(void)
{
	bool		found;

	ClustLinfo = (ClusterLockInfo *)
		ShmemInitStruct("Cluster Lock Info", ClusterLockShmemSize(), &found);

	if (!found)
	{
		/* First time through, so initialize */
		MemSet(ClustLinfo, 0, ClusterLockShmemSize());
		SpinLockInit(&ClustLinfo->cl_mutex);
	}
}

/*
 * AcquireClusterLock
 *
 *  Based on the argument passed in, try to update the shared memory
 *  appropriately. In case the conditions cannot be satisfied immediately this
 *  function resorts to a simple sleep. We don't envision PAUSE CLUSTER to
 *  occur that frequently so most of the calls will come out immediately here
 *  without any sleeps at all
 *
 *  We could have used a semaphore to allow the processes to sleep while the
 *  cluster lock is held. But again we are really not worried about performance
 *  and immediate wakeups around PAUSE CLUSTER functionality. Using the sleep
 *  in an infinite loop keeps things simple yet correct
 */
void
AcquireClusterLock(bool exclusive)
{
	volatile ClusterLockInfo *clinfo = ClustLinfo;

	if (exclusive && cluster_ex_lock_held)
	{
		return;
	}

	/*
	 * In the normal case, none of the backends will ask for exclusive lock, so
	 * they will just update the cl_process_count value and exit immediately
	 * from the below loop
	 */
	for (;;)
	{
		bool wait = false;

		SpinLockAcquire(&clinfo->cl_mutex);

		if (!exclusive)
		{
			if (clinfo->cl_holder_pid != 0)
				wait = true;
		}
		else /* pause cluster handling */
		{
			if (clinfo->cl_holder_pid != 0)
			{
				SpinLockRelease(&clinfo->cl_mutex);
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("pause cluster already in progress")));
			}

			/*
			 * There should be no other process
			 * holding the lock including ourself
			 */
				clinfo->cl_holder_pid = MyProcPid;
		}
		SpinLockRelease(&clinfo->cl_mutex);

		/*
		 * We use a simple sleep mechanism. If pause cluster has been invoked,
		 * we are not worried about immediate performance characteristics..
		 */
		if (wait)
		{
			CHECK_FOR_INTERRUPTS();
			pg_usleep(100000L);
		}
		else /* Got the proper semantic read/write lock.. */
			break;
	}
}

/*
 * ReleaseClusterLock
 *
 * 		Update the shared memory appropriately across the release call. We
 * 		really do not need the bool argument, but it's there for some
 * 		additional sanity checking
 */
void
ReleaseClusterLock(bool exclusive)
{
	volatile ClusterLockInfo *clinfo = ClustLinfo;

	if (!exclusive)
		return;
	SpinLockAcquire(&clinfo->cl_mutex);
	if (exclusive)
	{
		/*
		 * Reset the holder pid. Any waiters in AcquireClusterLock will
		 * eventually come out of their sleep and notice this new value and
		 * move ahead
		 */
		clinfo->cl_holder_pid = 0;
	}
	SpinLockRelease(&clinfo->cl_mutex);
}

#endif
