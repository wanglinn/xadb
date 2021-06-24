/*-------------------------------------------------------------------------
 *
 * varsup.c
 *	  postgres OID & XID variables support routines
 *
 * Copyright (c) 2000-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/transam/varsup.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "commands/dbcommands.h"
#include "miscadmin.h"
#include "postmaster/autovacuum.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "utils/syscache.h"
#include "replication/snapreceiver.h"

#ifdef ADB
#include "pgxc/pgxc.h"
#include "commands/copy.h"	/* for SimpleNextCopyFromNewFE */
#include "executor/clusterReceiver.h"	/* for CLUSTER_MSG_TRANSACTION_ID */
#include "executor/execCluster.h"		/* for in_cluster_mode */
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "replication/snapsender.h"
#include "storage/procarray.h"

/*
 * Parameters as below are used only in Datanode or NoMaster-Coordinator.
 */
/* ADBQ:
 * How to obtain global xid under the situation sub transaction
 * is involved. what about making gxid list?
 */
static GlobalTransactionId GlobalXid = InvalidGlobalTransactionId;
static bool GlobalXidSetFromCOOR = false;

/* Global xid force from AGTM to obtain  */
static bool ForceObtainXidFromAGTM = false;
#endif /* ADB */

/* Number of OIDs to prefetch (preallocate) per XLOG write */
#define VAR_OID_PREFETCH		8192

/* pointer to "variable cache" in shared memory (set up by shmem.c) */
VariableCache ShmemVariableCache = NULL;

#ifdef ADB
void
SetGlobalTransactionId(GlobalTransactionId gxid)
{
	Assert(!IsCnMaster());
	GlobalXid = gxid;
	GlobalXidSetFromCOOR = true;
}

void
UnsetGlobalTransactionId(void)
{
	GlobalXid = InvalidGlobalTransactionId;
	GlobalXidSetFromCOOR = false;
	//ForceObtainXidFromAGTM = false;
}

void
SetForceObtainXidFromAGTM(bool val)
{
	ForceObtainXidFromAGTM = val;
}

/*
 * See if we should force using AGTM
 * Useful for explicit VACUUM FULL
 */
bool
GetForceXidFromAGTM(void)
{
	return ForceObtainXidFromAGTM;
}

static GlobalTransactionId
ObtainGlobalTransactionId(bool isSubXact)
{
	GlobalTransactionId gxid = InvalidGlobalTransactionId;

	/*
	 * Master-Coordinator get xid from AGTM
	 */
	if (IsCnMaster())
	{
		gxid =  SnapRcvGetGlobalTransactionId(isSubXact);
		return gxid;
	}

	/*
	 * Datanode or NoMaster-Coordinator get xid
	 * from AGTM when
	 * a. GlobalXid is invalid.
	 * b. Current process is autovacuum process.
	 * c. Xid force from AGTM to obtain.
	 */
	if (GlobalXid == InvalidGlobalTransactionId ||
		GlobalXidSetFromCOOR == false ||
		ForceObtainXidFromAGTM == true ||
		IsAnyAutoVacuumProcess())
	{
		gxid =  SnapRcvGetGlobalTransactionId(isSubXact);
		return gxid;
	}

	/*
	 * Datanode or NoMaster-Coordinator return GlobalXid got
	 * from Master-Coordinator.
	 */
	return GlobalXid;
}

static TransactionId GetXidFromCoord(int level)
{
	StringInfoData buf;
	TransactionId xid;

	if (level == 1 &&
		GlobalXidSetFromCOOR &&
		GlobalXid != InvalidGlobalTransactionId)
		return GlobalXid;

	if (in_cluster_mode == false)
	{
		/*
		 * only in COPY BOTH mode can request transaction id,
		 * CLUSTER COMMANDS is in COPY BOTH mode
		 */
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("not in cluster command mode")));
	}

	initStringInfo(&buf);
	appendStringInfoChar(&buf, CLUSTER_MSG_TRANSACTION_ID);
	appendBinaryStringInfoNT(&buf, (char*)&level, sizeof(level));
	pq_putmessage('d', buf.data, buf.len);

	resetStringInfo(&buf);
	GetCopyDataFromNewFE(&buf, true, false);
	
	if (pq_getmsgbyte(&buf) != CLUSTER_MSG_TRANSACTION_ID)
		goto failed_get_xid_;
	pq_copymsgbytes(&buf, (char*)&xid, sizeof(xid));
	pq_getmsgend(&buf);

	return xid;

failed_get_xid_:
	ereport(ERROR,
			(errcode(ERRCODE_PROTOCOL_VIOLATION),
			 errmsg("invalid coordinator message format")));
	return InvalidTransactionId;	/* keep compler quiet */
}

FullTransactionId
GetNewGlobalTransactionId(int level)
{
	TransactionId gxid;
	FullTransactionId full_gxid;
	bool isSubXact = level > 1;
	static int last_vacuum_trigger_num = -1;

	/*
	 * Workers synchronize transaction state at the beginning of each parallel
	 * operation, so we can't account for new XIDs after that point.
	 */
	if (IsInParallelMode())
		elog(ERROR, "cannot assign TransactionIds during a parallel operation");

	/*
	 * During bootstrap initialization, we return the special bootstrap
	 * transaction id.
	 */
	if (IsBootstrapProcessingMode())
	{
		Assert(!isSubXact);
		MyProc->xid = BootstrapTransactionId;
		ProcGlobal->xids[MyProc->pgxactoff] = BootstrapTransactionId;
		return FullTransactionIdFromEpochAndXid(0, BootstrapTransactionId);
	}

	/* safety check, we should never get this far in a HS slave */
	if (RecoveryInProgress())
		elog(ERROR, "cannot assign TransactionIds during recovery");

	if (IsConnFromCoord())
	{
		if (MyProc->statusFlags & PROC_IS_CLUSTER_VACUUM_TOAST)
		{
			/* only for CLUSTER VACUUM TOAST table, it get XID from GTM directly */
			if (IsGTMNode())
			{
				gxid = XidFromFullTransactionId(GetNewTransactionId(isSubXact));
				elog(DEBUG5, "GTM CLUSTER vacuum toast table Get txid %u from local\n", gxid);
			}
			else
			{
				gxid = ObtainGlobalTransactionId(isSubXact);
				elog(DEBUG5, "CLUSTER vacuum toast table Get txid %u from GTM\n", gxid);
			}
		}
		else if (MyProc->statusFlags & PROC_IS_CLUSTER_VACUUM)
		{
			/* only for CLUSTER VACUUM  not TOAST table */
			gxid = GetClusterVacuumAnalyzeXid();
			elog(DEBUG5, "Get txid %u from cluster vacuum analyze\n", gxid);
		}
		else
			gxid = GetXidFromCoord(level);
	}
	else
		gxid = ObtainGlobalTransactionId(isSubXact);
	/*----------
	 * Check to see if it's safe to assign another XID.  This protects against
	 * catastrophic data loss due to XID wraparound.  The basic rules are:
	 *
	 * If we're past xidVacLimit, start trying to force autovacuum cycles.
	 * If we're past xidWarnLimit, start issuing warnings.
	 * If we're past xidStopLimit, refuse to execute transactions, unless
	 * we are running in single-user mode (which gives an escape hatch
	 * to the DBA who somehow got past the earlier defenses).
	 *
	 * Note that this coding also appears in GetNewMultiXactId.
	 *----------
	 * CN master has thro warning already
	 */
	if (!IsConnFromCoord() && TransactionIdFollowsOrEquals(gxid, ShmemVariableCache->xidVacLimit))
	{
		/*
		 * For safety's sake, we release XidGenLock while sending signals,
		 * warnings, etc.  This is not so much because we care about
		 * preserving concurrency in this situation, as to avoid any
		 * possibility of deadlock while doing get_database_name(). First,
		 * copy all the shared values we'll need in this path.
		 */
		TransactionId xidWarnLimit = ShmemVariableCache->xidWarnLimit;
		TransactionId xidStopLimit = ShmemVariableCache->xidStopLimit;
		TransactionId xidWrapLimit = ShmemVariableCache->xidWrapLimit;
		Oid 		oldest_datoid = ShmemVariableCache->oldestXidDB;

		/*
		 * To avoid swamping the postmaster with signals, we issue the autovac
		 * request only once per 64K transaction starts.  This still gives
		 * plenty of chances before we get into real trouble.
		 */
		if (IsUnderPostmaster)
		{
			if (last_vacuum_trigger_num == -1 || last_vacuum_trigger_num == 65536)
			{
				SendPostmasterSignal(PMSIGNAL_START_AUTOVAC_LAUNCHER);
				last_vacuum_trigger_num = 0;
			}
			else
				last_vacuum_trigger_num++;
		}
			

		if (IsUnderPostmaster &&
			TransactionIdFollowsOrEquals(gxid, xidStopLimit))
		{
			char	   *oldest_datname = get_database_name(oldest_datoid);

			/* complain even if that DB has disappeared */
			if (oldest_datname)
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("database is not accepting commands to avoid wraparound data loss in database \"%s\"",
								oldest_datname),
						 errhint("Stop the postmaster and vacuum that database in single-user mode.\n"
								 "You might also need to commit or roll back old prepared transactions.")));
			else
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("database is not accepting commands to avoid wraparound data loss in database with OID %u",
								oldest_datoid),
						 errhint("Stop the postmaster and vacuum that database in single-user mode.\n"
								 "You might also need to commit or roll back old prepared transactions.")));
		}
		else if (TransactionIdFollowsOrEquals(gxid, xidWarnLimit))
		{
			char	   *oldest_datname = get_database_name(oldest_datoid);

			/* complain even if that DB has disappeared */
			if (oldest_datname)
				ereport(WARNING,
						(errmsg("database \"%s\" must be vacuumed within %u transactions",
								oldest_datname,
								xidWrapLimit - gxid),
						 errhint("To avoid a database shutdown, execute a database-wide VACUUM in that database.\n"
								 "You might also need to commit or roll back old prepared transactions.")));
			else
				ereport(WARNING,
						(errmsg("database with OID %u must be vacuumed within %u transactions",
								oldest_datoid,
								xidWrapLimit - gxid),
						 errhint("To avoid a database shutdown, execute a database-wide VACUUM in that database.\n"
								 "You might also need to commit or roll back old prepared transactions.")));
		}
	}

	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);


	full_gxid = FullTransactionIdFromEpochAndXid(EpochFromFullTransactionId(ShmemVariableCache->nextXid),
												 gxid);
	if (FullTransactionIdPrecedesOrEquals(ShmemVariableCache->nextXid, full_gxid))
	{
		/*
		 * If we are allocating the first XID of a new page of the commit log,
		 * zero out that commit-log page before returning. We must do this while
		 * holding XidGenLock, else another xact could acquire and commit a later
		 * XID before we zero the page.  Fortunately, a page of the commit log
		 * holds 32K or more transactions, so we don't have to do this very often.
		 *
		 * Extend pg_subtrans and pg_commit_ts too.
		 */
		ExtendCLOG(gxid);
		ExtendCommitTs(gxid);
		ExtendSUBTRANS(gxid);

		ShmemVariableCache->nextXid = full_gxid;

		/*
		 * Now advance the nextXid counter.  This must not happen until after we
		 * have successfully completed ExtendCLOG() --- if that routine fails, we
		 * want the next incoming transaction to try it again.	We cannot assign
		 * more XIDs until there is CLOG space for them.
		 */
		FullTransactionIdAdvance(&ShmemVariableCache->nextXid);
	} else
	{
		/*
		 * Sometime, the global XID got from the AGTM may be used locally.
		 * It may lead to conflict of XidStatus of XID. For example, when
		 * we failover the AGTM master, the next XID of the AGTM slave may
		 * be less than the AGTM master.(It is possiable as XLOG has not
		 * been synchronized.)
		 *
		 * It is a fatal situation that global XID is less than
		 * ShmemVariableCache->nextXid.
		 */
		if (TransactionLogFetch(gxid) != TRANSACTION_STATUS_IN_PROGRESS)
		{
			LWLockRelease(XidGenLock);
			ereport(ERROR,
				(errmsg("Global XID %u is already in use, please try again!", gxid)));
		}
	}

	/*
	 * We must store the new XID into the shared ProcArray before releasing
	 * XidGenLock.  This ensures that every active XID older than
	 * latestCompletedXid is present in the ProcArray, which is essential for
	 * correct OldestXmin tracking; see src/backend/access/transam/README.
	 *
	 * Note that readers of ProcGlobal->xids/PGPROC->xid should be careful to
	 * fetch the value for each proc only once, rather than assume they can
	 * read a value multiple times and get the same answer each time.  Note we
	 * are assuming that TransactionId and int fetch/store are atomic.
	 *
	 * The same comments apply to the subxact xid count and overflow fields.
	 *
	 * Use of a write barrier prevents dangerous code rearrangement in this
	 * function; other backends could otherwise e.g. be examining my subxids
	 * info concurrently, and we don't want them to see an invalid
	 * intermediate state, such as an incremented nxids before the array entry
	 * is filled.
	 *
	 * Other processes that read nxids should do so before reading xids
	 * elements with a pg_read_barrier() in between, so that they can be sure
	 * not to read an uninitialized array element; see
	 * src/backend/storage/lmgr/README.barrier.
	 *
	 * If there's no room to fit a subtransaction XID into PGPROC, set the
	 * cache-overflowed flag instead.  This forces readers to look in
	 * pg_subtrans to map subtransaction XIDs up to top-level XIDs. There is a
	 * race-condition window, in that the new XID will not appear as running
	 * until its parent link has been placed into pg_subtrans. However, that
	 * will happen before anyone could possibly have a reason to inquire about
	 * the status of the XID, so it seems OK.  (Snapshots taken during this
	 * window *will* include the parent XID, so they will deliver the correct
	 * answer later on when someone does have a reason to inquire.)
	 */
	if (!isSubXact)
	{
		Assert(ProcGlobal->subxidStates[MyProc->pgxactoff].count == 0);
		Assert(!ProcGlobal->subxidStates[MyProc->pgxactoff].overflowed);
		Assert(MyProc->subxidStatus.count == 0);
		Assert(!MyProc->subxidStatus.overflowed);

		/* LWLockRelease acts as barrier */
		MyProc->xid = gxid;
		ProcGlobal->xids[MyProc->pgxactoff] = gxid;
	}
	else
	{
		XidCacheStatus *substat = &ProcGlobal->subxidStates[MyProc->pgxactoff];
		int			nxids = MyProc->subxidStatus.count;

		Assert(substat->count == MyProc->subxidStatus.count);
		Assert(substat->overflowed == MyProc->subxidStatus.overflowed);

		if (nxids < PGPROC_MAX_CACHED_SUBXIDS)
		{
			MyProc->subxids.xids[nxids] = gxid;
			pg_write_barrier();
			MyProc->subxidStatus.count = substat->count = nxids + 1;
		}
		else
			MyProc->subxidStatus.overflowed = substat->overflowed = true;
	}

	LWLockRelease(XidGenLock);

	elog(DEBUG1, "Return new global xid: %u", gxid);

	return full_gxid;
}
#endif

/*
 * Allocate the next FullTransactionId for a new transaction or
 * subtransaction.
 *
 * The new XID is also stored into MyProc->xid/ProcGlobal->xids[] before
 * returning.
 *
 * Note: when this is called, we are actually already inside a valid
 * transaction, since XIDs are now not allocated until the transaction
 * does something.  So it is safe to do a database lookup if we want to
 * issue a warning about XID wrap.
 */
FullTransactionId
GetNewTransactionId(bool isSubXact)
{
#ifdef ADB
	return GetNewTransactionIdExt(isSubXact, 1, true, true);
}
FullTransactionId GetNewTransactionIdExt(bool isSubXact, uint32 xidnum, bool isInsertXact, bool isNeedAssign)
{
	int	i;
#endif
	FullTransactionId full_xid;
	TransactionId xid;

	/*
	 * Workers synchronize transaction state at the beginning of each parallel
	 * operation, so we can't account for new XIDs after that point.
	 */
	if (IsInParallelMode())
		elog(ERROR, "cannot assign TransactionIds during a parallel operation");

	/*
	 * During bootstrap initialization, we return the special bootstrap
	 * transaction id.
	 */
	if (IsBootstrapProcessingMode())
	{
		Assert(!isSubXact);
		MyProc->xid = BootstrapTransactionId;
		ProcGlobal->xids[MyProc->pgxactoff] = BootstrapTransactionId;
		return FullTransactionIdFromEpochAndXid(0, BootstrapTransactionId);
	}

	/* safety check, we should never get this far in a HS standby */
	if (RecoveryInProgress())
		elog(ERROR, "cannot assign TransactionIds during recovery");

	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);

	full_xid = ShmemVariableCache->nextXid;
#ifdef ADB
	for (i=xidnum-1;i>0;--i)
		FullTransactionIdAdvance(&full_xid);
#endif
	xid = XidFromFullTransactionId(full_xid);

	/*----------
	 * Check to see if it's safe to assign another XID.  This protects against
	 * catastrophic data loss due to XID wraparound.  The basic rules are:
	 *
	 * If we're past xidVacLimit, start trying to force autovacuum cycles.
	 * If we're past xidWarnLimit, start issuing warnings.
	 * If we're past xidStopLimit, refuse to execute transactions, unless
	 * we are running in single-user mode (which gives an escape hatch
	 * to the DBA who somehow got past the earlier defenses).
	 *
	 * Note that this coding also appears in GetNewMultiXactId.
	 *----------
	 */
	if (TransactionIdFollowsOrEquals(xid, ShmemVariableCache->xidVacLimit))
	{
		/*
		 * For safety's sake, we release XidGenLock while sending signals,
		 * warnings, etc.  This is not so much because we care about
		 * preserving concurrency in this situation, as to avoid any
		 * possibility of deadlock while doing get_database_name(). First,
		 * copy all the shared values we'll need in this path.
		 */
		TransactionId xidWarnLimit = ShmemVariableCache->xidWarnLimit;
		TransactionId xidStopLimit = ShmemVariableCache->xidStopLimit;
		TransactionId xidWrapLimit = ShmemVariableCache->xidWrapLimit;
		Oid			oldest_datoid = ShmemVariableCache->oldestXidDB;

		LWLockRelease(XidGenLock);

		/*
		 * To avoid swamping the postmaster with signals, we issue the autovac
		 * request only once per 64K transaction starts.  This still gives
		 * plenty of chances before we get into real trouble.
		 */
		if (IsUnderPostmaster && (xid % 65536) == 0)
			SendPostmasterSignal(PMSIGNAL_START_AUTOVAC_LAUNCHER);

#ifdef ADB
		if (isInsertXact == true)
		{
#endif
		if (IsUnderPostmaster &&
			TransactionIdFollowsOrEquals(xid, xidStopLimit))
		{
			char	   *oldest_datname = get_database_name(oldest_datoid);

			/* complain even if that DB has disappeared */
			if (oldest_datname)
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("database is not accepting commands to avoid wraparound data loss in database \"%s\"",
								oldest_datname),
						 errhint("Stop the postmaster and vacuum that database in single-user mode.\n"
								 "You might also need to commit or roll back old prepared transactions, or drop stale replication slots.")));
			else
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("database is not accepting commands to avoid wraparound data loss in database with OID %u",
								oldest_datoid),
						 errhint("Stop the postmaster and vacuum that database in single-user mode.\n"
								 "You might also need to commit or roll back old prepared transactions, or drop stale replication slots.")));
		}
		else if (TransactionIdFollowsOrEquals(xid, xidWarnLimit))
		{
			char	   *oldest_datname = get_database_name(oldest_datoid);

			/* complain even if that DB has disappeared */
			if (oldest_datname)
				ereport(WARNING,
						(errmsg("database \"%s\" must be vacuumed within %u transactions",
								oldest_datname,
								xidWrapLimit - xid),
						 errhint("To avoid a database shutdown, execute a database-wide VACUUM in that database.\n"
								 "You might also need to commit or roll back old prepared transactions, or drop stale replication slots.")));
			else
				ereport(WARNING,
						(errmsg("database with OID %u must be vacuumed within %u transactions",
								oldest_datoid,
								xidWrapLimit - xid),
						 errhint("To avoid a database shutdown, execute a database-wide VACUUM in that database.\n"
								 "You might also need to commit or roll back old prepared transactions, or drop stale replication slots.")));
		}
#ifdef ADB
		}
#endif
		/* Re-acquire lock and start over */
		LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
		full_xid = ShmemVariableCache->nextXid;
#ifdef ADB
		for (i=xidnum-1;i>0;--i)
			FullTransactionIdAdvance(&full_xid);
#endif
		xid = XidFromFullTransactionId(full_xid);
	}

	/*
	 * If we are allocating the first XID of a new page of the commit log,
	 * zero out that commit-log page before returning. We must do this while
	 * holding XidGenLock, else another xact could acquire and commit a later
	 * XID before we zero the page.  Fortunately, a page of the commit log
	 * holds 32K or more transactions, so we don't have to do this very often.
	 *
	 * Extend pg_subtrans and pg_commit_ts too.
	 */
	ExtendCLOG(xid);
	ExtendCommitTs(xid);
	ExtendSUBTRANS(xid);

	/*
	 * Now advance the nextXid counter.  This must not happen until after we
	 * have successfully completed ExtendCLOG() --- if that routine fails, we
	 * want the next incoming transaction to try it again.  We cannot assign
	 * more XIDs until there is CLOG space for them.
	 */
#ifdef ADB
	for (i = 0; i < xidnum; i++)
#endif
	FullTransactionIdAdvance(&ShmemVariableCache->nextXid);

	/*
	 * We must store the new XID into the shared ProcArray before releasing
	 * XidGenLock.  This ensures that every active XID older than
	 * latestCompletedXid is present in the ProcArray, which is essential for
	 * correct OldestXmin tracking; see src/backend/access/transam/README.
	 *
	 * Note that readers of ProcGlobal->xids/PGPROC->xid should be careful to
	 * fetch the value for each proc only once, rather than assume they can
	 * read a value multiple times and get the same answer each time.  Note we
	 * are assuming that TransactionId and int fetch/store are atomic.
	 *
	 * The same comments apply to the subxact xid count and overflow fields.
	 *
	 * Use of a write barrier prevents dangerous code rearrangement in this
	 * function; other backends could otherwise e.g. be examining my subxids
	 * info concurrently, and we don't want them to see an invalid
	 * intermediate state, such as an incremented nxids before the array entry
	 * is filled.
	 *
	 * Other processes that read nxids should do so before reading xids
	 * elements with a pg_read_barrier() in between, so that they can be sure
	 * not to read an uninitialized array element; see
	 * src/backend/storage/lmgr/README.barrier.
	 *
	 * If there's no room to fit a subtransaction XID into PGPROC, set the
	 * cache-overflowed flag instead.  This forces readers to look in
	 * pg_subtrans to map subtransaction XIDs up to top-level XIDs. There is a
	 * race-condition window, in that the new XID will not appear as running
	 * until its parent link has been placed into pg_subtrans. However, that
	 * will happen before anyone could possibly have a reason to inquire about
	 * the status of the XID, so it seems OK.  (Snapshots taken during this
	 * window *will* include the parent XID, so they will deliver the correct
	 * answer later on when someone does have a reason to inquire.)
	 */
	if (!isSubXact)
	{
		Assert(ProcGlobal->subxidStates[MyProc->pgxactoff].count == 0);
		Assert(!ProcGlobal->subxidStates[MyProc->pgxactoff].overflowed);
		Assert(MyProc->subxidStatus.count == 0);
		Assert(!MyProc->subxidStatus.overflowed);

		/* LWLockRelease acts as barrier */
#ifdef ADB
		if (isInsertXact)
		{
#endif
		MyProc->xid = xid;
		ProcGlobal->xids[MyProc->pgxactoff] = xid;
#ifdef ADB
		}
#endif /* ADB */
	}
	else
	{
		XidCacheStatus *substat = &ProcGlobal->subxidStates[MyProc->pgxactoff];
		int			nxids = MyProc->subxidStatus.count;

		Assert(substat->count == MyProc->subxidStatus.count);
		Assert(substat->overflowed == MyProc->subxidStatus.overflowed);

		if (nxids < PGPROC_MAX_CACHED_SUBXIDS)
		{
			MyProc->subxids.xids[nxids] = xid;
			pg_write_barrier();
			MyProc->subxidStatus.count = substat->count = nxids + 1;
		}
		else
			MyProc->subxidStatus.overflowed = substat->overflowed = true;
	}

#ifdef ADB
	if (IsGTMNode() && !isSubXact)
	{
		/* we must add xid to xip and add xid to xid_assign before release XidGenLock*/
		/* for snapsender assing xid to cn/dn, we should add xid to snapsender->xip */
		/* for gtm local process, no need add xid to snapsender->xip.*/
		/* As from local porc array, we can get the xid */
		/* for gtm local process, we should assing xid and add xid to SnapSender->xid_assign */
		/* for snapsender assign xid to dn/cn, snapsender can add xid to  SNAPSENDER_XID_ARRAY_ASSIGN itselt*/
		if (!isNeedAssign)
			SnapSendAddXip(xid, xidnum, InvalidTransactionId);
		else
			SnapSendTransactionAssign(xid, xidnum, InvalidTransactionId);
	}
#endif

	LWLockRelease(XidGenLock);

#ifdef DEBUG_ADB
	adb_ereport(LOG,
		(errmsg("Return new local full xid: " UINT64_FORMAT, full_xid.value)));
#endif

	return full_xid;
}

/*
 * Read nextXid but don't allocate it.
 */
FullTransactionId
ReadNextFullTransactionId(void)
{
	FullTransactionId fullXid;

	LWLockAcquire(XidGenLock, LW_SHARED);
	fullXid = ShmemVariableCache->nextXid;
	LWLockRelease(XidGenLock);

	return fullXid;
}

/*
 * Advance nextXid to the value after a given xid.  The epoch is inferred.
 * This must only be called during recovery or from two-phase start-up code.
 */
void
AdvanceNextFullTransactionIdPastXid(TransactionId xid)
{
	FullTransactionId newNextFullXid;
	TransactionId next_xid;
	uint32		epoch;

	/*
	 * It is safe to read nextXid without a lock, because this is only called
	 * from the startup process or single-process mode, meaning that no other
	 * process can modify it.
	 */
	Assert(AmStartupProcess() || !IsUnderPostmaster);

	/* Fast return if this isn't an xid high enough to move the needle. */
	next_xid = XidFromFullTransactionId(ShmemVariableCache->nextXid);
	if (!TransactionIdFollowsOrEquals(xid, next_xid))
		return;

	/*
	 * Compute the FullTransactionId that comes after the given xid.  To do
	 * this, we preserve the existing epoch, but detect when we've wrapped
	 * into a new epoch.  This is necessary because WAL records and 2PC state
	 * currently contain 32 bit xids.  The wrap logic is safe in those cases
	 * because the span of active xids cannot exceed one epoch at any given
	 * point in the WAL stream.
	 */
	TransactionIdAdvance(xid);
	epoch = EpochFromFullTransactionId(ShmemVariableCache->nextXid);
	if (unlikely(xid < next_xid))
		++epoch;
	newNextFullXid = FullTransactionIdFromEpochAndXid(epoch, xid);

	/*
	 * We still need to take a lock to modify the value when there are
	 * concurrent readers.
	 */
	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	ShmemVariableCache->nextXid = newNextFullXid;
	LWLockRelease(XidGenLock);
}

/*
 * Advance the cluster-wide value for the oldest valid clog entry.
 *
 * We must acquire XactTruncationLock to advance the oldestClogXid. It's not
 * necessary to hold the lock during the actual clog truncation, only when we
 * advance the limit, as code looking up arbitrary xids is required to hold
 * XactTruncationLock from when it tests oldestClogXid through to when it
 * completes the clog lookup.
 */
void
AdvanceOldestClogXid(TransactionId oldest_datfrozenxid)
{
	LWLockAcquire(XactTruncationLock, LW_EXCLUSIVE);
	if (TransactionIdPrecedes(ShmemVariableCache->oldestClogXid,
							  oldest_datfrozenxid))
	{
		ShmemVariableCache->oldestClogXid = oldest_datfrozenxid;
	}
	LWLockRelease(XactTruncationLock);
}

/*
 * Determine the last safe XID to allocate using the currently oldest
 * datfrozenxid (ie, the oldest XID that might exist in any database
 * of our cluster), and the OID of the (or a) database with that value.
 */
void
SetTransactionIdLimit(TransactionId oldest_datfrozenxid, Oid oldest_datoid)
{
	TransactionId xidVacLimit;
	TransactionId xidWarnLimit;
	TransactionId xidStopLimit;
	TransactionId xidWrapLimit;
	TransactionId curXid;

	Assert(TransactionIdIsNormal(oldest_datfrozenxid));

	/*
	 * The place where we actually get into deep trouble is halfway around
	 * from the oldest potentially-existing XID.  (This calculation is
	 * probably off by one or two counts, because the special XIDs reduce the
	 * size of the loop a little bit.  But we throw in plenty of slop below,
	 * so it doesn't matter.)
	 */
	xidWrapLimit = oldest_datfrozenxid + (MaxTransactionId >> 1);
	if (xidWrapLimit < FirstNormalTransactionId)
		xidWrapLimit += FirstNormalTransactionId;

	/*
	 * We'll refuse to continue assigning XIDs in interactive mode once we get
	 * within 3M transactions of data loss.  This leaves lots of room for the
	 * DBA to fool around fixing things in a standalone backend, while not
	 * being significant compared to total XID space. (VACUUM requires an XID
	 * if it truncates at wal_level!=minimal.  "VACUUM (ANALYZE)", which a DBA
	 * might do by reflex, assigns an XID.  Hence, we had better be sure
	 * there's lots of XIDs left...)  Also, at default BLCKSZ, this leaves two
	 * completely-idle segments.  In the event of edge-case bugs involving
	 * page or segment arithmetic, idle segments render the bugs unreachable
	 * outside of single-user mode.
	 */
	xidStopLimit = xidWrapLimit - 3000000;
	if (xidStopLimit < FirstNormalTransactionId)
		xidStopLimit -= FirstNormalTransactionId;

	/*
	 * We'll start complaining loudly when we get within 40M transactions of
	 * data loss.  This is kind of arbitrary, but if you let your gas gauge
	 * get down to 2% of full, would you be looking for the next gas station?
	 * We need to be fairly liberal about this number because there are lots
	 * of scenarios where most transactions are done by automatic clients that
	 * won't pay attention to warnings.  (No, we're not gonna make this
	 * configurable.  If you know enough to configure it, you know enough to
	 * not get in this kind of trouble in the first place.)
	 */
	xidWarnLimit = xidWrapLimit - 40000000;
	if (xidWarnLimit < FirstNormalTransactionId)
		xidWarnLimit -= FirstNormalTransactionId;

	/*
	 * We'll start trying to force autovacuums when oldest_datfrozenxid gets
	 * to be more than autovacuum_freeze_max_age transactions old.
	 *
	 * Note: guc.c ensures that autovacuum_freeze_max_age is in a sane range,
	 * so that xidVacLimit will be well before xidWarnLimit.
	 *
	 * Note: autovacuum_freeze_max_age is a PGC_POSTMASTER parameter so that
	 * we don't have to worry about dealing with on-the-fly changes in its
	 * value.  It doesn't look practical to update shared state from a GUC
	 * assign hook (too many processes would try to execute the hook,
	 * resulting in race conditions as well as crashes of those not connected
	 * to shared memory).  Perhaps this can be improved someday.  See also
	 * SetMultiXactIdLimit.
	 */
	xidVacLimit = oldest_datfrozenxid + autovacuum_freeze_max_age;
	if (xidVacLimit < FirstNormalTransactionId)
		xidVacLimit += FirstNormalTransactionId;

	/* Grab lock for just long enough to set the new limit values */
	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	ShmemVariableCache->oldestXid = oldest_datfrozenxid;
	ShmemVariableCache->xidVacLimit = xidVacLimit;
	ShmemVariableCache->xidWarnLimit = xidWarnLimit;
	ShmemVariableCache->xidStopLimit = xidStopLimit;
	ShmemVariableCache->xidWrapLimit = xidWrapLimit;
	ShmemVariableCache->oldestXidDB = oldest_datoid;
	curXid = XidFromFullTransactionId(ShmemVariableCache->nextXid);
	LWLockRelease(XidGenLock);

	/* Log the info */
	ereport(DEBUG1,
			(errmsg_internal("transaction ID wrap limit is %u, limited by database with OID %u",
							 xidWrapLimit, oldest_datoid)));

	/*
	 * If past the autovacuum force point, immediately signal an autovac
	 * request.  The reason for this is that autovac only processes one
	 * database per invocation.  Once it's finished cleaning up the oldest
	 * database, it'll call here, and we'll signal the postmaster to start
	 * another iteration immediately if there are still any old databases.
	 */
	if (TransactionIdFollowsOrEquals(curXid, xidVacLimit) &&
		IsUnderPostmaster && !InRecovery)
		SendPostmasterSignal(PMSIGNAL_START_AUTOVAC_LAUNCHER);

	/* Give an immediate warning if past the wrap warn point */
	if (TransactionIdFollowsOrEquals(curXid, xidWarnLimit) && !InRecovery)
	{
		char	   *oldest_datname;

		/*
		 * We can be called when not inside a transaction, for example during
		 * StartupXLOG().  In such a case we cannot do database access, so we
		 * must just report the oldest DB's OID.
		 *
		 * Note: it's also possible that get_database_name fails and returns
		 * NULL, for example because the database just got dropped.  We'll
		 * still warn, even though the warning might now be unnecessary.
		 */
		if (IsTransactionState())
			oldest_datname = get_database_name(oldest_datoid);
		else
			oldest_datname = NULL;

		if (oldest_datname)
			ereport(WARNING,
					(errmsg("database \"%s\" must be vacuumed within %u transactions",
							oldest_datname,
							xidWrapLimit - curXid),
					 errhint("To avoid a database shutdown, execute a database-wide VACUUM in that database.\n"
							 "You might also need to commit or roll back old prepared transactions, or drop stale replication slots.")));
		else
			ereport(WARNING,
					(errmsg("database with OID %u must be vacuumed within %u transactions",
							oldest_datoid,
							xidWrapLimit - curXid),
					 errhint("To avoid a database shutdown, execute a database-wide VACUUM in that database.\n"
							 "You might also need to commit or roll back old prepared transactions, or drop stale replication slots.")));
	}
}


/*
 * ForceTransactionIdLimitUpdate -- does the XID wrap-limit data need updating?
 *
 * We primarily check whether oldestXidDB is valid.  The cases we have in
 * mind are that that database was dropped, or the field was reset to zero
 * by pg_resetwal.  In either case we should force recalculation of the
 * wrap limit.  Also do it if oldestXid is old enough to be forcing
 * autovacuums or other actions; this ensures we update our state as soon
 * as possible once extra overhead is being incurred.
 */
bool
ForceTransactionIdLimitUpdate(void)
{
	TransactionId nextXid;
	TransactionId xidVacLimit;
	TransactionId oldestXid;
	Oid			oldestXidDB;

	/* Locking is probably not really necessary, but let's be careful */
	LWLockAcquire(XidGenLock, LW_SHARED);
	nextXid = XidFromFullTransactionId(ShmemVariableCache->nextXid);
	xidVacLimit = ShmemVariableCache->xidVacLimit;
	oldestXid = ShmemVariableCache->oldestXid;
	oldestXidDB = ShmemVariableCache->oldestXidDB;
	LWLockRelease(XidGenLock);

	if (!TransactionIdIsNormal(oldestXid))
		return true;			/* shouldn't happen, but just in case */
	if (!TransactionIdIsValid(xidVacLimit))
		return true;			/* this shouldn't happen anymore either */
	if (TransactionIdFollowsOrEquals(nextXid, xidVacLimit))
		return true;			/* past xidVacLimit, don't delay updating */
	if (!SearchSysCacheExists1(DATABASEOID, ObjectIdGetDatum(oldestXidDB)))
		return true;			/* could happen, per comments above */
	return false;
}


/*
 * GetNewObjectId -- allocate a new OID
 *
 * OIDs are generated by a cluster-wide counter.  Since they are only 32 bits
 * wide, counter wraparound will occur eventually, and therefore it is unwise
 * to assume they are unique unless precautions are taken to make them so.
 * Hence, this routine should generally not be used directly.  The only direct
 * callers should be GetNewOidWithIndex() and GetNewRelFileNode() in
 * catalog/catalog.c.
 */
Oid
GetNewObjectId(void)
{
	Oid			result;

	/* safety check, we should never get this far in a HS standby */
	if (RecoveryInProgress())
		elog(ERROR, "cannot assign OIDs during recovery");

	LWLockAcquire(OidGenLock, LW_EXCLUSIVE);

	/*
	 * Check for wraparound of the OID counter.  We *must* not return 0
	 * (InvalidOid), and in normal operation we mustn't return anything below
	 * FirstNormalObjectId since that range is reserved for initdb (see
	 * IsCatalogRelationOid()).  Note we are relying on unsigned comparison.
	 *
	 * During initdb, we start the OID generator at FirstBootstrapObjectId, so
	 * we only wrap if before that point when in bootstrap or standalone mode.
	 * The first time through this routine after normal postmaster start, the
	 * counter will be forced up to FirstNormalObjectId.  This mechanism
	 * leaves the OIDs between FirstBootstrapObjectId and FirstNormalObjectId
	 * available for automatic assignment during initdb, while ensuring they
	 * will never conflict with user-assigned OIDs.
	 */
	if (ShmemVariableCache->nextOid < ((Oid) FirstNormalObjectId))
	{
		if (IsPostmasterEnvironment)
		{
			/* wraparound, or first post-initdb assignment, in normal mode */
			ShmemVariableCache->nextOid = FirstNormalObjectId;
			ShmemVariableCache->oidCount = 0;
		}
		else
		{
			/* we may be bootstrapping, so don't enforce the full range */
			if (ShmemVariableCache->nextOid < ((Oid) FirstBootstrapObjectId))
			{
				/* wraparound in standalone mode (unlikely but possible) */
				ShmemVariableCache->nextOid = FirstNormalObjectId;
				ShmemVariableCache->oidCount = 0;
			}
		}
	}

	/* If we run out of logged for use oids then we must log more */
	if (ShmemVariableCache->oidCount == 0)
	{
		XLogPutNextOid(ShmemVariableCache->nextOid + VAR_OID_PREFETCH);
		ShmemVariableCache->oidCount = VAR_OID_PREFETCH;
	}

	result = ShmemVariableCache->nextOid;

	(ShmemVariableCache->nextOid)++;
	(ShmemVariableCache->oidCount)--;

	LWLockRelease(OidGenLock);

	return result;
}


#ifdef USE_ASSERT_CHECKING

/*
 * Assert that xid is between [oldestXid, nextXid], which is the range we
 * expect XIDs coming from tables etc to be in.
 *
 * As ShmemVariableCache->oldestXid could change just after this call without
 * further precautions, and as a wrapped-around xid could again fall within
 * the valid range, this assertion can only detect if something is definitely
 * wrong, but not establish correctness.
 *
 * This intentionally does not expose a return value, to avoid code being
 * introduced that depends on the return value.
 */
void
AssertTransactionIdInAllowableRange(TransactionId xid)
{
	TransactionId oldest_xid;
	TransactionId next_xid;

	Assert(TransactionIdIsValid(xid));

	/* we may see bootstrap / frozen */
	if (!TransactionIdIsNormal(xid))
		return;

	/*
	 * We can't acquire XidGenLock, as this may be called with XidGenLock
	 * already held (or with other locks that don't allow XidGenLock to be
	 * nested). That's ok for our purposes though, since we already rely on
	 * 32bit reads to be atomic. While nextXid is 64 bit, we only look at the
	 * lower 32bit, so a skewed read doesn't hurt.
	 *
	 * There's no increased danger of falling outside [oldest, next] by
	 * accessing them without a lock. xid needs to have been created with
	 * GetNewTransactionId() in the originating session, and the locks there
	 * pair with the memory barrier below.  We do however accept xid to be <=
	 * to next_xid, instead of just <, as xid could be from the procarray,
	 * before we see the updated nextXid value.
	 */
	pg_memory_barrier();
	oldest_xid = ShmemVariableCache->oldestXid;
	next_xid = XidFromFullTransactionId(ShmemVariableCache->nextXid);

	Assert(TransactionIdFollowsOrEquals(xid, oldest_xid) ||
		   TransactionIdPrecedesOrEquals(xid, next_xid));
}
#endif
