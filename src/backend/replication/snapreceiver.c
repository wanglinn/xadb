#include "postgres.h"

#include "access/rmgr.h"
#include "access/rxact_mgr.h"
#include "access/xact.h"
#include "access/twophase.h"
#include "access/xlogrecord.h"
#include "access/commit_ts.h"
#include "access/subtrans.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "pgxc/pgxc.h"
#include "postmaster/postmaster.h"
#include "replication/walreceiver.h"
#include "replication/snapreceiver.h"
#include "replication/snapcommon.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/proclist.h"
#include "storage/shmem.h"
#include "storage/condition_variable.h"
#include "utils/dsa.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"

#define RESTART_STEP_MS		3000	/* 2 second */

int snap_receiver_timeout = 60 * 1000L;
int snap_sender_connect_timeout = 5000L;
int snap_force_globalxmin_sync_time = 30000L;
int max_cn_prealloc_xid_size = 0;

#define ReadConfigFile()			\
if (got_SIGHUP)						\
{									\
	got_SIGHUP = false;				\
	ProcessConfigFile(PGC_SIGHUP);	\
}


typedef struct SnapRcvData
{
	SnapCommonLock	comm_lock;
	pg_atomic_uint32	state;
	pg_atomic_uint32	rxact_state;
	pid_t			pid;
	int				procno;

	pg_time_t		startTime;

	char			sender_host[NI_MAXHOST];
	int				sender_port;

	ConditionVariable 		cv;
	proclist_head	waiters;	/* list of waiting event */
	proclist_head	ss_waiters;	/* snap sync waiters */

	proclist_head	geters;				/* list of getting gxid event */
	proclist_head	reters;				/* list of return gxid event */

	proclist_head	send_commiters;		/* list of commit gxid */
	proclist_head	wait_commiters;		/* list of commit gxid */
	proclist_head 	waiters_assign;		/* list of waiting assigne xid */
	proclist_head 	waiters_finish;		/* list of waiting assigne xid */

	slock_t			mutex;
	slock_t			gxid_mutex;

	TimestampTz		next_try_time;	/* next connection GTM time */
	TimestampTz		gtm_delta_time;

	uint32			xcnt;
	TransactionId	latestCompletedXid;
	pg_atomic_uint32	global_xmin;
	pg_atomic_uint32	local_global_xmin;
	TransactionId	xip[MAX_BACKENDS];
	pg_atomic_uint32	last_client_req_key; /* last client rquest snap sync key num*/
	pg_atomic_uint32	last_ss_req_key; 	/* last snaprcv rquest snap sync key num*/
	pg_atomic_uint32	last_ss_resp_key; /* last snaprcv reponse snap sync ken num*/
	pg_atomic_uint64	last_heartbeat_sync_time;

	uint32			cur_pre_alloc;
	TransactionId	xid_alloc[2*MAX_XID_PRE_ALLOC_NUM];

	uint32			wait_finish_cnt;
	TransactionId	wait_xid_finish[MAX_BACKENDS];

	uint32			is_send_realloc_num;  /* is need realloc from gc*/ 
	pg_atomic_uint32	global_finish_id;
}SnapRcvData;

/* GUC variables */
extern char *AGtmHost;
extern int	AGtmPort;
extern char *PGXCNodeName;
extern bool adb_check_sync_nextid;	/* in snapsender.c */

/* item in  slist_client */
typedef struct SnapRcvAssginXidClientInfo
{
	slist_node		snode;
	int				procno;
	slist_head		slist_xid; 		/* xiditem list */
}SnapRcvAssginXidClientInfo;

/* item in  slist_client */
typedef struct SnapRcvAssginXidItemInfo
{
	slist_node		snode;
	TransactionId	xid;
}SnapRcvAssginXidItemInfo;

/* libpqwalreceiver connection */
static WalReceiverConn *wrconn;
/* in walreceiver.c */
static StringInfoData reply_message;
static StringInfoData incoming_message;

static TimestampTz last_heat_beat_sendtime;

static bool finish_xid_ack_send = false;
static bool	heartbeat_sent = false;

/*
 * Flags set by interrupt handlers of walreceiver for later service in the
 * main loop.
 */
static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t got_SIGTERM = false;

static SnapRcvData *SnapRcv = NULL;
#define LOCK_SNAP_RCV()			SpinLockAcquire(&SnapRcv->mutex)
#define UNLOCK_SNAP_RCV()		SpinLockRelease(&SnapRcv->mutex)
#define LOCK_SNAP_GXID_RCV()	SpinLockAcquire(&SnapRcv->gxid_mutex)
#define UNLOCK_SNAP_GXID_RCV()	SpinLockRelease(&SnapRcv->gxid_mutex)
#define SNAP_RCV_SET_LATCH()	SetLatch(&(GetPGProcByNumber(SnapRcv->procno)->procLatch))
#define SNAP_RCV_RESET_LATCH()	ResetLatch(&(GetPGProcByNumber(SnapRcv->procno)->procLatch))
#define SNAP_RCV_LATCH_VALID()	(SnapRcv->procno != INVALID_PGPROCNO)

/* like WalRcvImmediateInterruptOK */
static volatile bool SnapRcvImmediateInterruptOK = false;

/* Prototypes for private functions */
static TimestampTz WaitUntilStartTime(void);
static void ProcessSnapRcvInterrupts(void);
static void EnableSnapRcvImmediateExit(void);
static void DisableSnapRcvImmediateExit(void);
static void SnapRcvDie(int code, Datum arg);
static void SnapRcvConnectGTM(void);
static void SnapRcvUpdateShmemConnInfo(void);
static void SnapRcvProcessMessage(unsigned char type, char *buf, Size len);
static void SnapRcvProcessSnapshot(char *buf, Size len);
static void SnapRcvProcessAssign(char *buf, Size len);
static void SnapRcvProcessComplete(char *buf, Size len);
static void SnapRcvProcessHeartBeat(char *buf, Size len);
static void SnapRcvProcessUpdateXid(char *buf, Size len);
static void WakeupTransaction(TransactionId xid);
static void WakeupTransactionInit(TransactionId *xid, int count, proclist_head *waiters);
static TransactionId SnapRcvGetLocalXmin(void);

/* Signal handlers */
static void SnapRcvSigHupHandler(SIGNAL_ARGS);
static void SnapRcvSigUsr1Handler(SIGNAL_ARGS);
static void SnapRcvShutdownHandler(SIGNAL_ARGS);
static void SnapRcvQuickDieHandler(SIGNAL_ARGS);

typedef bool (*WaitSnapRcvCond)(void *context);
typedef bool (*WaitGxidRcvCond)(void *context, proclist_head *reters);

static bool WaitSnapRcvCondTransactionComplate(void *context);
static bool WaitSnapRcvEvent(TimestampTz end, proclist_head *waiters, bool is_ss,
				WaitSnapRcvCond test, void *context);
static bool WaitSnapRcvSyncSnap(void *context);
static void WakeupSnapSync(uint32_t req_key);
static void SnapRcvDeleteProcList(proclist_head *reters, int procno);
static bool SnapRcvWaitGxidEvent(TimestampTz end, WaitGxidRcvCond test,
			proclist_head *reters, proclist_head *geters, void *context, bool is_need_setlatch);
static void SnapRcvStopAllWaitFinishBackend(void);

static void
ProcessSnapRcvInterrupts(void)
{
	/* like ProcessWalRcvInterrupts */
	CHECK_FOR_INTERRUPTS();

	if (got_SIGTERM)
	{
		SnapRcvStopAllWaitFinishBackend();
		SnapRcvImmediateInterruptOK = false;
		ereport(FATAL,
				(errcode(ERRCODE_ADMIN_SHUTDOWN),
				 errmsg("terminating snapreceiver process due to administrator command")));
	}
}

static void
EnableSnapRcvImmediateExit(void)
{
	SnapRcvImmediateInterruptOK = true;
	ProcessSnapRcvInterrupts();
}

static void
DisableSnapRcvImmediateExit(void)
{
	SnapRcvImmediateInterruptOK = false;
	ProcessSnapRcvInterrupts();
}

static void
SnapRcvSendHeartbeat(void)
{
	TransactionId 	xmin;

	xmin = SnapRcvGetLocalXmin();
	if (!TransactionIdIsNormal(xmin))
		return;
	/* Construct a new message */
	last_heat_beat_sendtime = GetCurrentTimestamp();
	resetStringInfo(&reply_message);
	pq_sendbyte(&reply_message, 'h');
	pq_sendint64(&reply_message, last_heat_beat_sendtime);
	pq_sendint64(&reply_message, xmin);

	/* Send it */
	walrcv_send(wrconn, reply_message.data, reply_message.len);
	pg_atomic_write_u64(&SnapRcv->last_heartbeat_sync_time, last_heat_beat_sendtime);
	pg_atomic_write_u32(&SnapRcv->local_global_xmin, xmin);
	heartbeat_sent = true;

	return;
}

static void SnapRcvProcessSnapSync(void)
{
	/* Construct a new message */
	uint32_t last_client_req_key,last_ss_req_key;

	last_client_req_key = pg_atomic_read_u32(&SnapRcv->last_client_req_key);
	last_ss_req_key = pg_atomic_read_u32(&SnapRcv->last_ss_req_key);
	//SNAP_SYNC_DEBUG_LOG(,(errmsg("last_client_req_key %lld, last_ss_req_key %lld\n", last_client_req_key, last_ss_req_key)));
	if (last_client_req_key != last_ss_req_key)
	{
		//SNAP_SYNC_DEBUG_LOG((errmsg("SnapRcvProcessSnapSync send SnapSync request key %lld\n", last_client_req_key)));
		/* Construct a new message */
		resetStringInfo(&reply_message);
		pq_sendbyte(&reply_message, 'p');
		pq_sendint64(&reply_message, last_client_req_key);
		walrcv_send(wrconn, reply_message.data, reply_message.len);
		pg_atomic_write_u32(&SnapRcv->last_ss_req_key, last_client_req_key);
	}
}

static TransactionId
SnapRcvGetLocalNextXid(void)
{
	FullTransactionId full_xid; 
	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	full_xid = ShmemVariableCache->nextFullXid;
	LWLockRelease(XidGenLock);

	return XidFromFullTransactionId(full_xid);
}

static void
SnapRcvSendPreAssginXid(int xid_num)
{
	if (!IS_PGXC_COORDINATOR)
		return;

	/* Construct a new message */
	resetStringInfo(&reply_message);
	pq_sendbyte(&reply_message, 'q');
	pq_sendstring(&reply_message, PGXCNodeName);
	pq_sendint32(&reply_message, xid_num);

	/* Send it */
	walrcv_send(wrconn, reply_message.data, reply_message.len);
}

static void SnapRcvDeleteProcList(proclist_head *reters, int procno)
{
	proclist_mutable_iter	iter;
	PGPROC					*proc;

	proclist_foreach_modify(iter, reters, GxidWaitLink)
	{
		proc = GetPGProcByNumber(iter.cur);
		if (proc->pgprocno == procno)
			proclist_delete(reters, procno, GxidWaitLink);
	}
}

static void SnapRcvCheckPreAssignArray(void)
{
	int req_num = 0;

	if (!IS_PGXC_COORDINATOR || max_cn_prealloc_xid_size == 0)
		return;

	ProcessSnapRcvInterrupts();
	LOCK_SNAP_GXID_RCV();
	if (SnapRcv->is_send_realloc_num == 0)
	{
		if (max_cn_prealloc_xid_size == 1 && SnapRcv->cur_pre_alloc == 0)
		{
			req_num = 1;
		}
		else if (SnapRcv->cur_pre_alloc <= (max_cn_prealloc_xid_size/2))
		{
			if (SnapRcv->cur_pre_alloc == 0)
				req_num = max_cn_prealloc_xid_size;
			else
				req_num = max_cn_prealloc_xid_size/2;
		}

		if (req_num > 0)
		{
			SnapRcvSendPreAssginXid(req_num);
			SnapRcv->is_send_realloc_num = 1;

			SNAP_SYNC_DEBUG_LOG((errmsg("max_cn_prealloc_xid_size is %d, send req_num is %d\n",
				max_cn_prealloc_xid_size, req_num)));
		}
		
	}
	UNLOCK_SNAP_GXID_RCV();
}

/*
 * when end < 0 wait until streaming or error
 *   when end == 0 not block
 * mutex must be locked
 */
static bool SnapRcvWaitGxidEvent(TimestampTz end, WaitGxidRcvCond test,
			proclist_head *reters, proclist_head *geters, void *context, bool is_need_setlatch)
{
	Latch				   *latch = &MyProc->procLatch;
	long					timeout;
	proclist_mutable_iter	iter;
	int						procno = MyProc->pgprocno;
	int						rc;
	int						waitEvent;
	bool					ret;

	ret = true;
	while ((*test)(context, reters))
	{
		bool in_ret_list = false;
		bool in_get_list = false;
		proclist_foreach_modify(iter, reters, GxidWaitLink)
		{
			if (iter.cur == procno)
			{
				in_ret_list = true;
				break;
			}
		}

		if (geters && !in_ret_list)
		{
			proclist_foreach_modify(iter, geters, GxidWaitLink)
			{
				if (iter.cur == procno)
				{
					in_get_list = true;
					break;
				}
			}
			if (!in_get_list)
			{
				pg_write_barrier();
				proclist_push_tail(geters, procno, GxidWaitLink);
			}
		}

		if (is_need_setlatch && SNAP_RCV_LATCH_VALID())
			SNAP_RCV_SET_LATCH();

		UNLOCK_SNAP_GXID_RCV();

		waitEvent = WL_POSTMASTER_DEATH | WL_LATCH_SET;
		if (end > 0)
		{
			long secs;
			int microsecs;
			TimestampDifference(GetCurrentTimestamp(), end, &secs, &microsecs);
			timeout = secs*1000 + microsecs/1000;
			waitEvent |= WL_TIMEOUT;
		}else if (end == 0)
		{
			timeout = 0;
			waitEvent |= WL_TIMEOUT;
		}else
		{
			timeout = -1;
		}

		rc = WaitLatch(latch, waitEvent, timeout, PG_WAIT_EXTENSION);
		ResetLatch(latch);
		if (rc & WL_POSTMASTER_DEATH)
		{
			ret = false;
		}else if(rc & WL_TIMEOUT)
		{
			ret = false;
		}

		ProcessSnapRcvInterrupts();
		LOCK_SNAP_GXID_RCV();
		if (ret == false)
			break;
	}


	/* check if we still in waiting list, remove */
	proclist_foreach_modify(iter, reters, GxidWaitLink)
	{
		if (iter.cur == procno)
		{
			proclist_delete(reters, procno, GxidWaitLink);
			break;
		}
	}

	return ret;
}

static void
SnapRcvProcessFinishList(void)
{
	proclist_mutable_iter	iter_gets;
	proclist_mutable_iter	iter_rets;
	PGPROC					*proc;

	ProcessSnapRcvInterrupts();
	LOCK_SNAP_GXID_RCV();
	if (proclist_is_empty(&SnapRcv->send_commiters))
	{
		UNLOCK_SNAP_GXID_RCV();
		return;
	}
	resetStringInfo(&reply_message);
	pq_sendbyte(&reply_message, 'c');
	pq_sendstring(&reply_message, PGXCNodeName);

	proclist_foreach_modify(iter_gets, &SnapRcv->send_commiters, GxidWaitLink)
	{
		proc = GetPGProcByNumber(iter_gets.cur);
		pq_sendint32(&reply_message, proc->pgprocno);
		pq_sendint32(&reply_message, proc->getGlobalTransaction);
		proclist_delete(&SnapRcv->send_commiters, iter_gets.cur, GxidWaitLink);

		SNAP_SYNC_DEBUG_LOG((errmsg("SnapRcv send finish xid %d for %d\n",
			 proc->getGlobalTransaction,
			 proc->pgprocno)));

		bool in_list = false;
		proclist_foreach_modify(iter_rets, &SnapRcv->wait_commiters, GxidWaitLink)
		{
			if (iter_rets.cur == iter_gets.cur)
			{
				in_list = true;
				break;
			}
		}

		Assert(!in_list);
		if (!in_list)
		{
			pg_write_barrier();
			proclist_push_tail(&SnapRcv->wait_commiters, iter_gets.cur, GxidWaitLink);
		}
	}
	UNLOCK_SNAP_GXID_RCV();

	/* Send it */
	walrcv_send(wrconn, reply_message.data, reply_message.len);
}

static void
SnapRcvProcessAssignList(void)
{
	proclist_mutable_iter	iter_gets;
	proclist_mutable_iter	iter_rets;

	ProcessSnapRcvInterrupts();
	LOCK_SNAP_GXID_RCV();
	if (proclist_is_empty(&SnapRcv->geters))
	{
		UNLOCK_SNAP_GXID_RCV();
		return;
	}

	resetStringInfo(&reply_message);
	pq_sendbyte(&reply_message, 'g');
	pq_sendstring(&reply_message, PGXCNodeName);

	proclist_foreach_modify(iter_gets, &SnapRcv->geters, GxidWaitLink)
	{
		pq_sendint32(&reply_message, iter_gets.cur);
		SNAP_SYNC_DEBUG_LOG((errmsg("SnapRcv assing xid for %u\n",
			 iter_gets.cur)));

		proclist_delete(&SnapRcv->geters, iter_gets.cur, GxidWaitLink);

		bool in_list = false;
		proclist_foreach_modify(iter_rets, &SnapRcv->reters, GxidWaitLink)
		{
			if (iter_rets.cur == iter_gets.cur)
			{
				in_list = true;
				break;
			}
		}

		Assert(!in_list);
		if (!in_list)
		{
			pg_write_barrier();
			proclist_push_tail(&SnapRcv->reters, iter_gets.cur, GxidWaitLink);
		}
	}
	UNLOCK_SNAP_GXID_RCV();

	/* Send it */
	walrcv_send(wrconn, reply_message.data, reply_message.len);
}

static void SnapRcvMainProcess(void)
{
	SnapRcvCheckPreAssignArray();
	SnapRcvProcessFinishList();
	SnapRcvProcessAssignList();
}

/* return list string for xid*/
static List*
SnapRcvSendInitSyncXid(void)
{
	List			*xid_prepared_list = NIL;
	List			*rxact_list = NIL;
	List			*local_assign_list = NIL;
	ListCell		*lc;
	TransactionId	xid;
	RxactTransactionInfo *info;
	List			*left_xid_str = NIL;
	char			buf[128];
	char			*str;
	TransactionId	*xids;
	int				xids_num, i;

	/* for slave node, there is no need sync left xid */
	if (!RecoveryInProgress() && adb_check_sync_nextid)
	{
		xid_prepared_list = GetPreparedXidList();

		/* for coordinator get all left two-phase xid*/
		if (IsCnNode())
		{
			local_assign_list = GetAllSnapRcvAssginXids();
			SNAP_SYNC_DEBUG_LOG((errmsg("SnapRcvSendInitSyncXid GetAllSnapRcvAssginXids list len %d\n",
										list_length(local_assign_list))));

			rxact_list = RxactGetRunningList();
			SNAP_SYNC_DEBUG_LOG((errmsg("SnapRcvSendInitSyncXid RxactGetRunningList list len %d\n",
										list_length(rxact_list))));
		}
	}
	else
		return left_xid_str;
	

	SNAP_SYNC_DEBUG_LOG((errmsg("SnapRcvSendInitSyncXid GetPreparedXidList xid_prepared_list len %d\n",
			 			list_length(xid_prepared_list))));

	foreach (lc, xid_prepared_list)
	{
		xid = lfirst_int(lc);

		pg_lltoa(xid, buf);
		str = pstrdup(buf);
		left_xid_str = lappend(left_xid_str, makeString(str));
		SNAP_SYNC_DEBUG_LOG((errmsg("SnapRcvSendInitSyncXid Add prepared xid %u, str %s\n",
			 			xid, str)));
	}
	list_free(xid_prepared_list);

	xids_num = list_length(rxact_list);
	if (xids_num > 0)
		xids = palloc0(sizeof(TransactionId) * xids_num);

	i = 0;
	foreach (lc, rxact_list)
	{
		info =  (RxactTransactionInfo*)lfirst(lc);
		xid = pg_strtouint64(&info->gid[1], NULL, 10);

		xids[i++] = xid;
		snprintf(buf, sizeof(buf), "+%u", xid);
		str = pstrdup(buf);
		left_xid_str = lappend(left_xid_str, makeString(str));
		SNAP_SYNC_DEBUG_LOG((errmsg("SnapRcvSendInitSyncXid Add rxact xid %u, str %s\n", xid, str)));
	}
	list_free(rxact_list);

	if (list_length(local_assign_list) > 0)
	{
		bool found = false;
		foreach (lc, local_assign_list)
		{
			xid = lfirst_int(lc);
			found = false;
			for (i = 0 ;i < xids_num; i++)
			{
				if (xids[i] == xid)
				{
					found = true;
					break;
				}
			}

			if (!found)
			{
				snprintf(buf, sizeof(buf), "+%u", xid);
				str = pstrdup(buf);
				left_xid_str = lappend(left_xid_str, makeString(str));
				SNAP_SYNC_DEBUG_LOG((errmsg("SnapRcvSendInitSyncXid Add local assign xid %u, str %s\n",
								xid, str)));
			}
		}

		LOCK_SNAP_GXID_RCV();
		SnapRcv->wait_finish_cnt = 0;
		foreach (lc, local_assign_list)
		{
			xid = lfirst_int(lc);
			SnapRcv->wait_xid_finish[SnapRcv->wait_finish_cnt++] = xid;
		}
		UNLOCK_SNAP_GXID_RCV();
		list_free(local_assign_list);
	}
	if (xids_num > 0)
		pfree(xids);
	return left_xid_str;
}

static void SnapRcvStopAllWaitFinishBackend(void)
{
	proclist_mutable_iter	iter;
	PGPROC					*proc;

	LOCK_SNAP_GXID_RCV();
	if (proclist_is_empty(&SnapRcv->send_commiters) && proclist_is_empty(&SnapRcv->wait_commiters))
	{
		UNLOCK_SNAP_GXID_RCV();
		return;
	}

	proclist_foreach_modify(iter, &SnapRcv->send_commiters, GxidWaitLink)
	{
		proc = GetPGProcByNumber(iter.cur);
		proc->getGlobalTransaction = InvalidTransactionId;
		SetLatch(&proc->procLatch);
		proclist_delete( &SnapRcv->send_commiters, iter.cur, GTMWaitLink);
	}

	proclist_foreach_modify(iter, &SnapRcv->wait_commiters, GxidWaitLink)
	{
		proc = GetPGProcByNumber(iter.cur);
		proc->getGlobalTransaction = InvalidTransactionId;
		proclist_delete( &SnapRcv->wait_commiters, iter.cur, GTMWaitLink);
		SetLatch(&proc->procLatch);
	}
	UNLOCK_SNAP_GXID_RCV();
}

void SnapReceiverMain(void)
{
	TimestampTz last_recv_timestamp;
	TimestampTz timeout;
	TimestampTz last_hb_st, now;
	bool		force_send;
	int 		loop_time;
	sigjmp_buf	local_sigjmp_buf;

	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		SnapRcvStopAllWaitFinishBackend();
		EmitErrorReport();
		exit(1);
	}
	PG_exception_stack = &local_sigjmp_buf;

	Assert(SnapRcv != NULL);

	now = GetCurrentTimestamp();

	/*
	 * Mark snapreceiver as running in shared memory.
	 *
	 * Do this as early as possible, so that if we fail later on, we'll set
	 * state to STOPPED. If we die before this, the startup process will keep
	 * waiting for us to start up, until it times out.
	 */
	switch (pg_atomic_read_u32(&SnapRcv->state))
	{
		case WALRCV_STOPPING:
			/* If we've already been requested to stop, don't start up. */
			pg_atomic_write_u32(&SnapRcv->state, WALRCV_STOPPED);
			UNLOCK_SNAP_RCV();
			proc_exit(1);
			break;

		case WALRCV_STOPPED:
			pg_atomic_write_u32(&SnapRcv->state, WALRCV_STARTING);
			/* fall through, do not add break */
		case WALRCV_STARTING:
			/* The usual case */
			break;

		case WALRCV_WAITING:
		case WALRCV_STREAMING:
		case WALRCV_RESTARTING:
		default:
			/* Shouldn't happen */
			UNLOCK_SNAP_RCV();
			elog(PANIC, "snapreceiver still running according to shared memory state");
	}

	LOCK_SNAP_RCV();
	Assert(SnapRcv->pid == 0);
	/* Advertise our PID so that the startup process can kill us */
	SnapRcv->pid = MyProcPid;
	SnapRcv->procno = MyProc->pgprocno;

	UNLOCK_SNAP_RCV();
	pg_atomic_write_u32(&SnapRcv->global_xmin, InvalidTransactionId);
	pg_atomic_write_u32(&SnapRcv->last_client_req_key, 0);
	pg_atomic_write_u32(&SnapRcv->last_ss_req_key, 0);
	pg_atomic_write_u32(&SnapRcv->last_ss_resp_key, 0);
	pg_atomic_write_u64(&SnapRcv->last_heartbeat_sync_time, 0);

	/* Arrange to clean up at walreceiver exit */
	on_shmem_exit(SnapRcvDie, (Datum)0);

	/* make sure dsa_area create */
	(void)SnapGetLockArea(&SnapRcv->comm_lock);

	now = WaitUntilStartTime();

	/* Properly accept or ignore signals the postmaster might send us */
	pqsignal(SIGHUP, SnapRcvSigHupHandler);	/* set flag to read config file */
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, SnapRcvShutdownHandler);	/* request shutdown */
	pqsignal(SIGQUIT, SnapRcvQuickDieHandler);	/* hard crash time */
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, SnapRcvSigUsr1Handler);
	pqsignal(SIGUSR2, SIG_IGN);

	/* Reset some signals that are accepted by postmaster but not here */
	pqsignal(SIGCHLD, SIG_DFL);
	pqsignal(SIGTTIN, SIG_DFL);
	pqsignal(SIGTTOU, SIG_DFL);
	pqsignal(SIGCONT, SIG_DFL);
	pqsignal(SIGWINCH, SIG_DFL);

	/* We allow SIGQUIT (quickdie) at all times */
	sigdelset(&BlockSig, SIGQUIT);

	PG_SETMASK(&UnBlockSig);

	/* Load the libpq-specific functions */
	load_file("libpqwalreceiver", false);
	if (WalReceiverFunctions == NULL)
		elog(ERROR, "libpqwalreceiver didn't initialize correctly");

	/*
	 * Create a resource owner to keep track of our resources (not clear that
	 * we need this, but may as well have one).
	 */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "Wal Receiver");

	initStringInfo(&reply_message);
	initStringInfo(&incoming_message);

	/* Unblock signals (they were blocked when the postmaster forked us) */
	PG_SETMASK(&UnBlockSig);

	ReadConfigFile();
	EnableSnapRcvImmediateExit();
	SnapRcvConnectGTM();
	DisableSnapRcvImmediateExit();

	SnapRcvUpdateShmemConnInfo();
	/* Initialize the last recv timestamp */
	last_recv_timestamp = GetCurrentTimestamp();
	for (;;)
	{
		WalRcvStreamOptions options;
		TransactionId	next_xid = SnapRcvGetLocalNextXid();

		/*
		 * Check that we're connected to a valid server using the
		 * IDENTIFY_SYSTEM replication command.
		 */
		EnableSnapRcvImmediateExit();

		/* options startpoint must be InvalidXLogRecPtr and timeline be 0 */
		options.logical = true;
		options.startpoint = InvalidXLogRecPtr;
		options.slotname = PGXCNodeName;

		if (TransactionIdIsNormal(next_xid))
			options.proto.logical.proto_version = next_xid;
		else
			options.proto.logical.proto_version = InvalidTransactionId;

		/*collect all left xid inluce rxact and prepared xid*/
		options.proto.logical.publication_names = SnapRcvSendInitSyncXid();
		if (walrcv_startstreaming(wrconn, &options))
		{
			//walrcv_endstreaming(wrconn, &primaryTLI);
			/* loop until end-of-streaming or error */
			heartbeat_sent = true;
			for(;;)
			{
				char	   *buf;
				int			len;
				pgsocket	wait_fd = PGINVALID_SOCKET;
				int			rc;
				bool		endofwal = false;

				ProcessSnapRcvInterrupts();

				ReadConfigFile();

				loop_time = snap_sender_connect_timeout;
				if (snap_receiver_timeout < loop_time)
					loop_time = snap_receiver_timeout;
				if (snap_force_globalxmin_sync_time < loop_time)
					loop_time = snap_force_globalxmin_sync_time;

				Assert(loop_time > 0);
				len = walrcv_receive(wrconn, &buf, &wait_fd);
				if (len != 0)
				{
					for (;;)
					{
						if (len > 0)
						{
							last_recv_timestamp = GetCurrentTimestamp();
							heartbeat_sent = false;
							SnapRcvProcessMessage(buf[0], &buf[1], len-1);
						}else if(len == 0)
						{
							break;
						}else if(len < 0)
						{
							ereport(LOG,
									(errmsg("replication terminated by primary server")));
							endofwal = true;
							break;
						}
						len = walrcv_receive(wrconn, &buf, &wait_fd);
					}
				}

				/* Check if we need to exit the streaming loop. */
				if (endofwal)
					break;

				Assert(wait_fd != PGINVALID_SOCKET);
				rc = WaitLatchOrSocket(&MyProc->procLatch,
									   WL_POSTMASTER_DEATH | WL_SOCKET_READABLE | WL_LATCH_SET | WL_TIMEOUT,
									   wait_fd,
									   loop_time,
									   PG_WAIT_EXTENSION);
				ResetLatch(&MyProc->procLatch);
				
				SnapRcvMainProcess();
				SnapRcvProcessSnapSync();

				if (rc & WL_POSTMASTER_DEATH)
				{
					/*
					 * Emergency bailout if postmaster has died.  This is to
					 * avoid the necessity for manual cleanup of all
					 * postmaster children.
					 */
					exit(1);
				}

				force_send = false;
				last_hb_st = pg_atomic_read_u64(&SnapRcv->last_heartbeat_sync_time);
				now = GetCurrentTimestamp();
				if ((now >= TimestampTzPlusMilliseconds(last_hb_st, snap_force_globalxmin_sync_time) && !heartbeat_sent) || last_hb_st == 0)
					force_send = true;

				if (((rc & WL_TIMEOUT) && !heartbeat_sent) || force_send)
				{
					timeout = TimestampTzPlusMilliseconds(last_recv_timestamp,
								snap_receiver_timeout);

					if ((now >= timeout || force_send))
						SnapRcvSendHeartbeat();
				}
			}
		}else
		{
			ereport(LOG,
					(errmsg("primary server not start send snapshot")));
		}
	}

	proc_exit(0);
}

Size SnapRcvShmemSize(void)
{
	return sizeof(SnapRcvData);
}

void SnapRcvShmemInit(void)
{
	bool		found;

	SnapRcv = (SnapRcvData*)
		ShmemInitStruct("Snapshort Receiver", SnapRcvShmemSize(), &found);

	if (!found)
	{
		/* First time through, so initialize */
		MemSet(SnapRcv, 0, SnapRcvShmemSize());
		pg_atomic_init_u32(&SnapRcv->state, WALRCV_STOPPED);
		ConditionVariableInit(&SnapRcv->cv);

		proclist_init(&SnapRcv->waiters);
		proclist_init(&SnapRcv->ss_waiters);
		proclist_init(&SnapRcv->geters);
		proclist_init(&SnapRcv->reters);
		proclist_init(&SnapRcv->send_commiters);
		proclist_init(&SnapRcv->wait_commiters);
		proclist_init(&SnapRcv->waiters_assign);
		proclist_init(&SnapRcv->waiters_finish);
		SnapRcv->procno = INVALID_PGPROCNO;
		SpinLockInit(&SnapRcv->mutex);
		SpinLockInit(&SnapRcv->gxid_mutex);

		SnapRcv->xcnt = 0;
		SnapRcv->cur_pre_alloc = 0;

		SnapRcv->comm_lock.handle_lock_info = DSM_HANDLE_INVALID;
		SnapRcv->comm_lock.first_lock_info = InvalidDsaPointer;
		SnapRcv->comm_lock.last_lock_info = InvalidDsaPointer;
		LWLockInitialize(&SnapRcv->comm_lock.lock_lock_info, LWTRANCHE_SNAPSHOT_COMMON_DSA);
		LWLockInitialize(&SnapRcv->comm_lock.lock_proc_link, LWTRANCHE_SNAPSHOT_COMMON_DSA);
		pg_atomic_init_u32(&SnapRcv->global_xmin, InvalidTransactionId);
		pg_atomic_init_u32(&SnapRcv->last_client_req_key, 0);
		pg_atomic_init_u32(&SnapRcv->last_ss_req_key, 0);
		pg_atomic_init_u32(&SnapRcv->last_ss_resp_key, 0);
		pg_atomic_init_u64(&SnapRcv->last_heartbeat_sync_time, 0);
	}
}

static TimestampTz WaitUntilStartTime(void)
{
	TimestampTz end;
	TimestampTz now;
	TimestampTz max_end;
	int rc;

	LOCK_SNAP_RCV();
	end = SnapRcv->next_try_time;
	UNLOCK_SNAP_RCV();

	now = GetCurrentTimestamp();
	if (now > end)
		return now;

	max_end = TimestampTzPlusMilliseconds(now, RESTART_STEP_MS);
	if (end > max_end)
		end = max_end;

	while(now < end)
	{
		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT,
					   100,
					   PG_WAIT_TIMEOUT);
		ResetLatch(&MyProc->procLatch);
		now = GetCurrentTimestamp();
		if (rc & WL_POSTMASTER_DEATH)
			exit(1);
	}

	return now;
}

/* SIGTERM: set flag for main loop, or shutdown immediately if safe */
static void
SnapRcvShutdownHandler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGTERM = true;

	if (SNAP_RCV_LATCH_VALID())
		SNAP_RCV_SET_LATCH();

	errno = save_errno;
}

static void SnapRcvDie(int code, Datum arg)
{
	uint32 state;

	state = pg_atomic_read_u32(&SnapRcv->state);
	/* Mark ourselves inactive in shared memory */
	
	Assert(state == WALRCV_STREAMING ||
		   state == WALRCV_RESTARTING ||
		   state == WALRCV_STARTING ||
		   state == WALRCV_WAITING ||
		   state == WALRCV_STOPPING);
	
	pg_atomic_write_u32(&SnapRcv->state, WALRCV_STOPPED);

	LOCK_SNAP_RCV();
	Assert(SnapRcv->pid == MyProcPid);
	SnapRcv->pid = 0;
	SnapRcv->procno = INVALID_PGPROCNO;
	SnapRcv->xcnt = 0;
	SnapRcv->cur_pre_alloc = 0;
	SnapRcv->wait_finish_cnt = 0;

	pg_atomic_write_u64(&SnapRcv->last_heartbeat_sync_time, 0);
	SnapRcv->next_try_time = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), RESTART_STEP_MS);	/* 3 seconds */
	UNLOCK_SNAP_RCV();

	/* Terminate the connection gracefully. */
	if (wrconn != NULL)
		walrcv_disconnect(wrconn);
}

static void isSnapRcvStreamOk(void)
{
	uint32		state;
	instr_time	start_time;
	instr_time	cur_time;
	long		timeout;

	state = pg_atomic_read_u32(&SnapRcv->state);
	if (likely(WALRCV_STREAMING == state))
		return;

	INSTR_TIME_SET_CURRENT(start_time);
	timeout = snap_receiver_timeout;
	ConditionVariablePrepareToSleep(&SnapRcv->cv);
	for(;;)
	{
		if (ConditionVariableTimedSleep(&SnapRcv->cv, timeout, WAIT_EVENT_SAFE_SNAPSHOT))
			ereport(ERROR, (errmsg("cannot connect to GTMCOORD: wait stream timeout")));

		state = pg_atomic_read_u32(&SnapRcv->state);
		SNAP_SYNC_DEBUG_LOG((errmsg("isSnapRcvStreamOk SnapRcv->state %d\n", state)));
		if (state == WALRCV_STREAMING)
			break;
		
		/* update timeout for next iteration */
		INSTR_TIME_SET_CURRENT(cur_time);
		INSTR_TIME_SUBTRACT(cur_time, start_time);
		timeout -= (long) INSTR_TIME_GET_MILLISEC(cur_time);

		/* Have we crossed the timeout threshold? */
		if (timeout <= 0)
			ereport(ERROR, (errmsg("cannot connect to GTMCOORD: wait stream timeout")));
	}
	SNAP_SYNC_DEBUG_LOG((errmsg("isSnapRcvStreamOk return\n")));
	ConditionVariableCancelSleep();
}


/* SIGHUP: set flag to re-read config file at next convenient time */
static void
SnapRcvSigHupHandler(SIGNAL_ARGS)
{
	got_SIGHUP = true;
}


/* SIGUSR1: used by latch mechanism */
static void
SnapRcvSigUsr1Handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	latch_sigusr1_handler();

	errno = save_errno;
}

/*
 * WalRcvQuickDieHandler() occurs when signalled SIGQUIT by the postmaster.
 *
 * Some backend has bought the farm, so we need to stop what we're doing and
 * exit.
 */
static void
SnapRcvQuickDieHandler(SIGNAL_ARGS)
{
	if (proc_exit_inprogress)
		return;

	proc_exit_inprogress = true;

	PG_SETMASK(&BlockSig);

	/*
	 * We DO NOT want to run proc_exit() callbacks -- we're here because
	 * shared memory may be corrupted, so we don't want to try to clean up our
	 * transaction.  Just nail the windows shut and get out of town.  Now that
	 * there's an atexit callback to prevent third-party code from breaking
	 * things by calling exit() directly, we have to reset the callbacks
	 * explicitly to make this work as intended.
	 */
	on_exit_reset();

	/*
	 * Note we do exit(2) not exit(0).  This is to force the postmaster into a
	 * system reset cycle if some idiot DBA sends a manual SIGQUIT to a random
	 * backend.  This is necessary precisely because we don't clean up our
	 * shared memory state.  (The "dead man switch" mechanism in pmsignal.c
	 * should ensure the postmaster sees this as a crash, too, but no harm in
	 * being doubly sure.)
	 */
	exit(2);
}

static void SnapRcvConnectGTM(void)
{
	char conninfo[MAXCONNINFO];
	char *errstr;

	Assert(wrconn == NULL);

	snprintf(conninfo, MAXCONNINFO,
			 "user=postgres host=%s port=%d contype=snaprcv",
			 AGtmHost, AGtmPort);
	wrconn = walrcv_connect(conninfo, true, "snapreceiver", &errstr);
	if (!wrconn)
		ereport(ERROR,
				(errmsg("could not connect to the GTM server: %s", errstr)));
}

void SnapRcvUpdateShmemConnInfo(void)
{
	char *sender_host;
	int sender_port;

	walrcv_get_senderinfo(wrconn, &sender_host, &sender_port);

	LOCK_SNAP_RCV();

	memset(SnapRcv->sender_host, 0, NI_MAXHOST);
	if (sender_host)
		strlcpy(SnapRcv->sender_host, sender_host, NI_MAXHOST);

	SnapRcv->sender_port = sender_port;

	UNLOCK_SNAP_RCV();

	if (sender_host)
		pfree(sender_host);
}

static void SnapRcvProcessSyncSnap(char *buf, Size len)
{
	uint32_t 	key;
	StringInfoData	msg;

	if (len != sizeof(uint64_t))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid sync response key length")));

	msg.data = buf;
	msg.len = msg.maxlen = len;
	msg.cursor = 0;
	key = pq_getmsgint64(&msg);
	SNAP_FORCE_DEBUG_LOG((errmsg("SnapRcvProcessSyncSnap key %lld\n", key)));
	WakeupSnapSync(key);
}

/* must has get the SnapRcv gxid lock */
static void SnapRcvRemoveWaitFinishList(TransactionId xid, bool is_miss_ok)
{
	int i, count;
	bool found;

	found = false;
	count = SnapRcv->wait_finish_cnt;

	if (!is_miss_ok)
		Assert(count > 0);
	for (i = 0; i < count; i++)
	{
		if (SnapRcv->wait_xid_finish[i] == xid)
		{
			SNAP_SYNC_DEBUG_LOG((errmsg("Remove finish wait xid %u from wait_xid_finish\n", xid)));
			found = true;
			memmove(&SnapRcv->wait_xid_finish[i],
						&SnapRcv->wait_xid_finish[i+1],
						(count-i-1) * sizeof(xid));
			SnapRcv->wait_finish_cnt--;
			break;
		}
	}

	if (!is_miss_ok)
		Assert(found);
}

static void SnapRcvProcessFinishRequest(char *buf, Size len)
{
	StringInfoData				msg;
	TransactionId				txid;
	int							procno;
	PGPROC						*proc;				
	proclist_mutable_iter		iter;
	//bool						found;

	msg.data = buf;
	msg.len = msg.maxlen = len;
	msg.cursor = 0;
	
	LOCK_SNAP_GXID_RCV();
	while(msg.cursor < msg.len)
	{
		procno = pq_getmsgint(&msg, sizeof(procno));
		txid = pq_getmsgint(&msg, sizeof(txid));

		SNAP_SYNC_DEBUG_LOG((errmsg("SnapRcv  rcv finish xid %u for %d\n", txid, procno)));
		SnapRcvRemoveWaitFinishList(txid, true);
		Assert(TransactionIdIsValid(txid));

		//found = false;
		proclist_foreach_modify(iter, &SnapRcv->wait_commiters, GxidWaitLink)
		{
			proc = GetPGProcByNumber(iter.cur);
			if (proc->pgprocno == procno && proc->getGlobalTransaction == txid)
			{
				proc->getGlobalTransaction = InvalidTransactionId;
				SetLatch(&proc->procLatch);
				//found = true;
				break;
			}
		}
		//Assert(found);
	}
	UNLOCK_SNAP_GXID_RCV();
}

static void SnapRcvProcessAssignRequest(char *buf, Size len)
{
	StringInfoData			msg;
	TransactionId			txid;
	int						procno;
	PGPROC					*proc;				
	proclist_mutable_iter	iter;
	bool					found;
	int						finish_num;

	msg.data = buf;
	msg.len = msg.maxlen = len;
	msg.cursor = 0;
	finish_num = 0;

	LOCK_SNAP_GXID_RCV();
	while(msg.cursor < msg.len)
	{
		procno = pq_getmsgint(&msg, sizeof(procno));
		txid = pq_getmsgint(&msg, sizeof(txid));

		Assert(TransactionIdIsValid(txid));
		SNAP_SYNC_DEBUG_LOG((errmsg("SnapRcv  rcv assing xid %u for %d\n", txid, procno)));

		found = false;
		proclist_foreach_modify(iter, &SnapRcv->reters, GxidWaitLink)
		{
			proc = GetPGProcByNumber(iter.cur);
			if (proc->pgprocno == procno && proc->getGlobalTransaction == InvalidTransactionId)
			{
				proc->getGlobalTransaction = txid;
				SetLatch(&proc->procLatch);
				found = true;
				break;
			}
		}
		/* when there is no dn/dn wait transaction id, we should finish this transaction id.*/
		if (!found)
		{
			if (finish_num == 0)
			{
				resetStringInfo(&reply_message);
				pq_sendbyte(&reply_message, 'c');
				pq_sendstring(&reply_message, PGXCNodeName);
			}
			finish_num++;
			pq_sendint32(&reply_message, 0);
			pq_sendint32(&reply_message, txid);
		}
	}
	UNLOCK_SNAP_GXID_RCV();

	if (finish_num > 0)
		walrcv_send(wrconn, reply_message.data, reply_message.len);
}

static int
SnapRcvXidComparator(const void *arg1, const void *arg2)
{
	TransactionId xid1 = *(const TransactionId *) arg1;
	TransactionId xid2 = *(const TransactionId *) arg2;

	if (xid1 < xid2)
		return 1;
	if (xid1 > xid2)
		return -1;
	return 0;
}

static void SnapRcvProcessPreAssign(char *buf, Size len)
{
	StringInfoData			msg;
	TransactionId			txid;
	int						num, start_index;
			
	msg.data = buf;
	msg.len = msg.maxlen = len;
	msg.cursor = 0;

	SNAP_SYNC_DEBUG_LOG((errmsg("SnapRcv rcv pre assing: ")));
	num = pq_getmsgint(&msg, sizeof(num));
	Assert(num > 0 && num <= MAX_XID_PRE_ALLOC_NUM);
	
	LOCK_SNAP_GXID_RCV();
	Assert((SnapRcv->cur_pre_alloc + num) <= MAX_XID_PRE_ALLOC_NUM);
	start_index = SnapRcv->cur_pre_alloc;
	while(msg.cursor < msg.len)
	{
		txid = pq_getmsgint(&msg, sizeof(txid));

		Assert(TransactionIdIsValid(txid));

		SNAP_SYNC_DEBUG_LOG((errmsg(" %u\n", txid)));
		num--;
		SnapRcv->xid_alloc[start_index + num] = txid;
		SnapRcv->cur_pre_alloc++;
	}
	SnapRcv->is_send_realloc_num = 0;
	Assert(SnapRcv->cur_pre_alloc <= MAX_XID_PRE_ALLOC_NUM);

	qsort(SnapRcv->xid_alloc, SnapRcv->cur_pre_alloc, sizeof(TransactionId), SnapRcvXidComparator);
	UNLOCK_SNAP_GXID_RCV();

	Assert(num == 0);	
}

static void SnapRcvProcessMessage(unsigned char type, char *buf, Size len)
{
	resetStringInfo(&incoming_message);

	switch (type)
	{
	case 's':				/* snapshot */
		SnapRcvProcessSnapshot(buf, len);
		SnapRcvSendHeartbeat();
		break;
	case 'a':
		SnapRcvProcessAssign(buf, len);
		break;
	case 'c':
		SnapRcvProcessComplete(buf, len);
		break;
	case 'g':
		SnapRcvProcessAssignRequest(buf, len);
		break;
	case 'f':
		SnapRcvProcessFinishRequest(buf, len);
		break;
	case 'q':
		SnapRcvProcessPreAssign(buf, len);
		break;
	case 'h':				/* heartbeat response */
		SnapRcvProcessHeartBeat(buf, len);
		break;
	case 'u':				/* heartbeat response */
		SnapRcvProcessUpdateXid(buf, len);
		break;
	case 'p':				/* heartbeat response */
		SnapRcvProcessSyncSnap(buf, len);
		break;
	default:
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg_internal("invalid replication message type %d",
								 type)));
	}
}

static void SnapRcvProcessSnapshot(char *buf, Size len)
{
	TransactionId latestCompletedXid, xmin;
	TransactionId *xid;
	uint32 i,count;
#define SNAP_HDR_LEN	(sizeof(TransactionId))

	if (len < SNAP_HDR_LEN ||
		(len - SNAP_HDR_LEN) % sizeof(TransactionId) != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg_internal("invalid snapshot message received from GTM")));
	}

	/* copy message to StringInfo */
	resetStringInfo(&incoming_message);
	appendBinaryStringInfoNT(&incoming_message, buf, len);

	xmin = pq_getmsgint(&incoming_message, sizeof(TransactionId));
	latestCompletedXid = pq_getmsgint(&incoming_message, sizeof(TransactionId));
	count = (incoming_message.len - incoming_message.cursor) / sizeof(TransactionId);

	SNAP_SYNC_DEBUG_LOG((errmsg("SnapRcvProcessSnapshot count %d, SnapRcv->latestCompletedXid %u\n", count, latestCompletedXid)));
	if (count > lengthof(SnapRcv->xip))
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("too many active transaction ID from GTM %u", count)));
	}
	i = 0;
	if (count > 0)
	{
		xid = palloc(count*sizeof(TransactionId));
		i = 0;
		while(incoming_message.cursor < incoming_message.len)
			xid[i++] = pq_getmsgint(&incoming_message, sizeof(TransactionId));
	}else
	{
		xid = NULL;
	}

	LOCK_SNAP_RCV();
	SnapRcv->latestCompletedXid = latestCompletedXid;
	if (count > 0)
	{
		SnapRcv->xcnt = count;
		memcpy(SnapRcv->xip, xid, sizeof(TransactionId)*count);
	}else
	{
		SnapRcv->xcnt = 0;
	}

	pg_atomic_write_u32(&SnapRcv->state, WALRCV_STREAMING);
	WakeupTransactionInit(xid, i, &SnapRcv->waiters);
	WakeupTransactionInit(xid, i, &SnapRcv->ss_waiters);

	UNLOCK_SNAP_RCV();
	ConditionVariableBroadcast(&SnapRcv->cv);

	if (TransactionIdIsValid(xmin))
		pg_atomic_write_u32(&SnapRcv->global_xmin, xmin);
	
	SnapReleaseSnapshotTxidLocks(&SnapRcv->comm_lock,xid, count, latestCompletedXid);

	if (xid != NULL)
		pfree(xid);

#undef SNAP_HDR_LEN
}

static void SnapRcvProcessUpdateXid(char *buf, Size len)
{
	StringInfoData	msg;
	TransactionId	xid;
	TransactionId	nextXid;
	uint32			epoch;

	msg.data = buf;
	msg.len = msg.maxlen = len;
	msg.cursor = 0;

	xid = pq_getmsgint64(&msg);
	
	SNAP_SYNC_DEBUG_LOG((errmsg("SnapRcvProcessUpdateXid xid %u\n", xid)));
	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	nextXid = XidFromFullTransactionId(ShmemVariableCache->nextFullXid);
	if (!NormalTransactionIdPrecedes(xid, nextXid))
	{
		epoch = EpochFromFullTransactionId(ShmemVariableCache->nextFullXid);
		if (unlikely(xid < nextXid))
			++epoch;
		ShmemVariableCache->nextFullXid = FullTransactionIdFromEpochAndXid(epoch, xid);
		FullTransactionIdAdvance(&ShmemVariableCache->nextFullXid);

		ShmemVariableCache->latestCompletedXid = XidFromFullTransactionId(ShmemVariableCache->nextFullXid);
		TransactionIdRetreat(ShmemVariableCache->latestCompletedXid);

		LOCK_SNAP_RCV();
		SnapRcv->latestCompletedXid = ShmemVariableCache->latestCompletedXid;
		UNLOCK_SNAP_RCV();
	}
	LWLockRelease(XidGenLock);
}

static void SnapRcvProcessAssign(char *buf, Size len)
{
	StringInfoData	msg;
	TransactionId	txid;
	TransactionId	nextXid;
	TransactionId	gxid = FirstNormalTransactionId;

	if ((len % sizeof(txid)) != 0 ||
		len == 0)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid snapshot transaction assign message length")));

	msg.data = buf;
	msg.len = msg.maxlen = len;
	msg.cursor = 0;

	LOCK_SNAP_RCV();
	while(msg.cursor < msg.len)
	{
		txid = pq_getmsgint(&msg, sizeof(txid));

		SNAP_SYNC_DEBUG_LOG((errmsg("SanpRcv recv assging xid %u\n", txid)));
		if (SnapRcv->xcnt < MAX_BACKENDS)
		{
			SnapRcv->xip[SnapRcv->xcnt++] = txid;
		}else
		{
			UNLOCK_SNAP_RCV();
			ereport(FATAL,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("too many active transaction from GTM")));
		}
		if (gxid == InvalidTransactionId || NormalTransactionIdFollows(txid, gxid))
			gxid = txid;
	}
	UNLOCK_SNAP_RCV();

	Assert(TransactionIdIsNormal(gxid));
	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);

	/*
	 * If we are allocating the first XID of a new page of the commit log,
	 * zero out that commit-log page before returning. We must do this while
	 * holding XidGenLock, else another xact could acquire and commit a later
	 * XID before we zero the page.  Fortunately, a page of the commit log
	 * holds 32K or more transactions, so we don't have to do this very often.
	 *
	 * Extend pg_subtrans and pg_commit_ts too.
	 */
	/* for slave cn/dn, we cannot extend clog and insert wal log */
	if (!RecoveryInProgress())
	{
		ExtendCLOG(gxid);
		ExtendCommitTs(gxid);
		ExtendSUBTRANS(gxid);
	}

	nextXid = XidFromFullTransactionId(ShmemVariableCache->nextFullXid);
	if (TransactionIdFollowsOrEquals(gxid, nextXid))
	{
		uint32	epoch;
		epoch = EpochFromFullTransactionId(ShmemVariableCache->nextFullXid);
		if (unlikely(gxid < nextXid))
			++epoch;

		ShmemVariableCache->nextFullXid = FullTransactionIdFromEpochAndXid(epoch, gxid);
	}
	/*
	 * Now advance the nextXid counter.  This must not happen until after we
	 * have successfully completed ExtendCLOG() --- if that routine fails, we
	 * want the next incoming transaction to try it again.	We cannot assign
	 * more XIDs until there is CLOG space for them.
	 */
	
	LWLockRelease(XidGenLock);
}

static void SnapRcvProcessComplete(char *buf, Size len)
{
	StringInfoData	msg;
	TransactionId	txid, max_xid;
	uint32			i,count;
	StringInfoData	xidmsg;

	if (((len-1) % sizeof(txid)) != 0 ||
		len == 0)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid snapshot transaction assign message length")));

	msg.data = buf;
	msg.len = msg.maxlen = len;
	msg.cursor = 0;
	max_xid = InvalidTransactionId;

	initStringInfo(&xidmsg);
	enlargeStringInfo(&xidmsg, msg.maxlen);

	finish_xid_ack_send = pq_getmsgbyte(&msg);
	if (finish_xid_ack_send)
		pq_sendbyte(&xidmsg, 'f');

	if (0)
	{
#ifdef USE_ASSERT_CHECKING
		while (msg.cursor < msg.len)
		{
			bool isInProgress;
			txid = pq_getmsgint(&msg, sizeof(txid));
			isInProgress = TransactionIdIsInProgressExt(txid, true);
			Assert(!isInProgress);
		}
#endif /* USE_ASSERT_CHECKING */
	}

	LOCK_SNAP_RCV();
	count = SnapRcv->xcnt;
	msg.cursor = sizeof(bool);
	while(msg.cursor < msg.len)
	{
		txid = pq_getmsgint(&msg, sizeof(txid));
		for (i=0;i<count;++i)
		{
			if (SnapRcv->xip[i] == txid)
			{
				SNAP_SYNC_DEBUG_LOG((errmsg("SanpRcv recv finish xid %u\n", txid)));
				memmove(&SnapRcv->xip[i],
						&SnapRcv->xip[i+1],
						(count-i-1) * sizeof(txid));
				if (TransactionIdPrecedes(SnapRcv->latestCompletedXid, txid))
					SnapRcv->latestCompletedXid = txid;
				break;
			}
		}
		if (i>=count)
		{
			UNLOCK_SNAP_RCV();
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("transaction %u from GTM not found in active transaction", txid)));
		}
		--count;
		WakeupTransaction(txid);
		if (finish_xid_ack_send)
			pq_sendint32(&xidmsg, txid);
	}

	SnapRcv->xcnt = count;
	max_xid = SnapRcv->latestCompletedXid;
	UNLOCK_SNAP_RCV();
	SNAP_SYNC_DEBUG_LOG((errmsg("SanpRcv xcnt now is %u\n", count)));

	if (finish_xid_ack_send)
		walrcv_send(wrconn, xidmsg.data, xidmsg.len);
	pfree(xidmsg.data);

	if (TransactionIdPrecedes(ShmemVariableCache->latestCompletedXid,
							  max_xid))
	{
		LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
		ShmemVariableCache->latestCompletedXid = max_xid;
		LWLockRelease(XidGenLock);
	}

	msg.cursor = sizeof(bool);
	while (msg.cursor < msg.len)
	{
		txid = pq_getmsgint(&msg, sizeof(txid));
		SnapReleaseTransactionLocks(&SnapRcv->comm_lock, txid);
	}
}

static void SnapRcvProcessHeartBeat(char *buf, Size len)
{
	StringInfoData	msg;
	TransactionId 	xmin;
	TimestampTz		t1, t2, t3, t4, deltatime;

	if (len != 4 * sizeof(t1))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid snapshot transaction timestamp length")));

	t4 = GetCurrentTimestamp();
	msg.data = buf;
	msg.len = msg.maxlen = len;
	msg.cursor = 0;

	t1 = pq_getmsgint64(&msg);
	t2 = pq_getmsgint64(&msg);
	t3 = pq_getmsgint64(&msg);
	xmin = pq_getmsgint64(&msg);
	if (t1 == last_heat_beat_sendtime)
	{
		deltatime = ((t2-t1)+(t3-t4))/2;

		LOCK_SNAP_RCV();
		SnapRcv->gtm_delta_time = deltatime;
		UNLOCK_SNAP_RCV();
	}

	pg_atomic_write_u32(&SnapRcv->global_xmin, xmin);
}

/*
 * when end < 0 wait until streaming or error
 *   when end == 0 not block
 * mutex must be locked
 */
static bool WaitSnapRcvEvent(TimestampTz end, proclist_head *waiters, bool is_ss, WaitSnapRcvCond test, void *context)
{
	Latch				   *latch = &MyProc->procLatch;
	long					timeout;
	proclist_mutable_iter	iter;
	int						procno = MyProc->pgprocno;
	int						rc;
	int						waitEvent;
	bool					ret;

	ret = true;
	while ((*test)(context))
	{
		bool in_list = false;
		proclist_foreach_modify(iter, waiters, GTMWaitLink)
		{
			if (iter.cur == procno)
			{
				in_list = true;
				break;
			}
		}
		if (!in_list)
		{
			pg_write_barrier();
			proclist_push_tail(waiters, procno, GTMWaitLink);
			if (is_ss && SNAP_RCV_LATCH_VALID())
				SNAP_RCV_SET_LATCH();
		}
		UNLOCK_SNAP_RCV();

		waitEvent = WL_POSTMASTER_DEATH | WL_LATCH_SET;
		if (end > 0)
		{
			long secs;
			int microsecs;
			TimestampDifference(GetCurrentTimestamp(), end, &secs, &microsecs);
			timeout = secs*1000 + microsecs/1000;
			waitEvent |= WL_TIMEOUT;
		}else if (end == 0)
		{
			timeout = 0;
			waitEvent |= WL_TIMEOUT;
		}else
		{
			timeout = -1;
		}
		rc = WaitLatch(latch, waitEvent, timeout, PG_WAIT_EXTENSION);
		ResetLatch(latch);
		if (rc & WL_POSTMASTER_DEATH)
		{
			exit(1);
		}else if(rc & WL_TIMEOUT)
		{
			ret = false;
		}

		LOCK_SNAP_RCV();
		if (ret == false)
			break;
	}

	/* check if we still in waiting list, remove */
	proclist_foreach_modify(iter, waiters, GTMWaitLink)
	{
		if (iter.cur == procno)
		{
			proclist_delete(waiters, procno, GTMWaitLink);
			break;
		}
	}

	return ret;
}

static bool WaitSnapRcvCondTransactionComplate(void *context)
{
	TransactionId	txid;
	TransactionId	xid;
	TransactionId	xmax;
	uint32			xcnt;

	/* not in streaming, wait */
	if (pg_atomic_read_u32(&SnapRcv->state) != WALRCV_STREAMING)
		return true;

	txid = (TransactionId)((size_t)context);
	xmax = SnapRcv->latestCompletedXid;
	xcnt = SnapRcv->xcnt;
	while(xcnt>0)
	{
		xid = SnapRcv->xip[--xcnt];

		/* active, wait */
		if (TransactionIdEquals(xid, txid))
			return true;

		if (NormalTransactionIdPrecedes(xmax, xid))
			xmax = xid;
	}

	/* not start yet, wait */
	if (NormalTransactionIdPrecedes(xmax, txid))
		return true;

	/* in streaming and not active, do not wait */
	return false;
}

static bool WaitSnapRcvSyncSnap(void *context)
{
	uint32_t req_key, ss_resp_key;
	req_key = (uint32_t)((size_t)context);
	ss_resp_key = pg_atomic_read_u32(&SnapRcv->last_ss_resp_key);
	if (req_key <= ss_resp_key && (ss_resp_key - req_key < SYNC_KEY_SAFE_GAP))
		return false;
	else
		return true;
}

static void WakeupSnapSync(uint32_t resp_key)
{
	proclist_mutable_iter	iter;
	PGPROC					*proc;

	LOCK_SNAP_RCV();
	proclist_foreach_modify(iter, &SnapRcv->ss_waiters, GTMWaitLink)
	{
		proc = GetPGProcByNumber(iter.cur);
		//ereport(LOG,(errmsg("proc->ss_req_key  %lld\n", proc->ss_req_key)));
		if (proc->ss_req_key <= resp_key)
		{
			//ereport(LOG,(errmsg("snaprcv wake up process %d, delete from list\n", proc->pgprocno)));
			proc->ss_req_key = 0;
			proclist_delete(&SnapRcv->ss_waiters, proc->pgprocno, GTMWaitLink);
			SetLatch(&proc->procLatch);
		}
	}
	UNLOCK_SNAP_RCV();

	//ereport(LOG,(errmsg("snaprcv last_ss_resp_key up to %lld\n", resp_key)));
	pg_atomic_write_u32(&SnapRcv->last_ss_resp_key, resp_key);
}

/* mutex must be locked */
static void WakeupTransaction(TransactionId txid)
{
	proclist_mutable_iter	iter;
	PGPROC				   *proc;

	proclist_foreach_modify(iter, &SnapRcv->waiters, GTMWaitLink)
	{
		proc = GetPGProcByNumber(iter.cur);

		if (proc->waitGlobalTransaction == txid)
		{
			proclist_delete(&SnapRcv->waiters, proc->pgprocno, GTMWaitLink);
			SetLatch(&proc->procLatch);
		}
	}
}

/* mutex must be locked */
static void WakeupTransactionInit(TransactionId *xids, int count, proclist_head	*waiters)
{
	proclist_mutable_iter	iter;
	PGPROC				 	*proc;
	int						i;
	TransactionId			xid;
	bool					found;

	proclist_foreach_modify(iter, waiters, GTMWaitLink)
	{
		proc = GetPGProcByNumber(iter.cur);
		if (proc->waitGlobalTransaction == InvalidTransactionId)
		{
			proclist_delete(waiters, proc->pgprocno, GTMWaitLink);
			SetLatch(&proc->procLatch);
		}
		else /* proc->waitGlobalTransaction not InvalidTransactionId */
		{
			found = false;
			for (i = 0; i < count; i++)
			{
				xid = xids[i];
				if (proc->waitGlobalTransaction == xid)
				{
					found = true;
					break;
				}
			}
			if (found == false)
			{
				proclist_delete(waiters, proc->pgprocno, GTMWaitLink);
				SetLatch(&proc->procLatch);
			}
		}
	}
}

Snapshot SnapRcvGetSnapshot(Snapshot snap, TransactionId last_mxid,
					bool isCatalog)
{
	TransactionId	xid,xmax,xmin,gfxid;
	uint32			i,count,xcnt;
	bool			is_wait_ok;
	TimestampTz		end;
	uint32_t		req_key;

	if (IsInitProcessingMode() && pg_atomic_read_u32(&SnapRcv->state) != WALRCV_STREAMING)
		return snap;

	if (snap->xip == NULL)
		EnlargeSnapshotXip(snap, GetMaxSnapshotXidCount());

	if (force_snapshot_consistent == FORCE_SNAP_CON_ON || (IsConnFromCoord() && RecoveryInProgress()))
	{
		end = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), snap_receiver_timeout);
		
		is_wait_ok = true;
		if (pg_atomic_read_u32(&SnapRcv->state) == WALRCV_STREAMING)
		{
			req_key = pg_atomic_add_fetch_u32(&SnapRcv->last_client_req_key, 1);
			SNAP_FORCE_DEBUG_LOG((errmsg("Add proce %d to wait snap sync list, req_key %lld,  SnapRcv->last_ss_resp_key %lld\n", 
					MyProc->pgprocno, req_key, pg_atomic_read_u32(&SnapRcv->last_ss_resp_key))));
			MyProc->ss_req_key = req_key;
			LOCK_SNAP_RCV();
			is_wait_ok = WaitSnapRcvEvent(end, &SnapRcv->ss_waiters, true, WaitSnapRcvSyncSnap, (void*)((size_t)req_key));
			UNLOCK_SNAP_RCV();
		}
		
		if (!is_wait_ok)
		{
			ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("Wait sync response msg from gtmc time out, which key number is %u", req_key),
						errhint("you can modfiy guc parameter \"waitglobaltransaction\" on coordinators to wait the global transaction id committed on agtm")));
		}
	}
	else if(force_snapshot_consistent == FORCE_SNAP_CON_NODE || force_snapshot_consistent == FORCE_SNAP_CON_SESSION)
	{
		if (IsCnMaster())
		{
			gfxid = pg_atomic_read_u32(&SnapRcv->global_finish_id);
			SNAP_FORCE_DEBUG_LOG((errmsg("wait gfxid finish %d\n", gfxid)));
			if (TransactionIdIsNormal(gfxid))
			{
				end = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), WaitGlobalTransaction);
				if (SnapRcvWaitTopTransactionEnd(gfxid, end) == false)
				{
					ereport(ERROR,
							(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("wait global last xid commit time out, which version is %u", gfxid),
							errhint("you can modfiy guc parameter \"waitglobaltransaction\" on coordinators to wait the global transaction id committed on agtm")));
				}
			}
			pg_atomic_compare_exchange_u32(&SnapRcv->global_finish_id, &gfxid, InvalidTransactionId);
		}

		if (TransactionIdIsNormal(last_mxid))
		{
			SNAP_FORCE_DEBUG_LOG((errmsg("SnapRcvGetSnapshot wait session xid %u", last_mxid)));
			end = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), WaitGlobalTransaction);
			SNAP_FORCE_DEBUG_LOG((errmsg("wait last global last_mxid %d\n", last_mxid)));
			if (SnapRcvWaitTopTransactionEnd(last_mxid, end) == false)
			{
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("wait session last xid commit time out, which version is %u", last_mxid),
						errhint("you can modfiy guc parameter \"waitglobaltransaction\" on coordinators to wait the global transaction id committed on agtm")));
			}
		}
	}

re_lock_:
	isSnapRcvStreamOk();
	LOCK_SNAP_RCV();
	if (snap->max_xcnt < SnapRcv->xcnt)
	{
		count = SnapRcv->xcnt;

		/*
		 * EnlargeSnapshotXip maybe report an error,
		 * so release lock first
		 */
		UNLOCK_SNAP_RCV();
		EnlargeSnapshotXip(snap, count);
		goto re_lock_;
	}

	xcnt = 0;
	count = SnapRcv->xcnt;
	xmax = SnapRcv->latestCompletedXid;
	Assert(TransactionIdIsNormal(xmax));
	TransactionIdAdvance(xmax);
	xmin = xmax;

	for (i=0; i<count; ++i)
	{
		xid = SnapRcv->xip[i];

		/* If the XID is >= xmax, we can skip it */
		if (!NormalTransactionIdPrecedes(xid, xmax))
			continue;

		if (NormalTransactionIdPrecedes(xid, xmin))
			xmin = xid;

		/* We don't include our own XIDs (if any) in the snapshot */
		if (xid == MyPgXact->xid)
			continue;
			
		/* Add XID to snapshot. */
		snap->xip[xcnt++] = xid;
	}

	SetGlobalDeltaTimeStamp(SnapRcv->gtm_delta_time);
	UNLOCK_SNAP_RCV();

	snap->xcnt = xcnt;
	snap->xmax = xmax;
	snap->xmin = xmin;

	/* for not suport sub transaction */
	snap->subxcnt = 0;
	snap->suboverflowed = false;

#ifdef USE_ASSERT_CHECKING
	for(i=0;i<xcnt;++i)
	{
		Assert(!NormalTransactionIdFollows(snap->xmin, snap->xip[i]));
	}
#endif /* USE_ASSERT_CHECKING */

	return snap;
}

bool SnapRcvWaitTopTransactionEnd(TransactionId txid, TimestampTz end)
{
	bool result;
	Assert(TransactionIdIsNormal(txid));

	MyProc->waitGlobalTransaction = txid;
	LOCK_SNAP_RCV();
	result = WaitSnapRcvEvent(end, &SnapRcv->waiters, false,
							  WaitSnapRcvCondTransactionComplate,
							  (void*)((size_t)txid));
	UNLOCK_SNAP_RCV();
	MyProc->waitGlobalTransaction = InvalidTransactionId;

	return result;
}

static TransactionId SnapRcvGetLocalXmin(void)
{
	TransactionId	xid,xmin,xmax, oldxmin;
	uint32			i,count;

	if (pg_atomic_read_u32(&SnapRcv->state) != WALRCV_STREAMING)
		return InvalidTransactionId;

	Assert(pg_atomic_read_u32(&SnapRcv->state) == WALRCV_STREAMING);
	LOCK_SNAP_RCV();
	count = SnapRcv->xcnt;
	xmin = xmax = SnapRcv->latestCompletedXid;

	for (i=0; i<count; ++i)
	{
		xid = SnapRcv->xip[i];

		/* If the XID is >= xmax, we can skip it */
		if (!NormalTransactionIdPrecedes(xid, xmax))
			continue;

		if (NormalTransactionIdPrecedes(xid, xmin))
			xmin = xid;
	}

	UNLOCK_SNAP_RCV();
	if (!RecoveryInProgress())
	{
		oldxmin = GetOldestXminExt(NULL, PROCARRAY_FLAGS_VACUUM, true);
		if (NormalTransactionIdPrecedes(oldxmin, xmin))
			xmin = oldxmin;
	}

	//ereport(LOG,(errmsg("SnapRcvGetLocalXmin xid %d\n", xmin)));
	return xmin;
}

TransactionId SnapRcvGetGlobalXmin(void)
{
	TransactionId 	xmin;
	xmin = pg_atomic_read_u32(&SnapRcv->global_xmin);
	return xmin;
}

void SnapRcvTransferLock(void **param, TransactionId xid, struct PGPROC *from)
{
	SnapTransferLock(&SnapRcv->comm_lock, param, xid, from);
}

static bool WaitSnapRcvCondReturn(void *context, proclist_head *reters)
{
	proclist_mutable_iter	iter;
	PGPROC					*proc;	
	int						procno = MyProc->pgprocno;
	/* not in streaming, wait */
	if (pg_atomic_read_u32(&SnapRcv->state) != WALRCV_STREAMING)
		return true;

	proclist_foreach_modify(iter, reters, GxidWaitLink)
	{
		proc = GetPGProcByNumber(iter.cur);
		if (proc->pgprocno == procno && TransactionIdIsValid(proc->getGlobalTransaction))
		{
			return false;
		}
	}
	return true;
}

static bool WaitSnapRcvCommitReturn(void *context, proclist_head *wait_commiters)
{
	proclist_mutable_iter	iter;
	PGPROC					*proc;
	int						procno = MyProc->pgprocno;

	/* not in streaming, wait */
	if (pg_atomic_read_u32(&SnapRcv->state) != WALRCV_STREAMING)
		return false;

	proclist_foreach_modify(iter, wait_commiters, GxidWaitLink)
	{
		proc = GetPGProcByNumber(iter.cur);
		if (proc->pgprocno == procno && !TransactionIdIsValid(proc->getGlobalTransaction))
		{
			return false;
		}
	}
	return true;
}

/* must has get the gxid lock */
static bool SnapRcvFoundWaitFinishList(TransactionId xid)
{
	int i, count;
	bool found;

	found = false;
	count = SnapRcv->wait_finish_cnt;

	for (i = 0; i < count; i++)
	{
		if (SnapRcv->wait_xid_finish[i] == xid)
		{
			found = true;
			break;
		}
	}

	return found;
}

TransactionId SnapRcvGetGlobalTransactionId(bool isSubXact)
{
	TimestampTz				endtime;
	int						retry_time, max_retry_time;
	int						wait_loop_time = snapshot_sync_waittime/10;

	if(isSubXact)
		ereport(ERROR, (errmsg("cannot assign XIDs in child transaction")));

	SNAP_SYNC_DEBUG_LOG((errmsg("call SnapRcvGetGlobalTransactionId\n")));
	MyProc->getGlobalTransaction = InvalidTransactionId;
	retry_time = 0;
	max_retry_time = 5;

RETRY:
	ProcessSnapRcvInterrupts();
	isSnapRcvStreamOk();
	LOCK_SNAP_GXID_RCV();
	if (SnapRcv->cur_pre_alloc > 0)
	{
		MyProc->getGlobalTransaction = SnapRcv->xid_alloc[SnapRcv->cur_pre_alloc - 1];
		SnapRcv->cur_pre_alloc--;
		Assert(TransactionIdIsValid(MyProc->getGlobalTransaction));

		SnapRcv->wait_xid_finish[SnapRcv->wait_finish_cnt++] = MyProc->getGlobalTransaction;
		UNLOCK_SNAP_GXID_RCV();

		if (SNAP_RCV_LATCH_VALID())
			SNAP_RCV_SET_LATCH();
		SNAP_SYNC_DEBUG_LOG((errmsg("Proce %d get xid %u from SnapRcv DIRECT\n",
				MyProc->pgprocno, MyProc->getGlobalTransaction)));
		return MyProc->getGlobalTransaction;
	}

	endtime = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), wait_loop_time * (retry_time + 1));
	SnapRcvWaitGxidEvent(endtime, WaitSnapRcvCondReturn, &SnapRcv->reters, &SnapRcv->geters, NULL, true);

	if (!TransactionIdIsValid(MyProc->getGlobalTransaction))
	{
		SnapRcvDeleteProcList(&SnapRcv->geters, MyProc->pgprocno);
		SnapRcvDeleteProcList(&SnapRcv->reters, MyProc->pgprocno);

		UNLOCK_SNAP_GXID_RCV();
		if (retry_time < max_retry_time)
		{
			retry_time++;
			ereport(WARNING,(errmsg("Cannot get xid from GTMCOORD, retry time %d\n", retry_time)));
			goto RETRY;
		}
		ereport(ERROR,
			(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			errmsg("Cannot get xid from GTMCOORD, after max retry time %d, please check GTMCOORD status\n", max_retry_time),
			errhint("you can modfiy guc parameter \"snapshot_sync_waittime\" on coordinators to request the global transaction id from gtmc")));
		ereport(ERROR,(errmsg("Cannot get xid from GTMCOORD, please check GTMCOORD status\n")));
	}
	else
		SnapRcv->wait_xid_finish[SnapRcv->wait_finish_cnt++] = MyProc->getGlobalTransaction;

	UNLOCK_SNAP_GXID_RCV();

	SNAP_SYNC_DEBUG_LOG((errmsg("Proce %d get xid %u from GxidRcv\n",
			MyProc->pgprocno, MyProc->getGlobalTransaction)));

	return MyProc->getGlobalTransaction;
}

void SnapRcvCommitTransactionId(TransactionId txid, SnapXidFinishOption finish_flag)
{
	TimestampTz				endtime;
	bool					ret;
	int						state;

	SNAP_SYNC_DEBUG_LOG((errmsg("Proce %d finish xid %u\n",
			MyProc->pgprocno, txid)));

	ProcessSnapRcvInterrupts();

	state = pg_atomic_read_u32(&SnapRcv->state);

	/* for stopped state and not rxact flag, we can ignore it directly*/
	if (state == WALRCV_STOPPED && (finish_flag & SNAP_XID_RXACT) == 0)
	{
		MyProc->getGlobalTransaction = InvalidTransactionId;
		ereport(WARNING, (errmsg("cannot connect to GTMCOORD, state is stooped. commit xid %d ignore", txid)));
		return;
	}

	isSnapRcvStreamOk();
	state = pg_atomic_read_u32(&SnapRcv->state);
	if (state != WALRCV_STREAMING)
	{
		if (finish_flag & SNAP_XID_RXACT)
			ereport(ERROR, (errmsg("state %d, rxact cannot connect to GTMCOORD, commit xid %d failed", state, txid)));
		else
		{
			MyProc->getGlobalTransaction = InvalidTransactionId;
			ereport(WARNING, (errmsg("cannot connect to GTMCOORD, state %d, commit xid %d ignore", state, txid)));
			return;
		}
	}

	LOCK_SNAP_GXID_RCV();
	ret = SnapRcvFoundWaitFinishList(txid);
	if (!ret && (finish_flag & SNAP_XID_RXACT) == 0)
	{
		UNLOCK_SNAP_GXID_RCV();
		MyProc->getGlobalTransaction = InvalidTransactionId;
		ereport(WARNING,(errmsg("xid %d is gone in snaprcv, maybe Snapsender/Snaprecv restart\n", txid)));
		return;
	}

	if (finish_flag & SNAP_XID_COMMIT)
	{
		UpdateAdbLastFinishXid(txid);
		pg_atomic_write_u32(&SnapRcv->global_finish_id, txid);
	}

	MyProc->getGlobalTransaction = txid;
	endtime = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), snapshot_sync_waittime);
	ret = SnapRcvWaitGxidEvent(endtime, WaitSnapRcvCommitReturn, &SnapRcv->wait_commiters,
				&SnapRcv->send_commiters, (void*)((size_t)txid), true);

	if (!ret)
	{
		SnapRcvRemoveWaitFinishList(txid, true);

		SnapRcvDeleteProcList(&SnapRcv->send_commiters, MyProc->pgprocno);
		SnapRcvDeleteProcList(&SnapRcv->wait_commiters, MyProc->pgprocno);
		UNLOCK_SNAP_GXID_RCV();
		MyProc->getGlobalTransaction = InvalidTransactionId;
		ereport(WARNING,(errmsg("SnapRcv wait xid timeout, which version is %d\n", txid)));
		return;
	}	

	UNLOCK_SNAP_GXID_RCV();
	MyProc->getGlobalTransaction = InvalidTransactionId;

	return;
}

static void SnapRcvConstructStatsBuf(TransactionId *xids, uint32	xids_len, StringInfo buf)
{
	int i = 0;
	qsort(xids, xids_len, sizeof(TransactionId), xidComparator);
	for (i = 0; i < xids_len; i++)
	{
		appendStringInfo(buf, "%u ", xids[i]);
		if (i > 0 && i % XID_PRINT_XID_LINE_NUM == 0)
			appendStringInfo(buf, "\n  ");
	}
	appendStringInfo(buf, "]\n");
}

void SnapRcvGetStat(StringInfo buf)
{
	int				i;
	TransactionId	*assign_xids = NULL;
	uint32			assign_len;
	TransactionId	last_finish_xid;

	TransactionId	*assign_preloc_xids = NULL;
	uint32			assign_preloc_len;
	TransactionId	*finish_gxid_xids = NULL;
	uint32			finish_gxid_len;

	assign_len = assign_preloc_len = finish_gxid_len = XID_ARRAY_STEP_SIZE;
	assign_xids = NULL;

re_lock_:
	if (!assign_xids)
		assign_xids = palloc0(sizeof(TransactionId) * assign_len);
	else
		assign_xids = repalloc(assign_xids, sizeof(TransactionId) * assign_len);
	
	if (!assign_preloc_xids)
		assign_preloc_xids = palloc0(sizeof(TransactionId) * assign_preloc_len);
	else
		assign_preloc_xids = repalloc(assign_preloc_xids, sizeof(TransactionId) * assign_preloc_len);

	if (!finish_gxid_xids)
		finish_gxid_xids = palloc0(sizeof(TransactionId) * finish_gxid_len);
	else
		finish_gxid_xids = repalloc(finish_gxid_xids, sizeof(TransactionId) * finish_gxid_len);
	LOCK_SNAP_RCV();

	if (assign_len <  SnapRcv->xcnt || assign_preloc_len < SnapRcv->cur_pre_alloc
		|| finish_gxid_len < SnapRcv->wait_finish_cnt)
	{
		UNLOCK_SNAP_RCV();
		assign_len += XID_ARRAY_STEP_SIZE;
		assign_preloc_len += XID_ARRAY_STEP_SIZE;
		finish_gxid_len += XID_ARRAY_STEP_SIZE;
		goto re_lock_;
	}

	assign_preloc_len = SnapRcv->cur_pre_alloc;
	for (i = 0; i < SnapRcv->cur_pre_alloc; i++)
	{
		assign_preloc_xids[i] = SnapRcv->xid_alloc[i];
	}

	finish_gxid_len = SnapRcv->wait_finish_cnt;
	for (i = 0; i < SnapRcv->wait_finish_cnt; i++)
	{
		finish_gxid_xids[i] = SnapRcv->wait_xid_finish[i];
	}

	assign_len = SnapRcv->xcnt;
	last_finish_xid = SnapRcv->latestCompletedXid;
	for (i = 0; i < SnapRcv->xcnt; i++)
	{
		assign_xids[i] = SnapRcv->xip[i];
	}
	UNLOCK_SNAP_RCV();

	appendStringInfo(buf, " status: %d \n", pg_atomic_read_u32(&SnapRcv->state));
	appendStringInfo(buf, "  latestCompletedXid: %d\n", last_finish_xid);

	appendStringInfo(buf, "  global_xmin: %u\n", pg_atomic_read_u32(&SnapRcv->global_xmin));
	appendStringInfo(buf, "  local global_xmin: %u\n", pg_atomic_read_u32(&SnapRcv->local_global_xmin));
	appendStringInfo(buf, "  local oldest_xmin: %u\n", GetOldestXminExt(NULL, PROCARRAY_FLAGS_VACUUM, true));
	appendStringInfo(buf, "  last_client_req_key: %u\n", pg_atomic_read_u32(&SnapRcv->last_client_req_key));
	appendStringInfo(buf, "  last_ss_req_key: %u\n", pg_atomic_read_u32(&SnapRcv->last_ss_req_key));
	appendStringInfo(buf, "  last_ss_resp_key: %u\n", pg_atomic_read_u32(&SnapRcv->last_ss_resp_key));

	appendStringInfo(buf, "  xcn: %u\n", assign_len);
	appendStringInfo(buf, "  xid_assign: [");
	SnapRcvConstructStatsBuf(assign_xids, assign_len, buf);

	appendStringInfo(buf, "  assign_preloc_len: %u\n", assign_preloc_len);
	appendStringInfo(buf, "  xid_preloc_assign: [");
	SnapRcvConstructStatsBuf(assign_preloc_xids, assign_preloc_len, buf);

	appendStringInfo(buf, "  finish_gxid_len: %u\n", finish_gxid_len);
	appendStringInfo(buf, "  xid_finish: [");
	SnapRcvConstructStatsBuf(finish_gxid_xids, finish_gxid_len, buf);

	pg_atomic_write_u64(&SnapRcv->last_heartbeat_sync_time, 0);
	if (SNAP_RCV_LATCH_VALID())
		SNAP_RCV_SET_LATCH();

	pfree(assign_xids);
}