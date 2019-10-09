#include "postgres.h"

#include "access/rmgr.h"
#include "access/xlogrecord.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/postmaster.h"
#include "replication/walreceiver.h"
#include "replication/gxidreceiver.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/proclist.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/resowner.h"
#include "pgxc/pgxc.h"

#define RESTART_STEP_MS		3000	/* 2 second */

#define MAX_XID_PRE_ASSGIN_NUM 50
/* like WalRcvImmediateInterruptOK */
static volatile bool GxidRcvImmediateInterruptOK = false;
int gxid_receiver_timeout = 60 * 1000L;
int max_pre_alloc_xid_size = MAX_XID_PRE_ASSGIN_NUM;

static bool is_use_prealloc_xids = true;

typedef struct GxidRcvData
{
	WalRcvState		state;
	pid_t			pid;
	int				procno;

	pg_time_t		startTime;

	char			sender_host[NI_MAXHOST];
	int				sender_port;

	proclist_head	geters;				/* list of getting gxid event */
	proclist_head	reters;				/* list of return gxid event */

	proclist_head	send_commiters;		/* list of commit gxid */
	proclist_head	wait_commiters;		/* list of commit gxid */

	slock_t			mutex;

	TimestampTz		next_try_time;	/* next connection GTM time */

	uint32			cur_pre_alloc;
	TransactionId	xid_alloc[MAX_XID_PRE_ASSGIN_NUM];

	uint32			wait_finish_cnt;
	TransactionId	wait_xid_finish[MAX_BACKENDS];

	uint32			is_send_realloc_num;  /* is need realloc from gc*/ 
}GxidRcvData;

/* item in  slist_client */
typedef struct GxiRcvAssginXidClientInfo
{
	slist_node		snode;
	int				procno;
	slist_head		slist_xid; 		/* xiditem list */
}GxiRcvAssginXidClientInfo;

/* item in  slist_client */
typedef struct GxiRcvAssginXidItemInfo
{
	slist_node		snode;
	TransactionId	xid;
}GxiRcvAssginXidItemInfo;

/* GUC variables */
extern char *AGtmHost;
extern char *PGXCNodeName;
extern int gxidsender_port;

/* libpqwalreceiver connection */
static WalReceiverConn *wrconn;
/* in transreceiver.c */
static StringInfoData reply_message;
static StringInfoData incoming_message;

/*
 * Flags set by interrupt handlers of walreceiver for later service in the
 * main loop.
 */
static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t got_SIGTERM = false;

static GxidRcvData *GxidRcv = NULL;
#define LOCK_GXID_RCV()			SpinLockAcquire(&GxidRcv->mutex)
#define UNLOCK_GXID_RCV()		SpinLockRelease(&GxidRcv->mutex)
#define GXID_RCV_SET_LATCH()	SetLatch(&(GetPGProcByNumber(GxidRcv->procno)->procLatch))
#define GXID_RCV_RESET_LATCH()	ResetLatch(&(GetPGProcByNumber(GxidRcv->procno)->procLatch))
#define GXID_RCV_LATCH_VALID()	(GxidRcv->procno != INVALID_PGPROCNO)

/* like WalRcvImmediateInterruptOK */
static volatile bool TransRcvImmediateInterruptOK = false;
static slist_head	slist_all_proc = SLIST_STATIC_INIT(slist_all_proc);

typedef bool (*WaitGxidRcvCond)(void *context, proclist_head *reters);

/* Prototypes for private functions */
static TimestampTz GxidRecvWaitUntilStartTime(void);
static void ProcessGxidRcvInterrupts(void);
static void EnableGxidRcvImmediateExit(void);
static void DisableGxidRcvImmediateExit(void);
static void GxidRcvDie(int code, Datum arg);
static void GxidRcvConnectTransSender(void);
static void GxidRcvUpdateShmemConnInfo(void);
static void GxidRcvProcessMessage(unsigned char type, char *buf, Size len);
static void GxidRcvMainProcess(void);
static void GxidRcvProcessAssignList(void);
static void GxidRcvProcessFinishList(void);
static void GxidRcvCheckPreAssignArray(void);
static void GxidRcvProcessEmptyProcList(void);
static bool WaitGxidRcvEvent(TimestampTz end, WaitGxidRcvCond test,
			proclist_head *reters, proclist_head *geters, void *context);
static void GxidRcvSendHeartbeat(void);
static void GxidRcvSendLocalNextXid(void);
static void GxidRcvSendPreAssginXid(int xid_num);
static void GxidRcvProcessPreAssign(char *buf, Size len);
static void GxidRcvProcessAssign(char *buf, Size len);
static void GxidRcvClaerAllClientInfo(void);
static bool GxidRcvFoundWaitFinishList(TransactionId xid);
static void GxidRcvRemoveWaitFinishList(TransactionId xid, bool is_miss_ok);
static void GxidRcvDeleteProcList(proclist_head *reters, int procno);

/* Signal handlers */
static void GxidRcvSigHupHandler(SIGNAL_ARGS);
static void GxidRcvSigUsr1Handler(SIGNAL_ARGS);
static void GxidRcvShutdownHandler(SIGNAL_ARGS);
static void GxidRcvQuickDieHandler(SIGNAL_ARGS);

typedef bool (*WaitTransRcvCond)(void *context, proclist_head *reters);
static bool WaitGxidRcvCondReturn(void *context, proclist_head *reters);
static bool WaitGxidRcvCommitReturn(void *context, proclist_head *wait_commiters);

static void
ProcessGxidRcvInterrupts(void)
{
	/* like ProcessTranslRcvInterrupts */
	CHECK_FOR_INTERRUPTS();

	if (got_SIGTERM)
	{
		TransRcvImmediateInterruptOK = false;
		ereport(FATAL,
				(errcode(ERRCODE_ADMIN_SHUTDOWN),
				 errmsg("terminating transreceiver process due to administrator command")));
	}
}

static void
EnableGxidRcvImmediateExit(void)
{
	TransRcvImmediateInterruptOK = true;
	ProcessGxidRcvInterrupts();
}

static void
DisableGxidRcvImmediateExit(void)
{
	TransRcvImmediateInterruptOK = false;
	ProcessGxidRcvInterrupts();
}

static void
GxidRcvSendHeartbeat(void)
{
	/* Construct a new message */
	resetStringInfo(&reply_message);
	pq_sendbyte(&reply_message, 'h');

	/* Send it */
	walrcv_send(wrconn, reply_message.data, reply_message.len);
}

static void
GxidRcvSendPreAssginXid(int xid_num)
{
	if (!IS_PGXC_COORDINATOR)
		return;

	/* Construct a new message */
	resetStringInfo(&reply_message);
	pq_sendbyte(&reply_message, 'p');
	pq_sendstring(&reply_message, PGXCNodeName);
	pq_sendint32(&reply_message, xid_num);

	/* Send it */
	walrcv_send(wrconn, reply_message.data, reply_message.len);
}

void GxidReceiverMain(void)
{
	TimestampTz now;
	TimestampTz last_recv_timestamp;
	TimestampTz timeout;
	bool		heartbeat_sent;

	Assert(GxidRcv != NULL);

	now = GetCurrentTimestamp();

	/*
	 * Mark snapreceiver as running in shared memory.
	 *
	 * Do this as early as possible, so that if we fail later on, we'll set
	 * state to STOPPED. If we die before this, the startup process will keep
	 * waiting for us to start up, until it times out.
	 */
	LOCK_GXID_RCV();
	Assert(GxidRcv->pid == 0);
	switch (GxidRcv->state)
	{
		case WALRCV_STOPPING:
			/* If we've already been requested to stop, don't start up. */
			GxidRcv->state = WALRCV_STOPPED;
			UNLOCK_GXID_RCV();
			proc_exit(1);
			break;

		case WALRCV_STOPPED:
			GxidRcv->state = WALRCV_STARTING;
			/* fall through, do not add break */
		case WALRCV_STARTING:
			/* The usual case */
			break;

		case WALRCV_WAITING:
		case WALRCV_STREAMING:
		case WALRCV_RESTARTING:
		default:
			/* Shouldn't happen */
			UNLOCK_GXID_RCV();
			elog(PANIC, "snapreceiver still running according to shared memory state");
	}
	/* Advertise our PID so that the startup process can kill us */
	GxidRcv->pid = MyProcPid;
	GxidRcv->procno = MyProc->pgprocno;

	UNLOCK_GXID_RCV();

	/* Arrange to clean up at walreceiver exit */
	on_shmem_exit(GxidRcvDie, (Datum)0);

	now = GxidRecvWaitUntilStartTime();

	/* Properly accept or ignore signals the postmaster might send us */
	pqsignal(SIGHUP, GxidRcvSigHupHandler);	/* set flag to read config file */
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, GxidRcvShutdownHandler);	/* request shutdown */
	pqsignal(SIGQUIT, GxidRcvQuickDieHandler);	/* hard crash time */
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, GxidRcvSigUsr1Handler);
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
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "Gxid Receiver");

	initStringInfo(&reply_message);
	initStringInfo(&incoming_message);

	/* Unblock signals (they were blocked when the postmaster forked us) */
	PG_SETMASK(&UnBlockSig);

	EnableGxidRcvImmediateExit();
	GxidRcvConnectTransSender();
	DisableGxidRcvImmediateExit();

	GxidRcvUpdateShmemConnInfo();

	/* Initialize the last recv timestamp */
	last_recv_timestamp = GetCurrentTimestamp();
	
	for (;;)
	{
		WalRcvStreamOptions options;

		/*
		 * Check that we're connected to a valid server using the
		 * IDENTIFY_SYSTEM replication command.
		 */
		EnableGxidRcvImmediateExit();

		/* options startpoint must be InvalidXLogRecPtr and timeline be 0 */
		options.logical = false;
		options.startpoint = InvalidXLogRecPtr;
		options.slotname = PGXCNodeName;
		options.proto.physical.startpointTLI = 0;

		if (walrcv_startstreaming(wrconn, &options))
		{
			//walrcv_endstreaming(wrconn, &primaryTLI);
			/* loop until end-of-streaming or error */
			GxidRcvSendLocalNextXid();
			GxidRcvClaerAllClientInfo();
			GxidRcv->cur_pre_alloc = 0;
			GxidRcv->wait_finish_cnt = 0;
			GxidRcv->is_send_realloc_num = 0;
			GxidRcvCheckPreAssignArray();
			heartbeat_sent = true;
			for(;;)
			{
				char	   *buf;
				int			len;
				pgsocket	wait_fd = PGINVALID_SOCKET;
				int			rc;
				bool		endofwal = false;

				ProcessGxidRcvInterrupts();
				if (got_SIGHUP)
				{
					got_SIGHUP = false;
					ProcessConfigFile(PGC_SIGHUP);
				}

				len = walrcv_receive(wrconn, &buf, &wait_fd);
				if (len != 0)
				{
					for (;;)
					{
						if (len > 0)
						{
							last_recv_timestamp = GetCurrentTimestamp();
							heartbeat_sent = false;
							GxidRcvProcessMessage(buf[0], &buf[1], len-1);
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
									   gxid_receiver_timeout,
									   PG_WAIT_EXTENSION);

				ResetLatch(&MyProc->procLatch);
				GxidRcvMainProcess();
				
				if (rc & WL_POSTMASTER_DEATH)
				{
					/*
					 * Emergency bailout if postmaster has died.  This is to
					 * avoid the necessity for manual cleanup of all
					 * postmaster children.
					 */
					exit(1);
				}

				if ((rc & WL_TIMEOUT) && gxid_receiver_timeout > 0 && !heartbeat_sent)
				{
					now = GetCurrentTimestamp();
					timeout = TimestampTzPlusMilliseconds(last_recv_timestamp,
								gxid_receiver_timeout);

					if (now >= timeout)
					{
						heartbeat_sent = true;
						GxidRcvSendHeartbeat();
					}
				}
			}
		}else
		{
			ereport(LOG,
					(errmsg("primary server not start send gxid")));
		}
	}

	proc_exit(0);
}

Size GxidRcvShmemSize(void)
{
	return sizeof(GxidRcvData);
}

void GxidRcvShmemInit(void)
{
	bool		found;

	GxidRcv = (GxidRcvData*)
		ShmemInitStruct("Gxid Receiver", GxidRcvShmemSize(), &found);

	if (!found)
	{
		/* First time through, so initialize */
		MemSet(GxidRcv, 0, GxidRcvShmemSize());
		GxidRcv->state = WALRCV_STOPPED;
		proclist_init(&GxidRcv->geters);
		proclist_init(&GxidRcv->reters);
		proclist_init(&GxidRcv->send_commiters);
		proclist_init(&GxidRcv->wait_commiters);
		GxidRcv->procno = INVALID_PGPROCNO;
		GxidRcv->cur_pre_alloc = 0;
		GxidRcv->wait_finish_cnt = 0;
		GxidRcv->is_send_realloc_num = 0;
		SpinLockInit(&GxidRcv->mutex);
	}
}

static TimestampTz GxidRecvWaitUntilStartTime(void)
{
	TimestampTz end;
	TimestampTz now;
	TimestampTz max_end;
	int rc;

	LOCK_GXID_RCV();
	end = GxidRcv->next_try_time;
	UNLOCK_GXID_RCV();

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

static void GxidRcvDie(int code, Datum arg)
{
	/* Mark ourselves inactive in shared memory */
	LOCK_GXID_RCV();
	Assert(GxidRcv->state == WALRCV_STREAMING ||
		   GxidRcv->state == WALRCV_RESTARTING ||
		   GxidRcv->state == WALRCV_STARTING ||
		   GxidRcv->state == WALRCV_WAITING ||
		   GxidRcv->state == WALRCV_STOPPING);
	Assert(GxidRcv->pid == MyProcPid);
	GxidRcv->state = WALRCV_STOPPED;
	GxidRcv->pid = 0;
	GxidRcv->procno = INVALID_PGPROCNO;
	//GxidRcv->xcnt = 0;
	GxidRcv->next_try_time = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), RESTART_STEP_MS);	/* 3 seconds */
	UNLOCK_GXID_RCV();

	/* Terminate the connection gracefully. */
	if (wrconn != NULL)
		walrcv_disconnect(wrconn);
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void
GxidRcvSigHupHandler(SIGNAL_ARGS)
{
	got_SIGHUP = true;
}


/* SIGUSR1: used by latch mechanism */
static void
GxidRcvSigUsr1Handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	latch_sigusr1_handler();

	errno = save_errno;
}

/* SIGTERM: set flag for main loop, or shutdown immediately if safe */
static void
GxidRcvShutdownHandler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGTERM = true;

	if (GXID_RCV_LATCH_VALID())
		GXID_RCV_SET_LATCH();

	/* Don't joggle the elbow of proc_exit */
	if (!proc_exit_inprogress && GxidRcvImmediateInterruptOK)
		ProcessGxidRcvInterrupts();

	errno = save_errno;
}

/*
 * WalRcvQuickDieHandler() occurs when signalled SIGQUIT by the postmaster.
 *
 * Some backend has bought the farm, so we need to stop what we're doing and
 * exit.
 */
static void
GxidRcvQuickDieHandler(SIGNAL_ARGS)
{
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

static void GxidRcvConnectTransSender(void)
{
	char conninfo[MAXCONNINFO];
	char *errstr;

	Assert(wrconn == NULL);

	snprintf(conninfo, MAXCONNINFO,
			 "user=postgres host=%s port=%d",
			 AGtmHost, gxidsender_port);
	wrconn = walrcv_connect(conninfo, false, "gxidreceiver", &errstr);
	if (!wrconn)
		ereport(ERROR,
				(errmsg("could not connect to the Gxid server: %s", errstr)));
}

void GxidRcvUpdateShmemConnInfo(void)
{
	char *sender_host;
	int sender_port;

	walrcv_get_senderinfo(wrconn, &sender_host, &sender_port);

	LOCK_GXID_RCV();

	memset(GxidRcv->sender_host, 0, NI_MAXHOST);
	if (sender_host)
		strlcpy(GxidRcv->sender_host, sender_host, NI_MAXHOST);

	GxidRcv->sender_port = sender_port;

	UNLOCK_GXID_RCV();

	if (sender_host)
		pfree(sender_host);
}

static void GxidRcvFinishLocalXid(TransactionId	txid, int procno)
{
	slist_mutable_iter			siter;
	bool						found_proc;
	GxiRcvAssginXidItemInfo		*xidinfo;
	GxiRcvAssginXidClientInfo	*client;

	found_proc = false;
	slist_foreach_modify(siter, &slist_all_proc)
	{
		client = slist_container(GxiRcvAssginXidClientInfo, snode, siter.cur);
		if (client->procno == procno)
		{
			found_proc = true;
			break;
		}
	}

	if (!found_proc)
		return;

	slist_foreach_modify(siter, &client->slist_xid)
	{
		xidinfo = slist_container(GxiRcvAssginXidItemInfo, snode, siter.cur);
		if (xidinfo->xid == txid)
		{
			slist_delete(&client->slist_xid, &xidinfo->snode);
			break;
		}
	}
}

static void GxidRcvSendLocalNextXid(void)
{
	TransactionId xid; 
	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	xid = ShmemVariableCache->nextXid;
	LWLockRelease(XidGenLock);

	if (!TransactionIdIsValid(xid))
	{
		GxidRcvSendHeartbeat();
		return;
	}

	/* Construct a new message */
	resetStringInfo(&reply_message);
	pq_sendbyte(&reply_message, 'u');
	pq_sendstring(&reply_message, PGXCNodeName);
	pq_sendint64(&reply_message, xid);

	/* Send it */
	walrcv_send(wrconn, reply_message.data, reply_message.len);
}

static void GxidRcvClaerAllClientInfo(void)
{
	slist_mutable_iter			siter;
	slist_mutable_iter			siter_xid;
	GxiRcvAssginXidItemInfo		*xidinfo;
	GxiRcvAssginXidClientInfo	*client;

	LOCK_GXID_RCV();
	slist_foreach_modify(siter, &slist_all_proc)
	{
		client = slist_container(GxiRcvAssginXidClientInfo, snode, siter.cur);
		slist_foreach_modify(siter_xid, &client->slist_xid)
		{
			xidinfo = slist_container(GxiRcvAssginXidItemInfo, snode, siter_xid.cur);
			slist_delete(&client->slist_xid, &xidinfo->snode);
			pfree(xidinfo);
		}
		slist_delete(&slist_all_proc, &client->snode);
		pfree(client);
	}
	UNLOCK_GXID_RCV();
}

static void GxidRcvProcessCommit(char *buf, Size len)
{
	StringInfoData				msg;
	TransactionId				txid;
	int							procno;
	PGPROC						*proc;				
	proclist_mutable_iter		iter;
	bool						found;

	msg.data = buf;
	msg.len = msg.maxlen = len;
	msg.cursor = 0;
	
	LOCK_GXID_RCV();
	while(msg.cursor < msg.len)
	{
		procno = pq_getmsgint(&msg, sizeof(procno));
		txid = pq_getmsgint(&msg, sizeof(txid));

#ifdef SNAP_SYNC_DEBUG
		ereport(LOG,(errmsg("GxidRcv  rcv finish xid %d for %d\n", txid, procno)));
#endif
		GxidRcvFinishLocalXid(txid, procno);
		GxidRcvRemoveWaitFinishList(txid, false);
		Assert(TransactionIdIsValid(txid));

		found = false;
		proclist_foreach_modify(iter, &GxidRcv->wait_commiters, GxidWaitLink)
		{
			proc = GetPGProcByNumber(iter.cur);
			if (proc->pgprocno == procno && proc->getGlobalTransaction == txid)
			{
				proc->getGlobalTransaction = InvalidTransactionId;
				SetLatch(&proc->procLatch);
				found = true;
				break;
			}
		}
		Assert(found);
	}
	UNLOCK_GXID_RCV();
}

static void GxidRcvProcessUpdateXid(char *buf, Size len)
{
	StringInfoData			msg;
	TransactionId			txid;

	msg.data = buf;

	msg.len = msg.maxlen = len;
	msg.cursor = 0;

	txid = pq_getmsgint(&msg, sizeof(txid));

	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	ereport(DEBUG2, (errmsg("GxidRcvProcessUpdateXid  %d, ShmemVariableCache->nextXid is %d\n", txid, ShmemVariableCache->nextXid)));
	if (!NormalTransactionIdPrecedes(txid, ShmemVariableCache->nextXid))
	{
 		ShmemVariableCache->nextXid = txid;
 		TransactionIdAdvance(ShmemVariableCache->nextXid);

		ShmemVariableCache->latestCompletedXid = ShmemVariableCache->nextXid;
		TransactionIdRetreat(ShmemVariableCache->latestCompletedXid);
	}
	LWLockRelease(XidGenLock);
}

static void GxidRcvProcessAssign(char *buf, Size len)
{
	StringInfoData			msg;
	TransactionId			txid;
	int						procno;
	PGPROC					*proc;				
	proclist_mutable_iter	iter;
	bool					found;

	msg.data = buf;

	msg.len = msg.maxlen = len;
	msg.cursor = 0;

	LOCK_GXID_RCV();
	while(msg.cursor < msg.len)
	{
		procno = pq_getmsgint(&msg, sizeof(procno));
		txid = pq_getmsgint(&msg, sizeof(txid));

		Assert(TransactionIdIsValid(txid));
#ifdef SNAP_SYNC_DEBUG
		ereport(LOG,(errmsg("GxidRcv  rcv assing xid %d for %d\n", txid, procno)));
#endif

		found = false;
		proclist_foreach_modify(iter, &GxidRcv->reters, GxidWaitLink)
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
		Assert(found);
	}
	UNLOCK_GXID_RCV();
}

static void GxidRcvProcessPreAssign(char *buf, Size len)
{
	StringInfoData			msg;
	TransactionId			txid;
	int						num, start_index;
			
	msg.data = buf;
	msg.len = msg.maxlen = len;
	msg.cursor = 0;

#ifdef SNAP_SYNC_DEBUG
	ereport(LOG,(errmsg("GxidRcv rcv pre assing: ")));
#endif

	num = pq_getmsgint(&msg, sizeof(num));
	Assert(num > 0 && num <= max_pre_alloc_xid_size);
	
	LOCK_GXID_RCV();
	Assert((GxidRcv->cur_pre_alloc + num) <= max_pre_alloc_xid_size);
	start_index = GxidRcv->cur_pre_alloc;
	while(msg.cursor < msg.len)
	{
		txid = pq_getmsgint(&msg, sizeof(txid));

		Assert(TransactionIdIsValid(txid));

#ifdef SNAP_SYNC_DEBUG
		ereport(LOG,(errmsg(" %d\n", txid)));
#endif
		num--;
		GxidRcv->xid_alloc[start_index + num] = txid;
		GxidRcv->cur_pre_alloc++;
	}
	GxidRcv->is_send_realloc_num = 0;
	Assert(GxidRcv->cur_pre_alloc <= max_pre_alloc_xid_size);
	UNLOCK_GXID_RCV();

	Assert(num == 0);	
}

static void GxidRcvProcessMessage(unsigned char type, char *buf, Size len)
{
	resetStringInfo(&incoming_message);

	switch (type)
	{
	case 's':
		LOCK_GXID_RCV();
		if (GxidRcv->state == WALRCV_STARTING)
			GxidRcv->state = WALRCV_STREAMING;
		UNLOCK_GXID_RCV();
		break;
	case 'q':
		GxidRcvProcessPreAssign(buf, len);
		break;
	case 'a':
		GxidRcvProcessAssign(buf, len);
		break;
	case 'f':
		GxidRcvProcessCommit(buf, len);
		break;
	case 'u':
		GxidRcvProcessUpdateXid(buf, len);
		break;
	case 'h':				/* heart beat msg */
		break;
	default:
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg_internal("invalid replication message type %d",
								 type)));
	}
}

static bool WaitGxidRcvCondReturn(void *context, proclist_head *reters)
{
	proclist_mutable_iter	iter;
	PGPROC					*proc;	
	int						procno = MyProc->pgprocno;
	/* not in streaming, wait */
	if (GxidRcv->state != WALRCV_STREAMING)
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

static bool WaitGxidRcvCommitReturn(void *context, proclist_head *wait_commiters)
{
	proclist_mutable_iter	iter;
	PGPROC					*proc;
	int						procno = MyProc->pgprocno;

	/* not in streaming, wait */
	if (GxidRcv->state != WALRCV_STREAMING)
		return true;

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

/*
 * when end < 0 wait until streaming or error
 *   when end == 0 not block
 * mutex must be locked
 */
static bool WaitGxidRcvEvent(TimestampTz end, WaitGxidRcvCond test,
			proclist_head *reters, proclist_head *geters, void *context)
{
	Latch				   *latch = &MyProc->procLatch;
	long					timeout;
	proclist_mutable_iter	iter;
	int						procno = MyProc->pgprocno;
	int						rc;
	int						waitEvent;

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

		if (!in_ret_list)
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

		GXID_RCV_SET_LATCH();
		UNLOCK_GXID_RCV();

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
			LOCK_GXID_RCV();
			return false;
		}

		LOCK_GXID_RCV();
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

	return true;
}

static void
GxidRcvProcessAssignList(void)
{
	proclist_mutable_iter	iter_gets;
	proclist_mutable_iter	iter_rets;

	LOCK_GXID_RCV();
	if (proclist_is_empty(&GxidRcv->geters))
	{
		UNLOCK_GXID_RCV();
		return;
	}

	resetStringInfo(&reply_message);
	pq_sendbyte(&reply_message, 'g');
	pq_sendstring(&reply_message, PGXCNodeName);

	proclist_foreach_modify(iter_gets, &GxidRcv->geters, GxidWaitLink)
	{
		pq_sendint32(&reply_message, iter_gets.cur);
#ifdef SNAP_SYNC_DEBUG
		ereport(LOG,(errmsg("GxidRcv assing xid for %d\n",
			 iter_gets.cur)));
#endif

		proclist_delete(&GxidRcv->geters, iter_gets.cur, GxidWaitLink);

		bool in_list = false;
		proclist_foreach_modify(iter_rets, &GxidRcv->reters, GxidWaitLink)
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
			proclist_push_tail(&GxidRcv->reters, iter_gets.cur, GxidWaitLink);
		}
	}
	UNLOCK_GXID_RCV();

	/* Send it */
	walrcv_send(wrconn, reply_message.data, reply_message.len);
}

static void GxidRcvMainProcess(void)
{
	GxidRcvCheckPreAssignArray();
	GxidRcvProcessFinishList();
	GxidRcvProcessAssignList();
	GxidRcvProcessEmptyProcList();
}

static void
GxidRcvProcessFinishList(void)
{
	proclist_mutable_iter	iter_gets;
	proclist_mutable_iter	iter_rets;
	PGPROC					*proc;

	LOCK_GXID_RCV();
	if (proclist_is_empty(&GxidRcv->send_commiters))
	{
		UNLOCK_GXID_RCV();
		return;
	}
	resetStringInfo(&reply_message);
	pq_sendbyte(&reply_message, 'c');
	pq_sendstring(&reply_message, PGXCNodeName);

	proclist_foreach_modify(iter_gets, &GxidRcv->send_commiters, GxidWaitLink)
	{
		proc = GetPGProcByNumber(iter_gets.cur);
		pq_sendint32(&reply_message, proc->pgprocno);
		pq_sendint32(&reply_message, proc->getGlobalTransaction);
		proclist_delete(&GxidRcv->send_commiters, iter_gets.cur, GxidWaitLink);

#ifdef SNAP_SYNC_DEBUG
		ereport(LOG,(errmsg("GxidRcv send finish xid %d for %d\n",
			 proc->getGlobalTransaction,
			 proc->pgprocno)));
#endif

		bool in_list = false;
		proclist_foreach_modify(iter_rets, &GxidRcv->wait_commiters, GxidWaitLink)
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
			proclist_push_tail(&GxidRcv->wait_commiters, iter_gets.cur, GxidWaitLink);
		}
	}
	UNLOCK_GXID_RCV();

	/* Send it */
	walrcv_send(wrconn, reply_message.data, reply_message.len);
}

static void GxidRcvCheckPreAssignArray(void)
{
	int req_num;

	if (!IS_PGXC_COORDINATOR || !is_use_prealloc_xids)
		return;

	LOCK_GXID_RCV();
	if (GxidRcv->cur_pre_alloc <= (max_pre_alloc_xid_size/2) && GxidRcv->is_send_realloc_num == 0)
	{
		if (GxidRcv->cur_pre_alloc == 0)
			req_num = max_pre_alloc_xid_size;
		else
			req_num = max_pre_alloc_xid_size/2;

		GxidRcvSendPreAssginXid(req_num);
		GxidRcv->is_send_realloc_num = 1;
	}
	UNLOCK_GXID_RCV();
}

static void GxidRcvProcessEmptyProcList(void)
{			
	slist_mutable_iter			siter;
	GxiRcvAssginXidClientInfo	*client;

	LOCK_GXID_RCV();
	slist_foreach_modify(siter, &slist_all_proc)
	{
		client = slist_container(GxiRcvAssginXidClientInfo, snode, siter.cur);
		if (slist_is_empty(&client->slist_xid))
		{
			slist_delete(&slist_all_proc, &client->snode);
			pfree(client);
		}
	}
	UNLOCK_GXID_RCV();
}

/* must has get the gxidrcv lock */
static void GxidRcvRemoveWaitFinishList(TransactionId xid, bool is_miss_ok)
{
	int i, count;
	bool found;

	found = false;
	count = GxidRcv->wait_finish_cnt;

	Assert(count > 0);
	for (i = 0; i < count; i++)
	{
		if (GxidRcv->wait_xid_finish[i] == xid)
		{
#ifdef SNAP_SYNC_DEBUG
			ereport(LOG,(errmsg("Remove finish wait xid %d from wait_xid_finish\n", xid)));
#endif
			found = true;
			memmove(&GxidRcv->wait_xid_finish[i],
						&GxidRcv->wait_xid_finish[i+1],
						(count-i-1) * sizeof(xid));
			GxidRcv->wait_finish_cnt--;
			break;
		}
	}

	if (!is_miss_ok)
		Assert(found);
}

/* must has get the gxidrcv lock */
static bool GxidRcvFoundWaitFinishList(TransactionId xid)
{
	int i, count;
	bool found;

	found = false;
	count = GxidRcv->wait_finish_cnt;

	for (i = 0; i < count; i++)
	{
		if (GxidRcv->wait_xid_finish[i] == xid)
		{
			found = true;
			break;
		}
	}

	return found;
}

static void GxidRcvDeleteProcList(proclist_head *reters, int procno)
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

TransactionId GixRcvGetGlobalTransactionId(bool isSubXact)
{
	TimestampTz				endtime;

	if(isSubXact)
		ereport(ERROR, (errmsg("cannot assign XIDs in child transaction")));

	MyProc->getGlobalTransaction = InvalidTransactionId;
	LOCK_GXID_RCV();

	if (GxidRcv->state != WALRCV_STREAMING)
	{
		UNLOCK_GXID_RCV();
		ereport(ERROR, (errmsg("cannot connect to GTMCOORD")));
	}

	if (GxidRcv->cur_pre_alloc > 0)
	{
		MyProc->getGlobalTransaction = GxidRcv->xid_alloc[GxidRcv->cur_pre_alloc - 1];
		GxidRcv->cur_pre_alloc--;
		Assert(TransactionIdIsValid(MyProc->getGlobalTransaction));

		GxidRcv->wait_xid_finish[GxidRcv->wait_finish_cnt++] = MyProc->getGlobalTransaction;
		UNLOCK_GXID_RCV();

		GXID_RCV_SET_LATCH();
#ifdef SNAP_SYNC_DEBUG
		ereport(LOG,(errmsg("Proce %d get xid %d from GxidRcv DIRECT\n",
				MyProc->pgprocno, MyProc->getGlobalTransaction)));
#endif
		return MyProc->getGlobalTransaction;
	}

	endtime = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), 1000);
	WaitGxidRcvEvent(endtime, WaitGxidRcvCondReturn, &GxidRcv->reters, &GxidRcv->geters, NULL);

	if (!TransactionIdIsValid(MyProc->getGlobalTransaction))
	{
		GxidRcvDeleteProcList(&GxidRcv->geters, MyProc->pgprocno);
		GxidRcvDeleteProcList(&GxidRcv->reters, MyProc->pgprocno);

		UNLOCK_GXID_RCV();
		ereport(ERROR,(errmsg("Cannot get xid from GTMCOORD, please check GTMCOORD status\n")));
	}
	else
		GxidRcv->wait_xid_finish[GxidRcv->wait_finish_cnt++] = MyProc->getGlobalTransaction;

	UNLOCK_GXID_RCV();

#ifdef SNAP_SYNC_DEBUG
	ereport(LOG,(errmsg("Proce %d get xid %d from GxidRcv\n",
			MyProc->pgprocno, MyProc->getGlobalTransaction)));
#endif

	return MyProc->getGlobalTransaction;
}

void GixRcvCommitTransactionId(TransactionId txid)
{
	TimestampTz				endtime;
	bool					ret;

#ifdef SNAP_SYNC_DEBUG
	ereport(LOG,(errmsg("Proce %d finish xid %d\n",
			MyProc->pgprocno, MyProc->getGlobalTransaction)));
#endif

	LOCK_GXID_RCV();

	if (GxidRcv->state != WALRCV_STREAMING)
	{
		UNLOCK_GXID_RCV();
		MyProc->getGlobalTransaction = InvalidTransactionId;
		ereport(WARNING, (errmsg("cannot connect to GTMCOORD, commit xid %d ignore", txid)));
		return;
	}

	ret = GxidRcvFoundWaitFinishList(txid);
	if (!ret)
	{
		UNLOCK_GXID_RCV();
		MyProc->getGlobalTransaction = InvalidTransactionId;
		ereport(WARNING,(errmsg("xid %d is gone, maybe gtmcoord restart\n", txid)));
		return;
	}

	MyProc->getGlobalTransaction = txid;
	endtime = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), 1000);
	ret = WaitGxidRcvEvent(endtime, WaitGxidRcvCommitReturn, &GxidRcv->wait_commiters,
				&GxidRcv->send_commiters, (void*)((size_t)txid));

	if (!ret)
	{
		GxidRcvRemoveWaitFinishList(txid, true);

		GxidRcvDeleteProcList(&GxidRcv->send_commiters, MyProc->pgprocno);
		GxidRcvDeleteProcList(&GxidRcv->wait_commiters, MyProc->pgprocno);
		UNLOCK_GXID_RCV();
		MyProc->getGlobalTransaction = InvalidTransactionId;
		ereport(WARNING,(errmsg("GxidRcv wait xid timeout, which version is %d\n", txid)));
		return;
	}	

	UNLOCK_GXID_RCV();
	MyProc->getGlobalTransaction = InvalidTransactionId;

	return;
}