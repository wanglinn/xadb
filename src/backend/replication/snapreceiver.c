#include "postgres.h"

#include "access/rmgr.h"
#include "access/xact.h"
#include "access/xlogrecord.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/postmaster.h"
#include "replication/walreceiver.h"
#include "replication/snapreceiver.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/proclist.h"
#include "storage/shmem.h"
#include "utils/dsa.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/tqual.h"

#define RESTART_STEP_MS		3000	/* 2 second */

int snap_receiver_timeout = 60 * 1000L;
int snap_sender_connect_timeout = 5000L;

typedef struct SnapHoldLock
{
	LOCKTAG		tag;
	LOCKMASK	holdMask;
}SnapHoldLock;

typedef struct SnapLockInfo
{
	dsa_pointer		next;
	TransactionId	xid;
	uint32			count;
	SnapHoldLock	locks[FLEXIBLE_ARRAY_MEMBER];
}SnapLockInfo;
#define SNAP_LOCK_INFO_SIZE(count)	(offsetof(SnapLockInfo,locks)+sizeof(SnapHoldLock)*(count))

typedef struct SnapRcvData
{
	WalRcvState		state;
	pid_t			pid;
	int				procno;

	pg_time_t		startTime;

	char			sender_host[NI_MAXHOST];
	int				sender_port;

	proclist_head	waiters;	/* list of waiting event */

	slock_t			mutex;

	TimestampTz		next_try_time;	/* next connection GTM time */
	TimestampTz		gtm_delta_time;

	dsa_pointer		first_lock_info;
	dsa_pointer		last_lock_info;
	dsa_handle		handle_lock_info;
	LWLock			lock_lock_info;
	LWLock			lock_proc_link;

	uint32			xcnt;
	TransactionId	latestCompletedXid;
	TransactionId	xip[MAX_BACKENDS];
}SnapRcvData;

/* GUC variables */
extern char *AGtmHost;
extern int	AGtmPort;

/* libpqwalreceiver connection */
static WalReceiverConn *wrconn;
/* in walreceiver.c */
static StringInfoData reply_message;
static StringInfoData incoming_message;

static TimestampTz last_heat_beat_sendtime;

static bool finish_xid_ack_send = false;

/*
 * Flags set by interrupt handlers of walreceiver for later service in the
 * main loop.
 */
static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t got_SIGTERM = false;

static SnapRcvData *SnapRcv = NULL;
#define LOCK_SNAP_RCV()			SpinLockAcquire(&SnapRcv->mutex)
#define UNLOCK_SNAP_RCV()		SpinLockRelease(&SnapRcv->mutex)
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
static void WakeupTransaction(TransactionId);
static void SnapRcvSendLocalNextXid(void);

/* Signal handlers */
static void SnapRcvSigHupHandler(SIGNAL_ARGS);
static void SnapRcvSigUsr1Handler(SIGNAL_ARGS);
static void SnapRcvShutdownHandler(SIGNAL_ARGS);
static void SnapRcvQuickDieHandler(SIGNAL_ARGS);

typedef bool (*WaitSnapRcvCond)(void *context);
static bool WaitSnapRcvCondStreaming(void *context);
static bool WaitSnapRcvCondTransactionComplate(void *context);
static bool WaitSnapRcvEvent(TimestampTz end, WaitSnapRcvCond test, void *context);

static dsa_area* SnapRcvGetLockArea(void);
static void SnapRcvReleaseTransactionLocks(TransactionId xid);
static void SnapRcvReleaseSnapshotTxidLocks(TransactionId *xip, uint32 count, TransactionId lastxid);

static void
ProcessSnapRcvInterrupts(void)
{
	/* like ProcessWalRcvInterrupts */
	CHECK_FOR_INTERRUPTS();

	if (got_SIGTERM)
	{
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
	last_heat_beat_sendtime = GetCurrentTimestamp();
	/* Construct a new message */
	resetStringInfo(&reply_message);
	pq_sendbyte(&reply_message, 'h');
	pq_sendint64(&reply_message, last_heat_beat_sendtime);

	/* Send it */
	walrcv_send(wrconn, reply_message.data, reply_message.len);
}

static void
SnapRcvSendLocalNextXid(void)
{
	TransactionId xid; 
	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	xid = ShmemVariableCache->nextXid;
	LWLockRelease(XidGenLock);

	if (!TransactionIdIsValid(xid))
	{
		SnapRcvSendHeartbeat();
		return;
	}

	/* Construct a new message */
	resetStringInfo(&reply_message);
	pq_sendbyte(&reply_message, 'u');
	pq_sendint64(&reply_message, xid);

	/* Send it */
	walrcv_send(wrconn, reply_message.data, reply_message.len);
}

void SnapReceiverMain(void)
{
	TimestampTz now;
	TimestampTz last_recv_timestamp;
	TimestampTz timeout;
	bool		heartbeat_sent;

	Assert(SnapRcv != NULL);

	now = GetCurrentTimestamp();

	/*
	 * Mark snapreceiver as running in shared memory.
	 *
	 * Do this as early as possible, so that if we fail later on, we'll set
	 * state to STOPPED. If we die before this, the startup process will keep
	 * waiting for us to start up, until it times out.
	 */
	LOCK_SNAP_RCV();
	Assert(SnapRcv->pid == 0);
	switch (SnapRcv->state)
	{
		case WALRCV_STOPPING:
			/* If we've already been requested to stop, don't start up. */
			SnapRcv->state = WALRCV_STOPPED;
			UNLOCK_SNAP_RCV();
			proc_exit(1);
			break;

		case WALRCV_STOPPED:
			SnapRcv->state = WALRCV_STARTING;
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
	/* Advertise our PID so that the startup process can kill us */
	SnapRcv->pid = MyProcPid;
	SnapRcv->procno = MyProc->pgprocno;

	UNLOCK_SNAP_RCV();

	/* Arrange to clean up at walreceiver exit */
	on_shmem_exit(SnapRcvDie, (Datum)0);

	/* make sure dsa_area create */
	(void)SnapRcvGetLockArea();

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

	EnableSnapRcvImmediateExit();
	SnapRcvConnectGTM();
	DisableSnapRcvImmediateExit();

	SnapRcvUpdateShmemConnInfo();

	/* Initialize the last recv timestamp */
	last_recv_timestamp = GetCurrentTimestamp();
	
	for (;;)
	{
		WalRcvStreamOptions options;

		/*
		 * Check that we're connected to a valid server using the
		 * IDENTIFY_SYSTEM replication command.
		 */
		EnableSnapRcvImmediateExit();

		/* options startpoint must be InvalidXLogRecPtr and timeline be 0 */
		options.logical = false;
		options.startpoint = InvalidXLogRecPtr;
		options.slotname = NULL;
		options.proto.physical.startpointTLI = 0;

		if (walrcv_startstreaming(wrconn, &options))
		{
			//walrcv_endstreaming(wrconn, &primaryTLI);
			/* loop until end-of-streaming or error */
			SnapRcvSendLocalNextXid();
			heartbeat_sent = true;
			for(;;)
			{
				char	   *buf;
				int			len;
				pgsocket	wait_fd = PGINVALID_SOCKET;
				int			rc;
				bool		endofwal = false;

				ProcessSnapRcvInterrupts();
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
									   snap_receiver_timeout,
									   PG_WAIT_EXTENSION);
				ResetLatch(&MyProc->procLatch);

				if (rc & WL_POSTMASTER_DEATH)
				{
					/*
					 * Emergency bailout if postmaster has died.  This is to
					 * avoid the necessity for manual cleanup of all
					 * postmaster children.
					 */
					exit(1);
				}

				if ((rc & WL_TIMEOUT) && snap_receiver_timeout > 0 && !heartbeat_sent)
				{
					now = GetCurrentTimestamp();
					timeout = TimestampTzPlusMilliseconds(last_recv_timestamp,
								snap_receiver_timeout);

					if (now >= timeout)
					{
						heartbeat_sent = true;
						SnapRcvSendHeartbeat();
					}
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
		SnapRcv->state = WALRCV_STOPPED;
		proclist_init(&SnapRcv->waiters);
		SnapRcv->procno = INVALID_PGPROCNO;
		SpinLockInit(&SnapRcv->mutex);

		SnapRcv->handle_lock_info = DSM_HANDLE_INVALID;
		SnapRcv->first_lock_info = InvalidDsaPointer;
		SnapRcv->last_lock_info = InvalidDsaPointer;
		LWLockInitialize(&SnapRcv->lock_lock_info, LWTRANCHE_SNAPSHOT_RECEIVER_DSA);
		LWLockInitialize(&SnapRcv->lock_proc_link, LWTRANCHE_SNAPSHOT_RECEIVER_DSA);
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

static void SnapRcvDie(int code, Datum arg)
{
	/* Mark ourselves inactive in shared memory */
	LOCK_SNAP_RCV();
	Assert(SnapRcv->state == WALRCV_STREAMING ||
		   SnapRcv->state == WALRCV_RESTARTING ||
		   SnapRcv->state == WALRCV_STARTING ||
		   SnapRcv->state == WALRCV_WAITING ||
		   SnapRcv->state == WALRCV_STOPPING);
	Assert(SnapRcv->pid == MyProcPid);
	SnapRcv->state = WALRCV_STOPPED;
	SnapRcv->pid = 0;
	SnapRcv->procno = INVALID_PGPROCNO;
	SnapRcv->xcnt = 0;
	SnapRcv->next_try_time = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), RESTART_STEP_MS);	/* 3 seconds */
	UNLOCK_SNAP_RCV();

	/* Terminate the connection gracefully. */
	if (wrconn != NULL)
		walrcv_disconnect(wrconn);
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

/* SIGTERM: set flag for main loop, or shutdown immediately if safe */
static void
SnapRcvShutdownHandler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGTERM = true;

	if (SNAP_RCV_LATCH_VALID())
		SNAP_RCV_SET_LATCH();

	/* Don't joggle the elbow of proc_exit */
	if (!proc_exit_inprogress && SnapRcvImmediateInterruptOK)
		ProcessSnapRcvInterrupts();

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
	wrconn = walrcv_connect(conninfo, false, "snapreceiver", &errstr);
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

static void SnapRcvProcessMessage(unsigned char type, char *buf, Size len)
{
	resetStringInfo(&incoming_message);

	switch (type)
	{
	case 's':				/* snapshot */
		SnapRcvProcessSnapshot(buf, len);
		break;
	case 'a':
		SnapRcvProcessAssign(buf, len);
		break;
	case 'c':
		SnapRcvProcessComplete(buf, len);
		break;
	case 'h':				/* heartbeat response */
		SnapRcvProcessHeartBeat(buf, len);
		break;
	case 'u':				/* heartbeat response */
		SnapRcvProcessUpdateXid(buf, len);
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
	TransactionId latestCompletedXid;
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

	latestCompletedXid = pq_getmsgint(&incoming_message, sizeof(TransactionId));
	count = (incoming_message.len - incoming_message.cursor) / sizeof(TransactionId);

	if (count > lengthof(SnapRcv->xip))
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("too many active transaction ID from GTM %u", count)));
	}
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

#ifdef SNAP_SYNC_DEBUG
	for(i =0 ; i < count; i++)
	{
		ereport(LOG,(errmsg("snaprcv init sync get xid %d\n", xid[i])));
	}
	ereport(LOG,(errmsg("snaprcv init sync toal %d xid\n", count)));
#endif

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
	if (SnapRcv->state == WALRCV_STARTING)
		SnapRcv->state = WALRCV_STREAMING;
	WakeupTransaction(InvalidTransactionId);
	UNLOCK_SNAP_RCV();

	SnapRcvReleaseSnapshotTxidLocks(xid, count, latestCompletedXid);

	if (xid != NULL)
		pfree(xid);

#undef SNAP_HDR_LEN
}

static void SnapRcvProcessUpdateXid(char *buf, Size len)
{
	StringInfoData	msg;
	TransactionId	xid;

	msg.data = buf;
	msg.len = msg.maxlen = len;
	msg.cursor = 0;

	xid = pq_getmsgint64(&msg);
	
	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	ereport(DEBUG2, (errmsg("SnapRcvProcessUpdateXid  %d, ShmemVariableCache->nextXid is %d\n", xid, ShmemVariableCache->nextXid)));
	if (!NormalTransactionIdPrecedes(xid, ShmemVariableCache->nextXid))
	{
 		ShmemVariableCache->nextXid = xid;
 		TransactionIdAdvance(ShmemVariableCache->nextXid);

		ShmemVariableCache->latestCompletedXid = ShmemVariableCache->nextXid;
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
#ifdef SNAP_SYNC_DEBUG
	ereport(LOG,(errmsg("SanpRcv recv assging xid %d\n", txid)));
#endif
		if (SnapRcv->xcnt < MAX_BACKENDS)
		{
			SnapRcv->xip[SnapRcv->xcnt++] = txid;
		}else
		{
			SpinLockRelease(&SnapRcv->mutex);
			ereport(FATAL,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("too many active transaction from GTM")));
		}
	}
	UNLOCK_SNAP_RCV();
}

static void SnapRcvProcessComplete(char *buf, Size len)
{
	StringInfoData	msg;
	TransactionId	txid;
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

	initStringInfo(&xidmsg);
	enlargeStringInfo(&xidmsg, msg.maxlen);

	finish_xid_ack_send = pq_getmsgbyte(&msg);
	if (finish_xid_ack_send)
		pq_sendbyte(&xidmsg, 'f');

	LOCK_SNAP_RCV();
	count = SnapRcv->xcnt;
	while(msg.cursor < msg.len)
	{
		txid = pq_getmsgint(&msg, sizeof(txid));
		for (i=0;i<count;++i)
		{
			if (SnapRcv->xip[i] == txid)
			{
#ifdef SNAP_SYNC_DEBUG
				ereport(LOG,(errmsg("SanpRcv recv finish xid %d\n", txid)));
#endif
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

	UNLOCK_SNAP_RCV();
#ifdef SNAP_SYNC_DEBUG
	ereport(LOG,(errmsg("SanpRcv xcnt now is %d\n", count)));
#endif

	if (finish_xid_ack_send)
		walrcv_send(wrconn, xidmsg.data, xidmsg.len);
	pfree(xidmsg.data);

	msg.cursor = sizeof(bool);
	while (msg.cursor < msg.len)
	{
		txid = pq_getmsgint(&msg, sizeof(txid));
		SnapRcvReleaseTransactionLocks(txid);
	}
}

static void SnapRcvProcessHeartBeat(char *buf, Size len)
{
	StringInfoData	msg;
	TimestampTz		t1, t2, t3, t4, deltatime;

	if (len != 3 * sizeof(t1))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid snapshot transaction timestamp length")));

	t4 = GetCurrentTimestamp();
	msg.data = buf;
	msg.len = msg.maxlen = len;
	msg.cursor = 0;

	t1 = pq_getmsgint64(&msg);
	Assert(t1 == last_heat_beat_sendtime);
	t2 = pq_getmsgint64(&msg);
	t3 = pq_getmsgint64(&msg);

	deltatime = ((t2-t1)+(t3-t4))/2;

	LOCK_SNAP_RCV();
	SnapRcv->gtm_delta_time = deltatime;
	UNLOCK_SNAP_RCV();
}

/*
 * when end < 0 wait until streaming or error
 *   when end == 0 not block
 * mutex must be locked
 */
static bool WaitSnapRcvEvent(TimestampTz end, WaitSnapRcvCond test, void *context)
{
	Latch				   *latch = &MyProc->procLatch;
	long					timeout;
	proclist_mutable_iter	iter;
	int						procno = MyProc->pgprocno;
	int						rc;
	int						waitEvent;

	while ((*test)(context))
	{
		bool in_list = false;
		proclist_foreach_modify(iter, &SnapRcv->waiters, GTMWaitLink)
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
			proclist_push_tail(&SnapRcv->waiters, procno, GTMWaitLink);
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
			return false;
		}

		LOCK_SNAP_RCV();
	}

	/* check if we still in waiting list, remove */
	proclist_foreach_modify(iter, &SnapRcv->waiters, GTMWaitLink)
	{
		if (iter.cur == procno)
		{
			proclist_delete(&SnapRcv->waiters, procno, GTMWaitLink);
			break;
		}
	}

	return true;
}

static bool WaitSnapRcvCondStreaming(void *context)
{
	return SnapRcv->state != WALRCV_STREAMING;
}

static bool WaitSnapRcvCondTransactionComplate(void *context)
{
	TransactionId	txid;
	TransactionId	xid;
	TransactionId	xmax;
	uint32			xcnt;

	/* not in streaming, wait */
	if (SnapRcv->state != WALRCV_STREAMING)
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

Snapshot SnapRcvGetSnapshot(Snapshot snap)
{
	TransactionId	xid,xmax,xmin;
	uint32			i,count,xcnt;
	bool			is_wait_ok;
	TimestampTz end;

	if (snap->xip == NULL)
		EnlargeSnapshotXip(snap, GetMaxSnapshotXidCount());

re_lock_:
	LOCK_SNAP_RCV();
	while (SnapRcv->state != WALRCV_STREAMING)
	{
		/* InvalidTransactionId for wait streaming */
		MyProc->waitGlobalTransaction = InvalidTransactionId;
		end = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), snap_sender_connect_timeout);
		is_wait_ok = WaitSnapRcvEvent(end, WaitSnapRcvCondStreaming, NULL);

		if (!is_wait_ok)
		{
			UNLOCK_SNAP_RCV();
			ereport(ERROR,
				(errmsg("cannot connect to GTMCOORD")));
		}
	}

	Assert(SnapRcv->state == WALRCV_STREAMING);

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

	SetCurrentTransactionStartTimestamp(SnapRcv->gtm_delta_time + GetCurrentTimestamp());
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
	result = WaitSnapRcvEvent(end,
							  WaitSnapRcvCondTransactionComplate,
							  (void*)((size_t)txid));
	UNLOCK_SNAP_RCV();
	MyProc->waitGlobalTransaction = InvalidTransactionId;

	return result;
}

static dsa_area* SnapRcvGetLockArea(void)
{
	static dsa_area *lock_area = NULL;
	if (lock_area == NULL)
	{
		MemoryContext old_context = MemoryContextSwitchTo(TopMemoryContext);
		dsa_handle handle;
		LWLockAcquire(&SnapRcv->lock_lock_info, LW_SHARED);
		handle = SnapRcv->handle_lock_info;
		LWLockRelease(&SnapRcv->lock_lock_info);
		if (handle == DSM_HANDLE_INVALID)
		{
			LWLockAcquire(&SnapRcv->lock_lock_info, LW_EXCLUSIVE);
			if (SnapRcv->handle_lock_info == DSM_HANDLE_INVALID)
			{
				lock_area = dsa_create(LWTRANCHE_SNAPSHOT_RECEIVER_DSA);
				SnapRcv->handle_lock_info = dsa_get_handle(lock_area);
				dsa_pin(lock_area);
			}else
			{
				handle = SnapRcv->handle_lock_info;
			}
			LWLockRelease(&SnapRcv->lock_lock_info);
		}

		if (lock_area == NULL)
		{
			Assert(handle != DSM_HANDLE_INVALID);
			lock_area = dsa_attach(handle);
		}
		dsa_pin_mapping(lock_area);
		MemoryContextSwitchTo(old_context);
	}
	Assert(lock_area != NULL);
	return lock_area;
}

static inline LWLock* SnapRcvGetProcLinkLock(void)
{
	return &SnapRcv->lock_proc_link;
}

static void SnapRcvReleaseLock(SnapLockInfo *lock)
{
	uint32			i;
	LOCKMODE		mode;
	SnapHoldLock   *hold;

	LWLockAcquire(SnapRcvGetProcLinkLock(), LW_EXCLUSIVE);
	for (i=0;i<lock->count;++i)
	{
		hold = &lock->locks[i];
		for (mode=0;mode<=MAX_LOCKMODES;++mode)
		{
			if (hold->holdMask & LOCKBIT_ON(mode))
				TryReleaseLock(&hold->tag, mode, MyProc);
		}
	}
	LWLockRelease(SnapRcvGetProcLinkLock());
}

static void SnapRcvReleaseTransactionLocks(TransactionId xid)
{
	dsa_area	   *lock_area = SnapRcvGetLockArea();
	MemoryContext	old_context = MemoryContextSwitchTo(TopMemoryContext);
	SnapLockInfo   *lock,*prev;
	dsa_pointer		dp, prev_dp;

	lock = prev = NULL;
	LWLockAcquire(&SnapRcv->lock_lock_info, LW_EXCLUSIVE);
	dp = SnapRcv->first_lock_info;
	prev_dp = InvalidDsaPointer;

	while (dp != InvalidDsaPointer)
	{
		lock = dsa_get_address(lock_area, dp);
		if (lock->xid == xid)
		{
			if (!prev)
			{
				Assert(SnapRcv->first_lock_info == dp);
				SnapRcv->first_lock_info = lock->next;
			}
			if (SnapRcv->last_lock_info == dp)
			{
				if (lock->next == InvalidDsaPointer && prev_dp != InvalidDsaPointer)
					SnapRcv->last_lock_info = prev_dp;
				else
					SnapRcv->last_lock_info = lock->next;
			}

			if (prev)
			{
				prev->next = lock->next;
			}
			break;
		}
		prev = lock;
		prev_dp = dp;
		dp = lock->next;
		lock = NULL;
	}
	LWLockRelease(&SnapRcv->lock_lock_info);

	if (lock != NULL)
	{
		Assert(dsa_get_address(lock_area, dp) == lock);
		SnapRcvReleaseLock(lock);
		dsa_free(lock_area, dp);
	}
	MemoryContextSwitchTo(old_context);
}

static void SnapRcvReleaseSnapshotTxidLocks(TransactionId *xip, uint32 count, TransactionId lastxid)
{
	SnapLockInfo   *lock,*prev;
	dsa_area	   *lock_area = SnapRcvGetLockArea();
	SnapshotData	snap;
	dsa_pointer		dp, prev_dp;
	uint32			i;

	MemSet(&snap, 0, sizeof(snap));
	TransactionIdAdvance(lastxid);
	snap.xmin = snap.xmax = lastxid;
	for (i=0;i<count;++i)
	{
		lastxid = xip[i];
		if (NormalTransactionIdPrecedes(lastxid, snap.xmin))
			snap.xmin = lastxid;
	}

	lock = prev = NULL;
	LWLockAcquire(&SnapRcv->lock_lock_info, LW_EXCLUSIVE);
	dp = SnapRcv->first_lock_info;
	prev_dp = InvalidDsaPointer;
	while (dp != InvalidDsaPointer)
	{
		lock = dsa_get_address(lock_area, dp);
		if (XidInMVCCSnapshot(lock->xid, &snap) == false)
		{
			dsa_pointer next = lock->next;
			if (prev)
			{
				prev->next = lock->next;
			}else
			{
				Assert(SnapRcv->first_lock_info == dp);
				SnapRcv->first_lock_info = lock->next;
			}
			if (SnapRcv->last_lock_info == dp)
			{
				if (lock->next == InvalidDsaPointer && prev_dp != InvalidDsaPointer)
					SnapRcv->last_lock_info = prev_dp;
				else
					SnapRcv->last_lock_info = lock->next;
			}
				
			SnapRcvReleaseLock(lock);
			dsa_free(lock_area, dp);
			prev_dp = dp;
			dp = next;
		}else
		{
			prev = lock;
			prev_dp = dp;
			dp = lock->next;
		}
	}
	LWLockRelease(&SnapRcv->lock_lock_info);
}

void* SnapRcvBeginTransferLock(void)
{
	HASHCTL		ctl;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(LOCKTAG);
	ctl.entrysize = sizeof(SnapHoldLock);
	return hash_create("snapshot hold lock",
					   MaxBackends,
					   &ctl,
					   HASH_ELEM|HASH_BLOBS);
}

void SnapRcvInsertTransferLock(void* map, const LOCKTAG *tag, LOCKMASK holdMask)
{
	bool found;
	SnapHoldLock *hold;

	if (tag->locktag_type != LOCKTAG_RELATION ||
		!(holdMask & LOCKBIT_ON(AccessExclusiveLock)))
		return;

	hold = hash_search(map, tag, HASH_ENTER, &found);
	if (found)
		elog(ERROR, "locktag already exist");
	hold->holdMask = holdMask;
}

bool SnapRcvIsHoldLock(void *map, const LOCKTAG *tag)
{
	return hash_search(map, tag, HASH_FIND, NULL) != NULL;
}

void SnapRcvTransferLock(void *map, TransactionId xid, struct PGPROC *from)
{
	uint32					i;
	volatile dsa_pointer	pointer;
	SnapLockInfo		   *info,*prev;
	SnapHoldLock		   *hold;
	HASH_SEQ_STATUS			seq_state;
	dsa_area			   *lock_area;
	PGPROC				   *newproc;

	if (hash_get_num_entries(map) <= 0)
		return;

	lock_area = SnapRcvGetLockArea();
	pointer = dsa_allocate0(lock_area, SNAP_LOCK_INFO_SIZE(hash_get_num_entries(map)));
	PG_TRY();
	{
		info = dsa_get_address(lock_area, pointer);
		info->xid = xid;
		info->count = (uint32)hash_get_num_entries(map);
		info->next = InvalidDsaPointer;
		hash_seq_init(&seq_state, map);
		i = 0;
		while ((hold = hash_seq_search(&seq_state)) != NULL)
		{
			Assert(i < info->count);
			info->locks[i] = *hold;
			++i;
		}

		LWLockAcquire(&SnapRcv->lock_lock_info, LW_EXCLUSIVE);
#ifdef USE_ASSERT_CHECKING
		{
			dsa_pointer dp = SnapRcv->first_lock_info;
			while (dp != InvalidDsaPointer)
			{
				prev = dsa_get_address(lock_area, dp);
				Assert(prev->xid != xid);
				dp = prev->next;
			}
		}
#endif /* USE_ASSERT_CHECKING */
		if (SnapRcv->last_lock_info == InvalidDsaPointer)
		{
			Assert(SnapRcv->first_lock_info == InvalidDsaPointer);
			SnapRcv->first_lock_info = pointer;
		}else
		{
			Assert(SnapRcv->first_lock_info != InvalidDsaPointer);
			prev = dsa_get_address(lock_area, SnapRcv->last_lock_info);
			Assert(prev->next == InvalidDsaPointer);
			prev->next = pointer;
		}
		SnapRcv->last_lock_info = pointer;
		LWLockRelease(&SnapRcv->lock_lock_info);
	}PG_CATCH();
	{
		dsa_free(SnapRcvGetLockArea(), pointer);
		PG_RE_THROW();
	}PG_END_TRY();

	newproc = GetSnapshotProcess();
	hash_seq_init(&seq_state, map);
	LWLockAcquire(&SnapRcv->lock_proc_link, LW_EXCLUSIVE);

	while((hold = hash_seq_search(&seq_state)) != NULL)
		TransferLock(&hold->tag, from, newproc);

	LWLockRelease(&SnapRcv->lock_proc_link);
}

void SnapRcvEndTransferLock(void* map)
{
	hash_destroy(map);
}