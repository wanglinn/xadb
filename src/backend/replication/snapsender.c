#include "postgres.h"

#include "access/transam.h"
#include "access/twophase.h"
#include "pgstat.h"
#include "lib/ilist.h"
#include "libpq/libpq.h"
#include "libpq/pqcomm.h"
#include "libpq/pqformat.h"
#include "libpq/pqnode.h"
#include "libpq/pqnone.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgxc/pgxc.h"
#include "pgxc/poolcomm.h"
#include "replication/snapsender.h"
#include "replication/gxidsender.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/procarray.h"
#include "storage/proclist.h"
#include "storage/spin.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/varlena.h"
#include "utils/hsearch.h"
#include "utils/snapmgr.h"

#define	MAX_CNT_SHMEM_XID_BUF	1024

typedef struct SnapSenderData
{
	proclist_head	waiters_assign;		/* list of waiting event space of xid_assign */
	proclist_head	waiters_complete;	/* list of waiting event space of xid_complete */
	proclist_head	waiters_finish;		/* list of waiting event xid finish ack */
	pid_t			pid;				/* PID of currently active snapsender process */
	int				procno;				/* proc number of current active snapsender process */

	slock_t			mutex;				/* locks shared variables */
	pg_atomic_flag	lock;				/* locks receive client sock */

	pg_atomic_uint32		global_xmin;
	pg_atomic_uint32		global_finish_id;
	volatile uint32			cur_cnt_assign;
	TransactionId	xid_assign[MAX_CNT_SHMEM_XID_BUF];

	volatile uint32			cur_cnt_complete;
	TransactionId	xid_complete[MAX_CNT_SHMEM_XID_BUF];
}SnapSenderData;

typedef struct WaitEventData
{
	void (*fun)(WaitEvent *event, time_t* time_last_latch);
}WaitEventData;

/* in hash table snapsender_xid_htab */
typedef struct XidClientHashItemInfo
{
	TransactionId	xid;
	slist_head		slist_client; 		/* cleint_sockid list */
}XidClientHashItemInfo;

/* item in XidClientHashItemInfo  slist_client */
typedef struct ClientIdListItemInfo
{
	slist_node		snode;
	pgsocket		cleint_sockid;
}ClientIdListItemInfo;

/* item in SnapClientData  slist_xid */
typedef struct SnapSendXidListItem
{
	slist_node         snode;
	TransactionId	   xid;
}SnapSendXidListItem;

typedef struct SnapClientData
{
	WaitEventData	evd;
	slist_node		snode;
	MemoryContext	context;
	pq_comm_node   *node;

	TransactionId  *xid;		/* current transaction count of synchronizing */
	uint32			cur_cnt;
	uint32			max_cnt;

	TimestampTz		last_msg;	/* last time of received message from client */
	ClientStatus	status;
	int				event_pos;
	slist_head		slist_xid;
	TransactionId	global_xmin;
}SnapClientData;

/* GUC variables */
extern char *AGtmHost;

int force_snapshot_consistent = FORCE_SNAP_CON_SESSION;
int snapshot_sync_waittime = 10000;
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_SIGHUP = false;

static SnapSenderData  *SnapSender = NULL;
static slist_head		slist_all_client = SLIST_STATIC_INIT(slist_all_client);
static StringInfoData	output_buffer;
static StringInfoData	input_buffer;

static WaitEventSet	   *wait_event_set = NULL;
static WaitEvent	   *wait_event = NULL;
static uint32			max_wait_event = 0;
static uint32			cur_wait_event = 0;
static int snap_send_timeout = 0;
static HTAB *snapsender_xid_htab;

#define WAIT_EVENT_SIZE_STEP	64
#define WAIT_EVENT_SIZE_START	128

#define SNAP_SENDER_MAX_LISTEN	16
static pgsocket			SnapSenderListenSocket[SNAP_SENDER_MAX_LISTEN];

static void SnapSenderStartup(void);
static void SnapSenderCheckXactPrepareList(void);

/* event handlers */
static void OnLatchSetEvent(WaitEvent *event, time_t* time_last_latch);
static void OnPostmasterDeathEvent(WaitEvent *event, time_t* time_last_latch);
static void OnListenEvent(WaitEvent *event, time_t* time_last_latch);
static void OnClientMsgEvent(WaitEvent *event, time_t* time_last_latch);
static void OnClientRecvMsg(SnapClientData *client, pq_comm_node *node, time_t* time_last_latch);
static void OnClientSendMsg(SnapClientData *client, pq_comm_node *node);

static void ProcessShmemXidMsg(TransactionId *xid, const uint32 xid_cnt, char msgtype);
static void DropClient(SnapClientData *client, bool drop_in_slist);
static bool AppendMsgToClient(SnapClientData *client, char msgtype, const char *data, int len, bool drop_if_failed);

typedef bool (*WaitSnapSenderCond)(void *context);
static const WaitEventData LatchSetEventData = {OnLatchSetEvent};
static const WaitEventData PostmasterDeathEventData = {OnPostmasterDeathEvent};
static const WaitEventData ListenEventData = {OnListenEvent};
static void SnapSendCheckTimeoutSocket(void);
static void snapsender_create_xid_htab(void);
static int	snapsender_match_xid(const void *key1, const void *key2, Size keysize);
static bool SnapSenderWakeupFinishXidEvent(TransactionId txid);
static void snapsenderProcessLocalMaxXid(SnapClientData *client, const char* data, int len);
static void snapsenderUpdateNextXid(TransactionId xid, SnapClientData *exclue_client);
static void SnapSenderSigHupHandler(SIGNAL_ARGS);
static TransactionId snapsenderGetSenderGlobalXmin(void);

/* Signal handlers */
static void SnapSenderSigUsr1Handler(SIGNAL_ARGS);
static void SnapSenderSigTermHandler(SIGNAL_ARGS);
static void SnapSenderQuickDieHander(SIGNAL_ARGS);

static void SnapSenderDie(int code, Datum arg)
{
	SpinLockAcquire(&SnapSender->mutex);
	Assert(SnapSender->pid == MyProc->pid);
	SnapSender->pid = 0;
	SnapSender->procno = INVALID_PGPROCNO;
	SpinLockRelease(&SnapSender->mutex);
}

Size SnapSenderShmemSize(void)
{
	return sizeof(SnapSenderData);
}

void SnapSenderShmemInit(void)
{
	Size		size = SnapSenderShmemSize();
	bool		found;

	SnapSender = (SnapSenderData*)ShmemInitStruct("Snapshot Sender", size, &found);

	if (!found)
	{
		MemSet(SnapSender, 0, size);
		SnapSender->procno = INVALID_PGPROCNO;
		proclist_init(&SnapSender->waiters_assign);
		proclist_init(&SnapSender->waiters_complete);
		proclist_init(&SnapSender->waiters_finish);
		SpinLockInit(&SnapSender->mutex);
		pg_atomic_init_u32(&SnapSender->global_xmin, FirstNormalTransactionId);
		pg_atomic_init_u32(&SnapSender->global_finish_id, InvalidTransactionId);
		pg_atomic_init_flag(&SnapSender->lock);
	}
}

static int snapsender_match_xid(const void *key1, const void *key2, Size keysize)
{
	Oid l,r;
	AssertArg(keysize == sizeof(Oid));

	l = *(TransactionId*)key1;
	r = *(TransactionId*)key2;
	if(l<r)
		return -1;
	else if(l > r)
		return 1;
	return 0;
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void
SnapSenderSigHupHandler(SIGNAL_ARGS)
{
	got_SIGHUP = true;
}

static void snapsender_create_xid_htab(void)
{
	HASHCTL hctl;

	memset(&hctl, 0, sizeof(hctl));
	hctl.keysize = sizeof(TransactionId);
	hctl.entrysize = sizeof(XidClientHashItemInfo);
	hctl.hash = oid_hash;
	hctl.match = snapsender_match_xid;
	hctl.hcxt = TopMemoryContext;
	snapsender_xid_htab = hash_create("hash SnapsenderXid", 100,
			&hctl, HASH_ELEM|HASH_FUNCTION|HASH_COMPARE|HASH_CONTEXT);
}

static bool SnapSenderWakeupFinishXidEvent(TransactionId txid)
{
	proclist_mutable_iter	iter;
	PGPROC					*proc;

	Assert(SnapSender != NULL);
	SpinLockAcquire(&SnapSender->mutex);
	if (SnapSender->procno == INVALID_PGPROCNO)
	{
		SpinLockRelease(&SnapSender->mutex);
		return false;
	}

	proclist_foreach_modify(iter, &SnapSender->waiters_finish, GTMWaitLink)
	{
		proc = GetPGProcByNumber(iter.cur);
		if (txid == proc->waitGlobalTransaction)
		{
			SetLatch(&proc->procLatch);
			proclist_delete(&SnapSender->waiters_finish, proc->pgprocno, GTMWaitLink);
		}
	}

	SpinLockRelease(&SnapSender->mutex);
	return true;
}

static void snapsenderProcessXidFinishAck(SnapClientData *client, const char* data, int len)
{
	StringInfoData	msg;
	TransactionId	txid;
	XidClientHashItemInfo *info;
	slist_mutable_iter siter;
	ClientIdListItemInfo* clientitem;
	SnapSendXidListItem* xiditem;
	pgsocket socket_id;
	bool found;

	msg.data = input_buffer.data;
	msg.len = msg.maxlen = input_buffer.len;
	msg.cursor = 1; /* skip msgtype */
	
	socket_id = socket_pq_node(client->node);

	while(msg.cursor < msg.len)
	{
		txid = pq_getmsgint(&msg, sizeof(txid));

		info = hash_search(snapsender_xid_htab, &txid, HASH_FIND, &found);
		if(found)
		{
			slist_foreach_modify(siter, &info->slist_client)
			{
				clientitem = slist_container(ClientIdListItemInfo, snode, siter.cur);
				if (socket_id == clientitem->cleint_sockid)
				{
					slist_delete_current(&siter);
					pfree(clientitem);
				}
			}
			
			slist_foreach_modify(siter, &client->slist_xid)
			{
				xiditem = slist_container(SnapSendXidListItem, snode, siter.cur);
				if (xiditem->xid == txid)
				{
					slist_delete_current(&siter);
					pfree(xiditem);
				}
			}

			/* if slist is empty, all txid finish response received*/
			if (slist_is_empty(&info->slist_client))
			{
				SnapSenderWakeupFinishXidEvent(txid);

				/* remove empty txid hash item*/
				hash_search(snapsender_xid_htab, &txid, HASH_REMOVE, &found);
			}
		}
	}
}

static void snapsenderUpdateNextXidAllClient(TransactionId xid, SnapClientData *exclude_client)
{
	slist_iter siter;
	SnapClientData *client;

	slist_foreach(siter, &slist_all_client)
	{
		client = slist_container(SnapClientData, snode, siter.cur);
		resetStringInfo(&output_buffer);
		pq_sendbyte(&output_buffer, 'u');
		pq_sendint64(&output_buffer, xid);
		if (AppendMsgToClient(client, 'd', output_buffer.data, output_buffer.len, false) == false)
		{
			client->status = CLIENT_STATUS_EXITING;
		}
	}
}

static void snapsenderUpdateNextXid(TransactionId xid, SnapClientData *client)
{
	if (!TransactionIdIsValid(xid))
		return;

	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	if (NormalTransactionIdFollows(xid, ShmemVariableCache->nextXid))
	{
 		ShmemVariableCache->nextXid = xid;
 		TransactionIdAdvance(ShmemVariableCache->nextXid);

		ShmemVariableCache->latestCompletedXid = ShmemVariableCache->nextXid;
		TransactionIdRetreat(ShmemVariableCache->latestCompletedXid);

		snapsenderUpdateNextXidAllClient(xid, client);
	}

	LWLockRelease(XidGenLock);
}

static void snapsenderProcessLocalMaxXid(SnapClientData *client, const char* data, int len)
{
	StringInfoData	msg;
	TransactionId	txid;

	msg.data = input_buffer.data;
	msg.len = msg.maxlen = input_buffer.len;
	msg.cursor = 1; /* skip msgtype */
	while(msg.cursor < msg.len)
	{
		txid = pq_getmsgint64(&msg);
		snapsenderUpdateNextXid(txid, client);
	}
}

static TransactionId snapsenderGetSenderGlobalXmin(void)
{
	slist_iter siter;
	SnapClientData *cur_client;
	TransactionId oldxmin;
	TransactionId global_xmin = FirstNormalTransactionId;

	slist_foreach(siter, &slist_all_client)
	{
		cur_client = slist_container(SnapClientData, snode, siter.cur);
		if (!TransactionIdIsValid(global_xmin))
			global_xmin = cur_client->global_xmin;
		else if (TransactionIdIsNormal(cur_client->global_xmin) &&
			NormalTransactionIdPrecedes(cur_client->global_xmin, global_xmin ))
			global_xmin = cur_client->global_xmin;
	}

	oldxmin = GetOldestXmin(NULL, PROCARRAY_FLAGS_DEFAULT);
	if (NormalTransactionIdPrecedes(oldxmin, global_xmin))
		global_xmin = oldxmin;
	if (TransactionIdIsValid(global_xmin))
	{
		pg_atomic_write_u32(&SnapSender->global_xmin, global_xmin);
		//ereport(LOG,(errmsg("snapsenderGetSenderGlobalXmin  set xid %d\n", global_xmin)));
	}

	return global_xmin;
}

static void snapsenderProcessHeartBeat(SnapClientData *client)
{
	TimestampTz t1, t2, t3;
	TransactionId xmin,global_xmin, oldxmin;
	slist_iter siter;
	SnapClientData *cur_client;

	input_buffer.cursor = 1;
	t2 = GetCurrentTimestamp();
	t1 = pq_getmsgint64(&input_buffer);
	xmin = pq_getmsgint64(&input_buffer);

	global_xmin = xmin;

	slist_foreach(siter, &slist_all_client)
	{
		cur_client = slist_container(SnapClientData, snode, siter.cur);
		if (client->event_pos == cur_client->event_pos)
		{
			client->global_xmin = xmin;
			continue;
		}
		if (TransactionIdIsNormal(cur_client->global_xmin) &&
			NormalTransactionIdPrecedes(cur_client->global_xmin,global_xmin ))
			global_xmin = cur_client->global_xmin;
	}

	oldxmin = GetOldestXmin(NULL, PROCARRAY_FLAGS_DEFAULT);
	if (TransactionIdIsNormal(global_xmin) && NormalTransactionIdPrecedes(oldxmin, global_xmin))
		global_xmin = oldxmin;

	if (TransactionIdIsNormal(global_xmin))
		pg_atomic_write_u32(&SnapSender->global_xmin, global_xmin);

	/* Send a HEARTBEAT Response message */
	resetStringInfo(&output_buffer);
	pq_sendbyte(&output_buffer, 'h');
	pq_sendint64(&output_buffer, t1);
	pq_sendint64(&output_buffer, t2);
	t3 = GetCurrentTimestamp();
	pq_sendint64(&output_buffer, t3);
	pq_sendint64(&output_buffer, global_xmin);
	if (AppendMsgToClient(client, 'd', output_buffer.data, output_buffer.len, false) == false)
	{
		client->status = CLIENT_STATUS_EXITING;
	}
}

static void snapsenderProcessSyncRequest(SnapClientData *client)
{
	uint64_t key;
	StringInfoData	msg;

	msg.data = input_buffer.data;
	msg.len = msg.maxlen = input_buffer.len;
	msg.cursor = 1; /* skip msgtype */

	key = pq_getmsgint64(&msg);
	SNAP_FORCE_DEBUG_LOG((errmsg("snapsenderProcessSyncRequest get key %lld\n", key)));
	/* Send a HEARTBEAT Response message */
	resetStringInfo(&output_buffer);
	pq_sendbyte(&output_buffer, 'p');
	pq_sendint64(&output_buffer, key);
	if (AppendMsgToClient(client, 'd', output_buffer.data, output_buffer.len, false) == false)
	{
		client->status = CLIENT_STATUS_EXITING;
	}
}

static void SnapSendCheckTimeoutSocket(void)
{
	TimestampTz  now;
	TimestampTz timeout;
	slist_mutable_iter		siter;

	now = GetCurrentTimestamp();
	slist_foreach_modify(siter, &slist_all_client)
	{
		SnapClientData *client = slist_container(SnapClientData, snode, siter.cur);
		if (client && (client->status == CLIENT_STATUS_STREAMING || client->status == CLIENT_STATUS_EXITING))
		{
			timeout = TimestampTzPlusMilliseconds(client->last_msg, snap_send_timeout);
			if (client->status == CLIENT_STATUS_EXITING || now >= timeout)
			{
				slist_delete_current(&siter);
				DropClient(client, false);
			}
		}
	}
	return;
}

void SnapSenderMain(void)
{
	WaitEvent	   *event;
	WaitEventData * volatile wed = NULL;
	sigjmp_buf		local_sigjmp_buf;
	int				rc;
	time_t			time_now,time_last_latch = 0;

	Assert(SnapSender != NULL);

	SpinLockAcquire(&SnapSender->mutex);
	if (SnapSender->pid != 0 ||
		SnapSender->procno != INVALID_PGPROCNO)
	{
		SpinLockRelease(&SnapSender->mutex);
		elog(PANIC, "snapsender running in other process");
	}
	pg_memory_barrier();
	SnapSender->pid = MyProc->pid;
	SnapSender->procno = MyProc->pgprocno;
	SpinLockRelease(&SnapSender->mutex);

	pg_atomic_write_u32(&SnapSender->global_xmin, FirstNormalTransactionId);
	snap_send_timeout = snap_receiver_timeout + 10000L;

	on_shmem_exit(SnapSenderDie, (Datum)0);

	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGHUP, SnapSenderSigHupHandler);
	pqsignal(SIGTERM, SnapSenderSigTermHandler);
	pqsignal(SIGQUIT, SnapSenderQuickDieHander);
	sigdelset(&BlockSig, SIGQUIT);
	pqsignal(SIGUSR1, SnapSenderSigUsr1Handler);
	pqsignal(SIGUSR2, SIG_IGN);

	PG_SETMASK(&UnBlockSig);

	SnapSenderStartup();
	SnapSenderCheckXactPrepareList();
	Assert(SnapSenderListenSocket[0] != PGINVALID_SOCKET);
	Assert(wait_event_set != NULL);

	snapsender_create_xid_htab();
	initStringInfo(&output_buffer);
	initStringInfo(&input_buffer);

	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		slist_mutable_iter siter;
		/* Since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevent interrupts while cleaning up */
		HOLD_INTERRUPTS();

		QueryCancelPending = false; /* second to avoid race condition */

		/* Make sure libpq is in a good state */
		pq_comm_reset();

		/* Report the error to the client and/or server log */
		EmitErrorReport();

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(TopMemoryContext);
		FlushErrorState();

		slist_foreach_modify(siter, &slist_all_client)
		{
			SnapClientData *client = slist_container(SnapClientData, snode, siter.cur);
			if (socket_pq_node(client->node) == PGINVALID_SOCKET)
			{
				slist_delete_current(&siter);
				DropClient(client, false);
			}else if(pq_node_send_pending(client->node))
			{
				ModifyWaitEvent(wait_event_set, client->event_pos, WL_SOCKET_WRITEABLE, NULL);
			}else if(client->status == CLIENT_STATUS_EXITING)
			{
				/* no data sending and exiting, close it */
				slist_delete_current(&siter);
				DropClient(client, false);
			}
		}

		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();
	}
	PG_exception_stack = &local_sigjmp_buf;
	FrontendProtocol = PG_PROTOCOL_LATEST;
	whereToSendOutput = DestRemote;

	while(got_sigterm==false)
	{
		pq_switch_to_none();
		wed = NULL;
		rc = WaitEventSetWait(wait_event_set,
							  100,
							  wait_event,
							  cur_wait_event,
							  PG_WAIT_CLIENT);

		time_now = time(NULL);
		if (rc == 0 ||	/* timeout */
			time_now != time_last_latch)
		{
			pg_memory_barrier();
			MyLatch->is_set = true;
		}
		while(rc > 0)
		{
			event = &wait_event[--rc];
			wed = event->user_data;
			(*wed->fun)(event, &time_last_latch); //
			pq_switch_to_none();
		}
		if (MyLatch->is_set)
			OnLatchSetEvent(NULL, &time_last_latch);

		SnapSendCheckTimeoutSocket();
		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
	}
	proc_exit(1);
}

static void SnapSenderStartup(void)
{
	/* check listen sockets */
	if (socket_snap_pair[1] == PGINVALID_SOCKET)
		ereport(FATAL,
				(errmsg("no socket created for snapsender listening")));

	/* create WaitEventSet */
#if (WAIT_EVENT_SIZE_START < SNAP_SENDER_MAX_LISTEN+2)
#error macro WAIT_EVENT_SIZE_START size too small
#endif
	wait_event_set = CreateWaitEventSet(TopMemoryContext, WAIT_EVENT_SIZE_START);
	wait_event = palloc0(WAIT_EVENT_SIZE_START * sizeof(WaitEvent));
	max_wait_event = WAIT_EVENT_SIZE_START;

	/* add latch */
	AddWaitEventToSet(wait_event_set,
					  WL_LATCH_SET,
					  PGINVALID_SOCKET,
					  &MyProc->procLatch,
					  (void*)&LatchSetEventData);
	++cur_wait_event;

	/* add postmaster death */
	AddWaitEventToSet(wait_event_set,
					  WL_POSTMASTER_DEATH,
					  PGINVALID_SOCKET,
					  NULL,
					  (void*)&PostmasterDeathEventData);
	++cur_wait_event;

	/* add listen sockets */
	Assert(cur_wait_event < max_wait_event);
	AddWaitEventToSet(wait_event_set,
						WL_SOCKET_READABLE,
						socket_snap_pair[1],
						NULL,
						(void*)&ListenEventData);
	++cur_wait_event;

	/* create a fake Port */
	MyProcPort = MemoryContextAllocZero(TopMemoryContext, sizeof(*MyProcPort));
	MyProcPort->remote_host = MemoryContextStrdup(TopMemoryContext, "snapshot receiver");
	MyProcPort->remote_hostname = MyProcPort->remote_host;
	MyProcPort->database_name = MemoryContextStrdup(TopMemoryContext, "snapshot sender");
	MyProcPort->user_name = MyProcPort->database_name;
	MyProcPort->SessionStartTime = GetCurrentTimestamp();
}

/* event handlers */
static void OnLatchSetEvent(WaitEvent *event, time_t* time_last_latch)
{
	TransactionId			xid_assgin[MAX_CNT_SHMEM_XID_BUF];
	TransactionId			xid_finish[MAX_CNT_SHMEM_XID_BUF];
	uint32					assgin_cnt;
	uint32					finish_cnt;
	proclist_mutable_iter	proc_iter_assgin;
	proclist_mutable_iter	proc_iter_finish;
	PGPROC				   *proc;
	time_t					time_now;

	if (!MyLatch->is_set)
		return;

	time_now = time(NULL);
	ResetLatch(&MyProc->procLatch);

	SpinLockAcquire(&SnapSender->mutex);
	Assert(SnapSender->cur_cnt_assign <= MAX_CNT_SHMEM_XID_BUF);
	Assert(SnapSender->cur_cnt_complete <= MAX_CNT_SHMEM_XID_BUF);

	assgin_cnt = SnapSender->cur_cnt_assign;
	finish_cnt = SnapSender->cur_cnt_complete;

	pg_memory_barrier();
	if (assgin_cnt > 0)
	{
		memcpy(xid_assgin, SnapSender->xid_assign, sizeof(TransactionId)*assgin_cnt);
		SnapSender->cur_cnt_assign = 0;
	}

	if (finish_cnt > 0)
	{
		memcpy(xid_finish, SnapSender->xid_complete, sizeof(TransactionId)*finish_cnt);
		SnapSender->cur_cnt_complete = 0;
	}

	proclist_foreach_modify(proc_iter_assgin, &SnapSender->waiters_assign, GTMWaitLink)
	{
		proc = GetPGProcByNumber(proc_iter_assgin.cur);
		Assert(proc->pgprocno == proc_iter_assgin.cur);
		proclist_delete(&SnapSender->waiters_assign, proc_iter_assgin.cur, GTMWaitLink);
		SetLatch(&proc->procLatch);
	}

	proclist_foreach_modify(proc_iter_finish, &SnapSender->waiters_complete, GTMWaitLink)
	{
		proc = GetPGProcByNumber(proc_iter_finish.cur);
		Assert(proc->pgprocno == proc_iter_finish.cur);
		proclist_delete(&SnapSender->waiters_complete, proc_iter_finish.cur, GTMWaitLink);
		SetLatch(&proc->procLatch);
	}
	SpinLockRelease(&SnapSender->mutex);

	/* check assign message */
	if (assgin_cnt > 0)
		ProcessShmemXidMsg(&xid_assgin[0], assgin_cnt, 'a');

	/* check finish transaction */
	if (finish_cnt > 0)
		ProcessShmemXidMsg(&xid_finish[0], finish_cnt, 'c');
	*time_last_latch = time_now;
}

static void ProcessShmemXidMsg(TransactionId *xid, const uint32 xid_cnt, char msgtype)
{
	//proclist_mutable_iter	proc_iter;
	slist_mutable_iter		siter;
	SnapClientData		   *client;
	//PGPROC				   *proc;
	uint32					i;

	if (xid_cnt <= 0)
		return;

	/* send TransactionIds to client */
	output_buffer.cursor = false;	/* use it as bool for flag serialized message */

	slist_foreach_modify(siter, &slist_all_client)
	{
		client = slist_container(SnapClientData, snode, siter.cur);
		Assert(GetWaitEventData(wait_event_set, client->event_pos) == client);
		if (client->status != CLIENT_STATUS_STREAMING)
			continue;
		/* initialize message */
		if (output_buffer.cursor == false)
		{
			resetStringInfo(&output_buffer);
			appendStringInfoChar(&output_buffer, msgtype);

			/* add msg whether need xid finish ack*/
			if (msgtype == 'c')
			{
				pq_sendbyte(&output_buffer, 0);
			}
			for(i=0;i<xid_cnt;++i)
			{
				pq_sendint32(&output_buffer, xid[i]);
				SNAP_SYNC_DEBUG_LOG((errmsg("SnapSend rel finsih/assing %c xid %d\n",
					msgtype, xid[i])));
			}
			output_buffer.cursor = true;
		}


		if (AppendMsgToClient(client, 'd', output_buffer.data, output_buffer.len, false) == false)
		{
					
			SNAP_SYNC_DEBUG_LOG((errmsg("SnapSend send to client event_pos %d error\n",
			 			client->event_pos)));
			client->status = CLIENT_STATUS_EXITING;
		}
	}

}

static void remove_hash_waiter(SnapClientData *client)
{
	slist_mutable_iter		siter;
	slist_mutable_iter		siter2;
	SnapSendXidListItem		*xiditem;
	XidClientHashItemInfo	*info;
	
	ClientIdListItemInfo	*clientitem;
	bool					found;
	pgsocket fd = socket_pq_node(client->node);

	slist_foreach_modify(siter, &client->slist_xid)
	{
		xiditem = slist_container(SnapSendXidListItem, snode, siter.cur);
		info = hash_search(snapsender_xid_htab, &xiditem->xid, HASH_REMOVE, &found);
		if(info)
		{
			slist_foreach_modify(siter2, &info->slist_client)
			{
				clientitem = slist_container(ClientIdListItemInfo, snode, siter2.cur);
				if (clientitem->cleint_sockid == fd)
				{
					slist_delete_current(&siter2);
					pfree(clientitem);
				}
			}

			if (slist_is_empty(&info->slist_client))
			{
				/* remove empty txid hash item*/
				hash_search(snapsender_xid_htab, &xiditem->xid, HASH_REMOVE, &found);
			}
		}

		slist_delete_current(&siter);
		pfree(xiditem);
	}

	return;
}

static void DropClient(SnapClientData *client, bool drop_in_slist)
{
	slist_iter siter;
	pgsocket fd = socket_pq_node(client->node);
	int pos = client->event_pos;

	SnapSendXidListItem* xiditem;
	slist_mutable_iter siter_xid;
	bool found;

	Assert(GetWaitEventData(wait_event_set, client->event_pos) == client);
	SNAP_SYNC_DEBUG_LOG((errmsg("SnapSend DropClient event_pos %d with drop_in_slist %d\n",
			 			client->event_pos, drop_in_slist)));
	if (drop_in_slist)
	{
		slist_delete(&slist_all_client, &client->snode);
		slist_foreach_modify(siter_xid, &client->slist_xid)
		{
			xiditem = slist_container(SnapSendXidListItem, snode, siter_xid.cur);	
			
			hash_search(snapsender_xid_htab, &xiditem->xid, HASH_REMOVE, &found);
			slist_delete_current(&siter_xid);
			pfree(xiditem);
		}
	}

	RemoveWaitEvent(wait_event_set, client->event_pos);
	remove_hash_waiter(client);
	
	pq_node_close(client->node);
	MemoryContextDelete(client->context);
	if (fd != PGINVALID_SOCKET)
		StreamClose(fd);

	slist_foreach(siter, &slist_all_client)
	{
		client = slist_container(SnapClientData, snode, siter.cur);
		if (client->event_pos > pos)
			--client->event_pos;
	}
}

static bool AppendMsgToClient(SnapClientData *client, char msgtype, const char *data, int len, bool drop_if_failed)
{
	pq_comm_node *node = client->node;
	bool old_send_pending = pq_node_send_pending(node);
	Assert(GetWaitEventData(wait_event_set, client->event_pos) == client);

	pq_node_putmessage_noblock_sock(node, msgtype, data, len);
	if (old_send_pending == false)
	{
		if (pq_node_flush_if_writable_sock(node) != 0)
		{
			if (drop_if_failed)
				DropClient(client, true);
			return false;
		}

		if (pq_node_send_pending(node))
		{
			ModifyWaitEvent(wait_event_set,
							client->event_pos,
							WL_SOCKET_WRITEABLE,
							NULL);
		}
		client->last_msg = GetCurrentTimestamp();
	}

	return true;
}

static void OnPostmasterDeathEvent(WaitEvent *event, time_t* time_last_latch)
{
	exit(1);
}

void OnListenEvent(WaitEvent *event, time_t* time_last_latch)
{
	MemoryContext volatile oldcontext = CurrentMemoryContext;
	MemoryContext volatile newcontext = NULL;
	SnapClientData *client;
	int				client_fdsock;

	if(pool_recvfds(event->fd, &client_fdsock, 1) != 0)
	{
		ereport(WARNING, (errmsg("receive client socke failed:%m\n")));
		return;
	}

	PG_TRY();
	{
		newcontext = AllocSetContextCreate(TopMemoryContext,
										   "Snapshot sender client",
										   ALLOCSET_DEFAULT_SIZES);

		client = palloc0(sizeof(*client));
		client->context = newcontext;
		client->evd.fun = OnClientMsgEvent;
		client->node = pq_node_new(client_fdsock, false);
		client->last_msg = GetCurrentTimestamp();
		client->max_cnt = GetMaxSnapshotXidCount();
		client->xid = palloc(client->max_cnt * sizeof(TransactionId));
		client->cur_cnt = 0;
		client->global_xmin = FirstNormalTransactionId;
		slist_init(&(client->slist_xid));
		client->status = CLIENT_STATUS_CONNECTED;

		if (cur_wait_event == max_wait_event)
		{
			wait_event_set = EnlargeWaitEventSet(wait_event_set,
												 cur_wait_event + WAIT_EVENT_SIZE_STEP);
			max_wait_event += WAIT_EVENT_SIZE_STEP;
			wait_event = repalloc(wait_event, max_wait_event * sizeof(WaitEvent));
			
		}
		client->event_pos = AddWaitEventToSet(wait_event_set,
											  WL_SOCKET_READABLE,	/* waiting start pack */
											  client_fdsock,
											  NULL,
											  client);
		slist_push_head(&slist_all_client, &client->snode);
		++cur_wait_event;

		MemoryContextSwitchTo(oldcontext);
	}PG_CATCH();
	{
		if (client_fdsock != PGINVALID_SOCKET)
			StreamClose(client_fdsock);

		MemoryContextSwitchTo(oldcontext);
		if (newcontext != NULL)
			MemoryContextDelete(newcontext);

		PG_RE_THROW();
	}PG_END_TRY();
}

static void OnClientMsgEvent(WaitEvent *event, time_t* time_last_latch)
{
	SnapClientData *volatile client = event->user_data;
	pq_comm_node   *node;
	uint32			new_event;

	Assert(GetWaitEventData(wait_event_set, client->event_pos) == client);

	PG_TRY();
	{
		node = client->node;
		new_event = 0;

		pq_node_switch_to(node);

		if (event->events & WL_SOCKET_READABLE)
		{
			if (client->status == CLIENT_STATUS_EXITING)
				ModifyWaitEvent(wait_event_set, event->pos, 0, NULL);
			else
				OnClientRecvMsg(client, node, time_last_latch);
		}
		if (event->events & (WL_SOCKET_WRITEABLE|WL_SOCKET_CONNECTED))
			OnClientSendMsg(client, node);

		if (pq_node_send_pending(node))
		{
			if ((event->events & (WL_SOCKET_WRITEABLE|WL_SOCKET_CONNECTED)) == 0)
				new_event = WL_SOCKET_WRITEABLE;
		}else if(client->status != CLIENT_STATUS_EXITING)
		{
			if ((event->events & WL_SOCKET_READABLE) == 0)
				new_event = WL_SOCKET_READABLE;
		}

		if (new_event != 0)
			ModifyWaitEvent(wait_event_set, event->pos, new_event, NULL);

		/* all data sended and exiting, close it */
		if(client->status == CLIENT_STATUS_EXITING)
			DropClient(client, true);
	}PG_CATCH();
	{
		client->status = CLIENT_STATUS_EXITING;
		PG_RE_THROW();
	}PG_END_TRY();
}

/* like GetSnapshotData, but serialize all active transaction IDs */
static void SerializeFullAssignXid(TransactionId *gs_xip, uint32 gs_cnt, TransactionId *ss_xip, uint32 ss_cnt, StringInfo buf)
{
	int index,i;
	TransactionId xid;
	bool	skip;

	/* get all Transaction IDs */
	for (index = 0; index < gs_cnt; ++index)
	{
		xid = gs_xip[index];
		skip = false;
		for (i = 0; i < ss_cnt ; i ++)
		{
			if (xid == ss_xip[i])
			{
				skip = true;
				break;
			}
		}

		if (skip)
			continue;

		pq_sendint32(buf, xid);
		Assert(TransactionIdIsNormal(xid));
		SNAP_SYNC_DEBUG_LOG((errmsg("SnapSend init sync xid %d\n", xid)));
	}
}

static void OnClientRecvMsg(SnapClientData *client, pq_comm_node *node, time_t* time_last_latch)
{
	int						msgtype;
	TransactionId			ss_xid_assgin[MAX_CNT_SHMEM_XID_BUF];
	uint32					ss_cnt_assign;
	TransactionId			*gs_xip;
	uint32					gs_cnt_assign;

	if (pq_node_recvbuf(node) != 0)
	{
		ereport(ERROR,
				(errmsg("client closed stream")));
	}

	client->last_msg = GetCurrentTimestamp();

	while (1)
	{
		resetStringInfo(&input_buffer);
		msgtype = pq_node_get_msg(&input_buffer, node);
		switch(msgtype)
		{
		case 'Q':
			/* only support "START_REPLICATION" command */
			if (strcasecmp(input_buffer.data, "START_REPLICATION 0/0 TIMELINE 0") != 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						errposition(0),
						errmsg("only support \"START_REPLICATION 0/0 TIMELINE 0\" command")));

			/* Send a CopyBothResponse message, and start streaming */
			resetStringInfo(&output_buffer);
			pq_sendbyte(&output_buffer, 0);
			pq_sendint16(&output_buffer, 0);
			AppendMsgToClient(client, 'W', output_buffer.data, output_buffer.len, false);

			/* send snapshot */
			resetStringInfo(&output_buffer);
			appendStringInfoChar(&output_buffer, 's');
			pq_sendint32(&output_buffer, snapsenderGetSenderGlobalXmin());
			SerializeActiveTransactionIds(&output_buffer);

			gs_xip = GxidSenderGetAllXip(&gs_cnt_assign);
			SpinLockAcquire(&SnapSender->mutex);
			pg_memory_barrier();
			ss_cnt_assign = SnapSender->cur_cnt_assign;
			if (ss_cnt_assign > 0)
				memcpy(ss_xid_assgin, SnapSender->xid_assign, sizeof(TransactionId)*ss_cnt_assign);
			
			SpinLockRelease(&SnapSender->mutex);

			SerializeFullAssignXid(gs_xip, gs_cnt_assign, ss_xid_assgin, ss_cnt_assign, &output_buffer);
			pfree(gs_xip);
			AppendMsgToClient(client, 'd', output_buffer.data, output_buffer.len, false);

			client->status = CLIENT_STATUS_STREAMING;
			break;
		case 'X':
			client->status = CLIENT_STATUS_EXITING;
			return;
		case 'c':
		case 'd':
			if (client->status != CLIENT_STATUS_STREAMING)
			{
				ereport(ERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						errmsg("not in copy mode")));
			}
			if (msgtype == 'c')
				client->status = CLIENT_STATUS_CONNECTED;
			else
			{
				if (strcasecmp(input_buffer.data, "h") == 0)
				{
					snapsenderProcessHeartBeat(client);
				}
				else if (strcasecmp(input_buffer.data, "f") == 0)
				{
					snapsenderProcessXidFinishAck(client, input_buffer.data, input_buffer.len);
				}
				else if (strcasecmp(input_buffer.data, "u") == 0)
				{
					snapsenderProcessLocalMaxXid(client, input_buffer.data, input_buffer.len);
				}
				else if (strcasecmp(input_buffer.data, "p") == 0)
				{
					OnLatchSetEvent(NULL, time_last_latch);
					snapsenderProcessSyncRequest(client);
				}
			}
			break;
		case 0:
			return;
		default:
			break;
		}
	}
}

static void OnClientSendMsg(SnapClientData *client, pq_comm_node *node)
{
	if (pq_node_flush_if_writable_sock(node) != 0)
	{
		client->status = CLIENT_STATUS_EXITING;
	}
	else
	{
		client->last_msg = GetCurrentTimestamp();
	}
}

/* SIGUSR1: used by latch mechanism */
static void SnapSenderSigUsr1Handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	latch_sigusr1_handler();

	errno = save_errno;
}

static void SnapSenderSigTermHandler(SIGNAL_ARGS)
{
	got_sigterm = true;
}

static void SnapSenderQuickDieHander(SIGNAL_ARGS)
{
	if (proc_exit_inprogress)
		return;

	proc_exit_inprogress = true;
	PG_SETMASK(&BlockSig);

	on_exit_reset();

	exit(2);
}

/* mutex must locked */
static void WaitSnapSendShmemSpace(volatile slock_t *mutex,
								   volatile uint32 *cur,
								   proclist_head *waiters)
{
	Latch				   *latch = &MyProc->procLatch;
	proclist_mutable_iter	iter;
	int						procno = MyProc->pgprocno;
	int						rc;

	while (*cur == MAX_CNT_SHMEM_XID_BUF)
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
			MyProc->waitGlobalTransaction = InvalidTransactionId;
			pg_write_barrier();
			proclist_push_tail(waiters, procno, GTMWaitLink);
		}
#ifdef USE_ASSERT_CHECKING
		else
		{
			Assert(MyProc->waitGlobalTransaction == InvalidTransactionId);
		}
#endif /* USE_ASSERT_CHECKING */
		SpinLockRelease(mutex);

		rc = WaitLatch(latch,
					   WL_POSTMASTER_DEATH | WL_LATCH_SET,
					   -1,
					   PG_WAIT_EXTENSION);
		ResetLatch(latch);
		if (rc & WL_POSTMASTER_DEATH)
		{
			exit(1);
		}
		SpinLockAcquire(mutex);
	}

	/* check if we still in wait list, remove */
	proclist_foreach_modify(iter, waiters, GTMWaitLink)
	{
		if (iter.cur == procno)
		{
			proclist_delete(waiters, procno, GTMWaitLink);
			break;
		}
	}
}

static void SnapSenderCheckXactPrepareList(void)
{
	List			*xid_list;
	ListCell		*lc;
	TransactionId	xid;

	xid_list = GetPreparedXidList();

	SpinLockAcquire(&SnapSender->mutex);
	foreach (lc, xid_list)
	{
		if(SnapSender->cur_cnt_assign == MAX_CNT_SHMEM_XID_BUF)
		WaitSnapSendShmemSpace(&SnapSender->mutex,
							   &SnapSender->cur_cnt_assign,
							   &SnapSender->waiters_assign);
		Assert(SnapSender->cur_cnt_assign < MAX_CNT_SHMEM_XID_BUF);
		xid = lfirst_int(lc);
		ereport(LOG,(errmsg("SnapSend restart get 2pc left xid %d\n",
			 			xid)));
		SnapSender->xid_assign[SnapSender->cur_cnt_assign++] = xid;
	}
	SpinLockRelease(&SnapSender->mutex);
	list_free(xid_list);
	return;
}

void SnapSendTransactionAssign(TransactionId txid, int txidnum, TransactionId parent)
{
	int i = 0;

	Assert(TransactionIdIsValid(txid));
	Assert(TransactionIdIsNormal(txid));
	if (!IsGTMNode())
		return;

	Assert(SnapSender != NULL);
	if (TransactionIdIsValid(parent))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("snapshot sender not support sub transaction yet!")));

	SpinLockAcquire(&SnapSender->mutex);
	if (SnapSender->procno == INVALID_PGPROCNO)
	{
		SpinLockRelease(&SnapSender->mutex);
		return;
	}

	for (i = txidnum; i > 0; i--)
	{
		SNAP_SYNC_DEBUG_LOG((errmsg("Call SnapSend assging xid %d\n",
							txid)));
		if(SnapSender->cur_cnt_assign == MAX_CNT_SHMEM_XID_BUF)
			WaitSnapSendShmemSpace(&SnapSender->mutex,
								&SnapSender->cur_cnt_assign,
								&SnapSender->waiters_assign);
		Assert(SnapSender->cur_cnt_assign < MAX_CNT_SHMEM_XID_BUF);
		SnapSender->xid_assign[SnapSender->cur_cnt_assign++] = txid--;
	}

	SetLatch(&(GetPGProcByNumber(SnapSender->procno)->procLatch));
	SpinLockRelease(&SnapSender->mutex);
}

void SnapSendTransactionAssignArray(TransactionId* xids, int xid_num, TransactionId parent)
{
	TransactionId 	txid;
	int				i;

	if (!IsGTMNode())
		return;
	
	Assert(SnapSender != NULL);
	if (TransactionIdIsValid(parent))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("snapshot sender not support sub transaction yet!")));

	SpinLockAcquire(&SnapSender->mutex);
	if (SnapSender->procno == INVALID_PGPROCNO)
	{
		SpinLockRelease(&SnapSender->mutex);
		return;
	}

	for (i = 0; i < xid_num; i++)
	{
		txid = xids[i];
		Assert(TransactionIdIsValid(txid));
		Assert(TransactionIdIsNormal(txid));

		SNAP_SYNC_DEBUG_LOG((errmsg("Call SnapSend assging xid %d\n",
			 			txid)));
		if(SnapSender->cur_cnt_assign == MAX_CNT_SHMEM_XID_BUF)
			WaitSnapSendShmemSpace(&SnapSender->mutex,
								&SnapSender->cur_cnt_assign,
								&SnapSender->waiters_assign);
		Assert(SnapSender->cur_cnt_assign < MAX_CNT_SHMEM_XID_BUF);
		SnapSender->xid_assign[SnapSender->cur_cnt_assign++] = txid;
	}
	SetLatch(&(GetPGProcByNumber(SnapSender->procno)->procLatch));
	SpinLockRelease(&SnapSender->mutex);
}

void SnapSendTransactionFinish(TransactionId txid)
{
	if(!TransactionIdIsValid(txid) || !IsGTMNode())
		return;

	Assert(TransactionIdIsNormal(txid));
	Assert(SnapSender != NULL);

	SpinLockAcquire(&SnapSender->mutex);
	if (SnapSender->procno == INVALID_PGPROCNO)
	{
		SpinLockRelease(&SnapSender->mutex);
		return;
	}

	SNAP_SYNC_DEBUG_LOG((errmsg("Call SnapSend finish xid %d\n",
			 			txid)));
	if(SnapSender->cur_cnt_complete == MAX_CNT_SHMEM_XID_BUF)
		WaitSnapSendShmemSpace(&SnapSender->mutex,
							   &SnapSender->cur_cnt_complete,
							   &SnapSender->waiters_complete);
	Assert(SnapSender->cur_cnt_complete < MAX_CNT_SHMEM_XID_BUF);
	SnapSender->xid_complete[SnapSender->cur_cnt_complete++] = txid;
	SetLatch(&(GetPGProcByNumber(SnapSender->procno)->procLatch));
	
	SpinLockRelease(&SnapSender->mutex);
}

void SnapSendLockSendSock(void)
{
re_lock_:
	if (!SnapSender)
	{
		pg_usleep(100000L);
		goto re_lock_;
	}

	if (!pg_atomic_test_set_flag(&SnapSender->lock))
	{
		pg_usleep(100000L);
		goto re_lock_;
	}
}

void SnapSendUnlockSendSock(void)
{
	pg_atomic_clear_flag(&SnapSender->lock);
}

TransactionId SnapSendGetGlobalXmin(void)
{
	TransactionId xmin;
	xmin = pg_atomic_read_u32(&SnapSender->global_xmin);
	//ereport(LOG,(errmsg("SnapSendGetGlobalXmin  get xid %d\n", xmin)));
	return xmin;
}

void SnapSenderGetStat(StringInfo buf)
{
	int				i;
	TransactionId	*assign_xids;
	uint32			assign_len;
	TransactionId	*finish_xids;
	uint32			finish_len;

	assign_len = finish_len = XID_ARRAY_STEP_SIZE;
	assign_xids = NULL;
	finish_xids = NULL;
re_lock_:
	if (!assign_xids)
		assign_xids = palloc0(sizeof(TransactionId) * assign_len);
	else
		assign_xids = repalloc(assign_xids, sizeof(TransactionId) * assign_len);
	
	if (!finish_xids)
		finish_xids = palloc0(sizeof(TransactionId) * finish_len);
	else
		finish_xids = repalloc(finish_xids, sizeof(TransactionId) * finish_len);
	
	SpinLockAcquire(&SnapSender->mutex);
	if (assign_len <  SnapSender->cur_cnt_assign || finish_len < SnapSender->cur_cnt_complete)
	{
		SpinLockRelease(&SnapSender->mutex);
		assign_len += XID_ARRAY_STEP_SIZE;
		finish_len += XID_ARRAY_STEP_SIZE;
		goto re_lock_;
	}

	assign_len = SnapSender->cur_cnt_assign;
	for (i = 0; i < SnapSender->cur_cnt_assign; i++)
	{
		assign_xids[i] = SnapSender->xid_assign[i];
	}

	finish_len = SnapSender->cur_cnt_complete;
	for (i = 0; i < SnapSender->cur_cnt_complete; i++)
	{
		finish_xids[i] = SnapSender->xid_complete[i];
	}
	SpinLockRelease(&SnapSender->mutex);

	appendStringInfo(buf, " cur_cnt_assign: %u \n", assign_len);
	appendStringInfo(buf, "  xid_assign: [");

	qsort(assign_xids, assign_len, sizeof(TransactionId), xidComparator);
	for (i = 0; i < assign_len; i++)
	{
		appendStringInfo(buf, "%u ", assign_xids[i]);
		if (i > 0 && i % XID_PRINT_XID_LINE_NUM == 0)
			appendStringInfo(buf, "\n  ");
	}
	appendStringInfo(buf, "]");

	appendStringInfo(buf, "\n cur_cnt_complete: %u \n", finish_len);
	appendStringInfo(buf, "  xid_complete: [");

	qsort(finish_xids, finish_len, sizeof(TransactionId), xidComparator);
	for (i = 0; i < finish_len; i++)
	{
		appendStringInfo(buf, "%u ", finish_xids[i]);
		if (i > 0 && i % XID_PRINT_XID_LINE_NUM == 0)
			appendStringInfo(buf, "\n  ");
	}
	appendStringInfo(buf, "]");

	pfree(assign_xids);
	pfree(finish_xids);
}