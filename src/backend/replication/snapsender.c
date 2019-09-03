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
#include "replication/snapsender.h"
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

#define	MAX_CNT_SHMEM_XID_BUF	100

typedef struct SnapSenderData
{
	proclist_head	waiters_assign;		/* list of waiting event space of xid_assign */
	proclist_head	waiters_complete;	/* list of waiting event space of xid_complete */
	proclist_head	waiters_finish;		/* list of waiting event xid finish ack */
	pid_t			pid;				/* PID of currently active snapsender process */
	int				procno;				/* proc number of current active snapsender process */

	slock_t			mutex;				/* locks shared variables */

	uint32			cur_cnt_assign;
	TransactionId	xid_assign[MAX_CNT_SHMEM_XID_BUF];

	uint32			cur_cnt_complete;
	TransactionId	xid_complete[MAX_CNT_SHMEM_XID_BUF];
}SnapSenderData;

typedef struct WaitEventData
{
	void (*fun)(WaitEvent *event);
}WaitEventData;

typedef enum ClientStatus
{
	CLIENT_STATUS_CONNECTED = 1,
	CLIENT_STATUS_STREAMING = 2,
	CLIENT_STATUS_EXITING = 3
}ClientStatus;

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
}SnapClientData;

/* GUC variables */
extern char *AGtmHost;
extern int snapsender_port;
extern int snap_receiver_timeout;

static volatile sig_atomic_t got_sigterm = false;

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
static void OnLatchSetEvent(WaitEvent *event);
static void OnPostmasterDeathEvent(WaitEvent *event);
static void OnListenEvent(WaitEvent *event);
static void OnClientMsgEvent(WaitEvent *event);
static void OnClientRecvMsg(SnapClientData *client, pq_comm_node *node);
static void OnClientSendMsg(SnapClientData *client, pq_comm_node *node);

static void ProcessShmemXidMsg(slock_t *lock, proclist_head *waiters, uint32 *cursor, TransactionId *shmemxid, char msgtype);
static void DropClient(SnapClientData *client, bool drop_in_slist);
static bool AppendMsgToClient(SnapClientData *client, char msgtype, const char *data, int len, bool drop_if_failed);

typedef bool (*WaitSnapSenderCond)(void *context);
static const WaitEventData LatchSetEventData = {OnLatchSetEvent};
static const WaitEventData PostmasterDeathEventData = {OnPostmasterDeathEvent};
static const WaitEventData ListenEventData = {OnListenEvent};
static void SnapSendCheckTimeoutSocket(void);
static void snapsender_create_xid_htab(void);
static void append_client_xid_to_htab(SnapClientData *client, TransactionId xid);
static int	snapsender_match_xid(const void *key1, const void *key2, Size keysize);
static bool SnapSenderWakeupFinishXidEvent(TransactionId txid);
static bool SnapSenderWaitTxidFinsihEvent(TimestampTz end, WaitSnapSenderCond test, void *context);
static bool WaitSnapSendCondTransactionComplate(void *context);
static void snapsenderProcessLocalMaxXid(SnapClientData *client, const char* data, int len);

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

static void append_client_xid_to_htab(SnapClientData *client, TransactionId xid)
{
	XidClientHashItemInfo *info;
	SnapSendXidListItem* xiditem;
	ClientIdListItemInfo* clientitem;
	slist_mutable_iter siter;
	bool found;

	xiditem = palloc0(sizeof(*xiditem));
	xiditem->xid = xid;
	slist_push_head(&client->slist_xid, &xiditem->snode);

	info = hash_search(snapsender_xid_htab, &xid, HASH_ENTER, &found);
	if(found == false)
	{
		MemSet(info, 0, sizeof(*info));
		info->xid = xid;
		slist_init(&(info->slist_client));
	}

	slist_foreach_modify(siter, &info->slist_client)
	{
		clientitem = slist_container(ClientIdListItemInfo, snode, siter.cur);
		if (socket_pq_node(client->node) == clientitem->cleint_sockid)
		{
			return;
		}
	}

	clientitem = palloc0(sizeof(*clientitem));
	clientitem->cleint_sockid = socket_pq_node(client->node);
	slist_push_head(&info->slist_client, &clientitem->snode);
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

static void snapsenderUpdateNextXid(TransactionId xid)
{
	if (!TransactionIdIsValid(xid))
		return;

	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	if (NormalTransactionIdFollows(xid, ShmemVariableCache->nextXid))
	{
 		ShmemVariableCache->nextXid = xid;
 		TransactionIdAdvance(ShmemVariableCache->nextXid);
	}
	LWLockRelease(XidGenLock);
}

static void snapsenderProcessLocalMaxXid(SnapClientData *client, const char* data, int len)
{
	StringInfoData	msg;
	TransactionId	txid;
	bool found;

	msg.data = input_buffer.data;
	msg.len = msg.maxlen = input_buffer.len;
	msg.cursor = 1; /* skip msgtype */
	
	while(msg.cursor < msg.len)
	{
		txid = pq_getmsgint(&msg, sizeof(txid));
		snapsenderUpdateNextXid(txid);
	}
}

static void snapsenderProcessHeartBeat(SnapClientData *client)
{
	TimestampTz t1, t2, t3;

	input_buffer.cursor = 1;
	t2 = GetCurrentTimestamp();
	t1 = pq_getmsgint64(&input_buffer);
	

	/* Send a HEARTBEAT Response message */
	resetStringInfo(&output_buffer);
	pq_sendbyte(&output_buffer, 'h');
	pq_sendint64(&output_buffer, t1);
	pq_sendint64(&output_buffer, t2);

	t3 = GetCurrentTimestamp();
	pq_sendint64(&output_buffer, t3);
	if (AppendMsgToClient(client, 'd', output_buffer.data, output_buffer.len, false) == false)
	{
		DropClient(client, true);
	}
}

static bool WaitSnapSendCondTransactionComplate(void *context)
{
	proclist_mutable_iter	iter;
	PGPROC				   	*proc;
	int						procno = MyProc->pgprocno;

	proclist_foreach_modify(iter, &SnapSender->waiters_finish, GTMWaitLink)
	{
		proc = GetPGProcByNumber(iter.cur);
		if (proc->pgprocno == procno)
		{
			return true;
		}
	}

	return false;
}

static bool SnapSenderWaitTxidFinsihEvent(TimestampTz end, WaitSnapSenderCond test, void *context)
{
	int						rc;
	Latch					*latch = &MyProc->procLatch;
	long					timeout;
	int						waitEvent;

	while ((*test)(context))
	{
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
		SpinLockRelease(&SnapSender->mutex);

		rc = WaitLatch(latch, waitEvent, timeout, PG_WAIT_EXTENSION);
		ResetLatch(latch);
		if (rc & WL_POSTMASTER_DEATH)
		{
			exit(1);
		}else if(rc & WL_TIMEOUT)
		{
			MyProc->waitGlobalTransaction = InvalidTransactionId;
			return false;
		}

		SpinLockAcquire(&SnapSender->mutex);
	}

	MyProc->waitGlobalTransaction = InvalidTransactionId;
	return true;
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
		if (client && client->status == CLIENT_STATUS_STREAMING)
		{
			timeout = TimestampTzPlusMilliseconds(client->last_msg, snap_send_timeout);
			if (now >= timeout)
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
	int				rc,i;

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
	snap_send_timeout = snap_receiver_timeout + 10000L;

	on_shmem_exit(SnapSenderDie, (Datum)0);

	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGHUP, SIG_IGN);
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
							  snap_send_timeout,
							  wait_event,
							  cur_wait_event,
							  PG_WAIT_CLIENT);

		for(i=0;i<rc;++i)
		{
			event = &wait_event[i];
			wed = event->user_data;
			(*wed->fun)(event);
			pq_switch_to_none();
		}

		SnapSendCheckTimeoutSocket();
	}
	proc_exit(1);
}

static void SnapSenderStartup(void)
{
	Size i;

	/* initialize listen sockets */
	for(i=SNAP_SENDER_MAX_LISTEN;i>0;--i)
		SnapSenderListenSocket[i-1] = PGINVALID_SOCKET;

	/* create listen sockets */
	if (AGtmHost)
	{
		char	   *rawstring;
		List	   *elemlist;
		ListCell   *l;
		int			status;

		rawstring = pstrdup(AGtmHost);
		/* Parse string into list of hostnames */
		if (!SplitIdentifierString(rawstring, ',', &elemlist))
		{
			/* syntax error in list */
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid list syntax in parameter \"%s\"",
							"agtm_host")));
		}

		foreach(l, elemlist)
		{
			char *curhost = lfirst(l);
			status = StreamServerPort(AF_UNSPEC,
									  strcmp(curhost, "*") == 0 ? NULL:curhost,
									  (unsigned short)snapsender_port,
									  NULL,
									  SnapSenderListenSocket,
									  SNAP_SENDER_MAX_LISTEN);
			if (status != STATUS_OK)
			{
				ereport(WARNING,
						(errcode_for_socket_access(),
						 errmsg("could not create listen socket for \"%s\"",
								curhost)));
			}
		}
		list_free(elemlist);
		pfree(rawstring);
	}else if (StreamServerPort(AF_UNSPEC, NULL,
							   (unsigned short)snapsender_port,
							   NULL,
							   SnapSenderListenSocket,
							   SNAP_SENDER_MAX_LISTEN) != STATUS_OK)
	{
		ereport(WARNING,
				(errcode_for_socket_access(),
				 errmsg("could not create listen socket for \"%s\"",
						"*")));
	}

	/* check listen sockets */
	if (SnapSenderListenSocket[0] == PGINVALID_SOCKET)
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
	for(i=0;i<SNAP_SENDER_MAX_LISTEN;++i)
	{
		if (SnapSenderListenSocket[i] == PGINVALID_SOCKET)
			break;

		Assert(cur_wait_event < max_wait_event);
		AddWaitEventToSet(wait_event_set,
						  WL_SOCKET_READABLE,
						  SnapSenderListenSocket[i],
						  NULL,
						  (void*)&ListenEventData);
		++cur_wait_event;
	}

	/* create a fake Port */
	MyProcPort = MemoryContextAllocZero(TopMemoryContext, sizeof(*MyProcPort));
	MyProcPort->remote_host = MemoryContextStrdup(TopMemoryContext, "snapshot receiver");
	MyProcPort->remote_hostname = MyProcPort->remote_host;
	MyProcPort->database_name = MemoryContextStrdup(TopMemoryContext, "snapshot sender");
	MyProcPort->user_name = MyProcPort->database_name;
	MyProcPort->SessionStartTime = GetCurrentTimestamp();
}

/* event handlers */
static void OnLatchSetEvent(WaitEvent *event)
{
	ResetLatch(&MyProc->procLatch);

	/* check assign message */
	ProcessShmemXidMsg(&SnapSender->mutex,
					   &SnapSender->waiters_assign,
					   &SnapSender->cur_cnt_assign,
					   SnapSender->xid_assign,
					   'a');

	/* check finish transaction */
	ProcessShmemXidMsg(&SnapSender->mutex,
					   &SnapSender->waiters_complete,
					   &SnapSender->cur_cnt_complete,
					   SnapSender->xid_complete,
					  'c');
}

static void ProcessShmemXidMsg(slock_t *lock, proclist_head *waiters, uint32 *cursor, TransactionId *shmemxid, char msgtype)
{
	proclist_mutable_iter	proc_iter;
	slist_mutable_iter		siter;
	SnapClientData		   *client;
	PGPROC				   *proc;
	TransactionId			xid[MAX_CNT_SHMEM_XID_BUF];
	uint32					xid_cnt,i;

	/* fetch TransactionIds */
	SpinLockAcquire(lock);
	Assert(*cursor < MAX_CNT_SHMEM_XID_BUF);
	xid_cnt = *cursor;
	pg_memory_barrier();
	if (xid_cnt > 0)
	{
		memcpy(xid, shmemxid, sizeof(TransactionId)*xid_cnt);
		*cursor = 0;
	}
	proclist_foreach_modify(proc_iter, waiters, GTMWaitLink)
	{
		proc = GetPGProcByNumber(proc_iter.cur);
		Assert(proc->pgprocno == proc_iter.cur);
		proclist_delete(&SnapSender->waiters_assign, proc_iter.cur, GTMWaitLink);
		SetLatch(&proc->procLatch);
	}
	SpinLockRelease(lock);

	/* send TransactionIds to client */
	if (xid_cnt > 0)
	{
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
				for(i=0;i<xid_cnt;++i)
				{
					pq_sendint32(&output_buffer, xid[i]);
					if (msgtype == 'c')
					{
						append_client_xid_to_htab(client, xid[i]);
					}
				}
				output_buffer.cursor = true;
			}
			else
			{
				if (msgtype == 'c')
				{
					for(i=0;i<xid_cnt;++i)
					{
						append_client_xid_to_htab(client, xid[i]);
					}
				}
			}

			if (AppendMsgToClient(client, 'd', output_buffer.data, output_buffer.len, false) == false)
			{
				slist_delete_current(&siter);
				DropClient(client, false);
			}
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
	Assert(GetWaitEventData(wait_event_set, client->event_pos) == client);

	if (drop_in_slist)
		slist_delete(&slist_all_client, &client->snode);

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

static void OnPostmasterDeathEvent(WaitEvent *event)
{
	exit(1);
}

void OnListenEvent(WaitEvent *event)
{
	MemoryContext volatile oldcontext = CurrentMemoryContext;
	MemoryContext volatile newcontext = NULL;
	SnapClientData *client;
	Port			port;

	PG_TRY();
	{
		MemSet(&port, 0, sizeof(port));
		if (StreamConnection(event->fd, &port) != STATUS_OK)
		{
			if (port.sock != PGINVALID_SOCKET)
				StreamClose(port.sock);
			return;
		}

		newcontext = AllocSetContextCreate(TopMemoryContext,
										   "Snapshot sender client",
										   ALLOCSET_DEFAULT_SIZES);

		client = palloc0(sizeof(*client));
		client->context = newcontext;
		client->evd.fun = OnClientMsgEvent;
		client->node = pq_node_new(port.sock, false);
		client->last_msg = GetCurrentTimestamp();
		client->max_cnt = GetMaxSnapshotXidCount();
		client->xid = palloc(client->max_cnt * sizeof(TransactionId));
		client->cur_cnt = 0;
		slist_init(&(client->slist_xid));
		client->status = CLIENT_STATUS_CONNECTED;

		if (cur_wait_event == max_wait_event)
		{
			wait_event_set = EnlargeWaitEventSet(wait_event_set,
												 cur_wait_event + WAIT_EVENT_SIZE_STEP);
			max_wait_event += WAIT_EVENT_SIZE_STEP;
		}
		client->event_pos = AddWaitEventToSet(wait_event_set,
											  WL_SOCKET_READABLE,	/* waiting start pack */
											  port.sock,
											  NULL,
											  client);
		slist_push_head(&slist_all_client, &client->snode);

		MemoryContextSwitchTo(oldcontext);
	}PG_CATCH();
	{
		if (port.sock != PGINVALID_SOCKET)
			StreamClose(port.sock);

		MemoryContextSwitchTo(oldcontext);
		if (newcontext != NULL)
			MemoryContextDelete(newcontext);

		PG_RE_THROW();
	}PG_END_TRY();
}

static void OnClientMsgEvent(WaitEvent *event)
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
				OnClientRecvMsg(client, node);
		}
		if (event->events & (WL_SOCKET_WRITEABLE|WL_SOCKET_CONNECTED))
			OnClientSendMsg(client, node);

		if (pq_node_send_pending(node))
		{
			if ((event->events & (WL_SOCKET_WRITEABLE|WL_SOCKET_CONNECTED)) == 0)
				new_event = WL_SOCKET_WRITEABLE;
		}else if(client->status == CLIENT_STATUS_EXITING)
		{
			/* all data sended and exiting, close it */
			DropClient(client, true);
		}else
		{
			if ((event->events & WL_SOCKET_READABLE) == 0)
				new_event = WL_SOCKET_READABLE;
		}

		if (new_event != 0)
			ModifyWaitEvent(wait_event_set, event->pos, new_event, NULL);
	}PG_CATCH();
	{
		client->status = CLIENT_STATUS_EXITING;
		PG_RE_THROW();
	}PG_END_TRY();
}

static void OnClientRecvMsg(SnapClientData *client, pq_comm_node *node)
{
	int msgtype;

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
			SerializeActiveTransactionIds(&output_buffer);
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
	PG_SETMASK(&BlockSig);

	on_exit_reset();

	exit(2);
}

/* mutex must locked */
static void WaitSnapSendShmemSpace(volatile slock_t *mutex,
								   uint32 *cur,
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
		ereport(DEBUG2,(errmsg("SnapSend restart get 2pc left xid %d\n",
			 			xid)));
		SnapSender->xid_assign[SnapSender->cur_cnt_assign++] = xid;
	}
	SpinLockRelease(&SnapSender->mutex);
	list_free(xid_list);
	return;
}

void SnapSendTransactionAssign(TransactionId txid, TransactionId parent)
{
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
	if(SnapSender->cur_cnt_assign == MAX_CNT_SHMEM_XID_BUF)
		WaitSnapSendShmemSpace(&SnapSender->mutex,
							   &SnapSender->cur_cnt_assign,
							   &SnapSender->waiters_assign);
	Assert(SnapSender->cur_cnt_assign < MAX_CNT_SHMEM_XID_BUF);
	SnapSender->xid_assign[SnapSender->cur_cnt_assign++] = txid;
	SetLatch(&(GetPGProcByNumber(SnapSender->procno)->procLatch));
	SpinLockRelease(&SnapSender->mutex);
}

void SnapSendTransactionFinish(TransactionId txid)
{
	proclist_mutable_iter	iter;
	TimestampTz				endtime;
	bool 					in_list = false;
	int						procno = MyProc->pgprocno;

	if(!TransactionIdIsValid(txid) ||
		!IsGTMNode())
		return;

	Assert(TransactionIdIsNormal(txid));
	Assert(SnapSender != NULL);

	SpinLockAcquire(&SnapSender->mutex);
	if (SnapSender->procno == INVALID_PGPROCNO)
	{
		SpinLockRelease(&SnapSender->mutex);
		return;
	}
	if(SnapSender->cur_cnt_complete == MAX_CNT_SHMEM_XID_BUF)
		WaitSnapSendShmemSpace(&SnapSender->mutex,
							   &SnapSender->cur_cnt_complete,
							   &SnapSender->waiters_complete);
	Assert(SnapSender->cur_cnt_complete < MAX_CNT_SHMEM_XID_BUF);
	SnapSender->xid_complete[SnapSender->cur_cnt_complete++] = txid;
	SetLatch(&(GetPGProcByNumber(SnapSender->procno)->procLatch));

	/*proclist_foreach_modify(iter, &SnapSender->waiters_finish, GTMWaitLink)
	{
		if (iter.cur == procno)
		{
			in_list = true;
			break;
		}
	}
	if (!in_list)
	{
		MyProc->waitGlobalTransaction = txid;
		pg_write_barrier();
		proclist_push_tail(&SnapSender->waiters_finish, procno, GTMWaitLink);
	}

	endtime = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), 500);
	SnapSenderWaitTxidFinsihEvent(endtime, WaitSnapSendCondTransactionComplate, (void*)((size_t)txid));*/
	SpinLockRelease(&SnapSender->mutex);
}
