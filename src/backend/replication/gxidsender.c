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
#include "replication/gxidsender.h"
#include "replication/snapsender.h"
#include "replication/snapcommon.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/procarray.h"
#include "storage/proclist.h"
#include "storage/spin.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/varlena.h"

typedef struct GxidSenderData
{
	SnapCommonLock	comm_lock;
	pid_t			pid;				/* PID of currently active transsender process */
	int				procno;				/* proc number of current active transsender process */

	slock_t			mutex;				/* locks shared variables */
	pg_atomic_flag 	lock;				/* locks receive client sock */

	uint32			xcnt;
	TransactionId	latestCompletedXid;
	TransactionId	xip[MAX_BACKENDS];
}GxidSenderData;

typedef struct GxidWaitEventData
{
	void (*fun)(WaitEvent *event);
}GxidWaitEventData;

/* item in  slist_client */
typedef struct ClientHashItemInfo
{
	char			client_name[NAMEDATALEN];
	int				xcnt;
	slist_head		gxid_assgin_xid_list; 		/* xiditem list */
}ClientHashItemInfo;

/* item in  slist_client */
typedef struct ClientXidItemInfo
{
	slist_node		snode;
	TransactionId	xid;
	int				procno;
}ClientXidItemInfo;

typedef struct GxidClientData
{
	GxidWaitEventData	evd;
	slist_node			snode;
	MemoryContext		context;
	pq_comm_node   		*node;

	TimestampTz			last_msg;	/* last time of received message from client */
	ClientStatus		status;
	int					event_pos;
	char				client_name[NAMEDATALEN];
}GxidClientData;

/* GUC variables */
extern char *AGtmHost;
extern int AGtmPort;
extern int gxid_receiver_timeout;

static volatile sig_atomic_t gxid_send_got_sigterm = false;

static GxidSenderData	*GxidSender = NULL;
static slist_head		gxid_send_all_client = SLIST_STATIC_INIT(gxid_send_all_client);
static StringInfoData	gxid_send_output_buffer;
static StringInfoData	gxid_send_input_buffer;

static HTAB *gxidsender_xid_htab;
static WaitEventSet	   	*gxid_send_wait_event_set = NULL;
static WaitEvent	   	*gxid_send_wait_event = NULL;
static uint32			gxid_send_max_wait_event = 0;
static uint32			gxid_send_cur_wait_event = 0;
static int 				gxid_send_timeout = 0;

#define GXID_WAIT_EVENT_SIZE_STEP	64
#define GXID_WAIT_EVENT_SIZE_START	128

#define GXID_SENDER_MAX_LISTEN	16
static pgsocket	GxidSenderListenSocket[GXID_SENDER_MAX_LISTEN];

static void GxidSenderStartup(void);

/* event handlers */
static void GxidSenderOnLatchSetEvent(WaitEvent *event);
static void GxidSenderOnPostmasterDeathEvent(WaitEvent *event);
static void GxidSenderOnListenEvent(WaitEvent *event);
static void GxidSenderOnClientMsgEvent(WaitEvent *event);
static void GxidSenderOnClientRecvMsg(GxidClientData *client, pq_comm_node *node);
static void GxidSenderOnClientSendMsg(GxidClientData *client, pq_comm_node *node);
static void GxidSenderDropClient(GxidClientData *client, bool drop_in_slist);
static bool GxidSenderAppendMsgToClient(GxidClientData *client, char msgtype, const char *data, int len, bool drop_if_failed);
static void GxidProcessFinishGxid(GxidClientData *client);
static void GxidProcessAssignGxid(GxidClientData *client);
static void GxidProcessPreAssignGxidArray(GxidClientData *client);
static void GxidSendCheckTimeoutSocket(void);
static void GxidSenderClearOldXid(GxidClientData *client);
static void GxidDropXidItem(TransactionId xid);
static void GxidDropXidList(ClientHashItemInfo	*clientitem);

static void gxidsender_create_xid_htab(void);
typedef bool (*WaitGxidSenderCond)(void *context);
static const GxidWaitEventData GxidSenderLatchSetEventData = {GxidSenderOnLatchSetEvent};
static const GxidWaitEventData GxidSenderPostmasterDeathEventData = {GxidSenderOnPostmasterDeathEvent};
static const GxidWaitEventData GxidSenderListenEventData = {GxidSenderOnListenEvent};

/* Signal handlers */
static void GixdSenderSigUsr1Handler(SIGNAL_ARGS);
static void GxidSenderSigTermHandler(SIGNAL_ARGS);
static void GxidSenderQuickDieHander(SIGNAL_ARGS);

static void GxidSenderDie(int code, Datum arg)
{
	SpinLockAcquire(&GxidSender->mutex);
	Assert(GxidSender->pid == MyProc->pid);
	GxidSender->pid = 0;
	GxidSender->procno = INVALID_PGPROCNO;
	SpinLockRelease(&GxidSender->mutex);
}

Size GxidSenderShmemSize(void)
{
	return sizeof(GxidSenderData);
}

static void gxidsender_create_xid_htab(void)
{
	HASHCTL		hctl;

	hctl.keysize = NAMEDATALEN;
	hctl.entrysize = sizeof(ClientHashItemInfo);

	gxidsender_xid_htab = hash_create("hash GxidsenderXid", 128, &hctl, HASH_ELEM);
}

void GxidSenderShmemInit(void)
{
	Size		size = GxidSenderShmemSize();
	bool		found;

	GxidSender = (GxidSenderData*)ShmemInitStruct("Gxid Sender", size, &found);

	if (!found)
	{
		MemSet(GxidSender, 0, size);
		GxidSender->procno = INVALID_PGPROCNO;
		GxidSender->xcnt = 0;
		SpinLockInit(&GxidSender->mutex);
		pg_atomic_init_flag(&GxidSender->lock);
		GxidSender->comm_lock.handle_lock_info = DSM_HANDLE_INVALID;
		GxidSender->comm_lock.first_lock_info = InvalidDsaPointer;
		GxidSender->comm_lock.last_lock_info = InvalidDsaPointer;
		LWLockInitialize(&GxidSender->comm_lock.lock_lock_info, LWTRANCHE_SNAPSHOT_COMMON_DSA);
		LWLockInitialize(&GxidSender->comm_lock.lock_proc_link, LWTRANCHE_SNAPSHOT_COMMON_DSA);
	}
}

static void GxidSendCheckTimeoutSocket(void)
{
	TimestampTz				now;
	TimestampTz				timeout;
	slist_mutable_iter		siter;
	GxidClientData 			*client;

	now = GetCurrentTimestamp();
	slist_foreach_modify(siter, &gxid_send_all_client)
	{
		client = slist_container(GxidClientData, snode, siter.cur);
		if (client && (client->status == CLIENT_STATUS_STREAMING || client->status == CLIENT_STATUS_EXITING))
		{
			timeout = TimestampTzPlusMilliseconds(client->last_msg, gxid_send_timeout);
			if (client->status == CLIENT_STATUS_EXITING || now >= timeout)
			{
				slist_delete_current(&siter);
				GxidSenderDropClient(client, false);
			}
		}
	}
	return;
}

void GxidSenderMain(void)
{
	WaitEvent	   		*event;
	GxidWaitEventData	* volatile wed = NULL;
	sigjmp_buf			local_sigjmp_buf;
	int					rc;

	Assert(GxidSender != NULL);

	SpinLockAcquire(&GxidSender->mutex);
	if (GxidSender->pid != 0 ||
		GxidSender->procno != INVALID_PGPROCNO)
	{
		SpinLockRelease(&GxidSender->mutex);
		elog(PANIC, "gxidsender running in other process");
	}
	pg_memory_barrier();
	GxidSender->pid = MyProc->pid;
	GxidSender->procno = MyProc->pgprocno;
	SpinLockRelease(&GxidSender->mutex);
	gxid_send_timeout = gxid_receiver_timeout + 10000L;

	on_shmem_exit(GxidSenderDie, (Datum)0);

	/* make sure dsa_area create */
	(void)SnapGetLockArea(&GxidSender->comm_lock);

	/* release all last store lock and invalid msgs*/
	SnapReleaseAllTxidLocks(&GxidSender->comm_lock);

	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGHUP, SIG_IGN);
	pqsignal(SIGTERM, GxidSenderSigTermHandler);
	pqsignal(SIGQUIT, GxidSenderQuickDieHander);
	sigdelset(&BlockSig, SIGQUIT);
	pqsignal(SIGUSR1, SIG_IGN);
	pqsignal(SIGUSR1, GixdSenderSigUsr1Handler);
	pqsignal(SIGUSR2, SIG_IGN);

	PG_SETMASK(&UnBlockSig);

	GxidSenderStartup();
	Assert(GxidSenderListenSocket[0] != PGINVALID_SOCKET);
	Assert(gxid_send_wait_event_set != NULL);

	gxidsender_create_xid_htab();
	initStringInfo(&gxid_send_output_buffer);
	initStringInfo(&gxid_send_input_buffer);

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

		slist_foreach_modify(siter, &gxid_send_all_client)
		{
			GxidClientData *client = slist_container(GxidClientData, snode, siter.cur);
			if (socket_pq_node(client->node) == PGINVALID_SOCKET)
			{
				slist_delete_current(&siter);
				GxidSenderDropClient(client, false);
			}else if(pq_node_send_pending(client->node))
			{
				ModifyWaitEvent(gxid_send_wait_event_set, client->event_pos, WL_SOCKET_WRITEABLE, NULL);
			}else if(client->status == CLIENT_STATUS_EXITING)
			{
				/* no data sending and exiting, close it */
				slist_delete_current(&siter);
				GxidSenderDropClient(client, false);
			}
		}

		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();
	}
	PG_exception_stack = &local_sigjmp_buf;
	FrontendProtocol = PG_PROTOCOL_LATEST;
	whereToSendOutput = DestRemote;

	while(gxid_send_got_sigterm==false)
	{
		pq_switch_to_none();
		wed = NULL;
		rc = WaitEventSetWait(gxid_send_wait_event_set,
							  gxid_send_timeout,
							  gxid_send_wait_event,
							  gxid_send_cur_wait_event,
							  PG_WAIT_CLIENT);

		while(rc > 0)
		{
			event = &gxid_send_wait_event[--rc];
			wed = event->user_data;
			(*wed->fun)(event);
			pq_switch_to_none();
		}

		GxidSendCheckTimeoutSocket();
	}
	proc_exit(1);
}

static void GxidSenderStartup(void)
{
	/* check listen sockets */
	if (socket_gxid_pair[1] == PGINVALID_SOCKET)
		ereport(FATAL,
				(errmsg("no socket created for gxid listening")));

	gxid_send_wait_event_set = CreateWaitEventSet(TopMemoryContext, GXID_WAIT_EVENT_SIZE_START);
	gxid_send_wait_event = palloc0(GXID_WAIT_EVENT_SIZE_START * sizeof(WaitEvent));
	gxid_send_max_wait_event = GXID_WAIT_EVENT_SIZE_START;

	/* add latch */
	AddWaitEventToSet(gxid_send_wait_event_set,
					  WL_LATCH_SET,
					  PGINVALID_SOCKET,
					  &MyProc->procLatch,
					  (void*)&GxidSenderLatchSetEventData);
	++gxid_send_cur_wait_event;

	/* add postmaster death */
	AddWaitEventToSet(gxid_send_wait_event_set,
					  WL_POSTMASTER_DEATH,
					  PGINVALID_SOCKET,
					  NULL,
					  (void*)&GxidSenderPostmasterDeathEventData);
	++gxid_send_cur_wait_event;

	/* add listen sockets */
	Assert(gxid_send_cur_wait_event < gxid_send_max_wait_event);
	AddWaitEventToSet(gxid_send_wait_event_set,
						WL_SOCKET_READABLE,
						socket_gxid_pair[1],
						NULL,
						(void*)&GxidSenderListenEventData);
	++gxid_send_cur_wait_event;

	/* create a fake Port */
	MyProcPort = MemoryContextAllocZero(TopMemoryContext, sizeof(*MyProcPort));
	MyProcPort->remote_host = MemoryContextStrdup(TopMemoryContext, "gxid receiver");
	MyProcPort->remote_hostname = MyProcPort->remote_host;
	MyProcPort->database_name = MemoryContextStrdup(TopMemoryContext, "gxid sender");
	MyProcPort->user_name = MyProcPort->database_name;
	MyProcPort->SessionStartTime = GetCurrentTimestamp();
}

/* event handlers */
static void GxidSenderOnLatchSetEvent(WaitEvent *event)
{
	ResetLatch(&MyProc->procLatch);
}

/* must have lock already */
static void GxidDropXidItem(TransactionId xid)
{
	int count;
	int i;

	count = GxidSender->xcnt;
	for (i=0;i<count;++i)
	{
		if (GxidSender->xip[i] == xid)
		{
			memmove(&GxidSender->xip[i],
					&GxidSender->xip[i+1],
					(count-i-1) * sizeof(xid));
			if (TransactionIdPrecedes(GxidSender->latestCompletedXid, xid))
				GxidSender->latestCompletedXid = xid;
			--count;
			break;
		}
	}
	GxidSender->xcnt = count;
}

static void GxidDropXidList(ClientHashItemInfo	*clientitem)
{
	slist_mutable_iter	siter;
	ClientXidItemInfo	*xiditem;
	TransactionId		*xids;
	int					i, count;

	if (!slist_is_empty(&clientitem->gxid_assgin_xid_list))
	{
		xids = palloc0(clientitem->xcnt * sizeof(TransactionId));
		i = 0;
		count = 0;
		SpinLockAcquire(&GxidSender->mutex);
		slist_foreach_modify(siter, &clientitem->gxid_assgin_xid_list)
		{
			xiditem = slist_container(ClientXidItemInfo, snode, siter.cur);
			clientitem->xcnt--;
			xids[count++] = xiditem->xid;
			slist_delete(&clientitem->gxid_assgin_xid_list, &xiditem->snode);
			GxidDropXidItem(xiditem->xid);
			pfree(xiditem);
		}
		SpinLockRelease(&GxidSender->mutex);

		for (i = 0; i < count; i++)
		{
			SnapSendTransactionFinish(xids[i]);
			SnapReleaseTransactionLocks(&GxidSender->comm_lock, xids[i]);
		}
		pfree(xids);
	}
	Assert(clientitem->xcnt == 0);
}

/* must have lock already */
static void GxidDropXidClientItem(TransactionId xid, ClientHashItemInfo	*clientitem)
{
	slist_mutable_iter	siter;
	ClientXidItemInfo	*xiditem;
	//bool found = false;

	slist_foreach_modify(siter, &clientitem->gxid_assgin_xid_list)
	{
		xiditem = slist_container(ClientXidItemInfo, snode, siter.cur);
		if (xiditem->xid == xid)
		{
			//found = true;
			clientitem->xcnt--;
			slist_delete(&clientitem->gxid_assgin_xid_list, &xiditem->snode);
			pfree(xiditem);
		}
	}
	//Assert(found);
	Assert(clientitem->xcnt >= 0);
}

static void GxidSenderDropClient(GxidClientData *client, bool drop_in_slist)
{
	slist_iter 				siter;
	ClientHashItemInfo		*clientitem;
	bool					found;

	pgsocket socket_fd = socket_pq_node(client->node);
	int pos = client->event_pos;
	Assert(GetWaitEventData(gxid_send_wait_event_set, client->event_pos) == client);

	if (drop_in_slist)
		slist_delete(&gxid_send_all_client, &client->snode);

	RemoveWaitEvent(gxid_send_wait_event_set, client->event_pos);
	gxid_send_cur_wait_event--;

	pq_node_close(client->node);
	MemoryContextDelete(client->context);
	if (socket_fd != PGINVALID_SOCKET)
		StreamClose(socket_fd);

	clientitem = hash_search(gxidsender_xid_htab, client->client_name, HASH_REMOVE, &found);
	if(found)
		GxidDropXidList(clientitem);

	slist_foreach(siter, &gxid_send_all_client)
	{
		client = slist_container(GxidClientData, snode, siter.cur);
		if (client->event_pos > pos)
			--client->event_pos;
	}
}

static bool GxidSenderAppendMsgToClient(GxidClientData *client, char msgtype, const char *data, int len, bool drop_if_failed)
{
	pq_comm_node *node = client->node;
	bool old_send_pending = pq_node_send_pending(node);
	Assert(GetWaitEventData(gxid_send_wait_event_set, client->event_pos) == client);

	pq_node_putmessage_noblock_sock(node, msgtype, data, len);
	if (old_send_pending == false)
	{
		if (pq_node_flush_if_writable_sock(node) != 0)
		{
			if (drop_if_failed)
				GxidSenderDropClient(client, true);
			return false;
		}

		if (pq_node_send_pending(node))
		{
			ModifyWaitEvent(gxid_send_wait_event_set,
							client->event_pos,
							WL_SOCKET_WRITEABLE,
							NULL);
		}
		client->last_msg = GetCurrentTimestamp();
	}

	return true;
}

static void GxidSenderOnPostmasterDeathEvent(WaitEvent *event)
{
	exit(1);
}

void GxidSenderOnListenEvent(WaitEvent *event)
{
	MemoryContext volatile oldcontext = CurrentMemoryContext;
	MemoryContext volatile newcontext = NULL;
	GxidClientData *client;
	int				client_fdsock;

	if(pool_recvfds(event->fd, &client_fdsock, 1) != 0)
	{
		ereport(WARNING, (errmsg("receive client socke failed:%m\n")));
		return;
	}

	PG_TRY();
	{
		newcontext = AllocSetContextCreate(TopMemoryContext,
										   "gxid sender client",
										   ALLOCSET_DEFAULT_SIZES);

		client = palloc0(sizeof(*client));
		client->context = newcontext;
		client->evd.fun = GxidSenderOnClientMsgEvent;
		client->node = pq_node_new(client_fdsock, false);
		client->last_msg = GetCurrentTimestamp();
		client->status = CLIENT_STATUS_CONNECTED;

		if (gxid_send_cur_wait_event == gxid_send_max_wait_event)
		{
			gxid_send_wait_event_set = EnlargeWaitEventSet(gxid_send_wait_event_set,
												gxid_send_cur_wait_event + GXID_WAIT_EVENT_SIZE_STEP);
			gxid_send_max_wait_event += GXID_WAIT_EVENT_SIZE_STEP;
			gxid_send_wait_event = repalloc(gxid_send_wait_event, gxid_send_max_wait_event * sizeof(WaitEvent));
		}
		client->event_pos = AddWaitEventToSet(gxid_send_wait_event_set,
											  WL_SOCKET_READABLE,	/* waiting start pack */
											  client_fdsock,
											  NULL,
											  client);
		++gxid_send_cur_wait_event;
		slist_push_head(&gxid_send_all_client, &client->snode);

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

static void GxidSenderOnClientMsgEvent(WaitEvent *event)
{
	GxidClientData *volatile client = event->user_data;
	pq_comm_node   *node;
	uint32			new_event;

	Assert(GetWaitEventData(gxid_send_wait_event_set, client->event_pos) == client);

	PG_TRY();
	{
		node = client->node;
		new_event = 0;
		pq_node_switch_to(node);

		if (event->events & WL_SOCKET_READABLE)
		{
			if (client->status == CLIENT_STATUS_EXITING)
				ModifyWaitEvent(gxid_send_wait_event_set, event->pos, 0, NULL);
			else
				GxidSenderOnClientRecvMsg(client, node);
		}
		if (event->events & (WL_SOCKET_WRITEABLE|WL_SOCKET_CONNECTED))
			GxidSenderOnClientSendMsg(client, node);

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
			ModifyWaitEvent(gxid_send_wait_event_set, event->pos, new_event, NULL);

		/* all data sended and exiting, close it */
		if (client->status == CLIENT_STATUS_EXITING)
			GxidSenderDropClient(client, true);
	}PG_CATCH();
	{
		client->status = CLIENT_STATUS_EXITING;
		PG_RE_THROW();
	}PG_END_TRY();
}

static void GxidSenderClearOldXid(GxidClientData *client)
{
	ClientHashItemInfo			*clientitem;
	bool						found;
	int 						sscan_ret;

	sscan_ret = sscanf(gxid_send_input_buffer.data, "%*s %*s \"%[^\" ]\" %*s", client->client_name);

	if (sscan_ret <= 0)
		return;

	clientitem = hash_search(gxidsender_xid_htab, client->client_name, HASH_REMOVE, &found);
	if(found == false)
		return;

	GxidDropXidList(clientitem);
}

static void GxidProcessAssignGxid(GxidClientData *client)
{
	int							procno;
	TransactionId				xid;
	slist_mutable_iter			siter;
	ClientHashItemInfo			*clientitem;
	ClientXidItemInfo			*xiditem;
	bool						found;
	slist_head					xid_slist =  SLIST_STATIC_INIT(xid_slist);

	clientitem = hash_search(gxidsender_xid_htab, client->client_name, HASH_ENTER, &found);
	if(found == false)
	{
		MemSet(clientitem, 0, sizeof(*clientitem));
		memcpy(clientitem->client_name, client->client_name, NAMEDATALEN);
		slist_init(&(clientitem->gxid_assgin_xid_list));
		clientitem->xcnt = 0;
	}

	resetStringInfo(&gxid_send_output_buffer);
	pq_sendbyte(&gxid_send_output_buffer, 'a');

	while(gxid_send_input_buffer.cursor < gxid_send_input_buffer.len)
	{
		procno = pq_getmsgint(&gxid_send_input_buffer, sizeof(procno));
		xid = GetNewTransactionIdExt(false, 1, false);

		xiditem = palloc0(sizeof(*xiditem));
		xiditem->procno = procno;
		xiditem->xid = xid;
		slist_push_head(&clientitem->gxid_assgin_xid_list, &xiditem->snode);
		clientitem->xcnt++;

		pq_sendint32(&gxid_send_output_buffer, procno);
		pq_sendint32(&gxid_send_output_buffer, xid);

		xiditem = palloc0(sizeof(*xiditem));
		xiditem->xid = xid;
		slist_push_head(&xid_slist, &xiditem->snode);
#ifdef SNAP_SYNC_DEBUG
		ereport(LOG,(errmsg("GxidSend assging xid %d to %d\n",
			 			xid, procno)));
#endif
	}

	if (GxidSenderAppendMsgToClient(client, 'd', gxid_send_output_buffer.data, gxid_send_output_buffer.len, false) == false)
	{
		client->status = CLIENT_STATUS_EXITING;
		//GxidSenderDropClient(client, true);
	}
	else
	{
		SpinLockAcquire(&GxidSender->mutex);
		slist_foreach_modify(siter, &xid_slist)
		{
			xiditem = slist_container(ClientXidItemInfo, snode, siter.cur);
			GxidSender->xip[GxidSender->xcnt++] = xiditem->xid;
			slist_delete(&xid_slist, &xiditem->snode);
			pfree(xiditem);
		}
		SpinLockRelease(&GxidSender->mutex);
	}
}

static void GxidProcessPreAssignGxidArray(GxidClientData *client)
{
	TransactionId				xid, xidmax; 
	ClientHashItemInfo			*clientitem;
	ClientXidItemInfo			**xiditem;
	bool						found;
	int							i, xid_num;

	xid_num = pq_getmsgint(&gxid_send_input_buffer, sizeof(xid_num));
	Assert(xid_num > 0 && xid_num <= MAX_XID_PRE_ALLOC_NUM);

	clientitem = hash_search(gxidsender_xid_htab, client->client_name, HASH_ENTER, &found);
	if(found == false)
	{
		MemSet(clientitem, 0, sizeof(*clientitem));
		memcpy(clientitem->client_name, client->client_name, NAMEDATALEN);
		clientitem->xcnt = 0;
		slist_init(&clientitem->gxid_assgin_xid_list);
	}

	resetStringInfo(&gxid_send_output_buffer);
	pq_sendbyte(&gxid_send_output_buffer, 'q');
	pq_sendint32(&gxid_send_output_buffer, xid_num);

	xiditem = palloc0(xid_num * sizeof(ClientXidItemInfo*));
	for (i = 0; i < xid_num; i++)
	{
		xiditem[i] = palloc0(sizeof(ClientXidItemInfo));
	}

#ifdef SNAP_SYNC_DEBUG
	ereport(LOG,(errmsg("GxidSend assging xid for %s\n", client->client_name)));
#endif

	xidmax = GetNewTransactionIdExt(false, xid_num, false);

	SpinLockAcquire(&GxidSender->mutex);
	for (i = 0; i < xid_num; i++)
	{
		xid = xidmax - xid_num + i + 1;
#ifdef SNAP_SYNC_DEBUG
		if (i == 0)
			ereport(LOG,(errmsg(" %d --\n", xid)));
		else if (i == xid_num-1)
			ereport(LOG,(errmsg(" %d\n", xid)));
#endif
		xiditem[i]->xid = xid;
		xiditem[i]->procno = 0;
		slist_push_head(&clientitem->gxid_assgin_xid_list, &xiditem[i]->snode);
		clientitem->xcnt++;
		pq_sendint32(&gxid_send_output_buffer, xid);

		GxidSender->xip[GxidSender->xcnt++] = xid;
	}

	SpinLockRelease(&GxidSender->mutex);

	pfree(xiditem);
	if (GxidSenderAppendMsgToClient(client, 'd', gxid_send_output_buffer.data, gxid_send_output_buffer.len, false) == false)
	{
		client->status = CLIENT_STATUS_EXITING;
	}
}

static void GxidProcessFinishGxid(GxidClientData *client)
{
	int							procno, start_cursor;
	TransactionId				xid; 
	ClientHashItemInfo			*clientitem;
	bool						found;
	size_t						input_buf_free_len, out_buf_free_len;

	clientitem = hash_search(gxidsender_xid_htab, client->client_name, HASH_FIND, &found);
	Assert(found);

	resetStringInfo(&gxid_send_output_buffer);
	pq_sendbyte(&gxid_send_output_buffer, 'f');

	start_cursor = gxid_send_input_buffer.cursor;

re_lock_:
	input_buf_free_len = gxid_send_input_buffer.maxlen - gxid_send_input_buffer.len;
	out_buf_free_len = gxid_send_output_buffer.maxlen - gxid_send_output_buffer.len;
	SpinLockAcquire(&GxidSender->mutex);

	if (input_buf_free_len > out_buf_free_len)
	{
		SpinLockRelease(&GxidSender->mutex);
		enlargeStringInfo(&gxid_send_output_buffer, input_buf_free_len - out_buf_free_len);
		goto re_lock_;
	}

	while(gxid_send_input_buffer.cursor < gxid_send_input_buffer.len)
	{
		procno = pq_getmsgint(&gxid_send_input_buffer, sizeof(procno));
		xid = pq_getmsgint(&gxid_send_input_buffer, sizeof(xid));

		pq_sendint32(&gxid_send_output_buffer, procno);
		pq_sendint32(&gxid_send_output_buffer, xid);
#ifdef SNAP_SYNC_DEBUG
		found = IsXidInPreparedState(xid);
		/* comman commite, xid should not left in Prepared*/
		Assert(!found);
#endif
		GxidDropXidClientItem(xid, clientitem);
		GxidDropXidItem(xid);
#ifdef SNAP_SYNC_DEBUG
		ereport(LOG,(errmsg("GxidSend finish xid %d for client %s\n",
			 			xid, clientitem->client_name)));
#endif
	}
	SpinLockRelease(&GxidSender->mutex);

	gxid_send_input_buffer.cursor = start_cursor;
	while(gxid_send_input_buffer.cursor < gxid_send_input_buffer.len)
	{
		procno = pq_getmsgint(&gxid_send_input_buffer, sizeof(procno));
		xid = pq_getmsgint(&gxid_send_input_buffer, sizeof(xid));
		SnapSendTransactionFinish(xid);
		SnapReleaseTransactionLocks(&GxidSender->comm_lock, xid);
	}

	if (GxidSenderAppendMsgToClient(client, 'd', gxid_send_output_buffer.data, gxid_send_output_buffer.len, false) == false)
	{
		client->status = CLIENT_STATUS_EXITING;
	}
}

static void GxidSenderOnClientRecvMsg(GxidClientData *client, pq_comm_node *node)
{
	int msgtype;
	int cmdtype;
	const char* client_name;

	if (pq_node_recvbuf(node) != 0)
	{
		ereport(ERROR,
				(errmsg("client closed stream")));
	}

	client->last_msg = GetCurrentTimestamp();
	while(1)
	{
		resetStringInfo(&gxid_send_input_buffer);
		msgtype = pq_node_get_msg(&gxid_send_input_buffer, node);
		switch(msgtype)
		{
		case 'Q':
			/* only support "START_REPLICATION" command */
			if (strncasecmp(gxid_send_input_buffer.data, "START_REPLICATION", strlen("START_REPLICATION")) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						errposition(0),
						errmsg("only support \"START_REPLICATION SLOT slot_name 0/0 TIMELINE 0\" command")));

			/* Send a CopyBothResponse message, and start streaming */
			resetStringInfo(&gxid_send_output_buffer);
			pq_sendbyte(&gxid_send_output_buffer, 0);
			pq_sendint16(&gxid_send_output_buffer, 0);
			GxidSenderAppendMsgToClient(client, 'W', gxid_send_output_buffer.data, gxid_send_output_buffer.len, false);

			GxidSenderClearOldXid(client);

			/* send streaming start */
			resetStringInfo(&gxid_send_output_buffer);
			appendStringInfoChar(&gxid_send_output_buffer, 's');
			GxidSenderAppendMsgToClient(client, 'd', gxid_send_output_buffer.data, gxid_send_output_buffer.len, false);
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
				cmdtype = pq_getmsgbyte(&gxid_send_input_buffer);
				if (cmdtype == 'g') /* assing one xid */
				{
					client_name = pq_getmsgstring(&gxid_send_input_buffer);
					memcpy(client->client_name, client_name, NAMEDATALEN);
					GxidProcessAssignGxid(client);
				}
				else if (cmdtype == 'p') /* pre-alloc xid array */
				{
					client_name = pq_getmsgstring(&gxid_send_input_buffer);
					memcpy(client->client_name, client_name, NAMEDATALEN);
					GxidProcessPreAssignGxidArray(client);
				}
				else if (cmdtype == 'c')
				{
					client_name = pq_getmsgstring(&gxid_send_input_buffer);
					memcpy(client->client_name, client_name, NAMEDATALEN);
					GxidProcessFinishGxid(client);
				}
				else if (cmdtype == 'h')
				{
					/* Send a HEARTBEAT Response message */
					resetStringInfo(&gxid_send_output_buffer);
					appendStringInfoChar(&gxid_send_output_buffer, 'h');
					if (GxidSenderAppendMsgToClient(client, 'd', gxid_send_output_buffer.data, gxid_send_output_buffer.len, false) == false)
					{
						client->status = CLIENT_STATUS_EXITING;
					}
				}
				else
					ereport(LOG,(errmsg("GxidSend recv unknow data %s\n", gxid_send_input_buffer.data)));
			}
			break;
		case 0:
			return;
		default:
			ereport(LOG,(errmsg("GxidSend recv unknow msgtype %d\n", msgtype)));
			break;
		}
		
	}
}

static void GxidSenderOnClientSendMsg(GxidClientData *client, pq_comm_node *node)
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
static void GixdSenderSigUsr1Handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	latch_sigusr1_handler();

	errno = save_errno;
}

static void GxidSenderSigTermHandler(SIGNAL_ARGS)
{
	gxid_send_got_sigterm = true;
}

static void GxidSenderQuickDieHander(SIGNAL_ARGS)
{
	if (proc_exit_inprogress)
		return;

	proc_exit_inprogress = true;

	PG_SETMASK(&BlockSig);

	on_exit_reset();

	exit(2);
}

Snapshot GxidSenderGetSnapshot(Snapshot snap, TransactionId *xminOld, TransactionId* xmaxOld,
			int *countOld)
{
	TransactionId	xid,xmax,xmin;
	uint32			i,xcnt;

	if (snap->xip == NULL)
		EnlargeSnapshotXip(snap, GetMaxSnapshotXidCount());

	SpinLockAcquire(&GxidSender->mutex);

	if (!TransactionIdIsNormal(GxidSender->latestCompletedXid))
	{
		SpinLockRelease(&GxidSender->mutex);
		return snap;
	}

	if (GxidSender->xcnt > 0 && (GxidSender->xcnt + GetMaxSnapshotXidCount()) > snap->max_xcnt)
		EnlargeSnapshotXip(snap, GxidSender->xcnt + GetMaxSnapshotXidCount());

	xmax = GxidSender->latestCompletedXid;
	Assert(TransactionIdIsNormal(xmax));
	TransactionIdAdvance(xmax);

	xmin = xmax;
	xcnt = *countOld;
	if (NormalTransactionIdFollows(xmax, *xmaxOld))
		*xmaxOld = xmax;

	for (i = 0; i < GxidSender->xcnt; ++i)
	{
		xid = GxidSender->xip[i];

		if (NormalTransactionIdPrecedes(xid, xmin))
			xmin = xid;

		/* if XID is >= xmax, we can skip it */
		if (!NormalTransactionIdPrecedes(xid, *xmaxOld))
			continue;
		
		/* Add XID to snapshot. */
		snap->xip[xcnt++] = xid;
	}

	*countOld = xcnt;
	if ((GxidSender->xcnt > 0 && NormalTransactionIdPrecedes(xmin, *xminOld))
			|| (*countOld == 0 && *xmaxOld == xmax))
		*xminOld = xmin;
	SpinLockRelease(&GxidSender->mutex);

#ifdef USE_ASSERT_CHECKING
	for(i=0;i<xcnt;++i)
	{
		Assert(!NormalTransactionIdFollows(*xminOld, snap->xip[i]));
	}
#endif /* USE_ASSERT_CHECKING */

	return snap;
}

TransactionId *GxidSenderGetAllXip(uint32 *cnt_num)
{
	TransactionId	*assign_xids;
	uint32			assign_len;

	assign_len = XID_ARRAY_STEP_SIZE;
	assign_xids = NULL;

re_lock_:
	if (!assign_xids)
		assign_xids = palloc0(sizeof(TransactionId) * assign_len);
	else
		assign_xids = repalloc(assign_xids, sizeof(TransactionId) * assign_len);

	SpinLockAcquire(&GxidSender->mutex);

	if (assign_len <  GxidSender->xcnt)
	{
		SpinLockRelease(&GxidSender->mutex);
		assign_len += XID_ARRAY_STEP_SIZE;
		goto re_lock_;
	}

	assign_len = GxidSender->xcnt;
	if (assign_len > 0)
		memcpy(assign_xids, GxidSender->xip, sizeof(TransactionId)*assign_len);
	SpinLockRelease(&GxidSender->mutex);

	*cnt_num = assign_len;
	return assign_xids;
}

void GxidSendLockSendSock(void)
{
re_lock_:
	if (!GxidSender )
	{
		pg_usleep(100000L);
		goto re_lock_;
	}

	if (!pg_atomic_test_set_flag(&GxidSender->lock))
	{
		pg_usleep(100000L);
		goto re_lock_;
	}
}

void GxidSendUnlockSendSock(void)
{
	pg_atomic_clear_flag(&GxidSender->lock);
}

void GxidSenderTransferLock(void **param, TransactionId xid, struct PGPROC *from)
{
	SnapTransferLock(&GxidSender->comm_lock, param, xid, from);
}


void GxidSenderGetStat(StringInfo buf)
{
	int				i;
	TransactionId	*assign_xids;
	uint32			assign_len;

	assign_len = XID_ARRAY_STEP_SIZE;
	assign_xids = NULL;

re_lock_:
	if (!assign_xids)
		assign_xids = palloc0(sizeof(TransactionId) * assign_len);
	else
		assign_xids = repalloc(assign_xids, sizeof(TransactionId) * assign_len);
	
	SpinLockAcquire(&GxidSender->mutex);

	if (assign_len <  GxidSender->xcnt)
	{
		SpinLockRelease(&GxidSender->mutex);
		assign_len += XID_ARRAY_STEP_SIZE;
		goto re_lock_;
	}

	assign_len = GxidSender->xcnt;
	for (i = 0; i < GxidSender->xcnt; i++)
	{
		assign_xids[i] = GxidSender->xip[i];
		if (i > 0 && i % XID_PRINT_XID_LINE_NUM == 0)
			appendStringInfo(buf, "\n  ");
	}
	SpinLockRelease(&GxidSender->mutex);

	appendStringInfo(buf, " xcnt: %u \n", assign_len);
	appendStringInfo(buf, "  xid_assign: [");

	qsort(assign_xids, assign_len, sizeof(TransactionId), xidComparator);
	for (i = 0; i < assign_len; i++)
	{
		appendStringInfo(buf, "%u ", assign_xids[i]);
		if (i > 0 && i % XID_PRINT_XID_LINE_NUM == 0)
			appendStringInfo(buf, "\n  ");
	}
	appendStringInfo(buf, "]");
	pfree(assign_xids);
}