#include "postgres.h"
#include "postmaster/bgworker.h"
#include "postmaster/autovacuum.h"

#include "access/transam.h"
#include "access/twophase.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/rxact_mgr.h"
#include "pgstat.h"
#include "lib/ilist.h"
#include "libpq/libpq.h"
#include "libpq/pqcomm.h"
#include "libpq/pqformat.h"
#include "libpq/pqnode.h"
#include "libpq/pqnone.h"
#include "libpq/pqsignal.h"
#include "libpq/pqmq.h"
#include "miscadmin.h"
#include "pgxc/pgxc.h"
#include "pgxc/poolcomm.h"
#include "pgxc/nodemgr.h"
#include "replication/snapsender.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/procarray.h"
#include "storage/proclist.h"
#include "storage/spin.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "storage/condition_variable.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/varlena.h"
#include "utils/hsearch.h"
#include "utils/snapmgr.h"
#include "intercomm/inter-node.h"
#include "catalog/pgxc_node.h"
#include "catalog/indexing.h"

#define	MAX_CNT_SHMEM_XID_BUF	1024
#include "postmaster/postmaster.h"

#define SNAPSENDER_STATE_STOPED 0
#define SNAPSENDER_STATE_STARTING 1
#define SNAPSENDER_STATE_OK 2

#define SNAPSENDER_ALL_DNMASTER_CONN_NOT_OK 0
#define SNAPSENDER_ALL_DNMASTER_CONN_OK 1

typedef struct SnapSenderData
{
	proclist_head	waiters_assign;		/* list of waiting event space of xid_assign */
	proclist_head	waiters_complete;	/* list of waiting event space of xid_complete */
	proclist_head	waiters_finish;		/* list of waiting event xid finish ack */
	SnapCommonLock	comm_lock;
	pid_t			pid;				/* PID of currently active snapsender process */
	int				procno;				/* proc number of current active snapsender process */
 			
	slock_t			mutex;				/* locks shared variables */
	slock_t			gxid_mutex;			/* locks shared variables */
	pg_atomic_flag	lock;				/* locks receive client sock */

	ConditionVariable 		cv;
	ConditionVariable 		cv_dn_con;
	pg_atomic_uint32		state;
	pg_atomic_uint32		dn_conn_state;
	pg_atomic_uint32		nextid_upcount;
	pg_atomic_uint32		nextid_upcount_cn;
	pg_atomic_uint32		global_xmin;
	pg_atomic_uint32		global_finish_id;
	volatile uint32			cur_cnt_assign;
	TransactionId			xid_assign[MAX_CNT_SHMEM_XID_BUF];

	volatile uint32			cur_cnt_complete;
	TransactionId			xid_complete[MAX_CNT_SHMEM_XID_BUF];

	uint32			xcnt;
	TransactionId	latestCompletedXid;
	TransactionId	xip[MAX_BACKENDS];
}SnapSenderData;

typedef struct WaitEventData
{
	void (*fun)(WaitEvent *event, time_t* time_last_latch);
}WaitEventData;

typedef struct SnapClientData
{
	WaitEventData	evd;
	slist_node		snode;
	MemoryContext	context;
	pq_comm_node	*node;
	bool			is_dn;

	TransactionId  *xid;		/* current transaction count of synchronizing */
	uint32			cur_cnt;
	uint32			max_cnt;

	TimestampTz		last_msg;	/* last time of received message from client */
	ClientStatus	status;
	int				event_pos;
	TransactionId	global_xmin;
	char			client_name[NAMEDATALEN];
}SnapClientData;

typedef struct SnapCnClientHoldData
{
	slist_node		snode;
	TransactionId	*xid;		/* current transaction count of synchronizing */
	uint32			cur_cnt;
	char			client_name[NAMEDATALEN];
}SnapCnClientHoldData;

typedef enum SnapSenderXidArrayType
{
	SNAPSENDER_XID_ARRAY_XACT2P = 1,
	SNAPSENDER_XID_ARRAY_ASSIGN = 2,
	SNAPSENDER_XID_ARRAY_FINISH = 3
}SnapSenderXidArrayType;

/* GUC variables */
extern bool is_need_check_dn_coon;

bool adb_check_sync_nextid = true;
int force_snapshot_consistent = FORCE_SNAP_CON_SESSION;
int snapshot_sync_waittime = 10000;
int snap_restart_timeout = 300000;
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_SIGHUP = false;

static SnapSenderData  *SnapSender = NULL;
static slist_head		slist_all_client = SLIST_STATIC_INIT(slist_all_client);
static slist_head		slist_cn_failed_client = SLIST_STATIC_INIT(slist_cn_failed_client);

/* store assing xid should send to the other nodes*/
static TransactionId	*assign_xid_array;
static uint32			assign_xid_list_len;
static uint32			assign_xid_list_max;

/* store finish xid should send to the other nodes*/
static TransactionId	*finish_xid_array;
static uint32			finish_xid_list_len;
static uint32			finish_xid_list_max;

/* reserve rxact and left two-phase xid*/
static TransactionId	*xid_xact2pc_array;
static uint32			xid_xact2pc_count;
static uint32			xid_xact2pc_max;

static StringInfoData	output_buffer;
static StringInfoData	input_buffer;

static WaitEventSet	   *wait_event_set = NULL;
static WaitEvent	   *wait_event = NULL;
static uint32			max_wait_event = 0;
static uint32			cur_wait_event = 0;
static int snap_send_timeout = 0;
static List	*dn_master_name_list = NIL;
static List	*cn_master_name_list = NIL;
static TimestampTz start_time = 0;
static bool is_snapsender_query_worker = false;
#define WAIT_EVENT_SIZE_STEP	64
#define WAIT_EVENT_SIZE_START	128

#define SNAP_SENDER_MAX_LISTEN	16

static void SnapSenderStartup(void);

/* event handlers */
static void OnLatchSetEvent(WaitEvent *event, time_t* time_last_latch);
static void OnPostmasterDeathEvent(WaitEvent *event, time_t* time_last_latch);
static void OnListenEvent(WaitEvent *event, time_t* time_last_latch);
static void OnClientMsgEvent(WaitEvent *event, time_t* time_last_latch);
static void OnClientRecvMsg(SnapClientData *client, pq_comm_node *node, time_t* time_last_latch);
static void OnClientSendMsg(SnapClientData *client, pq_comm_node *node);

static void ProcessShmemXidMsg(TransactionId *xid, const uint32 xid_cnt, char msgtype);
static void DropClient(SnapClientData *client, bool drop_in_slist);
static bool AppendMsgToClient(SnapClientData *client, char msgtype, const char *data, int len);

typedef bool (*WaitSnapSenderCond)(void *context);
static const WaitEventData LatchSetEventData = {OnLatchSetEvent};
static const WaitEventData PostmasterDeathEventData = {OnPostmasterDeathEvent};
static const WaitEventData ListenEventData = {OnListenEvent};
static void SnapSendCheckTimeoutSocket(void);
static void SnapSendCheckCnDnInitSyncWait(void);
static void snapsenderUpdateNextXid(TransactionId xid, SnapClientData *exclue_client);
static void SnapSenderSigHupHandler(SIGNAL_ARGS);
static TransactionId snapsenderGetSenderGlobalXmin(void);

/* Signal handlers */
static void SnapSenderQuickDieHander(SIGNAL_ARGS);

static bool isSnapSenderAllCnDnConnOk(bool is_wait);
static void WakeAllCnClientStream(void);
static void SnapSenderInitXidArray(SnapSenderXidArrayType ssxat);
static void SnapSenderFreeXidArray(SnapSenderXidArrayType ssxat);
static void SnapSenderXidArrayAddXid(SnapSenderXidArrayType ssxat, TransactionId xid);
static void SnapSenderXidArrayRemoveXid(SnapSenderXidArrayType ssxat, TransactionId xid);
static void SnapSenderDropXidList(SnapClientData *client, const TransactionId *cn_txids, const int txids_count);

static void SnapSenderTransferCnClientToFailledList(SnapClientData *client)
{
	if (client->is_dn)
	{
		SnapSenderDropXidList(client, NULL, 0);
	}
	else /* for cn client*/
	{
		if (client->cur_cnt > 0) /* when cn has assinged some xid, we should transfer and hold the xid*/
		{
			SnapCnClientHoldData *SnapCnData;
			SnapCnData = palloc0(sizeof(*SnapCnData));
			SnapCnData->cur_cnt = client->cur_cnt;
			memcpy(SnapCnData->client_name, client->client_name, NAMEDATALEN);
			SnapCnData->xid = palloc(client->cur_cnt * sizeof(TransactionId));
			memcpy(SnapCnData->xid, client->xid, sizeof(client->xid[0]) * (SnapCnData->cur_cnt));
			slist_push_head(&slist_cn_failed_client, &SnapCnData->snode);
		}
	}
}

static bool SnapSenderRecoveryXidListFromCnFailledList(SnapClientData *client)
{
	slist_mutable_iter		siter;
	SnapCnClientHoldData	*SnapCnData;
	bool					ret = false;

	slist_foreach_modify(siter, &slist_cn_failed_client)
	{
		SnapCnData = slist_container(SnapCnClientHoldData, snode, siter.cur);
		if(!strcasecmp(SnapCnData->client_name, client->client_name))
		{
			memcpy(client->xid, SnapCnData->xid, sizeof(SnapCnData->xid[0]) * (SnapCnData->cur_cnt));
			client->cur_cnt = SnapCnData->cur_cnt;
			slist_delete(&slist_cn_failed_client, &SnapCnData->snode);
			pfree(SnapCnData->xid);
			pfree(SnapCnData);
			ret = true;
			break;
		}
	}

	return ret;
}

static void SnapSenderDie(int code, Datum arg)
{
	SpinLockAcquire(&SnapSender->mutex);
	Assert(SnapSender->pid == MyProc->pid);
	SnapSender->pid = 0;
	SnapSender->procno = INVALID_PGPROCNO;
	SpinLockRelease(&SnapSender->mutex);
	pg_atomic_init_u32(&SnapSender->nextid_upcount, 0);
	pg_atomic_init_u32(&SnapSender->nextid_upcount_cn, 0);
	pg_atomic_init_u32(&SnapSender->state, SNAPSENDER_STATE_STOPED);
	pg_atomic_init_u32(&SnapSender->dn_conn_state, SNAPSENDER_ALL_DNMASTER_CONN_NOT_OK);

	SnapSenderFreeXidArray(SNAPSENDER_XID_ARRAY_XACT2P);
	SnapSenderFreeXidArray(SNAPSENDER_XID_ARRAY_ASSIGN);
	SnapSenderFreeXidArray(SNAPSENDER_XID_ARRAY_FINISH);
}

static void SnapSenderClientAddXid(SnapClientData *client, TransactionId xid)
{
	client->xid[client->cur_cnt++] = xid;
	Assert(client->cur_cnt <= client->max_cnt);
}

static bool SnapSenderClientRemoveXid(SnapClientData *client, TransactionId xid)
{
	int i;
	int count = client->cur_cnt;
	bool found = false;
	for (i = 0 ;i < count; i++)
	{
		if (client->xid[i] == xid)
		{
			memmove(&client->xid[i],
					&client->xid[i+1],
					(count-i-1) * sizeof(xid));
			--count;
			found = true;
			break;
		}
	}
	client->cur_cnt = count;
	return found;
}

static void SnapSenderInitXidArray(SnapSenderXidArrayType ssxat)
{
	if (ssxat == SNAPSENDER_XID_ARRAY_XACT2P)
	{
		xid_xact2pc_max = XID_ARRAY_STEP_SIZE;
		xid_xact2pc_count = 0;
		xid_xact2pc_array = palloc0(sizeof(TransactionId) * xid_xact2pc_max);
	}
	else if (ssxat == SNAPSENDER_XID_ARRAY_ASSIGN)
	{
		assign_xid_list_max = XID_ARRAY_STEP_SIZE;
		assign_xid_list_len = 0;
		assign_xid_array = palloc0(sizeof(TransactionId) * assign_xid_list_max);
	}
	else if(ssxat == SNAPSENDER_XID_ARRAY_FINISH)
	{
		finish_xid_list_max = XID_ARRAY_STEP_SIZE;
		finish_xid_list_len = 0;
		finish_xid_array = palloc0(sizeof(TransactionId) * finish_xid_list_max);
	}
	else
		ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					errmsg("not support SnapSenderXidArrayType type")));

	return;
}

static void SnapSenderFreeXidArray(SnapSenderXidArrayType ssxat)
{
	if (ssxat == SNAPSENDER_XID_ARRAY_XACT2P)
	{
		pfree(xid_xact2pc_array);
		xid_xact2pc_max = 0;
		xid_xact2pc_count = 0;
	}
	else if (ssxat == SNAPSENDER_XID_ARRAY_ASSIGN)
	{
		pfree(assign_xid_array);
		assign_xid_list_max = 0;
		assign_xid_list_len = 0;
	}
	else if(ssxat == SNAPSENDER_XID_ARRAY_FINISH)
	{
		pfree(finish_xid_array);
		finish_xid_list_max = 0;
		finish_xid_list_len = 0;
	}
	else
		ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					errmsg("not support SnapSenderXidArrayType type")));

	return;
}

static void SnapSenderXidArrayAddXid(SnapSenderXidArrayType ssxat, TransactionId xid)
{
	int		i;
	bool	found;

	found = false;

	if (ssxat == SNAPSENDER_XID_ARRAY_XACT2P)
	{
		found = false;
		for (i = 0 ;i < xid_xact2pc_count; i++)
		{
			if (xid_xact2pc_array[i] == xid)
			{
				found = true;
				break;
			}
		}

		if (found == false)
		{
			if (xid_xact2pc_count == xid_xact2pc_max)
			{
				xid_xact2pc_max += XID_ARRAY_STEP_SIZE;
				xid_xact2pc_array = repalloc(xid_xact2pc_array, sizeof(TransactionId) * xid_xact2pc_max);
			}

			Assert(xid_xact2pc_count < xid_xact2pc_max);
			xid_xact2pc_array[xid_xact2pc_count++] = xid;
		}
	}
	else if (ssxat == SNAPSENDER_XID_ARRAY_ASSIGN)
	{
		if (assign_xid_list_len == assign_xid_list_max)
		{
			assign_xid_list_max += XID_ARRAY_STEP_SIZE;
			assign_xid_array = repalloc(assign_xid_array, sizeof(TransactionId) * assign_xid_list_max);
		}

		Assert(assign_xid_list_len < assign_xid_list_max);
		assign_xid_array[assign_xid_list_len++] = xid;
	}
	else if(ssxat == SNAPSENDER_XID_ARRAY_FINISH)
	{
		if (finish_xid_list_len == finish_xid_list_max)
		{
			finish_xid_list_max += XID_ARRAY_STEP_SIZE;
			finish_xid_array = repalloc(finish_xid_array, sizeof(TransactionId) * finish_xid_list_max);
		}

		Assert(finish_xid_list_len < finish_xid_list_max);
		finish_xid_array[finish_xid_list_len++] = xid;
	}
	else
		ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					errmsg("not support SnapSenderXidArrayType type")));

	return;
}

static void SnapSenderXidArrayRemoveXid(SnapSenderXidArrayType ssxat, TransactionId xid)
{
	int		i;
	if (ssxat == SNAPSENDER_XID_ARRAY_XACT2P)
	{
		for (i = 0 ;i < xid_xact2pc_count; i++)
		{
			if (xid_xact2pc_array[i] == xid)
			{
				memmove(&xid_xact2pc_array[i],
						&xid_xact2pc_array[i+1],
						(xid_xact2pc_count-i-1) * sizeof(xid));
				--xid_xact2pc_count;
				break;
			}
		}
	}
	else
		ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					errmsg("not support SnapSenderXidArrayType type")));

	return;
}

static bool SnapSenderXidArrayIsExistXid(TransactionId xid)
{
	int		i;
	bool	found = false;
	for (i = 0 ;i < xid_xact2pc_count; i++)
	{
		if (xid_xact2pc_array[i] == xid)
		{
			found = true;
			break;
		}
	}

	return found;
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
		pg_atomic_init_u32(&SnapSender->state, SNAPSENDER_STATE_STOPED);
		pg_atomic_init_u32(&SnapSender->dn_conn_state, SNAPSENDER_ALL_DNMASTER_CONN_NOT_OK);
		ConditionVariableInit(&SnapSender->cv);
		ConditionVariableInit(&SnapSender->cv_dn_con);
		proclist_init(&SnapSender->waiters_assign);
		proclist_init(&SnapSender->waiters_complete);
		proclist_init(&SnapSender->waiters_finish);

		SnapSender->xcnt = 0;
		SnapSender->cur_cnt_complete = 0;
		SnapSender->cur_cnt_assign = 0;
		SpinLockInit(&SnapSender->mutex);
		SpinLockInit(&SnapSender->gxid_mutex);
		SnapSender->comm_lock.handle_lock_info = DSM_HANDLE_INVALID;
		SnapSender->comm_lock.first_lock_info = InvalidDsaPointer;
		SnapSender->comm_lock.last_lock_info = InvalidDsaPointer;

		LWLockInitialize(&SnapSender->comm_lock.lock_lock_info, LWTRANCHE_SNAPSHOT_COMMON_DSA);
		LWLockInitialize(&SnapSender->comm_lock.lock_proc_link, LWTRANCHE_SNAPSHOT_COMMON_DSA);

		pg_atomic_init_u32(&SnapSender->global_xmin, InvalidTransactionId);
		pg_atomic_init_u32(&SnapSender->global_finish_id, InvalidTransactionId);
		pg_atomic_init_u32(&SnapSender->nextid_upcount, 0);
		pg_atomic_init_u32(&SnapSender->nextid_upcount_cn, 0);
		pg_atomic_init_flag(&SnapSender->lock);
	}
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void
SnapSenderSigHupHandler(SIGNAL_ARGS)
{
	got_SIGHUP = true;
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
		if (AppendMsgToClient(client, 'd', output_buffer.data, output_buffer.len) == false)
		{
			client->status = CLIENT_STATUS_EXITING;
		}
	}
}

static void snapsenderUpdateNextXid(TransactionId xid, SnapClientData *client)
{
	TransactionId	nextXid;
	uint32			epoch;
	if (!TransactionIdIsValid(xid))
		return;

	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	nextXid = XidFromFullTransactionId(ShmemVariableCache->nextXid);
	if (NormalTransactionIdFollows(xid, nextXid))
	{
		epoch = EpochFromFullTransactionId(ShmemVariableCache->nextXid);
		if (unlikely(xid < nextXid))
			++epoch;
		ShmemVariableCache->nextXid = FullTransactionIdFromEpochAndXid(epoch, xid);
		FullTransactionIdAdvance(&ShmemVariableCache->nextXid);

		SNAP_SYNC_DEBUG_LOG((errmsg("xid  %d, ShmemVariableCache->nextXid %d\n",
				xid, XidFromFullTransactionId(ShmemVariableCache->nextXid))));
		ShmemVariableCache->latestCompletedXid = ShmemVariableCache->nextXid;
		FullTransactionIdRetreat(&ShmemVariableCache->latestCompletedXid);

		snapsenderUpdateNextXidAllClient(xid, client);
	}

	LWLockRelease(XidGenLock);
}

static TransactionId snapsenderGetSenderGlobalXmin(void)
{
	slist_iter siter;
	SnapClientData *cur_client;
	TransactionId oldxmin;
	TransactionId global_xmin = InvalidTransactionId;

	slist_foreach(siter, &slist_all_client)
	{
		cur_client = slist_container(SnapClientData, snode, siter.cur);
		if (!TransactionIdIsValid(global_xmin))
			global_xmin = cur_client->global_xmin;
		else if (TransactionIdIsNormal(cur_client->global_xmin) &&
			NormalTransactionIdPrecedes(cur_client->global_xmin, global_xmin ))
			global_xmin = cur_client->global_xmin;
	}

#warning TODO get GetOldestXminExt
	oldxmin = FirstNormalTransactionId;// GetOldestXminExt(NULL, PROCARRAY_FLAGS_VACUUM, true);
	if (!TransactionIdIsValid(global_xmin) || NormalTransactionIdPrecedes(oldxmin, global_xmin))
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

	t2 = GetCurrentTimestamp();
	t1 = pq_getmsgint64(&input_buffer);
	xmin = pq_getmsgint64(&input_buffer);

	global_xmin = xmin;

	slist_foreach(siter, &slist_all_client)
	{
		cur_client = slist_container(SnapClientData, snode, siter.cur);
		if (pq_get_socket(client->node) == pq_get_socket(cur_client->node))
		{
			client->global_xmin = xmin;
			continue;
		}
		if (TransactionIdIsNormal(cur_client->global_xmin) &&
			NormalTransactionIdPrecedes(cur_client->global_xmin,global_xmin ))
			global_xmin = cur_client->global_xmin;
	}

#warning TODO get GetOldestXminExt
	oldxmin = FirstNormalTransactionId;// GetOldestXminExt(NULL, PROCARRAY_FLAGS_VACUUM, true);
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
	if (AppendMsgToClient(client, 'd', output_buffer.data, output_buffer.len) == false)
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
	msg.cursor = input_buffer.cursor;

	key = pq_getmsgint64(&msg);
	SNAP_FORCE_DEBUG_LOG((errmsg("snapsenderProcessSyncRequest get key %lld\n", key)));
	/* Send a HEARTBEAT Response message */
	resetStringInfo(&output_buffer);
	pq_sendbyte(&output_buffer, 'p');
	pq_sendint64(&output_buffer, key);
	if (AppendMsgToClient(client, 'd', output_buffer.data, output_buffer.len) == false)
	{
		client->status = CLIENT_STATUS_EXITING;
	}
}

static void SnapSendCheckCnDnInitSyncWait(void)
{
	TimestampTz now;
	uint32 state, dn_state;

	state = pg_atomic_read_u32(&SnapSender->dn_conn_state);
	dn_state = pg_atomic_read_u32(&SnapSender->dn_conn_state);
	if (likely(state == SNAPSENDER_STATE_OK &&
				dn_state == SNAPSENDER_ALL_DNMASTER_CONN_OK))
		return;

	now = GetCurrentTimestamp();
	if ((now - start_time) > snap_restart_timeout * 1000)
	{
		SNAP_SYNC_DEBUG_LOG((errmsg("SnapSendCheckCnDnInitSyncWait timeout, snap_restart_timeout %d\n", snap_restart_timeout)));
		pg_atomic_write_u32(&SnapSender->nextid_upcount, 0);
		pg_atomic_write_u32(&SnapSender->nextid_upcount_cn, 0);
		pg_atomic_write_u32(&SnapSender->dn_conn_state, SNAPSENDER_ALL_DNMASTER_CONN_OK);
		pg_atomic_write_u32(&SnapSender->state, SNAPSENDER_STATE_OK);
		ConditionVariableBroadcast(&SnapSender->cv);
		ConditionVariableBroadcast(&SnapSender->cv_dn_con);
		WakeAllCnClientStream();
		start_time = 0;
	}
	return;
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
				SNAP_SYNC_DEBUG_LOG((errmsg("SnapSendCheckTimeoutSocket drop clientname %s status %d\n",
			 			client->client_name, client->status)));
				slist_delete_current(&siter);
				DropClient(client, false);
			}
		}
	}
	return;
}

#define SNAPSENDER_QUEUE_SIZE	(16*1024)
static void CreateSHMQPipe(dsm_segment *seg, shm_mq_handle** mqh_sender, shm_mq_handle **mqh_receiver, bool is_worker)
{
	shm_mq			   *mq_sender;
	shm_mq			   *mq_receiver;
	char			   *addr = dsm_segment_address(seg);

	if (is_worker)
	{
		mq_receiver = (shm_mq*)(addr);
		mq_sender = (shm_mq*)(addr+SNAPSENDER_QUEUE_SIZE);
	}else
	{
		mq_sender = shm_mq_create(addr, SNAPSENDER_QUEUE_SIZE);
		mq_receiver = shm_mq_create(addr+SNAPSENDER_QUEUE_SIZE,
									SNAPSENDER_QUEUE_SIZE);
	}
	shm_mq_set_sender(mq_sender, MyProc);
	*mqh_sender = shm_mq_attach(mq_sender, seg, NULL);
	shm_mq_set_receiver(mq_receiver, MyProc);
	*mqh_receiver = shm_mq_attach(mq_receiver, seg, NULL);
}

void SnapSenderMainQueryDnNodeName(Datum arg)
{
	HeapTuple		tuple;
	shm_mq_handle	*mqh_sender;
	shm_mq_handle	*mqh_receiver;
	dsm_segment		*seg;
	int 			res;
	Relation		rel;
	SysScanDesc 	scan;
	is_snapsender_query_worker = true;

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnection("postgres", NULL, 0);

	seg = dsm_attach(DatumGetUInt32(arg));
	CreateSHMQPipe(seg, &mqh_sender, &mqh_receiver, true);
	pq_redirect_to_shm_mq(seg, mqh_sender);

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());

	Assert(IsGTMNode());
	Assert(IsTransactionState());

	rel = table_open(PgxcNodeRelationId, AccessShareLock);
	scan = systable_beginscan(rel, PgxcNodeOidIndexId, true,
							  NULL, 0, NULL);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		res = shm_mq_send(mqh_sender, tuple->t_len, tuple->t_data, false);
		if (res != SHM_MQ_SUCCESS)
		{
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					errmsg("could not send message")));
		}
	}
	systable_endscan(scan);
	table_close(rel, AccessShareLock);

	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_stat(false);
	pgstat_report_activity(STATE_IDLE, NULL);
}

static void StartSnapSenderMainQueryDnNodeName(void)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	pid_t		pid;
	HeapTupleData htup;
	shm_mq_result result;
	Size		nbytes;
	void	   *data;

	dsm_segment	   *dsm_seg = dsm_create(SNAPSENDER_QUEUE_SIZE*2, 0);
	shm_mq_handle  *mqh_sender;
	shm_mq_handle  *mqh_receiver;
	Form_pgxc_node 	node;

	CreateSHMQPipe(dsm_seg, &mqh_sender, &mqh_receiver, false);

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	sprintf(worker.bgw_library_name, "postgres");
	sprintf(worker.bgw_function_name, "SnapSenderMainQueryDnNodeName");
	snprintf(worker.bgw_name, BGW_MAXLEN, "SnapSenderMainQueryDnNodeName worker");
	snprintf(worker.bgw_type, BGW_MAXLEN, "SnapSenderMainQueryDnNodeName");
	worker.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(dsm_seg));
	/* set bgw_notify_pid so that we can use WaitForBackgroundWorkerStartup */
	worker.bgw_notify_pid = MyProc->pid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("Could not register background process"),
				 errhint("You may need to increase max_worker_processes.")));

	status = WaitForBackgroundWorkerStartup(handle, &pid);
	if (status != BGWH_STARTED && status != BGWH_STOPPED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not start background process"),
				 errhint("More details may be available in the server log.")));

	list_free(dn_master_name_list);
	dn_master_name_list = NIL;

	list_free(cn_master_name_list);
	cn_master_name_list = NIL;
	for (;;)
	{
		result = shm_mq_receive(mqh_receiver, &nbytes, &data, false);

		/* If queue is detached, set *done and return NULL. */
		if (result == SHM_MQ_DETACHED)
		{
			ereport(DEBUG1,
					(errmsg("receive message from snapsender worker got MQ detached")));
			break;
		}

		/* In non-blocking mode, bail out if no message ready yet. */
		if (result == SHM_MQ_WOULD_BLOCK)
		{
			ereport(ERROR,
					(errmsg("receive message from snapsender worker SHM_MQ_WOULD_BLOCK")));
		}
	
		if (result != SHM_MQ_SUCCESS)
			ereport(ERROR,
					(errmsg("receive message from snapsender worker got MQ detached")));

		ItemPointerSetInvalid(&htup.t_self);
		htup.t_tableOid = InvalidOid;
		htup.t_len = nbytes;
		htup.t_data = data;

		node = (Form_pgxc_node)GETSTRUCT(&htup);

		SNAP_SYNC_DEBUG_LOG((errmsg("node->node_type %c, node->name %s\n", node->node_type, NameStr(node->node_name))));
		if (node->node_type == PGXC_NODE_DATANODE)
			dn_master_name_list = lappend(dn_master_name_list, pstrdup(NameStr(node->node_name)));
		
		if (node->node_type == PGXC_NODE_COORDINATOR)
			cn_master_name_list = lappend(cn_master_name_list, pstrdup(NameStr(node->node_name)));
	}

	pg_atomic_write_u32(&SnapSender->nextid_upcount, list_length(dn_master_name_list));
	pg_atomic_write_u32(&SnapSender->nextid_upcount_cn, list_length(cn_master_name_list) - 1); //exclude gtmc
	SNAP_SYNC_DEBUG_LOG((errmsg("list_length(dn_master_name_list) %d, SnapSender->nextid_upcount %d\n",
			 			list_length(dn_master_name_list), pg_atomic_read_u32(&SnapSender->nextid_upcount))));
	SNAP_SYNC_DEBUG_LOG((errmsg("list_length(cn_master_name_list) %d, SnapSender->nextid_upcount_cn %d\n",
			 			list_length(cn_master_name_list), pg_atomic_read_u32(&SnapSender->nextid_upcount_cn))));

	if (pg_atomic_read_u32(&SnapSender->nextid_upcount_cn) == 0)
	{
		SNAP_SYNC_DEBUG_LOG((errmsg("StartSnapSenderMainQueryDnNodeName SnapSender->state to Ok\n")));
		pg_atomic_write_u32(&SnapSender->state, SNAPSENDER_STATE_OK);
		ConditionVariableBroadcast(&SnapSender->cv);
	}

	if (pg_atomic_read_u32(&SnapSender->nextid_upcount) == 0)
	{
		SNAP_SYNC_DEBUG_LOG((errmsg("StartSnapSenderMainQueryDnNodeName SnapSender->dn_conn_state to Ok\n")));
		pg_atomic_write_u32(&SnapSender->dn_conn_state, SNAPSENDER_ALL_DNMASTER_CONN_OK);
		ConditionVariableBroadcast(&SnapSender->cv_dn_con);
	}

	if (pg_atomic_read_u32(&SnapSender->state) == SNAPSENDER_STATE_OK &&
			pg_atomic_read_u32(&SnapSender->dn_conn_state) == SNAPSENDER_ALL_DNMASTER_CONN_OK)
		WakeAllCnClientStream();
}

static void SnapSenderCheckRxactAndTwoPhaseXids()
{
	List						*rxact_list = NIL;
	ListCell					*lc;
	TransactionId				xid;
	RxactTransactionInfo 		*info;
	TransactionId				*xid_array_xact;
	int							array_xact_len, i;

	/* for gtm get rxact left xid*/
	/* for two-phase left xid, when get snapshot we can get the xid from local proc array*/
	if (IsGTMNode() && !RecoveryInProgress())
	{
		rxact_list = RxactGetRunningList();
		SNAP_SYNC_DEBUG_LOG((errmsg("SnapSenderCheckRxactXids RxactGetRunningList list len %d\n",
			 			list_length(rxact_list))));
	}

	array_xact_len = list_length(rxact_list);
	if (array_xact_len > 0)
		xid_array_xact = palloc0(sizeof(TransactionId) * array_xact_len);

	i = 0;
	foreach (lc, rxact_list)
	{
		info =  (RxactTransactionInfo*)lfirst(lc);
		xid = pg_strtouint64(&info->gid[1], NULL, 10);
		SnapSenderXidArrayAddXid(SNAPSENDER_XID_ARRAY_XACT2P, xid);
		SNAP_SYNC_DEBUG_LOG((errmsg("SnapSenderCheckRxactAndTwoPhaseXids Add rxact xid %u\n", xid)));
		xid_array_xact[i++] = xid;
	}
	list_free(rxact_list);

	if (array_xact_len > 0)
	{
		SpinLockAcquire(&SnapSender->gxid_mutex);
		memcpy(&SnapSender->xip[SnapSender->xcnt], xid_array_xact, sizeof(TransactionId)*array_xact_len);
		SnapSender->xcnt += array_xact_len;
		SpinLockRelease(&SnapSender->gxid_mutex);
	}

	if (array_xact_len > 0)
		pfree(xid_array_xact);

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
	pg_atomic_write_u32(&SnapSender->state, SNAPSENDER_STATE_STARTING);
	SnapSender->procno = MyProc->pgprocno;
	SpinLockRelease(&SnapSender->mutex);

	pg_atomic_write_u32(&SnapSender->global_xmin, InvalidTransactionId);
	snap_send_timeout = snap_receiver_timeout + 10000L;

	on_shmem_exit(SnapSenderDie, (Datum)0);

	/* make sure dsa_area create */
	(void)SnapGetLockArea(&SnapSender->comm_lock);

	/* release all last store lock and invalid msgs*/
	SnapReleaseAllTxidLocks(&SnapSender->comm_lock);

	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGHUP, SnapSenderSigHupHandler);
	pqsignal(SIGTERM, SIG_IGN);
	pqsignal(SIGQUIT, SnapSenderQuickDieHander);
	sigdelset(&BlockSig, SIGQUIT);
	pqsignal(SIGUSR1, SIG_IGN);
	pqsignal(SIGUSR2, SIG_IGN);

	PG_SETMASK(&UnBlockSig);

	SnapSenderStartup();
	Assert(wait_event_set != NULL);

	initStringInfo(&output_buffer);
	initStringInfo(&input_buffer);

	SnapSenderInitXidArray(SNAPSENDER_XID_ARRAY_XACT2P);
	SnapSenderInitXidArray(SNAPSENDER_XID_ARRAY_ASSIGN);
	SnapSenderInitXidArray(SNAPSENDER_XID_ARRAY_FINISH);

	StartSnapSenderMainQueryDnNodeName();
	SnapSenderCheckRxactAndTwoPhaseXids();
	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	SnapSender->latestCompletedXid = XidFromFullTransactionId(ShmemVariableCache->latestCompletedXid);
	LWLockRelease(XidGenLock);

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
				SNAP_SYNC_DEBUG_LOG((errmsg("sigsetjmp1 drop clientname %s\n",
			 			client->client_name)));
				slist_delete_current(&siter);
				DropClient(client, false);
			}else if(pq_node_send_pending(client->node))
			{
				ModifyWaitEvent(wait_event_set, client->event_pos,
						(client->status == CLIENT_STATUS_EXITING ? 0 : WL_SOCKET_READABLE) | WL_SOCKET_WRITEABLE, NULL);
			}else if(client->status == CLIENT_STATUS_EXITING)
			{
				SNAP_SYNC_DEBUG_LOG((errmsg("sigsetjmp2 drop clientname %s\n",
			 			client->client_name)));
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
	start_time = GetCurrentTimestamp();

	SetLatch(&MyProc->procLatch);
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
		if (start_time > 0)
			SnapSendCheckCnDnInitSyncWait();
		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
			//SnapSenderReloadDnNameList();
		}
	}

	ereport(FATAL,
				(errcode(ERRCODE_ADMIN_SHUTDOWN),
				 errmsg("terminating snaprsender process due to administrator command")));
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
}

/* must have lock gxid_mutex already */
static void SnapSenderDropXidItem(TransactionId xid)
{
	int count;
	int i;

	count = SnapSender->xcnt;
	for (i=0;i<count;++i)
	{
		if (SnapSender->xip[i] == xid)
		{
			memmove(&SnapSender->xip[i],
					&SnapSender->xip[i+1],
					(count-i-1) * sizeof(xid));
			if (TransactionIdPrecedes(SnapSender->latestCompletedXid, xid))
				SnapSender->latestCompletedXid = xid;
			--count;
			SNAP_SYNC_DEBUG_LOG((errmsg("SnapSenderDropXidItem remove xid %u\n", xid)));
			break;
		}
	}
	SnapSender->xcnt = count;
}

static void SnapSenderTriggerAssingAndFinishList(TransactionId *xid_assign, uint32 local_assign_cnt, TransactionId *xid_finish, uint32 local_finish_cnt)
{
	TransactionId			*assign_array;
	TransactionId			*finish_array;

	if (assign_xid_list_len > 0 && local_assign_cnt > 0)
	{
		assign_array = palloc0(sizeof(TransactionId) * (assign_xid_list_len + local_assign_cnt));
		memcpy(assign_array, assign_xid_array, sizeof(TransactionId)*assign_xid_list_len);
		memcpy(assign_array + assign_xid_list_len, xid_assign, sizeof(TransactionId)*local_assign_cnt);

		ProcessShmemXidMsg(assign_array, assign_xid_list_len + local_assign_cnt, 'a');
		assign_xid_list_len = 0;
		pfree(assign_array);
	}
	else if (assign_xid_list_len > 0)
	{
		ProcessShmemXidMsg(assign_xid_array, assign_xid_list_len, 'a');
		assign_xid_list_len = 0;
	}
	else if (local_assign_cnt > 0) /* check assign message */
		ProcessShmemXidMsg(&xid_assign[0], local_assign_cnt, 'a');

	if (finish_xid_list_len > 0 && local_finish_cnt > 0)
	{
		finish_array = palloc0(sizeof(TransactionId) * (finish_xid_list_len + local_finish_cnt));
		memcpy(finish_array, finish_xid_array, sizeof(TransactionId)*finish_xid_list_len);
		memcpy(finish_array + finish_xid_list_len, xid_finish, sizeof(TransactionId)*local_finish_cnt);

		ProcessShmemXidMsg(finish_array, finish_xid_list_len + local_finish_cnt, 'c');
		finish_xid_list_len = 0;
		pfree(finish_array);
	}
	else if (finish_xid_list_len > 0)
	{
		ProcessShmemXidMsg(finish_xid_array, finish_xid_list_len, 'c');
		finish_xid_list_len = 0;
	}
	else if (local_finish_cnt > 0) /* check finish transaction */
		ProcessShmemXidMsg(&xid_finish[0], local_finish_cnt, 'c');

	return;
}

void SnapSenderGetAllAssingFinishXids(TransactionId *xid_assign, uint32 *assign_cnt, TransactionId *xid_finish, uint32 *finish_cnt, TransactionId *latestCompletedXid)
{
	uint32					local_assign_cnt, local_finish_cnt;
	proclist_mutable_iter	proc_iter_assign;
	proclist_mutable_iter	proc_iter_finish;
	PGPROC					*proc;

	SpinLockAcquire(&SnapSender->mutex);
	Assert(SnapSender->cur_cnt_assign <= MAX_CNT_SHMEM_XID_BUF);
	Assert(SnapSender->cur_cnt_complete <= MAX_CNT_SHMEM_XID_BUF);

	local_assign_cnt = SnapSender->cur_cnt_assign;
	local_finish_cnt = SnapSender->cur_cnt_complete;

	pg_memory_barrier();
	if (local_assign_cnt > 0)
	{
		memcpy(xid_assign, SnapSender->xid_assign, sizeof(TransactionId)*local_assign_cnt);
		SnapSender->cur_cnt_assign = 0;
	}

	if (local_finish_cnt > 0)
	{
		int i;
		memcpy(xid_finish, SnapSender->xid_complete, sizeof(TransactionId)*local_finish_cnt);
		SnapSender->cur_cnt_complete = 0;
		if (xid_xact2pc_count > 0)
		{
			for (i = 0; i <local_finish_cnt; i++)
			{
				SnapSenderXidArrayRemoveXid(SNAPSENDER_XID_ARRAY_XACT2P, xid_finish[i]);
			}
		}
	}

	proclist_foreach_modify(proc_iter_assign, &SnapSender->waiters_assign, GTMWaitLink)
	{
		proc = GetPGProcByNumber(proc_iter_assign.cur);
		Assert(proc->pgprocno == proc_iter_assign.cur);
		proclist_delete(&SnapSender->waiters_assign, proc_iter_assign.cur, GTMWaitLink);
		SetLatch(&proc->procLatch);
	}

	proclist_foreach_modify(proc_iter_finish, &SnapSender->waiters_complete, GTMWaitLink)
	{
		proc = GetPGProcByNumber(proc_iter_finish.cur);
		Assert(proc->pgprocno == proc_iter_finish.cur);
		proclist_delete(&SnapSender->waiters_complete, proc_iter_finish.cur, GTMWaitLink);
		SetLatch(&proc->procLatch);
	}

	if (latestCompletedXid && TransactionIdIsNormal(SnapSender->latestCompletedXid))
	{
		if (TransactionIdFollows(SnapSender->latestCompletedXid,
									*latestCompletedXid))
				*latestCompletedXid= SnapSender->latestCompletedXid;
	}
	SpinLockRelease(&SnapSender->mutex);

	*assign_cnt = local_assign_cnt;
	*finish_cnt = local_finish_cnt;
	return;
}

/* event handlers */
static void OnLatchSetEvent(WaitEvent *event, time_t* time_last_latch)
{
	TransactionId			xid_assign[MAX_CNT_SHMEM_XID_BUF];
	TransactionId			xid_finish[MAX_CNT_SHMEM_XID_BUF];
	uint32					assign_cnt, finish_cnt;
	time_t					time_now;

	if (!MyLatch->is_set)
		return;

	time_now = time(NULL);
	ResetLatch(&MyProc->procLatch);

	SnapSenderGetAllAssingFinishXids(xid_assign, &assign_cnt, xid_finish, &finish_cnt, NULL);
	SnapSenderTriggerAssingAndFinishList(xid_assign, assign_cnt, xid_finish, finish_cnt);
	*time_last_latch = time_now;
}

static void ProcessShmemXidMsg(TransactionId *xids, const uint32 xid_cnt, char msgtype)
{
	slist_mutable_iter		siter;
	SnapClientData			*client;
	uint32					i;

	if (xid_cnt <= 0)
		return;

	resetStringInfo(&output_buffer);
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
				pq_sendint32(&output_buffer, xids[i]);
				SNAP_SYNC_DEBUG_LOG((errmsg("SnapSend rel finsih/assing %c xid %u\n",
					msgtype, xids[i])));
			}
			output_buffer.cursor = true;
		}

		SNAP_SYNC_DEBUG_LOG((errmsg("SnapSend rel finsih/assing xids to client %s, client->status %d\n",
					client->client_name, client->status)));
		if (AppendMsgToClient(client, 'd', output_buffer.data, output_buffer.len) == false)
		{
					
			SNAP_SYNC_DEBUG_LOG((errmsg("SnapSend send to client event_pos %d error\n",
			 			client->event_pos)));
			client->status = CLIENT_STATUS_EXITING;
		}
	}
}

static void DropClient(SnapClientData *client, bool drop_in_slist)
{
	slist_iter siter;
	pgsocket fd = socket_pq_node(client->node);
	int pos = client->event_pos;

	Assert(GetWaitEventData(wait_event_set, client->event_pos) == client);
	SNAP_SYNC_DEBUG_LOG((errmsg("SnapSend DropClient event_pos %d with drop_in_slist %d clientname %s\n",
			 			client->event_pos, drop_in_slist, client->client_name)));
	if (drop_in_slist)
	{
		slist_delete(&slist_all_client, &client->snode);
	}
	SnapSenderTransferCnClientToFailledList(client);

	RemoveWaitEvent(wait_event_set, client->event_pos);
	cur_wait_event--;

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

static bool AppendMsgToClient(SnapClientData *client, char msgtype, const char *data, int len)
{
	pq_comm_node *node = client->node;
	bool old_send_pending = pq_node_send_pending(node);
	Assert(GetWaitEventData(wait_event_set, client->event_pos) == client);

	pq_node_putmessage_noblock_sock(node, msgtype, data, len);
	if (old_send_pending == false)
	{
		if (pq_node_flush_if_writable_sock(node) != 0)
		{
			return false;
		}

		if (pq_node_send_pending(node))
		{
			ModifyWaitEvent(wait_event_set,
							client->event_pos,
							WL_SOCKET_READABLE|WL_SOCKET_WRITEABLE,
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
		client->global_xmin = InvalidTransactionId;
		client->status = CLIENT_STATUS_CONNECTED;
		client->is_dn = true;

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

	Assert(GetWaitEventData(wait_event_set, client->event_pos) == client);

	PG_TRY();
	{
		node = client->node;
		pq_node_switch_to(node);

		if (event->events & WL_SOCKET_READABLE)
		{
			if (client->status == CLIENT_STATUS_EXITING)
				ModifyWaitEvent(wait_event_set, client->event_pos, 0, NULL);
			else
				OnClientRecvMsg(client, node, time_last_latch);
		}
		if (event->events & (WL_SOCKET_WRITEABLE|WL_SOCKET_CONNECTED))
			OnClientSendMsg(client, node);

		/* all data sended and exiting, close it */
		if(client->status == CLIENT_STATUS_EXITING)
			DropClient(client, true);
		else
			ModifyWaitEvent(wait_event_set,
							client->event_pos,
							(pq_node_send_pending(node) ? WL_SOCKET_WRITEABLE : 0) | WL_SOCKET_READABLE,
							NULL);
		
	}PG_CATCH();
	{
		client->status = CLIENT_STATUS_EXITING;
		PG_RE_THROW();
	}PG_END_TRY();
}

static bool IsXidAlreadyInInitSyncList(TransactionId xid,TransactionId *xid_array_sync,int xid_sync_num)
{
	int index;
	bool found = false;

	for (index = 0; index < xid_sync_num; index++)
	{
		if (xid_array_sync[index] == xid)
		{
			found = true;
			break;
		}
	}

	return found;
}

/* like GetSnapshotData, but serialize all active transaction IDs */
/* xids get xid from local proc arraty. gs_xip get xid from SnapSender->xid. */
/* ss_xip get xid from Snapsender->xid_assign, sf_xip get xid from SnapSender->xid_finish. */
void SerializeFullAssignXid(TransactionId *xids, uint32 cnt, TransactionId *gs_xip, uint32 gs_cnt,
							TransactionId *ss_xip, uint32 ss_cnt,
							TransactionId *sf_xip, uint32 sf_cnt, StringInfo buf)
{
	int index,xid_sync_num;
	bool found;
	TransactionId xid;
	TransactionId *xid_array_sync;

	xid_sync_num = 0;
	xid_array_sync = palloc0(sizeof(TransactionId) * (cnt+gs_cnt+xid_xact2pc_count));

	/* for gtm local proc array assign xid num,*/
	for(index=0;index<cnt;++index)
	{
		xid = xids[index];
		pq_sendint32(buf, xid);
		xid_array_sync[xid_sync_num++] = xid;
		SNAP_SYNC_DEBUG_LOG((errmsg("SnapSend init sync add local proc assing xid %u\n", xid)));
	}

	/* for SnapSender->xid the xids which have assign to the other CN/DN */
	for (index = 0; index < gs_cnt; ++index)
	{
		xid = gs_xip[index];
		/* continue check init sync xids list*/
		found = IsXidAlreadyInInitSyncList(xid, xid_array_sync, xid_sync_num);
		if (!found)
		{
			pq_sendint32(buf, gs_xip[index]);
			xid_array_sync[xid_sync_num++] = xid;
			SNAP_SYNC_DEBUG_LOG((errmsg("SnapSend init sync add SnapSender xids xid %u\n", xid)));
		}
	}

	/* last check xid_xact2pc_array */
	for (index = 0; index < xid_xact2pc_count; ++index)
	{
		xid = xid_xact2pc_array[index];
		/* continue check init sync xids list*/
		found = IsXidAlreadyInInitSyncList(xid, xid_array_sync, xid_sync_num);
		if (!found)
		{
			pq_sendint32(buf, gs_xip[index]);
			xid_array_sync[xid_sync_num++] = xid;
			SNAP_SYNC_DEBUG_LOG((errmsg("SnapSend init sync add xid_xact2pc xid %u\n", xid)));
		}
	}
	pfree(xid_array_sync);

	/*Before we init Sync all xid to client, we should trigger porcess assign and finish list first*/
	SnapSenderTriggerAssingAndFinishList(ss_xip,ss_cnt, sf_xip, sf_cnt);
}

static bool snapsenderGetIsCnConn(SnapClientData *client)
{
	char*			list_client_name;
	ListCell 		*node_ceil;
	int				comp_ret;
	bool			found = false;

	foreach(node_ceil, cn_master_name_list)
	{
		list_client_name = (char *)lfirst(node_ceil);
		comp_ret = strcasecmp(list_client_name, client->client_name);

		if (comp_ret == 0)
		{
			found = true;
			break;
		}
	}

	return found;
}

static void snapsenderProcessNextXid(SnapClientData *client, TransactionId txid)
{
	char*			list_client_name;
	ListCell 		*node_ceil;
	uint32			current_count;
	int				comp_ret;

	snapsenderUpdateNextXid(txid, client);

	if (pg_atomic_read_u32(&SnapSender->dn_conn_state) != SNAPSENDER_ALL_DNMASTER_CONN_OK)
	{
		foreach(node_ceil, dn_master_name_list)
		{
			list_client_name = (char *)lfirst(node_ceil);
			comp_ret = strcasecmp(list_client_name, client->client_name);

			if (comp_ret == 0)
			{
				current_count = pg_atomic_sub_fetch_u32(&SnapSender->nextid_upcount, 1);
				if (current_count == 0)
				{
					SNAP_SYNC_DEBUG_LOG((errmsg("snapsenderProcessNextXid DN SnapSender->state dn_conn_state to Ok\n")));
					pg_atomic_write_u32(&SnapSender->dn_conn_state, SNAPSENDER_ALL_DNMASTER_CONN_OK);
					if (pg_atomic_read_u32(&SnapSender->state) == SNAPSENDER_STATE_OK)
					{
						SNAP_SYNC_DEBUG_LOG((errmsg("wakeup all waite CN and backend1.\n")));
						ConditionVariableBroadcast(&SnapSender->cv_dn_con);
						ConditionVariableBroadcast(&SnapSender->cv);
						WakeAllCnClientStream();
					}
				}
				break;
			}
		}
	}

	if (pg_atomic_read_u32(&SnapSender->state) != SNAPSENDER_STATE_OK)
	{
		foreach(node_ceil, cn_master_name_list)
		{
			list_client_name = (char *)lfirst(node_ceil);
			comp_ret = strcasecmp(list_client_name, client->client_name);

			if (comp_ret == 0)
			{
				current_count = pg_atomic_sub_fetch_u32(&SnapSender->nextid_upcount_cn, 1);
				if (current_count == 0)
				{
					SNAP_SYNC_DEBUG_LOG((errmsg("snapsenderProcessNextXid CN SnapSender->state to Ok\n")));
					pg_atomic_write_u32(&SnapSender->state, SNAPSENDER_STATE_OK);
					if (pg_atomic_read_u32(&SnapSender->dn_conn_state) == SNAPSENDER_ALL_DNMASTER_CONN_OK)
					{
						SNAP_SYNC_DEBUG_LOG((errmsg("wakeup all waite CN and backend2.\n")));
						ConditionVariableBroadcast(&SnapSender->cv_dn_con);
						ConditionVariableBroadcast(&SnapSender->cv);
						WakeAllCnClientStream();
					}
				}
				break;
			}
		}
	}
}

static void SnapSenderProcessAssignGxid(SnapClientData *client)
{
	int							procno, start_cursor, xid_num, index;
	TransactionId				xid, xidmax;
	TransactionId				*xid_array;

	if (adb_check_sync_nextid)
		isSnapSenderWaitNextIdOk();

	resetStringInfo(&output_buffer);
	pq_sendbyte(&output_buffer, 'g');

	start_cursor = input_buffer.cursor;

	xid_num = 0;
	while(input_buffer.cursor < input_buffer.len)
	{
		procno = pq_getmsgint(&input_buffer, sizeof(procno));
		xid_num++;
	}
	xid_array = palloc0(sizeof(TransactionId) * xid_num);

	index = 0;
	input_buffer.cursor = start_cursor;

	xidmax = XidFromFullTransactionId(GetNewTransactionIdExt(false, xid_num, false, false));
	while(input_buffer.cursor < input_buffer.len)
	{
		procno = pq_getmsgint(&input_buffer, sizeof(procno));
		xid = xidmax;
		TransactionIdRetreat(xidmax);

		SnapSenderClientAddXid(client, xid);
		pq_sendint32(&output_buffer, procno);
		pq_sendint32(&output_buffer, xid);

		SnapSenderXidArrayAddXid(SNAPSENDER_XID_ARRAY_ASSIGN, xid);
		SNAP_SYNC_DEBUG_LOG((errmsg("SnapSenderProcessAssignGxid Add assgin xid %u\n", xid)));
		xid_array[index++] = xid;
	}
	SetLatch(&MyProc->procLatch);

	if (AppendMsgToClient(client, 'd', output_buffer.data, output_buffer.len) == false)
	{
		client->status = CLIENT_STATUS_EXITING;
	}
	pfree(xid_array);
}

static void SnapSenderProcessPreAssignGxidArray(SnapClientData *client)
{
	TransactionId				xid, xidmax; 
	int							i, xid_num;

	if (adb_check_sync_nextid && !IsAutoVacuumWorkerProcess())
		isSnapSenderWaitNextIdOk();

	xid_num = pq_getmsgint(&input_buffer, sizeof(xid_num));
	Assert(xid_num > 0 && xid_num <= MAX_XID_PRE_ALLOC_NUM);

	resetStringInfo(&output_buffer);
	pq_sendbyte(&output_buffer, 'q');
	pq_sendint32(&output_buffer, xid_num);

	SNAP_SYNC_DEBUG_LOG((errmsg("GxidSend assging xid for %s\n", client->client_name)));
	xidmax = XidFromFullTransactionId(GetNewTransactionIdExt(false, xid_num, false, false));

	for (i = 0; i < xid_num; i++)
	{
		xid = xidmax;
		TransactionIdRetreat(xidmax);
		SnapSenderClientAddXid(client, xid);

		SnapSenderXidArrayAddXid(SNAPSENDER_XID_ARRAY_ASSIGN, xid);
		SNAP_SYNC_DEBUG_LOG((errmsg("SnapSenderProcessPreAssignGxidArray Add assgin xid %u\n", xid)));
		pq_sendint32(&output_buffer, xid);
	}

	SetLatch(&MyProc->procLatch);

	Assert(xid_num <= MAX_XID_PRE_ALLOC_NUM);
	if (AppendMsgToClient(client, 'd', output_buffer.data, output_buffer.len) == false)
	{
		client->status = CLIENT_STATUS_EXITING;
	}
}

static void SnapSenderProcessFinishGxid(SnapClientData *client)
{
	int							procno, start_cursor;
	TransactionId				xid; 
	size_t						input_buf_free_len, out_buf_free_len;
  
	resetStringInfo(&output_buffer);
	pq_sendbyte(&output_buffer, 'f');

	start_cursor = input_buffer.cursor;

re_lock_:
	input_buf_free_len = input_buffer.maxlen - input_buffer.len;
	out_buf_free_len = output_buffer.maxlen - output_buffer.len;
	SpinLockAcquire(&SnapSender->gxid_mutex);

	if (input_buf_free_len > out_buf_free_len)
	{
		SpinLockRelease(&SnapSender->gxid_mutex);
		enlargeStringInfo(&output_buffer, output_buffer.maxlen + input_buf_free_len - out_buf_free_len);
		goto re_lock_;
	}

	while(input_buffer.cursor < input_buffer.len)
	{
		procno = pq_getmsgint(&input_buffer, sizeof(procno));
		xid = pq_getmsgint(&input_buffer, sizeof(xid));

		pq_sendint32(&output_buffer, procno);
		pq_sendint32(&output_buffer, xid);\
		/* comman commite, xid should not left in Prepared*/
/*
#ifdef SNAP_SYNC_DEBUG_LOG
		found = IsXidInPreparedState(xid);
		Assert(!found);
#endif*/
		SnapSenderDropXidItem(xid);
		SnapSenderXidArrayRemoveXid(SNAPSENDER_XID_ARRAY_XACT2P, xid);
		SNAP_SYNC_DEBUG_LOG((errmsg("SnapSend finish xid %u for client %s\n",
			 			xid, client->client_name)));
	}
	SpinLockRelease(&SnapSender->gxid_mutex);

	input_buffer.cursor = start_cursor;
	while(input_buffer.cursor < input_buffer.len)
	{
		procno = pq_getmsgint(&input_buffer, sizeof(procno));
		xid = pq_getmsgint(&input_buffer, sizeof(xid));

		if(SnapSenderClientRemoveXid(client, xid))
		{
			SnapSenderXidArrayAddXid(SNAPSENDER_XID_ARRAY_FINISH, xid);
			SNAP_SYNC_DEBUG_LOG((errmsg("SnapSenderProcessFinishGxid Add finish xid %u\n", xid)));
		}

		SnapReleaseTransactionLocks(&SnapSender->comm_lock, xid);
		SnapSenderXidArrayRemoveXid(SNAPSENDER_XID_ARRAY_XACT2P, xid);
	}
	SetLatch(&MyProc->procLatch);

	if (AppendMsgToClient(client, 'd', output_buffer.data, output_buffer.len) == false)
	{
		client->status = CLIENT_STATUS_EXITING;
	}
}

static void 
SnapSenderDropXidList(SnapClientData *client, const TransactionId *cn_txids, const int txids_count)
{
	TransactionId			*xids_assign;
	int						i, index, count;
	bool					found;
	TransactionId			xid;
	slist_iter				siter;
	SnapClientData			*client_item;

	if (txids_count > 0)
		xids_assign = palloc0(txids_count * sizeof(TransactionId));
	else
		xids_assign = NULL;

	SNAP_SYNC_DEBUG_LOG((errmsg("SnapSenderDropXidList client %s client->cur_cnt %u, txids_count %u\n", client->client_name, client->cur_cnt, txids_count)));
	/* CASE 1, client->xid(server reserve) has xid, but init sync has no this xid*/
	if (client->cur_cnt > 0)
	{
		TransactionId	*xids;
		xids = palloc0(client->cur_cnt * sizeof(TransactionId));
		i = 0;
		count = 0;

		for (i = 0 ; i < client->cur_cnt; i++)
		{
			found = false;
			xid = client->xid[i];

			SNAP_SYNC_DEBUG_LOG((errmsg("SnapSender client %s left %d/%d xid %u\n", client->client_name, i, client->cur_cnt, xid)));
			for (index = 0 ;index < txids_count; index++)
			{
				if (xid == cn_txids[index])
				{
					found = true;
					break;
				}
			}

			/* when snapsender hold the xid, but client has no this xid sync, we should finish it*/
			if (found == false)
			{
				SNAP_SYNC_DEBUG_LOG((errmsg("SnapSender remove client %s xid %u\n", client->client_name, xid)));
				xids[count++] = xid;
			}
		}
		
		if (count > 0)
		{
			SpinLockAcquire(&SnapSender->gxid_mutex);
			for (i = 0; i < count; i++)
			{
				SnapSenderDropXidItem(xids[i]);
			}
			SpinLockRelease(&SnapSender->gxid_mutex);

			for (i = 0; i < count; i++)
			{
				if(SnapSenderClientRemoveXid(client, xids[i]))
				{
					SnapSenderXidArrayAddXid(SNAPSENDER_XID_ARRAY_FINISH, xids[i]);
					SNAP_SYNC_DEBUG_LOG((errmsg("SnapSenderDropXidList Add finish xid %u\n", xids[i])));
				}
				SnapReleaseTransactionLocks(&SnapSender->comm_lock, xids[i]);
			}
		}
		pfree(xids);
	}

	/* CASE 2,  when init sync xid which does not exist in sever, assgin and add it*/
	for (index = 0 ;index < txids_count; index++)
	{
		found = false;
		xid = cn_txids[index];

		/* when append for coordinator, new coordinator will repost the xids which are not from this client. */
		/* we check the other client first */
		slist_foreach(siter, &slist_all_client)
		{
			client_item = slist_container(SnapClientData, snode, siter.cur);
			if (client_item != client)
			{
				for (i = 0 ; i < client_item->cur_cnt; i++)
				{
					if (client_item->xid[i] == xid)
					{
						found = true;
						break;
					}
				}
			}
		}

		/* check local cient */
		if (found == false)
		{
			for (i = 0 ; i < client->cur_cnt; i++)
			{
				if (client->xid[i] == xid)
				{
					found = true;
					break;
				}
			}
		}

		/* can not found in assign list*/
		if (found == false)
			SnapSenderClientAddXid(client, xid);
	}

	if (xids_assign)
		pfree(xids_assign);
}

TransactionId *SnapSenderGetAllXip(uint32 *cnt_num)
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

	SpinLockAcquire(&SnapSender->gxid_mutex);

	if (assign_len <  SnapSender->xcnt)
	{
		SpinLockRelease(&SnapSender->gxid_mutex);
		assign_len += XID_ARRAY_STEP_SIZE;
		goto re_lock_;
	}

	assign_len = SnapSender->xcnt;
	if (assign_len > 0)
		memcpy(assign_xids, SnapSender->xip, sizeof(TransactionId)*assign_len);
	SpinLockRelease(&SnapSender->gxid_mutex);

	*cnt_num = assign_len;
	return assign_xids;
}

static void SnapSenderProcessInitSyncRequest(SnapClientData *client, char* xid_list_str)
{
	int				txid_cn_count;
	int				array_2pc_len;
	TransactionId	*cn_txids = NULL;
	TransactionId	*xid_2pc_array = NULL;
	List	   		*xid_list;
	ListCell   		*lc;
	TransactionId	xid;
	int				list_len, i, index;
	bool			found, is_rxact;
	char			*xid_str_org;

	xid_str_org = pstrdup(xid_list_str);
	if (!SplitIdentifierString(xid_str_org, ',', &xid_list))
	{
		/* syntax error in name list */
		pfree(xid_str_org);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("parameter publication_names must be a list of xids")));
	}

	list_len = list_length(xid_list);
	if (list_len > 0)
	{
		cn_txids = palloc0(sizeof(TransactionId) * list_len);
		xid_2pc_array = palloc0(sizeof(TransactionId) * list_len);
	}

	txid_cn_count = 0;
	array_2pc_len = 0;
	foreach(lc, xid_list)
	{
		const char *xid_str = (const char *) lfirst(lc);
		
		is_rxact = false;
		SNAP_SYNC_DEBUG_LOG((errmsg("xid_str %s\n",
								xid_str)));
		if (xid_str[0] == '+') //for rxact init sync xid skip + character
		{
			is_rxact = true;
			xid = pg_strtouint64(&xid_str[1], NULL, 10);
		}
		else
			xid = pg_strtouint64(xid_str, NULL, 10);

		SNAP_SYNC_DEBUG_LOG((errmsg("get xid number %u\n",
								xid)));
		Assert(TransactionIdIsNormal(xid));
		/* sync left twh-phase xid from client */
		if (is_rxact == false)
		{
			if (!SnapSenderXidArrayIsExistXid(xid))
			{
				SnapSenderXidArrayAddXid(SNAPSENDER_XID_ARRAY_XACT2P, xid);
				SNAP_SYNC_DEBUG_LOG((errmsg("SnapSenderProcessInitSyncRequest Add prepared txid %u\n",
								xid)));
				xid_2pc_array[array_2pc_len++] = xid;
			}
		}
		else/* sync left rxact xid from client */
		{
			SnapSenderXidArrayAddXid(SNAPSENDER_XID_ARRAY_XACT2P, xid);
			cn_txids[txid_cn_count++] = xid;
			SNAP_SYNC_DEBUG_LOG((errmsg("SnapSenderProcessInitSyncRequest Add rxact xid %u\n",
							xid)));
		}
	}

	list_free(xid_list);
	if (array_2pc_len > 0 || txid_cn_count > 0)
	{
		SpinLockAcquire(&SnapSender->gxid_mutex);
		for (i = 0; i < array_2pc_len; i++)
		{
			found = false;
			for (index = 0; index <SnapSender->xcnt; index++)
			{
				if (SnapSender->xip[index] == xid_2pc_array[i])
				{
					found = true;
					break;
				}
			}

			if (!found)
			{
				SnapSender->xip[SnapSender->xcnt++] = xid_2pc_array[i];
				SNAP_SYNC_DEBUG_LOG((errmsg("SnapSenderProcessInitSyncRequest real Add 2pc  id %u\n",
							xid_2pc_array[i])));
				SnapSenderXidArrayAddXid(SNAPSENDER_XID_ARRAY_ASSIGN, xid_2pc_array[i]);
				SNAP_SYNC_DEBUG_LOG((errmsg("SnapSenderProcessInitSyncRequest Add 2pc assign xid %u\n", xid_2pc_array[i])));
			}
		}

		for (i = 0; i < txid_cn_count; i++)
		{
			found = false;
			for (index = 0; index <SnapSender->xcnt; index++)
			{
				if (SnapSender->xip[index] == cn_txids[i])
				{
					found = true;
					break;
				}
			}

			if (!found)
			{
				SnapSender->xip[SnapSender->xcnt++] = cn_txids[i];
				SNAP_SYNC_DEBUG_LOG((errmsg("SnapSenderProcessInitSyncRequest real Add rxact/local_assing  id %u\n",
							cn_txids[i])));
				SnapSenderXidArrayAddXid(SNAPSENDER_XID_ARRAY_ASSIGN, cn_txids[i]);
				SNAP_SYNC_DEBUG_LOG((errmsg("SnapSenderProcessInitSyncRequest Add rxact/local_assing assign xid %u\n", cn_txids[i])));
			}
		}
		SpinLockRelease(&SnapSender->gxid_mutex);
	}

	SnapSenderDropXidList(client, cn_txids, txid_cn_count);

	SetLatch(&MyProc->procLatch);
	if (list_len > 0)
	{
		pfree(cn_txids);
		pfree(xid_2pc_array);
	}
	pfree(xid_str_org);
	return;
}

static void SnapSenderCheckOldClientList(SnapClientData *client)
{
	slist_mutable_iter		siter;
	SnapClientData			*client_item;

	slist_foreach_modify(siter, &slist_all_client)
	{
		client_item = slist_container(SnapClientData, snode, siter.cur);
		if(!strcasecmp(client_item->client_name, client->client_name) &&
			pq_get_socket(client_item->node) != pq_get_socket(client->node))
		{
			SNAP_SYNC_DEBUG_LOG((errmsg("SnapSenderCheckOldClientList drop old clientname %s\n",
			 			client_item->client_name)));
			SnapSenderTransferCnClientToFailledList(client_item);
			slist_delete_current(&siter);
		}
	}
}

static void SnapSenderSendInitSnapShot(SnapClientData *client)
{
	/* send snapshot */
	static StringInfoData	iss_buffer;

	initStringInfo(&iss_buffer);
	appendStringInfoChar(&iss_buffer, 's');
	pq_sendint32(&iss_buffer, snapsenderGetSenderGlobalXmin());

	SerializeActiveTransactionIds(&iss_buffer);

	AppendMsgToClient(client, 'd', iss_buffer.data, iss_buffer.len);
	pfree(iss_buffer.data);
}

static void WakeAllCnClientStream(void)
{
	slist_iter siter;
	SnapClientData *client;

	slist_foreach(siter, &slist_all_client)
	{
		client = slist_container(SnapClientData, snode, siter.cur);
		if (client->status == CLIENT_STATUS_CN_WAIT)
		{
			Assert(!client->is_dn);
			SnapSenderSendInitSnapShot(client);
			client->status = CLIENT_STATUS_STREAMING;
		}
	}
}

static void OnClientRecvMsg(SnapClientData *client, pq_comm_node *node, time_t* time_last_latch)
{
	int						ret_ssc;
	TransactionId			next_id;
	int						msgtype, cmdtype;
	const char				*client_name PG_USED_FOR_ASSERTS_ONLY;
	bool					is_cn = false;
	char					*xid_string_list;
	char					next_id_str[1024];

	if (pq_node_recvbuf(node) != 0)
	{
		ereport(ERROR,
				(errmsg("client closed stream %s", client->client_name)));
	}

	while (1)
	{
		resetStringInfo(&input_buffer);
		msgtype = pq_node_get_msg(&input_buffer, node);

		client->last_msg = GetCurrentTimestamp();
		switch(msgtype)
		{
		case 'Q':
			/* only support "START_REPLICATION" command */
			//START_REPLICATION SLOT ""cn1"" LOGICAL 0/0 (proto_version '725', publication_names '""100"",""200""')
			SNAP_SYNC_DEBUG_LOG((errmsg("Q input_buffer.data %s\n", input_buffer.data)));
			if (strncasecmp(input_buffer.data, "START_REPLICATION", strlen("START_REPLICATION")) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						errposition(0),
						errmsg("only support \"START_REPLICATION 0/0 TIMELINE 0\" command")));

			/* Send a CopyBothResponse message, and start streaming */
			resetStringInfo(&output_buffer);
			pq_sendbyte(&output_buffer, 0);
			pq_sendint16(&output_buffer, 0);
			AppendMsgToClient(client, 'W', output_buffer.data, output_buffer.len);

			xid_string_list = (char *)palloc0(input_buffer.len * sizeof(char));
			ret_ssc = sscanf(input_buffer.data, "%*s %*s \"%[^\" ]\" %*s %*s %*s \'%[^\']\', %*s \'%[^\']\')",
							client->client_name, next_id_str, xid_string_list);
			next_id = pg_strtouint64(next_id_str, NULL, 10);
			SNAP_SYNC_DEBUG_LOG((errmsg("ret_ssc %d, client->client_name %s,client->status %d, next_id_str %s,next_id %u, xid_string_list %s， client->cur_cnt %u\n", 
						ret_ssc, client->client_name, client->status, next_id_str, next_id, xid_string_list, client->cur_cnt)));
			if (ret_ssc > 0)
			{
				is_cn = snapsenderGetIsCnConn(client);
				client->is_dn = !is_cn;
				SnapSenderCheckOldClientList(client);

				if (!client->is_dn)
					SnapSenderRecoveryXidListFromCnFailledList(client);

				/* for dn, if gtm dose not restart, we don't need to sync xid from xid. As snapsend store all xid which has assigned to cn*/
				if (!client->is_dn || pg_atomic_read_u32(&SnapSender->dn_conn_state) != SNAPSENDER_ALL_DNMASTER_CONN_OK)
					SnapSenderProcessInitSyncRequest(client, xid_string_list);

				SNAP_SYNC_DEBUG_LOG((errmsg("SnapSender got init sync request from %s\n", client->client_name)));
				snapsenderProcessNextXid(client, next_id);
			}
			pfree(xid_string_list);

			SNAP_SYNC_DEBUG_LOG((errmsg("client->client_name %s, is_cn %d, SnapSender->dn_conn_state %d, SnapSender->state %d\n",
									client->client_name, is_cn, pg_atomic_read_u32(&SnapSender->dn_conn_state), pg_atomic_read_u32(&SnapSender->state))));
			if (adb_check_sync_nextid && is_cn &&
				(pg_atomic_read_u32(&SnapSender->dn_conn_state) != SNAPSENDER_ALL_DNMASTER_CONN_OK
				|| pg_atomic_read_u32(&SnapSender->state) != SNAPSENDER_STATE_OK))
			{
				SNAP_SYNC_DEBUG_LOG((errmsg("get cn conn request, but not all dn or cn master conn ok\n")));
				client->status = CLIENT_STATUS_CN_WAIT;
				return;
			}

			/* send snapshot */
			SnapSenderSendInitSnapShot(client);
			client->status = CLIENT_STATUS_STREAMING;

			/* all cn and dn conn is OK*/
			if (pg_atomic_read_u32(&SnapSender->state) == SNAPSENDER_STATE_OK &&
			pg_atomic_read_u32(&SnapSender->dn_conn_state) == SNAPSENDER_ALL_DNMASTER_CONN_OK)
			{
				/* clear 2pc and rxact sync list*/
				xid_xact2pc_count = 0;
			}
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
						errmsg("client %s not in copy mode", client->client_name)));
			}
			if (msgtype == 'c')
				client->status = CLIENT_STATUS_CONNECTED;
			else
			{
				cmdtype = pq_getmsgbyte(&input_buffer);
				if (cmdtype == 'h')
				{
					snapsenderProcessHeartBeat(client);
				}
				else if (cmdtype == 'p')
				{
					OnLatchSetEvent(NULL, time_last_latch);
					snapsenderProcessSyncRequest(client);
				}
				else if (cmdtype == 'g') /* assing one xid */
				{
					client_name = pq_getmsgstring(&input_buffer);
					Assert(!strcasecmp(client->client_name, client_name));
					SnapSenderProcessAssignGxid(client);
				}
				else if (cmdtype == 'q') /* pre-alloc xid array */
				{
					client_name = pq_getmsgstring(&input_buffer);
					Assert(!strcasecmp(client->client_name, client_name));
					SnapSenderProcessPreAssignGxidArray(client);
				}
				else if (cmdtype == 'c')
				{
					client_name = pq_getmsgstring(&input_buffer);
					Assert(!strcasecmp(client->client_name, client_name));
					SnapSenderProcessFinishGxid(client);
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

static void SnapSenderQuickDieHander(SIGNAL_ARGS)
{
	if (proc_exit_inprogress)
		return;

	proc_exit_inprogress = true;
	PG_SETMASK(&BlockSig);

	on_exit_reset();

	exit(2);
}

void SnapSendAddXip(TransactionId txid, int txidnum, TransactionId parent)
{
	int i = 0;
	TransactionId  xid, xid_tmp;

	Assert(TransactionIdIsValid(txid));
	Assert(TransactionIdIsNormal(txid));
	if (!IsGTMNode())
		return;

	Assert(SnapSender != NULL);
	if (TransactionIdIsValid(parent))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("snapshot sender not support sub transaction yet!")));

	xid_tmp = txid;
	SpinLockAcquire(&SnapSender->gxid_mutex);
	for (i = txidnum; i > 0; i--)
	{
		xid = xid_tmp--;
		SNAP_SYNC_DEBUG_LOG((errmsg("Call SnapSend add xip xid %u\n",
							xid)));
		SnapSender->xip[SnapSender->xcnt++] = xid--;
	}
	SpinLockRelease(&SnapSender->gxid_mutex);
}

void SnapSendTransactionAssign(TransactionId txid, int txidnum, TransactionId parent)
{
	int i = 0;
	TransactionId  xid, xid_tmp;

	Assert(TransactionIdIsValid(txid));
	Assert(TransactionIdIsNormal(txid));
	if (!IsGTMNode())
		return;

	Assert(SnapSender != NULL);
	if (TransactionIdIsValid(parent))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("snapshot sender not support sub transaction yet!")));

	xid_tmp = txid;
	SpinLockAcquire(&SnapSender->mutex);
	for (i = txidnum; i > 0; i--)
	{
		if(SnapSender->cur_cnt_assign == MAX_CNT_SHMEM_XID_BUF)
			WaitSnapCommonShmemSpace(&SnapSender->mutex,
								&SnapSender->cur_cnt_assign,
								&SnapSender->waiters_assign, true);
		Assert(SnapSender->cur_cnt_assign < MAX_CNT_SHMEM_XID_BUF);

		xid = xid_tmp--;
		SNAP_SYNC_DEBUG_LOG((errmsg("Call SnapSend assging xid %u\n",
							xid)));
		SnapSender->xid_assign[SnapSender->cur_cnt_assign++] = xid;
	}

	if (SnapSender->procno != INVALID_PGPROCNO)
		SetLatch(&(GetPGProcByNumber(SnapSender->procno)->procLatch));
	SpinLockRelease(&SnapSender->mutex);
}

void SnapSendTransactionFinish(TransactionId txid, SnapXidFinishOption finish_flag)
{
	if(!TransactionIdIsValid(txid) || !IsGTMNode())
		return;

	Assert(TransactionIdIsNormal(txid));
	Assert(SnapSender != NULL);

	if (finish_flag & SNAP_XID_RXACT)
		isSnapSenderAllCnDnConnOk(true);

	SpinLockAcquire(&SnapSender->mutex);
	if (SnapSender->procno == INVALID_PGPROCNO)
	{
		SpinLockRelease(&SnapSender->mutex);
		return;
	}

	SNAP_SYNC_DEBUG_LOG((errmsg("Call SnapSend finish xid %u finish_flag %d\n",
			 			txid, finish_flag)));

	if(SnapSender->cur_cnt_complete == MAX_CNT_SHMEM_XID_BUF)
		WaitSnapCommonShmemSpace(&SnapSender->mutex,
							   &SnapSender->cur_cnt_complete,
							   &SnapSender->waiters_complete, true);
	Assert(SnapSender->cur_cnt_complete < MAX_CNT_SHMEM_XID_BUF);
	SnapSender->xid_complete[SnapSender->cur_cnt_complete++] = txid;
	SetLatch(&(GetPGProcByNumber(SnapSender->procno)->procLatch));

	SpinLockRelease(&SnapSender->mutex);

	if (finish_flag & SNAP_XID_RXACT)
	{
		SpinLockAcquire(&SnapSender->gxid_mutex);
		SnapSenderDropXidItem(txid);
		SpinLockRelease(&SnapSender->gxid_mutex);
		SnapSenderXidArrayRemoveXid(SNAPSENDER_XID_ARRAY_XACT2P, txid);
	}
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

static bool isSnapSenderAllCnDnConnOk(bool is_wait)
{
	uint32 dn_state, cn_state;


	cn_state = pg_atomic_read_u32(&SnapSender->state);
	dn_state = pg_atomic_read_u32(&SnapSender->dn_conn_state);
	if (likely(SNAPSENDER_ALL_DNMASTER_CONN_OK == dn_state &&
				SNAPSENDER_STATE_OK == cn_state))
		return true;

	/* when is_wait is false, and snapsender connection state is not ok, we should return derectly*/
	if (is_wait == false)
		return false;

	if (dn_state != SNAPSENDER_ALL_DNMASTER_CONN_OK)
	{
		for(;;)
		{
			ConditionVariableSleep(&SnapSender->cv_dn_con, WAIT_EVENT_SAFE_SNAPSHOT);
			dn_state = pg_atomic_read_u32(&SnapSender->dn_conn_state);
			SNAP_SYNC_DEBUG_LOG((errmsg("isSnapSenderAllCnDnConnOk SnapSender->dn_conn_state %d\n", dn_state)));
			if (dn_state == SNAPSENDER_ALL_DNMASTER_CONN_OK)
				break;
		}
		ConditionVariableCancelSleep();
	}

	if (cn_state != SNAPSENDER_STATE_OK)
	{
		for(;;)
		{
			ConditionVariableSleep(&SnapSender->cv, WAIT_EVENT_SAFE_SNAPSHOT);
			cn_state = pg_atomic_read_u32(&SnapSender->state);
			SNAP_SYNC_DEBUG_LOG((errmsg("isSnapSenderAllCnDnConnOk SnapSender->state %d\n", cn_state)));
			if (cn_state == SNAPSENDER_STATE_OK)
				break;
		}
		ConditionVariableCancelSleep();
	}
	return true;
}

Snapshot SnapSenderGetSnapshot(Snapshot snap, TransactionId *xminOld, TransactionId* xmaxOld,
			int *countOld)
{
	TransactionId	xid,xmax,xmin;
	uint32			i,xcnt;
	bool			update_xmin = false;
	bool			is_wait_ok = false;

	if (RecoveryInProgress() || !adb_check_sync_nextid || !is_need_check_dn_coon)
		return snap;

	/* when gtmc get snapshot, we musk make sure all dn has synced two phase xid */
	if (is_need_check_dn_coon && adb_check_sync_nextid &&
			!IsAutoVacuumWorkerProcess() && !is_snapsender_query_worker)
	{
		is_wait_ok = isSnapSenderAllCnDnConnOk(false);
		if (is_wait_ok == false)
			return NULL;
	}

	if (snap->xip == NULL)
		EnlargeSnapshotXip(snap, GetMaxSnapshotXidCount());

	SpinLockAcquire(&SnapSender->gxid_mutex);

	if (!TransactionIdIsNormal(SnapSender->latestCompletedXid))
	{
		SpinLockRelease(&SnapSender->gxid_mutex);
		return snap;
	}

	if (SnapSender->xcnt > 0 && (SnapSender->xcnt + GetMaxSnapshotXidCount()) > snap->max_xcnt)
		EnlargeSnapshotXip(snap, SnapSender->xcnt + GetMaxSnapshotXidCount());

	xmax = SnapSender->latestCompletedXid;
	Assert(TransactionIdIsNormal(xmax));
	TransactionIdAdvance(xmax);

	xmin = xmax;
	xcnt = *countOld;
	if (NormalTransactionIdFollows(xmax, *xmaxOld))
		*xmaxOld = xmax;

	for (i = 0; i < SnapSender->xcnt; ++i)
	{
		xid = SnapSender->xip[i];

		/* if XID is >= xmax, we can skip it */
		if (!NormalTransactionIdPrecedes(xid, *xmaxOld))
			continue;

		if (NormalTransactionIdPrecedes(xid, xmin))
			xmin = xid;

		/* We don't include our own XIDs (if any) in the snapshot */
		if (xid == MyProc->xid)
			continue;

		/* Add XID to snapshot. */
		snap->xip[xcnt++] = xid;
		update_xmin = true;
	}

	if ((update_xmin && NormalTransactionIdPrecedes(xmin, *xminOld))
			|| (*countOld == 0 && *xmaxOld == xmax))
		*xminOld = xmin;

	*countOld = xcnt;
	SpinLockRelease(&SnapSender->gxid_mutex);

#ifdef USE_ASSERT_CHECKING
	for(i=0;i<xcnt;++i)
	{
		Assert(!NormalTransactionIdFollows(*xminOld, snap->xip[i]));
	}
#endif /* USE_ASSERT_CHECKING */

	return snap;
}

void SnapSenderTransferLock(void **param, TransactionId xid, struct PGPROC *from)
{
	SnapTransferLock(&SnapSender->comm_lock, param, xid, from);
}

void SnapSenderGetStat(StringInfo buf)
{
	int				i;
	TransactionId	*assign_xids = NULL;
	uint32			assign_len;
	TransactionId	*finish_xids = NULL;
	uint32			finish_len;
	TransactionId	*assign_gxid_xids = NULL;
	uint32			assign_gxid_xids_len;

	assign_len = finish_len = assign_gxid_xids_len = XID_ARRAY_STEP_SIZE;
	assign_xids = NULL;
	finish_xids = NULL;
	assign_gxid_xids = NULL;

re_lock_mutex:
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
		goto re_lock_mutex;
	}

	assign_len = SnapSender->cur_cnt_assign;
	if (assign_len > 0)
		memcpy(assign_xids, SnapSender->xid_assign, sizeof(TransactionId)*assign_len);

	finish_len = SnapSender->cur_cnt_complete;
	if (finish_len > 0)
		memcpy(finish_xids, SnapSender->xid_complete, sizeof(TransactionId)*finish_len);
	SpinLockRelease(&SnapSender->mutex);

re_lock_gxid_mutex:
	if (!assign_gxid_xids)
		assign_gxid_xids = palloc0(sizeof(TransactionId) * assign_gxid_xids_len);
	else
		assign_gxid_xids = repalloc(assign_gxid_xids, sizeof(TransactionId) * assign_gxid_xids_len);

	SpinLockAcquire(&SnapSender->gxid_mutex);
	if (assign_gxid_xids_len < SnapSender->xcnt)
	{
		SpinLockRelease(&SnapSender->gxid_mutex);
		assign_gxid_xids_len += XID_ARRAY_STEP_SIZE;
		goto re_lock_gxid_mutex;
	}

	assign_gxid_xids_len = SnapSender->xcnt;
	if (assign_gxid_xids_len > 0)
		memcpy(assign_gxid_xids, SnapSender->xip, sizeof(TransactionId)*assign_gxid_xids_len);
	SpinLockRelease(&SnapSender->gxid_mutex);


	appendStringInfo(buf, " state: %d \n", pg_atomic_read_u32(&SnapSender->state));
	appendStringInfo(buf, " local global xmin: %u\n", pg_atomic_read_u32(&SnapSender->global_xmin));
#warning TODO get GetOldestXminExt
	//appendStringInfo(buf, " local oldest_xmin: %u\n", GetOldestXminExt(NULL, PROCARRAY_FLAGS_VACUUM, true));
	appendStringInfo(buf, " nextid_upcount: %d \n", pg_atomic_read_u32(&SnapSender->nextid_upcount));
	appendStringInfo(buf, " nextid_upcount_cn: %d \n", pg_atomic_read_u32(&SnapSender->nextid_upcount_cn));
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

	appendStringInfo(buf, "\n current gxid assign: %u \n", assign_gxid_xids_len);
	appendStringInfo(buf, "  xid_xip: [");

	qsort(assign_gxid_xids, assign_gxid_xids_len, sizeof(TransactionId), xidComparator);
	for (i = 0; i < assign_gxid_xids_len; i++)
	{
		appendStringInfo(buf, "%u ", assign_gxid_xids[i]);
		if (i > 0 && i % XID_PRINT_XID_LINE_NUM == 0)
			appendStringInfo(buf, "\n  ");
	}
	appendStringInfo(buf, "]");

	pfree(assign_xids);
	pfree(finish_xids);
	pfree(assign_gxid_xids);
}

void isSnapSenderWaitNextIdOk(void)
{
	uint32 state;

	state = pg_atomic_read_u32(&SnapSender->state);
	if (likely(SNAPSENDER_STATE_OK == state))
		return;
	
	for(;;)
	{
		ConditionVariableSleep(&SnapSender->cv, WAIT_EVENT_SAFE_SNAPSHOT);
		state = pg_atomic_read_u32(&SnapSender->state);
		SNAP_SYNC_DEBUG_LOG((errmsg("isSnapSenderWaitNextIdOk SnapSender->state %d\n", state)));
		if (state == SNAPSENDER_STATE_OK)
			break;
	}
	ConditionVariableCancelSleep();
}