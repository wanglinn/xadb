#include "postgres.h"

#include "access/htup_details.h"
#include "executor/clusterReceiver.h"
#include "executor/tuptable.h"
#include "libpq/pqmq.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/dsa.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

#include "utils/dynamicreduce.h"
#include "utils/dr_private.h"

#define DR_DSA_DEFAULT_SIZE			(1024*1024)		/* 1M */

dsm_segment *dr_mem_seg = NULL;
shm_mq_handle *dr_mq_backend_sender = NULL;
shm_mq_handle *dr_mq_worker_sender = NULL;
SharedFileSet *dr_shared_fs = NULL;
dsa_area	  *dr_dsa = NULL;
static uint32 dr_shared_fs_num = 0U;

#if (defined DR_USING_EPOLL) || (defined WITH_REDUCE_RDMA) 
static void dr_wait_latch(void)
{
	sigset_t	sigmask;
	sigset_t	origmask;

	if (MyLatch->is_set)
		return;
	sigprocmask(0, NULL, &sigmask);
	sigaddset(&sigmask, SIGUSR1);
	sigprocmask(SIG_SETMASK, &sigmask, &origmask);
	while(MyLatch->is_set == false)
		sigsuspend(&origmask);

	sigprocmask(SIG_SETMASK, &origmask, NULL);
}
#endif

static void check_error_message_from_reduce(void)
{
	Size			size;
	void		   *data;
	shm_mq_result	result;

	if (dr_mq_worker_sender == NULL)
		return;
		
re_get_:
	result = shm_mq_receive(dr_mq_worker_sender, &size, &data, true);
	if (result == SHM_MQ_WOULD_BLOCK)
	{
		return;
	}else if (result == SHM_MQ_DETACHED)
	{
		ereport(ERROR,
				(errmsg("receive message from dynamic reduce failed: MQ detached")));
	}
	Assert(result == SHM_MQ_SUCCESS);
	if (DynamicReduceHandleMessage(data, size))
		goto re_get_;
	
	/* should not run to here */
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("got a not to be received message type %d from dynamic reduce", *(char*)data)));
}

static uint8 recv_msg_from_plan(shm_mq_handle *mqh, Size *sizep, void **datap, DynamicReduceRecvInfo *info)
{
	shm_mq_result	result;
	unsigned char  *addr;
	Size			size;

	result = shm_mq_receive(mqh, &size, (void**)&addr, true);
	if (result == SHM_MQ_WOULD_BLOCK)
	{
		return ADB_DR_MSG_INVALID;
	}else if (result == SHM_MQ_DETACHED)
	{
		ereport(ERROR,
				(errmsg("can not receive message from dynamic reduce for plan: MQ detached")));
	}

	Assert(result == SHM_MQ_SUCCESS);
	Assert(size > 0);
	switch(addr[0])
	{
	case ADB_DR_MSG_TUPLE:
		Assert(size > 8);
		if (info)
			info->u32 = *((Oid*)(addr+4));
		*datap = addr+8;
		*sizep = size-8;
		break;
	case ADB_DR_MSG_SHARED_FILE_NUMBER:
		Assert(size == 8);
		if (info)
			info->u32 = *((uint32*)(addr+4));
		*datap = NULL;
		*sizep = 0;
		break;
	case ADB_DR_MSG_END_OF_PLAN:
		Assert(size == sizeof(addr[0]) || size == sizeof(addr[0])*2);
		*sizep = 0;
		*datap = NULL;
		if (info)
			MemSet(info, 0, sizeof(*info));
		break;
	case ADB_DR_MSG_SHARED_TUPLE_STORE:
		Assert(size == SIZEOF_DSA_POINTER*2);
		*sizep = 0;
		*datap = NULL;
		if (info)
			info->dp = *((dsa_pointer*)(addr+SIZEOF_DSA_POINTER));
		break;
	default:
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("unknown MQ message type %d from dynamic reduce for plan", addr[0])));
		break;	/* never run */
	}

	return addr[0];
}

static inline bool send_msg_to_plan(shm_mq_handle *mqh, shm_mq_iovec *iov)
{
	shm_mq_result result = shm_mq_sendv(mqh, iov, 1, true);
	if (result == SHM_MQ_SUCCESS)
		return true;
	if (result == SHM_MQ_WOULD_BLOCK)
		return false;

	Assert(result == SHM_MQ_DETACHED);
	ereport(ERROR,
			(errmsg("can not send message to dynamic reduce for plan: MQ detached")));

	return false;	/* never run */
}

static void set_slot_data(void *data, Size size, TupleTableSlot *slot, StringInfo buf)
{
	MinimalTuple	tuple;

	if (data == NULL)
	{
		Assert(size == 0);
		ExecClearTuple(slot);
	}else
	{
		buf->len = buf->cursor = 0;	/* inline resetStringInfo() */
		enlargeStringInfo(buf, size + MINIMAL_TUPLE_DATA_OFFSET);
		Assert(((Size)buf->data) % MAXIMUM_ALIGNOF == 0);

		tuple = (MinimalTuple)buf->data;
		buf->len = tuple->t_len = size + MINIMAL_TUPLE_DATA_OFFSET;
		memcpy((char*)tuple + MINIMAL_TUPLE_DATA_OFFSET, data, size);
		ExecStoreMinimalTuple(tuple, slot, false);
	}
}

/*
 * result:
 *   DR_MSG_INVALID: would block
 *   DR_MSG_RECV: data saved in slot, node oid saved in info
 *   DR_MSG_RECV_SF: shared file number saved in info
 *   DR_MSG_RECV_STS: shared tuplestore saved in info
 */
uint8
DynamicReduceRecvTuple(shm_mq_handle *mqh, struct TupleTableSlot *slot, StringInfo buf,
					   DynamicReduceRecvInfo *info, bool nowait)
{
	void		   *data;
	Size			size;
#if (!defined DR_USING_EPOLL) && (!defined WITH_REDUCE_RDMA)
	WaitEvent		event;
#endif
	uint8			result;

	result = recv_msg_from_plan(mqh, &size, &data, info);
	if (result == ADB_DR_MSG_INVALID)
	{
		check_error_message_from_reduce();
		if (nowait == true)
			return DR_MSG_INVALID;
	}

	while(result == ADB_DR_MSG_INVALID)
	{
#if (defined DR_USING_EPOLL) || (defined WITH_REDUCE_RDMA) 
		dr_wait_latch();
#else
		WaitEventSetWait(dr_wait_event_set, -1, &event, 1, WAIT_EVENT_MQ_RECEIVE);
#endif /* DR_USING_EPOLL */
		ResetLatch(&MyProc->procLatch);
		CHECK_FOR_INTERRUPTS();

		check_error_message_from_reduce();
		result = recv_msg_from_plan(mqh, &size, &data, info);
	}

	Assert(result != ADB_DR_MSG_INVALID);
	switch(result)
	{
	case ADB_DR_MSG_TUPLE:
	case ADB_DR_MSG_END_OF_PLAN:
		set_slot_data(data, size, slot, buf);
		return DR_MSG_RECV;
	case ADB_DR_MSG_SHARED_FILE_NUMBER:
		return DR_MSG_RECV_SF;
	case ADB_DR_MSG_SHARED_TUPLE_STORE:
		return DR_MSG_RECV_STS;
	default:
		elog(ERROR, "unknown message type %u from dynamic reduce", result);
		break;
	}

	return DR_MSG_INVALID;	/* keep compiler quiet */
}

int DynamicReduceSendOrRecvTuple(shm_mq_handle *mqsend, shm_mq_handle *mqrecv,
								 StringInfo send_buf, struct TupleTableSlot *slot_recv,
								 StringInfo recv_buf, DynamicReduceRecvInfo *info)
{
	void		   *data;
	Size			size;
	shm_mq_iovec	iov;
#if (!defined DR_USING_EPOLL) && (!defined WITH_REDUCE_RDMA)
	WaitEvent		event;
#endif
	int				flags = 0;
	uint8			msg_type;

	Assert(send_buf->len > 0);
	iov.data = send_buf->data;
	iov.len = send_buf->len;

	for (;;)
	{
		/* try send first */
		if (send_msg_to_plan(mqsend, &iov))
			flags |= DR_MSG_SEND;

		/* and try recv */
		msg_type = recv_msg_from_plan(mqrecv, &size, &data, info);
		switch(msg_type)
		{
		case ADB_DR_MSG_INVALID:
			break;
		case ADB_DR_MSG_TUPLE:
		case ADB_DR_MSG_END_OF_PLAN:
			set_slot_data(data, size, slot_recv, recv_buf);
			flags |= DR_MSG_RECV;
			break;
		case ADB_DR_MSG_SHARED_FILE_NUMBER:
			flags |= DR_MSG_RECV_SF;
			break;
		case ADB_DR_MSG_SHARED_TUPLE_STORE:
			flags |= DR_MSG_RECV_STS;
			break;
		default:
			elog(ERROR, "unknown message type from dynamic reduce");
			break;
		}
		
		if (flags)
			return flags;

		check_error_message_from_reduce();
#if (defined DR_USING_EPOLL) || (defined WITH_REDUCE_RDMA) 
		dr_wait_latch();
#else
		WaitEventSetWait(dr_wait_event_set, -1, &event, 1, WAIT_EVENT_MQ_INTERNAL);
#endif
		ResetLatch(&MyProc->procLatch);
		CHECK_FOR_INTERRUPTS();
	}

	return flags;
}

bool DynamicReduceSendMessage(shm_mq_handle *mqh, Size nbytes, void *data, bool nowait)
{
	shm_mq_iovec	iov;
#if (!defined DR_USING_EPOLL) && (!defined WITH_REDUCE_RDMA)
	WaitEvent		event;
#endif

	iov.data = data;
	iov.len = nbytes;
	if (send_msg_to_plan(mqh, &iov))
		return true;
	if (nowait)
		return false;
	
	for (;;)
	{
		check_error_message_from_reduce();

#if (defined DR_USING_EPOLL) || (defined WITH_REDUCE_RDMA) 
		dr_wait_latch();
#else
		WaitEventSetWait(dr_wait_event_set, -1, &event, 1, WAIT_EVENT_MQ_INTERNAL);
#endif
		ResetLatch(&MyProc->procLatch);
		CHECK_FOR_INTERRUPTS();

		if (send_msg_to_plan(mqh, &iov))
			return true;
	}

	return false;	/* never run, keep compiler quiet */
}

/* return EOF local */
bool DynamicReduceNotifyAttach(shm_mq_handle *mq_send, shm_mq_handle *mq_recv,
							   uint8 *remote, DynamicReduceRecvInfo *info)
{
	static const uint32 msg = (ADB_DR_MSG_ATTACH_PLAN << 24);
	shm_mq_iovec iov = {(const char*)&msg, sizeof(msg)};
	shm_mq_result mq_result;
#if (!defined DR_USING_EPOLL) && (!defined WITH_REDUCE_RDMA)
	WaitEvent event;
#endif

re_send_:
	mq_result = shm_mq_sendv(mq_send, &iov, 1, true);
	if (mq_result == SHM_MQ_WOULD_BLOCK)
	{

#if (defined DR_USING_EPOLL) || (defined WITH_REDUCE_RDMA) 
		dr_wait_latch();
#else
		WaitEventSetWait(dr_wait_event_set, -1, &event, 1, WAIT_EVENT_MQ_SEND);
#endif
		ResetLatch(&MyProc->procLatch);
		CHECK_FOR_INTERRUPTS();
		check_error_message_from_reduce();
		goto re_send_;
	}
	/*
	 * when mq detached, maybe dynamic reduce closed plan,
	 * we shoud also can get an EOF message
	 */
	Assert(mq_result == SHM_MQ_SUCCESS || mq_result == SHM_MQ_DETACHED);

	iov.len = 0;
	iov.data = NULL;
re_get_:
	mq_result = shm_mq_receive(mq_recv, &iov.len, (void**)&iov.data, true);
	if (mq_result == SHM_MQ_WOULD_BLOCK)
	{
#if (defined DR_USING_EPOLL) || (defined WITH_REDUCE_RDMA) 
		dr_wait_latch();
#else
		WaitEventSetWait(dr_wait_event_set, -1, &event, 1, WAIT_EVENT_MQ_RECEIVE);
#endif
		ResetLatch(&MyProc->procLatch);
		CHECK_FOR_INTERRUPTS();
		check_error_message_from_reduce();
		goto re_get_;
	}else if (mq_result == SHM_MQ_DETACHED)
	{
		ereport(ERROR,
				(errmsg("can not receive message from dynamic reduce for plan: MQ detached")));
	}
	Assert(mq_result == SHM_MQ_SUCCESS && iov.len>0);

	switch(iov.data[0])
	{
	case ADB_DR_MSG_ATTACH_PLAN:
	case ADB_DR_MSG_END_OF_PLAN:
		Assert(iov.len == sizeof(iov.data[0])*2);
		break;
	case ADB_DR_MSG_SHARED_FILE_NUMBER:
		Assert(iov.len == 8);
		info->u32 = *((uint32*)(iov.data+4));
		break;
	case ADB_DR_MSG_SHARED_TUPLE_STORE:
		Assert(iov.len == SIZEOF_DSA_POINTER*2);
		info->dp = *((dsa_pointer*)(iov.data+SIZEOF_DSA_POINTER));
		break;
	default:
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("unknown MQ attach message type %d from dynamic reduce for plan", iov.data[0])));
		break;
	}
	*remote = iov.data[0];
	return iov.data[1];
}

bool DRSendMsgToReduce(const char *data, Size len, bool nowait)
{
#if (!defined DR_USING_EPOLL) && (!defined WITH_REDUCE_RDMA)
	WaitEvent		event;
#endif
	shm_mq_result	result;
	shm_mq_iovec	iov;

	iov.data = data;
	iov.len = len;

	result = shm_mq_sendv(dr_mq_backend_sender, &iov, 1, true);
	if (result == SHM_MQ_WOULD_BLOCK && nowait)
		return false;

	while (result != SHM_MQ_SUCCESS)
	{
		if (result == SHM_MQ_DETACHED)
		{
			ereport(ERROR,
					(errmsg("send message to dynamic reduce failed: MQ detached")));
		}

		Assert(result == SHM_MQ_WOULD_BLOCK);
		check_error_message_from_reduce();

#if (defined DR_USING_EPOLL) || (defined WITH_REDUCE_RDMA) 
		dr_wait_latch();
#else
		WaitEventSetWait(dr_wait_event_set, -1, &event, 1, WAIT_EVENT_MQ_SEND);
#endif
		ResetLatch(&MyProc->procLatch);
		CHECK_FOR_INTERRUPTS();

		result = shm_mq_sendv(dr_mq_backend_sender, &iov, 1, true);
	}

	Assert(result == SHM_MQ_SUCCESS);
	return true;
}

bool DRRecvMsgFromReduce(Size *sizep, void **datap, bool nowait)
{
#if (!defined DR_USING_EPOLL) && (!defined WITH_REDUCE_RDMA)
	WaitEvent		event;
#endif
	shm_mq_result	result;

re_get_:
	result = shm_mq_receive(dr_mq_worker_sender, sizep, datap, true);
	if (result == SHM_MQ_WOULD_BLOCK && nowait)
		return false;

	while (result != SHM_MQ_SUCCESS)
	{
		if (result == SHM_MQ_DETACHED)
		{
			ereport(ERROR,
					(errmsg("receive message from dynamic reduce failed: MQ detached")));
		}

		Assert(result == SHM_MQ_WOULD_BLOCK);
#if (defined DR_USING_EPOLL) || (defined WITH_REDUCE_RDMA) 
		dr_wait_latch();
#else
		WaitEventSetWait(dr_wait_event_set, -1, &event, 1, WAIT_EVENT_MQ_RECEIVE);
#endif
		ResetLatch(&MyProc->procLatch);
		CHECK_FOR_INTERRUPTS();

		result = shm_mq_receive(dr_mq_worker_sender, sizep, datap, true);
	}

	if (result == SHM_MQ_SUCCESS)
	{
		if (DynamicReduceHandleMessage(*datap, *sizep))
			goto re_get_;
	}

	Assert(result == SHM_MQ_SUCCESS);

	return true;
}

bool DRSendMsgToBackend(const char *data, Size len, bool nowait)
{
	shm_mq_iovec	iov;

	iov.data = data;
	iov.len = len;
	switch(shm_mq_sendv(dr_mq_worker_sender, &iov, 1, nowait))
	{
	case SHM_MQ_SUCCESS:
		return true;
	case SHM_MQ_WOULD_BLOCK:
		return false;
	case SHM_MQ_DETACHED:
		ereport(ERROR,
				(errmsg("can not send message to backend: MQ detached")));
		break;
	}

	return false;	/* should never run */
}

bool DRRecvMsgFromBackend(Size *sizep, void **datap, bool nowait)
{
	switch( shm_mq_receive(dr_mq_backend_sender, sizep, datap, nowait))
	{
	case SHM_MQ_SUCCESS:
		return true;
	case SHM_MQ_WOULD_BLOCK:
		return false;
	case SHM_MQ_DETACHED:
		ereport(ERROR,
				(errmsg("can not receive message from backend: MQ detached")));
		break;
	}

	return false;	/* should never run */
}

bool DRSendConfirmToBackend(bool nowait)
{
	static const char confirm_msg[1] = {ADB_DR_MQ_MSG_CONFIRM};
	return DRSendMsgToBackend(confirm_msg, sizeof(confirm_msg), nowait);
}

bool DRRecvConfirmFromReduce(bool nowait)
{
	void   *data;
	Size	size;

	if (DRRecvMsgFromReduce(&size, &data, nowait) == false)
		return false;
	
	if (size != 1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid length %zu for confirm message from dynamic reduce", size),
				 size > 0 ? errdetail("first char is %u", *(unsigned char*)data) : 0));
	}
	if (*(unsigned char*)data != ADB_DR_MQ_MSG_CONFIRM)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("invalid message type %d from dynamic reduce", *(unsigned char*)data),
				 errhint("expect message %d", ADB_DR_MQ_MSG_CONFIRM)));
	}
	return true;
}

void SerializeEndOfPlanMessage(StringInfo buf)
{
	static const uint32 msg = (ADB_DR_MSG_END_OF_PLAN << 24);
	appendBinaryStringInfoNT(buf, (const char*)&msg, sizeof(msg));
}

bool SendEndOfPlanMessageToMQ(shm_mq_handle *mqh, bool nowait)
{
	static const uint32 msg = (ADB_DR_MSG_END_OF_PLAN << 24);
	shm_mq_iovec iov;
	iov.data = (const char*)&msg;
	iov.len = sizeof(msg);

	return send_msg_to_plan(mqh, &iov);
}

bool SendRejectPlanMessageToMQ(shm_mq_handle *mqh, bool nowait)
{
#warning TODO SendRejectPlanMessageToMQ
	return true;
}

void ResetDynamicReduceWork(void)
{
	static Size msg_magic = 0;
	static const char reset_msg[1] = {ADB_DR_MQ_MSG_RESET};
	Size			size;
	char		   *data;
#ifndef DR_USING_EPOLL
	//WaitEvent		event;
#endif
	shm_mq_result	result;
	shm_mq_iovec	iov[2];

	if (dr_mq_backend_sender == NULL)
		return;

	msg_magic++;
	iov[0].data = reset_msg;
	iov[0].len = sizeof(reset_msg);
	iov[1].data = (char*)&msg_magic;
	iov[1].len = sizeof(msg_magic);
	result = shm_mq_sendv(dr_mq_backend_sender, iov, lengthof(iov), true);
	while(result != SHM_MQ_SUCCESS)
	{
		if (result == SHM_MQ_DETACHED)
		{
			StopDynamicReduceWorker();
			return;
		}
		Assert(result == SHM_MQ_WOULD_BLOCK);

		/* try error message */
		result = shm_mq_receive(dr_mq_worker_sender, &size, (void**)&data, true);
		if (result == SHM_MQ_DETACHED)
		{
			StopDynamicReduceWorker();
			return;
		}/*else(result == SHM_MQ_SUCCESS)
		{
		}*/

#if (defined DR_USING_EPOLL) || (defined WITH_REDUCE_RDMA) 
		dr_wait_latch();
#else
		WaitEventSetWait(dr_wait_event_set, -1, &event, 1, WAIT_EVENT_MQ_SEND);
#endif
		ResetLatch(&MyProc->procLatch);
		CHECK_FOR_INTERRUPTS();

		result = shm_mq_sendv(dr_mq_backend_sender, iov, lengthof(iov), true);
	}

	/* wait message */
reget_reset_msg_:
	result = shm_mq_receive(dr_mq_worker_sender, &size, (void**)&data, true);
	while(result != SHM_MQ_SUCCESS)
	{
		if (result == SHM_MQ_DETACHED)
		{
			StopDynamicReduceWorker();
			return;
		}
#if (defined DR_USING_EPOLL) || (defined WITH_REDUCE_RDMA) 
		dr_wait_latch();
#else
		WaitEventSetWait(dr_wait_event_set, -1, &event, 1, WAIT_EVENT_MQ_RECEIVE);
#endif
		ResetLatch(&MyProc->procLatch);
		CHECK_FOR_INTERRUPTS();

		result = shm_mq_receive(dr_mq_worker_sender, &size, (void**)&data, true);
	}
	if (data[0] != ADB_DR_MQ_MSG_RESET ||
		memcmp(&data[1], &msg_magic, sizeof(msg_magic)) != 0)
		goto reget_reset_msg_;
}

void SerializeDynamicReducePlanData(StringInfo buf, const void *data, uint32 len, struct OidBufferData *target)
{
	uint32		count;
	Assert(len > 0);

	count = target->len;
	if (count == 0 ||
		(count & 0xff000000) != 0)
	{
		ereport(ERROR,
				(errmsg("invalid remote node count %u", count)));
	}
	count |= (ADB_DR_MSG_TUPLE << 24);
	appendBinaryStringInfoNT(buf, (char*)&count, sizeof(count));
	appendBinaryStringInfoNT(buf, (char*)target->oids, sizeof(Oid)*target->len);
	appendBinaryStringInfoNT(buf, data, len);
}

void SerializeDynamicReduceSlot(StringInfo buf, struct TupleTableSlot *slot, struct OidBufferData *target)
{
	MinimalTuple	tuple;
	bool			need_free;

	resetStringInfo(buf);
	tuple = fetch_slot_message(slot, &need_free);
	SerializeDynamicReducePlanData(buf,
								   (char*)tuple + MINIMAL_TUPLE_DATA_OFFSET,
								   tuple->t_len - MINIMAL_TUPLE_DATA_OFFSET,
								   target);
	if (need_free)
		pfree(tuple);
}

void DRSetupShmem(void)
{
	Size			size;
	MemoryContext	oldcontext;
	ResourceOwner	saved_owner;

	saved_owner = CurrentResourceOwner;;
	CurrentResourceOwner = NULL;
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	/*
	 * Create shared memory segment,
	 * We need two message queues, one for backend and one for worker
	 */
	size = add_size(MAXALIGN(ADB_DYNAMIC_REDUCE_QUERY_SIZE),
					MAXALIGN(ADB_DYNAMIC_REDUCE_QUERY_SIZE));
	size = add_size(size, MAXALIGN(sizeof(SharedFileSet)));
	size = add_size(size, MAXALIGN(DR_DSA_DEFAULT_SIZE));

	dr_mem_seg = dsm_create(size, 0);

	MemoryContextSwitchTo(oldcontext);
	CurrentResourceOwner = saved_owner;

	DRResetShmem();
}

void DRResetShmem(void)
{
	MemoryContext	oldcontext;
	ResourceOwner	saved_owner;
	char		   *addr;
	shm_mq		   *mq[2];

	saved_owner = CurrentResourceOwner;;
	CurrentResourceOwner = NULL;
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	Assert(dr_mem_seg != NULL);
	if (dr_mq_backend_sender)
	{
		shm_mq_detach(dr_mq_backend_sender);
		dr_mq_backend_sender = NULL;
	}
	if (dr_mq_worker_sender)
	{
		shm_mq_detach(dr_mq_worker_sender);
		dr_mq_worker_sender = NULL;
	}
	if (dr_shared_fs)
	{
		SharedFileSetDetach(dr_shared_fs, dr_mem_seg);
		dr_shared_fs = NULL;
	}

	addr = dsm_segment_address(dr_mem_seg);

	/* two shm_mq */
	mq[0] = shm_mq_create(addr, MAXALIGN(ADB_DYNAMIC_REDUCE_QUERY_SIZE));
	addr += MAXALIGN(ADB_DYNAMIC_REDUCE_QUERY_SIZE);
	mq[1] = shm_mq_create(addr, MAXALIGN(ADB_DYNAMIC_REDUCE_QUERY_SIZE));
	addr += MAXALIGN(ADB_DYNAMIC_REDUCE_QUERY_SIZE);

	/* initialize backend sender shm_mq */
	shm_mq_set_sender(mq[ADB_DR_MQ_BACKEND_SENDER], MyProc);
	dr_mq_backend_sender = shm_mq_attach(mq[ADB_DR_MQ_BACKEND_SENDER], dr_mem_seg, NULL);

	/* initialize worker sender shm_mq */
	shm_mq_set_receiver(mq[ADB_DR_MQ_WORKER_SENDER], MyProc);
	dr_mq_worker_sender = shm_mq_attach(mq[ADB_DR_MQ_WORKER_SENDER], dr_mem_seg, NULL);

	/* initialize shared file set */
	SharedFileSetInit((SharedFileSet*)addr, dr_mem_seg);
	dr_shared_fs = (SharedFileSet*)addr;
	addr += MAXALIGN(sizeof(SharedFileSet));

	if (dr_dsa)
	{
		dsa_trim(dr_dsa);
	}else
	{
		dr_dsa = dsa_create_in_place(addr,
									 MAXALIGN(DR_DSA_DEFAULT_SIZE),
									 LWTRANCHE_DYNAMIC_REDUCE_DSA,
									 dr_mem_seg);
		dsa_pin_mapping(dr_dsa);
	}
	addr += MAXALIGN(DR_DSA_DEFAULT_SIZE);

	MemoryContextSwitchTo(oldcontext);
	CurrentResourceOwner = saved_owner;
}

void DRAttachShmem(Datum datum, bool isDynamicReduce)
{
	char		   *addr;
	shm_mq		   *mq[2];
	MemoryContext	oldcontext;

	oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	dr_mem_seg = dsm_attach(DatumGetUInt32(datum));
	if (dr_mem_seg == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not map dynamic shared memory segment")));
	addr = dsm_segment_address(dr_mem_seg);

	mq[0] = (shm_mq*)addr;
	addr += MAXALIGN(ADB_DYNAMIC_REDUCE_QUERY_SIZE);
	mq[1] = (shm_mq*)addr;
	addr += MAXALIGN(ADB_DYNAMIC_REDUCE_QUERY_SIZE);

	if (isDynamicReduce)
	{
		shm_mq_set_receiver(mq[ADB_DR_MQ_BACKEND_SENDER], MyProc);
		dr_mq_backend_sender = shm_mq_attach(mq[ADB_DR_MQ_BACKEND_SENDER], dr_mem_seg, NULL);

		shm_mq_set_sender(mq[ADB_DR_MQ_WORKER_SENDER], MyProc);
		dr_mq_worker_sender = shm_mq_attach(mq[ADB_DR_MQ_WORKER_SENDER], dr_mem_seg, NULL);
	}

	SharedFileSetAttach((SharedFileSet*)addr, dr_mem_seg);
	dr_shared_fs = (SharedFileSet*)addr;
	addr += MAXALIGN(sizeof(SharedFileSet));

	dr_dsa = dsa_attach_in_place(addr, dr_mem_seg);
	dsa_pin_mapping(dr_dsa);
	addr += MAXALIGN(DR_DSA_DEFAULT_SIZE);

	MemoryContextSwitchTo(oldcontext);

	if (isDynamicReduce)
		pq_redirect_to_shm_mq(dr_mem_seg, dr_mq_worker_sender);
}

void DRDetachShmem(void)
{
	void *tmp;
#define MEM_DETACH(mem, fun)\
	if (mem)				\
	{						\
		tmp = mem;			\
		mem = NULL;			\
		fun(tmp);			\
	}

	MEM_DETACH(dr_mq_backend_sender, shm_mq_detach);
	MEM_DETACH(dr_mq_worker_sender, shm_mq_detach);
	/* dr_shared_fs auto detach in dsm_detach() function */
	dr_shared_fs = NULL;
	dr_shared_fs_num = 0U;
	MEM_DETACH(dr_dsa, dsa_detach);
	MEM_DETACH(dr_mem_seg, dsm_detach);

#undef MEM_DETACH
}

dsm_segment* DynamicReduceGetSharedMemory(void)
{
	return dr_mem_seg;
}

SharedFileSet* DynamicReduceGetSharedFileSet(void)
{
	return dr_shared_fs;
}

uint32 DRNextSharedFileSetNumber(void)
{
	return dr_shared_fs_num++;
}

void DRShmemResetSharedFile(void)
{
	if (dr_shared_fs)
	{
		SharedFileSetDeleteAll(dr_shared_fs);
		dr_shared_fs_num = 0;
	}
}

Size EstimateDynamicReduceStateSpace(void)
{
	if (dr_mem_seg == NULL)
		return 0;
	return sizeof(dsm_handle);
}

void SerializeDynamiceReduceState(Size maxsize, char *start_address)
{
	Assert(dr_mem_seg != NULL);
	*(dsm_handle*)start_address = dsm_segment_handle(dr_mem_seg);
}

void RestoreDynamicReduceState(void *state)
{
	dsm_handle handle = *(dsm_handle*)state;
	Assert(handle != DSM_HANDLE_INVALID);
	DRAttachShmem(UInt32GetDatum(handle), false);
}
