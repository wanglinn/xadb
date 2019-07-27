#include "postgres.h"

#include "access/htup_details.h"
#include "executor/clusterReceiver.h"
#include "executor/tuptable.h"
#include "libpq/pqmq.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

#include "utils/dynamicreduce.h"
#include "utils/dr_private.h"

dsm_segment *dr_mem_seg = NULL;
shm_mq_handle *dr_mq_backend_sender = NULL;
shm_mq_handle *dr_mq_worker_sender = NULL;

static void check_error_message_from_reduce(void)
{
	Size			size;
	void		   *data;
	shm_mq_result	result;

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

static bool recv_msg_from_plan(shm_mq_handle *mqh, Size *sizep, void **datap, Oid *nodeoid)
{
	shm_mq_result	result;
	unsigned char  *addr;
	Size			size;

	result = shm_mq_receive(mqh, &size, (void**)&addr, true);
	if (result == SHM_MQ_WOULD_BLOCK)
	{
		return false;
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
		if (nodeoid)
			*nodeoid = *((Oid*)(addr+4));
		*datap = addr+8;
		*sizep = size-8;
		break;
	case ADB_DR_MSG_END_OF_PLAN:
		Assert(size == sizeof(addr[0]));
		*sizep = 0;
		*datap = NULL;
		if (nodeoid)
			*nodeoid = InvalidOid;
		break;
	default:
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("unknown MQ message type %d from dynamic reduce for plan", addr[0])));
		break;	/* never run */
	}

	return true;
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

bool
DynamicReduceRecvTuple(shm_mq_handle *mqh, struct TupleTableSlot *slot, StringInfo buf,
					   Oid *nodeoid, bool nowait)
{
	void		   *data;
	Size			size;
	WaitEvent		event;
	bool			result;

	result = recv_msg_from_plan(mqh, &size, &data, nodeoid);
	if (result == false)
	{
		check_error_message_from_reduce();
		if (nowait == true)
			return false;
	}

	while(result == false)
	{
		WaitEventSetWait(dr_wait_event_set, -1, &event, 1, WAIT_EVENT_MQ_RECEIVE);
		ResetLatch(&MyProc->procLatch);
		CHECK_FOR_INTERRUPTS();

		check_error_message_from_reduce();
		result = recv_msg_from_plan(mqh, &size, &data, nodeoid);
	}

	set_slot_data(data, size, slot, buf);

	return true;
}

int DynamicReduceSendOrRecvTuple(shm_mq_handle *mqsend, shm_mq_handle *mqrecv,
								 StringInfo send_buf, struct TupleTableSlot *slot_recv, StringInfo recv_buf)
{
	void		   *data;
	Size			size;
	shm_mq_iovec	iov;
	WaitEvent		event;
	int				flags = 0;

	Assert(send_buf->len > 0);
	iov.data = send_buf->data;
	iov.len = send_buf->len;

	for (;;)
	{
		/* try send first */
		if (send_msg_to_plan(mqsend, &iov))
			flags |= DR_MSG_SEND;

		/* and try recv */
		if (recv_msg_from_plan(mqrecv, &size, &data, NULL))
		{
			set_slot_data(data, size, slot_recv, recv_buf);
			flags |= DR_MSG_RECV;
		}
		
		if (flags)
			return flags;

		check_error_message_from_reduce();

		WaitEventSetWait(dr_wait_event_set, -1, &event, 1, WAIT_EVENT_MQ_INTERNAL);
		ResetLatch(&MyProc->procLatch);
		CHECK_FOR_INTERRUPTS();
	}

	return flags;
}

bool DynamicReduceSendMessage(shm_mq_handle *mqh, Size nbytes, void *data, bool nowait)
{
	shm_mq_iovec	iov;
	WaitEvent		event;

	iov.data = data;
	iov.len = nbytes;
	if (send_msg_to_plan(mqh, &iov))
		return true;
	if (nowait)
		return false;
	
	for (;;)
	{
		check_error_message_from_reduce();

		WaitEventSetWait(dr_wait_event_set, -1, &event, 1, WAIT_EVENT_MQ_INTERNAL);
		ResetLatch(&MyProc->procLatch);
		CHECK_FOR_INTERRUPTS();

		if (send_msg_to_plan(mqh, &iov))
			return true;
	}

	return false;	/* never run, keep compiler quiet */
}

bool DRSendMsgToReduce(const char *data, Size len, bool nowait)
{
	WaitEvent		event;
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

		WaitEventSetWait(dr_wait_event_set, -1, &event, 1, WAIT_EVENT_MQ_SEND);
		ResetLatch(&MyProc->procLatch);
		CHECK_FOR_INTERRUPTS();

		result = shm_mq_sendv(dr_mq_backend_sender, &iov, 1, true);
	}

	Assert(result == SHM_MQ_SUCCESS);
	return true;
}

bool DRRecvMsgFromReduce(Size *sizep, void **datap, bool nowait)
{
	WaitEvent		event;
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
		WaitEventSetWait(dr_wait_event_set, -1, &event, 1, WAIT_EVENT_MQ_RECEIVE);
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
	static const char reset_msg[1] = {ADB_DR_MQ_MSG_RESET};
	if (dr_mq_backend_sender)
	{
		DRSendMsgToReduce(reset_msg, sizeof(reset_msg), false);
		DRRecvConfirmFromReduce(false);
	}
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
	size = add_size(size,
					MAXALIGN(ADB_DYNAMIC_REDUCE_QUERY_SIZE));

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

	MemoryContextSwitchTo(oldcontext);
	CurrentResourceOwner = saved_owner;
}

void DRAttachShmem(Datum datum)
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
	/* addr += MAXALIGN(ADB_DYNAMIC_REDUCE_QUERY_SIZE); */

	shm_mq_set_receiver(mq[ADB_DR_MQ_BACKEND_SENDER], MyProc);
	dr_mq_backend_sender = shm_mq_attach(mq[ADB_DR_MQ_BACKEND_SENDER], dr_mem_seg, NULL);

	shm_mq_set_sender(mq[ADB_DR_MQ_WORKER_SENDER], MyProc);
	dr_mq_worker_sender = shm_mq_attach(mq[ADB_DR_MQ_WORKER_SENDER], dr_mem_seg, NULL);

	MemoryContextSwitchTo(oldcontext);

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
	MEM_DETACH(dr_mem_seg, dsm_detach);

#undef MEM_DETACH
}
