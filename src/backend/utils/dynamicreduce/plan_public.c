#include "postgres.h"

#include "access/htup_details.h"
#include "miscadmin.h"
#include "utils/dynamicreduce.h"
#include "utils/dr_private.h"
#include "utils/memutils.h"

static HTAB		   *htab_plan_info = NULL;

bool DRSendPlanWorkerMessage(PlanWorkerInfo *pwi, PlanInfo *pi)
{
	shm_mq_result result;

	if (pwi->sendBuffer.len > 0)
	{
		Assert(pwi->sendBuffer.cursor == 0);
		result = shm_mq_send(pwi->reduce_sender,
							 pwi->sendBuffer.len,
							 pwi->sendBuffer.data,
							 true);
		if (result == SHM_MQ_SUCCESS)
		{
			DR_PLAN_DEBUG((errmsg("send plan %d worker %d with data length %d success",
								  pi->plan_id, pwi->worker_id, pwi->sendBuffer.len)));
			pwi->sendBuffer.len = 0;
			return true;
		}else if (result == SHM_MQ_DETACHED)
		{
			ereport(ERROR,
					(errmsg("plan %d parallel %d MQ detached",
							pi->plan_id, pwi->worker_id)));
		}
#ifdef USE_ASSERT_CHECKING
		else
		{
			Assert(result == SHM_MQ_WOULD_BLOCK);
		}
#endif /* USE_ASSERT_CHECKING */
	}

	return false;
}

bool DRRecvPlanWorkerMessage(PlanWorkerInfo *pwi, PlanInfo *pi)
{
	unsigned char  *addr,*saved_addr;
	MinimalTuple	mtup;
	HeapTupleData	tup;
	MemoryContext	oldcontext;
	Size			size;
	shm_mq_result	result;
	int				msg_type;
	uint32			msg_head;

	if (pwi->end_of_plan_recv ||
		pwi->last_data != NULL)
		return false;

	result = shm_mq_receive(pwi->worker_sender, &size, (void**)&addr, true);
	if (result == SHM_MQ_WOULD_BLOCK)
	{
		return false;
	}else if(result == SHM_MQ_DETACHED)
	{
		pwi->end_of_plan_recv = true;
		ereport(ERROR,
				(errmsg("plan %d parallel %d MQ detached",
						pi->plan_id, pwi->worker_id)));
	}
	Assert(result == SHM_MQ_SUCCESS);
	if (size < sizeof(msg_head))
		goto invalid_plan_message_;

	msg_head = *(uint32*)addr;
	msg_type = (msg_head >> 24) & 0xff;
	DR_PLAN_DEBUG((errmsg("plan %d got message %d from MQ size %zu head %08x",
						  pi->plan_id, msg_type, size, msg_head)));
	if (msg_type == ADB_DR_MSG_TUPLE)
	{
		saved_addr = addr;

		pwi->dest_cursor = 0;
		pwi->dest_count = (msg_head & 0xffffff);
		addr += sizeof(msg_head);

		pwi->dest_oids = (Oid*)addr;
		addr += sizeof(Oid)*pwi->dest_count;
		if ((addr - saved_addr) >= size)
			goto invalid_plan_message_;
		pwi->last_size = size - (addr - saved_addr);
		pwi->last_data = addr;
		pwi->last_msg_type = ADB_DR_MSG_TUPLE;

		if (pi->type_convert)
		{
			MemoryContextReset(pi->convert_context);
			oldcontext = MemoryContextSwitchTo(pi->convert_context);

			DRStoreTypeConvertTuple(pwi->slot_plan_src, pwi->last_data, pwi->last_size, &tup);
			do_type_convert_slot_out(pi->type_convert, pwi->slot_plan_src, pwi->slot_plan_dest, false);
			mtup = ExecFetchSlotMinimalTuple(pwi->slot_plan_dest);
			ExecClearTuple(pwi->slot_plan_src);

			pwi->last_size = mtup->t_len - MINIMAL_TUPLE_DATA_OFFSET;
			pwi->last_data = (char*)mtup + MINIMAL_TUPLE_DATA_OFFSET;

			MemoryContextSwitchTo(oldcontext);
		}

		return true;
	}else if(msg_type == ADB_DR_MSG_END_OF_PLAN)
	{
		pwi->last_msg_type = ADB_DR_MSG_END_OF_PLAN;
		return true;
	}

invalid_plan_message_:
	pwi->end_of_plan_recv = true;
	ereport(ERROR,
			(errmsg("Invalid MQ message format plan %d parallel %d", pi->plan_id, pwi->worker_id)));
	return false;	/* keep compiler quiet */
}

TupleTableSlot* DRStoreTypeConvertTuple(TupleTableSlot *slot, const char *data, uint32 len, HeapTuple head)
{
	MinimalTuple mtup;
	if (((Size)data - MINIMAL_TUPLE_DATA_OFFSET) % MAXIMUM_ALIGNOF == 0)
	{
		head->t_len = len - (MINIMAL_TUPLE_OFFSET + MINIMAL_TUPLE_DATA_OFFSET);
		head->t_data = (HeapTupleHeader)((char*)data - (MINIMAL_TUPLE_OFFSET + MINIMAL_TUPLE_DATA_OFFSET));
		ExecStoreTuple(head, slot, InvalidBuffer, false);
	}else
	{
		mtup = palloc(len + MINIMAL_TUPLE_DATA_OFFSET);
		mtup->t_len = len + MINIMAL_TUPLE_DATA_OFFSET;
		memcpy((char*)mtup + MINIMAL_TUPLE_DATA_OFFSET, data, len);
		ExecStoreMinimalTuple(mtup, slot, false);
	}

	return slot;
}

/* active waiting plan */
void ActiveWaitingPlan(DRNodeEventData *ned)
{
	PlanInfo		   *pi;
	HASH_SEQ_STATUS		seq_status;
	bool				hint = false;

	DRPlanSeqInit(&seq_status);
	while ((pi = hash_seq_search(&seq_status)) != NULL)
	{
		if (pi->waiting_node == ned->nodeoid)
		{
			DR_PLAN_DEBUG((errmsg("activing plan %d by node %u", pi->plan_id, ned->nodeoid)));
			(*pi->OnNodeIdle)(pi, NULL, ned);
			hint = true;
		}
	}

	if (hint)
		SetLatch(MyLatch);
}

void DRSetupPlanTypeConvert(PlanInfo *pi, TupleDesc desc)
{
	MemoryContext	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	pi->type_convert = create_type_convert(desc, true, true);
	if (pi->type_convert)
	{
		pi->convert_context = AllocSetContextCreate(TopMemoryContext,
													"plan tuple convert",
													ALLOCSET_DEFAULT_SIZES);
	}else
	{
		pi->convert_context = NULL;
	}
	MemoryContextSwitchTo(oldcontext);
}

void DRSetupPlanWorkTypeConvert(PlanInfo *pi, PlanWorkerInfo *pwi)
{
	MemoryContext oldcontext;
	if (pi->type_convert == NULL)
		return;
	
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	pwi->slot_plan_src = MakeSingleTupleTableSlot(pi->type_convert->base_desc);
	pwi->slot_plan_dest = MakeSingleTupleTableSlot(pi->type_convert->out_desc);
	pwi->slot_node_src = MakeSingleTupleTableSlot(pi->type_convert->out_desc);
	pwi->slot_node_dest = MakeSingleTupleTableSlot(pi->type_convert->base_desc);

	MemoryContextSwitchTo(oldcontext);
}

void DRInitPlanSearch(void)
{
	HASHCTL ctl;
	if (htab_plan_info == NULL)
	{
		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(int);
		ctl.entrysize = sizeof(PlanInfo);
		ctl.hash = uint32_hash;
		ctl.hcxt = TopMemoryContext;
		htab_plan_info = hash_create("Dynamic reduce plan info",
									 DR_HTAB_DEFAULT_SIZE,
									 &ctl,
									 HASH_ELEM|HASH_CONTEXT|HASH_FUNCTION);
	}
}

PlanInfo* DRPlanSearch(int planid, HASHACTION action, bool *found)
{
	return hash_search(htab_plan_info, &planid, action, found);
}

bool DRPlanSeqInit(HASH_SEQ_STATUS *seq)
{
	if (htab_plan_info)
	{
		hash_seq_init(seq, htab_plan_info);
		return true;
	}
	return false;
}
