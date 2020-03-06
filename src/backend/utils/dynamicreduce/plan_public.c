#include "postgres.h"

#include "access/htup_details.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "utils/dynamicreduce.h"
#include "utils/dr_private.h"
#include "utils/memutils.h"

static HTAB		   *htab_plan_info = NULL;

static bool DRSendPlanWorkerMessageInternal(PlanWorkerInfo *pwi, PlanInfo *pi, bool on_failed)
{
	shm_mq_result result;
	bool sended = false;

re_send_:
	if (pwi->sendBuffer.len > 0)
	{
		Assert(pwi->sendBuffer.cursor == 0);
		result = shm_mq_send_ext(pwi->reduce_sender,
								 pwi->sendBuffer.len,
								 pwi->sendBuffer.data,
								 true, false);
		if (result == SHM_MQ_SUCCESS)
		{
			DR_PLAN_DEBUG((errmsg("send plan %d worker %d with data length %d success",
								  pi->plan_id, pwi->worker_id, pwi->sendBuffer.len)));
			pwi->sendBuffer.len = 0;
			sended = true;
		}else if (result == SHM_MQ_DETACHED)
		{
			if (on_failed)
			{
				pwi->sendBuffer.len = 0;
				sended = true;
			}else
			{
				ereport(ERROR,
						(errmsg("plan %d parallel %d MQ detached", pi->plan_id, pwi->worker_id),
						 DRKeepError()));
			}
		}
		else if(result == SHM_MQ_WOULD_BLOCK)
		{
			return sended;
		}else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("unknown shm_mq_send result %d", result)));
		}
	}

	Assert(pwi->sendBuffer.len == 0);
	switch(pwi->plan_send_state)
	{
	case DR_PLAN_SEND_WORKING:
	case DR_PLAN_SEND_ENDED:
		break;
	case DR_PLAN_SEND_GENERATE_CACHE:
		if (on_failed == false &&	/* on failed state, don't send cache message, we need quick end plan */
			pi->GenerateCacheMsg &&
			pi->GenerateCacheMsg(pwi, pi))
		{
			Assert(pwi->sendBuffer.len > 0);
			pwi->plan_send_state = DR_PLAN_SEND_SENDING_CACHE;
			goto re_send_;
		}
		/* do not add break, need generate EOF message */
	case DR_PLAN_SEND_SENDING_CACHE:
	case DR_PLAN_SEND_GENERATE_EOF:
		appendStringInfoChar(&pwi->sendBuffer, ADB_DR_MSG_END_OF_PLAN);
		if (pwi->plan_recv_state == DR_PLAN_RECV_WAITING_ATTACH)
		{
			/* parallel */
			Assert(pi->local_eof == true);
			appendStringInfoChar(&pwi->sendBuffer, pi->local_eof);
		}
		pwi->plan_send_state = DR_PLAN_SEND_SENDING_EOF;
		goto re_send_;
	case DR_PLAN_SEND_SENDING_EOF:
		pwi->plan_send_state = DR_PLAN_SEND_ENDED;
		break;
	default:
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("unknown worker sending state %u", pwi->plan_send_state)));
		break;	/* never run */
	}

	return sended;
}

bool DRSendPlanWorkerMessage(PlanWorkerInfo *pwi, PlanInfo *pi)
{
	return DRSendPlanWorkerMessageInternal(pwi, pi, false);
}

bool DRRecvPlanWorkerMessage(PlanWorkerInfo *pwi, PlanInfo *pi)
{
	unsigned char  *addr,*saved_addr;
	Size			size;
	shm_mq_result	result;
	int				msg_type;
	uint32			msg_head;

	if (pwi->plan_recv_state == DR_PLAN_RECV_ENDED ||
		pwi->last_data != NULL)
		return false;

	result = shm_mq_receive(pwi->worker_sender, &size, (void**)&addr, true);
	if (result == SHM_MQ_WOULD_BLOCK)
	{
		return false;
	}else if(result == SHM_MQ_DETACHED)
	{
		pwi->plan_recv_state = DR_PLAN_RECV_ENDED;
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

		return true;
	}else if(msg_type == ADB_DR_MSG_END_OF_PLAN)
	{
		DR_PLAN_DEBUG_EOF((errmsg("plan %d worker %d got end of plan message from backend",
								  pi->plan_id, pwi->worker_id)));
		pwi->last_msg_type = ADB_DR_MSG_END_OF_PLAN;
		return true;
	}else if(msg_type == ADB_DR_MSG_ATTACH_PLAN)
	{
		if (pwi->plan_recv_state != DR_PLAN_RECV_WAITING_ATTACH)
		{
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("paln %d worker %d is not in waiting attach state",
					 		pi->plan_id, pwi->worker_id)));
		}
		DR_PLAN_DEBUG_ATTACH((errmsg("plan %d worker %d got attach message", pi->plan_id, pwi->worker_id)));
		pwi->plan_recv_state = DR_PLAN_RECV_WORKING;
		pwi->last_msg_type = ADB_DR_MSG_ATTACH_PLAN;
		return true;
	}

invalid_plan_message_:
	pwi->plan_recv_state = DR_PLAN_RECV_ENDED;
	ereport(ERROR,
			(errmsg("Invalid MQ message format plan %d parallel %d", pi->plan_id, pwi->worker_id)));
	return false;	/* keep compiler quiet */
}

void DRSendWorkerMsgToNode(PlanWorkerInfo *pwi, PlanInfo *pi, DRNodeEventData *ned)
{
	uint32			i,count;
	if (pwi->last_msg_type == ADB_DR_MSG_INVALID)
		return;

	for (i=pwi->dest_cursor,count=pwi->dest_count; i<count; ++i)
	{
		if (ned == NULL ||
			pwi->dest_oids[i] != ned->nodeoid)
			ned = DRSearchNodeEventData(pwi->dest_oids[i], HASH_FIND, NULL);

		if (ned == NULL ||
			PutMessageToNode(ned,
							 pwi->last_msg_type,
							 pwi->last_data,
							 pwi->last_size,
							 pi->plan_id) == false)
		{
			pwi->dest_cursor = i;
			pi->waiting_node = pwi->waiting_node = pwi->dest_oids[i];
			return;
		}
	}
	pwi->last_msg_type = ADB_DR_MSG_INVALID;
	pwi->last_data = NULL;
	pi->waiting_node = pwi->waiting_node = InvalidOid;
}

void DRSerializePlanInfo(int plan_id, dsm_segment *seg, void *addr, Size size, List *work_nodes, StringInfo buf)
{
	Size		offset;
	ListCell   *lc;
	OidBufferData oids;
	dsm_handle	handle;

	Assert(plan_id >= 0);
	if (list_length(work_nodes) == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("invalid work nodes fro dynamic reduce plan %d", plan_id)));
	}
	Assert(IsA(work_nodes, OidList));

	handle = dsm_segment_handle(seg);
	offset = (char*)addr - (char*)dsm_segment_address(seg);
	Assert((char*)addr >= (char*)dsm_segment_address(seg));
	Assert(offset + size <= dsm_segment_map_length(seg));

	pq_sendbytes(buf, (char*)&plan_id, sizeof(plan_id));
	pq_sendbytes(buf, (char*)&handle, sizeof(handle));
	pq_sendbytes(buf, (char*)&offset, sizeof(offset));
	initOidBufferEx(&oids, list_length(work_nodes), CurrentMemoryContext);
	foreach(lc, work_nodes)
		appendOidBufferUniqueOid(&oids, lfirst_oid(lc));
	pq_sendbytes(buf, (char*)&oids.len, sizeof(uint32));
	pq_sendbytes(buf, (char*)oids.oids, sizeof(Oid) * oids.len);
	pfree(oids.oids);
}

PlanInfo* DRRestorePlanInfo(StringInfo buf, void **shm, Size size, void(*clear)(PlanInfo*))
{
	Size			offset;
	dsm_handle		handle;
	PlanInfo * volatile
					pi = NULL;
	int				plan_id;
	uint32			node_count;
	Oid				oid;
	bool			found;

	PG_TRY();
	{
		pq_copymsgbytes(buf, (char*)&plan_id, sizeof(plan_id));
		pq_copymsgbytes(buf, (char*)&handle, sizeof(handle));
		pq_copymsgbytes(buf, (char*)&offset, sizeof(offset));

		if (plan_id < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("invalid plan ID %d", plan_id)));

		pi = DRPlanSearch(plan_id, HASH_ENTER, &found);
		if (found)
		{
			pi = NULL;
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_IN_USE),
					 errmsg("plan ID %d is in use", plan_id)));
		}
		MemSet(pi, 0, sizeof(*pi));
		pi->plan_id = plan_id;
		pi->OnDestroy = clear;

		if ((pi->seg = dsm_find_mapping(handle)) == NULL)
			pi->seg = dsm_attach(handle);
		if (offset + size > dsm_segment_map_length(pi->seg))
		{
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("invalid shared memory offset or size of DSM")));
		}
		*shm = ((char*)dsm_segment_address(pi->seg)) + offset;

		initOidBuffer(&pi->end_of_plan_nodes);
		pq_copymsgbytes(buf, (char*)&node_count, sizeof(node_count));
		if (node_count == 0 ||
			node_count > dr_latch_data->work_oid_buf.len+1)
		{
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("invalid work node length %u", node_count)));
		}
		initOidBufferEx(&pi->working_nodes, node_count, CurrentMemoryContext);
		found = false;
		while (node_count > 0)
		{
			--node_count;
			pq_copymsgbytes(buf, (char*)&oid, sizeof(Oid));
			if (oid == PGXCNodeOid)
			{
				found = true;
				continue;
			}

			if (oidBufferMember(&pi->working_nodes, oid, NULL))
			{
				ereport(ERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("replicate node %u for plan %d", oid, plan_id)));
			}
			if (oidBufferMember(&dr_latch_data->work_oid_buf, oid, NULL) == false)
			{
				ereport(ERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("node %u not in dynamic reduce work", oid)));
			}

			appendOidBufferOid(&pi->working_nodes, oid);
		}
		if (found == false)
		{
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("our node %u not found in plan %d for dynamic reduce",
							PGXCNodeOid, plan_id)));
		}
	}PG_CATCH();
	{
		if (pi)
			(*pi->OnDestroy)(pi);
		PG_RE_THROW();
	}PG_END_TRY();

	return pi;
}

void DRSetupPlanWorkInfo(PlanInfo *pi, PlanWorkerInfo *pwi, DynamicReduceMQ mq, int worker_id, uint8 recv_state)
{
	pwi->worker_id = worker_id;
	pwi->waiting_node = InvalidOid;
	pwi->plan_recv_state = recv_state;
	pwi->plan_send_state = DR_PLAN_SEND_WORKING;

	shm_mq_set_receiver((shm_mq*)mq->worker_sender_mq, MyProc);
	shm_mq_set_sender((shm_mq*)mq->reduce_sender_mq, MyProc);
	pwi->worker_sender = shm_mq_attach((shm_mq*)mq->worker_sender_mq, pi->seg, NULL);
	pwi->reduce_sender = shm_mq_attach((shm_mq*)mq->reduce_sender_mq, pi->seg, NULL);
	initStringInfo(&pwi->sendBuffer);
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

void DRPlanSeqInit(HASH_SEQ_STATUS *seq)
{
	Assert(htab_plan_info);
	hash_seq_init(seq, htab_plan_info);
}

long DRCurrentPlanCount(void)
{
	if (htab_plan_info == NULL)
		return 0;
	return hash_get_num_entries(htab_plan_info);
}

void DRClearPlanWorkInfo(PlanInfo *pi, PlanWorkerInfo *pwi)
{
	if (pwi == NULL)
		return;
	if (pwi->sendBuffer.data)
		pfree(pwi->sendBuffer.data);
	if (pwi->reduce_sender)
		shm_mq_detach(pwi->reduce_sender);
	if (pwi->worker_sender)
		shm_mq_detach(pwi->worker_sender);
	if (pwi->slot_node_dest)
		ExecDropSingleTupleTableSlot(pwi->slot_node_dest);
	if (pwi->slot_node_src)
		ExecDropSingleTupleTableSlot(pwi->slot_node_src);
	if (pwi->slot_plan_dest)
		ExecDropSingleTupleTableSlot(pwi->slot_plan_dest);
	if (pwi->slot_plan_src)
		ExecDropSingleTupleTableSlot(pwi->slot_plan_src);
}

void DRClearPlanInfo(PlanInfo *pi)
{
	if (pi == NULL)
		return;

	if (pi->end_of_plan_nodes.oids)
	{
		pfree(pi->end_of_plan_nodes.oids);
		pi->end_of_plan_nodes.oids = NULL;
	}
	if (pi->working_nodes.oids)
	{
		pfree(pi->working_nodes.oids);
		pi->working_nodes.oids = NULL;
	}
	if (pi->seg)
	{
		HASH_SEQ_STATUS seq;
		PlanInfo *tmp;
		bool found = false;

		hash_seq_init(&seq, htab_plan_info);
		while ((tmp=hash_seq_search(&seq)) != NULL)
		{
			if (tmp != pi &&
				tmp->seg == pi->seg)
			{
				found = true;
				hash_seq_term(&seq);
				break;
			}
		}

		if (found == false)
			dsm_detach(pi->seg);
		pi->seg = NULL;
	}
}

void OnDefaultPlanPreWait(PlanInfo *pi)
{
	PlanWorkerInfo *pwi = pi->pwi;
	if (pwi->plan_recv_state == DR_PLAN_RECV_ENDED &&
		pwi->last_msg_type == ADB_DR_MSG_INVALID &&
		pwi->plan_send_state == DR_PLAN_SEND_ENDED)
		(*pi->OnDestroy)(pi);
}

void OnDefaultPlanLatch(PlanInfo *pi)
{
	PlanWorkerInfo *pwi;
	uint32			msg_type;

	pwi = pi->pwi;
	if (DRSendPlanWorkerMessage(pwi, pi))
		DRActiveNode(pi->plan_id);

	while (pwi->waiting_node == InvalidOid &&
		   pwi->plan_recv_state == DR_PLAN_RECV_WORKING &&
		   pwi->last_msg_type == ADB_DR_MSG_INVALID)
	{
		if (DRRecvPlanWorkerMessage(pwi, pi) == false)
			break;
		msg_type = pwi->last_msg_type;

		if (msg_type == ADB_DR_MSG_END_OF_PLAN)
		{
			pwi->plan_recv_state = DR_PLAN_RECV_ENDED;
			DRGetEndOfPlanMessage(pi, pwi);
		}else
		{
			Assert(msg_type == ADB_DR_MSG_TUPLE);
		}

		/* send message to remote */
		DRSendWorkerMsgToNode(pwi, pi, NULL);
	}
}

void OnDefaultPlanIdleNode(PlanInfo *pi, WaitEvent *w, DRNodeEventData *ned)
{
	PlanWorkerInfo *pwi = pi->pwi;;
	if (pwi->last_msg_type == ADB_DR_MSG_INVALID)
		return;
	if (pwi->dest_oids[pwi->dest_cursor] != ned->nodeoid)
		return;

	DRSendWorkerMsgToNode(pwi, pi, ned);
}

/***************************failed functions *******************/
static void OnPlanFailedLatchSet(PlanInfo *pi)
{
	DRSendPlanWorkerMessageInternal(pi->pwi, pi, true);
}

static void OnParallelPlanFailedLatchSet(PlanInfo *pi)
{
	uint32 i = pi->count_pwi;
	while (i>0)
		DRSendPlanWorkerMessageInternal(&pi->pwi[--i], pi, true);
}

static bool OnPlanFailedRecvedData(PlanInfo *pi, const char *data, int len, Oid nodeoid)
{
	return true;
}

static void OnPlanFailedNodeIdle(PlanInfo *pi, WaitEvent *we, DRNodeEventData *ned)
{
	/* nothing todo */
}

static bool OnPlanFailedEndOfPlan(PlanInfo *pi, Oid nodeoid)
{
	return true;
}

static void OnPlanFailedError(PlanInfo *pi)
{
	/* nothing todo */
}

void SetPlanFailedFunctions(PlanInfo *pi, bool send_eof, bool parallel)
{
	PlanWorkerInfo *pwi;
	uint32			i;
	pi->OnLatchSet = parallel ? OnParallelPlanFailedLatchSet : OnPlanFailedLatchSet;
	pi->OnNodeRecvedData = OnPlanFailedRecvedData;
	pi->OnNodeIdle = OnPlanFailedNodeIdle;
	pi->OnNodeEndOfPlan = OnPlanFailedEndOfPlan;
	pi->OnPlanError = OnPlanFailedError;
	pi->OnPreWait = NULL;

	if (send_eof)
	{
		i = parallel ? pi->count_pwi : 1;
		while (i>0)
		{
			pwi = &pi->pwi[--i];
			if (pwi->plan_send_state < DR_PLAN_SEND_GENERATE_CACHE)
				pwi->plan_send_state = DR_PLAN_SEND_GENERATE_CACHE;
		}
	}
}