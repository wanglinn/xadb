#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "libpq/pqformat.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

#include "utils/dynamicreduce.h"
#include "utils/dr_private.h"

static void OnNormalPlanLatch(PlanInfo *pi);
static bool OnNormalPlanMessage(PlanInfo *pi, const char *data, int len, Oid nodeoid);
static void OnNormalPlanIdleNode(PlanInfo *pi, WaitEvent *we, DRNodeEventData *ned);
static bool OnNormalPlanNodeEndOfPlan(PlanInfo *pi, Oid nodeoid);

static void ClearNormalPlanInfo(PlanInfo *pi)
{
	if (pi == NULL)
		return;
	DR_PLAN_DEBUG((errmsg("clean normal plan %d(%p)", pi->plan_id, pi)));
	DRPlanSearch(pi->plan_id, HASH_REMOVE, NULL);
	if (pi->pwi)
	{
		PlanWorkerInfo *pwi = pi->pwi;
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
		pfree(pi->pwi);
		pi->pwi = NULL;
	}
	if (pi->type_convert)
	{
		TupleDesc desc = pi->type_convert->base_desc;
		free_type_convert(pi->type_convert);
		pi->type_convert = NULL;
		FreeTupleDesc(desc);
	}
	if (pi->convert_context)
	{
		MemoryContextDelete(pi->convert_context);
		pi->convert_context = NULL;
	}
	if (pi->seg)
	{
		dsm_detach(pi->seg);
		pi->seg = NULL;
	}
}

static void OnNormalPlanPreWait(PlanInfo *pi)
{
	PlanWorkerInfo *pwi = pi->pwi;
	if (pwi->end_of_plan_recv &&
		pwi->end_of_plan_send &&
		pwi->last_msg_type == ADB_DR_MSG_INVALID &&
		pwi->sendBuffer.len == 0)
		ClearNormalPlanInfo(pi);
}

static void OnNormalPlanLatch(PlanInfo *pi)
{
	PlanWorkerInfo *pwi;
	uint32			msg_type;

	pwi = pi->pwi;
	if (DRSendPlanWorkerMessage(pwi, pi))
		DRActiveNode(pi->plan_id);

	while (pwi->waiting_node == InvalidOid &&
		   pwi->end_of_plan_recv == false &&
		   pwi->last_msg_type == ADB_DR_MSG_INVALID)
	{
		if (DRRecvPlanWorkerMessage(pwi, pi) == false)
			break;
		msg_type = pwi->last_msg_type;

		if (msg_type == ADB_DR_MSG_END_OF_PLAN)
		{
			pwi->end_of_plan_recv = true;
			DRGetEndOfPlanMessage(pwi);
		}else
		{
			Assert(msg_type == ADB_DR_MSG_TUPLE);
		}

		/* send message to remote */
		pwi->last_msg_type = msg_type;
		OnNormalPlanIdleNode(pi, NULL, NULL);
	}
}

static bool OnNormalPlanMessage(PlanInfo *pi, const char *data, int len, Oid nodeoid)
{
	HeapTupleData	tup;
	PlanWorkerInfo *pwi;
	MinimalTuple	mtup;
	MemoryContext	oldcontext;

	pwi = pi->pwi;
	/* we only cahe one tuple */
	if (pwi->sendBuffer.len != 0)
		return false;

	DR_PLAN_DEBUG((errmsg("normal plan %d(%p) got a tuple from %u length %d",
						  pi->plan_id, pi, nodeoid, len)));
	appendStringInfoChar(&pwi->sendBuffer, ADB_DR_MSG_TUPLE);
	appendStringInfoSpaces(&pwi->sendBuffer, sizeof(nodeoid)-sizeof(char));	/* for align */
	appendBinaryStringInfoNT(&pwi->sendBuffer, (char*)&nodeoid, sizeof(nodeoid));
	if (pi->type_convert)
	{
		MemoryContextSwitchTo(pi->convert_context);
		oldcontext = MemoryContextSwitchTo(pi->convert_context);

		DRStoreTypeConvertTuple(pwi->slot_node_src, data, len, &tup);
		do_type_convert_slot_in(pi->type_convert, pwi->slot_node_src, pwi->slot_node_dest, false);
		mtup = ExecFetchSlotMinimalTuple(pwi->slot_node_dest);
		ExecClearTuple(pwi->slot_node_src);

		appendBinaryStringInfoNT(&pwi->sendBuffer,
								 (char*)mtup + MINIMAL_TUPLE_DATA_OFFSET,
								 mtup->t_len - MINIMAL_TUPLE_DATA_OFFSET);
		MemoryContextSwitchTo(oldcontext);
	}else
	{
		appendBinaryStringInfoNT(&pwi->sendBuffer, data, len);
	}

	DRSendPlanWorkerMessage(pwi, pi);

	return true;
}

static void OnNormalPlanIdleNode(PlanInfo *pi, WaitEvent *we, DRNodeEventData *ned)
{
	PlanWorkerInfo *pwi = pi->pwi;;
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

static bool OnNormalPlanNodeEndOfPlan(PlanInfo *pi, Oid nodeoid)
{
	PlanWorkerInfo *pwi = pi->pwi;
	if (pwi->sendBuffer.len != 0)
		return false;

	DR_PLAN_DEBUG_EOF((errmsg("normal plan %d(%p) got end of plan message from node %u",
						  pi->plan_id, pi, nodeoid)));
	Assert(oidBufferMember(&dr_latch_data->work_oid_buf, nodeoid, NULL));
	appendOidBufferUniqueOid(&pi->end_of_plan_nodes, nodeoid);

	if (pi->end_of_plan_nodes.len == dr_latch_data->work_oid_buf.len)
	{
		DR_PLAN_DEBUG_EOF((errmsg("normal plan %d(%p) sending end of plan message", pi->plan_id, pi)));
		appendStringInfoChar(&pwi->sendBuffer, ADB_DR_MSG_END_OF_PLAN);
		DRSendPlanWorkerMessage(pwi, pi);
		pwi->end_of_plan_send = true;
	}
	return true;
}

void DRStartNormalPlanMessage(StringInfo msg)
{
	PlanInfo * volatile		pi = NULL;
	dsm_segment * volatile	seg = NULL;
	TupleDesc volatile		desc = NULL;
	DynamicReduceMQ			mq;
	Size					offset;
	dsm_handle				handle;
	int						plan_id;
	PlanWorkerInfo		   *pwi;
	MemoryContext			oldcontext;
	ResourceOwner			oldowner;

	if (!IsTransactionState())
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("not in transaction state")));
	}

	PG_TRY();
	{
		pq_copymsgbytes(msg, (char*)&plan_id, sizeof(plan_id));
		pq_copymsgbytes(msg, (char*)&handle, sizeof(handle));
		pq_copymsgbytes(msg, (char*)&offset, sizeof(offset));
		desc = RestoreTupleDesc(msg);
		pq_getmsgend(msg);

		if (plan_id < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					errmsg("invalid plan ID %d", plan_id)));

		pi = DRPlanSearch(plan_id, HASH_FIND, NULL);
		if (pi != NULL)
		{
			pi = NULL;
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_IN_USE),
					errmsg("plan ID %d is in use", plan_id)));
		}

		oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		oldowner = CurrentResourceOwner;
		CurrentResourceOwner = NULL;

		seg = dsm_attach(handle);
		if (offset + sizeof(*mq) > dsm_segment_map_length(seg))
		{
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					errmsg("invalid MQ offset of DSM")));
		}
		mq = (DynamicReduceMQ)((char*)dsm_segment_address(seg)+offset);

		pi = DRPlanSearch(plan_id, HASH_ENTER, NULL);
		MemSet(pi, 0, sizeof(*pi));
		pi->plan_id = plan_id;
		pi->seg = seg;
		seg = NULL;

		pwi = pi->pwi = MemoryContextAllocZero(TopMemoryContext, sizeof(PlanWorkerInfo));
		pwi->worker_id = -1;
		pwi->waiting_node = InvalidOid;
		shm_mq_set_receiver((shm_mq*)mq->worker_sender_mq, MyProc);
		shm_mq_set_sender((shm_mq*)mq->reduce_sender_mq, MyProc);
		pwi->worker_sender = shm_mq_attach((shm_mq*)mq->worker_sender_mq, seg, NULL);
		pwi->reduce_sender = shm_mq_attach((shm_mq*)mq->reduce_sender_mq, seg, NULL);
		CurrentResourceOwner = oldowner;

		initOidBufferEx(&pi->end_of_plan_nodes, OID_BUF_DEF_SIZE, TopMemoryContext);
		initStringInfo(&pwi->sendBuffer);

		DRSetupPlanTypeConvert(pi, desc);
		if (pi->convert_context == NULL)
			FreeTupleDesc(desc);
		desc = NULL;
		DRSetupPlanWorkTypeConvert(pi, pwi);

		pi->OnLatchSet = OnNormalPlanLatch;
		pi->OnNodeRecvedData = OnNormalPlanMessage;
		pi->OnNodeIdle = OnNormalPlanIdleNode;
		pi->OnNodeEndOfPlan = OnNormalPlanNodeEndOfPlan;
		pi->OnPlanError = ClearNormalPlanInfo;
		pi->OnPreWait = OnNormalPlanPreWait;
		pi->OnDestroy = ClearNormalPlanInfo;

		MemoryContextSwitchTo(oldcontext);
		DR_PLAN_DEBUG((errmsg("normal plan %d(%p) stared", pi->plan_id, pi)));
	}PG_CATCH();
	{
		if (pi->type_convert)
			desc = NULL;
		ClearNormalPlanInfo(pi);
		if (seg)
			dsm_detach(seg);
		if (desc)
			FreeTupleDesc(desc);
		PG_RE_THROW();
	}PG_END_TRY();

	DRActiveNode(plan_id);
}

void DynamicReduceStartNormalPlan(int plan_id, dsm_segment *seg, DynamicReduceMQ mq, TupleDesc desc)
{
	Size			offset;
	StringInfoData	buf;
	dsm_handle		handle;
	Assert(plan_id >= 0);

	DRCheckStarted();

	handle = dsm_segment_handle(seg);
	offset = (char*)mq - (char*)dsm_segment_address(seg);
	Assert((char*)mq >= (char*)dsm_segment_address(seg));
	Assert(offset + sizeof(*mq) <= dsm_segment_map_length(seg));

	initStringInfo(&buf);
	pq_sendbyte(&buf, ADB_DR_MQ_MSG_START_PLAN_NORMAL);
	pq_sendbytes(&buf, (char*)&plan_id, sizeof(plan_id));
	pq_sendbytes(&buf, (char*)&handle, sizeof(handle));
	pq_sendbytes(&buf, (char*)&offset, sizeof(offset));
	SerializeTupleDesc(&buf, desc);

	DRSendMsgToReduce(buf.data, buf.len, false);
	pfree(buf.data);

	DRRecvConfirmFromReduce(false);
}
