#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "libpq/pqformat.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

#include "utils/dynamicreduce.h"
#include "utils/dr_private.h"

static void ClearParallelPlanInfo(PlanInfo *pi)
{
	if (pi == NULL)
		return;

	DR_PLAN_DEBUG((errmsg("clean parallel plan %d(%p)", pi->plan_id, pi)));
	DRPlanSearch(pi->plan_id, HASH_REMOVE, NULL);
	if (pi->pwi)
	{
		uint32 i;
		for (i=0;i<pi->count_pwi;++i)
			DRClearPlanWorkInfo(pi, &pi->pwi[i]);
		pfree(pi->pwi);
		pi->pwi = NULL;
	}
	DRClearPlanInfo(pi);
}

static void OnParallelPlanPreWait(PlanInfo *pi)
{
	PlanWorkerInfo *pwi;
	uint32			count = pi->count_pwi;
	bool			need_wait = false;

	while(count > 0)
	{
		pwi = &pi->pwi[--count];
		if (pwi->end_of_plan_recv == false ||
			pwi->end_of_plan_send == false ||
			pwi->last_msg_type != ADB_DR_MSG_INVALID ||
			pwi->sendBuffer.len > 0)
		{
			need_wait = true;
			break;
		}
	}
	if (need_wait == false)
		ClearParallelPlanInfo(pi);
}

static void OnParallelPlanLatch(PlanInfo *pi)
{
	PlanWorkerInfo *pwi;
	uint32			msg_type;
	uint32			count;
	bool			need_active_node;

	need_active_node = false;
	for(count = pi->count_pwi;count>0;)
	{
		pwi = &pi->pwi[--count];
		if (DRSendPlanWorkerMessage(pwi, pi))
			need_active_node = true;
	}
	if (need_active_node)
		DRActiveNode(pi->plan_id);

	for(count = pi->count_pwi; count>0;)
	{
		pwi = &pi->pwi[--count];
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
				++(pi->end_count_pwi);
				Assert(pi->end_count_pwi <= pi->count_pwi);

				/* is all parallel got end? */
				if (pi->end_count_pwi == pi->count_pwi)
				{
					DRGetEndOfPlanMessage(pi, pwi);
				}else
				{
					pwi->last_msg_type = ADB_DR_MSG_INVALID;
					break;
				}
			}else
			{
				Assert(msg_type == ADB_DR_MSG_TUPLE);
			}

			/* send message to remote */
			DRSendWorkerMsgToNode(pwi, pi, NULL);
		}
	}
}

static bool OnParallelPlanMessage(PlanInfo *pi, const char *data, int len, Oid nodeoid)
{
	HeapTupleData	tup;
	PlanWorkerInfo *pwi;
	MinimalTuple	mtup;
	MemoryContext	oldcontext;
	uint32			i,count;

	i=pi->last_pwi;
	for(count=pi->count_pwi;count>0;--count)
	{
		if (++i >= pi->count_pwi)
			i = 0;
		pwi = &pi->pwi[i];

		/* we only cache one tuple */
		if (pwi->sendBuffer.len != 0)
			continue;

		DR_PLAN_DEBUG((errmsg("parallel plan %d(%p) worker %u got a tuple from %u length %d",
							  pi->plan_id, pi, pwi->worker_id, nodeoid, len)));
		appendStringInfoChar(&pwi->sendBuffer, ADB_DR_MSG_TUPLE);
		appendStringInfoSpaces(&pwi->sendBuffer, sizeof(nodeoid)-sizeof(char));	/* for align */
		appendBinaryStringInfoNT(&pwi->sendBuffer, (char*)&nodeoid, sizeof(nodeoid));
		if (pi->type_convert)
		{
			MemoryContextReset(pi->convert_context);
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

		pi->last_pwi = i;

		return true;
	}

	return false;
}

static void OnParallelPlanIdleNode(PlanInfo *pi, WaitEvent *we, DRNodeEventData *ned)
{
	PlanWorkerInfo *pwi;
	uint32			i;

	for(i=pi->count_pwi;i>0;)
	{
		pwi = &pi->pwi[--i];
		if (pwi->last_msg_type == ADB_DR_MSG_INVALID ||
			pwi->dest_oids[pwi->dest_cursor] != ned->nodeoid)
			continue;

		DRSendWorkerMsgToNode(pwi, pi, ned);
	}
}

static bool OnParallelPlanNodeEndOfPlan(PlanInfo *pi, Oid nodeoid)
{
	PlanWorkerInfo *pwi;
	uint32			i;

	DR_PLAN_DEBUG_EOF((errmsg("parallel plan %d(%p) got end of plan message from node %u",
							  pi->plan_id, pi, nodeoid)));
	Assert(oidBufferMember(&pi->working_nodes, nodeoid, NULL));
	appendOidBufferUniqueOid(&pi->end_of_plan_nodes, nodeoid);

	if (pi->end_of_plan_nodes.len == pi->working_nodes.len)
	{
		for(i=pi->count_pwi;i>0;)
		{
			if (pi->pwi[--i].sendBuffer.len != 0)
				return false;
		}

		DR_PLAN_DEBUG_EOF((errmsg("parallel plan %d(%p) sending end of plan message", pi->plan_id, pi)));
		for(i=pi->count_pwi;i>0;)
		{
			pwi = &pi->pwi[--i];
			appendStringInfoChar(&pwi->sendBuffer, ADB_DR_MSG_END_OF_PLAN);
			DRSendPlanWorkerMessage(pwi, pi);
			pwi->end_of_plan_send = true;
		}
	}
	return true;
}

void DRStartParallelPlanMessage(StringInfo msg)
{
	PlanInfo * volatile		pi = NULL;
	DynamicReduceMQ			mq;
	MemoryContext			oldcontext;
	ResourceOwner			oldowner;
	int						parallel_max;
	int						i;

	if (!IsTransactionState())
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("not in transaction state")));
	}

	PG_TRY();
	{
		oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		oldowner = CurrentResourceOwner;
		CurrentResourceOwner = NULL;

		pq_copymsgbytes(msg, (char*)&parallel_max, sizeof(parallel_max));
		if (parallel_max <= 1)
			elog(ERROR, "invalid parallel count");

		pi = DRRestorePlanInfo(msg, (void**)&mq, sizeof(*mq)*parallel_max, ClearParallelPlanInfo);
		pi->pwi = MemoryContextAllocZero(TopMemoryContext, sizeof(PlanWorkerInfo)*parallel_max);
		pi->count_pwi = parallel_max;
		for (i=0;i<parallel_max;++i)
			DRSetupPlanWorkInfo(pi, &pi->pwi[i], &mq[i], i-1);

		CurrentResourceOwner = oldowner;
		for (i=0;i<parallel_max;++i)
			DRSetupPlanWorkTypeConvert(pi, &pi->pwi[i]);

		pi->OnLatchSet = OnParallelPlanLatch;
		pi->OnNodeRecvedData = OnParallelPlanMessage;
		pi->OnNodeIdle = OnParallelPlanIdleNode;
		pi->OnNodeEndOfPlan = OnParallelPlanNodeEndOfPlan;
		pi->OnPlanError = ClearParallelPlanInfo;
		pi->OnPreWait = OnParallelPlanPreWait;

		MemoryContextSwitchTo(oldcontext);
		DR_PLAN_DEBUG((errmsg("normal plan %d(%p) stared", pi->plan_id, pi)));

	}PG_CATCH();
	{
		ClearParallelPlanInfo(pi);
		PG_RE_THROW();
	}PG_END_TRY();

	Assert(pi != NULL);
	DRActiveNode(pi->plan_id);
}

void DynamicReduceStartParallelPlan(int plan_id, struct dsm_segment *seg,
									DynamicReduceMQ mq, TupleDesc desc, List *work_nodes, int parallel_max)
{
	StringInfoData	buf;
	Assert(plan_id >= 0);
	Assert(parallel_max > 1);

	DRCheckStarted();

	initStringInfo(&buf);
	pq_sendbyte(&buf, ADB_DR_MQ_MSG_START_PLAN_PARALLEL);

	pq_sendbytes(&buf, (char*)&parallel_max, sizeof(parallel_max));
	DRSerializePlanInfo(plan_id, seg, mq, sizeof(*mq)*parallel_max, desc, work_nodes, &buf);

	DRSendMsgToReduce(buf.data, buf.len, false);
	pfree(buf.data);

	DRRecvConfirmFromReduce(false);
}