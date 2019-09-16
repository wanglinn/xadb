#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "libpq/pqformat.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

#include "utils/dynamicreduce.h"
#include "utils/dr_private.h"

static bool OnNormalPlanMessage(PlanInfo *pi, const char *data, int len, Oid nodeoid);
static bool OnNormalPlanNodeEndOfPlan(PlanInfo *pi, Oid nodeoid);

static void ClearNormalPlanInfo(PlanInfo *pi)
{
	if (pi == NULL)
		return;
	DR_PLAN_DEBUG((errmsg("clean normal plan %d(%p)", pi->plan_id, pi)));
	DRPlanSearch(pi->plan_id, HASH_REMOVE, NULL);
	if (pi->pwi)
	{
		DRClearPlanWorkInfo(pi, pi->pwi);
		pfree(pi->pwi);
		pi->pwi = NULL;
	}
	DRClearPlanInfo(pi);
}

static bool OnNormalPlanMessage(PlanInfo *pi, const char *data, int len, Oid nodeoid)
{
	HeapTupleData	tup;
	PlanWorkerInfo *pwi;
	MinimalTuple	mtup;
	MemoryContext	oldcontext;

	pwi = pi->pwi;
	/* we only cache one tuple */
	if (pwi->sendBuffer.len != 0)
		return false;

	DR_PLAN_DEBUG((errmsg("normal plan %d(%p) got a tuple from %u length %d",
						  pi->plan_id, pi, nodeoid, len)));
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

	return true;
}

static bool OnNormalPlanNodeEndOfPlan(PlanInfo *pi, Oid nodeoid)
{
	PlanWorkerInfo *pwi = pi->pwi;
	if (pwi->sendBuffer.len != 0)
		return false;

	DR_PLAN_DEBUG_EOF((errmsg("normal plan %d(%p) got end of plan message from node %u",
							  pi->plan_id, pi, nodeoid)));
	Assert(oidBufferMember(&pi->working_nodes, nodeoid, NULL));
	appendOidBufferUniqueOid(&pi->end_of_plan_nodes, nodeoid);

	if (pi->end_of_plan_nodes.len == pi->working_nodes.len)
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
	DynamicReduceMQ			mq;
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
		oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		oldowner = CurrentResourceOwner;
		CurrentResourceOwner = NULL;

		pi = DRRestorePlanInfo(msg, (void**)&mq, sizeof(*mq), ClearNormalPlanInfo);
		Assert(DRPlanSearch(pi->plan_id, HASH_FIND, NULL) == pi);
		pq_getmsgend(msg);

		pwi = pi->pwi = MemoryContextAllocZero(TopMemoryContext, sizeof(PlanWorkerInfo));
		DRSetupPlanWorkInfo(pi, pwi, mq, -1);

		CurrentResourceOwner = oldowner;
		DRSetupPlanWorkTypeConvert(pi, pwi);

		pi->OnLatchSet = OnDefaultPlanLatch;
		pi->OnNodeRecvedData = OnNormalPlanMessage;
		pi->OnNodeIdle = OnDefaultPlanIdleNode;
		pi->OnNodeEndOfPlan = OnNormalPlanNodeEndOfPlan;
		pi->OnPlanError = ClearNormalPlanInfo;
		pi->OnPreWait = OnDefaultPlanPreWait;

		MemoryContextSwitchTo(oldcontext);
		DR_PLAN_DEBUG((errmsg("normal plan %d(%p) stared", pi->plan_id, pi)));
	}PG_CATCH();
	{
		ClearNormalPlanInfo(pi);
		PG_RE_THROW();
	}PG_END_TRY();

	Assert(pi != NULL);
	DRActiveNode(pi->plan_id);
}

void DynamicReduceStartNormalPlan(int plan_id, dsm_segment *seg, DynamicReduceMQ mq, TupleDesc desc, List *work_nodes)
{
	StringInfoData	buf;
	Assert(plan_id >= 0);

	DRCheckStarted();

	initStringInfo(&buf);
	pq_sendbyte(&buf, ADB_DR_MQ_MSG_START_PLAN_NORMAL);

	DRSerializePlanInfo(plan_id, seg, mq, sizeof(*mq), desc, work_nodes, &buf);

	DRSendMsgToReduce(buf.data, buf.len, false);
	pfree(buf.data);

	DRRecvConfirmFromReduce(false);
}
