#include "postgres.h"

#include "access/htup_details.h"
#include "lib/binaryheap.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/sortsupport.h"

#include "utils/dynamicreduce.h"
#include "utils/dr_private.h"

typedef struct MergeItemInfo
{
	Oid			sortOperator;
	Oid			collation;
	AttrNumber	colIdx;
	bool		nullFirst;
}MergeItemInfo;

typedef struct MergeInputItem
{
	DRNodeEventData	   *node;
	TupleTableSlot	   *slot;
}MergeInputItem;

typedef struct MergeInput
{
	binaryheap		   *binheap;
	uint32				nitem;			/* initialized size */
	uint32				nsort_keys;
	SortSupport			sort_keys;
	MergeInputItem		items[FLEXIBLE_ARRAY_MEMBER];
}MergeInput;

extern bool IsTransactionState(void);	/* avoid include xact.h */

static void DRSerializeMergeInfo(int numCols, AttrNumber *sortColIdx, Oid *sortOperators, Oid *collations, bool *nullsFirst, StringInfo buf);
static MergeItemInfo* DRRestoreMergeInfo(StringInfo buf, int *count);
static void ClearMergePlanInfo(PlanInfo *pi);
static MergeInput* DRCreateMergeInput(PlanInfo *pi);
static void DRClearMergeInput(MergeInput *minput);
static int DRCompareMergeInput(Datum a, Datum b, void *arg);
static void OnMergePlanLatchInitialize(PlanInfo *pi);
static void OnMergePlanLatch(PlanInfo *pi);
static bool OnMergePlanMessage(PlanInfo *pi, const char *data, int len, Oid nodeoid);
static void OnMergePlanIdleNode(PlanInfo *pi, WaitEvent *we, DRNodeEventData *ned);
static bool OnMergePlanNodeEndOfPlan(PlanInfo *pi, Oid nodeoid);
static void OnMergePlanPreWait(PlanInfo *pi);

static void DRSerializeMergeInfo(int numCols, AttrNumber *sortColIdx, Oid *sortOperators, Oid *collations, bool *nullsFirst, StringInfo buf)
{
	int		i;
	Assert(numCols > 0);

	pq_sendbytes(buf, (char*)&numCols, sizeof(numCols));
	for (i=0;i<numCols;++i)
	{
		pq_sendbytes(buf, (char*)&sortOperators[i], sizeof(sortOperators[0]));
		pq_sendbytes(buf, (char*)&collations[i], sizeof(collations[0]));
		pq_sendbytes(buf, (char*)&sortColIdx[i], sizeof(sortColIdx[0]));
		pq_sendbyte(buf, nullsFirst[i]);
	}
}

static MergeItemInfo* DRRestoreMergeInfo(StringInfo buf, int *count)
{
	MergeItemInfo  *info;
	int				i,numCols;

	pq_copymsgbytes(buf, (char*)&numCols, sizeof(numCols));
	if (numCols <= 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid number %d of merge", numCols)));
	}

	info = palloc(sizeof(info[0])*numCols);
	*count = numCols;

	for (i=0;i<numCols;++i)
	{
		MergeItemInfo *tmp = &info[i];
		pq_copymsgbytes(buf, (char*)&tmp->sortOperator, sizeof(tmp->sortOperator));
		pq_copymsgbytes(buf, (char*)&tmp->collation, sizeof(tmp->collation));
		pq_copymsgbytes(buf, (char*)&tmp->colIdx, sizeof(tmp->colIdx));
		tmp->nullFirst = (bool)pq_getmsgbyte(buf);
	}

	return info;
}

static void ClearMergePlanInfo(PlanInfo *pi)
{
	if (pi == NULL)
		return;
	DR_PLAN_DEBUG((errmsg("clean merge plan %d(%p)", pi->plan_id, pi)));
	DRPlanSearch(pi->plan_id, HASH_REMOVE, NULL);
	if (pi->sort_context)
		MemoryContextDelete(pi->sort_context);
	if (pi->pwi)
	{
		PlanWorkerInfo *pwi = pi->pwi;
		DRClearMergeInput(pwi->private);
		DRClearPlanWorkInfo(pi, pwi);
		pfree(pwi);
		pi->pwi = NULL;
	}
	DRClearPlanInfo(pi);
}

void DRStartMergePlanMessage(StringInfo msg)
{
	PlanInfo * volatile		pi = NULL;
	DynamicReduceMQ			mq;
	PlanWorkerInfo		   *pwi;
	MergeItemInfo		   *minfo;
	MemoryContext			oldcontext;
	ResourceOwner			oldowner;
	int						i,count;

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

		pi = DRRestorePlanInfo(msg, (void**)&mq, sizeof(*mq), ClearMergePlanInfo);
		Assert(DRPlanSearch(pi->plan_id, HASH_FIND, NULL) == pi);
		minfo = DRRestoreMergeInfo(msg, &count);
		Assert(count > 0 && minfo != NULL);
		pq_getmsgend(msg);

		pwi = pi->pwi = MemoryContextAllocZero(TopMemoryContext, sizeof(PlanWorkerInfo));
		DRSetupPlanWorkInfo(pi, pwi, mq, -1);

		CurrentResourceOwner = oldowner;
		DRSetupPlanWorkTypeConvert(pi, pwi);

		pi->sort_context = AllocSetContextCreate(TopMemoryContext,
												 "plan merge context",
												 ALLOCSET_DEFAULT_SIZES);
		MemoryContextSwitchTo(pi->sort_context);
		pi->nsort_keys = count;
		pi->sort_keys = MemoryContextAllocZero(pi->sort_context, sizeof(pi->sort_keys[0]) * pi->nsort_keys);
		for (i=0;i<count;++i)
		{
			SortSupport sort = &pi->sort_keys[i];
			MergeItemInfo *info = &minfo[i];
			sort->ssup_cxt = pi->sort_context;
			sort->ssup_collation = info->collation;
			sort->ssup_nulls_first = info->nullFirst;
			sort->ssup_attno = info->colIdx;

			sort->abbreviate = false;

			PrepareSortSupportFromOrderingOp(info->sortOperator, sort);
		}
		pfree(minfo);

		pwi->private = DRCreateMergeInput(pi);

		pi->OnLatchSet = OnMergePlanLatchInitialize;
		pi->OnNodeRecvedData = OnMergePlanMessage;
		pi->OnNodeIdle = OnMergePlanIdleNode;
		pi->OnNodeEndOfPlan = OnMergePlanNodeEndOfPlan;
		pi->OnPlanError = ClearMergePlanInfo;
		Assert(pi->OnDestroy == ClearMergePlanInfo);
		pi->OnPreWait = OnMergePlanPreWait;

		MemoryContextSwitchTo(oldcontext);
		DR_PLAN_DEBUG((errmsg("merge plan %d(%p) stared", pi->plan_id, pi)));
	}PG_CATCH();
	{
		ClearMergePlanInfo(pi);
		PG_RE_THROW();
	}PG_END_TRY();
}

void DynamicReduceStartMergePlan(int plan_id, struct dsm_segment *seg, DynamicReduceMQ mq, TupleDesc desc, List *work_nodes,
								 int numCols, AttrNumber *sortColIdx, Oid *sortOperators, Oid *collations, bool *nullsFirst)
{
	StringInfoData		buf;
	Assert(plan_id >= 0 && numCols > 0);

	DRCheckStarted();

	initStringInfoExtend(&buf, 128);
	appendStringInfoChar(&buf, ADB_DR_MQ_MSG_START_PLAN_MERGE);

	DRSerializePlanInfo(plan_id, seg, mq, sizeof(*mq), desc, work_nodes, &buf);

	DRSerializeMergeInfo(numCols, sortColIdx, sortOperators, collations, nullsFirst, &buf);

	DRSendMsgToReduce(buf.data, buf.len, false);
	pfree(buf.data);

	DRRecvConfirmFromReduce(false);
}

static MergeInput* DRCreateMergeInput(PlanInfo *pi)
{
	MergeInput *minput;
	uint32		max_item = pi->working_nodes.len;
	uint32		i;

	minput = palloc0(offsetof(MergeInput, items) + sizeof(minput->items[0])*max_item);
	minput->binheap = binaryheap_allocate(max_item, DRCompareMergeInput, minput);
	for (i=0;i<max_item;++i)
	{
		minput->items[i].node = NULL;
		minput->items[i].slot = MakeTupleTableSlot(pi->base_desc);
	}

	minput->nsort_keys = pi->nsort_keys;
	minput->sort_keys = pi->sort_keys;

	return minput;
}

static void DRClearMergeInput(MergeInput *minput)
{
	TupleTableSlot *slot;
	uint32	i;
	uint32	max_item;
	if (minput == NULL)
		return;

	if (minput->binheap == NULL)
	{
		max_item = minput->binheap->bh_space;
		for (i=0;i<max_item;++i)
		{
			if ((slot = minput->items[i].slot) != NULL)
				ExecDropSingleTupleTableSlot(slot);
		}
		binaryheap_free(minput->binheap);
	}

	pfree(minput);
}

static int DRCompareMergeInput(Datum a, Datum b, void *arg)
{
	MergeInput	   *minput = arg;
	TupleTableSlot *slot1 = minput->items[DatumGetUInt32(a)].slot;
	TupleTableSlot *slot2 = minput->items[DatumGetUInt32(b)].slot;
	SortSupport		sort;
	Datum			datum1,datum2;
	bool			null1,null2;
	uint32			i;
	int				compare;

	Assert(!TupIsNull(slot1));
	Assert(!TupIsNull(slot2));

	for (i=0;i<minput->nsort_keys;++i)
	{
		sort = minput->sort_keys + i;

		datum1 = slot_getattr(slot1, sort->ssup_attno, &null1);
		datum2 = slot_getattr(slot2, sort->ssup_attno, &null2);

		compare = ApplySortComparator(datum1, null1,
									  datum2, null2,
									  sort);
		if (compare != 0)
			return -compare;
	}

	return 0;
}

static inline void ProcessBackendMergePlanMessage(PlanWorkerInfo *pwi, PlanInfo *pi)
{
	uint32	msg_type;

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
			DRGetEndOfPlanMessage(pi, pwi);
		}else
		{
			Assert(msg_type == ADB_DR_MSG_TUPLE);
		}

		/* send message to remote */
		DRSendWorkerMsgToNode(pwi, pi, NULL);
	}
}

static void PutTupleToMergeSlot(PlanInfo *pi, TupleTableSlot *slot, char *data, int len)
{
	HeapTupleData	tup;
	MemoryContext	oldcontext;
	TupleTableSlot *slot_node_src;
	MinimalTuple	mtup;
	Assert(data && len > 0);

	if (pi->type_convert)
	{
		slot_node_src = pi->pwi->slot_node_src;
		MemoryContextReset(pi->convert_context);
		oldcontext = MemoryContextSwitchTo(pi->convert_context);
		DRStoreTypeConvertTuple(slot_node_src, data, len, &tup);
		do_type_convert_slot_in(pi->type_convert, slot_node_src, slot, false);
		MemoryContextSwitchTo(slot->tts_mcxt);
		mtup = ExecCopySlotMinimalTuple(slot);
		ExecStoreMinimalTuple(mtup, slot, true);
		ExecClearTuple(slot_node_src);
		MemoryContextSwitchTo(oldcontext);
	}else
	{
		oldcontext = MemoryContextSwitchTo(slot->tts_mcxt);
		mtup = MemoryContextAlloc(slot->tts_mcxt, len + MINIMAL_TUPLE_DATA_OFFSET);
		mtup->t_len = len + MINIMAL_TUPLE_DATA_OFFSET;
		memcpy((char*)mtup + MINIMAL_TUPLE_DATA_OFFSET, data, len);
		ExecStoreMinimalTuple(mtup, slot, false);
	}
}

static void PutTupleToWorkerInfo(PlanWorkerInfo *pwi, TupleTableSlot *slot, Oid nodeoid)
{
	MinimalTuple	mtup;
	Assert(pwi->sendBuffer.len == 0);/* only cache one tuple */

	appendStringInfoChar(&pwi->sendBuffer, ADB_DR_MSG_TUPLE);
	appendStringInfoSpaces(&pwi->sendBuffer, sizeof(nodeoid)-sizeof(char));	/* for align */
	appendBinaryStringInfoNT(&pwi->sendBuffer, (char*)&nodeoid, sizeof(nodeoid));
	mtup = ExecFetchSlotMinimalTuple(slot);
	appendBinaryStringInfoNT(&pwi->sendBuffer,
							 (char*)mtup + MINIMAL_TUPLE_DATA_OFFSET,
							 mtup->t_len - MINIMAL_TUPLE_DATA_OFFSET);
}

static void OnMergePlanLatch(PlanInfo *pi)
{
	PlanWorkerInfo *pwi = pi->pwi;
	MergeInput	   *minput = pwi->private;
	MergeInputItem *mitem;
	char		   *data;
	int				len;
	uint32			i;

	if (pwi->sendBuffer.len > 0)
		DRSendPlanWorkerMessage(pwi, pi);

	if (pwi->sendBuffer.len == 0 &&
		binaryheap_empty(minput->binheap) == false)
	{
		while (pwi->sendBuffer.len == 0)
		{
			i = DatumGetUInt32(binaryheap_first(minput->binheap));
			mitem = &minput->items[i];
			if (DRNodeFetchTuple(mitem->node, pi->plan_id, &data, &len) == false)
				break;
			
			if (data == NULL)
			{
				Assert(len == 0);
				DR_PLAN_DEBUG_EOF((errmsg("merge plan %d(%p) fetched end of plan message from node %u",
										  pi->plan_id, pi, mitem->node->nodeoid)));
				appendOidBufferOid(&pi->end_of_plan_nodes, mitem->node->nodeoid);
				binaryheap_remove_first(minput->binheap);
				if (binaryheap_empty(minput->binheap))
					break;
			}else
			{
				PutTupleToMergeSlot(pi, mitem->slot, data, len);
				Assert(!TupIsNull(mitem->slot));
				binaryheap_replace_first(minput->binheap, UInt32GetDatum(i));
				
				i = DatumGetUInt32(binaryheap_first(minput->binheap));
				mitem = &minput->items[i];
				PutTupleToWorkerInfo(pwi, mitem->slot, mitem->node->nodeoid);
			}
		}
	}

	if (binaryheap_empty(minput->binheap) &&
		pwi->sendBuffer.len == 0 &&
		pwi->end_of_plan_send == false)
	{
		Assert(pi->end_of_plan_nodes.len == pi->working_nodes.len);
		pwi->end_of_plan_send = true;
		DR_PLAN_DEBUG_EOF((errmsg("merge plan %d(%p) sending end of plan message", pi->plan_id, pi)));
		appendStringInfoChar(&pwi->sendBuffer, ADB_DR_MSG_END_OF_PLAN);
		DRSendPlanWorkerMessage(pwi, pi);
	}

	ProcessBackendMergePlanMessage(pwi, pi);
}

static void OnMergePlanLatchInitialize(PlanInfo *pi)
{
	uint32				i,
						count = pi->working_nodes.len;
	Oid				   *oids = pi->working_nodes.oids;
	PlanWorkerInfo	   *pwi = pi->pwi;
	MergeInput		   *minput = pwi->private;
	MergeInputItem	   *mitem;
	char			   *data;
	int					len;
	bool				finish = true;

	for (i=0;i<count;++i)
	{
		Assert(oids[i] != PGXCNodeOid);
		mitem = &minput->items[i];

		if (!TupIsNull(mitem->slot) ||
			oidBufferMember(&pi->end_of_plan_nodes, oids[i], NULL))
			continue;

		if (mitem->node == NULL)
			mitem->node = DRSearchNodeEventData(oids[i], HASH_FIND, NULL);

		if (mitem->node == NULL ||
			DRNodeFetchTuple(mitem->node, pi->plan_id, &data, &len) == false)
		{
			finish = false;
			continue;
		}
		Assert(mitem->node->nodeoid == oids[i]);

		if (data == NULL)
		{
			/* end of plan message */
			Assert(len == 0);
			DR_PLAN_DEBUG_EOF((errmsg("merge plan %d(%p) fetched end of plan message from node %u",
									  pi->plan_id, pi, oids[i])));
			appendOidBufferOid(&pi->end_of_plan_nodes, oids[i]);
			continue;
		}
		PutTupleToMergeSlot(pi, mitem->slot, data, len);
		Assert(!TupIsNull(mitem->slot));
		binaryheap_add_unordered(minput->binheap, UInt32GetDatum(i));
	}

	if (finish)
	{
		binaryheap_build(minput->binheap);
		if (binaryheap_empty(minput->binheap))
		{
			Assert(pi->end_of_plan_nodes.len == pi->working_nodes.len);
			pwi->end_of_plan_send = true;
		}else
		{
			i = DatumGetUInt32(binaryheap_first(minput->binheap));
			mitem = &minput->items[i];
			pi->OnLatchSet = OnMergePlanLatch;
			PutTupleToWorkerInfo(pwi, mitem->slot, mitem->node->nodeoid);
			OnMergePlanLatch(pi);
			return;
		}
	}

	ProcessBackendMergePlanMessage(pwi, pi);
}

static bool OnMergePlanMessage(PlanInfo *pi, const char *data, int len, Oid nodeoid)
{
	SetLatch(MyLatch);
	return false;
}

static void OnMergePlanIdleNode(PlanInfo *pi, WaitEvent *we, DRNodeEventData *ned)
{
	PlanWorkerInfo *pwi = pi->pwi;;
	if (pwi->last_msg_type == ADB_DR_MSG_INVALID)
		return;
	if (pwi->dest_oids[pwi->dest_cursor] != ned->nodeoid)
		return;

	DRSendWorkerMsgToNode(pwi, pi, ned);
}

static bool OnMergePlanNodeEndOfPlan(PlanInfo *pi, Oid nodeoid)
{
	SetLatch(MyLatch);
	return false;
}

static void OnMergePlanPreWait(PlanInfo *pi)
{
	PlanWorkerInfo *pwi = pi->pwi;
	if (pwi->end_of_plan_recv &&
		pwi->end_of_plan_send &&
		pwi->last_msg_type == ADB_DR_MSG_INVALID &&
		pwi->sendBuffer.len == 0)
		ClearMergePlanInfo(pi);
}
