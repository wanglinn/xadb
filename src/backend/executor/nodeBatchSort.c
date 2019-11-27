#include "postgres.h"

#include "executor/executor.h"
#include "executor/nodeBatchSort.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/hashutils.h"
#include "utils/tuplesort.h"
#include "utils/typcache.h"

static TupleTableSlot *ExecBatchSort(PlanState *pstate)
{
	TupleTableSlot *slot = pstate->ps_ResultTupleSlot;
	BatchSortState *state = castNode(BatchSortState, pstate);
	Assert(state->sort_Done);

re_get_:
	if (tuplesort_gettupleslot(state->batches[state->curBatch],
							   true,
							   false,
							   slot,
							   NULL) == false &&
		state->curBatch < castNode(BatchSort, pstate->plan)->numBatches-1)
	{
		state->curBatch++;
		goto re_get_;
	}

	return slot;
}

static TupleTableSlot *ExecBatchSortFirst(PlanState *pstate)
{
	BatchSort		   *node = castNode(BatchSort, pstate->plan);
	BatchSortState	   *state = castNode(BatchSortState, pstate);
	PlanState		   *outerNode = outerPlanState(pstate);
	TupleTableSlot	   *slot;
	ListCell		   *lc;
	FunctionCallInfo	fcinfo;
	uint32				hash;
	int					i;
	AttrNumber			maxAttr;
	Assert(state->sort_Done == false);
	Assert(list_length(state->groupFuns) == node->numGroupCols);

	for (i=node->numBatches;i>0;)
	{
		state->batches[--i] = tuplesort_begin_heap(ExecGetResultType(outerNode),
												   node->numSortCols,
												   node->sortColIdx,
												   node->sortOperators,
												   node->collations,
												   node->nullsFirst,
												   work_mem / node->numBatches,
												   NULL,
												   false);
	}

	maxAttr = 0;
	for (i=node->numGroupCols;i>0;)
	{
		if (maxAttr < node->grpColIdx[--i])
			maxAttr = node->grpColIdx[i];
	}
	for (i=node->numSortCols;i>0;)
	{
		if (maxAttr < node->sortColIdx[--i])
			maxAttr = node->sortColIdx[i];
	}
	Assert(maxAttr > 0);

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();
		slot = ExecProcNode(outerNode);
		if (TupIsNull(slot))
			break;
		slot_getsomeattrs(slot, maxAttr);

		hash = 0;
		i = 0;
		foreach(lc, state->groupFuns)
		{
			AttrNumber att = node->grpColIdx[i++]-1;
			if (slot->tts_isnull[att] == false)
			{
				fcinfo = lfirst(lc);
				fcinfo->arg[0] = slot->tts_values[att];
				hash = hash_combine(hash, DatumGetUInt32(FunctionCallInvoke(fcinfo)));
				Assert(fcinfo->isnull == false);
			}
		}

		tuplesort_puttupleslot(state->batches[hash%node->numBatches], slot);
	}

	for (i=node->numBatches;i>0;)
		tuplesort_performsort(state->batches[--i]);
	state->curBatch = 0;
	state->sort_Done = true;

	ExecSetExecProcNode(pstate, ExecBatchSort);
	return ExecBatchSort(pstate);
}

BatchSortState* ExecInitBatchSort(BatchSort *node, EState *estate, int eflags)
{
	BatchSortState *state;
	TypeCacheEntry *typentry;
	TupleDesc		desc;
	int				i;

	if (eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK))
	{
		/* for now, we only using in group aggregate */
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("not support execute flag(s) %d for group sort", eflags)));
	}

	state = makeNode(BatchSortState);
	state->ss.ps.plan = (Plan*) node;
	state->ss.ps.state = estate;
	state->ss.ps.ExecProcNode = ExecBatchSortFirst;

	state->sort_Done = false;
	state->batches = palloc0(node->numBatches * sizeof(Tuplesortstate*));

	outerPlanState(state) = ExecInitNode(outerPlan(node), estate, eflags);

	/*
	 * Initialize scan slot and type.
	 */
	ExecCreateScanSlotFromOuterPlan(estate, &state->ss);

	/*
	 * Initialize return slot and type. No need to initialize projection info
	 * because this node doesn't do projections.
	 */
	ExecInitResultTupleSlotTL(estate, &state->ss.ps);
	state->ss.ps.ps_ProjInfo = NULL;

	Assert(node->numGroupCols > 0);
	desc = ExecGetResultType(outerPlanState(state));
	for (i=0;i<node->numGroupCols;++i)
	{
		FmgrInfo			   *flinfo;
		FunctionCallInfoData   *fcinfo;
		Oid typid = TupleDescAttr(desc, node->grpColIdx[i]-1)->atttypid;
		typentry = lookup_type_cache(typid, TYPECACHE_HASH_PROC);
		if (!OidIsValid(typentry->hash_proc))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("could not identify an extended hash function for type %s",
							format_type_be(typid))));
		flinfo = palloc0(sizeof(*flinfo));
		fcinfo = palloc0(sizeof(*fcinfo));
		fmgr_info(typentry->hash_proc, flinfo);
		InitFunctionCallInfoData(*fcinfo, flinfo, 1, InvalidOid, NULL, NULL);
		fcinfo->argnull[0] = false;
		state->groupFuns = lappend(state->groupFuns, fcinfo);
	}

	return state;
}

static void CleanBatchSort(BatchSortState *node)
{
	int i;

	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	if (node->sort_Done)
	{
		for (i=castNode(BatchSort, node->ss.ps.plan)->numBatches;i>0;)
		{
			tuplesort_end(node->batches[--i]);
			node->batches[i] = NULL;
		}
		node->sort_Done = false;
	}
}

void ExecEndBatchSort(BatchSortState *node)
{
	ExecClearTuple(node->ss.ss_ScanTupleSlot);
	CleanBatchSort(node);
	ExecEndNode(outerPlanState(node));
}

void ExecReScanBatchSort(BatchSortState *node)
{
	CleanBatchSort(node);
	if (outerPlanState(node)->chgParam != NULL)
		ExecReScan(outerPlanState(node));
}
