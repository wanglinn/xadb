#include "postgres.h"

#include "executor/executor.h"
#include "executor/nodeBatchSort.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port/atomics.h"
#include "storage/barrier.h"
#include "utils/builtins.h"
#include "utils/hashutils.h"
#include "utils/tuplesort.h"
#include "utils/typcache.h"

typedef struct ParallelBatchSort
{
	Barrier				barrier;
	pg_atomic_uint32	attached;
	pg_atomic_uint32	cur_batch;
	Size				tuplesort_size;	/* MAXIMUM_ALIGNOF*n */
}ParallelBatchSort;

#define PARALLEL_BATCH_SORT_SIZE		MAXALIGN(sizeof(ParallelBatchSort))
#define PARALLEL_BATCH_SORT_SHARED(p,n)	\
	(Sharedsort*)(((char*)p) + PARALLEL_BATCH_SORT_SIZE + (p)->tuplesort_size * n)

static bool ExecNextParallelBatchSort(BatchSortState *state)
{
	ParallelBatchSort  *parallel = state->parallel;
	BatchSort		   *plan = castNode(BatchSort, state->ss.ps.plan);
	SortCoordinateData	coord;
	uint32				cur_batch;
	Assert(parallel != NULL);

	if (state->curBatch >= 0 &&
		state->curBatch < plan->numBatches &&
		state->batches[state->curBatch] != NULL)
	{
		tuplesort_end(state->batches[state->curBatch]);
		state->batches[state->curBatch] = NULL;
	}

	cur_batch = pg_atomic_fetch_add_u32(&parallel->cur_batch, 1);
	if (cur_batch >= plan->numBatches)
	{
		state->curBatch = plan->numBatches;
		return false;
	}

	Assert(state->batches[cur_batch] == NULL);
	state->curBatch = cur_batch;
	coord.isWorker = false;
	coord.nParticipants = pg_atomic_read_u32(&parallel->attached);
	coord.sharedsort = PARALLEL_BATCH_SORT_SHARED(parallel, cur_batch);
	state->batches[cur_batch] = tuplesort_begin_heap(ExecGetResultType(outerPlanState(state)),
													 plan->numSortCols,
													 plan->sortColIdx,
													 plan->sortOperators,
													 plan->collations,
													 plan->nullsFirst,
													 work_mem,
													 &coord,
													 false);
	tuplesort_performsort(state->batches[cur_batch]);
	return true;
}

static TupleTableSlot *ExecEmptyBatchSort(PlanState *pstate)
{
	return ExecClearTuple(pstate->ps_ResultTupleSlot);
}

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
		if (state->parallel)
		{
			if (ExecNextParallelBatchSort(state) == false)
			{
				ExecSetExecProcNode(pstate, ExecEmptyBatchSort);
				return ExecClearTuple(slot);
			}
		}else
		{
			state->curBatch++;
		}
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
	ParallelBatchSort  *parallel = state->parallel;
	SortCoordinateData	coord;
	FunctionCallInfo	fcinfo;
	uint32				hash;
	int					i;
	AttrNumber			maxAttr;
	Assert(state->sort_Done == false);
	Assert(list_length(state->groupFuns) == node->numGroupCols);

	if (parallel)
	{
		BarrierAttach(&parallel->barrier);
		pg_atomic_add_fetch_u32(&parallel->attached, 1);
	}

	for (i=node->numBatches;i>0;)
	{
		--i;
		if (parallel)
		{
			coord.isWorker = true;
			coord.nParticipants = -1;
			coord.sharedsort = PARALLEL_BATCH_SORT_SHARED(parallel, i);
		}
		state->batches[i] = tuplesort_begin_heap(ExecGetResultType(outerNode),
												 node->numSortCols,
												 node->sortColIdx,
												 node->sortOperators,
												 node->collations,
												 node->nullsFirst,
												 work_mem / node->numBatches,
												 parallel ? &coord : NULL,
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
	if (parallel)
	{
		for (i=node->numBatches;i>0;)
		{
			--i;
			tuplesort_end(state->batches[i]);
			state->batches[i] = NULL;
		}
		BarrierArriveAndWait(&parallel->barrier, WAIT_EVENT_BATCH_SORT_BUILD);
		BarrierDetach(&parallel->barrier);

		if (ExecNextParallelBatchSort(state))
			ExecSetExecProcNode(pstate, ExecBatchSort);
		else
			ExecSetExecProcNode(pstate, ExecEmptyBatchSort);
	}else
	{
		state->curBatch = 0;
		ExecSetExecProcNode(pstate, ExecBatchSort);
	}
	state->sort_Done = true;

	return (*pstate->ExecProcNodeReal)(pstate);
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
			if (node->batches[--i] != NULL)
			{
				tuplesort_end(node->batches[i]);
				node->batches[i] = NULL;
			}
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
	ExecSetExecProcNode(&node->ss.ps, ExecBatchSortFirst);
}

void ExecShutdownBatchSort(BatchSortState *node)
{
	CleanBatchSort(node);
}

void ExecBatchSortEstimate(BatchSortState *node, ParallelContext *pcxt)
{
	Size size = mul_size(MAXALIGN(tuplesort_estimate_shared(pcxt->nworkers)),
						 castNode(BatchSort, node->ss.ps.plan)->numBatches);
	size = add_size(size, PARALLEL_BATCH_SORT_SIZE);

	shm_toc_estimate_chunk(&pcxt->estimator, size);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

static void InitializeBatchSortParallel(ParallelBatchSort *parallel,
										int num_batches,
										int num_workers,
										dsm_segment *seg)
{
	int i;
	BarrierInit(&parallel->barrier, 0);
	pg_atomic_init_u32(&parallel->attached, 0);
	pg_atomic_init_u32(&parallel->cur_batch, 0);
	for (i=0;i<num_batches;++i)
	{
		tuplesort_initialize_shared(PARALLEL_BATCH_SORT_SHARED(parallel, i),
									num_workers,
									seg);
	}
}

void ExecBatchSortInitializeDSM(BatchSortState *node, ParallelContext *pcxt)
{
	ParallelBatchSort *parallel;
	BatchSort *plan = castNode(BatchSort, node->ss.ps.plan);
	Size tuplesort_size = MAXALIGN(tuplesort_estimate_shared(pcxt->nworkers));
	Size size = mul_size(tuplesort_size, plan->numBatches);
	size = add_size(PARALLEL_BATCH_SORT_SIZE, size);

	node->parallel = parallel = shm_toc_allocate(pcxt->toc, size);
	parallel->tuplesort_size = tuplesort_size;
	InitializeBatchSortParallel(parallel, plan->numBatches, pcxt->nworkers, pcxt->seg);
	shm_toc_insert(pcxt->toc, plan->plan.plan_node_id, parallel);
}

void ExecBatchSortReInitializeDSM(BatchSortState *node, ParallelContext *pcxt)
{
	InitializeBatchSortParallel(node->parallel,
								castNode(BatchSort, node->ss.ps.plan)->numBatches,
								pcxt->nworkers,
								pcxt->seg);
	ExecSetExecProcNode(&node->ss.ps, ExecBatchSortFirst);
}

void ExecBatchSortInitializeWorker(BatchSortState *node, ParallelWorkerContext *pwcxt)
{
	uint32 i;
	BatchSort *plan = castNode(BatchSort, node->ss.ps.plan);
	ParallelBatchSort *parallel = shm_toc_lookup(pwcxt->toc,
												 plan->plan.plan_node_id,
												 false);
	node->parallel = parallel;
	for (i=0;i<plan->numBatches;++i)
	{
		tuplesort_attach_shared(PARALLEL_BATCH_SORT_SHARED(parallel, i),
								pwcxt->seg);
	}
}
