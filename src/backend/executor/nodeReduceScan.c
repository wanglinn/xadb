
#include "postgres.h"

#include "commands/tablespace.h"
#include "executor/executor.h"
#include "executor/nodeHash.h"
#include "executor/nodeReduceScan.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "utils/hashutils.h"
#include "utils/memutils.h"
#include "utils/sharedtuplestore.h"
#include "utils/typcache.h"

typedef struct RedcueScanSharedMemory
{
	SharedFileSet	sfs;
	char			padding[sizeof(SharedFileSet)%MAXIMUM_ALIGNOF ? 
							MAXIMUM_ALIGNOF - sizeof(SharedFileSet)%MAXIMUM_ALIGNOF : 0];
	char			sts_mem[FLEXIBLE_ARRAY_MEMBER];
}RedcueScanSharedMemory;

#define REDUCE_SCAN_SHM_SIZE(nbatch)													\
	(StaticAssertExpr(offsetof(RedcueScanSharedMemory, sts_mem) % MAXIMUM_ALIGNOF == 0,	\
					  "sts_mem not align to max"),										\
	 offsetof(RedcueScanSharedMemory, sts_mem) + MAXALIGN(sts_estimate(1)) * (nbatch))
#define REDUCE_SCAN_STS_ADDR(start, batch)		\
	(SharedTuplestore*)((char*)start + MAXALIGN(sts_estimate(1)) * batch)

int reduce_scan_bucket_size = 1024*1024;	/* 1MB */
int reduce_scan_max_buckets = 1024;

static TupleTableSlot *ExecSeqReduceScan(PlanState *pstate);
static TupleTableSlot* ExecHashReduceScan(PlanState *pstate);
static TupleTableSlot *ExecEmptyReduceScan(PlanState *pstate);
static TupleTableSlot* SeqReduceScanNext(ReduceScanState *node);
static TupleTableSlot* HashReduceScanNext(ReduceScanState *node);
static bool ReduceScanRecheck(ReduceScanState *node, TupleTableSlot *slot);
static uint32 ExecReduceScanGetHashValue(ExprContext *econtext, List *exprs, FmgrInfo *fmgr, bool *isnull);
static inline SharedTuplestoreAccessor* ExecGetReduceScanBatch(ReduceScanState *node, uint32 hashval)
{
	return node->batchs[hashval%node->nbatchs];
}

ReduceScanState *ExecInitReduceScan(ReduceScan *node, EState *estate, int eflags)
{
	Plan	   *outer_plan;
	TupleDesc	tupDesc;
	ReduceScanState *rcs = makeNode(ReduceScanState);

	rcs->ss.ps.plan = (Plan*)node;
	rcs->ss.ps.state = estate;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &rcs->ss.ps);

	/*
	 * initialize child expressions
	 */
	rcs->ss.ps.qual = ExecInitQual(node->plan.qual, (PlanState *) rcs);

	outer_plan = outerPlan(node);
	outerPlanState(rcs) = ExecInitNode(outer_plan, estate, eflags & ~(EXEC_FLAG_REWIND|EXEC_FLAG_BACKWARD));
	tupDesc = ExecGetResultType(outerPlanState(rcs));

	/*
	 * initialize tuple type.  no need to initialize projection info because
	 * this node doesn't do projections.
	 */
	ExecInitScanTupleSlot(estate, &rcs->ss, tupDesc);
	ExecInitResultTupleSlotTL(estate, &rcs->ss.ps);
	ExecConditionalAssignProjectionInfo(&rcs->ss.ps, tupDesc, OUTER_VAR);

	if(node->param_hash_keys != NIL)
	{
		ListCell *lc;
		size_t space_allowed;
		int i;
		int nbuckets;
		int nskew_mcvs;
		int saved_work_mem = work_mem;
		Assert(list_length(node->param_hash_keys) == list_length(node->scan_hash_keys));

		rcs->ncols_hash = list_length(node->param_hash_keys);
		work_mem = reduce_scan_bucket_size;
		ExecChooseHashTableSize(outer_plan->plan_rows,
								outer_plan->plan_width,
								false,
								false,
								0,
								&space_allowed,
								&nbuckets,
								&rcs->nbatchs,
								&nskew_mcvs);
		work_mem = saved_work_mem;
		if (rcs->nbatchs > reduce_scan_max_buckets)
			rcs->nbatchs = reduce_scan_max_buckets;
		rcs->param_hash_exprs = ExecInitExprList(node->param_hash_keys, (PlanState*)rcs);
		rcs->scan_hash_exprs = ExecInitExprList(node->scan_hash_keys, (PlanState*)rcs);
		rcs->param_hash_funs = palloc(sizeof(rcs->param_hash_funs[0]) * rcs->ncols_hash);
		rcs->scan_hash_funs = palloc(sizeof(rcs->scan_hash_funs[0]) * rcs->ncols_hash);

		i=0;
		foreach(lc, node->param_hash_keys)
		{
			TypeCacheEntry *typeCache = lookup_type_cache(exprType(lfirst(lc)), TYPECACHE_HASH_PROC);
			Assert(OidIsValid(typeCache->hash_proc));
			fmgr_info(typeCache->hash_proc, &rcs->param_hash_funs[i]);
			++i;
		}

		i=0;
		foreach(lc, node->scan_hash_keys)
		{
			TypeCacheEntry *typeCache = lookup_type_cache(exprType(lfirst(lc)), TYPECACHE_HASH_PROC);
			Assert(OidIsValid(typeCache->hash_proc));
			fmgr_info(typeCache->hash_proc, &rcs->scan_hash_funs[i]);
			++i;
		}
		rcs->ss.ps.ExecProcNode = ExecHashReduceScan;
	}else
	{
		rcs->nbatchs = 1;
		rcs->ss.ps.ExecProcNode = ExecSeqReduceScan;
	}

	return rcs;
}

static TupleTableSlot *ExecSeqReduceScan(PlanState *pstate)
{
	ReduceScanState *node = castNode(ReduceScanState, pstate);
	/* call FetchReduceScanOuter first */
	Assert(node->cur_batch != NULL);

	return ExecScan(&node->ss,
					(ExecScanAccessMtd)SeqReduceScanNext,
					(ExecScanRecheckMtd)ReduceScanRecheck);
}

static TupleTableSlot* ExecHashReduceScan(PlanState *pstate)
{
	ReduceScanState *node = castNode(ReduceScanState, pstate);
	/* call FetchReduceScanOuter first */
	Assert(node->cur_batch != NULL);

	return ExecScan(&node->ss,
					(ExecScanAccessMtd)HashReduceScanNext,
					(ExecScanRecheckMtd)ReduceScanRecheck);
}

static TupleTableSlot *ExecEmptyReduceScan(PlanState *pstate)
{
	return ExecClearTuple(pstate->ps_ResultTupleSlot);
}

static inline TupleTableSlot* SetAndExecEmptyReduceScan(PlanState *pstate)
{
	ExecSetExecProcNode(pstate, ExecEmptyReduceScan);
	return ExecClearTuple(pstate->ps_ResultTupleSlot);
}

void FetchReduceScanOuter(ReduceScanState *node)
{
	TupleTableSlot	   *slot;
	PlanState		   *outer_ps;
	ExprContext		   *econtext;
	MemoryContext		oldcontext;
	RedcueScanSharedMemory *shm;
	int					i;

	if(node->batchs)
		return;

	oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(node));

	node->dsm_seg = dsm_create(REDUCE_SCAN_SHM_SIZE(node->nbatchs), 0);
	shm = dsm_segment_address(node->dsm_seg);
	SharedFileSetInit(&shm->sfs, node->dsm_seg);

	node->batchs = palloc(sizeof(node->batchs[0]) * node->nbatchs);
	for (i=0;i<node->nbatchs;++i)
	{
		char name[64];
		sprintf(name, "reduce-scan-%d-b%d", node->ss.ps.plan->plan_node_id, i);
		node->batchs[i] = sts_initialize(REDUCE_SCAN_STS_ADDR(shm->sts_mem, i),
										 1,
										 0,
										 node->scan_hash_funs ? sizeof(uint32) : 0,
										 0,
										 &shm->sfs,
										 name);
	}

	/* we need read all outer slot first */
	outer_ps = outerPlanState(node);
	econtext = node->ss.ps.ps_ExprContext;
	if(node->scan_hash_exprs)
	{
		uint32 hashvalue;
		bool isnull;
		for(;;)
		{
			slot = ExecProcNode(outer_ps);
			if(TupIsNull(slot))
				break;

			ResetExprContext(econtext);
			econtext->ecxt_outertuple = slot;
			hashvalue = ExecReduceScanGetHashValue(econtext,
												   node->scan_hash_exprs,
												   node->scan_hash_funs,
												   &isnull);
			if (isnull)
				continue;

			sts_puttuple(ExecGetReduceScanBatch(node, hashvalue),
						 &hashvalue,
						 ExecFetchSlotMinimalTuple(slot));
		}
	}else
	{
		SharedTuplestoreAccessor *accessor = node->batchs[0];
		for(;;)
		{
			slot = ExecProcNode(outer_ps);
			if(TupIsNull(slot))
				break;

			sts_puttuple(accessor, NULL, ExecFetchSlotMinimalTuple(slot));
		}
		node->cur_batch = node->batchs[0];
	}

	for (i=0;i<node->nbatchs;++i)
		sts_end_write(node->batchs[i]);

	econtext->ecxt_outertuple = node->ss.ss_ScanTupleSlot;
	MemoryContextSwitchTo(oldcontext);
}

void ExecEndReduceScan(ReduceScanState *node)
{
	if (node->batchs)
	{
		pfree(node->batchs);
		node->batchs = NULL;
		node->nbatchs = 0;
	}
	node->cur_batch = NULL;
	if (node->dsm_seg)
	{
		dsm_detach(node->dsm_seg);
		node->dsm_seg = NULL;
	}
	ExecEndNode(outerPlanState(node));
}

void ExecReduceScanMarkPos(ReduceScanState *node)
{
	elog(ERROR, "not support yet!");
}

void ExecReduceScanRestrPos(ReduceScanState *node)
{
	elog(ERROR, "not support yet!");
}

void ExecReScanReduceScan(ReduceScanState *node)
{
	if (node->cur_batch != NULL)
	{
		sts_end_scan(node->cur_batch);
		node->cur_batch = NULL;
	}

	if(node->param_hash_exprs)
	{
		ExprContext *econtext = node->ss.ps.ps_ExprContext;
		node->cur_hashval = ExecReduceScanGetHashValue(econtext,
													   node->param_hash_exprs,
													   node->param_hash_funs,
													   &node->cur_hash_is_null);
		if (node->cur_hash_is_null)
		{
			node->cur_batch = NULL;
			ExecSetExecProcNode(&node->ss.ps, ExecEmptyReduceScan);
		}else
		{
			node->cur_batch = ExecGetReduceScanBatch(node, node->cur_hashval);
			ExecSetExecProcNode(&node->ss.ps, ExecHashReduceScan);
		}
	}else
	{
		node->cur_batch = node->batchs[0];
		ExecSetExecProcNode(&node->ss.ps, ExecSeqReduceScan);
	}

	if (node->cur_batch)
		sts_begin_scan(node->cur_batch);
}

static TupleTableSlot* SeqReduceScanNext(ReduceScanState *node)
{
	MinimalTuple mtup = sts_scan_next(node->cur_batch, NULL);

	if (mtup == NULL)
		return SetAndExecEmptyReduceScan(&node->ss.ps);
	else
		return ExecStoreMinimalTuple(mtup, node->ss.ss_ScanTupleSlot, false);
}

static TupleTableSlot* HashReduceScanNext(ReduceScanState *node)
{
	uint32			hashval;
	MinimalTuple	mtup;
	
	for (;;)
	{
		mtup = sts_scan_next(node->cur_batch, &hashval);
		if (mtup == NULL)
			return SetAndExecEmptyReduceScan(&node->ss.ps);
		else if (hashval == node->cur_hashval)
			return ExecStoreMinimalTuple(mtup, node->ss.ss_ScanTupleSlot, false);
	}

	return NULL;	/* keep compler quiet */
}

static bool ReduceScanRecheck(ReduceScanState *node, TupleTableSlot *slot)
{
	return true;
}

static uint32 ExecReduceScanGetHashValue(ExprContext *econtext, List *exprs, FmgrInfo *fmgr, bool *isnull)
{
	ListCell *lc;
	ExprState *expr_state;
	Datum key_value;
	uint32 hash_value = 0;
	int i;

	i = 0;
	foreach(lc, exprs)
	{
		expr_state = lfirst(lc);

		key_value = ExecEvalExpr(expr_state, econtext, isnull);
		if (*isnull)
			return 0;

		key_value = FunctionCall1(&fmgr[i], key_value);
		hash_value = hash_combine(hash_value, DatumGetUInt32(key_value));
		++i;
	}

	return hash_value;
}

static bool SetEmptyResultWalker(ReduceScanState *state, void *context)
{
	if (state == NULL)
		return false;

	if (IsA(state, ReduceScanState))
	{
		Assert(state->batchs != NULL);
		ExecSetExecProcNode(&state->ss.ps, ExecEmptyReduceScan);
		if (state->cur_batch)
		{
			sts_end_scan(state->cur_batch);
			state->cur_batch = NULL;
		}
		return false;
	}

	return planstate_tree_walker(&state->ss.ps, SetEmptyResultWalker, context);
}

void BeginDriveClusterReduce(PlanState *node)
{
	SetEmptyResultWalker((ReduceScanState*)node, NULL);
}
