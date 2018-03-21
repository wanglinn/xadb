
#include "postgres.h"

#include "access/htup_details.h"
#include "commands/tablespace.h"
#include "executor/executor.h"
#include "executor/nodeHash.h"
#include "executor/nodeReduceScan.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "utils/hashstore.h"
#include "utils/memutils.h"
#include "utils/typcache.h"

static TupleTableSlot *ExecReduceScan(PlanState *pstate);
static TupleTableSlot* ReduceScanNext(ReduceScanState *node);
static bool ReduceScanRecheck(ReduceScanState *node, TupleTableSlot *slot);
static uint32 ExecReduceScanGetHashValue(ExprContext *econtext, List *exprs, FmgrInfo *fmgr);
static void ExecReduceScanSaveTuple(ReduceScanState *node, TupleTableSlot *slot, uint32 hashvalue);

ReduceScanState *ExecInitReduceScan(ReduceScan *node, EState *estate, int eflags)
{
	Plan	   *outer_plan;
	ReduceScanState *rcs = makeNode(ReduceScanState);

	rcs->ss.ps.plan = (Plan*)node;
	rcs->ss.ps.state = estate;
	rcs->ss.ps.ExecProcNode = ExecReduceScan;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &rcs->ss.ps);

	/*
	 * initialize child expressions
	 */
	rcs->ss.ps.qual = ExecInitExpr((Expr *) node->plan.qual, (PlanState *) rcs);

	/*
	 * tuple table initialization
	 *
	 * material nodes only return tuples from their materialized relation.
	 */
	ExecInitResultTupleSlot(estate, &rcs->ss.ps);
	ExecInitScanTupleSlot(estate, &rcs->ss);

	outer_plan = outerPlan(node);
	outerPlanState(rcs) = ExecInitNode(outer_plan, estate, eflags);

	/*
	 * initialize tuple type.  no need to initialize projection info because
	 * this node doesn't do projections.
	 */
	ExecAssignResultTypeFromTL(&rcs->ss.ps);
	ExecAssignScanTypeFromOuterPlan(&rcs->ss);
	ExecAssignProjectionInfo(&rcs->ss.ps, rcs->ss.ss_ScanTupleSlot->tts_tupleDescriptor);

	if(node->param_hash_keys != NIL)
	{
		ListCell *lc;
		int i;
		int nbatches;
		int nbuckets;
		int nskew_mcvs;
		Assert(list_length(node->param_hash_keys) == list_length(node->scan_hash_keys));

		rcs->ncols_hash = list_length(node->param_hash_keys);
		ExecChooseHashTableSize(outer_plan->plan_rows,
								outer_plan->plan_width,
								false,
								&nbuckets,
								&nbatches,
								&nskew_mcvs);
		rcs->buffer_hash = hashstore_begin_heap(false, nbuckets);
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
	}

	return rcs;
}

TupleTableSlot *ExecReduceScan(PlanState *pstate)
{
	ReduceScanState *node = castNode(ReduceScanState, pstate);
	/* call FetchReduceScanOuter first */
	Assert(node->buffer_nulls != NULL);

	return ExecScan(&node->ss,
					(ExecScanAccessMtd)(ReduceScanNext),
					(ExecScanRecheckMtd)ReduceScanRecheck);
}

void FetchReduceScanOuter(ReduceScanState *node)
{
	TupleTableSlot *slot;
	PlanState *outer_ps;
	ExprContext *econtext;
	MemoryContext oldcontext;

	if(node->buffer_nulls)
		return;

	oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(node));
	node->buffer_nulls = tuplestore_begin_heap(false, false, work_mem);
	tuplestore_set_eflags(node->buffer_nulls, EXEC_FLAG_REWIND);

	if(node->scan_hash_exprs)
		PrepareTempTablespaces();

	/* we need read all outer slot first */
	outer_ps = outerPlanState(node);
	econtext = node->ss.ps.ps_ExprContext;
	if(node->scan_hash_exprs)
	{
		uint32 hashvalue;
		for(;;)
		{
			slot = ExecProcNode(outer_ps);
			if(TupIsNull(slot))
				break;

			ResetExprContext(econtext);
			econtext->ecxt_outertuple = slot;
			hashvalue = ExecReduceScanGetHashValue(econtext,
												   node->scan_hash_exprs,
												   node->scan_hash_funs);
			ExecReduceScanSaveTuple(node, slot, hashvalue);
		}
	}else
	{
		Tuplestorestate *buffer = node->buffer_nulls;
		for(;;)
		{
			slot = ExecProcNode(outer_ps);
			if(TupIsNull(slot))
				break;

			tuplestore_puttupleslot(buffer, slot);
		}
	}
	econtext->ecxt_outertuple = node->ss.ss_ScanTupleSlot;
	MemoryContextSwitchTo(oldcontext);
}

void ExecEndReduceScan(ReduceScanState *node)
{
	if(node->buffer_nulls)
	{
		tuplestore_end(node->buffer_nulls);
		node->buffer_nulls = NULL;
	}
	hashstore_end(node->buffer_hash);
	node->buffer_hash = NULL;
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
	if(node->buffer_nulls)
		tuplestore_rescan(node->buffer_nulls);
	if (node->cur_reader != INVALID_HASHSTORE_READER)
	{
		hashstore_end_read(node->buffer_hash, node->cur_reader);
		node->cur_reader = INVALID_HASHSTORE_READER;
	}

	if(node->param_hash_exprs)
	{
		ExprContext *econtext = node->ss.ps.ps_ExprContext;
		uint32 hashvalue = ExecReduceScanGetHashValue(econtext,
													  node->param_hash_exprs,
													  node->param_hash_funs);
		if (hashvalue != 0)
			node->cur_reader= hashstore_begin_read(node->buffer_hash, hashvalue);
		else
			tuplestore_rescan(node->buffer_nulls);
	}
}

static TupleTableSlot* ReduceScanNext(ReduceScanState *node)
{
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	CHECK_FOR_INTERRUPTS();
	if(node->cur_reader != INVALID_HASHSTORE_READER)
	{
		slot = hashstore_next_slot(node->buffer_hash, slot, node->cur_reader, false);
		if (TupIsNull(slot))
		{
			hashstore_end_read(node->buffer_hash, node->cur_reader);
			node->cur_reader = INVALID_HASHSTORE_READER;
		}
		return slot;
	}else
	{
		if(tuplestore_gettupleslot(node->buffer_nulls, true, true, slot))
			return slot;
		ExecClearTuple(slot);
		return NULL;
	}

	return NULL;	/* keep compler quiet */
}

static bool ReduceScanRecheck(ReduceScanState *node, TupleTableSlot *slot)
{
	return true;
}

static uint32 ExecReduceScanGetHashValue(ExprContext *econtext, List *exprs, FmgrInfo *fmgr)
{
	ListCell *lc;
	ExprState *expr_state;
	Datum key_value;
	uint32 hash_value = 0;
	int i;
	bool isnull;

	i = 0;
	foreach(lc, exprs)
	{
		expr_state = lfirst(lc);

		/* rotate hashkey left 1 bit at each step */
		hash_value = (hash_value << 1) | ((hash_value & 0x80000000) ? 1 : 0);

		key_value = ExecEvalExpr(expr_state, econtext, &isnull);
		if(isnull == false)
		{
			key_value = FunctionCall1(&fmgr[i], key_value);
			hash_value ^= DatumGetUInt32(key_value);
		}
		++i;
	}

	return hash_value;
}

static void ExecReduceScanSaveTuple(ReduceScanState *node, TupleTableSlot *slot, uint32 hashvalue)
{
	if(hashvalue)
	{
		hashstore_put_tupleslot(node->buffer_hash, slot, hashvalue);
	}else
	{
		tuplestore_puttupleslot(node->buffer_nulls, slot);
	}
}
