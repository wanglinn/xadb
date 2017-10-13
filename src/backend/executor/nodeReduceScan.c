
#include "postgres.h"

#include "access/htup_details.h"
#include "commands/tablespace.h"
#include "executor/executor.h"
#include "executor/nodeHash.h"
#include "executor/nodeReduceScan.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "storage/buffile.h"
#include "utils/memutils.h"
#include "utils/typcache.h"

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

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &rcs->ss.ps);

	/*
	 * initialize child expressions
	 */
	rcs->ss.ps.targetlist = (List *)ExecInitExpr((Expr *) node->plan.targetlist, (PlanState *) rcs);
	rcs->ss.ps.qual = (List *)ExecInitExpr((Expr *) node->plan.qual, (PlanState *) rcs);

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
		int nskew_mcvs;
		Assert(list_length(node->param_hash_keys) == list_length(node->scan_hash_keys));

		rcs->ncols_hash = list_length(node->param_hash_keys);
		ExecChooseHashTableSize(outer_plan->plan_rows,
								outer_plan->plan_width,
								false,
								&rcs->nbuckets,
								&nbatches,
								&nskew_mcvs);
		rcs->hash_files = palloc0(rcs->nbuckets * sizeof(rcs->hash_files[0]));
		rcs->param_hash_exprs = (List*)ExecInitExpr((Expr*)node->param_hash_keys, (PlanState*)rcs);
		rcs->scan_hash_exprs = (List*)ExecInitExpr((Expr*)node->scan_hash_keys, (PlanState*)rcs);
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

TupleTableSlot *ExecReduceScan(ReduceScanState *node)
{
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
	int i;
	if(node->buffer_nulls)
	{
		tuplestore_end(node->buffer_nulls);
		node->buffer_nulls = NULL;
	}
	for(i=0;i<node->nbuckets;++i)
	{
		BufFile *file = node->hash_files[i];
		if(file)
			BufFileClose(file);
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
	if(node->buffer_nulls)
		tuplestore_rescan(node->buffer_nulls);
	node->cur_hashvalue = 0;
	if(node->param_hash_exprs)
	{
		ExprContext *econtext = node->ss.ps.ps_ExprContext;
		node->cur_hashvalue = ExecReduceScanGetHashValue(econtext,
														 node->param_hash_exprs,
														 node->param_hash_funs);
	}
	node->cur_hash_file = NULL;
}

static TupleTableSlot* ReduceScanNext(ReduceScanState *node)
{
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	if(node->cur_hashvalue)
	{
		size_t nread;
		MinimalTuple tuple;
		uint32 header[2];

		if(node->cur_hash_file == NULL)
		{
			node->cur_hash_file = node->hash_files[node->cur_hashvalue % node->nbuckets];

			if(node->cur_hash_file == NULL)
			{
				ExecClearTuple(slot);
				return NULL;
			}

			if(BufFileSeek(node->cur_hash_file, 0, 0L, SEEK_SET) != 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not seek in reduce-scan temporary file: %m")));
		}

		for(;;)
		{
			CHECK_FOR_INTERRUPTS();
			nread = BufFileRead(node->cur_hash_file, header, sizeof(header));
			if(nread == 0)
			{
				ExecClearTuple(slot);
				return NULL;
			}else if(nread != sizeof(header))
			{
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read from reduce-scan temporary file: %m")));
			}
			if(header[0] != node->cur_hashvalue)
			{
				if(BufFileSeek(node->cur_hash_file, 0, header[1]-sizeof(uint32), SEEK_CUR) != 0)
					ereport(ERROR,
							(errcode_for_file_access(),
							errmsg("could not seek in reduce-scan temporary file: %m")));
				continue;
			}else
			{
				tuple = palloc(header[1]);
				tuple->t_len = header[1];
				nread = BufFileRead(node->cur_hash_file,
									((char*)tuple)+sizeof(uint32),
									header[1] - sizeof(uint32));
				if(nread != header[1] - sizeof(uint32))
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not read from reduce-scan temporary file: %m")));
				ExecStoreMinimalTuple(tuple, slot, true);
				return slot;
			}
		}
	}else
	{
		CHECK_FOR_INTERRUPTS();
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

		key_value = ExecEvalExpr(expr_state, econtext, &isnull, NULL);
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
		BufFile	*file;
		BufFile	**ppbuf;
		size_t		written;
		MinimalTuple tuple = ExecFetchSlotMinimalTuple(slot);

		ppbuf = &(node->hash_files[hashvalue % node->nbuckets]);
		if(*ppbuf == NULL)
			*ppbuf = BufFileCreateTemp(false);
		file = *ppbuf;

		written = BufFileWrite(file, (void *) &hashvalue, sizeof(uint32));
		if (written != sizeof(uint32))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write to reduce-scan temporary file: %m")));

		written = BufFileWrite(file, (void *) tuple, tuple->t_len);
		if (written != tuple->t_len)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write to reduce-scan temporary file: %m")));
	}else
	{
		tuplestore_puttupleslot(node->buffer_nulls, slot);
	}
}
