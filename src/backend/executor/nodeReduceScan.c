
#include "postgres.h"

#include "executor/executor.h"
#include "executor/nodeReduceScan.h"
#include "miscadmin.h"
#include "utils/memutils.h"

static TupleTableSlot* ReduceScanNext(ReduceScanState *node);
static bool ReduceScanRecheck(ReduceScanState *node, TupleTableSlot *slot);

ReduceScanState *ExecInitReduceScan(ReduceScan *node, EState *estate, int eflags)
{
	Plan	   *outerPlan;
	ReduceScanState *rcs = makeNode(ReduceScanState);

	rcs->ss.ps.plan = (Plan*)node;
	rcs->ss.ps.state = estate;
	rcs->eflags = EXEC_FLAG_REWIND;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &rcs->ss.ps);

	/*
	 * initialize child expressions
	 */
	rcs->ss.ps.targetlist = (List *)ExecInitExpr((Expr *) node->targetlist, (PlanState *) rcs);
	rcs->ss.ps.qual = (List *)ExecInitExpr((Expr *) node->qual, (PlanState *) rcs);

	/*
	 * tuple table initialization
	 *
	 * material nodes only return tuples from their materialized relation.
	 */
	ExecInitResultTupleSlot(estate, &rcs->ss.ps);
	ExecInitScanTupleSlot(estate, &rcs->ss);

	outerPlan = outerPlan(node);
	outerPlanState(rcs) = ExecInitNode(outerPlan, estate, eflags);

	/*
	 * initialize tuple type.  no need to initialize projection info because
	 * this node doesn't do projections.
	 */
	ExecAssignResultTypeFromTL(&rcs->ss.ps);
	ExecAssignScanTypeFromOuterPlan(&rcs->ss);
	//ExecAssignScanProjectionInfo(&rcs->ss);
	ExecAssignProjectionInfo(&rcs->ss.ps, rcs->ss.ss_ScanTupleSlot->tts_tupleDescriptor);
	rcs->ss.ps.ps_ExprContext->ecxt_outertuple = rcs->ss.ss_ScanTupleSlot;

	return rcs;
}

TupleTableSlot *ExecReduceScan(ReduceScanState *node)
{
	/* call FetchReduceScanOuter first */
	Assert(node->buffer != NULL);

	return ExecScan(&node->ss,
					(ExecScanAccessMtd)ReduceScanNext,
					(ExecScanRecheckMtd)ReduceScanRecheck);
}

void FetchReduceScanOuter(ReduceScanState *node)
{
	Tuplestorestate *store;
	TupleTableSlot *slot;
	PlanState *outer_ps;
	MemoryContext oldcontext;
	MemoryContext context;

	if(node->buffer)
		return;

	context = GetMemoryChunkContext(node);
	oldcontext = MemoryContextSwitchTo(context);
	store = tuplestore_begin_heap(false, false, work_mem);
	tuplestore_set_eflags(store, node->eflags);

	/* we need read all outer slot first */
	outer_ps = outerPlanState(node);
	for(;;)
	{
		slot = ExecProcNode(outer_ps);
		if(TupIsNull(slot))
			break;
		tuplestore_puttupleslot(store, slot);
	}
	node->buffer = store;
	MemoryContextSwitchTo(oldcontext);
}

void ExecEndReduceScan(ReduceScanState *node)
{
	if(node->buffer)
	{
		tuplestore_end(node->buffer);
		node->buffer = NULL;
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
	if(node->buffer)
		tuplestore_rescan(node->buffer);
}

static TupleTableSlot* ReduceScanNext(ReduceScanState *node)
{
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	if(tuplestore_gettupleslot(node->buffer, true, true, slot))
		return slot;
	ExecClearTuple(slot);
	return NULL;
}

static bool ReduceScanRecheck(ReduceScanState *node, TupleTableSlot *slot)
{
	return true;
}