/*-------------------------------------------------------------------------
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeParamTuplestoreScan.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/executor.h"
#include "executor/nodeParamTuplestoreScan.h"
#include "miscadmin.h"
#include "pgxc/pgxc.h"	/* for PGXCNodeOid */

static TupleTableSlot *ExecParamTuplestoreScan(PlanState *ps);

/* ----------------------------------------------------------------
 *		ParamTuplestoreScanNext
 *
 *		This is a workhorse for ExecParamTuplestoreScan
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ParamTuplestoreScanNext(ParamTuplestoreScanState *node)
{
	TupleTableSlot *slot;

	/* We intentionally do not support backward scan. */
	Assert(ScanDirectionIsForward(node->ss.ps.state->es_direction));

	/*
	 * Get the next tuple from tuplestore. Return NULL if no more tuples.
	 */
	slot = node->ss.ss_ScanTupleSlot;
	tuplestore_select_read_pointer(node->ts, node->readptr);
	(void) tuplestore_gettupleslot(node->ts, true, false, slot);
	return slot;
}

static TupleTableSlot *ParamTuplestoreScanNull(ParamTuplestoreScanState* node)
{
	return NULL;
}

static TupleTableSlot *
ParamTuplestoreScanInit(ParamTuplestoreScanState *node)
{
	EState *estate;
	ParamExecData *param;
	Tuplestorestate *ts;
	ParamTuplestoreScan *scan = (ParamTuplestoreScan*)node->ss.ps.plan;
	if (list_member_oid(scan->scan.execute_nodes, PGXCNodeOid) == false)
	{
		node->ScanNext = (ExecScanAccessMtd)ParamTuplestoreScanNull;
		return NULL;
	}

	if (node->readptr > 0)
	{
		Assert(node->ts != NULL);
		return ParamTuplestoreScanNext(node);
	}

	estate = node->ss.ps.state;
	param = &(estate->es_param_exec_vals[scan->paramid]);
	ts = (Tuplestorestate*)DatumGetPointer(param->value);
	if (ts == NULL)
	{
		ereport(ERROR,
				(errmsg("ParamTuplestoreScan can not found tuplestore"),
				 errdetail("param id %d", scan->paramid),
				 errcode(ERRCODE_INTERNAL_ERROR)));
	}
	node->ts = ts;
	node->readptr = tuplestore_alloc_read_pointer(ts, 0);
	node->ScanNext = (ExecScanAccessMtd)ParamTuplestoreScanNext;
	return ParamTuplestoreScanNext(node);
}
/*
 * ParamTuplestoreScanRecheck -- access method routine to recheck a tuple in
 * EvalPlanQual
 */
static bool
ParamTuplestoreScanRecheck(ParamTuplestoreScanState *node, TupleTableSlot *slot)
{
	/* nothing to check */
	return true;
}

/* ----------------------------------------------------------------
 *		ExecParamTuplestoreScan(node)
 *
 *		Scans the CTE sequentially and returns the next qualifying tuple.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecParamTuplestoreScan(PlanState *ps)
{
	ParamTuplestoreScanState *node = castNode(ParamTuplestoreScanState, ps);
	return ExecScan(&node->ss,
					node->ScanNext,
					(ExecScanRecheckMtd) ParamTuplestoreScanRecheck);
}


/* ----------------------------------------------------------------
 *		ExecInitParamTuplestoreScan
 * ----------------------------------------------------------------
 */
ParamTuplestoreScanState *
ExecInitParamTuplestoreScan(ParamTuplestoreScan *node, EState *estate, int eflags)
{
	ParamTuplestoreScanState *scanstate;
	ListCell *lc;
	AttrNumber attno;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * ParamTuplestoreScan should not have any children.
	 */
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create new ParamTuplestoreScanState for node
	 */
	scanstate = makeNode(ParamTuplestoreScanState);
	scanstate->ss.ps.plan = (Plan *) node;
	scanstate->ss.ps.state = estate;
	scanstate->ts = NULL;
	scanstate->ScanNext = (ExecScanAccessMtd)ParamTuplestoreScanInit;
	scanstate->ss.ps.ExecProcNode = ExecParamTuplestoreScan;

	scanstate->tupdesc = CreateTemplateTupleDesc(list_length(node->vars));
	attno = 0;
	foreach(lc, node->vars)
	{
		Var *var = lfirst(lc);
		Assert(IsA(var, Var));
		TupleDescInitEntry(scanstate->tupdesc,
						   ++attno,
						   NULL,
						   var->vartype,
						   var->vartypmod,
						   0);
	}

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &scanstate->ss.ps);

	/* and create slot with the appropriate rowtype */
	ExecInitScanTupleSlot(estate, &scanstate->ss, scanstate->tupdesc, &TTSOpsMinimalTuple);

	/*
	 * initialize child expressions
	 */
	scanstate->ss.ps.qual = ExecInitQual(node->scan.plan.qual, (PlanState*)scanstate);

	/*
	 * Initialize result tuple type and projection info.
	 * The scan tuple type is specified for the tuplestore.
	 */
	ExecInitResultTupleSlotTL(&scanstate->ss.ps, &TTSOpsVirtual);
	ExecConditionalAssignProjectionInfo(&scanstate->ss.ps, scanstate->tupdesc, node->scan.scanrelid);

	return scanstate;
}

/* ----------------------------------------------------------------
 *		ExecEndParamTuplestoreScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void
ExecEndParamTuplestoreScan(ParamTuplestoreScanState *node)
{
	/*
	 * Free exprcontext
	 */
	ExecFreeExprContext(&node->ss.ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);
}

/* ----------------------------------------------------------------
 *		ExecReScanParamTuplestoreScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanParamTuplestoreScan(ParamTuplestoreScanState *node)
{
	Tuplestorestate *tuplestorestate = node->ts;

	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

	ExecScanReScan(&node->ss);

	/*
	 * Rewind my own pointer.
	 */
	if (tuplestorestate)
	{
		tuplestore_select_read_pointer(tuplestorestate, node->readptr);
		tuplestore_rescan(tuplestorestate);
	}
}
