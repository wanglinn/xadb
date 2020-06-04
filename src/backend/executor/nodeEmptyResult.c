
#include "postgres.h"

#include "executor/executor.h"
#include "executor/nodeEmptyResult.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"

static TupleTableSlot *ExecEmptyResult(PlanState *pstate);

EmptyResultState *ExecInitEmptyResult(EmptyResult *node, EState *estate, int eflags)
{
	EmptyResultState *ers = makeNode(EmptyResultState);

	ers->ps.plan = (Plan*) node;
	ers->ps.state = estate;
	ers->ps.ExecProcNode = ExecEmptyResult;

	/*
	 * initialize outer and inner nodes if exist
	 */
	if (outerPlan(node))
		outerPlanState(ers) = ExecInitNode(outerPlan(node), estate, 0);
	if (innerPlan(node))
		innerPlanState(ers) = ExecInitNode(innerPlan(node), estate, 0);

	/*
	 * initialize tuple table and tuple type
	 */
	ExecInitResultTupleSlotTL(&ers->ps, &TTSOpsVirtual);

	/* initialize special node if need */
	switch(node->typeFrom)
	{
	case T_BitmapAnd:
	case T_BitmapOr:
	case T_BitmapIndexScan:
		ers->special = (Node*)tbm_create(64*1024L, NULL);
		break;
	default:
		break;
	}

	return ers;
}

static TupleTableSlot *ExecEmptyResult(PlanState *pstate)
{
	EmptyResultState *node = castNode(EmptyResultState, pstate);
	if (node->special)
	{
		elog(ERROR,
			 "Empty result node does not support ExecProcNode call convention when from %d",
			 ((EmptyResult*)(node->ps.plan))->typeFrom);
	}

	return ExecClearTuple(node->ps.ps_ResultTupleSlot);
}

void ExecEndEmptyResult(EmptyResultState *node)
{
	if (outerPlanState(node))
		ExecEndNode(outerPlanState(node));
	if (innerPlanState(node))
		ExecEndNode(innerPlanState(node));
}
void ExecEmptyResultMarkPos(EmptyResultState *node)
{
	/* nothing todo */
}
void ExecEmptyResultRestrPos(EmptyResultState *node)
{
	/* nothing todo */
}
void ExecReScanEmptyResult(EmptyResultState *node)
{
	/* nothing todo */
}

Node* MultiExecEmptyResult(EmptyResultState *node)
{
	if (node->special == NULL)
	{
		elog(ERROR,
			 "Empty result node does not MultiExecProcNode call when from %d",
			 ((EmptyResult*)(node->ps.plan))->typeFrom);
	}
	return node->special;
}

Plan* MakeEmptyResultPlan(Plan *from)
{
	ListCell *lc;
	TargetEntry *te;
	Expr *expr;
	EmptyResult *result = palloc(sizeof(EmptyResult));
	memcpy(result, from, sizeof(Plan));
	NodeSetTag(result, T_EmptyResult);

	result->typeFrom = nodeTag(from);
	result->plan.plan_rows = 0.0;
	result->plan.plan_width = 0;
	result->plan.startup_cost = result->plan.total_cost = 0.0;

	result->plan.targetlist = NIL;
	foreach(lc, from->targetlist)
	{
		te = palloc(sizeof(*te));
		memcpy(te, lfirst(lc), sizeof(*te));

		expr = te->expr;
		te->expr = (Expr*)makeNullConst(exprType((Node*)expr), exprTypmod((Node*)expr), exprCollation((Node*)expr));

		if (te->resname)
			te->resname = strdup(te->resname);
		te->resorigtbl = InvalidOid;
		te->resorigcol = InvalidAttrNumber;

		result->plan.targetlist = lappend(result->plan.targetlist, te);
	}

	return (Plan*)result;
}
