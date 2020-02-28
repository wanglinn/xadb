
#include "postgres.h"

#include "executor/executor.h"
#include "executor/nodeEmptyResult.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"

static TupleTableSlot *ExecEmptyResult(PlanState *pstate);

EmptyResultState *ExecInitEmptyResult(EmptyResult *node, EState *estate, int eflags)
{
	ListCell *lc;
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

	foreach (lc, node->subPlan)
	{
		/*
		 * we don't use SubPlan expr,
		 * just init it, let it save in PlanState::subPlan
		 * so AdvanceReduce known it
		 */
		ExecInitExpr(lfirst(lc), &ers->ps);
	}

	/* initialize special node if need */
	switch(node->typeFrom)
	{
	case T_BitmapAnd:
	case T_BitmapOr:
	case T_BitmapIndexScan:
		ers->special = (Node*)tbm_create(64*1024, node->isshared ? estate->es_query_dsa : NULL);
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

static bool FindSubPlanWalker(Node *node, EmptyResult *result)
{
	if (node == NULL)
		return false;

	if(IsA(node, SubPlan))
	{
		result->subPlan = lappend(result->subPlan, node);
		return false;
	}

	return expression_tree_walker(node, FindSubPlanWalker, result);
}

Plan* MakeEmptyResultPlan(Plan *from)
{
	ListCell	   *lc;
	TargetEntry	   *te;
	Expr		   *expr;
	EmptyResult	   *result = palloc0(sizeof(EmptyResult));
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

	FindSubPlanWalker((Node*)from->targetlist, result);
	FindSubPlanWalker((Node*)from->qual, result);
	switch(nodeTag(from))
	{
	case T_ForeignScan:
		{
			ForeignScan *fs = (ForeignScan*)from;
			FindSubPlanWalker((Node*)fs->fdw_exprs, result);
			FindSubPlanWalker((Node*)fs->fdw_scan_tlist, result);
			FindSubPlanWalker((Node*)fs->fdw_recheck_quals, result);
		}
		break;
	case T_Agg:
	case T_SeqScan:
	case T_TidScan:
		break;
	case T_IndexScan:
		{
			IndexScan *is = (IndexScan*)from;
			FindSubPlanWalker((Node*)is->indexqual, result);
			FindSubPlanWalker((Node*)is->indexqualorig, result);
			FindSubPlanWalker((Node*)is->indexorderby, result);
			FindSubPlanWalker((Node*)is->indexorderbyorig, result);
		}
		break;
	case T_IndexOnlyScan:
		{
			IndexOnlyScan *ios = (IndexOnlyScan*)from;
			FindSubPlanWalker((Node*)ios->indexqual, result);
			FindSubPlanWalker((Node*)ios->indexorderby, result);
			FindSubPlanWalker((Node*)ios->indextlist, result);
		}
		break;
	case T_BitmapIndexScan:
		{
			BitmapIndexScan *bis = (BitmapIndexScan*)from;
			FindSubPlanWalker((Node*)bis->indexqual, result);
			FindSubPlanWalker((Node*)bis->indexqualorig, result);
			result->isshared = bis->isshared;
		}
		break;
	case T_BitmapOr:
		result->isshared = ((BitmapOr*)from)->isshared;
		break;
	default:
		ereport(ERROR,
				(errmsg("unknown plan type %d to EmptyResult", nodeTag(from))));
		break;
	}

	return (Plan*)result;
}
