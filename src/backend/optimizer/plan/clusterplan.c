#include "postgres.h"

#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "optimizer/planmain.h"

typedef struct ReducePlanFindContext
{
	List	   *exclude_node;
	List	   *list_reduce;
}ReducePlanFindContext;

static bool ReducePlanFindWalker(Plan *plan, PlannedStmt *stmt, void *context)
{
	ReducePlanFindContext  *finder;
	Plan				   *exclude_node;
	bool					result;
	if (plan == NULL)
		return false;

	finder = context;

	if (list_member_ptr(finder->exclude_node, plan))
		return false;

	exclude_node = NULL;
	switch(nodeTag(plan))
	{
	/*case T_Sort:
	case T_Hash:
	case T_Unique:
		return false;
	case T_Agg:
		if (((Agg*)plan)->aggstrategy != AGG_BATCH_HASH)
			exclude_node = outerPlan(plan);
		break;
	case T_WindowAgg:*/
	case T_ReduceScan:
		exclude_node = outerPlan(plan);
		break;
	case T_ClusterReduce:
		finder->list_reduce = lappend(finder->list_reduce, plan);
		break;
	default:
		break;
	}

	if (exclude_node)
		finder->exclude_node = lcons(exclude_node, finder->exclude_node);
	result = plan_tree_walker(plan, (Node*)stmt, ReducePlanFindWalker, context);
	if (exclude_node)
	{
		Assert(linitial(finder->exclude_node) == exclude_node);
		finder->exclude_node = list_delete_first(finder->exclude_node);
	}

	return result;
}

static List* FindReduceInSubPlan(List *subplan_list, PlannedStmt *stmt)
{
	ReducePlanFindContext	context;
	ListCell			   *lc;
	SubPlan				   *subplan;
	Plan				   *plan;

	if (subplan_list == NIL)
		return NIL;

	MemSet(&context, 0, sizeof(context));
	foreach(lc, subplan_list)
	{
		subplan = lfirst(lc);
		if (IsA(subplan, SubPlan))
		{
			plan = list_nth(stmt->subplans, subplan->plan_id-1);
			(void)ReducePlanFindWalker(plan, stmt, &context);
		}
	}

	return context.list_reduce;
}

static inline List* FindReducePlan(Plan *plan, PlannedStmt *stmt)
{
	ReducePlanFindContext	context;

	MemSet(&context, 0, sizeof(context));
	(void)ReducePlanFindWalker(plan, stmt, &context);
	return context.list_reduce;
}

static bool OptimizeClusterReduceWalker(Plan *plan, PlannedStmt *stmt, void *context)
{
	if (plan == NULL)
		return false;

	if (IsA(plan, NestLoop))
	{
		List	   *init_reduce;
		List	   *inner_reduce = NIL;
		List	   *outer_reduce;
		ListCell   *lc;

		if ((init_reduce = FindReduceInSubPlan(plan->initPlan, stmt)) != NIL ||
			(inner_reduce = FindReducePlan(innerPlan(plan), stmt)) != NIL)
		{
			outer_reduce = FindReducePlan(outerPlan(plan), stmt);
			foreach (lc, outer_reduce)
				lfirst_node(ClusterReduce, lc)->reduce_flags |= CRF_FETCH_LOCAL_FIRST;
			foreach (lc, inner_reduce)
				lfirst_node(ClusterReduce, lc)->reduce_flags |= CRF_DISK_ALWAYS;
			list_free(outer_reduce);
			list_free(inner_reduce);
			list_free(init_reduce);
		}
	}else if(IsA(plan, Hash) &&
			 IsA(outerPlan(plan), ClusterReduce))
	{
		((ClusterReduce*)outerPlan(plan))->reduce_flags |= CRF_DISK_UNNECESSARY;
		((ClusterReduce*)outerPlan(plan))->reduce_flags &= ~(CRF_FETCH_LOCAL_FIRST|CRF_DISK_ALWAYS);
	}else if (IsA(plan, ClusterReduce))
	{
		List	   *outer_reduce = FindReducePlan(outerPlan(plan), stmt);
		ListCell   *lc;
		foreach (lc, outer_reduce)
			lfirst_node(ClusterReduce, lc)->reduce_flags |= CRF_FETCH_LOCAL_FIRST;
		list_free(outer_reduce);
	}

	return plan_tree_walker(plan, (Node*)stmt, OptimizeClusterReduceWalker, context);
}

void OptimizeClusterPlan(PlannedStmt *stmt)
{
	(void)OptimizeClusterReduceWalker(stmt->planTree, stmt, NULL);
}
