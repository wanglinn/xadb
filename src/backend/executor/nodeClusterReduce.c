
#include "postgres.h"

#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "pgxc/pgxc.h"

#include "executor/nodeClusterReduce.h"

ClusterReduceState *ExecInitClusterReduce(ClusterReduce *node, EState *estate, int eflags)
{
	ClusterReduceState *rs;

	Assert(outerPlan(node) != NULL);
	Assert(innerPlan(node) == NULL);
	Assert((eflags & (EXEC_FLAG_MARK|EXEC_FLAG_BACKWARD|EXEC_FLAG_REWIND)) == 0);

	rs = makeNode(ClusterReduceState);
	rs->ps.plan = (Plan*)node;
	rs->ps.state = estate;

	ExecInitResultTupleSlot(estate, &rs->ps);
	ExecAssignExprContext(estate, &rs->ps);

	ExecAssignResultTypeFromTL(&rs->ps);

	rs->reduceState = ExecInitExpr(node->reduce, &rs->ps);

	outerPlanState(rs) = ExecInitNode(outerPlan(node), estate, eflags);

	/*if((flags & EXEC_FLAG_EXPLAIN_ONLY) == 0)
	{
		TODO init cluster execute info
	}*/

	return rs;
}

TupleTableSlot *ExecClusterReduce(ClusterReduceState *node)
{
	TupleTableSlot *slot;
	ExprContext *econtext;
	ExprDoneCond done;
	bool isNull;
	Oid oid;

	slot = ExecProcNode(outerPlanState(node));
	ExecClearTuple(node->ps.ps_ResultTupleSlot);
	if(!TupIsNull(slot))
	{
		econtext = node->ps.ps_ExprContext;
		econtext->ecxt_outertuple = slot;
		for(;;)
		{
			Datum datum;
			datum = ExecEvalExpr(node->reduceState, econtext, &isNull, &done);
			if(isNull)
			{
				/* TODO null */
			}else if(done == ExprEndResult)
			{
				break;
			}else
			{
				oid = DatumGetObjectId(datum);
				if(oid == PGXCNodeOid)
				{
					ExecCopySlot(node->ps.ps_ResultTupleSlot, slot);
				}else
				{
					/* send to remote */
				}
				if(done == ExprSingleResult)
					break;
			}
		}
	}

	return node->ps.ps_ResultTupleSlot;
}

void ExecEndClusterReduce(ClusterReduceState *node)
{
}
