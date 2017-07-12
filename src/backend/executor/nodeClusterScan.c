
/*-------------------------------------------------------------------------
 *
 * src/backend/executor/nodeClusterScan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor/executor.h"
#include "executor/execCluster.h"
#include "executor/nodeClusterScan.h"
#include "pgxc/pgxc.h"

ClusterScanState *ExecInitClusterScan(ClusterScan *node, EState *estate, int flags)
{
	ClusterScanState *ss;

	Assert(outerPlan(node) != NULL);
	Assert(innerPlan(node) == NULL);

	ss = makeNode(ClusterScanState);
	ss->ps.plan = (Plan*)node;
	ss->ps.state = estate;

	/*ExecAssignExprContext(estate, &ss->ps);*/

	ExecInitResultTupleSlot(estate, &ss->ps);

	outerPlanState(ss) = ExecInitNode(outerPlan(node), estate, flags);

	ExecAssignResultTypeFromTL(&ss->ps);

	if(list_member_oid(node->rnodes, PGXCNodeOid))
		ss->run_node = true;

	return ss;
}

TupleTableSlot *ExecClusterScan(ClusterScanState *node)
{
	if(node->run_node)
		return ExecProcNode(outerPlanState(node));
	return NULL;
}

void ExecEndClusterScan(ClusterScanState *node)
{
	/*ExecFreeExprContext(&node->ps);*/
	ExecEndNode(outerPlanState(node));
}

void ExecReScanClusterScan(ClusterScanState *node)
{
	if(node->run_node)
		ExecReScan(outerPlanState(node));
}
