
/*-------------------------------------------------------------------------
 *
 * src/backend/executor/nodeClusterGather.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor/executor.h"
#include "executor/clusterReceiver.h"
#include "executor/execCluster.h"
#include "executor/nodeClusterGather.h"
#include "libpq/libpq-fe.h"
#include "libpq/libpq-node.h"
#include "miscadmin.h"

static bool cg_pqexec_finish_hook(void *context, struct pg_conn *conn, PQNHookFuncType type, ...);

ClusterGatherState *ExecInitClusterGather(ClusterGather *node, EState *estate, int flags)
{
	ClusterGatherState *gatherstate;

	Assert(outerPlan(node) != NULL);
	Assert(innerPlan(node) == NULL);
	Assert((flags & (EXEC_FLAG_MARK|EXEC_FLAG_BACKWARD|EXEC_FLAG_REWIND)) == 0);

	gatherstate = makeNode(ClusterGatherState);
	gatherstate->ps.plan = (Plan*)node;
	gatherstate->ps.state = estate;

	/*ExecAssignExprContext(estate, &gatherstate->ps);*/

	ExecInitResultTupleSlot(estate, &gatherstate->ps);

	ExecAssignResultTypeFromTL(&gatherstate->ps);

	outerPlanState(gatherstate) = ExecStartClusterPlan(outerPlan(node)
		, estate, flags, node->rnodes);
	if((flags & EXEC_FLAG_EXPLAIN_ONLY) == 0)
		gatherstate->remotes = PQNGetConnUseOidList(node->rnodes);

	return gatherstate;
}

TupleTableSlot *ExecClusterGather(ClusterGatherState *node)
{
	ExecClearTuple(node->ps.ps_ResultTupleSlot);
	PQNListExecFinish(node->remotes, cg_pqexec_finish_hook, node);
	return node->ps.ps_ResultTupleSlot;
}

void ExecEndClusterGather(ClusterGatherState *node)
{
	ListCell *lc;
	PGconn *conn;
	foreach(lc, node->remotes)
	{
		conn = lfirst(lc);
		if(PQisCopyInState(conn))
			PQputCopyEnd(conn, NULL);
	}
	ExecEndNode(outerPlanState(node));
	if(node->remotes != NIL)
		PQNListExecFinish(node->remotes, PQNEFHNormal, NULL);
}

void ExecReScanClusterGather(ClusterGatherState *node)
{
}

static bool cg_pqexec_finish_hook(void *context, struct pg_conn *conn, PQNHookFuncType type, ...)
{
	PlanState *ps;
	va_list args;
	switch(type)
	{
	case PQNHFT_ERROR:
		return PQNEFHNormal(NULL, conn, type);
	case PQNHFT_COPY_OUT_DATA:
		{
			const char *buf;
			int len;
			va_start(args, type);
			buf = va_arg(args, const char*);
			len = va_arg(args, int);
			ps = context;
			if(clusterRecvTuple(ps->ps_ResultTupleSlot, buf, len, ps, conn))
			{
				va_end(args);
				return true;
			}
			va_end(args);
		}
		break;
	case PQNHFT_COPY_IN_ONLY:
		PQputCopyEnd(conn, NULL);
		break;
	case PQNHFT_RESULT:
		{
			PGresult *res;
			va_start(args, type);
			res = va_arg(args, PGresult*);
			if(res)
			{
				ExecStatusType status = PQresultStatus(res);
				if(status == PGRES_FATAL_ERROR)
					PQNReportResultError(res, conn, ERROR, true);
				else if(status == PGRES_COPY_IN)
					PQputCopyEnd(conn, NULL);
			}
			va_end(args);
		}
		break;
	}
	return false;
}