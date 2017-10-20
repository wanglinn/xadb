
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
	gatherstate->local_end = false;

	/*ExecAssignExprContext(estate, &gatherstate->ps);*/

	ExecInitResultTupleSlot(estate, &gatherstate->ps);

	ExecAssignResultTypeFromTL(&gatherstate->ps);

	outerPlanState(gatherstate) = ExecStartClusterPlan(outerPlan(node)
		, estate, flags, node->rnodes);
	if((flags & EXEC_FLAG_EXPLAIN_ONLY) == 0)
		gatherstate->remotes = PQNGetConnUseOidList(node->rnodes);

	gatherstate->recv_state = createClusterRecvState((PlanState*)gatherstate);

	return gatherstate;
}

TupleTableSlot *ExecClusterGather(ClusterGatherState *node)
{
	TupleTableSlot *slot;
	ClusterGatherType gatherType;
	bool blocking;
	ExecClearTuple(node->ps.ps_ResultTupleSlot);

	gatherType = ((ClusterGather*)node->ps.plan)->gatherType;
	blocking = false;	/* first time try nonblocking */
	while(node->remotes != NIL || node->local_end == false)
	{
		/* first try get remote data */
		if(node->remotes != NIL
			&& PQNListExecFinish(node->remotes, NULL, cg_pqexec_finish_hook, node, blocking))
		{
			if((gatherType & CLUSTER_GATHER_DATANODE) == 0)
			{
				ExecClearTuple(node->ps.ps_ResultTupleSlot);
				continue;
			}
			return node->ps.ps_ResultTupleSlot;
		}

		/* try local node */
		if(node->local_end == false)
		{
			slot = ExecProcNode(outerPlanState(node));
			if(TupIsNull(slot))
				node->local_end = true;
			else if(gatherType & CLUSTER_GATHER_COORD)
				return slot;
		}
		if(blocking)
			break;
		blocking = true;
	}

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
		PQNListExecFinish(node->remotes, NULL, PQNEFHNormal, NULL, true);
}

void ExecReScanClusterGather(ClusterGatherState *node)
{
}

static bool cg_pqexec_finish_hook(void *context, struct pg_conn *conn, PQNHookFuncType type, ...)
{
	ClusterGatherState *cgs;
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
			cgs = context;
			if(cgs->recv_state)
			{
				if(clusterRecvTupleEx(cgs->recv_state, buf, len, conn))
				{
					va_end(args);
					return true;
				}
			}else if(clusterRecvTuple(cgs->ps.ps_ResultTupleSlot, buf, len, &cgs->ps, conn))
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
	default:
		break;
	}
	return false;
}
