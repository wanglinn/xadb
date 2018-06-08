
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
#include "intercomm/inter-comm.h"
#include "libpq/libpq-fe.h"
#include "libpq/libpq-node.h"
#include "miscadmin.h"
#include "utils/memutils.h"

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
		gatherstate->remote_running = GetPGconnAttatchCurrentInterXact(node->rnodes);

	gatherstate->recv_state = createClusterRecvState((PlanState*)gatherstate, false);

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
	while(node->remote_running != NIL || node->local_end == false)
	{
		/* first try get remote data */
		node->last_run_end = NULL;
		if(node->remote_running != NIL
			&& PQNListExecFinish(node->remote_running, NULL, cg_pqexec_finish_hook, node, blocking))
		{
			if (node->last_run_end)
			{
				MemoryContext context = GetMemoryChunkContext(node);
				MemoryContext oldcontext = MemoryContextSwitchTo(context);
				node->remote_running = list_delete_ptr(node->remote_running, node->last_run_end);
				node->remote_run_end = lappend(node->remote_run_end , node->last_run_end);
				MemoryContextSwitchTo(oldcontext);
				continue;
			}
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

	/* when run here, no more tuple */

	return ExecClearTuple(node->ps.ps_ResultTupleSlot);
}

void ExecFinishClusterGather(ClusterGatherState *node)
{
	if (node->remote_run_end)
	{
		ListCell *lc;
		PGconn *conn;
		MemoryContext oldcontext = CurrentMemoryContext;
		MemoryContext context = GetMemoryChunkContext(node);

		foreach(lc, node->remote_running)
		{
			conn = lfirst(lc);
			if(PQisCopyInState(conn))
				PQputCopyEnd(conn, NULL);
		}

		while(node->remote_running != NIL)
		{
			node->last_run_end = NULL;
			ResetPerTupleExprContext(node->ps.state);
			PQNListExecFinish(node->remote_running, NULL, cg_pqexec_finish_hook, node, true);
			if (node->last_run_end)
			{
				MemoryContextSwitchTo(context);
				node->remote_running = list_delete_ptr(node->remote_running, node->last_run_end);
				node->remote_run_end = lappend(node->remote_run_end, node->last_run_end);
				MemoryContextSwitchTo(oldcontext);
			}
		}
	}

	PQNListExecFinish(node->remote_run_end, NULL, cg_pqexec_finish_hook, node, true);
}

void ExecEndClusterGather(ClusterGatherState *node)
{
	freeClusterRecvState(node->recv_state);
	ExecEndNode(outerPlanState(node));
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
			if (buf[0] == CLUSTER_MSG_EXECUTOR_RUN_END)
			{
				cgs->last_run_end = conn;
				va_end(args);
				return true;
			}else if(cgs->recv_state)
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
