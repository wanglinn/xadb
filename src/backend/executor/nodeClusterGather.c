
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

#define CG_HOOK_GET_STATE(hook_) ((ClusterGatherState*)((char*)hook_ - offsetof(ClusterGatherState, hook_funcs)))

static TupleTableSlot *ExecClusterGather(PlanState *pstate);
static bool cg_pqexec_recv_hook(PQNHookFunctions *pub, struct pg_conn *conn, const char *buf, int len);

ClusterGatherState *ExecInitClusterGather(ClusterGather *node, EState *estate, int flags)
{
	ClusterGatherState *gatherstate;
	TupleDesc			tupDesc;

	Assert(outerPlan(node) != NULL);
	Assert(innerPlan(node) == NULL);
	Assert((flags & (EXEC_FLAG_MARK|EXEC_FLAG_BACKWARD|EXEC_FLAG_REWIND)) == 0);

	gatherstate = makeNode(ClusterGatherState);
	gatherstate->ps.plan = (Plan*)node;
	gatherstate->ps.state = estate;
	gatherstate->ps.ExecProcNode = ExecClusterGather;
	gatherstate->local_end = false;

	ExecAssignExprContext(estate, &gatherstate->ps);

	outerPlanState(gatherstate) = ExecStartClusterPlan(outerPlan(node),
													   estate, flags, node->rnodes);
	tupDesc = ExecGetResultType(outerPlanState(gatherstate));

	/*
	 * Initialize result slot, type and projection.
	 */
	ExecInitResultTupleSlotTL(estate, &gatherstate->ps);
	ExecConditionalAssignProjectionInfo(&gatherstate->ps, tupDesc, OUTER_VAR);

	if((flags & EXEC_FLAG_EXPLAIN_ONLY) == 0)
		gatherstate->remote_running = GetPGconnAttatchCurrentInterXact(node->rnodes);

	gatherstate->recv_state = createClusterRecvState((PlanState*)gatherstate, false);

	gatherstate->check_rep_processed = node->check_rep_processed;

	gatherstate->hook_funcs = PQNDefaultHookFunctions;
	gatherstate->hook_funcs.HookCopyOut = cg_pqexec_recv_hook;

	return gatherstate;
}

static TupleTableSlot *ExecClusterGather(PlanState *pstate)
{
	TupleTableSlot *slot;
	ClusterGatherState *node = castNode(ClusterGatherState, pstate);
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
			&& PQNListExecFinish(node->remote_running, NULL, &node->hook_funcs, blocking))
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
			PQNListExecFinish(node->remote_running, NULL, &node->hook_funcs, true);
			if (node->last_run_end)
			{
				MemoryContextSwitchTo(context);
				node->remote_running = list_delete_ptr(node->remote_running, node->last_run_end);
				node->remote_run_end = lappend(node->remote_run_end, node->last_run_end);
				MemoryContextSwitchTo(oldcontext);
			}
		}
	}

	PQNListExecFinish(node->remote_run_end, NULL, &node->hook_funcs, true);
}

void ExecEndClusterGather(ClusterGatherState *node)
{
	freeClusterRecvState(node->recv_state);
	ExecEndNode(outerPlanState(node));
}

void ExecReScanClusterGather(ClusterGatherState *node)
{
}

static bool cg_pqexec_recv_hook(PQNHookFunctions *pub, struct pg_conn *conn, const char *buf, int len)
{
	ClusterGatherState *cgs = CG_HOOK_GET_STATE(pub);

	if (buf[0] == CLUSTER_MSG_EXECUTOR_RUN_END)
	{
		cgs->last_run_end = conn;
		return true;
	}else if (buf[0] == CLUSTER_MSG_PROCESSED)
	{
		EState *estate = cgs->ps.state;
		uint64 processed = restore_processed_message(buf+1, len-1);
		if (cgs->check_rep_processed)
		{
			if (cgs->got_processed)
			{
				if (estate->es_processed != processed)
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
								errmsg("All datanode modified table row count not same")));
			}else
			{
				cgs->got_processed = true;
				estate->es_processed += processed;
			}
		}else
		{
			estate->es_processed += processed;
		}
	}else if(cgs->recv_state)
	{
		if(clusterRecvTupleEx(cgs->recv_state, buf, len, conn))
			return true;
	}else if(clusterRecvTuple(cgs->ps.ps_ResultTupleSlot, buf, len, &cgs->ps, conn))
	{
		return true;
	}

	return false;
}
