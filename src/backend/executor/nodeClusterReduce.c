
#include "postgres.h"

#include "executor/executor.h"
#include "executor/nodeClusterReduce.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "pgxc/pgxc.h"
#include "reduce/adb_reduce.h"
#include "miscadmin.h"

extern bool enable_cluster_plan;

static bool ExecConnectReduceWalker(ClusterReduceState *node, EState *estate);
static bool EndReduceStateWalker(PlanState *crstate, void *context);

ClusterReduceState *
ExecInitClusterReduce(ClusterReduce *node, EState *estate, int eflags)
{
	ClusterReduceState	   *crstate;
	Plan				   *outerPlan;

	Assert(outerPlan(node) != NULL);
	Assert(innerPlan(node) == NULL);
	Assert((eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)) == 0);

	/*
	 * create state structure
	 */
	crstate = makeNode(ClusterReduceState);
	crstate->ps.plan = (Plan*)node;
	crstate->ps.state = estate;
	crstate->closed_remote = NIL;
	crstate->eof_underlying = false;
	crstate->eof_network = false;
	crstate->port = NULL;

	/*
	 * This time don't connect Reduce subprocess.
	 */
	if ((eflags & (EXEC_FLAG_EXPLAIN_ONLY|EXEC_FLAG_IN_SUBPLAN)) == 0)
	{
		crstate->port = ConnectSelfReduce(TYPE_PLAN, PlanNodeID(&(node->plan)));
		if (IsRdcPortError(crstate->port))
			ereport(ERROR,
					(errmsg("fail to connect self reduce subprocess"),
					 errdetail("%s", RdcError(crstate->port))));
		RdcFlags(crstate->port) = RDC_FLAG_VALID;
	}

	ExecInitResultTupleSlot(estate, &crstate->ps);

	outerPlan = outerPlan(node);
	outerPlanState(crstate) = ExecInitNode(outerPlan, estate, eflags);

	ExecAssignExprContext(estate, &crstate->ps);
	ExecAssignResultTypeFromTL(&crstate->ps);
	if(node->special_node == PGXCNodeOid)
	{
		Assert(OidIsValid(PGXCNodeOid) && node->special_reduce != NULL);
		crstate->reduceState = ExecInitExpr(node->special_reduce, &crstate->ps);
	}else
	{
		crstate->reduceState = ExecInitExpr(node->reduce, &crstate->ps);
	}

	return crstate;
}

static bool ExecConnectReduceWalker(ClusterReduceState *crstate, EState *estate)
{
	if(crstate == NULL)
		return false;

	if (IsA(crstate, ClusterReduceState) &&
		crstate->port == NULL)
	{
		crstate->port = ConnectSelfReduce(TYPE_PLAN, PlanNodeID(crstate->ps.plan));
		if (IsRdcPortError(crstate->port))
			ereport(ERROR,
					(errmsg("fail to connect self reduce subprocess"),
					 errdetail("%s", RdcError(crstate->port))));
		RdcFlags(crstate->port) = RDC_FLAG_VALID;
	}
	return planstate_tree_walker((PlanState*)crstate, ExecConnectReduceWalker, estate);
}

void ExecConnectReduce(PlanState *node)
{
	Assert((node->state->es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY) == 0);
	ExecConnectReduceWalker((ClusterReduceState*)node, node->state);
}

TupleTableSlot *
ExecClusterReduce(ClusterReduceState *node)
{
	TupleTableSlot	   *slot;
	ExprContext		   *econtext;
	RdcPort			   *port;
	ExprDoneCond		done;
	bool				isNull;
	Oid					oid;

	port = node->port;
	node->started = true;
	Assert(port);

	slot = node->ps.ps_ResultTupleSlot;
	{
		TupleTableSlot *outerslot;
		PlanState	   *outerNode;
		bool			outerValid;
		List		   *destOids = NIL;

		while (!node->eof_underlying || !node->eof_network)
		{
			/* fetch tuple from outer node */
			if (!node->eof_underlying)
			{
				outerValid = false;
				outerNode = outerPlanState(node);
				outerslot = ExecProcNode(outerNode);
				if (!TupIsNull(outerslot))
				{
					econtext = node->ps.ps_ExprContext;
					econtext->ecxt_outertuple = outerslot;
					if (destOids)
						list_free(destOids);
					destOids = NIL;
					for(;;)
					{
						Datum datum;
						datum = ExecEvalExpr(node->reduceState, econtext, &isNull, &done);
						if(isNull)
						{
							Assert(0);
						}else if(done == ExprEndResult)
						{
							break;
						}else
						{
							oid = DatumGetObjectId(datum);
							if(oid == PGXCNodeOid)
								outerValid = true;
							else
							{
								/* This tuple should be sent to remote nodes */
								if (!list_member_oid(node->closed_remote, oid))
									destOids = lappend_oid(destOids, oid);
							}

							if(done == ExprSingleResult)
								break;
						}
					}

					/* Here we truly send tuple to remote plan nodes */
					SendSlotToRemote(port, destOids, outerslot);

					if (outerValid)
						return outerslot;

					ExecClearTuple(outerslot);
				} else
				{
					/* Here we send eof to remote plan nodes */
					SendEofToRemote(port);

					node->eof_underlying = true;
				}
			}

			/* fetch tuple from network */
			if (!node->eof_network)
			{
				ExecClearTuple(slot);
				if (node->eof_underlying)
					rdc_set_block(port);
				outerslot = GetSlotFromRemote(port, slot, &node->eof_network, &node->closed_remote);
				if (!node->eof_network && !TupIsNull(outerslot))
					return outerslot;
			}
		}
	}

	/*
	 * Nothing left ...
	 */
	return ExecClearTuple(slot);
}

void ExecEndClusterReduce(ClusterReduceState *node)
{
	Assert(node);

	/*
	 * if either of these(node->eof_underlying and node->eof_network)
	 * is false, it means local backend doesn't fetch all tuple (include
	 * tuple from other backend and from the outer node).
	 *
	 * Here we should tell other backend that the local cluster reduce
	 * will be closed and no more data is needed.
	 *
	 * If we have already sent EOF message of current plan node, it is
	 * no need to broadcast CLOSE message to other reduce.
	 */
	if (node->port && !RdcSendCLOSE(node->port))
		SendPlanCloseToSelfReduce(node->port, !RdcSendEOF(node->port));
	rdc_freeport(node->port);
	node->port = NULL;
	node->eof_network = false;
	node->eof_underlying = false;
	list_free(node->closed_remote);
	node->closed_remote = NIL;

	ExecEndNode(outerPlanState(node));
}

void ExecReScanClusterReduce(ClusterReduceState *node)
{
	if(node->started)
	{
		ereport(ERROR, (errmsg("rescan cluster reduce no support")));
	}
}

static bool
EndReduceStateWalker(PlanState *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, ClusterReduceState))
	{
		ClusterReduceState *crs = (ClusterReduceState *) node;
		Assert(crs->port);

		/*
		 * Drive all ClusterReduce to send slot, discard slot
		 * used for local.
		 */
		while (!crs->eof_network || !crs->eof_underlying)
		{
			(void) ExecProcNode(node);
		}

		return false;
	}

	return planstate_tree_walker(node, EndReduceStateWalker, context);
}

void
ExecEndAllReduceState(PlanState *node)
{
	if (!enable_cluster_plan || !IsUnderPostmaster)
		return ;

	elog(LOG,
		 "Top-down drive cluster reduce to send EOF message");
	(void) EndReduceStateWalker(node, NULL);
}
