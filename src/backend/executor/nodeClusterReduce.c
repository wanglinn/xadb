
#include "postgres.h"

#include "executor/executor.h"
#include "executor/nodeClusterReduce.h"
#include "nodes/execnodes.h"
#include "pgxc/pgxc.h"
#include "reduce/adb_reduce.h"
#include "miscadmin.h"

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
	crstate->eof_underlying = false;
	crstate->eof_network = false;
	crstate->port = NULL;

	ExecInitResultTupleSlot(estate, &crstate->ps);

	outerPlan = outerPlan(node);
	outerPlanState(crstate) = ExecInitNode(outerPlan, estate, eflags);

	ExecAssignExprContext(estate, &crstate->ps);
	ExecAssignResultTypeFromTL(&crstate->ps);
	crstate->reduceState = ExecInitExpr(node->reduce, &crstate->ps);

	return crstate;
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
	/*
	 * First time to connect Reduce subprocess.
	 */
	if (port == NULL)
	{
		port = ConnectSelfReduce(TYPE_PLAN, PlanNodeID(node->ps.plan));
		if (IsRdcPortError(port))
			ereport(ERROR,
					(errmsg("fail to connect Reduce subprocess:%s",
					 RdcError(port))));
		node->port = port;
	}

	slot = node->ps.ps_ResultTupleSlot;
	{
		TupleTableSlot *outerslot;
		PlanState	   *outerNode;
		bool			outerValid;
		List		   *destOids = NIL;

		while (!node->eof_underlying || !node->eof_network)
		{
			/* fetch tuple from network */
			if (!node->eof_network)
			{
				ExecClearTuple(slot);
				if (node->eof_underlying)
					rdc_set_block(port);
				outerslot = GetSlotFromRemote(port, slot, &node->eof_network);
				if (!node->eof_network && !TupIsNull(outerslot))
					return outerslot;
			}

			/* fetch tuple from subnode */
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
								/* This tuple should be sent to remote nodes */
								destOids = lappend_oid(destOids, oid);

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
					node->eof_underlying = true;

					/* Here we send eof to remote plan nodes */
					SendSlotToRemote(port, destOids, outerslot);
				}
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
	ExecEndNode(outerPlanState(node));
}