
#include "postgres.h"

#include "executor/executor.h"
#include "executor/nodeClusterReduce.h"
#include "nodes/execnodes.h"
#include "pgxc/pgxc.h"
#include "reduce/adb_reduce.h"
#include "miscadmin.h"

static void SendTupleToRemote(EState *estate, List *destOids);

ClusterReduceState *
ExecInitClusterReduce(ClusterReduce *node, EState *estate, int eflags)
{
	ClusterReduceState	   *crstate;
	Plan				   *outerPlan;

	Assert(outerPlan(node) != NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create state structure
	 */
	crstate = makeNode(ClusterReduceState);
	crstate->ps.plan = (Plan*)node;
	crstate->ps.state = estate;

	/*
	 * We must have a tuplestore buffering the subplan output to do backward
	 * scan or mark/restore.  We also prefer to materialize the subplan output
	 * if we might be called on to rewind and replay it many times. However,
	 * if none of these cases apply, we can skip storing the data.
	 */
	crstate->eflags = (eflags & (EXEC_FLAG_REWIND |
								 EXEC_FLAG_BACKWARD |
								 EXEC_FLAG_MARK));

	/*
	 * Tuplestore's interpretation of the flag bits is subtly different from
	 * the general executor meaning: it doesn't think BACKWARD necessarily
	 * means "backwards all the way to start".  If told to support BACKWARD we
	 * must include REWIND in the tuplestore eflags, else tuplestore_trim
	 * might throw away too much.
	 */
	if (eflags & EXEC_FLAG_BACKWARD)
		crstate->eflags |= EXEC_FLAG_REWIND;

	crstate->eof_underlying = false;
	crstate->eof_network = false;
	crstate->tuplestorestate = NULL;
	crstate->port = NULL;

	ExecInitResultTupleSlot(estate, &crstate->ps);

	/*
	 * initialize child nodes
	 *
	 * We shield the child node from the need to support REWIND, BACKWARD, or
	 * MARK/RESTORE.
	 */
	eflags &= ~(EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK);

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
	EState			   *estate;
	ScanDirection		dir;
	bool				forward;
	Tuplestorestate	   *tuplestorestate;
	RdcPort			   *port;
	bool				eof_tuplestore;
	bool				eof_network;
	ExprDoneCond		done;
	bool				isNull;
	Oid					oid;
	List			   *destOids = NIL;

	/*
	 * get state info from node
	 */
	estate = node->ps.state;
	dir = estate->es_direction;
	forward = ScanDirectionIsForward(dir);
	tuplestorestate = node->tuplestorestate;
	port = node->port;

	/*
	 * If first time through, and we need a tuplestore, initialize it.
	 */
	if (tuplestorestate == NULL && node->eflags != 0)
	{
		tuplestorestate = tuplestore_begin_heap(true, false, work_mem);
		tuplestore_set_eflags(tuplestorestate, node->eflags);
		if (node->eflags & EXEC_FLAG_MARK)
		{
			/*
			 * Allocate a second read pointer to serve as the mark. We know it
			 * must have index 1, so needn't store that.
			 */
			int ptrno	PG_USED_FOR_ASSERTS_ONLY;

			ptrno = tuplestore_alloc_read_pointer(tuplestorestate,
												  node->eflags);
			Assert(ptrno == 1);
		}
		node->tuplestorestate = tuplestorestate;
	}

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
				Assert(0);
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
					/* This tuple should be sent to remote nodes */
					destOids = lappend_oid(destOids, oid);
				}
				if(done == ExprSingleResult)
					break;
			}
		}

		/* Here we truly send to remote plan nodes */
		SendTupleToRemote(estate, destOids);
	}

	return node->ps.ps_ResultTupleSlot;
}

void ExecEndClusterReduce(ClusterReduceState *node)
{
	ExecEndNode(outerPlanState(node));
}

static void
SendTupleToRemote(EState *estate, List *destOids)
{
	/* TODO: */
}