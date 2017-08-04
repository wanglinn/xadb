
#include "postgres.h"

#include "executor/executor.h"
#include "executor/nodeClusterReduce.h"
#include "lib/binaryheap.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "pgxc/pgxc.h"
#include "reduce/adb_reduce.h"
#include "miscadmin.h"

extern bool enable_cluster_plan;

typedef int32 SlotNumber;
static int32 cmr_heap_compare_slots(Datum a, Datum b, void *arg);
static int Oid2NodeIndex(ClusterReduceState *node, Oid noid);
static TupleTableSlot *GetSlotFromOuterNode(ClusterReduceState *node);
static TupleTableSlot *GetSlotFromSpecialRemote(ClusterReduceState *node,
												Oid remote_oid,
												TupleTableSlot *slot);
static TupleTableSlot *ExecClusterMergeReduce(ClusterReduceState *node);
static bool ExecConnectReduceWalker(PlanState *node, EState *estate);
static bool EndReduceStateWalker(PlanState *node, void *context);

ClusterReduceState *
ExecInitClusterReduce(ClusterReduce *node, EState *estate, int eflags)
{
	ClusterReduceState	   *crstate;
	Plan				   *outerPlan;
	List				   *nodesReduceTo;
	List				   *nodesReduceFrom;
	ListCell			   *lc;
	int						i;

	Assert(outerPlan(node) != NULL);
	Assert(innerPlan(node) == NULL);
	Assert((eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)) == 0);

	nodesReduceTo = GetReducePathExprNodes(node->reduce);

	/*
	 * create state structure
	 */
	crstate = makeNode(ClusterReduceState);
	crstate->ps.plan = (Plan*)node;
	crstate->ps.state = estate;
	crstate->port = NULL;
	crstate->closed_remote = NIL;
	crstate->eof_underlying = false;
	crstate->eof_network = false;

	ExecInitResultTupleSlot(estate, &crstate->ps);
	ExecAssignExprContext(estate, &crstate->ps);
	ExecAssignResultTypeFromTL(&crstate->ps);

	/*
	 * This time don't connect Reduce subprocess.
	 */
	if (!(eflags & (EXEC_FLAG_EXPLAIN_ONLY | EXEC_FLAG_IN_SUBPLAN)))
	{
		crstate->port = ConnectSelfReduce(TYPE_PLAN, PlanNodeID(&(node->plan)));
		if (IsRdcPortError(crstate->port))
			ereport(ERROR,
					(errmsg("fail to connect self reduce subprocess"),
					 errdetail("%s", RdcError(crstate->port))));
		RdcFlags(crstate->port) = RDC_FLAG_VALID;

		nodesReduceFrom = GetReduceGroup();
		crstate->nrdcs = list_length(nodesReduceFrom);
		crstate->neofs = 0;
		crstate->rdc_oids = (Oid *) palloc0(sizeof(Oid) * crstate->nrdcs);
		crstate->rdc_eofs = (Oid *) palloc0(sizeof(Oid) * crstate->nrdcs);
		i = 0;
		foreach (lc, nodesReduceFrom)
			crstate->rdc_oids[i++] = lfirst_oid(lc);
	}

	/* Need ClusterReduce to merge sort */
	if (node->numCols > 0)
	{
		if (list_member_oid((const List *) nodesReduceTo, PGXCNodeOid))
		{
			crstate->nkeys = node->numCols;
			crstate->sortkeys = palloc0(sizeof(SortSupportData) * node->numCols);
			for (i = 0; i < node->numCols; i++)
			{
				SortSupport sortKey = crstate->sortkeys + i;

				sortKey->ssup_cxt = CurrentMemoryContext;
				sortKey->ssup_collation = node->collations[i];
				sortKey->ssup_nulls_first = node->nullsFirst[i];
				sortKey->ssup_attno = node->sortColIdx[i];

				/*
				 * It isn't feasible to perform abbreviated key conversion, since
				 * tuples are pulled into mergestate's binary heap as needed.  It
				 * would likely be counter-productive to convert tuples into an
				 * abbreviated representation as they're pulled up, so opt out of that
				 * additional optimization entirely.
				 */
				sortKey->abbreviate = false;

				PrepareSortSupportFromOrderingOp(node->sortOperators[i], sortKey);
			}

			if (!(eflags & EXEC_FLAG_EXPLAIN_ONLY))
			{
				crstate->slots = (TupleTableSlot **) palloc0(sizeof(TupleTableSlot *) * crstate->nrdcs);
				crstate->stores = (Tuplestorestate **) palloc0(sizeof(Tuplestorestate *) * crstate->nrdcs);
				for (i = 0; i < crstate->nrdcs; i++)
				{
					crstate->slots[i] = MakeSingleTupleTableSlot(crstate->ps.ps_ResultTupleSlot->tts_tupleDescriptor);
					crstate->stores[i] = tuplestore_begin_heap(true, false, work_mem);
				}
				crstate->binheap = binaryheap_allocate(crstate->nrdcs, cmr_heap_compare_slots, crstate);
			}
			crstate->initialized = false;
		}
	}

	outerPlan = outerPlan(node);
	outerPlanState(crstate) = ExecInitNode(outerPlan, estate, eflags);

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

static int
Oid2NodeIndex(ClusterReduceState *node, Oid noid)
{
	int idx;

	for (idx = 0; idx < node->nrdcs; idx++)
	{
		if ((node->rdc_oids)[idx] == noid)
			return idx;
	}
	Assert(false);
	return -1;
}

static TupleTableSlot *
GetSlotFromOuterNode(ClusterReduceState *node)
{
	TupleTableSlot	   *slot;
	ExprContext		   *econtext;
	RdcPort			   *port;
	ExprDoneCond		done;
	bool				isNull;
	Oid					oid;
	TupleTableSlot	   *outerslot;
	PlanState		   *outerNode;
	bool				outerValid;
	List			   *destOids = NIL;

	Assert(node && node->port);
	port = node->port;
	slot = node->ps.ps_ResultTupleSlot;
	while (!node->eof_underlying)
	{
		outerValid = false;
		outerNode = outerPlanState(node);
		outerslot = ExecProcNode(outerNode);
		if (!TupIsNull(outerslot))
		{
			econtext = node->ps.ps_ExprContext;
			econtext->ecxt_outertuple = outerslot;
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
			list_free(destOids);
			destOids = NIL;

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

	return ExecClearTuple(slot);
}

TupleTableSlot *
ExecClusterReduce(ClusterReduceState *node)
{
	TupleTableSlot	   *slot;
	RdcPort			   *port;
	Oid					eof_oid;
	int					nidx;

	/* ClusterReduce need to sort by keys */
	if (node->nkeys > 0)
		return ExecClusterMergeReduce(node);

	port = node->port;
	node->started = true;
	Assert(port);

	slot = node->ps.ps_ResultTupleSlot;
	{
		TupleTableSlot *outerslot;

		while (!node->eof_underlying || !node->eof_network)
		{
			/* fetch tuple from outer node */
			if (!node->eof_underlying)
			{
				outerslot = GetSlotFromOuterNode(node);
				if (!TupIsNull(outerslot))
					return outerslot;
			}

			/* fetch tuple from network */
			if (!node->eof_network)
			{
				ExecClearTuple(slot);
				eof_oid = InvalidOid;
				if (node->eof_underlying)
					rdc_set_block(port);
				else
					(void) rdc_try_read_some(port);
				outerslot = GetSlotFromRemote(port, slot, NULL, &eof_oid, &node->closed_remote);
				if (OidIsValid(eof_oid))
				{
					nidx = Oid2NodeIndex(node, eof_oid);
					/*
					 * There will not be two EOF messages
					 * of the same reduce.
					 */
					Assert(node->rdc_eofs[nidx] == InvalidOid);
					node->rdc_eofs[nidx] = eof_oid;
					node->neofs++;
					node->eof_network = (node->neofs == node->nrdcs - 1);
				} else if (!TupIsNull(outerslot))
					return outerslot;
			}
		}
	}

	/*
	 * Nothing left ...
	 */
	return ExecClearTuple(slot);
}

static TupleTableSlot *
GetSlotFromSpecialRemote(ClusterReduceState *node, Oid remote_oid, TupleTableSlot *slot)
{
	TupleTableSlot	   *outerslot;
	Tuplestorestate	   *tuplestorestate;
	RdcPort			   *port;
	Oid					slot_oid;
	Oid					eof_oid;
	int					ridx;
	int					nidx;

	Assert(node && node->port && node->nkeys > 0);
	Assert(OidIsValid(remote_oid));

	ridx = Oid2NodeIndex(node, remote_oid);

	/*
	 * try to get from its Tuplestorestate
	 */
	tuplestorestate = node->stores[ridx];
	Assert(tuplestorestate);
	if (!tuplestore_ateof(tuplestorestate))
	{
		if (tuplestore_gettupleslot(tuplestorestate, true, true, slot))
		{
#ifdef DEBUG_ADB
			elog(LOG, "got slot of %u from store", remote_oid);
#endif
			return slot;
		}
	}

	/*
	 * already receive EOF message, so
	 * return NULL slot.
	 */
	if (node->rdc_eofs[ridx] == remote_oid)
		return ExecClearTuple(slot);
	Assert(node->rdc_eofs[ridx] == InvalidOid);

	port = node->port;
	while (node->rdc_eofs[ridx] == InvalidOid)
	{
		ExecClearTuple(slot);
		slot_oid = InvalidOid;
		eof_oid = InvalidOid;
		if (node->eof_underlying)
			rdc_set_block(port);
		else
			(void) rdc_try_read_some(port);
		outerslot = GetSlotFromRemote(port, slot, &slot_oid, &eof_oid, &(node->closed_remote));
		if (OidIsValid(eof_oid))
		{
			nidx = Oid2NodeIndex(node, eof_oid);
			/*
			 * There will not be two EOF messages
			 * of the same reduce.
			 */
			Assert(node->rdc_eofs[nidx] == InvalidOid);
			node->rdc_eofs[nidx] = eof_oid;
			node->neofs++;
			node->eof_network = (node->neofs == node->nrdcs - 1);
			if (eof_oid == remote_oid)
				return ExecClearTuple(slot);
		} else if (!TupIsNull(outerslot))
		{
			Assert(OidIsValid(slot_oid));
			if (slot_oid == remote_oid)
				return outerslot;

#ifdef DEBUG_ADB
			elog(LOG, "put slot from %u into store", slot_oid);
#endif
			nidx = Oid2NodeIndex(node, slot_oid);
			tuplestorestate = node->stores[nidx];
			Assert(tuplestorestate);
			if (tuplestore_ateof(tuplestorestate))
				tuplestore_clear(tuplestorestate);
			tuplestore_puttupleslot(tuplestorestate, outerslot);
		}
	}

	return ExecClearTuple(slot);
}

static TupleTableSlot *
ExecClusterMergeReduce(ClusterReduceState *node)
{
	TupleTableSlot	   *result;
	SlotNumber			i;
	Oid					oid;
	int					nidx;

	Assert(node && node->nkeys > 0);
	if (!node->initialized)
	{
		/* initialize local slot */
		nidx = Oid2NodeIndex(node, PGXCNodeOid);
		node->slots[nidx] = GetSlotFromOuterNode(node);
		if (!TupIsNull(node->slots[nidx]))
			binaryheap_add_unordered(node->binheap, Int32GetDatum(nidx));

		/* iniialize remote slot */
		for (i = 0; i < node->nrdcs; i++)
		{
			oid = node->rdc_oids[i];
			if (oid == PGXCNodeOid)
				continue;
			node->slots[i] = GetSlotFromSpecialRemote(node, oid, node->slots[i]);
			if (!TupIsNull(node->slots[i]))
				binaryheap_add_unordered(node->binheap, Int32GetDatum(i));
		}
		binaryheap_build(node->binheap);
		node->initialized = true;
	} else
	{
		i = DatumGetInt32(binaryheap_first(node->binheap));
		oid = node->rdc_oids[i];
		if (oid == PGXCNodeOid)
			node->slots[i] = GetSlotFromOuterNode(node);
		else
			node->slots[i] = GetSlotFromSpecialRemote(node, oid, node->slots[i]);

		if (!TupIsNull(node->slots[i]))
			binaryheap_replace_first(node->binheap, Int32GetDatum(i));
		else
			(void) binaryheap_remove_first(node->binheap);
	}

	if (binaryheap_empty(node->binheap))
	{
		result = ExecClearTuple(node->ps.ps_ResultTupleSlot);
	} else
	{
		i = DatumGetInt32(binaryheap_first(node->binheap));
		result = node->slots[i];
	}

	return result;
}

/*
 * Compare the tuples in the two given slots.
 */
static int32
cmr_heap_compare_slots(Datum a, Datum b, void *arg)
{
	ClusterReduceState *node = (ClusterReduceState *) arg;
	SlotNumber	slot1 = DatumGetInt32(a);
	SlotNumber	slot2 = DatumGetInt32(b);

	TupleTableSlot *s1 = node->slots[slot1];
	TupleTableSlot *s2 = node->slots[slot2];
	int			nkey;

	Assert(!TupIsNull(s1));
	Assert(!TupIsNull(s2));

	for (nkey = 0; nkey < node->nkeys; nkey++)
	{
		SortSupport sortKey = node->sortkeys + nkey;
		AttrNumber	attno = sortKey->ssup_attno;
		Datum		datum1,
					datum2;
		bool		isNull1,
					isNull2;
		int			compare;

		datum1 = slot_getattr(s1, attno, &isNull1);
		datum2 = slot_getattr(s2, attno, &isNull2);

		compare = ApplySortComparator(datum1, isNull1,
									  datum2, isNull2,
									  sortKey);
		if (compare != 0)
			return -compare;
	}
	return 0;
}

void ExecEndClusterReduce(ClusterReduceState *node)
{
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
	safe_pfree(node->rdc_oids);
	safe_pfree(node->rdc_eofs);
	node->rdc_oids = NULL;
	node->rdc_eofs = NULL;
	if (node->stores)
	{
		int i;
		for (i = 0; i < node->nrdcs; i++)
		{
			Assert(node->stores[i]);
			tuplestore_end(node->stores[i]);
			node->stores[i] = NULL;
		}
	}

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
ExecConnectReduceWalker(PlanState *node, EState *estate)
{
	if(node == NULL)
		return false;

	if (IsA(node, ClusterReduceState))
	{
		ClusterReduceState *crstate = (ClusterReduceState *) node;
		if (crstate->port == NULL)
		{
			crstate->port = ConnectSelfReduce(TYPE_PLAN, PlanNodeID(crstate->ps.plan));
			if (IsRdcPortError(crstate->port))
				ereport(ERROR,
						(errmsg("fail to connect self reduce subprocess"),
						 errdetail("%s", RdcError(crstate->port))));
			RdcFlags(crstate->port) = RDC_FLAG_VALID;
		}
	}

	return planstate_tree_walker(node, ExecConnectReduceWalker, estate);
}

void
ExecConnectReduce(PlanState *node)
{
	Assert((node->state->es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY) == 0);
	ExecConnectReduceWalker(node, node->state);
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
