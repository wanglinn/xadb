
#include "postgres.h"
#include "miscadmin.h"

#include "executor/executor.h"
#include "executor/nodeClusterReduce.h"
#include "lib/binaryheap.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "pgxc/pgxc.h"
#include "reduce/adb_reduce.h"
#include "utils/hsearch.h"

extern bool enable_cluster_plan;

typedef int32 SlotNumber;
static int32 cmr_heap_compare_slots(Datum a, Datum b, void *arg);
static TupleTableSlot *GetSlotFromOuterNode(ClusterReduceState *node);
static TupleTableSlot *GetSlotFromSpecialRemote(ClusterReduceState *node,
												ReduceEntry entry);
static TupleTableSlot *ExecClusterMergeReduce(ClusterReduceState *node);
static bool ExecConnectReduceWalker(PlanState *node, EState *estate);
static bool EndReduceStateWalker(PlanState *node, void *context);

ClusterReduceState *
ExecInitClusterReduce(ClusterReduce *node, EState *estate, int eflags)
{
	ClusterReduceState	   *crstate;
	TupleTableSlot		   *slot;
	Plan				   *outerPlan;
	List				   *nodesReduceTo;
	List				   *nodesReduceFrom;
	ListCell			   *lc;
	ReduceEntry				entry;
	int						i;
	HASHCTL					hctl;
	Oid						rdc_oid;

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

		hctl.keysize = sizeof(Oid);
		hctl.entrysize = sizeof(ReduceEntryData);
		hctl.hcxt = CurrentMemoryContext;
		crstate->rdc_htab = hash_create("reduce group",
										32,
										&hctl,	/* magic number here FIXME */
										HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

		crstate->rdc_elts = (ReduceEntry *) palloc0(sizeof(ReduceEntry) * crstate->nrdcs);
		i = 0;
		foreach (lc, nodesReduceFrom)
		{
			rdc_oid = lfirst_oid(lc);
			entry = hash_search(crstate->rdc_htab, &rdc_oid, HASH_ENTER, NULL);
			entry->re_eof = false;
			entry->re_slot = NULL;
			entry->re_store = NULL;
			crstate->rdc_elts[i] = entry;
			i++;
		}
	}

	/* Need ClusterReduce to merge sort */
	if (list_member_oid((const List *) nodesReduceTo, PGXCNodeOid))
	{
		if (node->numCols > 0)
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
				for (i = 0; i < crstate->nrdcs; i++)
				{
					entry = crstate->rdc_elts[i];
					slot = crstate->ps.ps_ResultTupleSlot;
					if (entry->re_key != PGXCNodeOid)
						entry->re_slot = MakeSingleTupleTableSlot(slot->tts_tupleDescriptor);
					entry->re_store = tuplestore_begin_heap(true, false, work_mem);
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
	ReduceEntry			entry;
	bool				found;
	Oid					eof_oid;

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
					found = false;
					entry = hash_search(node->rdc_htab, &eof_oid, HASH_FIND, &found);
					Assert(found && !entry->re_eof);
					entry->re_eof = true;
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
GetSlotFromSpecialRemote(ClusterReduceState *node, ReduceEntry entry)
{
	TupleTableSlot	   *outerslot;
	TupleTableSlot	   *cur_slot;
	Tuplestorestate	   *cur_store;
	RdcPort			   *port;
	ReduceEntry			othr_entry;
	Oid					cur_oid;
	Oid					slot_oid;
	Oid					eof_oid;
	bool				found;

	Assert(node && node->port && node->nkeys > 0);

	cur_oid = entry->re_key;
	cur_slot = entry->re_slot;
	cur_store = entry->re_store;

	/*
	 * try to get from its Tuplestorestate
	 */
	if (!tuplestore_ateof(cur_store))
	{
		if (tuplestore_gettupleslot(cur_store, true, true, cur_slot))
		{
#ifdef DEBUG_ADB
			elog(LOG, "got slot of %u from store", cur_oid);
#endif
			return cur_slot;
		}
	}

	/*
	 * already receive EOF message, so
	 * return NULL slot.
	 */
	if (entry->re_eof)
		return ExecClearTuple(cur_slot);

	port = node->port;
	while (!entry->re_eof)
	{
		ExecClearTuple(cur_slot);
		slot_oid = InvalidOid;
		eof_oid = InvalidOid;
		if (node->eof_underlying)
			rdc_set_block(port);
		else
			(void) rdc_try_read_some(port);
		outerslot = GetSlotFromRemote(port, cur_slot, &slot_oid, &eof_oid, &(node->closed_remote));
		if (OidIsValid(eof_oid))
		{
			node->neofs++;
			node->eof_network = (node->neofs == node->nrdcs - 1);
			if (eof_oid == cur_oid)
			{
				entry->re_eof = true;
				return ExecClearTuple(cur_slot);
			} else
			{
				found = false;
				othr_entry = hash_search(node->rdc_htab, &eof_oid, HASH_FIND, &found);
				Assert(found && !othr_entry->re_eof);
				othr_entry->re_eof = true;
			}
		} else if (!TupIsNull(outerslot))
		{
			Assert(OidIsValid(slot_oid));
			if (slot_oid == cur_oid)
				return outerslot;

#ifdef DEBUG_ADB
			elog(LOG, "put slot from %u into store", slot_oid);
#endif
			found = false;
			othr_entry = hash_search(node->rdc_htab, &slot_oid, HASH_FIND, &found);
			Assert(found && !othr_entry->re_eof);
			cur_store = othr_entry->re_store;
			if (tuplestore_ateof(cur_store))
				tuplestore_clear(cur_store);
			tuplestore_puttupleslot(cur_store, outerslot);
		}
	}

	return ExecClearTuple(cur_slot);
}

static TupleTableSlot *
ExecClusterMergeReduce(ClusterReduceState *node)
{
	TupleTableSlot	   *result;
	ReduceEntry			entry;
	bool				found;
	int					i;

	Assert(node && node->nkeys > 0);
	if (!node->initialized)
	{
		/* initialize local slot */
		found = false;
		entry = hash_search(node->rdc_htab, &PGXCNodeOid, HASH_FIND, &found);
		Assert(found && !entry->re_eof);
		entry->re_slot = GetSlotFromOuterNode(node);
		if (!TupIsNull(entry->re_slot))
			binaryheap_add_unordered(node->binheap, PointerGetDatum(entry));

		/* iniialize remote slot */
		for (i = 0; i < node->nrdcs; i++)
		{
			entry = node->rdc_elts[i];
			if (entry->re_key == PGXCNodeOid)
				continue;
			entry->re_slot = GetSlotFromSpecialRemote(node, entry);
			if (!TupIsNull(entry->re_slot))
				binaryheap_add_unordered(node->binheap, PointerGetDatum(entry));
		}
		binaryheap_build(node->binheap);
		node->initialized = true;
	} else
	{
		entry = (ReduceEntry) DatumGetPointer(binaryheap_first(node->binheap));
		if (entry->re_key == PGXCNodeOid)
			entry->re_slot = GetSlotFromOuterNode(node);
		else
			entry->re_slot = GetSlotFromSpecialRemote(node, entry);

		if (!TupIsNull(entry->re_slot))
			binaryheap_replace_first(node->binheap, PointerGetDatum(entry));
		else
			(void) binaryheap_remove_first(node->binheap);
	}

	if (binaryheap_empty(node->binheap))
	{
		result = ExecClearTuple(node->ps.ps_ResultTupleSlot);
	} else
	{
		entry = (ReduceEntry) DatumGetPointer(binaryheap_first(node->binheap));
		result = entry->re_slot;
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
	ReduceEntry			re1 = (ReduceEntry) PointerGetDatum(a);
	ReduceEntry			re2 = (ReduceEntry) PointerGetDatum(b);
	TupleTableSlot	   *s1 = re1->re_slot;
	TupleTableSlot	   *s2 = re2->re_slot;
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
	if (node->rdc_elts)
	{
		int i;
		for (i = 0; i < node->nrdcs; i++)
		{
			if (node->rdc_elts[i]->re_store)
				tuplestore_end(node->rdc_elts[i]->re_store);
			node->rdc_elts[i]->re_store = NULL;
		}
		pfree(node->rdc_elts);
	}
	if (node->rdc_htab)
		hash_destroy(node->rdc_htab);

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
