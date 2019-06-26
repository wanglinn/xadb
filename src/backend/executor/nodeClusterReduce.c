
#include "postgres.h"
#include "miscadmin.h"

#include "access/tuptypeconvert.h"
#include "executor/executor.h"
#include "executor/nodeClusterReduce.h"
#include "executor/nodeCtescan.h"
#include "executor/nodeMaterial.h"
#include "executor/tuptable.h"
#include "lib/binaryheap.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "pgxc/pgxc.h"
#include "reduce/adb_reduce.h"
#include "utils/hsearch.h"

extern bool enable_cluster_plan;
extern bool print_reduce_debug_log;

#define PlanGetTargetNodes(plan)	\
	((ClusterReduce *) (plan))->reduce_oids

#define PlanStateGetTargetNodes(state) \
	PlanGetTargetNodes(((ClusterReduceState *) (state))->ps.plan)

static void ExecInitClusterReduceStateExtra(ClusterReduceState *crstate);
static void PrepareForReScanClusterReduce(ClusterReduceState *node);
static bool ExecConnectReduceWalker(PlanState *node, EState *estate);
static int32 cmr_heap_compare_slots(Datum a, Datum b, void *arg);
static TupleTableSlot *ExecClusterReduce(PlanState *pstate);
static TupleTableSlot *GetSlotFromOuter(ClusterReduceState *node);
static TupleTableSlot *GetMergeSlotFromOuter(ClusterReduceState *node, ReduceEntry entry);
static TupleTableSlot *GetMergeSlotFromRemote(ClusterReduceState *node, ReduceEntry entry);
static TupleTableSlot *ExecClusterMergeReduce(ClusterReduceState *node);
static void ClusterReducePortCleanupCallback(void *arg);
static void ExecDisconnectClusterReduce(ClusterReduceState *node, bool noerror);
static bool DriveClusterReduceState(ClusterReduceState *node);
static bool DriveCteScanState(PlanState *node);
static bool DriveMaterialState(PlanState *node);
static bool DriveClusterReduceWalker(PlanState *node);
static bool IsThereClusterReduce(PlanState *node);

static void
ExecInitClusterReduceStateExtra(ClusterReduceState *crstate)
{
	ClusterReduce  *plan;
	List		   *nodesReduceFrom;
	ListCell	   *lc;
	ReduceEntry		entry;
	int				i;
	HASHCTL			hctl;
	Oid				rdc_oid;
	bool			is_tgt_node;

	AssertArg(crstate);
	AssertArg(crstate->port == NULL);
	plan = (ClusterReduce *) crstate->ps.plan;

	crstate->port = ConnectSelfReduce(TYPE_PLAN, PlanNodeID(plan), MyProcPid, NULL);
	if (IsRdcPortError(crstate->port))
		ereport(ERROR,
				(errmsg("[PLAN %d] fail to connect self reduce subprocess", PlanNodeID(plan)),
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

	crstate->rdc_entrys = (ReduceEntry *) palloc0(sizeof(ReduceEntry) * crstate->nrdcs);
	for (lc = list_head(nodesReduceFrom), i = 0; lc != NULL; lc = lnext(lc), i++)
	{
		rdc_oid = lfirst_oid(lc);
		entry = hash_search(crstate->rdc_htab, &rdc_oid, HASH_ENTER, NULL);
		entry->re_eof = false;
		entry->re_slot = NULL;
		entry->re_store = NULL;
		crstate->rdc_entrys[i] = entry;
	}

	is_tgt_node = list_member_oid(PlanGetTargetNodes(plan), PGXCNodeOid);
	if (plan->numCols > 0 && is_tgt_node)
	{
		TupleTableSlot *slot = crstate->ps.ps_ResultTupleSlot;
		for (i = 0; i < crstate->nrdcs; i++)
		{
			entry = crstate->rdc_entrys[i];
			entry->re_slot = MakeSingleTupleTableSlot(slot->tts_tupleDescriptor);
			entry->re_store = tuplestore_begin_heap(true, false, work_mem);
		}
		crstate->binheap = binaryheap_allocate(crstate->nrdcs, cmr_heap_compare_slots, crstate);
		crstate->initialized = false;
	}
}

ClusterReduceState *
ExecInitClusterReduce(ClusterReduce *node, EState *estate, int eflags)
{
	ClusterReduceState *crstate;
	Plan			   *outerPlan;
	Expr			   *expr;
	TupleDesc			tupDesc;

	Assert(outerPlan(node) != NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create state structure
	 */
	crstate = makeNode(ClusterReduceState);
	crstate->ps.plan = (Plan*)node;
	crstate->ps.state = estate;
	crstate->ps.ExecProcNode = ExecClusterReduce;

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

	crstate->port = NULL;
	crstate->closed_remote = NIL;
	crstate->eof_underlying = false;
	crstate->eof_network = false;
	crstate->started = false;
	crstate->tuplestorestate = NULL;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &crstate->ps);

	Assert(OidIsValid(PGXCNodeOid));

	/* Need ClusterReduce to merge sort */
	if (list_member_oid(PlanGetTargetNodes(node), PGXCNodeOid))
	{
		if (node->numCols > 0)
		{
			int i;
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
		}
	} else
		crstate->eof_network = true;

	/*
	 * Initialize result slot, type and projection.
	 */
	ExecInitResultTupleSlotTL(estate, &crstate->ps);

	/*
	 * Do not connect Reduce subprocess at this time.
	 */
	if (!(eflags & (EXEC_FLAG_EXPLAIN_ONLY | EXEC_FLAG_IN_SUBPLAN)))
		ExecInitClusterReduceStateExtra(crstate);

	/*
	 * initialize child nodes
	 *
	 * We shield the child node from the need to support REWIND, BACKWARD, or
	 * MARK/RESTORE.
	 */
	eflags &= ~(EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK);

	outerPlan = outerPlan(node);
	outerPlanState(crstate) = ExecInitNode(outerPlan, estate, eflags);
	tupDesc = ExecGetResultType(outerPlanState(crstate));

	/* init reduce expr */
	if(node->special_node == PGXCNodeOid)
	{
		Assert(node->special_reduce != NULL);
		expr = node->special_reduce;
	}else
	{
		expr = node->reduce;
	}
	Assert(expr != NULL);
	crstate->reduceState = ExecInitReduceExpr(expr);

	estate->es_reduce_plan_inited = true;

	crstate->convert = create_type_convert(crstate->ps.ps_ResultTupleSlot->tts_tupleDescriptor, true, true);
	if(crstate->convert)
		crstate->convert_slot = MakeSingleTupleTableSlot(crstate->convert->out_desc);

	RegisterReduceCleanup(ClusterReducePortCleanupCallback, crstate);

	return crstate;
}

static TupleTableSlot *
GetSlotFromOuter(ClusterReduceState *node)
{
	TupleTableSlot *slot;
	ExprContext	   *econtext;
	RdcPort		   *port;
	ExprDoneCond	done;
	bool			isNull;
	Oid				oid;
	TupleTableSlot *outerslot;
	PlanState	   *outerNode;
	bool			outerValid;
	List		   *destOids = NIL;

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
				Datum datum = ExecEvalReduceExpr(node->reduceState, econtext, &isNull, &done);
				if(done == ExprEndResult)
				{
					break;
				}else if(isNull)
				{
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("reduce expression for plan %d returned a null value", node->ps.plan->plan_node_id)));
				}else
				{
					oid = DatumGetObjectId(datum);
					if(oid == PGXCNodeOid)
					{
						outerValid = true;
					}else if(!OidIsValid(oid))
					{
						ereport(ERROR,
								(errcode(ERRCODE_INTERNAL_ERROR),
								 errmsg("reduce expression for plan %d returned an invalid OID", node->ps.plan->plan_node_id)));
					}else
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
			if(destOids != NIL)
			{
				if(node->convert)
				{
					do_type_convert_slot_out(node->convert,
											 outerslot,
											 node->convert_slot,
											 false);
					SendSlotToRemote(port, destOids, node->convert_slot);
				}else
				{
					SendSlotToRemote(port, destOids, outerslot);
				}
				list_free(destOids);
				destOids = NIL;
			}

			if (outerValid)
				return outerslot;
		} else
		{
			/* Here we send eof to remote plan nodes */
			SendEofToRemote(port, PlanStateGetTargetNodes(node));

			node->eof_underlying = true;
		}
	}

	return ExecClearTuple(slot);
}

TupleTableSlot *
ExecClusterReduce(PlanState *pstate)
{
	ClusterReduceState *node = castNode(ClusterReduceState, pstate);
	TupleTableSlot *slot;
	RdcPort		   *port;
	ReduceEntry		entry;
	bool			found;
	Oid				eof_oid;
	EState		   *estate;
	Tuplestorestate*tuplestorestate;
	ScanDirection	dir;
	bool			forward;
	bool			eof_tuplestore;

	port = node->port;
	estate = node->ps.state;
	dir = estate->es_direction;
	forward = ScanDirectionIsForward(dir);
	tuplestorestate = node->tuplestorestate;
	node->started = true;
	Assert(port);

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
	 * If we are not at the end of the tuplestore, or are going backwards, try
	 * to fetch a tuple from tuplestore.
	 */
	eof_tuplestore = (tuplestorestate == NULL) ||
		tuplestore_ateof(tuplestorestate);

	if (!forward && eof_tuplestore)
	{
		if (!node->eof_underlying)
		{
			/*
			 * When reversing direction at tuplestore EOF, the first
			 * gettupleslot call will fetch the last-added tuple; but we want
			 * to return the one before that, if possible. So do an extra
			 * fetch.
			 */
			if (!tuplestore_advance(tuplestorestate, forward))
				return NULL;	/* the tuplestore must be empty */
		}
		eof_tuplestore = false;
	}

	slot = node->ps.ps_ResultTupleSlot;
	if (!eof_tuplestore)
	{
		if (tuplestore_gettupleslot(tuplestorestate, forward, false, slot))
			return slot;
		if (forward)
			eof_tuplestore = true;
	}

	if (eof_tuplestore)
	{
		TupleTableSlot *outerslot = NULL;

		/* ClusterReduce need to sort keys */
		if (node->nkeys > 0)
			outerslot = ExecClusterMergeReduce(node);
		else
		{
			while (!node->eof_underlying || !node->eof_network)
			{
				/* fetch tuple from outer node */
				if (!node->eof_underlying)
				{
					outerslot = GetSlotFromOuter(node);
					if (!TupIsNull(outerslot))
						break;
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

					if(node->convert)
					{
						GetSlotFromRemote(port, node->convert_slot, NULL, &eof_oid, &node->closed_remote);
						outerslot = do_type_convert_slot_in(node->convert, node->convert_slot, slot, false);
					}else
					{
						outerslot = GetSlotFromRemote(port, slot, NULL, &eof_oid, &node->closed_remote);
					}

					if (OidIsValid(eof_oid))
					{
						found = false;
						entry = hash_search(node->rdc_htab, &eof_oid, HASH_FIND, &found);
						Assert(found);
						if (!entry->re_eof)
						{
							entry->re_eof = true;
							node->neofs++;
						}
						node->eof_network = (node->neofs == node->nrdcs - 1);
					} else if (!TupIsNull(outerslot))
						break;
				}
			}
		}

		/*
		 * Append a copy of the returned tuple to tuplestore.  NOTE: because
		 * the tuplestore is certainly in EOF state, its read position will
		 * move forward over the added tuple.  This is what we want.
		 */
		if (!TupIsNull(outerslot) && tuplestorestate)
			tuplestore_puttupleslot(tuplestorestate, outerslot);

		/*
		 * We can just return the subplan's returned tuple, without copying.
		 */
		return outerslot;
	}

	/*
	 * Nothing left ...
	 */
	return ExecClearTuple(slot);
}

static TupleTableSlot *
GetMergeSlotFromOuter(ClusterReduceState *node, ReduceEntry entry)
{
	TupleTableSlot	   *outerslot;
	TupleTableSlot	   *cur_slot;
	Tuplestorestate	   *cur_store;

	Assert(node && node->port && entry);
	Assert(entry->re_key == PGXCNodeOid);

	cur_store = entry->re_store;
	cur_slot = entry->re_slot;

	/*
	 * traversal scan local outer node, keep the slots belong to myself in
	 * the Tuplestorestate and send out the slots that don't belong to myself.
	 */
	while (!node->eof_underlying)
	{
		outerslot = GetSlotFromOuter(node);
		if (TupIsNull(outerslot))
			break;
		if (tuplestore_ateof(cur_store))
			tuplestore_clear(cur_store);
		tuplestore_puttupleslot(cur_store, outerslot);
	}
	Assert(node->eof_underlying);

	/* record the EOF of local outer node */
	entry->re_eof = true;

	/*
	 * try to get from its Tuplestorestate
	 */
	if (!tuplestore_ateof(cur_store))
	{
		if (tuplestore_gettupleslot(cur_store, true, true, cur_slot))
			return cur_slot;
	}

	return ExecClearTuple(cur_slot);
}

static TupleTableSlot *
GetMergeSlotFromRemote(ClusterReduceState *node, ReduceEntry entry)
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
			return cur_slot;
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

		if(node->convert)
		{
			GetSlotFromRemote(port, node->convert_slot, &slot_oid, &eof_oid, &(node->closed_remote));
			outerslot = do_type_convert_slot_in(node->convert, node->convert_slot, cur_slot, true);
		}else
		{
			outerslot = GetSlotFromRemote(port, cur_slot, &slot_oid, &eof_oid, &(node->closed_remote));
		}

		if (OidIsValid(eof_oid))
		{
			if (eof_oid == cur_oid)
			{
				entry->re_eof = true;
				node->neofs++;
				node->eof_network = (node->neofs == node->nrdcs - 1);
				return ExecClearTuple(cur_slot);
			} else
			{
				found = false;
				othr_entry = hash_search(node->rdc_htab, &eof_oid, HASH_FIND, &found);
				Assert(found);
				if (!othr_entry->re_eof)
				{
					othr_entry->re_eof = true;
					node->neofs++;
					node->eof_network = (node->neofs == node->nrdcs - 1);
				}
			}
		} else if (!TupIsNull(outerslot))
		{
			Assert(OidIsValid(slot_oid));
			if (slot_oid == cur_oid)
				return outerslot;

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
		entry->re_slot = GetMergeSlotFromOuter(node, entry);
		if (!TupIsNull(entry->re_slot))
			binaryheap_add_unordered(node->binheap, PointerGetDatum(entry));

		/* iniialize remote slot */
		for (i = 0; i < node->nrdcs; i++)
		{
			entry = node->rdc_entrys[i];
			if (entry->re_key == PGXCNodeOid)
				continue;
			entry->re_slot = GetMergeSlotFromRemote(node, entry);
			if (!TupIsNull(entry->re_slot))
				binaryheap_add_unordered(node->binheap, PointerGetDatum(entry));
		}
		binaryheap_build(node->binheap);
		node->initialized = true;
	} else
	{
		entry = (ReduceEntry) DatumGetPointer(binaryheap_first(node->binheap));
		if (entry->re_key == PGXCNodeOid)
			entry->re_slot = GetMergeSlotFromOuter(node, entry);
		else
			entry->re_slot = GetMergeSlotFromRemote(node, entry);

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

static void
ClusterReducePortCleanupCallback(void *arg)
{
	ClusterReduceState *node = (ClusterReduceState *) arg;

	ExecDisconnectClusterReduce(node, true);
}

static void
ExecDisconnectClusterReduce(ClusterReduceState *node, bool noerorr)
{
	if (node && node->port)
	{
		List *dest_nodes = NIL;

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
		if (!RdcSendEOF(node->port))
			dest_nodes = PlanStateGetTargetNodes(node);
		DisConnectSelfReduce(node->port, dest_nodes, noerorr);
		node->port = NULL;
	}
}

void
ExecEndClusterReduce(ClusterReduceState *node)
{
	ExecDisconnectClusterReduce(node, false);
	list_free(node->closed_remote);
	node->closed_remote = NIL;
	if (node->rdc_entrys)
	{
		TupleTableSlot	   *re_slot;
		Tuplestorestate	   *re_store;
		int					i;
		for (i = 0; i < node->nrdcs; i++)
		{
			re_slot = node->rdc_entrys[i]->re_slot;
			re_store = node->rdc_entrys[i]->re_store;
			if (re_slot)
				ExecDropSingleTupleTableSlot(re_slot);
			if (re_store)
				tuplestore_end(re_store);
			node->rdc_entrys[i]->re_slot = NULL;
			node->rdc_entrys[i]->re_store = NULL;
		}
		pfree(node->rdc_entrys);
		node->rdc_entrys = NULL;
	}
	if (node->rdc_htab)
	{
		hash_destroy(node->rdc_htab);
		node->rdc_htab = NULL;
	}

	/*
	 * Release tuplestore resources
	 */
	if (node->tuplestorestate != NULL)
		tuplestore_end(node->tuplestorestate);
	node->tuplestorestate = NULL;

	/* release convert */
	if(node->convert)
	{
		ExecDropSingleTupleTableSlot(node->convert_slot);
		free_type_convert(node->convert);
	}

	ExecEndNode(outerPlanState(node));
}

/* ----------------------------------------------------------------
 *		ExecClusterReduceMarkPos
 *
 *		Calls tuplestore to save the current position in the stored file.
 * ----------------------------------------------------------------
 */
void
ExecClusterReduceMarkPos(ClusterReduceState *node)
{
	Assert(node->eflags & EXEC_FLAG_MARK);

	/*
	 * if we haven't materialized yet, just return.
	 */
	if (!node->tuplestorestate)
		return;

	/*
	 * copy the active read pointer to the mark.
	 */
	tuplestore_copy_read_pointer(node->tuplestorestate, 0, 1);

	/*
	 * since we may have advanced the mark, try to truncate the tuplestore.
	 */
	tuplestore_trim(node->tuplestorestate);
}

/* ----------------------------------------------------------------
 *		ExeClusterReduceRestrPos
 *
 *		Calls tuplestore to restore the last saved file position.
 * ----------------------------------------------------------------
 */
void
ExecClusterReduceRestrPos(ClusterReduceState *node)
{
	Assert(node->eflags & EXEC_FLAG_MARK);

	/*
	 * if we haven't materialized yet, just return.
	 */
	if (!node->tuplestorestate)
		return;

	/*
	 * copy the mark to the active read pointer.
	 */
	tuplestore_copy_read_pointer(node->tuplestorestate, 1, 0);
}

void
ExecReScanClusterReduce(ClusterReduceState *node)
{
	PlanState  *outerPlan = outerPlanState(node);

	/* Just return if not start ExecClusterReduce */
	if (!node->started)
		return;

	ExecClearTuple(node->ps.ps_ResultTupleSlot);

	if (node->eflags != 0)
	{
		/*
		 * If we haven't materialized yet, just return. If outerplan's
		 * chgParam is not NULL then it will be re-scanned by ExecProcNode,
		 * else no reason to re-scan it at all.
		 */
		if (!node->tuplestorestate)
			return;

		/*
		 * If subnode is to be rescanned then we forget previous stored
		 * results; we have to re-read the subplan and re-store.  Also, if we
		 * told tuplestore it needn't support rescan, we lose and must
		 * re-read.  (This last should not happen in common cases; else our
		 * caller lied by not passing EXEC_FLAG_REWIND to us.)
		 *
		 * Otherwise we can just rewind and rescan the stored output. The
		 * state of the subnode does not change.
		 */
		if (outerPlan->chgParam != NULL ||
			(node->eflags & EXEC_FLAG_REWIND) == 0)
		{
			tuplestore_end(node->tuplestorestate);
			node->tuplestorestate = NULL;
			if (outerPlan->chgParam == NULL)
				ExecReScan(outerPlan);
			node->eof_underlying = false;

			PrepareForReScanClusterReduce(node);
		}
		else
			tuplestore_rescan(node->tuplestorestate);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ClusterReduce not support rescan without EXEC_FLAG_REWIND flag")));
	}
}

static void
PrepareForReScanClusterReduce(ClusterReduceState *node)
{
	if (node->rdc_entrys)
	{
		TupleTableSlot	   *re_slot;
		Tuplestorestate	   *re_store;
		int					i;

		for (i = 0; i < node->nrdcs; i++)
		{
			re_slot = node->rdc_entrys[i]->re_slot;
			re_store = node->rdc_entrys[i]->re_store;
			if (re_slot)
				ExecClearTuple(re_slot);
			if (re_store)
				tuplestore_clear(re_store);
			node->rdc_entrys[i]->re_eof = false;
		}
	}

	if (node->nkeys > 0)
	{
		binaryheap_reset(node->binheap);
		node->initialized = false;
	}

	list_free(node->closed_remote);
	node->closed_remote = NIL;

	node->eof_network = false;
	node->neofs = 0;
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
			ExecInitClusterReduceStateExtra(crstate);
	}

	return planstate_tree_walker(node, ExecConnectReduceWalker, estate);
}

void
ExecConnectReduce(PlanState *node)
{
	if (node == NULL)
		return;
	Assert((node->state->es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY) == 0);
	ExecConnectReduceWalker(node, node->state);
}

static bool
DriveClusterReduceState(ClusterReduceState *node)
{
	Assert(node && IsA(node, ClusterReduceState));
	if (!node->eof_network &&
		!RdcSendEOF(node->port) &&
		!RdcSendCLOSE(node->port))
	{
		int i;

		/* reject to get slot from remote, tell them */
		if (list_member_oid(PlanStateGetTargetNodes(node), PGXCNodeOid))
			SendRejectToRemote(node->port, PlanStateGetTargetNodes(node));

		for (i = 0; i < node->nrdcs; i++)
			node->rdc_entrys[i]->re_eof = true;
		node->eof_network = true;
	}

	while (!node->eof_underlying)
		(void) GetSlotFromOuter(node);

	return false;
}

static bool
DriveCteScanState(PlanState *node)
{
	TupleTableSlot *slot = NULL;
	ListCell	   *lc = NULL;
	SubPlanState   *sps = NULL;

	Assert(node && IsA(node, CteScanState));

	if (!IsThereClusterReduce(node))
		return false;

	/*
	 * Here we do ExecCteScan instead of just driving ClusterReduce,
	 * because other plan node may need the results of the CteScan.
	 */
	for (;;)
	{
		slot = node->ExecProcNode(node);
		if (TupIsNull(slot))
			break;
	}

	/*
	 * Do not forget to drive subPlan-s.
	 */
	foreach (lc, node->subPlan)
	{
		sps = (SubPlanState *) lfirst(lc);

		Assert(IsA(sps, SubPlanState));
		if (DriveClusterReduceWalker(sps->planstate))
			return true;
	}

	/*
	 * Do not forget to drive initPlan-s.
	 */
	foreach (lc, node->initPlan)
	{
		sps = (SubPlanState *) lfirst(lc);

		Assert(IsA(sps, SubPlanState));
		if (DriveClusterReduceWalker(sps->planstate))
			return true;
	}

	return false;
}

static bool
DriveMaterialState(PlanState *node)
{
	TupleTableSlot *slot = NULL;

	Assert(node && IsA(node, MaterialState));

	if (!IsThereClusterReduce(node))
		return false;

	/*
	 * Here we do ExecMaterial instead of just driving ClusterReduce,
	 * because other plan node may need the results of the Material.
	 */
	for (;;)
	{
		slot = node->ExecProcNode(node);
		if (TupIsNull(slot))
			break;
	}

	return false;
}

static bool
DriveClusterReduceWalker(PlanState *node)
{
	EState	   *estate;
	int			planid;
	bool		res;

	if (node == NULL)
		return false;

	estate = node->state;
	if (list_member_ptr(estate->es_auxmodifytables, node))
	{
		ModifyTableState *mtstate = (ModifyTableState *) node;

		/*
		 * It's safe to drive ClusterReduce if the secondary
		 * ModifyTableState is done(mt_done is true). otherwise
		 * the secondary ModifyTableState will be done by
		 * ExecPostprocessPlan later and it is not correct to
		 * drive here.
		 */
		if (!mtstate->mt_done)
			return false;
	}

	/* do not drive twice */
	planid = PlanNodeID(node->plan);
	if (bms_is_member(planid, estate->es_reduce_drived_set))
		return false;

	if (IsA(node, ClusterReduceState))
	{
		ClusterReduceState *crs = (ClusterReduceState *) node;
		Assert(crs->port);

		if (!crs->eof_network || !crs->eof_underlying)
			adb_elog(print_reduce_debug_log, LOG,
				"Drive ClusterReduce(%d) to send EOF message", planid);

		/*
		 * Drive all ClusterReduce to send slot, discard slot
		 * used for local.
		 */
		res = DriveClusterReduceState(crs);
	} else
	if (IsA(node, CteScanState))
	{
		res = DriveCteScanState(node);
	} else
	if (IsA(node, MaterialState))
	{
		res = DriveMaterialState(node);
	} else
	{
		res = planstate_tree_exec_walker(node, DriveClusterReduceWalker, NULL);
	}

	estate->es_reduce_drived_set = bms_add_member(estate->es_reduce_drived_set, planid);

	return res;
}

static bool
IsThereClusterReduce(PlanState *node)
{
	if (node == NULL)
		return false;

	if (IsA(node, ClusterReduceState))
		return true;

	if (IsA(node, CteScanState) &&
		IsThereClusterReduce(((CteScanState *) node)->cteplanstate))
		return true;

	return planstate_tree_walker(node, IsThereClusterReduce, NULL);
}

void
TopDownDriveClusterReduce(PlanState *node)
{
	if (!enable_cluster_plan || !IsUnderPostmaster)
		return ;

	/* just return if there is no ClusterReduce plan */
	if (!node->state->es_reduce_plan_inited)
		return ;

	(void) DriveClusterReduceWalker(node);
}
