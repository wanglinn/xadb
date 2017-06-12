
#include "postgres.h"

#include "nodes/execnodes.h"

#include "executor/clusterReceiver.h"
#include "executor/execCluster.h"
#include "executor/executor.h"
#include "lib/binaryheap.h"
#include "libpq/libpq-node.h"
#include "libpq/libpq-fe.h"

#include "executor/nodeClusterMergeGather.h"

static int cmg_heap_compare_slots(Datum a, Datum b, void *arg);
static TupleTableSlot *cmg_get_remote_slot(PGconn *conn, TupleTableSlot *slot);

ClusterMergeGatherState *ExecInitClusterMergeGather(ClusterMergeGather *node, EState *estate, int eflags)
{
	ClusterMergeGatherState *ps = makeNode(ClusterMergeGatherState);
	int nremote;
	int i;

	/* check for unsupported flags */
	Assert((eflags & (EXEC_FLAG_BACKWARD|EXEC_FLAG_MARK|EXEC_FLAG_REWIND)) == 0);

	ps->ps.plan = (Plan*)node;
	ps->ps.state = estate;

	ExecInitResultTupleSlot(estate, &ps->ps);
	ExecAssignResultTypeFromTL(&ps->ps);

	ps->nkeys = node->numCols;
	ps->sortkeys = palloc0(sizeof(ps->sortkeys[0]) * node->numCols);

	for(i=0;i<node->numCols;++i)
	{
		SortSupport sortkey = &ps->sortkeys[i];

		sortkey->ssup_cxt = CurrentMemoryContext;
		sortkey->ssup_collation = node->collations[i];
		sortkey->ssup_nulls_first = node->nullsFirst[i];
		sortkey->ssup_attno = node->sortColIdx[i];

		PrepareSortSupportFromOrderingOp(node->sortOperators[i], sortkey);
	}

	nremote = list_length(node->rnodes);
	Assert(nremote > 0);
	ps->nremote = nremote;

	ps->binheap = binaryheap_allocate(nremote, cmg_heap_compare_slots, ps);
	ps->conns = palloc0(sizeof(ps->conns[0]) * nremote);
	ps->slots = palloc0(sizeof(ps->slots[0]) * nremote);
	for(i=0;i<nremote;++i)
	{
		ps->slots[i] = ExecAllocTableSlot(&estate->es_tupleTable);
		ExecSetSlotDescriptor(ps->slots[i], ps->ps.ps_ResultTupleSlot->tts_tupleDescriptor);
	}

	outerPlanState(ps) = ExecStartClusterPlan(outerPlan(node)
							, estate, eflags, node->rnodes);

	if((eflags & EXEC_FLAG_EXPLAIN_ONLY) == 0)
	{
		ListCell *lc;
		List *list = PQNGetConnUseOidList(node->rnodes);
		Assert(list_length(list) == nremote);
		for(i=0,lc=list_head(list);lc!=NULL;lc=lnext(lc),++i)
			ps->conns[i] = lfirst(lc);
		Assert(i == nremote);
	}

	ps->initialized = false;

	return ps;
}

TupleTableSlot *ExecClusterMergeGather(ClusterMergeGatherState *node)
{
	TupleTableSlot *result;
	int32	i;

	if(node->initialized == false)
	{
		for(i=0;i<node->nremote;++i)
		{
			result = cmg_get_remote_slot(node->conns[i], node->slots[i]);
			if(!TupIsNull(result))
				binaryheap_add_unordered(node->binheap, Int32GetDatum(i));
		}
		binaryheap_build(node->binheap);
		node->initialized = true;
	}else
	{
		i = DatumGetInt32(binaryheap_first(node->binheap));
		result = cmg_get_remote_slot(node->conns[i], node->slots[i]);
		if(!TupIsNull(result))
			binaryheap_replace_first(node->binheap, Int32GetDatum(i));
		else
			binaryheap_remove_first(node->binheap);
	}

	if(binaryheap_empty(node->binheap))
	{
		result = ExecClearTuple(node->ps.ps_ResultTupleSlot);
	}else
	{
		i = DatumGetInt32(binaryheap_first(node->binheap));
		result = node->slots[i];
	}

	return result;
}

void ExecEndClusterMergeGather(ClusterMergeGatherState *node)
{
	List *list;
	int i;

	for(i=0;i<node->nremote;++i)
	{
		PGconn *conn = node->conns[i];
		if(conn != NULL && PQisCopyInState(conn))
			PQputCopyEnd(conn, NULL);
	}
	ExecEndNode(outerPlanState(node));

	list = NIL;
	for(i=0;i<node->nremote;++i)
	{
		if(node->conns[i] == NULL)
			continue;
		if(PQstatus(node->conns[i]) != CONNECTION_OK
			|| PQtransactionStatus(node->conns[i]) != PQTRANS_IDLE)
		{
			list = lappend(list, node->conns[i]);
		}
	}
	if(list != NIL)
	{
		PQNListExecFinish(list, PQNEFHNormal, NULL);
		list_free(list);
	}
}

void ExecReScanClusterMergeGather(ClusterMergeGatherState *node)
{
	ereport(ERROR, (errmsg("ClusterMergeGather not support rescan")));
}

/*
 * Compare the tuples in the two given slots.
 */
static int32
cmg_heap_compare_slots(Datum a, Datum b, void *arg)
{
	ClusterMergeGatherState *node = (ClusterMergeGatherState *) arg;
	int32	slot1 = DatumGetInt32(a);
	int32	slot2 = DatumGetInt32(b);

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

static TupleTableSlot *cmg_get_remote_slot(PGconn *conn, TupleTableSlot *slot)
{
	const char *buf;
	PGresult *res;
	ExecStatusType status;
	int n;

	for(;;)
	{
		if(PQstatus(conn) == CONNECTION_BAD)
		{
			res = PQgetResult(conn);
			PQNReportResultError(res, conn, ERROR, true);
			return NULL;	/* never run */
		}
		if(PQisCopyOutState(conn))
		{
			/* TODO select and CHECK_FOR_INTERRUPTS */
			n = PQgetCopyDataBuffer(conn, &buf, false);
			if(n > 0)
			{
				if(clusterRecvTuple(slot, buf, n))
					return slot;
				continue;
			}else if(n < 0)
			{
				continue;
			}
		}else if(PQisCopyInState(conn))
		{
			PQputCopyEnd(conn, NULL);
		}else if(PQisBusy(conn) == false)
		{
			res = PQgetResult(conn);
			if(res == NULL)
			{
				return ExecClearTuple(slot);
			}
			status = PQresultStatus(res);
			if(status == PGRES_FATAL_ERROR)
			{
				PQNReportResultError(res, conn, ERROR, true);
			}else if(status == PGRES_COMMAND_OK)
			{
				return ExecClearTuple(slot);
			}else if(status == PGRES_COPY_IN)
			{
				PQputCopyEnd(conn, NULL);
				continue;
			}else if(status == PGRES_COPY_OUT)
			{
				continue;
			}else
			{
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("not support execute status type \"%d\"", status)));
			}
		}
	}
	return ExecClearTuple(slot);
}
