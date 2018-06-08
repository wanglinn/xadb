
#include "postgres.h"

#include "nodes/execnodes.h"

#include "executor/clusterReceiver.h"
#include "executor/execCluster.h"
#include "executor/executor.h"
#include "intercomm/inter-comm.h"
#include "lib/binaryheap.h"
#include "libpq/libpq-node.h"
#include "libpq/libpq-fe.h"

#include "executor/nodeClusterMergeGather.h"

typedef struct CMGHookContext
{
	PlanState *ps;
	TupleTableSlot *slot;
	ClusterRecvState *state;
}CMGHookContext;

static int cmg_heap_compare_slots(Datum a, Datum b, void *arg);
static TupleTableSlot *cmg_get_remote_slot(PGconn *conn, TupleTableSlot *slot, ClusterMergeGatherState *ps);
static bool cmg_pqexec_finish_hook(void *context, struct pg_conn *conn, PQNHookFuncType type, ...);
static bool cmg_pqexec_normal_hook(void *context, struct pg_conn *conn, PQNHookFuncType type, ...);

ClusterMergeGatherState *ExecInitClusterMergeGather(ClusterMergeGather *node, EState *estate, int eflags)
{
	ClusterMergeGatherState *ps = makeNode(ClusterMergeGatherState);
	int nremote;
	int i;

	/* check for unsupported flags */
	Assert((eflags & (EXEC_FLAG_BACKWARD|EXEC_FLAG_MARK|EXEC_FLAG_REWIND)) == 0);

	ps->ps.plan = (Plan*)node;
	ps->ps.state = estate;
	ps->local_end = false;

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

	ps->binheap = binaryheap_allocate(nremote+1, cmg_heap_compare_slots, ps);
	ps->conns = palloc0(sizeof(ps->conns[0]) * nremote);
	ps->slots = palloc0(sizeof(ps->slots[0]) * (nremote+1));
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
		List *list = GetPGconnAttatchCurrentInterXact(node->rnodes);
		Assert(list_length(list) == nremote);
		for(i=0,lc=list_head(list);lc!=NULL;lc=lnext(lc),++i)
			ps->conns[i] = lfirst(lc);
		Assert(i == nremote);
	}

	ps->initialized = false;

	ps->recv_state = createClusterRecvState((PlanState*)ps, true);

	return ps;
}

TupleTableSlot *ExecClusterMergeGather(ClusterMergeGatherState *node)
{
	TupleTableSlot *result;
	ClusterGatherType gatherType;
	int32	i;

re_get_:
	if(node->initialized == false)
	{
		result = ExecProcNode(outerPlanState(node));
		if(!TupIsNull(result))
		{
			node->slots[node->nremote] = result;
			binaryheap_add_unordered(node->binheap, Int32GetDatum(node->nremote));
		}else
		{
			node->local_end = true;
		}
		for(i=0;i<node->nremote;++i)
		{
			result = cmg_get_remote_slot(node->conns[i], node->slots[i], node);
			if(!TupIsNull(result))
				binaryheap_add_unordered(node->binheap, Int32GetDatum(i));
		}
		binaryheap_build(node->binheap);
		node->initialized = true;
	}else if (!binaryheap_empty(node->binheap))
	{
		i = DatumGetInt32(binaryheap_first(node->binheap));
		if(i < node->nremote)
		{
			result = cmg_get_remote_slot(node->conns[i], node->slots[i], node);
		}else
		{
			Assert(i == node->nremote);
			result = ExecProcNode(outerPlanState(node));
			node->slots[i] = result;
		}
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
		gatherType = ((ClusterMergeGather*)node->ps.plan)->gatherType;
		i = DatumGetInt32(binaryheap_first(node->binheap));
		if(i == node->nremote)
		{
			if((gatherType & CLUSTER_GATHER_COORD) == 0)
				goto re_get_;
		}else
		{
			if((gatherType & CLUSTER_GATHER_DATANODE) == 0)
				goto re_get_;
		}
		result = node->slots[i];
	}

	return result;
}

void ExecFinishClusterMergeGather(ClusterMergeGatherState *node)
{
	List *list;
	int i;

	for(i=0;i<node->nremote;++i)
	{
		PGconn *conn = node->conns[i];
		if(conn != NULL && PQisCopyInState(conn))
			PQputCopyEnd(conn, NULL);
	}

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
		node->recv_state->base_slot = node->ps.ps_ResultTupleSlot;
		PQNListExecFinish(list, NULL, cmg_pqexec_normal_hook, node, true);
		list_free(list);
	}
}

void ExecEndClusterMergeGather(ClusterMergeGatherState *node)
{
	ExecEndNode(outerPlanState(node));
	freeClusterRecvState(node->recv_state);
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

static TupleTableSlot *cmg_get_remote_slot(PGconn *conn, TupleTableSlot *slot, ClusterMergeGatherState *ps)
{
	CMGHookContext context;
	context.ps = &ps->ps;
	context.slot = slot;
	context.state = ps->recv_state;
	ExecClearTuple(slot);
	PQNOneExecFinish(conn, cmg_pqexec_finish_hook, &context, true);
	return slot;
}

bool cmg_pqexec_finish_hook(void *context, struct pg_conn *conn, PQNHookFuncType type, ...)
{
	va_list args;
	const char *buf;
	CMGHookContext *cmcontext;
	int len;

	switch(type)
	{
	case PQNHFT_ERROR:
		return PQNEFHNormal(NULL, conn, type);
	case PQNHFT_COPY_OUT_DATA:
		cmcontext = context;
		va_start(args, type);
		buf = va_arg(args, const char*);
		len = va_arg(args, int);
		cmcontext->state->base_slot = cmcontext->slot;
		if (buf[0] == CLUSTER_MSG_EXECUTOR_RUN_END)
		{
			va_end(args);
			return true;
		}
		if(clusterRecvTupleEx(cmcontext->state, buf, len, conn))
		{
			va_end(args);
			return true;
		}
		va_end(args);
		break;
	case PQNHFT_COPY_IN_ONLY:
		PQputCopyEnd(conn, NULL);
		break;
	case PQNHFT_RESULT:
		{
			PGresult *res;
			ExecStatusType status;
			va_start(args, type);
			res = va_arg(args, PGresult*);
			if(res)
			{
				status = PQresultStatus(res);
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

static bool cmg_pqexec_normal_hook(void *context, struct pg_conn *conn, PQNHookFuncType type, ...)
{
	ClusterMergeGatherState *cmgs;
	va_list args;
	const char *buf;
	int len;

	if (type == PQNHFT_COPY_OUT_DATA)
	{
		cmgs = context;
		va_start(args, type);
		buf = va_arg(args, const char*);
		len = va_arg(args, int);
		clusterRecvTupleEx(cmgs->recv_state, buf, len, conn);
		return false;
	}else
	{
		return PQNEFHNormal(NULL, conn, type);
	}
}
