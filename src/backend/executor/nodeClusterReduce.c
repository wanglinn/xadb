
#include "postgres.h"
#include "miscadmin.h"

#include "executor/executor.h"
#include "executor/nodeClusterReduce.h"
#include "executor/nodeCtescan.h"
#include "executor/nodeMaterial.h"
#include "executor/tuptable.h"
#include "lib/binaryheap.h"
#include "lib/oidbuffer.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "pgxc/pgxc.h"
#include "storage/buffile.h"
#include "storage/shm_mq.h"
#include "utils/dynamicreduce.h"
#include "utils/hsearch.h"

typedef enum ReduceType
{
	RT_NOTHING = 1,
	RT_NORMAL,
	RT_MERGE
}ReduceType;

typedef struct NormalReduceState
{
	dsm_segment	   *dsm_seg;
	DynamicReduceIOBuffer
					drio;
}NormalReduceState;

typedef struct MergeNodeInfo
{
	TupleTableSlot	   *slot;
	BufFile			   *file;
	StringInfoData		read_buf;
	Oid					nodeoid;
}MergeNodeInfo;

typedef struct MergeReduceState
{
	NormalReduceState	normal;
	MergeNodeInfo	   *nodes;
	binaryheap		   *binheap;
	SortSupport			sortkeys;
	uint32				nkeys;
	uint32				nnodes;
}MergeReduceState;

extern bool enable_cluster_plan;

static int cmr_heap_compare_slots(Datum a, Datum b, void *arg);
static bool DriveClusterReduceState(ClusterReduceState *node);
static bool DriveCteScanState(PlanState *node);
static bool DriveMaterialState(PlanState *node);
static bool DriveClusterReduceWalker(PlanState *node);
static bool IsThereClusterReduce(PlanState *node);

/* ======================= nothing reduce========================== */
static TupleTableSlot* ExecNothingReduce(PlanState *pstate)
{
	return ExecClearTuple(pstate->ps_ResultTupleSlot);
}

/* ======================= normal reduce ========================== */
static TupleTableSlot* ExecNormalReduce(PlanState *pstate)
{
	ClusterReduceState *node = castNode(ClusterReduceState, pstate);
	NormalReduceState  *normal = node->private_state;
	Assert(normal != NULL && node->reduce_method == RT_NORMAL);

	return DynamicReduceFetchSlot(&normal->drio);
}

static TupleTableSlot* ExecReduceFetchLocal(void *pstate, ExprContext *econtext)
{
	TupleTableSlot *slot = ExecProcNode(pstate);
	econtext->ecxt_outertuple = slot;
	return slot;
}

static void InitNormalReduceState(NormalReduceState *normal, Size shm_size, ClusterReduceState *crstate)
{
	DynamicReduceMQ		drmq;
	Expr			   *expr;
	ClusterReduce	   *plan;

	normal->dsm_seg = dsm_create(shm_size, 0);
	drmq = dsm_segment_address(normal->dsm_seg);

	DynamicReduceInitFetch(&normal->drio,
						   normal->dsm_seg,
						   crstate->ps.ps_ResultTupleSlot->tts_tupleDescriptor,
						   drmq->worker_sender_mq, sizeof(drmq->worker_sender_mq),
						   drmq->reduce_sender_mq, sizeof(drmq->reduce_sender_mq));
	normal->drio.econtext = crstate->ps.ps_ExprContext;
	normal->drio.FetchLocal = ExecReduceFetchLocal;
	normal->drio.user_data = outerPlanState(crstate);

	/* init reduce expr */
	plan = castNode(ClusterReduce, crstate->ps.plan);
	if(plan->special_node == PGXCNodeOid)
	{
		Assert(plan->special_reduce != NULL);
		expr = plan->special_reduce;
	}else
	{
		expr = plan->reduce;
	}
	Assert(expr != NULL);
	normal->drio.expr_state = ExecInitReduceExpr(expr);
}
static void InitNormalReduce(ClusterReduceState *crstate)
{
	MemoryContext		oldcontext;
	NormalReduceState  *normal;
	Assert(crstate->private_state == NULL);

	oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(crstate));
	normal = palloc0(sizeof(NormalReduceState));
	InitNormalReduceState(normal, sizeof(DynamicReduceMQData), crstate);
	crstate->private_state = normal;
	ExecSetExecProcNode(&crstate->ps, ExecNormalReduce);
	DynamicReduceStartNormalPlan(crstate->ps.plan->plan_node_id, 
								 normal->dsm_seg,
								 dsm_segment_address(normal->dsm_seg),
								 crstate->ps.ps_ResultTupleSlot->tts_tupleDescriptor);
	MemoryContextSwitchTo(oldcontext);
}
static void EndNormalReduce(NormalReduceState *normal)
{
	DynamicReduceClearFetch(&normal->drio);
	dsm_detach(normal->dsm_seg);
}
static void DriveNormalReduce(ClusterReduceState *node)
{
	TupleTableSlot	   *slot;
	NormalReduceState  *normal = node->private_state;

	if (normal->drio.eof_local == false ||
		normal->drio.eof_remote == false ||
		normal->drio.send_buf.len > 0)
	{
		do
		{
			slot = DynamicReduceFetchSlot(&normal->drio);
		}while(!TupIsNull(slot));
	}
}

/* ========================= merge reduce =========================== */
static inline TupleTableSlot* GetMergeReduceResult(MergeReduceState *merge, ClusterReduceState *node)
{
	if (binaryheap_empty(merge->binheap))
		return ExecClearTuple(node->ps.ps_ResultTupleSlot);

	return merge->nodes[DatumGetUInt32(binaryheap_first(merge->binheap))].slot;
}

static TupleTableSlot* ExecMergeReduce(PlanState *pstate)
{
	ClusterReduceState *node = castNode(ClusterReduceState, pstate);
	MergeReduceState   *merge = node->private_state;
	MergeNodeInfo	   *info;
	uint32				i;

	i = DatumGetUInt32(binaryheap_first(merge->binheap));
	info = &merge->nodes[i];
	DynamicReduceReadSFSTuple(info->slot, info->file, &info->read_buf);
	if (TupIsNull(info->slot))
		binaryheap_remove_first(merge->binheap);
	else
		binaryheap_replace_first(merge->binheap, UInt32GetDatum(i));

	return GetMergeReduceResult(merge, node);
}

static BufFile* GetMergeBufFile(MergeReduceState *merge, Oid nodeoid)
{
	uint32	i,count;

	for (i=0,count=merge->nnodes;i<count;++i)
	{
		if (merge->nodes[i].nodeoid == nodeoid)
			return merge->nodes[i].file;
	}

	return NULL;
}

static void OpenMergeBufFiles(MergeReduceState *merge)
{
	MemoryContext		oldcontext;
	MergeNodeInfo	   *info;
	DynamicReduceSFS	sfs;
	uint32				i;
	char				name[MAXPGPATH];

	oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(merge));
	sfs = dsm_segment_address(merge->normal.dsm_seg);
	for(i=0;i<merge->nnodes;++i)
	{
		info = &merge->nodes[i];
		if (info->file == NULL)
		{
			info->file = BufFileOpenShared(&sfs->sfs,
										   DynamicReduceSFSFileName(name, info->nodeoid));
		}
		if (BufFileSeek(info->file, 0, 0, SEEK_SET) != 0)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("can not seek SFS file to head")));
		}
	}
	MemoryContextSwitchTo(oldcontext);
}

static void BuildMergeBinaryHeap(MergeReduceState *merge)
{
	MergeNodeInfo  *info;
	uint32			i,count;

	for(i=0,count=merge->nnodes;i<count;++i)
	{
		info = &merge->nodes[i];
		DynamicReduceReadSFSTuple(info->slot, info->file, &info->read_buf);
		if (!TupIsNull(info->slot))
			binaryheap_add_unordered(merge->binheap, UInt32GetDatum(i));
	}
	binaryheap_build(merge->binheap);
}

static TupleTableSlot* ExecMergeReduceFirst(PlanState *pstate)
{
	ClusterReduceState *node = castNode(ClusterReduceState, pstate);
	MergeReduceState   *merge = node->private_state;
	BufFile			   *file;
	TupleTableSlot	   *slot;

	/* find local MergeNodeInfo */
	file = GetMergeBufFile(merge, PGXCNodeOid);
	Assert(file != NULL);

	while(merge->normal.drio.eof_local == false)
	{
		slot = DynamicReduceFetchLocal(&merge->normal.drio);
		if (merge->normal.drio.send_buf.len > 0)
		{
			DynamicReduceSendMessage(merge->normal.drio.mqh_sender,
									 merge->normal.drio.send_buf.len,
									 merge->normal.drio.send_buf.data,
									 false);
			merge->normal.drio.send_buf.len = 0;
		}
		if (!TupIsNull(slot))
			DynamicReduceWriteSFSTuple(slot, file);
	}

	/* wait dynamic reduce end of plan */
	DynamicReduceRecvTuple(merge->normal.drio.mqh_receiver,
						   pstate->ps_ResultTupleSlot,
						   &merge->normal.drio.recv_buf,
						   NULL,
						   false);
	Assert(TupIsNull(pstate->ps_ResultTupleSlot));
	merge->normal.drio.eof_remote = true;

	ExecSetExecProcNode(pstate, ExecMergeReduce);

	OpenMergeBufFiles(merge);
	BuildMergeBinaryHeap(merge);
	return GetMergeReduceResult(merge, node);
}

static void InitMergeReduceState(ClusterReduceState *state, MergeReduceState *merge)
{
	TupleDesc		desc = state->ps.ps_ResultTupleSlot->tts_tupleDescriptor;
	ClusterReduce  *plan = castNode(ClusterReduce, state->ps.plan);
	DynamicReduceSFS sfs;
	const Oid	   *nodes;
	uint32 i,count;
	Assert(plan->numCols > 0);

	nodes = DynamicReduceGetCurrentWorkingNodes(&count);
	if (count == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Can not find working nodes")));
	}
	InitNormalReduceState(&merge->normal, DRSFSD_SIZE(count), state);
	sfs = dsm_segment_address(merge->normal.dsm_seg);
	SharedFileSetInit(&sfs->sfs, merge->normal.dsm_seg);
	sfs->nnode = count;
	memcpy(sfs->nodes, nodes, sizeof(nodes[0])*count);

	merge->nodes = palloc0(sizeof(merge->nodes[0]) * count);
	merge->nnodes = count;
	for (i=0;i<count;++i)
	{
		MergeNodeInfo *info = &merge->nodes[i];
		info->slot = ExecInitExtraTupleSlot(state->ps.state, desc);
		initStringInfo(&info->read_buf);
		info->nodeoid = nodes[i];
		if (info->nodeoid == PGXCNodeOid)
		{
			char name[MAXPGPATH];
			info->file = BufFileCreateShared(&sfs->sfs,
											 DynamicReduceSFSFileName(name, info->nodeoid));
		}
	}

	merge->binheap = binaryheap_allocate(count, cmr_heap_compare_slots, merge);
	merge->sortkeys = palloc0(sizeof(merge->sortkeys[0]) * plan->numCols);
	merge->nkeys = plan->numCols;
	for (i=0;i<merge->nkeys;++i)
	{
		SortSupport sort = &merge->sortkeys[i];
		sort->ssup_cxt = CurrentMemoryContext;
		sort->ssup_collation = plan->collations[i];
		sort->ssup_nulls_first = plan->nullsFirst[i];
		sort->ssup_attno = plan->sortColIdx[i];

		sort->abbreviate = false;

		PrepareSortSupportFromOrderingOp(plan->sortOperators[i], sort);
	}
}
static void InitMergeReduce(ClusterReduceState *crstate)
{
	MemoryContext		oldcontext;
	MergeReduceState   *merge;
	Assert(crstate->private_state == NULL);

	oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(crstate));
	merge = palloc0(sizeof(MergeReduceState));
	InitMergeReduceState(crstate, merge);
	crstate->private_state = merge;
	ExecSetExecProcNode(&crstate->ps, ExecMergeReduceFirst);

	DynamicReduceStartSharedFileSetPlan(crstate->ps.plan->plan_node_id,
										merge->normal.dsm_seg,
										dsm_segment_address(merge->normal.dsm_seg),
										crstate->ps.ps_ResultTupleSlot->tts_tupleDescriptor);

	MemoryContextSwitchTo(oldcontext);
}
static void EndMergeReduce(MergeReduceState *merge)
{
	uint32				i,count;
	MergeNodeInfo	   *info;
	DynamicReduceSFS	sfs = dsm_segment_address(merge->normal.dsm_seg);
	char				name[MAXPGPATH];

	for (i=0,count=merge->nnodes;i<count;++i)
	{
		info = &merge->nodes[i];
		if(info->file)
			BufFileClose(info->file);
		BufFileDeleteShared(&sfs->sfs, DynamicReduceSFSFileName(name, info->nodeoid));
	}
	EndNormalReduce(&merge->normal);
	pfree(merge->sortkeys);
}
#define DriveMergeReduce(node) DriveNormalReduce(node)

/* ======================================================== */
static void InitReduceMethod(ClusterReduceState *crstate)
{
	Assert(crstate->private_state == NULL);
	switch(crstate->reduce_method)
	{
	case RT_NOTHING:
		ExecSetExecProcNode(&crstate->ps, ExecNothingReduce);
		break;
	case RT_NORMAL:
		InitNormalReduce(crstate);
		break;
	case RT_MERGE:
		InitMergeReduce(crstate);
		break;
	default:
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("unknown reduce method %u", crstate->reduce_method)));
		break;
	}
	Assert(crstate->private_state != NULL ||
		   crstate->reduce_method == RT_NOTHING);
}
static TupleTableSlot* ExecDefaultClusterReduce(PlanState *pstate)
{
	ClusterReduceState *crstate = castNode(ClusterReduceState, pstate);
	if (crstate->private_state != NULL)
		return pstate->ExecProcNodeReal(pstate);

	InitReduceMethod(crstate);

	return pstate->ExecProcNodeReal(pstate);
}

ClusterReduceState *
ExecInitClusterReduce(ClusterReduce *node, EState *estate, int eflags)
{
	ClusterReduceState *crstate;
	Plan			   *outerPlan;
	TupleDesc			tupDesc;

	Assert(outerPlan(node) != NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create state structure
	 */
	crstate = makeNode(ClusterReduceState);
	crstate->ps.plan = (Plan*)node;
	crstate->ps.state = estate;
	crstate->ps.ExecProcNode = ExecDefaultClusterReduce;
	if (list_member_oid(node->reduce_oids, PGXCNodeOid) == false)
		crstate->reduce_method = (uint8)RT_NOTHING;
	else if (node->numCols > 0)
		crstate->reduce_method = (uint8)RT_MERGE;
	else
		crstate->reduce_method = (uint8)RT_NORMAL;

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

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &crstate->ps);

	Assert(OidIsValid(PGXCNodeOid));

	/*
	 * Initialize result slot, type and projection.
	 */
	ExecInitResultTupleSlotTL(estate, &crstate->ps);

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

	estate->es_reduce_plan_inited = true;

	return crstate;
}

/*
 * Compare the tuples in the two given slots.
 */
static int
cmr_heap_compare_slots(Datum a, Datum b, void *arg)
{
	MergeReduceState   *merge = (MergeReduceState*)arg;
	TupleTableSlot	   *s1 = merge->nodes[DatumGetUInt32(a)].slot;
	TupleTableSlot	   *s2 = merge->nodes[DatumGetUInt32(b)].slot;
	uint32				nkeys = merge->nkeys;
	uint32				nkey;

	Assert(!TupIsNull(s1));
	Assert(!TupIsNull(s2));

	for (nkey = 0; nkey < nkeys; nkey++)
	{
		SortSupport sortKey = &merge->sortkeys[nkey];
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

void
ExecEndClusterReduce(ClusterReduceState *node)
{
	if ((node->eflags & EXEC_FLAG_EXPLAIN_ONLY) != 0)
		DriveClusterReduceState(node);

	if (node->private_state)
	{
		switch(node->reduce_method)
		{
		case RT_NORMAL:
			EndNormalReduce(node->private_state);
			break;
		case RT_MERGE:
			EndMergeReduce(node->private_state);
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("unknown reduce method %u", node->reduce_method)));
			break;
		}
		pfree(node->private_state);
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
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cluster reduce not support mark pos")));
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
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cluster reduce not support restr pos")));
}

void
ExecReScanClusterReduce(ClusterReduceState *node)
{
	/* Just return if not start yet! */
	if (node->private_state == NULL)
		return;

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cluster reduce not support rescan")));
}

static bool
DriveClusterReduceState(ClusterReduceState *node)
{
	if (node->private_state == NULL)
		InitReduceMethod(node);

	switch(node->reduce_method)
	{
	case RT_NOTHING:
		break;
	case RT_NORMAL:
		DriveNormalReduce(node);
		break;
	case RT_MERGE:
		DriveMergeReduce(node);
		break;
	default:
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("unknown reduce method %u", node->reduce_method)));
		break;
	}

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
		/*
		 * Drive all ClusterReduce to send slot, discard slot
		 * used for local.
		 */
		res = DriveClusterReduceState((ClusterReduceState *) node);
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
