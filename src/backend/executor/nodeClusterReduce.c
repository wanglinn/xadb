
#include "postgres.h"
#include "miscadmin.h"

#include "access/parallel.h"
#include "access/tuptypeconvert.h"
#include "executor/executor.h"
#include "access/htup_details.h"
#include "executor/nodeClusterReduce.h"
#include "executor/nodeCtescan.h"
#include "executor/nodeMaterial.h"
#include "executor/nodeReduceScan.h"
#include "executor/tuptable.h"
#include "lib/binaryheap.h"
#include "lib/oidbuffer.h"
#include "nodes/enum_funcs.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "pgstat.h"
#include "pgxc/pgxc.h"
#include "storage/barrier.h"
#include "storage/buffile.h"
#include "storage/shm_mq.h"
#include "storage/condition_variable.h"
#include "utils/dynamicreduce.h"
#include "utils/hsearch.h"

typedef enum ReduceType
{
	RT_NOTHING = 1,
	RT_NORMAL,
	RT_REDUCE_FIRST,
	RT_REDUCE_FIRST_EPQ,
	RT_PARALLEL_REDUCE_FIRST,
	RT_ADVANCE,
	RT_ADVANCE_PARALLEL,
	RT_MERGE
}ReduceType;

typedef struct NormalReduceState
{
	dsm_segment	   *dsm_seg;
	DynamicReduceIOBuffer
					drio;
}NormalReduceState;

typedef struct NormalReduceFirstState
{
	NormalReduceState	normal;	/* must be first */
	BufFile			   *file_local;
	BufFile			   *file_remote;
	uint32				file_no;
	bool				ready_local;
	bool				ready_remote;
}NormalReduceFirstState;

typedef struct ReduceFirstEPQState
{
	BufFile			   *file_local;
	BufFile			   *file_remote;
	TupleTypeConvert   *convert;
	TupleTableSlot	   *slot_remote;
	StringInfoData		recv_buf;
	bool				got_remote;
}ReduceFirstEPQState;

typedef struct ParallelReduceFirstState
{
	NormalReduceState			normal;	/* must be first */
	SharedFileSet			   *sfs;
	Barrier					   *barrier;
	SharedTuplestoreAccessor   *sta;
}ParallelReduceFirstState;

typedef struct AdvanceNodeInfo
{
	BufFile			   *file;
	Oid					nodeoid;
}AdvanceNodeInfo;

typedef struct AdvanceReduceState
{
	NormalReduceState	normal;
	StringInfoData		read_buf;
	uint32				nnodes;
	bool				got_remote;
	AdvanceNodeInfo	   *cur_node;
	AdvanceNodeInfo		nodes[FLEXIBLE_ARRAY_MEMBER];
}AdvanceReduceState;

#define APR_NPART			2
#define APR_BACKEND_PART	0
#define APR_REDUCE_PART		1

typedef struct AdvanceParallelSharedMemory
{
	ConditionVariable	cv;
	pg_atomic_uint32	remote_state;
	char				padding[(sizeof(ConditionVariable)+sizeof(pg_atomic_uint32))%MAXIMUM_ALIGNOF ? 
								MAXIMUM_ALIGNOF-(sizeof(ConditionVariable)+sizeof(pg_atomic_uint32))%MAXIMUM_ALIGNOF : 0];
	DynamicReduceSTSData sts;
}AdvanceParallelSharedMemory;
#define APSM_REMOTE_WAIT_GET	0
#define APSM_REMOTE_GETTING		1
#define APSM_REMOTE_GOT			2
#define APSM_REMOTE_FAILED		3

typedef struct AdvanceParallelNode
{
	SharedTuplestoreAccessor
					   *accessor;
	Oid					nodeoid;
}AdvanceParallelNode;

typedef struct AdvanceParallelState
{
	NormalReduceState	normal;
	uint32				nnodes;
	AdvanceParallelSharedMemory
					   *shm;
	AdvanceParallelNode *cur_node;
	AdvanceParallelNode	nodes[FLEXIBLE_ARRAY_MEMBER];
}AdvanceParallelState;

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
static bool DriveClusterReduceWalker(PlanState *node);
static bool IsThereClusterReduce(PlanState *node);
static void OnDsmDatchShutdownReduce(dsm_segment *seg, Datum arg);

static inline void WaitFetchEOFMessage(DynamicReduceIOBuffer *io, int plan_id, ReduceType type)
{
	uint8 flags = DynamicReduceRecvTuple(io->mqh_receiver,
										 io->slot_local,
										 &io->recv_buf,
										 NULL,
										 false);
	if (flags != DR_MSG_RECV || !TupIsNull(io->slot_local))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("got message %d and slot empty is %d", flags, TupIsNull(io->slot_local)),
				 errdetail("plan %d method %d expect %d and slot is empty", plan_id, type, DR_MSG_RECV)));
	}
	io->eof_remote = true;
}

static List *GetReduceOids(ClusterReduce *cr)
{
	ListCell   *lc;
	List	   *list = NIL;
	foreach (lc, cr->reduce_oids)
		list = list_append_unique_oid(list, lfirst_oid(lc));
	return list;
}

#define FreeReduceOids(list_) list_free(list_)

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
	Assert(normal != NULL);
	Assert(node->reduce_method == RT_NORMAL ||
		   node->reduce_method == RT_REDUCE_FIRST);

	return DynamicReduceFetchSlot(&normal->drio);
}

static TupleTableSlot* ExecParallelReduceAttach(PlanState *pstate)
{
	/* notify attach */
	NormalReduceState  *normal = castNode(ClusterReduceState, pstate)->private_state;
	DynamicReduceAttachPallel(&normal->drio);
	ExecSetExecProcNode(pstate, ExecNormalReduce);
	if (normal->drio.eof_local == false)
	{
		TupleTableSlot *slot = DynamicReduceFetchLocal(&normal->drio);
		if (!TupIsNull(slot))
			return slot;
		if (normal->drio.eof_local == false)
			return ExecNormalReduce(pstate);
	}

	Assert(normal->drio.eof_local);
	/*
	 * when first execute in parallel and local is end, dynamic reduce maybe
	 * end of MQ receive, so send EOF message maybe get detached result,
	 * so we need ignore send EOF message result
	 */
	if (normal->drio.send_buf.len == 0)
		SerializeEndOfPlanMessage(&normal->drio.send_buf);
	shm_mq_send(normal->drio.mqh_sender,
				normal->drio.send_buf.len,
				normal->drio.send_buf.data,
				false);
	normal->drio.send_buf.len = 0;

	return ExecNormalReduce(pstate);
}

static TupleTableSlot* ExecReduceFetchLocal(void *pstate, ExprContext *econtext)
{
	TupleTableSlot *slot = ExecProcNode(pstate);
	econtext->ecxt_outertuple = slot;
	return slot;
}

static void SetupNormalReduceState(NormalReduceState *normal, DynamicReduceMQ drmq,
								   ClusterReduceState *crstate, uint32 flags)
{
	Expr			   *expr;
	ClusterReduce	   *plan;

	DynamicReduceInitFetch(&normal->drio,
						   normal->dsm_seg,
						   crstate->ps.ps_ResultTupleSlot->tts_tupleDescriptor,
						   flags,
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

static void InitNormalReduceState(NormalReduceState *normal, Size shm_size, ClusterReduceState *crstate)
{
	normal->dsm_seg = dsm_create(shm_size, 0);
	SetupNormalReduceState(normal, dsm_segment_address(normal->dsm_seg), crstate, DR_MQ_INIT_ATTACH_SEND_RECV);
}

static void InitNormalReduce(ClusterReduceState *crstate)
{
	MemoryContext		oldcontext;
	NormalReduceState  *normal;
	ClusterReduce	   *plan = castNode(ClusterReduce, crstate->ps.plan);
	Assert(crstate->private_state == NULL);

	oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(crstate));
	normal = palloc0(sizeof(NormalReduceState));
	InitNormalReduceState(normal, sizeof(DynamicReduceMQData), crstate);
	crstate->private_state = normal;
	ExecSetExecProcNode(&crstate->ps, ExecNormalReduce);
	DynamicReduceStartNormalPlan(crstate->ps.plan->plan_node_id, 
								 normal->dsm_seg,
								 dsm_segment_address(normal->dsm_seg),
								 plan->reduce_oids,
								 plan->reduce_flags & CRF_DISK_UNNECESSARY ? DR_CACHE_ON_DISK_DO_NOT:DR_CACHE_ON_DISK_AUTO);
	MemoryContextSwitchTo(oldcontext);
}

static void InitParallelReduce(ClusterReduceState *crstate, ParallelContext *pcxt)
{
	MemoryContext		oldcontext;
	NormalReduceState  *normal;
	DynamicReduceMQ		drmq;
	ClusterReduce	   *plan = castNode(ClusterReduce, crstate->ps.plan);
	char			   *addr;
	int					i;
	Assert(crstate->private_state == NULL);

	addr = shm_toc_allocate(pcxt->toc, sizeof(Size) + (pcxt->nworkers+1) * sizeof(DynamicReduceMQData));
	*(Size*)addr = RT_NORMAL;
	drmq = (DynamicReduceMQ)(addr + sizeof(Size));
	for(i=0;i<=pcxt->nworkers;++i)
	{
		shm_mq_create(drmq[i].reduce_sender_mq, sizeof(drmq->reduce_sender_mq));
		shm_mq_create(drmq[i].worker_sender_mq, sizeof(drmq->worker_sender_mq));
	}
	shm_toc_insert(pcxt->toc, crstate->ps.plan->plan_node_id, addr);

	oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(crstate));
	normal = palloc0(sizeof(NormalReduceState));
	SetupNormalReduceState(normal, drmq, crstate, DR_MQ_ATTACH_SEND_RECV);
	crstate->private_state = normal;
	ExecSetExecProcNode(&crstate->ps, ExecParallelReduceAttach);
	DynamicReduceStartParallelPlan(crstate->ps.plan->plan_node_id,
								   pcxt->seg,
								   drmq,
								   plan->reduce_oids,
								   pcxt->nworkers+1,
								   plan->reduce_flags & CRF_DISK_UNNECESSARY ? DR_CACHE_ON_DISK_DO_NOT:DR_CACHE_ON_DISK_AUTO);
	MemoryContextSwitchTo(oldcontext);
	on_dsm_detach(pcxt->seg, OnDsmDatchShutdownReduce, PointerGetDatum(crstate));
}

static void InitParallelReduceWorker(ClusterReduceState *crstate, ParallelWorkerContext *pwcxt, char *addr)
{
	MemoryContext		oldcontext;
	NormalReduceState  *normal;
	DynamicReduceMQ		drmq;
	Assert(crstate->private_state == NULL);

	drmq = (DynamicReduceMQ)(addr);
	drmq = &drmq[ParallelWorkerNumber+1];

	oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(crstate));
	normal = palloc0(sizeof(NormalReduceState));
	SetupNormalReduceState(normal, drmq, crstate, DR_MQ_ATTACH_SEND_RECV);
	crstate->private_state = normal;
	ExecSetExecProcNode(&crstate->ps, ExecParallelReduceAttach);
	MemoryContextSwitchTo(oldcontext);
}
static inline void EstimateNormalReduce(ParallelContext *pcxt)
{
	shm_toc_estimate_chunk(&pcxt->estimator,
						   sizeof(Size) + (pcxt->nworkers+1) * sizeof(DynamicReduceMQData));
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}
static void EndNormalReduce(NormalReduceState *normal)
{
	DynamicReduceClearFetch(&normal->drio);
	if (normal->dsm_seg)
		dsm_detach(normal->dsm_seg);
}
static void DriveNormalReduce(ClusterReduceState *node)
{
	TupleTableSlot	   *slot;
	NormalReduceState  *normal = node->private_state;

	if (normal->dsm_seg == NULL &&				/* pallel */
		normal->drio.called_attach == false)	/* not attached */
	{
		DynamicReduceAttachPallel(&normal->drio);
		if (normal->drio.eof_local == false)
			DynamicReduceFetchLocal(&normal->drio);
		if (normal->drio.eof_local)
		{
			/* send EOF message, like function ExecParallelReduceAttach */
			if (normal->drio.send_buf.len == 0)
				SerializeEndOfPlanMessage(&normal->drio.send_buf);
			shm_mq_send(normal->drio.mqh_sender,
						normal->drio.send_buf.len,
						normal->drio.send_buf.data,
						false);
			normal->drio.send_buf.len = 0;
		}
	}

	if (normal->drio.eof_local == false ||
		normal->drio.eof_remote == false ||
		normal->drio.send_buf.len > 0)
	{
		do
		{
			CHECK_FOR_INTERRUPTS();
			slot = DynamicReduceFetchSlot(&normal->drio);
		}while(!TupIsNull(slot));
	}
}

/* ===================== Normal Reduce First ========================== */
static TupleTableSlot* ExecReduceFirstRemote(PlanState *pstate)
{
	NormalReduceFirstState *state = castNode(ClusterReduceState, pstate)->private_state;

	if (state->file_remote == NULL)
		return ExecClearTuple(pstate->ps_ResultTupleSlot);
	return DynamicReduceFetchBufFile(&state->normal.drio, state->file_remote);
}

static TupleTableSlot* ExecReduceFirstRemoteEPQConvert(PlanState *pstate)
{
	ReduceFirstEPQState *state = castNode(ClusterReduceState, pstate)->private_state;
	resetStringInfo(&state->recv_buf);
	DynamicReduceReadSFSTuple(state->slot_remote, state->file_remote, &state->recv_buf);
	if (unlikely(TupIsNull(state->slot_remote)))
		return ExecClearTuple(pstate->ps_ResultTupleSlot);

	return do_type_convert_slot_in(state->convert,
								   state->slot_remote,
								   pstate->ps_ResultTupleSlot,
								   false);
}

static TupleTableSlot* ExecReduceFirstRemoteEPQ(PlanState *pstate)
{
	ReduceFirstEPQState *state = castNode(ClusterReduceState, pstate)->private_state;
	resetStringInfo(&state->recv_buf);
	return DynamicReduceReadSFSTuple(pstate->ps_ResultTupleSlot, state->file_remote, &state->recv_buf);
}

static void ExecReduceFirstGetRemote(NormalReduceFirstState *state, DynamicReduceRecvInfo *info, PlanState *pstate)
{
	char name[MAXPGPATH];
	MemoryContext oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(state));
	Assert(state->ready_remote == false);
	state->file_remote = BufFileOpenShared(DynamicReduceGetSharedFileSet(),
										   DynamicReduceSharedFileName(name, info->u32));
	state->file_no = info->u32;
	state->ready_remote = true;
	MemoryContextSwitchTo(oldcontext);
	/* should get EOF message */
	WaitFetchEOFMessage(&state->normal.drio, pstate->plan->plan_node_id, RT_REDUCE_FIRST);
	ExecSetExecProcNode(pstate, ExecReduceFirstRemote);
}

static inline void ExecReduceFirstEndRemote(NormalReduceFirstState *state, PlanState *pstate)
{
	Assert(state->ready_remote == false);
	state->ready_remote = true;
	state->normal.drio.eof_remote = true;
	ExecSetExecProcNode(pstate, ExecReduceFirstRemote);
}

static TupleTableSlot* ExecReduceFirstWaitRemote(PlanState *pstate)
{
	NormalReduceFirstState *state = castNode(ClusterReduceState, pstate)->private_state;
	TupleTableSlot		   *slot;
	DynamicReduceRecvInfo	info;
	int						flags;
	Assert(state->ready_remote == false && state->file_remote == NULL);

	resetStringInfo(&state->normal.drio.recv_buf);
	if (state->normal.drio.convert)
		slot = state->normal.drio.slot_remote;
	else
		slot = state->normal.drio.slot_local;
	flags = DynamicReduceRecvTuple(state->normal.drio.mqh_receiver,
								   slot,
								   &state->normal.drio.recv_buf,
								   &info,
								   false);
	if (flags == DR_MSG_RECV)
	{
		if (TupIsNull(slot))
		{
			/* end of remote, and no cached tuple */
			ExecReduceFirstEndRemote(state, pstate);
			return ExecClearTuple(pstate->ps_ResultTupleSlot);
		}
		if (unlikely(castNode(ClusterReduce, pstate->plan)->reduce_flags & CRF_MAYBE_EPQ))
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("reduce first reduce get a tuple, should not get a tuple")));
		}
		if (state->normal.drio.convert)
			slot = do_type_convert_slot_in(state->normal.drio.convert,
										   slot,
										   state->normal.drio.slot_local,
										   false);
		if (castNode(ClusterReduce, pstate->plan)->reduce_flags & CRF_DISK_ALWAYS)
			DynamicReduceWriteSFSTuple(slot, state->file_local);
		return slot;
	}else if (flags == DR_MSG_RECV_SF)
	{
		ExecReduceFirstGetRemote(state, &info, pstate);
		Assert(pstate->ExecProcNodeReal == ExecReduceFirstRemote);
		return ExecReduceFirstRemote(pstate);
	}else if (flags == DR_MSG_RECV_STS)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("reduce first reduce get a sharedtuple, should got a shared file")));
	}else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("reduce first reduce got unknown message from dynamic reduce %u", flags)));
	}

	/* keep compiler quiet */
	return ExecClearTuple(pstate->ps_ResultTupleSlot);
}

static TupleTableSlot* ExecReduceFirstLocal(PlanState *pstate)
{
	NormalReduceFirstState *state = castNode(ClusterReduceState, pstate)->private_state;
	TupleTableSlot		   *slot;

	resetStringInfo(&state->normal.drio.recv_buf);
	slot = DynamicReduceReadSFSTuple(pstate->ps_ResultTupleSlot,
									 state->file_local,
									 &state->normal.drio.recv_buf);
	if (!TupIsNull(slot))
		return slot;

	if (state->ready_remote == false)
	{
		ExecSetExecProcNode(pstate, ExecReduceFirstWaitRemote);
		return ExecReduceFirstWaitRemote(pstate);
	}

	if (state->file_remote &&
		BufFileSeek(state->file_remote, 0, 0, SEEK_SET) != 0)
	{
		ereport(ERROR,
			(errcode_for_file_access(),
			 errmsg("can not seek buffer file to head")));
	}
	ExecSetExecProcNode(pstate, ExecReduceFirstRemote);
	return ExecReduceFirstRemote(pstate);
}

static TupleTableSlot* ExecReduceFirstEPQLocal(PlanState *pstate)
{
	ReduceFirstEPQState *state = castNode(ClusterReduceState, pstate)->private_state;
	TupleTableSlot *slot = pstate->ps_ResultTupleSlot;

	resetStringInfo(&state->recv_buf);
	DynamicReduceReadSFSTuple(slot, state->file_local, &state->recv_buf);
	if (unlikely(TupIsNull(slot)))
	{
		ClusterReduceState	   *origin_pstate = ((ClusterReduceState*)pstate)->origin_state;
		NormalReduceFirstState *origin_state = origin_pstate->private_state;
		DynamicReduceRecvInfo	info;
		int						flags;

		if (state->got_remote == false)
		{
			if (origin_state->ready_remote == false)
			{
				resetStringInfo(&state->recv_buf);
				flags = DynamicReduceRecvTuple(origin_state->normal.drio.mqh_receiver,
											   pstate->ps_ResultTupleSlot,
											   &state->recv_buf,	/* don't use origin buffer, maybe in using */
											   &info,
											   false);
				if (flags == DR_MSG_RECV_SF)
				{
					if(likely(TupIsNull(pstate->ps_ResultTupleSlot)))
						ExecReduceFirstEndRemote(origin_state, &origin_pstate->ps);
					else
						ereport(ERROR,
								(errcode(ERRCODE_INTERNAL_ERROR),
								 errmsg("origin reduce first reduce get a tuple, should not get a tuple")));
				}else if (flags == DR_MSG_RECV_SF)
				{
					ExecReduceFirstGetRemote(origin_state, &info, &origin_pstate->ps);
				}else if (flags == DR_MSG_RECV_STS)
				{
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("reduce first reduce get a sharedtuple, should got a shared file")));
				}else
				{
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("reduce first reduce got unknown message from dynamic reduce %u", flags)));
				}
			}

			Assert(origin_state->ready_remote);
			if (origin_state->file_remote)
			{
				char name[MAXPGPATH];
				MemoryContext oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(state));
				state->file_remote = BufFileOpenShared(DynamicReduceGetSharedFileSet(),
													   DynamicReduceSharedFileName(name, origin_state->file_no));
				MemoryContextSwitchTo(oldcontext);
			}
			state->got_remote = true;
		}
		Assert(state->got_remote);
		if (state->file_remote == NULL)
		{
			ExecSetExecProcNode(pstate, ExecNothingReduce);
			return ExecClearTuple(pstate->ps_ResultTupleSlot);
		}

		if (BufFileSeek(state->file_remote, 0, 0, SEEK_SET) != 0)
			ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("can not seek buffer file to head")));

		if (state->convert)
		{
			ExecSetExecProcNode(pstate, ExecReduceFirstRemoteEPQConvert);
			slot = ExecReduceFirstRemoteEPQConvert(pstate);
		}else
		{
			ExecSetExecProcNode(pstate, ExecReduceFirstRemoteEPQ);
			slot = ExecReduceFirstRemoteEPQ(pstate);
		}
	}

	return slot;
}

static inline char* GetReduceFirstSFName(char *name, int plan_id)
{
	sprintf(name, "rf_plan_%d", plan_id);
	return name;
}

static void ExecReduceFirstSend(NormalReduceFirstState *state, int plan_id, bool seek_to_head)
{
	MemoryContext	oldcontext;
	char			name[MAXPGPATH];

	Assert(state->normal.drio.eof_local == false);
	Assert(state->ready_local == false && state->file_local == NULL);

	/* create local file */
	oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(state));
	state->file_local = BufFileCreateShared(DynamicReduceGetSharedFileSet(),
											GetReduceFirstSFName(name, plan_id));
	MemoryContextSwitchTo(oldcontext);

	DynamicReduceFetchAllLocalAndSend(&state->normal.drio,
									  state->file_local,
									  DRFetchSaveSFS);
	BufFileExportShared(state->file_local);
	state->ready_local = true;

	if (seek_to_head &&
		BufFileSeek(state->file_local, 0, 0, SEEK_SET) != 0)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("can not seek buffer file to head")));
	}
}

static TupleTableSlot* ExecReduceFirstPrepare(PlanState *pstate)
{
	NormalReduceFirstState *state = castNode(ClusterReduceState, pstate)->private_state;

	ExecReduceFirstSend(state, pstate->plan->plan_node_id, true);
	Assert(state->normal.drio.eof_local &&
		   state->ready_local &&
		   state->file_local);

	ExecSetExecProcNode(pstate, ExecReduceFirstLocal);
	return ExecReduceFirstLocal(pstate);
}

static void DriveReduceFirst(ClusterReduceState *node)
{
	NormalReduceFirstState *state = node->private_state;

	if (state->file_local == NULL)
		ExecReduceFirstSend(state, node->ps.plan->plan_node_id, false);

	/* maybe rescan this paln, so open remote data */
	while (state->ready_remote == false)
		(void)ExecReduceFirstWaitRemote(&node->ps);
}

static void InitReduceFirst(ClusterReduceState *crstate)
{
	MemoryContext			oldcontext;
	NormalReduceFirstState *state;
	ClusterReduce		   *plan = castNode(ClusterReduce, crstate->ps.plan);
	Assert(crstate->private_state == NULL);

	oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(crstate));

	crstate->private_state = state = palloc0(sizeof(*state));
	InitNormalReduceState(&state->normal, sizeof(DynamicReduceMQData), crstate);
	DynamicReduceStartNormalPlan(crstate->ps.plan->plan_node_id,
								 state->normal.dsm_seg,
								 dsm_segment_address(state->normal.dsm_seg),
								 plan->reduce_oids,
								 (plan->reduce_flags & (CRF_DISK_ALWAYS|CRF_MAYBE_EPQ)) ? DR_CACHE_ON_DISK_ALWAYS:DR_CACHE_ON_DISK_AUTO);
	ExecSetExecProcNode(&crstate->ps, ExecReduceFirstPrepare);

	MemoryContextSwitchTo(oldcontext);
}

static void EndReduceFirst(NormalReduceFirstState *state, int plan_id)
{
	char name[MAXPGPATH];
	if (state->file_local)
	{
		BufFileClose(state->file_local);
		BufFileDeleteShared(DynamicReduceGetSharedFileSet(),
							GetReduceFirstSFName(name, plan_id));
	}
	if (state->file_remote)
	{
		BufFileClose(state->file_remote);
		BufFileDeleteShared(DynamicReduceGetSharedFileSet(),
							DynamicReduceSharedFileName(name, state->file_no));
	}
	EndNormalReduce(&state->normal);
}

static void InitEPQReduceFirst(ClusterReduceState *crstate, ClusterReduceState *origin)
{
	MemoryContext			oldcontext;
	ReduceFirstEPQState	   *state;
	NormalReduceFirstState *origin_state = origin->private_state;
	char					name[MAXPGPATH];
	Assert(crstate->private_state == NULL);

	oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(crstate));

	crstate->reduce_method = RT_REDUCE_FIRST_EPQ;
	crstate->private_state = state = palloc0(sizeof(*state));
	if (origin_state->ready_local == false)
	{
		ExecReduceFirstSend(origin_state, origin->ps.plan->plan_node_id, true);
		ExecSetExecProcNode(&origin->ps, ExecReduceFirstLocal);
	}

	state->file_local = BufFileOpenShared(DynamicReduceGetSharedFileSet(),
										  GetReduceFirstSFName(name, origin->ps.plan->plan_node_id));
	initStringInfo(&state->recv_buf);
	MemSet(state->recv_buf.data, 0, state->recv_buf.maxlen);
	state->convert = create_type_convert(crstate->ps.ps_ResultTupleSlot->tts_tupleDescriptor, false, true);
	if (state->convert)
		state->slot_remote = MakeTupleTableSlot(state->convert->out_desc);
	ExecSetExecProcNode(&crstate->ps, ExecReduceFirstEPQLocal);

	MemoryContextSwitchTo(oldcontext);
}

static void EndEPQReduceFirst(ReduceFirstEPQState *state)
{
	if (state->convert)
		free_type_convert(state->convert);
	if (state->slot_remote)
		ExecDropSingleTupleTableSlot(state->slot_remote);
	if (state->recv_buf.data)
		pfree(state->recv_buf.data);
	if (state->file_remote)
		BufFileClose(state->file_remote);
	if (state->file_local)
		BufFileClose(state->file_local);
}

static TupleTableSlot* ExecParallelReduceFirstRemote(PlanState *pstate)
{
	ParallelReduceFirstState *state = castNode(ClusterReduceState, pstate)->private_state;
	return DynamicReduceFetchSTS(&state->normal.drio, state->normal.drio.sts);
}

static void ExecParallelReduceFirstWaitRemote(ParallelReduceFirstState *state, int plan_id)
{
	MemoryContext			oldcontext;
	DynamicReduceRecvInfo	info;
	uint8					flags;
	Assert(state->normal.drio.eof_remote == false);

	if (state->normal.drio.sts_dsa_ptr == InvalidDsaPointer)
	{
		flags = DynamicReduceRecvTuple(state->normal.drio.mqh_receiver,
									   state->normal.drio.slot_local,
									   &state->normal.drio.recv_buf,
									   &info,
									   false);
		if (flags == DR_MSG_RECV_STS)
		{
			oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(state));
			state->normal.drio.sts = DynamicReduceOpenSharedTuplestore(info.dp);
			state->normal.drio.sts_dsa_ptr = info.dp;
			sts_begin_parallel_scan(state->normal.drio.sts);
			MemoryContextSwitchTo(oldcontext);
		}else
		{
			ereport(ERROR,
					(errmsg("parallel reduce first %d got message %d", plan_id, flags),
					 errdetail("expect message %d", DR_MSG_RECV_STS)));
		}
	}

	if (state->normal.drio.eof_remote == false)
		WaitFetchEOFMessage(&state->normal.drio, plan_id, RT_PARALLEL_REDUCE_FIRST);
}

static TupleTableSlot* ExecParallelReduceFirstLocal(PlanState *pstate)
{
	ParallelReduceFirstState   *state = castNode(ClusterReduceState, pstate)->private_state;
	MinimalTuple				mtup = sts_parallel_scan_next(state->sta, NULL);

	if (mtup != NULL)
		return ExecStoreMinimalTuple(mtup, pstate->ps_ResultTupleSlot, false);

	if (state->normal.drio.eof_remote == false)
		ExecParallelReduceFirstWaitRemote(state, pstate->plan->plan_node_id);
	else
		sts_begin_parallel_scan(state->normal.drio.sts);
	Assert(state->normal.drio.eof_remote &&
		   state->normal.drio.sts);

	ExecSetExecProcNode(pstate, ExecParallelReduceFirstRemote);
	return ExecParallelReduceFirstRemote(pstate);
}

static void ExecParallelReduceFirstSendLocal(ParallelReduceFirstState *state)
{
	Assert(state->normal.drio.eof_local == false);
	DynamicReduceAttachPallel(&state->normal.drio);
	if (BarrierAttach(state->barrier) > 0)
	{
		/*
		 * we also need send EOF message to dynamic reduce. if not it maybe get
		 * a MQ deatched result
		 */
		if (state->normal.drio.eof_local == false)
		{
			TupleTableSlot *slot PG_USED_FOR_ASSERTS_ONLY = DynamicReduceFetchLocal(&state->normal.drio);
			Assert(TupIsNull(slot));	/* should no more tuple */
			Assert(state->normal.drio.eof_local == true);	/* should set eof_local */
			Assert(state->normal.drio.send_buf.len > 0);	/* and should have an eof message */
		}else
		{
			SerializeEndOfPlanMessage(&state->normal.drio.send_buf);
			Assert(state->normal.drio.send_buf.len > 0);
		}

		/* 
		 * here we don't use DynamicReduceSendMessage function,
		 * because dynamic reduce MQ maybe deatched and DynamicReduceSendMessage
		 * report an error.
		 * here it's OK for dynamic reduce deatch
		 */
		shm_mq_send(state->normal.drio.mqh_sender,
					state->normal.drio.send_buf.len,
					state->normal.drio.send_buf.data,
					false);
		state->normal.drio.send_buf.len = 0;
	}else
	{
		/* save local and send to remote */
		DynamicReduceFetchAllLocalAndSend(&state->normal.drio,
										  state->sta,
										  DRFetchSaveSTS);
		sts_end_write(state->sta);
		BarrierArriveAndWait(state->barrier, WAIT_EVENT_DATA_FILE_READ);
	}
	Assert(state->normal.drio.eof_local == true);

	BarrierDetach(state->barrier);
	sts_begin_parallel_scan(state->sta);
}

static TupleTableSlot* ExecParallelReduceFirstPrepare(PlanState *pstate)
{
	ExecParallelReduceFirstSendLocal(castNode(ClusterReduceState, pstate)->private_state);
	ExecSetExecProcNode(pstate, ExecParallelReduceFirstLocal);
	return ExecParallelReduceFirstLocal(pstate);
}

static void DriveParallelReduceFirst(ClusterReduceState *node)
{
	ParallelReduceFirstState   *state = node->private_state;

	if (state->normal.drio.eof_local == false)
	{
		resetStringInfo(&state->normal.drio.send_buf);
		DynamicReduceAttachPallel(&state->normal.drio);
		if (BarrierAttach(state->barrier) == 0)
		{
			/* send to remote and save local */
			DynamicReduceFetchAllLocalAndSend(&state->normal.drio,
											  state->sta,
											  DRFetchSaveSTS);
			Assert(state->normal.drio.eof_local == true);
			Assert(state->normal.drio.send_buf.len == 0);
			sts_end_write(state->sta);
			/* don't need wait */
			BarrierArriveAndDetach(state->barrier);
		}else
		{
			SerializeEndOfPlanMessage(&state->normal.drio.send_buf);
			shm_mq_send(state->normal.drio.mqh_sender,
						state->normal.drio.send_buf.len,
						state->normal.drio.send_buf.data,
						false);
			BarrierDetach(state->barrier);
			state->normal.drio.send_buf.len = 0;
			state->normal.drio.eof_local = true;
		}
	}

	/* wait remote if not got EOF */
	if (state->normal.drio.eof_remote == false)
		ExecParallelReduceFirstWaitRemote(state, node->ps.plan->plan_node_id);
}

static Size GetReduceFirstShmSize(ParallelContext *pcxt)
{
	Size size = sizeof(Size) * 2;		/* reduce method and shared tuplestore size */
	size = add_size(size, MAXALIGN(SIZEOF_DSA_POINTER));				/* remote dsa pointer */
	size = add_size(size, MAXALIGN(sizeof(SharedFileSet)));				/* shared fileset */
	size = add_size(size, MAXALIGN(sizeof(Barrier)));					/* barrier */
	size = add_size(size, MAXALIGN(sts_estimate(pcxt->nworkers + 1)));	/* shared tuplestore */
	size = add_size(size, (pcxt->nworkers+1)*sizeof(DynamicReduceMQData));	/* MQ */

	return size;
}

static void EstimateReduceFirst(ParallelContext *pcxt)
{
	shm_toc_estimate_chunk(&pcxt->estimator, GetReduceFirstShmSize(pcxt));
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

static void ReinitParallelReduceFirst(ClusterReduceState *node, ParallelContext *pcxt)
{
	ParallelReduceFirstState   *state = node->private_state;
	char					   *addr;

	if (state->normal.drio.eof_local == false)
		ExecParallelReduceFirstSendLocal(state);
	if (state->normal.drio.eof_remote == false)
		ExecParallelReduceFirstWaitRemote(state, node->ps.plan->plan_node_id);
	Assert(state->normal.drio.eof_local &&
		   state->normal.drio.eof_remote);
	Assert(DsaPointerIsValid(state->normal.drio.sts_dsa_ptr));

	addr = shm_toc_lookup(pcxt->toc, node->ps.plan->plan_node_id, false);
	Assert(*(Size*)addr == RT_PARALLEL_REDUCE_FIRST);	/* reduce method */
	addr += sizeof(Size)*2;				/* skip reduce method and shared tuplestore size */

	/* save dsa pointer to shared memory, let worker now */
	*(dsa_pointer*)addr = state->normal.drio.sts_dsa_ptr;

	sts_reinitialize(state->sta);
	sts_begin_parallel_scan(state->sta);
	sts_reinitialize(state->normal.drio.sts);

	ExecSetExecProcNode(&node->ps, ExecParallelReduceFirstLocal);
}

static void InitParallelReduceFirstCommon(ClusterReduceState *node, ParallelContext *pcxt,
										  ParallelWorkerContext *pwcxt, char *addr)
{
	MemoryContext				oldcontext;
	ParallelReduceFirstState   *state;
	Size						sts_size;
	dsa_pointer					dp;
	DynamicReduceMQ				drmq;
	Assert(node->private_state == NULL);

	/* shared tuplestore size */
	if (pcxt)
		*(Size*)addr = sts_size = MAXALIGN(sts_estimate(pcxt->nworkers+1));
	else
		sts_size = *(Size*)addr;
	addr += sizeof(Size);

	/* dsa of remote sharedtuplestore */
	if (pcxt)
		*(dsa_pointer*)addr = dp = InvalidDsaPointer;
	else
		dp = *(dsa_pointer*)addr;
	addr += MAXALIGN(SIZEOF_DSA_POINTER);

	oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(node));
	node->private_state = state = palloc0(sizeof(*state));

	/* shared fileset */
	state->sfs = (SharedFileSet*)addr;
	addr += MAXALIGN(sizeof(SharedFileSet));
	if (pcxt)
		SharedFileSetInit(state->sfs, pcxt->seg);
	else
		SharedFileSetAttach(state->sfs, pwcxt->seg);

	/* barrier */
	state->barrier = (Barrier*)addr;
	addr += MAXALIGN(sizeof(Barrier));
	if (pcxt)
		BarrierInit(state->barrier, 0);

	/* shared tuplestore */
	if (pcxt)
		state->sta = sts_initialize((SharedTuplestore*)addr,
									pcxt->nworkers+1,
									ParallelWorkerNumber+1,
									0,
									0,
									state->sfs,
									"prf");
	else
		state->sta = sts_attach((SharedTuplestore*)addr,
								ParallelWorkerNumber+1,
								state->sfs);
	addr += sts_size;

	/* MQ */
	drmq = (DynamicReduceMQ)addr;
	if (pcxt)
	{
		int i;
		for (i=0;i<=pcxt->nworkers;++i)
		{
			shm_mq_create(drmq[i].reduce_sender_mq, sizeof(drmq->reduce_sender_mq));
			shm_mq_create(drmq[i].worker_sender_mq, sizeof(drmq->worker_sender_mq));
		}

		SetupNormalReduceState(&state->normal, drmq, node, DR_MQ_ATTACH_SEND_RECV);
		DynamicReduceStartParallelPlan(node->ps.plan->plan_node_id,
									   pcxt->seg,
									   drmq,
									   castNode(ClusterReduce, node->ps.plan)->reduce_oids,
									   pcxt->nworkers+1,
									   DR_CACHE_ON_DISK_ALWAYS);
		on_dsm_detach(pcxt->seg, OnDsmDatchShutdownReduce, PointerGetDatum(node));
	}else
	{
		SetupNormalReduceState(&state->normal, &drmq[ParallelWorkerNumber+1], node, DR_MQ_ATTACH_SEND_RECV);
		if (DsaPointerIsValid(dp))
		{
			/* rescan, got remote */
			sts_begin_parallel_scan(state->sta);
			state->normal.drio.eof_local = state->normal.drio.eof_remote = true;
			state->normal.drio.sts = DynamicReduceOpenSharedTuplestore(dp);
			state->normal.drio.sts_dsa_ptr = dp;
			sts_begin_parallel_scan(state->normal.drio.sts);
		}
	}
	if (pwcxt && DsaPointerIsValid(dp))
		ExecSetExecProcNode(&node->ps, ExecParallelReduceFirstLocal);
	else
		ExecSetExecProcNode(&node->ps, ExecParallelReduceFirstPrepare);

	MemoryContextSwitchTo(oldcontext);
}

static void InitParallelReduceFirst(ClusterReduceState *node, ParallelContext *pcxt)
{
	char					   *addr;

	addr = shm_toc_allocate(pcxt->toc, GetReduceFirstShmSize(pcxt));
	shm_toc_insert(pcxt->toc, node->ps.plan->plan_node_id, addr);

	/* reduce method */
	node->reduce_method = RT_PARALLEL_REDUCE_FIRST;
	*(Size*)addr = RT_PARALLEL_REDUCE_FIRST;
	addr += sizeof(Size);

	InitParallelReduceFirstCommon(node, pcxt, NULL, addr);
}

static void EndParallelReduceFirst(ParallelReduceFirstState *state)
{
	sts_detach(state->sta);
	EndNormalReduce(&state->normal);
}

/* ========================= advance reduce ========================= */
static void ExecAdvanceReduceWaitRemote(ClusterReduceState *node, AdvanceReduceState *state)
{
	char name[MAXPGPATH];
	MemoryContext oldcontext;
	AdvanceNodeInfo *info;
	DynamicReduceSFS sfs;
	uint32 i;
	uint8 flag PG_USED_FOR_ASSERTS_ONLY;
	Assert(state->normal.drio.eof_remote == false);

	/* wait dynamic reduce end of plan */
	flag = DynamicReduceRecvTuple(state->normal.drio.mqh_receiver,
								  state->normal.drio.slot_local,
								  &state->normal.drio.recv_buf,
								  NULL,
								  false);
	Assert(flag == DR_MSG_RECV && TupIsNull(state->normal.drio.slot_local));
	state->got_remote = true;

	/* open remote SFS files */
	sfs = dsm_segment_address(state->normal.dsm_seg);
	oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(state));
	for (i=0;i<state->nnodes;++i)
	{
		info = &state->nodes[i];
		if (info->file == NULL)
		{
			info->file = BufFileOpenShared(&sfs->sfs,
										   DynamicReduceSFSFileName(name, info->nodeoid));
		}else
		{
			Assert(info->nodeoid == PGXCNodeOid);
		}
	}
	MemoryContextSwitchTo(oldcontext);
}

static TupleTableSlot *ExecAdvanceReduce(PlanState *pstate)
{
	AdvanceReduceState *state = castNode(ClusterReduceState, pstate)->private_state;
	AdvanceNodeInfo *cur_info = state->cur_node;
	TupleTableSlot *slot;

re_get_:
	if (state->normal.drio.convert &&
		cur_info->nodeoid != PGXCNodeOid)
		slot = DynamicReduceFetchBufFile(&state->normal.drio, cur_info->file);
	else
		slot = DynamicReduceReadSFSTuple(pstate->ps_ResultTupleSlot, cur_info->file, &state->read_buf);
	if (TupIsNull(slot))
	{
		if (cur_info->nodeoid == PGXCNodeOid &&
			state->got_remote == false)
			ExecAdvanceReduceWaitRemote((ClusterReduceState*)pstate, state);

		/* next node */
		cur_info = &cur_info[1];
		if (cur_info >= &state->nodes[state->nnodes])
			cur_info = state->nodes;
		if (cur_info->nodeoid != PGXCNodeOid)
			goto re_get_;
	}

	return slot;
}

static void BeginAdvanceReduce(ClusterReduceState *crstate)
{
	MemoryContext		oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(crstate));
	AdvanceReduceState *state;
	DynamicReduceSFS	sfs;
	AdvanceNodeInfo	   *myinfo;
	List			   *reduce_oids;
	ListCell		   *lc;
	uint32 				i,count;

	reduce_oids = GetReduceOids(castNode(ClusterReduce, crstate->ps.plan));
	count = list_length(reduce_oids);
	if (count == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Can not find working nodes")));
	}

	state = palloc0(offsetof(AdvanceReduceState, nodes) + sizeof(state->nodes[0]) * count);
	crstate->private_state = state;
	crstate->reduce_method = RT_ADVANCE;
	state->nnodes = count;
	initStringInfo(&state->read_buf);
	InitNormalReduceState(&state->normal, sizeof(*sfs), crstate);
	sfs = dsm_segment_address(state->normal.dsm_seg);
	SharedFileSetInit(&sfs->sfs, state->normal.dsm_seg);

	myinfo = NULL;
	lc = list_head(reduce_oids);
	for(i=0;i<count;++i)
	{
		AdvanceNodeInfo *info = &state->nodes[i];
		info->nodeoid = lfirst_oid(lc);
		lc = lnext(lc);
		if (info->nodeoid == PGXCNodeOid)
		{
			char name[MAXPGPATH];
			info->file = BufFileCreateShared(&sfs->sfs,
											 DynamicReduceSFSFileName(name, info->nodeoid));
			Assert(myinfo == NULL);
			myinfo = info;
		}
	}
	Assert(myinfo != NULL);

	DynamicReduceStartSharedFileSetPlan(crstate->ps.plan->plan_node_id,
										state->normal.dsm_seg,
										dsm_segment_address(state->normal.dsm_seg),
										reduce_oids);

	DynamicReduceFetchAllLocalAndSend(&state->normal.drio,
									  myinfo->file,
									  DRFetchSaveSFS);
	if (BufFileSeek(myinfo->file, 0, 0, SEEK_SET) != 0)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("can not seek SFS file to head")));
	}
	state->cur_node = myinfo;

	ExecSetExecProcNode(&crstate->ps, ExecAdvanceReduce);

	MemoryContextSwitchTo(oldcontext);
	FreeReduceOids(reduce_oids);
}

static void EndAdvanceReduce(AdvanceReduceState *state, ClusterReduceState *crs)
{
	uint32				i,count;
	AdvanceNodeInfo	   *info;
	DynamicReduceSFS	sfs = dsm_segment_address(state->normal.dsm_seg);
	char				name[MAXPGPATH];

	if (state->got_remote == false)
	{
		uint8 flag PG_USED_FOR_ASSERTS_ONLY;
		flag = DynamicReduceRecvTuple(state->normal.drio.mqh_receiver,
									  crs->ps.ps_ResultTupleSlot,
									  &state->normal.drio.recv_buf,
									  NULL,
									  false);
		Assert(flag == DR_MSG_RECV && TupIsNull(crs->ps.ps_ResultTupleSlot));
	}

	for (i=0,count=state->nnodes;i<count;++i)
	{
		info = &state->nodes[i];
		if(info->file)
			BufFileClose(info->file);
		BufFileDeleteShared(&sfs->sfs, DynamicReduceSFSFileName(name, info->nodeoid));
	}
	EndNormalReduce(&state->normal);
}

/* ========================= advance parallel ======================= */
static void ExecAdvanceParallelReduceWaitRemote(AdvanceParallelSharedMemory *shm, dsm_segment *dsm_seg, TupleTableSlot *slot)
{
	shm_mq_handle  *mqh;
	StringInfoData	buf;
	uint32			state;
	uint8			flag PG_USED_FOR_ASSERTS_ONLY;

	state = pg_atomic_read_u32(&shm->remote_state);
re_switch:
	CHECK_FOR_INTERRUPTS();
	switch(state)
	{
	case APSM_REMOTE_WAIT_GET:
		if(pg_atomic_compare_exchange_u32(&shm->remote_state, &state, APSM_REMOTE_GETTING) == false)
			goto re_switch;

		PG_TRY();
		{
			initStringInfo(&buf);
			shm_mq_set_receiver((shm_mq*)shm->sts.sfs.mq.reduce_sender_mq, MyProc);
			mqh = shm_mq_attach((shm_mq*)shm->sts.sfs.mq.reduce_sender_mq, dsm_seg, NULL);
			flag = DynamicReduceRecvTuple(mqh, slot, &buf, NULL, false);
			Assert(flag == DR_MSG_RECV && TupIsNull(slot));
		}PG_CATCH();
		{
			/* set remote state to failed */
			state = APSM_REMOTE_GETTING;
			flag = pg_atomic_compare_exchange_u32(&shm->remote_state, &state, APSM_REMOTE_FAILED);
			Assert(flag == true);
			ConditionVariableBroadcast(&shm->cv);
			PG_RE_THROW();
		}PG_END_TRY();

		state = APSM_REMOTE_GETTING;
		flag = pg_atomic_compare_exchange_u32(&shm->remote_state, &state, APSM_REMOTE_GOT);
		Assert(flag == true);
		ConditionVariableBroadcast(&shm->cv);
		pfree(buf.data);
		shm_mq_detach(mqh);
		break;
	case APSM_REMOTE_GETTING:
		do
		{
			ConditionVariableSleep(&shm->cv, WAIT_EVENT_DATA_FILE_READ);
		}while ((state = pg_atomic_read_u32(&shm->remote_state)) == APSM_REMOTE_GETTING);
		ConditionVariableCancelSleep();
		goto re_switch;
	case APSM_REMOTE_GOT:
		break;
	case APSM_REMOTE_FAILED:
		ereport(ERROR,
				(errmsg("other worker set advance parallel reduce to failed")));
		break;
	default:
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("unknown advance parallel reduce remote state %u", state)));
	}
}

static TupleTableSlot* ExecAdvanceParallelReduce(PlanState *ps)
{
	AdvanceParallelState   *state = castNode(ClusterReduceState, ps)->private_state;
	AdvanceParallelNode	   *cur_node = state->cur_node;
	MinimalTuple			mtup;

re_get_:
	mtup = sts_parallel_scan_next(cur_node->accessor, NULL);
	if (mtup != NULL)
	{
		if (cur_node->nodeoid != PGXCNodeOid &&
			state->normal.drio.convert)
			return do_type_convert_slot_in(state->normal.drio.convert,
										   ExecStoreMinimalTuple(mtup, state->normal.drio.slot_remote, false),
										   state->normal.drio.slot_local,
										   false);
		return ExecStoreMinimalTuple(mtup, ps->ps_ResultTupleSlot, false);
	}

	sts_end_parallel_scan(cur_node->accessor);
	if (cur_node->nodeoid == PGXCNodeOid)
		ExecAdvanceParallelReduceWaitRemote(state->shm, state->normal.dsm_seg, ps->ps_ResultTupleSlot);

	cur_node = &cur_node[1];
	if (cur_node >= &state->nodes[state->nnodes])
		cur_node = state->nodes;
	if (cur_node->nodeoid != PGXCNodeOid)
	{
		sts_begin_parallel_scan(cur_node->accessor);
		state->cur_node = cur_node;
		goto re_get_;
	}

	return ExecClearTuple(ps->ps_ResultTupleSlot);
}

static void DriveAdvanceParallelReduce(ClusterReduceState *node)
{
	AdvanceParallelState   *state = node->private_state;
	if (!IsParallelWorker())
	{
		ExecAdvanceParallelReduceWaitRemote(state->shm,
											state->normal.dsm_seg,
											node->ps.ps_ResultTupleSlot);
	}
}

static AdvanceParallelNode* BeginAdvanceSharedTuplestore(AdvanceParallelNode *arr, DynamicReduceSTS sts,
														 List *oids, bool init)
{
	char				name[MAXPGPATH];
	ListCell		   *lc;
	SharedTuplestore   *addr;
	AdvanceParallelNode *my_node;
	uint32				i;

	Assert(list_member_oid(oids, PGXCNodeOid));

	i = 0;
	foreach (lc, oids)
	{
		if (lfirst_oid(lc) == PGXCNodeOid)
			continue;

		arr[i].nodeoid = lfirst_oid(lc);
		addr = DRSTSD_ADDR(sts->sts, APR_NPART, i);
		if (init)
		{
			arr[i].accessor = sts_initialize(addr,
											 APR_NPART,
											 APR_BACKEND_PART,
											 0,
											 SHARED_TUPLESTORE_SINGLE_PASS,
											 &sts->sfs.sfs,
											 DynamicReduceSFSFileName(name, lfirst_oid(lc)));
		}else
		{
			arr[i].accessor = sts_attach_read_only(addr, &sts->sfs.sfs);
		}
		++i;
	}

	Assert(i == list_length(oids)-1);
	my_node = &arr[i];
	my_node->nodeoid = PGXCNodeOid;
	addr = DRSTSD_ADDR(sts->sts, APR_NPART, i);
	if (init)
	{
		my_node->accessor = sts_initialize(addr,
										   APR_NPART,
										   APR_BACKEND_PART,
										   0,
										   SHARED_TUPLESTORE_SINGLE_PASS,
										   &sts->sfs.sfs,
										   DynamicReduceSFSFileName(name, PGXCNodeOid));
	}else
	{
		my_node->accessor = sts_attach_read_only(addr, &sts->sfs.sfs);
	}

	return my_node;
}

static void InitAdvanceParallelReduceWorker(ClusterReduceState *crstate, ParallelWorkerContext *pwcxt, char *addr)
{
	MemoryContext			oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(crstate));
	List				   *oid_list = GetReduceOids(castNode(ClusterReduce, crstate->ps.plan));
	AdvanceParallelState   *state;
	AdvanceParallelSharedMemory *shm;
	uint32					count;

	count = list_length(oid_list);
	state = palloc0(offsetof(AdvanceParallelState, nodes) + sizeof(state->nodes[0]) * count);
	crstate->private_state = state;
	crstate->reduce_method = RT_ADVANCE_PARALLEL;
	state->nnodes = count;
	state->normal.dsm_seg = dsm_attach(*(dsm_handle*)addr);
	state->shm = shm = dsm_segment_address(state->normal.dsm_seg);
	SharedFileSetAttach(&shm->sts.sfs.sfs, state->normal.dsm_seg);
	state->cur_node = BeginAdvanceSharedTuplestore(state->nodes, &shm->sts, oid_list, false);
	Assert(state->cur_node->nodeoid == PGXCNodeOid);
	sts_begin_parallel_scan(state->cur_node->accessor);

	ExecSetExecProcNode(&crstate->ps, ExecAdvanceParallelReduce);

	MemoryContextSwitchTo(oldcontext);
	FreeReduceOids(oid_list);
}

static void BeginAdvanceParallelReduce(ClusterReduceState *crstate)
{
	MemoryContext					oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(crstate));
	AdvanceParallelState		   *state;
	AdvanceParallelSharedMemory	   *shm;
	SharedTuplestoreAccessor	   *accessor;
	List						   *oid_list;
	uint32 							count;

	oid_list = GetReduceOids(castNode(ClusterReduce, crstate->ps.plan));
	count = list_length(oid_list);
	if (count == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Can not find working nodes")));
	}

	state = palloc0(offsetof(AdvanceParallelState, nodes) + sizeof(state->nodes[0]) * count);
	crstate->private_state = state;
	crstate->reduce_method = RT_ADVANCE_PARALLEL;
	state->nnodes = count;
	state->normal.dsm_seg = dsm_create(offsetof(AdvanceParallelSharedMemory, sts) + DRSTSD_SIZE(APR_NPART, count), 0);
	state->shm = shm = dsm_segment_address(state->normal.dsm_seg);
	SetupNormalReduceState(&state->normal, &shm->sts.sfs.mq, crstate, DR_MQ_INIT_ATTACH_SEND|DR_MQ_INIT_RECV);
	SharedFileSetInit(&shm->sts.sfs.sfs, state->normal.dsm_seg);
	state->cur_node = BeginAdvanceSharedTuplestore(state->nodes, &shm->sts, oid_list, true);
	Assert(state->cur_node->nodeoid == PGXCNodeOid);
	pg_atomic_init_u32(&shm->remote_state, APSM_REMOTE_WAIT_GET);
	ConditionVariableInit(&shm->cv);

	DynamicReduceStartSharedTuplestorePlan(crstate->ps.plan->plan_node_id,
										   state->normal.dsm_seg,
										   &shm->sts,
										   oid_list,
										   APR_NPART,
										   APR_REDUCE_PART);

	accessor = state->cur_node->accessor;
	DynamicReduceFetchAllLocalAndSend(&state->normal.drio,
									  accessor,
									  DRFetchSaveSTS);
	sts_end_write(accessor);
	sts_begin_parallel_scan(accessor);

	ExecSetExecProcNode(&crstate->ps, ExecAdvanceParallelReduce);

	MemoryContextSwitchTo(oldcontext);
	FreeReduceOids(oid_list);
}

static void EndAdvanceParallelReduce(AdvanceParallelState *state)
{
	dsm_detach(state->normal.dsm_seg);
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
	if (info->nodeoid != PGXCNodeOid &&
		merge->normal.drio.convert != NULL)
	{
		TupleTableSlot *slot = DynamicReduceReadSFSTuple(merge->normal.drio.slot_remote,
														 info->file,
														 &info->read_buf);
		do_type_convert_slot_in(merge->normal.drio.convert, slot, info->slot, false);
	}else
	{
		DynamicReduceReadSFSTuple(info->slot, info->file, &info->read_buf);
	}
	if (TupIsNull(info->slot))
		binaryheap_remove_first(merge->binheap);
	else
		binaryheap_replace_first(merge->binheap, UInt32GetDatum(i));

	return GetMergeReduceResult(merge, node);
}

static TupleTableSlot* ExecMergeReduceFirstTuple(PlanState *pstate)
{
	ClusterReduceState *crstate = castNode(ClusterReduceState, pstate);
	TupleTableSlot *slot = GetMergeReduceResult(crstate->private_state, crstate);
	if (!TupIsNull(slot))
		ExecSetExecProcNode(pstate, ExecMergeReduce);
	return slot;
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

static void OpenMergeBufFiles(MergeReduceState *merge, DynamicReduceSFS sfs)
{
	MemoryContext		oldcontext;
	MergeNodeInfo	   *info;
	uint32				i;
	char				name[MAXPGPATH];

	oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(merge));
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
		if (info->nodeoid != PGXCNodeOid &&
			merge->normal.drio.convert != NULL)
		{
			TupleTableSlot *slot = DynamicReduceReadSFSTuple(merge->normal.drio.slot_remote,
															 info->file,
															 &info->read_buf);
			do_type_convert_slot_in(merge->normal.drio.convert, slot, info->slot, false);
		}else
		{
			DynamicReduceReadSFSTuple(info->slot, info->file, &info->read_buf);
		}
		if (!TupIsNull(info->slot))
			binaryheap_add_unordered(merge->binheap, UInt32GetDatum(i));
	}
	binaryheap_build(merge->binheap);
}

static void ExecMergeReduceLocal(ClusterReduceState *node)
{
	MergeReduceState   *merge = node->private_state;
	BufFile			   *file;

	/* find local MergeNodeInfo */
	file = GetMergeBufFile(merge, PGXCNodeOid);
	Assert(file != NULL);

	DynamicReduceFetchAllLocalAndSend(&merge->normal.drio,
									  file,
									  DRFetchSaveSFS);
}

static inline void ExecMergeReduceWaitRemote(ClusterReduceState *node, MergeReduceState *merge)
{
	uint8				flag PG_USED_FOR_ASSERTS_ONLY;
	Assert(merge->normal.drio.eof_remote == false);

	/* wait dynamic reduce end of plan */
	flag = DynamicReduceRecvTuple(merge->normal.drio.mqh_receiver,
								  node->ps.ps_ResultTupleSlot,
								  &merge->normal.drio.recv_buf,
								  NULL,
								  false);
	Assert(flag = DR_MSG_RECV && TupIsNull(node->ps.ps_ResultTupleSlot));
	merge->normal.drio.eof_remote = true;

	OpenMergeBufFiles(merge, dsm_segment_address(merge->normal.dsm_seg));
	BuildMergeBinaryHeap(merge);
}

static TupleTableSlot* ExecMergeReduceFinal(PlanState *pstate)
{
	ClusterReduceState *node = castNode(ClusterReduceState, pstate);
	MergeReduceState   *merge = node->private_state;

	ExecMergeReduceWaitRemote(node, merge);

	ExecSetExecProcNode(&node->ps, ExecMergeReduceFirstTuple);
	return ExecMergeReduceFirstTuple(pstate);
}

static TupleTableSlot* ExecMergeReduceFirst(PlanState *pstate)
{
	ExecMergeReduceLocal(castNode(ClusterReduceState, pstate));

	return ExecMergeReduceFinal(pstate);
}

static void InitMergeReduceState(ClusterReduceState *state, MergeReduceState *merge, dsm_segment *seg)
{
	TupleDesc		desc = state->ps.ps_ResultTupleSlot->tts_tupleDescriptor;
	ClusterReduce  *plan = castNode(ClusterReduce, state->ps.plan);
	DynamicReduceSFS sfs;
	List		   *reduce_oids;
	ListCell	   *lc;
	uint32			i,count;
	Assert(plan->numCols > 0);

	reduce_oids = GetReduceOids(castNode(ClusterReduce, state->ps.plan));
	count = list_length(reduce_oids);
	if (count == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Can not find working nodes")));
	}

	if (seg == NULL)
	{
		InitNormalReduceState(&merge->normal, sizeof(*sfs), state);
		sfs = dsm_segment_address(merge->normal.dsm_seg);
		SharedFileSetInit(&sfs->sfs, merge->normal.dsm_seg);
	}else
	{
		sfs = dsm_segment_address(seg);
		DynamicReduceInitFetch(&merge->normal.drio,
							   seg,
							   desc,
							   0,
							   NULL,0,
							   NULL,0);
	}

	merge->nodes = palloc0(sizeof(merge->nodes[0]) * count);
	merge->nnodes = count;
	lc = list_head(reduce_oids);
	for (i=0;i<count;++i)
	{
		MergeNodeInfo *info = &merge->nodes[i];
		info->slot = ExecInitExtraTupleSlot(state->ps.state, desc);
		initStringInfo(&info->read_buf);
		enlargeStringInfo(&info->read_buf, SizeofMinimalTupleHeader);
		MemSet(info->read_buf.data, SizeofMinimalTupleHeader, 0);
		info->nodeoid = lfirst_oid(lc);
		lc = lnext(lc);
		if (info->nodeoid == PGXCNodeOid &&
			seg == NULL)
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
	FreeReduceOids(reduce_oids);
}
static void InitMergeReduce(ClusterReduceState *crstate)
{
	MemoryContext		oldcontext;
	MergeReduceState   *merge;
	Assert(crstate->private_state == NULL);

	oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(crstate));
	merge = palloc0(sizeof(MergeReduceState));
	InitMergeReduceState(crstate, merge, NULL);
	crstate->private_state = merge;
	ExecSetExecProcNode(&crstate->ps, ExecMergeReduceFirst);

	DynamicReduceStartSharedFileSetPlan(crstate->ps.plan->plan_node_id,
										merge->normal.dsm_seg,
										dsm_segment_address(merge->normal.dsm_seg),
										castNode(ClusterReduce, crstate->ps.plan)->reduce_oids);

	MemoryContextSwitchTo(oldcontext);
}
static void EndMergeReduce(MergeReduceState *merge)
{
	uint32				i,count;
	MergeNodeInfo	   *info;
	DynamicReduceSFS	sfs;
	char				name[MAXPGPATH];

	if (merge->normal.dsm_seg)
		sfs = dsm_segment_address(merge->normal.dsm_seg);
	else
		sfs = NULL;

	for (i=0,count=merge->nnodes;i<count;++i)
	{
		info = &merge->nodes[i];
		if(info->file)
			BufFileClose(info->file);
		if (sfs)
			BufFileDeleteShared(&sfs->sfs, DynamicReduceSFSFileName(name, info->nodeoid));
	}
	EndNormalReduce(&merge->normal);
	pfree(merge->sortkeys);
}

static void DriveMergeReduce(ClusterReduceState *node)
{
	MergeReduceState *merge = node->private_state;

	if (node->eflags & EXEC_FLAG_IN_EPQ)
		return;

	if (merge->normal.drio.eof_local == false)
	{
		ExecMergeReduceLocal(node);
		Assert(merge->normal.drio.eof_local);
		ExecSetExecProcNode(&node->ps, ExecMergeReduceFinal);
	}
	if (merge->normal.drio.eof_remote == false)
		ExecMergeReduceWaitRemote(node, merge);
}

static void BeginAdvanceMerge(ClusterReduceState *crstate)
{
	MemoryContext		oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(crstate));

	InitMergeReduce(crstate);
	MemoryContextSwitchTo(crstate->ps.state->es_query_cxt);
	ExecMergeReduceLocal(crstate);
	ExecSetExecProcNode(&crstate->ps, ExecMergeReduceFinal);

	MemoryContextSwitchTo(oldcontext);
}

static void InitEPQMergeReduce(ClusterReduceState *crstate, ClusterReduceState *origin)
{
	MemoryContext		oldcontext;
	MergeReduceState   *merge;
	MergeReduceState   *origin_merge = origin->private_state;
	Assert(crstate->private_state == NULL);

	if (origin_merge->normal.drio.eof_local == false)
	{
		ExecMergeReduceLocal(origin);
		ExecSetExecProcNode(&origin->ps, ExecMergeReduceFinal);
	}
	if (origin_merge->normal.drio.eof_remote == false)
	{
		ExecMergeReduceWaitRemote(origin, origin_merge);
		ExecSetExecProcNode(&origin->ps, ExecMergeReduce);
	}
	Assert(origin_merge->normal.drio.eof_local &&
		   origin_merge->normal.drio.eof_remote);

	oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(crstate));
	crstate->private_state = merge = palloc0(sizeof(MergeReduceState));
	crstate->initialized = true;
	InitMergeReduceState(crstate, merge, origin_merge->normal.dsm_seg);
	merge->normal.drio.eof_remote = merge->normal.drio.eof_local = true;
	OpenMergeBufFiles(merge, dsm_segment_address(origin_merge->normal.dsm_seg));
	BuildMergeBinaryHeap(merge);

	MemoryContextSwitchTo(oldcontext);

	ExecSetExecProcNode(&crstate->ps, ExecMergeReduceFirstTuple);
}

/* ======================================================== */
static void InitReduceMethod(ClusterReduceState *crstate)
{
	Assert(crstate->private_state == NULL);
	Assert(crstate->initialized == false);
	switch(crstate->reduce_method)
	{
	case RT_NOTHING:
		ExecSetExecProcNode(&crstate->ps, ExecNothingReduce);
		break;
	case RT_NORMAL:
		InitNormalReduce(crstate);
		break;
	case RT_REDUCE_FIRST:
		InitReduceFirst(crstate);
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
	crstate->initialized = true;
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

static TupleTableSlot* ExecEPQDefaultClusterReduce(PlanState *pstate)
{
	ClusterReduceState *node = castNode(ClusterReduceState, pstate);
	ClusterReduceState *origin;
	if (node->private_state != NULL ||
		node->initialized)
	{
		ereport(ERROR,
				(errmsg("ClusterReduce plan %d alredy initialized", pstate->plan->plan_node_id),
				 errcode(ERRCODE_INTERNAL_ERROR)));
	}
	if (node->origin_state == NULL)
	{
		ereport(ERROR,
				(errmsg("ClusterReduce plan %d not set origin plan state", pstate->plan->plan_node_id),
				 errcode(ERRCODE_INTERNAL_ERROR)));
	}
	origin = castNode(ClusterReduceState, node->origin_state);

	if (origin->initialized == false)
	{
		/* we can initialize origin cluster reduce, for now we just report an error */
		ereport(ERROR,
				(errmsg("Origin cluster reduce plan %d not initialized", origin->ps.plan->plan_node_id),
				 errcode(ERRCODE_INTERNAL_ERROR)));
	}

	switch(origin->reduce_method)
	{
	case RT_REDUCE_FIRST:
		InitEPQReduceFirst(node, origin);
		break;
	case RT_MERGE:
		InitEPQMergeReduce(node, origin);
		break;
	default:
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("unknown origin reduce method %u", origin->reduce_method)));
		break;
	}
	node->initialized = true;

	return pstate->ExecProcNodeReal(pstate);
}

void ExecClusterReduceEstimate(ClusterReduceState *node, ParallelContext *pcxt)
{
	switch(node->reduce_method)
	{
	case RT_NOTHING:
		break;
	case RT_NORMAL:
		EstimateNormalReduce(pcxt);
		break;
	case RT_REDUCE_FIRST:
		EstimateReduceFirst(pcxt);
		break;
	case RT_ADVANCE:
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("advance reduce not support parallel yet")));
		break;
	case RT_ADVANCE_PARALLEL:
		shm_toc_estimate_chunk(&pcxt->estimator, sizeof(Size)+sizeof(dsm_handle));
		shm_toc_estimate_keys(&pcxt->estimator, 1);
		break;
	case RT_MERGE:
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("merge reduce not support parallel yet")));
		break;
	default:
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("unknown reduce method %u", node->reduce_method)));
		break;
	}
}
void ExecClusterReduceInitializeDSM(ClusterReduceState *node, ParallelContext *pcxt)
{
	switch(node->reduce_method)
	{
	case RT_NOTHING:
		break;
	case RT_NORMAL:
		InitParallelReduce(node, pcxt);
		break;
	case RT_REDUCE_FIRST:
		InitParallelReduceFirst(node, pcxt);
		break;
	case RT_ADVANCE_PARALLEL:
		{
			char *addr = shm_toc_allocate(pcxt->toc, sizeof(dsm_handle));
			dsm_handle *h = (dsm_handle*)(addr + sizeof(Size));
			AdvanceParallelState *state = node->private_state;
			*(Size*)addr = RT_ADVANCE_PARALLEL;
			*h = dsm_segment_handle(state->normal.dsm_seg);
			shm_toc_insert(pcxt->toc, node->ps.plan->plan_node_id, addr);
		}
		break;
	case RT_MERGE:
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("merge reduce not support parallel yet")));
		break;
	default:
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("unknown reduce method %u", node->reduce_method)));
		break;
	}
	node->initialized = true;
}

void ExecClusterReduceInitializeWorker(ClusterReduceState *node, ParallelWorkerContext *pwcxt)
{
	char *addr = shm_toc_lookup(pwcxt->toc, node->ps.plan->plan_node_id, false);
	node->reduce_method = (uint8)(*(Size*)addr);
	addr += sizeof(Size);

	DynamicReduceStartParallel();
	switch(node->reduce_method)
	{
	case RT_NOTHING:
		break;
	case RT_NORMAL:
		InitParallelReduceWorker(node, pwcxt, addr);
		break;
	case RT_REDUCE_FIRST:
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("reduce first not support parallel yet")));
		break;
	case RT_PARALLEL_REDUCE_FIRST:
		InitParallelReduceFirstCommon(node, NULL, pwcxt, addr);
		break;
	case RT_ADVANCE_PARALLEL:
		InitAdvanceParallelReduceWorker(node, pwcxt, addr);
		break;
	case RT_MERGE:
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("merge reduce not support parallel yet")));
		break;
	default:
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("unknown reduce method %u", node->reduce_method)));
		break;
	}
	node->initialized = true;
}

void ExecClusterReduceReInitializeDSM(ClusterReduceState *node, ParallelContext *pcxt)
{
	switch(node->reduce_method)
	{
	case RT_NOTHING:
		break;
	case RT_PARALLEL_REDUCE_FIRST:
		ReinitParallelReduceFirst(node, pcxt);
		break;
	default:
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("reduce method %u not support reinitialize dsm", node->reduce_method)));
	}
}

ClusterReduceState *
ExecInitClusterReduce(ClusterReduce *node, EState *estate, int eflags)
{
	ClusterReduceState *crstate;
	Plan			   *outerPlan;

	Assert(outerPlan(node) != NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create state structure
	 */
	crstate = makeNode(ClusterReduceState);
	crstate->ps.plan = (Plan*)node;
	crstate->ps.state = estate;
	crstate->ps.ExecProcNode = ExecDefaultClusterReduce;

	/*
	 * We must have a tuplestore buffering the subplan output to do backward
	 * scan or mark/restore.  We also prefer to materialize the subplan output
	 * if we might be called on to rewind and replay it many times. However,
	 * if none of these cases apply, we can skip storing the data.
	 */
	crstate->eflags = (eflags & (EXEC_FLAG_REWIND |
								 EXEC_FLAG_BACKWARD |
								 EXEC_FLAG_MARK |
								 EXEC_FLAG_IN_EPQ |
								 EXEC_FLAG_EXPLAIN_ONLY));

	/*
	 * Tuplestore's interpretation of the flag bits is subtly different from
	 * the general executor meaning: it doesn't think BACKWARD necessarily
	 * means "backwards all the way to start".  If told to support BACKWARD we
	 * must include REWIND in the tuplestore eflags, else tuplestore_trim
	 * might throw away too much.
	 */
	if (eflags & EXEC_FLAG_BACKWARD)
		crstate->eflags |= EXEC_FLAG_REWIND;

	if (list_member_oid(node->reduce_oids, PGXCNodeOid) == false)
		crstate->reduce_method = (uint8)RT_NOTHING;
	else if (node->numCols > 0)
		crstate->reduce_method = (uint8)RT_MERGE;
	else if (node->reduce_flags & (CRF_FETCH_LOCAL_FIRST|CRF_DISK_ALWAYS) ||
			 crstate->eflags != 0)
		crstate->reduce_method = (uint8)RT_REDUCE_FIRST;
	else
		crstate->reduce_method = (uint8)RT_NORMAL;

	if ((eflags & EXEC_FLAG_IN_EPQ) &&
		crstate->reduce_method != RT_NOTHING)
		crstate->ps.ExecProcNode = ExecEPQDefaultClusterReduce;

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

static void ExecShutdownClusterReduce(ClusterReduceState *node)
{
	if (node->private_state)
	{
		switch(node->reduce_method)
		{
		case RT_NORMAL:
			EndNormalReduce(node->private_state);
			break;
		case RT_REDUCE_FIRST:
			EndReduceFirst(node->private_state, node->ps.plan->plan_node_id);
			break;
		case RT_REDUCE_FIRST_EPQ:
			EndEPQReduceFirst(node->private_state);
			break;
		case RT_PARALLEL_REDUCE_FIRST:
			EndParallelReduceFirst(node->private_state);
			break;
			break;
		case RT_ADVANCE:
			EndAdvanceReduce(node->private_state, node);
			break;
		case RT_ADVANCE_PARALLEL:
			EndAdvanceParallelReduce(node->private_state);
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
		node->private_state = NULL;
	}
}

void
ExecEndClusterReduce(ClusterReduceState *node)
{
	if ((node->eflags & EXEC_FLAG_EXPLAIN_ONLY) == 0)
		DriveClusterReduceState(node);

	ExecShutdownClusterReduce(node);

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

	if (bms_is_subset(outerPlanState(node)->chgParam,
					  castNode(ClusterReduce, node->ps.plan)->ignore_params) == false)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cluster reduce not support sub plan change param")));
	}

	switch(node->reduce_method)
	{
	case RT_NOTHING:
		break;
	case RT_NORMAL:
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("clsuter reduce normal method not support rescan")));
		break;	/* should never run */
	case RT_PARALLEL_REDUCE_FIRST:
		{
			ParallelReduceFirstState *state = node->private_state;
			if (state->normal.drio.eof_local == false)
				ExecParallelReduceFirstSendLocal(state);
			if (state->normal.drio.eof_remote == false)
				ExecParallelReduceFirstWaitRemote(state, node->ps.plan->plan_node_id);
			sts_reinitialize(state->sta);
			sts_begin_parallel_scan(state->sta);
			sts_reinitialize(state->normal.drio.sts);
			ExecSetExecProcNode(&node->ps, ExecParallelReduceFirstLocal);
		}
		break;
	case RT_REDUCE_FIRST:
		{
			NormalReduceFirstState *state = node->private_state;
			if (state->ready_local)
			{
				if (BufFileSeek(state->file_local, 0, 0, SEEK_SET) != 0)
					ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("can not seek buffer file to head")));
				ExecSetExecProcNode(&node->ps, ExecReduceFirstLocal);
			}else
			{
				ExecSetExecProcNode(&node->ps, ExecReduceFirstPrepare);
			}
		}
		break;
	case RT_REDUCE_FIRST_EPQ:
		{
			ReduceFirstEPQState *state = node->private_state;
			if (BufFileSeek(state->file_local, 0, 0, SEEK_SET) != 0)
				ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("can not seek buffer file to head")));
			ExecSetExecProcNode(&node->ps, ExecReduceFirstEPQLocal);
		}
		break;
	case RT_ADVANCE:
		{
			AdvanceReduceState *state = node->private_state;
			AdvanceNodeInfo	   *info;
			uint32				i;

			state->cur_node = NULL;
			for (i=0;i<state->nnodes;++i)
			{
				info = &state->nodes[i];
				if (info->file != NULL &&
					BufFileSeek(info->file, 0, 0, SEEK_SET) != 0)
				{
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("can not seek buffer file to head")));
				}
				if (info->nodeoid == PGXCNodeOid)
					state->cur_node = info;
			}
			Assert(state->cur_node != NULL);
			Assert(state->cur_node->nodeoid == PGXCNodeOid);
		}
		break;
	case RT_ADVANCE_PARALLEL:
		{
			AdvanceParallelState   *state = node->private_state;
			AdvanceParallelNode	   *node;
			uint32					i,remote_state;

			remote_state = pg_atomic_read_u32(&state->shm->remote_state);
			state->cur_node = NULL;
			for (i=0;i<state->nnodes;++i)
			{
				node = &state->nodes[i];
				if (node->accessor)
				{
					sts_end_parallel_scan(node->accessor);
					if (node->nodeoid == PGXCNodeOid ||
						remote_state == APSM_REMOTE_GOT)
						sts_reinitialize(node->accessor);	/* rescan */
				}
				if (node->nodeoid == PGXCNodeOid)
				{
					Assert(state->cur_node == NULL);
					state->cur_node = node;
				}
			}
			Assert(state->cur_node != NULL);
			Assert(state->cur_node->nodeoid == PGXCNodeOid);
			sts_begin_parallel_scan(state->cur_node->accessor);
		}
		break;
	case RT_MERGE:
		{
			MergeReduceState   *state = node->private_state;
			MergeNodeInfo	   *info;
			uint32				i;
			if (state->normal.drio.eof_remote == true)
			{
				binaryheap_reset(state->binheap);
				for (i=0;i<state->nnodes;++i)
				{
					info = &state->nodes[i];
					ExecClearTuple(info->slot);
					resetStringInfo(&info->read_buf);
					if (BufFileSeek(info->file, 0, 0, SEEK_SET) != 0)
					{
						ereport(ERROR,
								(errcode_for_file_access(),
								 errmsg("can not seek SFS file to head")));
					}
				}
				BuildMergeBinaryHeap(state);
				ExecSetExecProcNode(&node->ps, ExecMergeReduceFirstTuple);
			}
		}
		break;
	default:
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("unknown cluster reduce method %u", node->reduce_method)));
		break;
	}
}

static bool
DriveClusterReduceState(ClusterReduceState *node)
{
	if (node->eflags & EXEC_FLAG_IN_EPQ)
	{
		return false;
	}else if (node->initialized == false)
	{
		if (node->private_state == NULL)
			InitReduceMethod(node);
	}else if(node->private_state == NULL)
	{
		/* execute finished */
		return false;
	}

	switch(node->reduce_method)
	{
	case RT_NOTHING:
	case RT_ADVANCE:
		break;
	case RT_ADVANCE_PARALLEL:
		DriveAdvanceParallelReduce(node);
		break;
	case RT_NORMAL:
		DriveNormalReduce(node);
		break;
	case RT_REDUCE_FIRST:
		DriveReduceFirst(node);
		break;
	case RT_REDUCE_FIRST_EPQ:
		break;
	case RT_PARALLEL_REDUCE_FIRST:
		DriveParallelReduceFirst(node);
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
		if (((ClusterReduceState*)node)->reduce_method == RT_NOTHING)
			res = DriveClusterReduceWalker(outerPlanState(node));
		else
			res = DriveClusterReduceState((ClusterReduceState *) node);
	} else
	if (IsA(node, CteScanState))
	{
		res = DriveCteScanState(node);
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

	if (IsA(node, ReduceScanState))
	{
		/* ReduceScan's outer should be executed */
		return false;
	}

	return planstate_tree_walker(node, IsThereClusterReduce, NULL);
}

void
TopDownDriveClusterReduce(PlanState *node)
{
	if (!enable_cluster_plan ||
		!IsUnderPostmaster ||
		node->state->es_top_eflags & EXEC_FLAG_IN_EPQ)
		return ;

	/* just return if there is no ClusterReduce plan */
	if (!node->state->es_reduce_plan_inited)
		return ;

	if (bms_is_member(node->plan->plan_node_id, node->state->es_reduce_drived_set))
		return;

	BeginDriveClusterReduce(node);
	(void) DriveClusterReduceWalker(node);
}

static void OnDsmDatchShutdownReduce(dsm_segment *seg, Datum arg)
{
	ExecShutdownClusterReduce((ClusterReduceState*)DatumGetPointer(arg));
}

static bool SetClusterReduceOriginWorker(PlanState *ps, ClusterReduceState *node)
{
	if (ps == NULL)
		return false;
	if (IsA(ps, ClusterReduceState) &&
		ps->plan->plan_node_id == node->ps.plan->plan_node_id)
	{
		ClusterReduceState *origin_state = (ClusterReduceState*)ps;
		if (origin_state->origin_state)
			node->origin_state = origin_state->origin_state;
		else
			node->origin_state = origin_state;
		Assert((node->origin_state->eflags & EXEC_FLAG_IN_EPQ) == 0);
		return true;
	}

	return planstate_tree_walker(ps, SetClusterReduceOriginWorker, node);
}

static inline bool SetClusterReduceOrigin(EPQState *epq, PlanState *ps, bool (*fun)())
{
	ListCell *lc;
	if ((*fun)(epq->owner, ps))
		return true;
	foreach(lc, epq->estate->es_subplanstates)
	{
		if ((*fun)(lfirst(lc), ps))
			return true;
	}
	return false;
}

static bool EnumClusterReduceWorker(PlanState *ps, EPQState *epq)
{
	if (ps == NULL)
		return false;
	if (IsA(ps, ClusterReduceState))
	{
		if (SetClusterReduceOrigin(epq, ps, SetClusterReduceOriginWorker) == false)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Can not found origin plan state for cluster reduce plan %d",
							ps->plan->plan_node_id)));
		Assert(((ClusterReduceState*)ps)->origin_state != NULL);
	}
	return planstate_tree_walker(ps, EnumClusterReduceWorker, epq);
}

void StartEPQClusterReduce(EPQState *epq)
{
	(void)EnumClusterReduceWorker(epq->planstate, epq);
}

/* =========================================================================== */
#define ACR_FLAG_INVALID	0x0
#define ACR_FLAG_OUTER		0x1
#define ACR_FLAG_INNER		0x2
#define ACR_FLAG_APPEND		0x5
#define ACR_FLAG_SUBQUERY	0x6
#define ACR_MARK_SPECIAL	0xFFFF0000
#define ACR_FLAG_SUBPLAN	0x10000
#define ACR_FLAG_INITPLAN	0x20000

static inline void AdvanceReduce(ClusterReduceState *crs, PlanState *parent, uint32 flags)
{
	ClusterReduce  *plan = castNode(ClusterReduce, crs->ps.plan);

	if (IsA(outerPlan(plan), ParamTuplestoreScan))
	{
		/*
		 * this reduce is only for auxiliary table sync for now,
		 * can't advance before main table update
		 */
		return;
	}

	if ((flags & ACR_MARK_SPECIAL) == 0)
		return;
	
	switch(crs->reduce_method)
	{
	case RT_NOTHING:
		return;
	case RT_NORMAL:
	case RT_REDUCE_FIRST:
		if (plan->plan.parallel_safe)
			BeginAdvanceParallelReduce(crs);
		else
			BeginAdvanceReduce(crs);
		break;
	case RT_ADVANCE:
	case RT_ADVANCE_PARALLEL:
		break;
	case RT_MERGE:
		BeginAdvanceMerge(crs);
		break;
	default:
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("unknown reduce method %u", crs->reduce_method)));
		break;
	}

	/* save initialized state */
	crs->initialized = true;
}

#define WalkerList(list, type_)						\
	if ((list) != NIL)								\
	{												\
		ListCell *lc;								\
		foreach(lc, (list))							\
			AdvanceClusterReduceWorker(lfirst_node(SubPlanState, lc)->planstate, ps, type_); \
	}while(false)

#define WalkerMembers(State, arr, count, type_)		\
	do{												\
		uint32 i,n=(((State*)ps)->count);			\
		PlanState **subs = (((State*)ps)->arr);		\
		for(i=0;i<n;++i)							\
			AdvanceClusterReduceWorker(subs[i], ps, type_);	\
	}while(false)

static void AdvanceClusterReduceWorker(PlanState *ps, PlanState *pps, uint32 flags)
{
	if (ps == NULL)
		return;

	check_stack_depth();

	/* initPlan-s */
	WalkerList(ps->initPlan, ACR_FLAG_INITPLAN);

	/* outer */
	AdvanceClusterReduceWorker(outerPlanState(ps), ps, 
							   (flags&ACR_MARK_SPECIAL)|ACR_FLAG_OUTER);

	/* inner */
	AdvanceClusterReduceWorker(innerPlanState(ps), ps,
							   (flags&ACR_MARK_SPECIAL)|ACR_FLAG_INNER);

	switch(nodeTag(ps))
	{
	case T_ClusterReduceState:
		Assert(flags != ACR_FLAG_INVALID);
		AdvanceReduce((ClusterReduceState*)ps, pps, flags);
		break;
	case T_ModifyTableState:
		WalkerMembers(ModifyTableState, mt_plans, mt_nplans,
					  (flags&ACR_MARK_SPECIAL)|ACR_FLAG_APPEND);
		break;
	case T_AppendState:
		WalkerMembers(AppendState, appendplans, as_nplans,
					  (flags&ACR_MARK_SPECIAL)|ACR_FLAG_APPEND);
		break;
	case T_MergeAppendState:
		WalkerMembers(MergeAppendState, mergeplans, ms_nplans,
					  (flags&ACR_MARK_SPECIAL)|ACR_FLAG_APPEND);
		break;
	case T_BitmapAndState:
		WalkerMembers(BitmapAndState, bitmapplans, nplans,
					  (flags&ACR_MARK_SPECIAL)|ACR_FLAG_APPEND);
		break;
	case T_BitmapOrState:
		WalkerMembers(BitmapOrState, bitmapplans, nplans,
					  (flags&ACR_MARK_SPECIAL)|ACR_FLAG_APPEND);
		break;
	case T_SubqueryScanState:
		AdvanceClusterReduceWorker(((SubqueryScanState*)ps)->subplan, ps,
								   (flags&ACR_MARK_SPECIAL)|ACR_FLAG_SUBQUERY);
		break;
	case T_CustomScanState:
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cluster plan not support custom yet")));
		break;
	default:
		break;
	}

	WalkerList(ps->subPlan, ACR_FLAG_SUBPLAN);

	if (IsA(ps, ReduceScanState))
		FetchReduceScanOuter((ReduceScanState*)ps);
}

void AdvanceClusterReduce(PlanState *pstate)
{
	if (IsParallelWorker())
		return;

	AdvanceClusterReduceWorker(pstate, NULL, ACR_FLAG_INVALID);
}

TupleTableSlot* ExecFakeProcNode(PlanState *pstate)
{
	Plan *plan = pstate->plan;
	const char *node_str = get_enum_string_NodeTag(nodeTag(plan));
	if (node_str == NULL)
	{
		node_str = "unknown";
	}else if(node_str[0] == 'T' &&
			 node_str[1] == '_')
	{
		node_str += 2;
	}

	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("plan %s id %d should not be execute", node_str, plan->plan_node_id)));

	return NULL;	/* keep compiler quiet */
}
