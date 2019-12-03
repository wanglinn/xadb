#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "libpq/pqformat.h"
#include "utils/dsa.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

#include "utils/dynamicreduce.h"
#include "utils/dr_private.h"

typedef struct ParallelPlanPrivate
{
	SharedTuplestoreAccessor *sta;
	DynamicReduceSharedTuplestore *shm;
	dsa_pointer			dsa_ptr;
	TupleTableSlot	   *slot_node_src;		/* type convert for recv tuple in from other node */
	TupleTableSlot	   *slot_node_dest;		/* type convert for recv tuple out from other node */
	StringInfoData		tup_buf;
}ParallelPlanPrivate;

#if 0
#define SHOW_PLAN_INFO_STATE(msg, pi)							\
	do{															\
		PlanWorkerInfo *w;uint32 n;								\
		for (n=pi->count_pwi;n>0;)								\
		{														\
			w = &pi->pwi[--n];									\
			ereport(LOG_SERVER_ONLY,							\
					(errmsg(msg " plan %d worker %d leof %d reof %d rstate %d sstate %d", \
							pi->plan_id, w->worker_id,			\
							pi->local_eof,pi->remote_eof,		\
							w->plan_recv_state,					\
							w->plan_send_state)));				\
		}														\
	}while(0)
#else
#define SHOW_PLAN_INFO_STATE(msg, pi) ((void)true)
#endif

static void ClearParallelPlanInfo(PlanInfo *pi)
{
	if (pi == NULL)
		return;

	DR_PLAN_DEBUG((errmsg("clean parallel plan %d(%p)", pi->plan_id, pi)));
	DRPlanSearch(pi->plan_id, HASH_REMOVE, NULL);
	if (pi->pwi)
	{
		uint32 i;
		SHOW_PLAN_INFO_STATE("clean", pi);
		for (i=0;i<pi->count_pwi;++i)
			DRClearPlanWorkInfo(pi, &pi->pwi[i]);
		pfree(pi->pwi);
		pi->pwi = NULL;
	}
	if (pi->private)
	{
		ParallelPlanPrivate *private = pi->private;
		if (private->sta)
		{
			sts_detach(private->sta);
			private->sta = NULL;
		}
		/*
		 * don't need call dsa_free
		 * when error, dsm will reset
		 * normal, last worker will call dsa_free
		 */
		if (private->slot_node_src)
		{
			ExecDropSingleTupleTableSlot(private->slot_node_src);
			private->slot_node_src = NULL;
		}
		if (private->slot_node_dest)
		{
			ExecDropSingleTupleTableSlot(private->slot_node_dest);
			private->slot_node_dest = NULL;
		}
		if (private->tup_buf.data)
		{
			pfree(private->tup_buf.data);
			private->tup_buf.data = NULL;
		}
		pfree(private);
		pi->private = NULL;
	}
	DRClearPlanInfo(pi);
}

static void OnParallelPlanPreWait(PlanInfo *pi)
{
	PlanWorkerInfo *pwi;
	uint32			count = pi->count_pwi;
	bool			need_wait = false;

	while(count > 0)
	{
		pwi = &pi->pwi[--count];
		if (pwi->plan_recv_state == DR_PLAN_RECV_WORKING ||
			pwi->last_msg_type != ADB_DR_MSG_INVALID ||
			pwi->plan_send_state != DR_PLAN_SEND_ENDED)
		{
			need_wait = true;
			break;
		}
	}
	if (need_wait == false)
		ClearParallelPlanInfo(pi);
}

static inline bool NeedGenerateEndMessage(PlanInfo *pi)
{
	PlanWorkerInfo *pwi;
	uint32 result;
	uint32 count;

	if (pi->local_eof)
		return false;
	if (pi->end_count_pwi == pi->count_pwi)
		return true;

	SHOW_PLAN_INFO_STATE("NeedGenerateEndMessage", pi);

	result = 0;
	for (count=pi->count_pwi;count>0;)
	{
		pwi = &pi->pwi[--count];
		if (pwi->plan_recv_state != DR_PLAN_RECV_WAITING_ATTACH)
			++result;
	}

	return result == pi->end_count_pwi;
}

static inline void NotifyNotAttachedWorker(PlanInfo *pi)
{
	PlanWorkerInfo *w;
	uint32 n;
	for (n=pi->count_pwi;n>0;)
	{
		w = &pi->pwi[--n];
		if (w->plan_recv_state == DR_PLAN_RECV_WAITING_ATTACH)
		{
			w->plan_send_state = DR_PLAN_SEND_GENERATE_CACHE;
			while (w->plan_send_state != DR_PLAN_SEND_ENDED)
			{
				if (DRSendPlanWorkerMessage(w, pi) == false)
					break;
			}
		}
	}
	SHOW_PLAN_INFO_STATE("notify", pi);
}

static void OnParallelPlanLatch(PlanInfo *pi)
{
	PlanWorkerInfo *pwi;
	uint32			msg_type;
	uint32			count;
	bool			need_active_node;

	need_active_node = false;
	for(count = pi->count_pwi;count>0;)
	{
		pwi = &pi->pwi[--count];
		if (pwi->plan_recv_state == DR_PLAN_RECV_WAITING_ATTACH)
		{
			if (DRRecvPlanWorkerMessage(pwi, pi))
			{
				Assert(pwi->last_msg_type == ADB_DR_MSG_ATTACH_PLAN &&
					   pwi->plan_recv_state == DR_PLAN_RECV_WORKING &&
					   pwi->sendBuffer.len == 0);
				SHOW_PLAN_INFO_STATE("got attach", pi);
				/* clear last message */
				pwi->last_msg_type = ADB_DR_MSG_INVALID;
				appendStringInfoCharMacro(&pwi->sendBuffer, ADB_DR_MSG_ATTACH_PLAN);
				appendStringInfoCharMacro(&pwi->sendBuffer, pi->local_eof);
				DRSendPlanWorkerMessage(pwi, pi);
				need_active_node = true;
			}
		}else if (DRSendPlanWorkerMessage(pwi, pi))
		{
			need_active_node = true;
		}
	}
	if (need_active_node)
		DRActiveNode(pi->plan_id);

	for(count = pi->count_pwi; count>0 && pi->waiting_node == InvalidOid;)
	{
		pwi = &pi->pwi[--count];
		while (pwi->waiting_node == InvalidOid &&
			   pwi->plan_recv_state == DR_PLAN_RECV_WORKING &&
			   pwi->last_msg_type == ADB_DR_MSG_INVALID)
		{
			if (DRRecvPlanWorkerMessage(pwi, pi) == false)
				break;
			msg_type = pwi->last_msg_type;

			if (msg_type == ADB_DR_MSG_END_OF_PLAN)
			{
				if (pwi->got_eof)
					ereport(ERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg("already got EOF message from plan %d worker %d", pi->plan_id, pwi->worker_id)));
				pwi->got_eof = true;
				pwi->plan_recv_state = DR_PLAN_RECV_ENDED;
				++(pi->end_count_pwi);
				Assert(pi->end_count_pwi <= pi->count_pwi);

				/* is all parallel got end? */
				if (NeedGenerateEndMessage(pi))
				{
					DR_PLAN_DEBUG_EOF((errmsg("parall plan %d(%p) worker %d will send end of plan message to remote",
											  pi->plan_id, pi, pwi->worker_id)));
					pi->local_eof = true;
					if (pi->remote_eof)
						NotifyNotAttachedWorker(pi);
					DRGetEndOfPlanMessage(pi, pwi);
				}else
				{
					pwi->last_msg_type = ADB_DR_MSG_INVALID;
					break;
				}
			}else
			{
				Assert(msg_type == ADB_DR_MSG_TUPLE);
			}

			/* send message to remote */
			DRSendWorkerMsgToNode(pwi, pi, NULL);
		}
	}
}

static inline SharedTuplestoreAccessor* ParallelPlanGetCacheSTS(ParallelPlanPrivate *private, uint32 npart)
{
	char			name[MAXPGPATH];
	MemoryContext	oldcontext;

	if (private->sta == NULL)
	{
		oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(private));

		private->dsa_ptr = dsa_allocate(dr_dsa, offsetof(DynamicReduceSharedTuplestore, sts) + sts_estimate(1));
		private->shm = dsa_get_address(dr_dsa, private->dsa_ptr);
		pg_atomic_init_u32(&private->shm->attached, npart);
		private->sta = sts_initialize((SharedTuplestore*)private->shm->sts,
									  1, 0, 0, 0,
									  dr_shared_fs,
									  DynamicReduceSharedFileName(name, DRNextSharedFileSetNumber()));
		MemoryContextSwitchTo(oldcontext);
	}

	return private->sta;
}

static inline MinimalTuple ParallelConvertNodeTupIn(MemoryContext context,
													TupleTypeConvert *convert,
													TupleTableSlot *src,
													TupleTableSlot *dest,
													const char *data,
													int len)
{
	HeapTupleData	tup;
	MinimalTuple	mtup;
	MemoryContext	oldcontext;
	
	MemoryContextReset(context);
	oldcontext = MemoryContextSwitchTo(context);

	DRStoreTypeConvertTuple(src, data, len, &tup);
	do_type_convert_slot_in(convert, src, dest, false);
	mtup = ExecFetchSlotMinimalTuple(dest);
	ExecClearTuple(src);

	MemoryContextSwitchTo(oldcontext);
	return mtup;
}

static bool OnParallelPlanMessage(PlanInfo *pi, const char *data, int len, Oid nodeoid)
{
	PlanWorkerInfo *pwi;
	MinimalTuple	mtup;
	uint32			i,count;

	i=pi->last_pwi;
	for(count=pi->count_pwi;count>0;--count)
	{
		if (++i >= pi->count_pwi)
			i = 0;
		pwi = &pi->pwi[i];

		if (pwi->sendBuffer.len != 0 ||		/* we only cache one tuple */
			pwi->plan_recv_state == DR_PLAN_RECV_WAITING_ATTACH) /* not attachd */
			continue;

		CHECK_WORKER_IN_WROKING(pwi, pi);

		DR_PLAN_DEBUG((errmsg("parallel plan %d(%p) worker %u got a tuple from %u length %d",
							  pi->plan_id, pi, pwi->worker_id, nodeoid, len)));
		appendStringInfoChar(&pwi->sendBuffer, ADB_DR_MSG_TUPLE);
		appendStringInfoSpaces(&pwi->sendBuffer, sizeof(nodeoid)-sizeof(char));	/* for align */
		appendBinaryStringInfoNT(&pwi->sendBuffer, (char*)&nodeoid, sizeof(nodeoid));
		if (pi->type_convert)
		{
			mtup = ParallelConvertNodeTupIn(pi->convert_context,
											pi->type_convert,
											pwi->slot_node_src,
											pwi->slot_node_dest,
											data,
											len);
			appendBinaryStringInfoNT(&pwi->sendBuffer,
									 (char*)mtup + MINIMAL_TUPLE_DATA_OFFSET,
									 mtup->t_len - MINIMAL_TUPLE_DATA_OFFSET);
		}else
		{
			appendBinaryStringInfoNT(&pwi->sendBuffer, data, len);
		}

		DRSendPlanWorkerMessage(pwi, pi);

		pi->last_pwi = i;

		return true;
	}

	/* try cache on disk */
	if (pi->private)
	{
		ParallelPlanPrivate *private = pi->private;
		DR_PLAN_DEBUG((errmsg("parallel plan %d(%p) sts got a tuple from %u length %d",
							  pi->plan_id, pi, nodeoid, len)));
		if (pi->type_convert)
		{
			mtup = ParallelConvertNodeTupIn(pi->convert_context,
											pi->type_convert,
											private->slot_node_src,
											private->slot_node_dest,
											data,
											len);
		}else
		{
			resetStringInfo(&private->tup_buf);
			Assert(private->tup_buf.maxlen >= MINIMAL_TUPLE_DATA_OFFSET);
			private->tup_buf.len = MINIMAL_TUPLE_DATA_OFFSET;
			appendBinaryStringInfoNT(&private->tup_buf, data, len);
			mtup = (MinimalTuple)private->tup_buf.data;
			mtup->t_len = len + MINIMAL_TUPLE_DATA_OFFSET;
		}
		sts_puttuple(ParallelPlanGetCacheSTS(private, pi->count_pwi), NULL, mtup);
		return true;
	}

	return false;
}

static void OnParallelPlanIdleNode(PlanInfo *pi, WaitEvent *we, DRNodeEventData *ned)
{
	PlanWorkerInfo *pwi;
	uint32			i;

	for(i=pi->count_pwi;i>0;)
	{
		pwi = &pi->pwi[--i];
		if (pwi->last_msg_type == ADB_DR_MSG_INVALID ||
			pwi->dest_oids[pwi->dest_cursor] != ned->nodeoid)
			continue;

		DRSendWorkerMsgToNode(pwi, pi, ned);
	}
}

static bool OnParallelPlanNodeEndOfPlan(PlanInfo *pi, Oid nodeoid)
{
	PlanWorkerInfo *pwi;
	uint32			i;

	DR_PLAN_DEBUG_EOF((errmsg("parallel plan %d(%p) got end of plan message from node %u",
							  pi->plan_id, pi, nodeoid)));
	if (oidBufferMember(&pi->end_of_plan_nodes, nodeoid, NULL))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("plan %d already got end of plan message from node %u", pi->plan_id, nodeoid)));
	appendOidBufferUniqueOid(&pi->end_of_plan_nodes, nodeoid);

	if (pi->end_of_plan_nodes.len == pi->working_nodes.len)
	{
		DR_PLAN_DEBUG_EOF((errmsg("plan %d generating EOF message to all worker", pi->plan_id)));
		SHOW_PLAN_INFO_STATE("begin EOF plan", pi);
		pi->remote_eof = true;
		for(i=pi->count_pwi;i>0;)
		{
			pwi = &pi->pwi[--i];
			CHECK_WORKER_IN_WROKING(pwi, pi);
			if (pi->local_eof == false &&
				pwi->plan_recv_state == DR_PLAN_RECV_WAITING_ATTACH)
				continue;	/* don't send EOF message for now */
			pwi->plan_send_state = DR_PLAN_SEND_GENERATE_CACHE;
			while (pwi->sendBuffer.len == 0 &&
				   pwi->plan_send_state != DR_PLAN_SEND_ENDED)
			{
				/* when not busy and not send end, send message immediately */
				if (DRSendPlanWorkerMessage(pwi, pi) == false)
					break;
			}
		}
		SHOW_PLAN_INFO_STATE("end EOF plan",pi);
	}
	return true;
}

static bool GenerateParallelCacheMessage(PlanWorkerInfo *pwi, PlanInfo *pi)
{
	ParallelPlanPrivate *private = pi->private;
	if (private->sta != NULL)
	{
		Assert(private->dsa_ptr != InvalidDsaPointer);
		Assert(pwi->sendBuffer.len == 0);
		sts_end_write(private->sta);
		appendStringInfoChar(&pwi->sendBuffer, ADB_DR_MSG_SHARED_TUPLE_STORE);
		if (pwi->plan_recv_state == DR_PLAN_RECV_WAITING_ATTACH)
		{
			Assert(pi->local_eof == true);
			appendStringInfoChar(&pwi->sendBuffer, pi->local_eof);
		}
		appendStringInfoSpaces(&pwi->sendBuffer, SIZEOF_DSA_POINTER-pwi->sendBuffer.len);	/* align */
		appendBinaryStringInfoNT(&pwi->sendBuffer, (char*)&(private->dsa_ptr), SIZEOF_DSA_POINTER);
		return true;
	}
	return false;
}

void DRStartParallelPlanMessage(StringInfo msg)
{
	PlanInfo * volatile		pi = NULL;
	DynamicReduceMQ			mq;
	MemoryContext			oldcontext;
	ResourceOwner			oldowner;
	int						parallel_max;
	int						i;
	bool					cache_on_disk;

	if (!IsTransactionState())
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("not in transaction state")));
	}

	PG_TRY();
	{
		oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		oldowner = CurrentResourceOwner;
		CurrentResourceOwner = NULL;

		pq_copymsgbytes(msg, (char*)&parallel_max, sizeof(parallel_max));
		if (parallel_max <= 1)
			elog(ERROR, "invalid parallel count");
		cache_on_disk = (bool)pq_getmsgbyte(msg);

		pi = DRRestorePlanInfo(msg, (void**)&mq, sizeof(*mq)*parallel_max, ClearParallelPlanInfo);
		pi->pwi = MemoryContextAllocZero(TopMemoryContext, sizeof(PlanWorkerInfo)*parallel_max);
		pi->count_pwi = parallel_max;
		for (i=0;i<parallel_max;++i)
			DRSetupPlanWorkInfo(pi, &pi->pwi[i], &mq[i], i-1, DR_PLAN_RECV_WAITING_ATTACH);

		CurrentResourceOwner = oldowner;
		for (i=0;i<parallel_max;++i)
			DRSetupPlanWorkTypeConvert(pi, &pi->pwi[i]);

		if (cache_on_disk)
		{
			ParallelPlanPrivate *private = MemoryContextAllocZero(TopMemoryContext,
																  sizeof(ParallelPlanPrivate));
			pi->private = private;
			private->dsa_ptr = InvalidDsaPointer;
			if (pi->convert_context == NULL)
			{
				initStringInfo(&private->tup_buf);
				enlargeStringInfo(&private->tup_buf, MINIMAL_TUPLE_DATA_OFFSET);
				MemSet(private->tup_buf.data, 0, MINIMAL_TUPLE_DATA_OFFSET);
			}else
			{
				private->slot_node_src = MakeSingleTupleTableSlot(pi->type_convert->out_desc);
				private->slot_node_dest = MakeSingleTupleTableSlot(pi->type_convert->base_desc);
			}
			pi->GenerateCacheMsg = GenerateParallelCacheMessage;
		}

		pi->OnLatchSet = OnParallelPlanLatch;
		pi->OnNodeRecvedData = OnParallelPlanMessage;
		pi->OnNodeIdle = OnParallelPlanIdleNode;
		pi->OnNodeEndOfPlan = OnParallelPlanNodeEndOfPlan;
		pi->OnPlanError = ClearParallelPlanInfo;
		pi->OnPreWait = OnParallelPlanPreWait;

		MemoryContextSwitchTo(oldcontext);
		DR_PLAN_DEBUG((errmsg("normal plan %d(%p) stared", pi->plan_id, pi)));

	}PG_CATCH();
	{
		ClearParallelPlanInfo(pi);
		PG_RE_THROW();
	}PG_END_TRY();

	Assert(pi != NULL);
	DRActiveNode(pi->plan_id);
}

void DynamicReduceStartParallelPlan(int plan_id, struct dsm_segment *seg,
									DynamicReduceMQ mq, TupleDesc desc, List *work_nodes,
									int parallel_max, bool cache_on_disk)
{
	StringInfoData	buf;
	Assert(plan_id >= 0);
	Assert(parallel_max > 1);

	DRCheckStarted();

	initStringInfo(&buf);
	pq_sendbyte(&buf, ADB_DR_MQ_MSG_START_PLAN_PARALLEL);

	pq_sendbytes(&buf, (char*)&parallel_max, sizeof(parallel_max));
	pq_sendbyte(&buf, cache_on_disk);
	DRSerializePlanInfo(plan_id, seg, mq, sizeof(*mq)*parallel_max, desc, work_nodes, &buf);

	DRSendMsgToReduce(buf.data, buf.len, false);
	pfree(buf.data);

	DRRecvConfirmFromReduce(false);
}