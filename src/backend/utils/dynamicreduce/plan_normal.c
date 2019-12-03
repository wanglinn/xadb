#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "libpq/pqformat.h"
#include "storage/buffile.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

#include "utils/dynamicreduce.h"
#include "utils/dr_private.h"

typedef struct NormalSharedFile
{
	BufFile	   *buffile;
	uint32		file_number;
}NormalSharedFile;

static bool OnNormalPlanMessage(PlanInfo *pi, const char *data, int len, Oid nodeoid);
static bool OnNormalPlanNodeEndOfPlan(PlanInfo *pi, Oid nodeoid);

static void ClearNormalPlanInfo(PlanInfo *pi)
{
	if (pi == NULL)
		return;
	DR_PLAN_DEBUG((errmsg("clean normal plan %d(%p)", pi->plan_id, pi)));
	DRPlanSearch(pi->plan_id, HASH_REMOVE, NULL);
	if (pi->pwi)
	{
		DRClearPlanWorkInfo(pi, pi->pwi);
		pfree(pi->pwi);
		pi->pwi = NULL;
	}
	if (pi->private)
	{
		NormalSharedFile *sf = pi->private;
		if (sf->buffile)
			BufFileClose(sf->buffile);
		pfree(sf);
		pi->private = NULL;
	}
	DRClearPlanInfo(pi);
}

static inline BufFile* NormalPlanGetSharedFile(NormalSharedFile *sf)
{
	char			name[MAXPGPATH];
	MemoryContext	oldcontext;
	
	if (sf->buffile == NULL)
	{
		oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(sf));

		sf->file_number = DRNextSharedFileSetNumber();
		sf->buffile = BufFileCreateShared(dr_shared_fs,
										  DynamicReduceSharedFileName(name, sf->file_number));
		MemoryContextSwitchTo(oldcontext);
	}

	return sf->buffile;
}

static inline MinimalTuple NormalPlanConvertTuple(PlanInfo *pi, const char *data, int len)
{
	HeapTupleData	tup;
	MinimalTuple	mtup;
	MemoryContext	oldcontext;
	PlanWorkerInfo *pwi = pi->pwi;

	MemoryContextReset(pi->convert_context);
	oldcontext = MemoryContextSwitchTo(pi->convert_context);

	DRStoreTypeConvertTuple(pwi->slot_node_src, data, len, &tup);
	do_type_convert_slot_in(pi->type_convert, pwi->slot_node_src, pwi->slot_node_dest, false);
	mtup = ExecFetchSlotMinimalTuple(pwi->slot_node_dest);
	ExecClearTuple(pwi->slot_node_src);

	MemoryContextSwitchTo(oldcontext);

	return mtup;
}

static bool OnNormalPlanMessage(PlanInfo *pi, const char *data, int len, Oid nodeoid)
{
	PlanWorkerInfo *pwi = pi->pwi;

	CHECK_WORKER_IN_WROKING(pwi, pi);

	/* we only cache one tuple */
	if (pwi->sendBuffer.len != 0)
	{
		if (pi->private)
		{
			/* cache on disk */
			BufFile *file = NormalPlanGetSharedFile(pi->private);
			Assert(file != NULL);

			if (pi->type_convert)
			{
				MinimalTuple mtup = NormalPlanConvertTuple(pi, data, len);
				DynamicReduceWriteSFSMinTuple(file, mtup);
			}else
			{
				DynamicReduceWriteSFSMsgTuple(file, data, len);
			}
			DR_PLAN_DEBUG((errmsg("normal plan %d(%p) cache a tuple from %u length %d",
								  pi->plan_id, pi, nodeoid, len)));
			return true;
		}
		return false;
	}

	DR_PLAN_DEBUG((errmsg("normal plan %d(%p) got a tuple from %u length %d",
						  pi->plan_id, pi, nodeoid, len)));
	appendStringInfoChar(&pwi->sendBuffer, ADB_DR_MSG_TUPLE);
	appendStringInfoSpaces(&pwi->sendBuffer, sizeof(nodeoid)-sizeof(char));	/* for align */
	appendBinaryStringInfoNT(&pwi->sendBuffer, (char*)&nodeoid, sizeof(nodeoid));
	if (pi->type_convert)
	{
		MinimalTuple mtup = NormalPlanConvertTuple(pi, data, len);

		appendBinaryStringInfoNT(&pwi->sendBuffer,
								 (char*)mtup + MINIMAL_TUPLE_DATA_OFFSET,
								 mtup->t_len - MINIMAL_TUPLE_DATA_OFFSET);
	}else
	{
		appendBinaryStringInfoNT(&pwi->sendBuffer, data, len);
	}

	DRSendPlanWorkerMessage(pwi, pi);

	return true;
}

static bool OnNormalPlanNodeEndOfPlan(PlanInfo *pi, Oid nodeoid)
{
	PlanWorkerInfo *pwi = pi->pwi;
	if (pwi->sendBuffer.len != 0)
		return false;

	DR_PLAN_DEBUG_EOF((errmsg("normal plan %d(%p) got end of plan message from node %u",
							  pi->plan_id, pi, nodeoid)));
	Assert(oidBufferMember(&pi->working_nodes, nodeoid, NULL));
	appendOidBufferUniqueOid(&pi->end_of_plan_nodes, nodeoid);

	if (pi->end_of_plan_nodes.len == pi->working_nodes.len)
	{
		DR_PLAN_DEBUG_EOF((errmsg("normal plan %d(%p) sending end of plan message", pi->plan_id, pi)));
		pwi->plan_send_state = DR_PLAN_SEND_GENERATE_CACHE;
		if (pwi->sendBuffer.len == 0)
		{
			/* when not busy, generate and send message immediately */
			DRSendPlanWorkerMessage(pwi, pi);
		}
	}
	return true;
}

static bool GenerateNormalCacheMessage(PlanWorkerInfo *pwi, PlanInfo *pi)
{
	NormalSharedFile *sf = (NormalSharedFile*)pi->private;
	Assert(pwi->sendBuffer.len == 0);
	Assert(pwi->plan_send_state == DR_PLAN_SEND_GENERATE_CACHE);
	if (sf->buffile != NULL)
	{
		BufFileExportShared(sf->buffile);
		appendStringInfoChar(&pwi->sendBuffer, ADB_DR_MSG_SHARED_FILE_NUMBER);
		appendStringInfoSpaces(&pwi->sendBuffer, sizeof(sf->file_number)-sizeof(char)); /* align */
		appendBinaryStringInfoNT(&pwi->sendBuffer, (char*)&sf->file_number, sizeof(sf->file_number));
		return true;
	}
	return false;
}

void DRStartNormalPlanMessage(StringInfo msg)
{
	PlanInfo * volatile		pi = NULL;
	DynamicReduceMQ			mq;
	PlanWorkerInfo		   *pwi;
	MemoryContext			oldcontext;
	ResourceOwner			oldowner;

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

		pi = DRRestorePlanInfo(msg, (void**)&mq, sizeof(*mq), ClearNormalPlanInfo);
		Assert(DRPlanSearch(pi->plan_id, HASH_FIND, NULL) == pi);
		if (pq_getmsgbyte(msg))
		{
			/* cache on disk */
			pi->private = MemoryContextAllocZero(TopMemoryContext,
												 sizeof(NormalSharedFile));
			pi->GenerateCacheMsg = GenerateNormalCacheMessage;
		}
		pq_getmsgend(msg);

		pwi = pi->pwi = MemoryContextAllocZero(TopMemoryContext, sizeof(PlanWorkerInfo));
		DRSetupPlanWorkInfo(pi, pwi, mq, -1, DR_PLAN_RECV_WORKING);

		CurrentResourceOwner = oldowner;
		DRSetupPlanWorkTypeConvert(pi, pwi);

		pi->OnLatchSet = OnDefaultPlanLatch;
		pi->OnNodeRecvedData = OnNormalPlanMessage;
		pi->OnNodeIdle = OnDefaultPlanIdleNode;
		pi->OnNodeEndOfPlan = OnNormalPlanNodeEndOfPlan;
		pi->OnPlanError = ClearNormalPlanInfo;
		pi->OnPreWait = OnDefaultPlanPreWait;

		MemoryContextSwitchTo(oldcontext);
		DR_PLAN_DEBUG((errmsg("normal plan %d(%p) stared", pi->plan_id, pi)));
	}PG_CATCH();
	{
		ClearNormalPlanInfo(pi);
		PG_RE_THROW();
	}PG_END_TRY();

	Assert(pi != NULL);
	DRActiveNode(pi->plan_id);
}

void DynamicReduceStartNormalPlan(int plan_id, dsm_segment *seg, DynamicReduceMQ mq, TupleDesc desc, List *work_nodes, bool cache_on_disk)
{
	StringInfoData	buf;
	Assert(plan_id >= 0);

	DRCheckStarted();

	initStringInfo(&buf);
	pq_sendbyte(&buf, ADB_DR_MQ_MSG_START_PLAN_NORMAL);

	DRSerializePlanInfo(plan_id, seg, mq, sizeof(*mq), desc, work_nodes, &buf);
	pq_sendbyte(&buf, cache_on_disk);

	DRSendMsgToReduce(buf.data, buf.len, false);
	pfree(buf.data);

	DRRecvConfirmFromReduce(false);
}
