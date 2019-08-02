#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "libpq/pqformat.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

#include "utils/dynamicreduce.h"
#include "utils/dr_private.h"

typedef struct OidBufFile
{
	Oid			oid;
	BufFile	   *buffile;
}OidBufFile;

static void ClearSFSPlanInfo(PlanInfo *pi);
static bool OnSFSPlanConvertMessage(PlanInfo *pi, const char *data, int len, Oid nodeoid);
static bool OnSFSPlanMessage(PlanInfo *pi, const char *data, int len, Oid nodeoid);
static void OnSFSPlanIdleNode(PlanInfo *pi, WaitEvent *we, DRNodeEventData *ned);
static bool OnSFSPlanNodeEndOfPlan(PlanInfo *pi, Oid nodeoid);
static void OnSFSPlanPreWait(PlanInfo *pi);
static void OnSFSPlanLatch(PlanInfo *pi);
static void CreateOidBufFiles(void **ptr, DynamicReduceSFS sfs);
static void DestroyOidBufFiles(void *ptr);
static BufFile *GetNodeBufFile(void *ptr, Oid nodeoid, int plan_id);
static void ExportNodeBufFile(void *ptr);

static inline void SFSWriteTupleData(BufFile *file, uint32 len, const void *data)
{
	if (BufFileWrite(file, &len, sizeof(len)) != sizeof(len) ||
		BufFileWrite(file, (void*)data, len) != len)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to SFS plan file: %m")));
	}
}
static bool OnSFSPlanConvertMessage(PlanInfo *pi, const char *data, int len, Oid nodeoid)
{
	HeapTupleData	tup;
	MinimalTuple	mtup;
	MemoryContext	oldcontext;
	PlanWorkerInfo *pwi = pi->pwi;
	BufFile		   *file = GetNodeBufFile(pwi->private, nodeoid, pi->plan_id);

	MemoryContextReset(pi->convert_context);
	oldcontext = MemoryContextSwitchTo(pi->convert_context);

	DRStoreTypeConvertTuple(pwi->slot_node_src, data, len, &tup);
	do_type_convert_slot_in(pi->type_convert, pwi->slot_node_src, pwi->slot_node_dest, false);
	mtup = ExecFetchSlotMinimalTuple(pwi->slot_node_dest);
	ExecClearTuple(pwi->slot_node_src);

	SFSWriteTupleData(file,
					  mtup->t_len - MINIMAL_TUPLE_DATA_OFFSET,
					  (char*)mtup + MINIMAL_TUPLE_DATA_OFFSET);
	ExecClearTuple(pwi->slot_node_dest);
	MemoryContextSwitchTo(oldcontext);

	return true;
}

static bool OnSFSPlanMessage(PlanInfo *pi, const char *data, int len, Oid nodeoid)
{
	BufFile *file = GetNodeBufFile(pi->pwi->private, nodeoid, pi->plan_id);
	SFSWriteTupleData(file, len, data);
	return true;
}

void DynamicReduceWriteSFSTuple(TupleTableSlot *slot, BufFile *file)
{
	MinimalTuple	mtup = ExecFetchSlotMinimalTuple(slot);
	SFSWriteTupleData(file,
					  mtup->t_len - MINIMAL_TUPLE_DATA_OFFSET,
					  (char*)mtup + MINIMAL_TUPLE_DATA_OFFSET);
}

TupleTableSlot *DynamicReduceReadSFSTuple(TupleTableSlot *slot, BufFile *file, StringInfo buf)
{
	MinimalTuple	mtup;
	Size			nread;
	int				len;

	nread = BufFileRead(file, &len, sizeof(len));
	if (nread == 0)
		return ExecClearTuple(slot);	/* end of file */
	if (nread != sizeof(mtup->t_len))
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from SFS plan file: %m")));
	}

	enlargeStringInfo(buf, len+MINIMAL_TUPLE_DATA_OFFSET);
	mtup = (MinimalTuple)buf->data;
	if (BufFileRead(file, (char*)mtup + MINIMAL_TUPLE_DATA_OFFSET, len) != len)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from SFS plan file: %m")));
	}

	mtup->t_len = len + MINIMAL_TUPLE_DATA_OFFSET;
	buf->len = mtup->t_len;

	return ExecStoreMinimalTuple(mtup, slot, false);
}

static void OnSFSPlanIdleNode(PlanInfo *pi, WaitEvent *we, DRNodeEventData *ned)
{
	PlanWorkerInfo *pwi = pi->pwi;;
	if (pwi->last_msg_type == ADB_DR_MSG_INVALID)
		return;
	if (pwi->dest_oids[pwi->dest_cursor] != ned->nodeoid)
		return;

	DRSendWorkerMsgToNode(pwi, pi, ned);
}

static bool OnSFSPlanNodeEndOfPlan(PlanInfo *pi, Oid nodeoid)
{
	PlanWorkerInfo *pwi = pi->pwi;

	DR_PLAN_DEBUG_EOF((errmsg("SFS plan %d(%p) got end of plan message from node %u",
							  pi->plan_id, pi, nodeoid)));
	Assert(oidBufferMember(&dr_latch_data->work_oid_buf, nodeoid, NULL));
	appendOidBufferUniqueOid(&pi->end_of_plan_nodes, nodeoid);

	if (pi->end_of_plan_nodes.len == dr_latch_data->work_oid_buf.len)
	{
		DR_PLAN_DEBUG_EOF((errmsg("SFS plan %d(%p) sending end of plan message", pi->plan_id, pi)));
		ExportNodeBufFile(pwi->private);
		appendStringInfoChar(&pwi->sendBuffer, ADB_DR_MSG_END_OF_PLAN);
		DRSendPlanWorkerMessage(pwi, pi);
		pwi->end_of_plan_send = true;
	}
	return true;

}

static void OnSFSPlanPreWait(PlanInfo *pi)
{
	PlanWorkerInfo *pwi = pi->pwi;
	if (pwi->end_of_plan_recv &&
		pwi->end_of_plan_send &&
		pwi->last_msg_type == ADB_DR_MSG_INVALID &&
		pwi->sendBuffer.len == 0)
		ClearSFSPlanInfo(pi);
}

static void OnSFSPlanLatch(PlanInfo *pi)
{
	PlanWorkerInfo *pwi;
	uint32			msg_type;

	pwi = pi->pwi;
	if (pwi->sendBuffer.len)
	{
		DRSendPlanWorkerMessage(pwi, pi);
		/* don't need active NODE, we always result true when got message */
	}

	while (pwi->waiting_node == InvalidOid &&
		   pwi->end_of_plan_recv == false &&
		   pwi->last_msg_type == ADB_DR_MSG_INVALID)
	{
		if (DRRecvPlanWorkerMessage(pwi, pi) == false)
			break;
		msg_type = pwi->last_msg_type;

		if (msg_type == ADB_DR_MSG_END_OF_PLAN)
		{
			pwi->end_of_plan_recv = true;
			DRGetEndOfPlanMessage(pwi);
		}else
		{
			Assert(msg_type == ADB_DR_MSG_TUPLE);
		}

		/* send message to remote */
		DRSendWorkerMsgToNode(pwi, pi, NULL);
	}
}

void DRStartSFSPlanMessage(StringInfo msg)
{
	PlanInfo * volatile		pi = NULL;
	DynamicReduceSFS		sfs;
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

		pi = DRRestorePlanInfo(msg, (void**)&sfs, sizeof(*sfs), ClearSFSPlanInfo);
		if ((char*)sfs + DRSFSD_SIZE(sfs->nnode) > (char*)dsm_segment_address(pi->seg) + dsm_segment_map_length(pi->seg))
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid DSM length or SFS parameter")));
		}
		Assert(DRPlanSearch(pi->plan_id, HASH_FIND, NULL) == pi);
		pq_getmsgend(msg);

		pwi = pi->pwi = MemoryContextAllocZero(TopMemoryContext, sizeof(PlanWorkerInfo));
		DRSetupPlanWorkInfo(pi, pwi, &sfs->mq, -1);
		SharedFileSetAttach(&sfs->sfs, pi->seg);

		CurrentResourceOwner = oldowner;
		CreateOidBufFiles(&pwi->private, sfs);
		DRSetupPlanWorkTypeConvert(pi, pwi);

		pi->OnNodeRecvedData = pi->type_convert ? OnSFSPlanConvertMessage:OnSFSPlanMessage;
		pi->OnLatchSet = OnSFSPlanLatch;
		pi->OnNodeIdle = OnSFSPlanIdleNode;
		pi->OnNodeEndOfPlan = OnSFSPlanNodeEndOfPlan;
		pi->OnPlanError = ClearSFSPlanInfo;
		pi->OnPreWait = OnSFSPlanPreWait;

		MemoryContextSwitchTo(oldcontext);
		DR_PLAN_DEBUG((errmsg("SFS plan %d(%p) stared", pi->plan_id, pi)));
	}PG_CATCH();
	{
		ClearSFSPlanInfo(pi);
		PG_RE_THROW();
	}PG_END_TRY();

	Assert(pi != NULL);
	DRActiveNode(pi->plan_id);
}

void DynamicReduceStartSharedFileSetPlan(int plan_id, struct dsm_segment *seg, DynamicReduceSFS sfs, struct tupleDesc *desc)
{
	StringInfoData	buf;
	Assert(plan_id >= 0);

	DRCheckStarted();
	if (sfs->nnode == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid remote node length for dynamic reduce SFS plan")));
	}

	initStringInfo(&buf);
	pq_sendbyte(&buf, ADB_DR_MQ_MSG_START_PLAN_SFS);

	DRSerializePlanInfo(plan_id, seg, sfs, DRSFSD_SIZE(sfs->nnode), desc, &buf);

	DRSendMsgToReduce(buf.data, buf.len, false);
	pfree(buf.data);

	DRRecvConfirmFromReduce(false);
}

char* DynamicReduceSFSFileName(char *name, Oid nodeoid)
{
	snprintf(name, MAXPGPATH, "%u", nodeoid);
	return name;
}

static void ClearSFSPlanInfo(PlanInfo *pi)
{
	if (pi == NULL)
		return;
	DR_PLAN_DEBUG((errmsg("clean SFS plan %d(%p)", pi->plan_id, pi)));
	DRPlanSearch(pi->plan_id, HASH_REMOVE, NULL);
	if (pi->pwi)
	{
		DestroyOidBufFiles(pi->pwi->private);
		DRClearPlanWorkInfo(pi, pi->pwi);
		pfree(pi->pwi);
		pi->pwi = NULL;
	}
	DRClearPlanInfo(pi);
}

static void CreateOidBufFiles(void **ptr, DynamicReduceSFS sfs)
{
	HTAB	   *htab;
	OidBufFile *buf;
	HASHCTL		ctl;
	uint32		i;
	char		name[MAXPGPATH];
	bool		found;

	if (sfs->nnode != dr_latch_data->work_oid_buf.len+1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("remote node count not equal working count for SFS plan"),
				 errdetail("working count is %u, plan count is %u",
				 		   dr_latch_data->work_oid_buf.len, sfs->nnode)));
	}

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(OidBufFile);
	ctl.hash = oid_hash;
	ctl.hcxt = TopMemoryContext;
	*ptr = htab = hash_create("Dynamic reduce SFS buffer file",
							  DR_HTAB_DEFAULT_SIZE,
							  &ctl,
							  HASH_ELEM|HASH_CONTEXT|HASH_FUNCTION);
	
	for (i=0;i<sfs->nnode;++i)
	{
		if (sfs->nodes[i] == PGXCNodeOid)
			continue;

		if (oidBufferMember(&dr_latch_data->work_oid_buf, sfs->nodes[i], NULL) == false)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("remote node oid %u not in dynamic reduce working", sfs->nodes[i])));
		}
		buf = hash_search(htab, &sfs->nodes[i], HASH_ENTER, &found);
		if (found)
		{
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("duplicate remote node oid %u for dynamic reduce SFS plan", sfs->nodes[i])));
		}
		Assert(buf->oid == sfs->nodes[i]);
		buf->buffile = BufFileCreateShared(&sfs->sfs, DynamicReduceSFSFileName(name, sfs->nodes[i]));
	}
}

static void DestroyOidBufFiles(void *ptr)
{
	HASH_SEQ_STATUS	seq;
	OidBufFile	   *buf;

	hash_seq_init(&seq, ptr);
	while((buf=hash_seq_search(&seq)) != NULL)
	{
		if (buf->buffile)
			BufFileClose(buf->buffile);
	}
	hash_destroy(ptr);
}

static BufFile *GetNodeBufFile(void *ptr, Oid nodeoid, int plan_id)
{
	OidBufFile *buf = hash_search(ptr, &nodeoid, HASH_FIND, NULL);
	if (buf == NULL)
	{
		ereport(ERROR,
				(errmsg("shared file set is exist for node %u in plan %d",
						nodeoid, plan_id)));
	}
	return buf->buffile;
}

static void ExportNodeBufFile(void *ptr)
{
	HASH_SEQ_STATUS	seq;
	OidBufFile	   *buf;

	hash_seq_init(&seq, ptr);
	while((buf=hash_seq_search(&seq)) != NULL)
		BufFileExportShared(buf->buffile);
}
