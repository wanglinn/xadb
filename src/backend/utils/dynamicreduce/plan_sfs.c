#include "postgres.h"

#include "access/htup_details.h"
#include "common/hashfn.h"
#include "libpq/pqformat.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

#include "utils/dynamicreduce.h"
#include "utils/dr_private.h"

typedef struct OidBufFile
{
	Oid			oid;
	BufFile	   *buffile;
}OidBufFile;

static void ClearSFSPlanInfo(PlanInfo *pi);
static bool OnSFSPlanMessage(PlanInfo *pi, const char *data, int len, Oid nodeoid);
static bool OnSFSPlanNodeEndOfPlan(PlanInfo *pi, Oid nodeoid);
static void OnSFSPlanLatch(PlanInfo *pi);
static void CreateOidBufFiles(PlanInfo *pi, DynamicReduceSFS sfs);
static void DestroyOidBufFiles(void *ptr);
static BufFile *GetNodeBufFile(void *ptr, Oid nodeoid, int plan_id);
static void ExportNodeBufFile(void *ptr);

static void OnSFSPlanError(PlanInfo *pi)
{
	SetPlanFailedFunctions(pi, true, false);
}

static inline void SFSWriteTupleData(BufFile *file, uint32 len, const void *data)
{
	BufFileWrite(file, &len, sizeof(len));
	BufFileWrite(file, (void*)data, len);
}

static bool OnSFSPlanMessage(PlanInfo *pi, const char *data, int len, Oid nodeoid)
{
	BufFile *file = GetNodeBufFile(pi->pwi->private, nodeoid, pi->plan_id);
	SFSWriteTupleData(file, len, data);
	return true;
}

void DynamicReduceWriteSFSMsgTuple(struct BufFile *file, const char *data, int len)
{
	SFSWriteTupleData(file, len, data);
}

void DynamicReduceWriteSFSTuple(TupleTableSlot *slot, BufFile *file)
{
	bool			shouldFree;
	MinimalTuple	mtup = ExecFetchSlotMinimalTuple(slot, &shouldFree);
	SFSWriteTupleData(file,
					  mtup->t_len - MINIMAL_TUPLE_DATA_OFFSET,
					  (char*)mtup + MINIMAL_TUPLE_DATA_OFFSET);
	if (shouldFree)
		pfree(mtup);
}

void DynamicReduceWriteSFSMinTuple(struct BufFile *file, MinimalTuple mtup)
{
	SFSWriteTupleData(file,
					  mtup->t_len - MINIMAL_TUPLE_DATA_OFFSET,
					  (char*)mtup + MINIMAL_TUPLE_DATA_OFFSET);
}

bool DRReadSFSTupleData(struct BufFile *file, StringInfo buf)
{
	size_t			nread;
	int				len;

	nread = BufFileRead(file, &len, sizeof(len));
	if (nread == 0)
		return false;	/* end of file */
	if (nread != sizeof(len))
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from SFS plan file: %m")));
	}

	resetStringInfo(buf);
	enlargeStringInfo(buf, len);
	if (BufFileRead(file, buf->data, len) != len)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from SFS plan file: %m")));
	}

	buf->len = len;
	return true;
}

TupleTableSlot *DynamicReduceReadSFSTuple(TupleTableSlot *slot, BufFile *file, StringInfo buf)
{
	MinimalTuple	mtup;
	size_t			nread;
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

static bool OnSFSPlanNodeEndOfPlan(PlanInfo *pi, Oid nodeoid)
{
	PlanWorkerInfo *pwi = pi->pwi;

	DR_PLAN_DEBUG_EOF((errmsg("SFS plan %d(%p) got end of plan message from node %u",
							  pi->plan_id, pi, nodeoid)));
	Assert(oidBufferMember(&pi->working_nodes, nodeoid, NULL));
	appendOidBufferUniqueOid(&pi->end_of_plan_nodes, nodeoid);

	if (pi->end_of_plan_nodes.len == pi->working_nodes.len)
	{
		DR_PLAN_DEBUG_EOF((errmsg("SFS plan %d(%p) sending end of plan message", pi->plan_id, pi)));
		Assert(pwi->sendBuffer.len == 0);	/* before send EOF it is empty */
		ExportNodeBufFile(pwi->private);
		pwi->plan_send_state = DR_PLAN_SEND_GENERATE_EOF;
		DRSendPlanWorkerMessage(pwi, pi);
	}
	return true;

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
		   pwi->plan_recv_state == DR_PLAN_RECV_WORKING &&
		   pwi->last_msg_type == ADB_DR_MSG_INVALID)
	{
		if (DRRecvPlanWorkerMessage(pwi, pi) == false)
			break;
		msg_type = pwi->last_msg_type;

		if (msg_type == ADB_DR_MSG_END_OF_PLAN)
		{
			pwi->plan_recv_state = DR_PLAN_RECV_ENDED;
			DRGetEndOfPlanMessage(pi, pwi);
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

	PG_TRY();
	{
		oldcontext = MemoryContextSwitchTo(TopMemoryContext);

		pi = DRRestorePlanInfo(msg, (void**)&sfs, sizeof(*sfs), ClearSFSPlanInfo);
		Assert(DRPlanSearch(pi->plan_id, HASH_FIND, NULL) == pi);
		pq_getmsgend(msg);

		pwi = pi->pwi = MemoryContextAllocZero(TopMemoryContext, sizeof(PlanWorkerInfo));
		DRSetupPlanWorkInfo(pi, pwi, &sfs->mq, -1, DR_PLAN_RECV_WORKING);
		SharedFileSetAttach(&sfs->sfs, pi->seg);

		CreateOidBufFiles(pi, sfs);

		pi->OnNodeRecvedData = OnSFSPlanMessage;
		pi->OnLatchSet = OnSFSPlanLatch;
		pi->OnNodeIdle = OnDefaultPlanIdleNode;
		pi->OnNodeEndOfPlan = OnSFSPlanNodeEndOfPlan;
		pi->OnPlanError = OnSFSPlanError;
		pi->OnPreWait = OnDefaultPlanPreWait;

		MemoryContextSwitchTo(oldcontext);
		DR_PLAN_DEBUG((errmsg("SFS plan %d(%p) stared", pi->plan_id, pi)));
	}PG_CATCH();
	{
		ClearSFSPlanInfo(pi);
		PG_RE_THROW();
	}PG_END_TRY();

	Assert(pi != NULL);
	if (dr_status == DRS_FAILED)
		OnSFSPlanError(pi);
	DRActiveNode(pi->plan_id);
}

void DynamicReduceStartSharedFileSetPlan(int plan_id, struct dsm_segment *seg, DynamicReduceSFS sfs, List *work_nodes)
{
	StringInfoData	buf;
	Assert(plan_id >= 0);

	DRCheckStarted();

	initStringInfo(&buf);
	pq_sendbyte(&buf, ADB_DR_MQ_MSG_START_PLAN_SFS);

	DRSerializePlanInfo(plan_id, seg, sfs, sizeof(*sfs), work_nodes, &buf);

	DRSendMsgToReduce(buf.data, buf.len, false, false);
	pfree(buf.data);

	DRRecvConfirmFromReduce(false, false);
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
	if (DRPlanSearch(pi->plan_id, HASH_FIND, NULL) == pi)
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

static void CreateOidBufFiles(PlanInfo *pi, DynamicReduceSFS sfs)
{
	HTAB	   *htab;
	OidBufFile *buf;
	HASHCTL		ctl;
	uint32		i;
	char		name[MAXPGPATH];
	bool		found;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(OidBufFile);
	ctl.hash = oid_hash;
	ctl.hcxt = TopMemoryContext;
	htab = hash_create("Dynamic reduce SFS buffer file",
					   DR_HTAB_DEFAULT_SIZE,
					   &ctl,
					   HASH_ELEM|HASH_CONTEXT|HASH_FUNCTION);
	pi->pwi->private = htab;
	
	for (i=0;i<pi->working_nodes.len;++i)
	{
		Oid oid = pi->working_nodes.oids[i];
		if (oid == PGXCNodeOid)
			continue;

		buf = hash_search(htab, &oid, HASH_ENTER, &found);
		if (found)
		{
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("duplicate remote node oid %u for dynamic reduce SFS plan",
							oid)));
		}
		Assert(buf->oid == oid);
		buf->buffile = BufFileCreateShared(&sfs->sfs, DynamicReduceSFSFileName(name, oid));
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
				(errmsg("shared file set is not exist for node %u in plan %d",
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
