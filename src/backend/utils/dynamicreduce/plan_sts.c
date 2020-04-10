#include "postgres.h"

#include "access/htup_details.h"
#include "libpq/pqformat.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

#include "utils/dynamicreduce.h"
#include "utils/dr_private.h"

typedef struct OidAccessor
{
	Oid			oid;
	SharedTuplestoreAccessor
			   *accessor;
}OidAccessor;

static void CreateOidAccessor(PlanInfo *pi, DynamicReduceSTS sts, int npart, int mypart);
static SharedTuplestoreAccessor* GetNodeAccessor(void *ptr, Oid nodeoid, int plan_id);
static void EndSTSPut(void *ptr);

static void ClearSTSPlanInfo(PlanInfo *pi)
{
	if (pi == NULL)
		return;
	DR_PLAN_DEBUG((errmsg("clean STS plan %d(%p)", pi->plan_id, pi)));
	if (DRPlanSearch(pi->plan_id, HASH_FIND, NULL) == pi)
		DRPlanSearch(pi->plan_id, HASH_REMOVE, NULL);
	if (pi->pwi)
	{
		if (pi->pwi->private)
			EndSTSPut(pi->pwi->private);
		DRClearPlanWorkInfo(pi, pi->pwi);
		pfree(pi->pwi);
		pi->pwi = NULL;
	}
	if (pi->sts_context)
		MemoryContextDelete(pi->sts_context);
	DRClearPlanInfo(pi);
}

static void OnSTSPlanError(PlanInfo *pi)
{
	SetPlanFailedFunctions(pi, true, false);
}

static void OnSTSPlanLatch(PlanInfo *pi)
{
	PlanWorkerInfo *pwi;
	uint32			msg_type;

	pwi = pi->pwi;
	DRSendPlanWorkerMessage(pwi, pi);

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

static MinimalTuple GetSTSBufferTuple(PlanInfo *pi, const char *data, int len)
{
	MinimalTuple	mtup;
	uint32			tup_len = len+MINIMAL_TUPLE_DATA_OFFSET;

	enlargeStringInfo(&pi->sts_tup_buf, tup_len);
	mtup = (MinimalTuple)pi->sts_tup_buf.data;
	mtup->t_len = tup_len;
	memcpy(pi->sts_tup_buf.data + MINIMAL_TUPLE_DATA_OFFSET, data, len);

	return mtup;
}

static bool OnSTSPlanMessage(PlanInfo *pi, const char *data, int len, Oid nodeoid)
{
	MinimalTuple mtup = GetSTSBufferTuple(pi, data, len);
	SharedTuplestoreAccessor *accessor = GetNodeAccessor(pi->pwi->private, nodeoid, pi->plan_id);
	MemoryContext oldcontext = MemoryContextSwitchTo(pi->sts_context);
	sts_puttuple(accessor, NULL, mtup);
	MemoryContextSwitchTo(oldcontext);

	return true;
}

static bool OnSTSPlanNodeEndOfPlan(PlanInfo *pi, Oid nodeoid)
{
	PlanWorkerInfo *pwi = pi->pwi;
	if (pwi->sendBuffer.len != 0)
		return false;

	DR_PLAN_DEBUG_EOF((errmsg("STS plan %d(%p) got end of plan message from node %u",
							  pi->plan_id, pi, nodeoid)));
	Assert(oidBufferMember(&pi->working_nodes, nodeoid, NULL));
	appendOidBufferUniqueOid(&pi->end_of_plan_nodes, nodeoid);

	if (pi->end_of_plan_nodes.len == pi->working_nodes.len)
	{
		DR_PLAN_DEBUG_EOF((errmsg("STS plan %d(%p) sending end of plan message", pi->plan_id, pi)));
		EndSTSPut(pwi->private);
		Assert(pwi->sendBuffer.len == 0);	/* before send EOF it is empty */
		pwi->plan_send_state = DR_PLAN_SEND_GENERATE_EOF;
		DRSendPlanWorkerMessage(pwi, pi);
	}
	return true;
}

void DRStartSTSPlanMessage(StringInfo msg)
{
	PlanInfo * volatile		pi = NULL;
	DynamicReduceSTS		sts;
	PlanWorkerInfo		   *pwi;
	MemoryContext			oldcontext;
	int						npart,mypart;

	PG_TRY();
	{
		oldcontext = MemoryContextSwitchTo(TopMemoryContext);

		pq_copymsgbytes(msg, (char*)&npart, sizeof(npart));
		pq_copymsgbytes(msg, (char*)&mypart, sizeof(mypart));
		if (npart <= 0 ||
			mypart < 0 ||
			mypart >= npart)
		{
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("invalid argument for start shared tuplestore plan")));
		}

		pi = DRRestorePlanInfo(msg, (void**)&sts, sizeof(*sts), ClearSTSPlanInfo);
		Assert(DRPlanSearch(pi->plan_id, HASH_FIND, NULL) == pi);
		if (((char*)sts) + DRSTSD_SIZE(npart, pi->working_nodes.len + 1) - (char*)dsm_segment_address(pi->seg) >
			dsm_segment_map_length(pi->seg))
		{
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("invalid argument for start shared tuplestore plan")));
		}
		pq_getmsgend(msg);

		pwi = pi->pwi = MemoryContextAllocZero(TopMemoryContext, sizeof(PlanWorkerInfo));
		DRSetupPlanWorkInfo(pi, pwi, &sts->sfs.mq, -1, DR_PLAN_RECV_WORKING);
		SharedFileSetAttach(&sts->sfs.sfs, pi->seg);

		CreateOidAccessor(pi, sts, npart, mypart);

		pi->OnNodeRecvedData = OnSTSPlanMessage;
		pi->OnLatchSet = OnSTSPlanLatch;
		pi->OnNodeIdle = OnDefaultPlanIdleNode;
		pi->OnNodeEndOfPlan = OnSTSPlanNodeEndOfPlan;
		pi->OnPlanError = OnSTSPlanError;
		pi->OnPreWait = OnDefaultPlanPreWait;

		MemoryContextSwitchTo(oldcontext);
		DR_PLAN_DEBUG((errmsg("STS plan %d(%p) stared", pi->plan_id, pi)));
	}PG_CATCH();
	{
		ClearSTSPlanInfo(pi);
		PG_RE_THROW();
	}PG_END_TRY();

	Assert(pi != NULL);
	if (dr_status == DRS_FAILED)
		OnSTSPlanError(pi);
	DRActiveNode(pi->plan_id);
}

void DynamicReduceStartSharedTuplestorePlan(int plan_id, struct dsm_segment *seg,
											DynamicReduceSTS sts, List *work_nodes,
											int npart, int reduce_part)
{
	StringInfoData	buf;
	Assert(plan_id >= 0);
	Assert(npart > 0);
	Assert(reduce_part >= 0 && reduce_part < npart);

	DRCheckStarted();

	initStringInfo(&buf);
	pq_sendbyte(&buf, ADB_DR_MQ_MSG_START_PLAN_STS);

	pq_sendbytes(&buf, (char*)&npart, sizeof(npart));
	pq_sendbytes(&buf, (char*)&reduce_part, sizeof(reduce_part));

	DRSerializePlanInfo(plan_id, seg, sts,
						DRSTSD_SIZE(npart, list_length(work_nodes)),
						work_nodes, &buf);

	DRSendMsgToReduce(buf.data, buf.len, false, false);
	pfree(buf.data);

	DRRecvConfirmFromReduce(false, false);
}

static void CreateOidAccessor(PlanInfo *pi, DynamicReduceSTS sts, int npart, int mypart)
{
	MemoryContext	oldcontext;
	OidAccessor	   *buf;
	HTAB		   *htab;
	HASHCTL			ctl;
	uint32			i;
	bool			found;

	Assert(pi->sts_context == NULL);
	pi->sts_context = AllocSetContextCreate(TopMemoryContext,
											"Dynamic reduce shared tuplestore",
											ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(pi->sts_context);

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(OidAccessor);
	ctl.hash = oid_hash;
	ctl.hcxt = pi->sts_context;
	htab = hash_create("Dynamic reduce STS buffer",
					   DR_HTAB_DEFAULT_SIZE,
					   &ctl,
					   HASH_ELEM|HASH_CONTEXT|HASH_FUNCTION);
	Assert(pi->pwi->private == NULL);
	pi->pwi->private = htab;

	for (i=0;i<pi->working_nodes.len;++i)
	{
		Assert(pi->working_nodes.oids[i] != PGXCNodeOid);

		buf = hash_search(htab, &pi->working_nodes.oids[i], HASH_ENTER, &found);
		if (found)
		{
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("duplicate remote node oid %u for dynamic reduce SFS plan",
							pi->working_nodes.oids[i])));
		}
		Assert(buf->oid == pi->working_nodes.oids[i]);
		buf->accessor = sts_attach(DRSTSD_ADDR(sts->sts, npart, i),
								   mypart,
								   &sts->sfs.sfs);
	}

	initStringInfo(&pi->sts_tup_buf);
	enlargeStringInfo(&pi->sts_tup_buf, MINIMAL_TUPLE_DATA_OFFSET);
	memset(pi->sts_tup_buf.data, 0, MINIMAL_TUPLE_DATA_OFFSET);

	MemoryContextSwitchTo(oldcontext);
}

static SharedTuplestoreAccessor* GetNodeAccessor(void *ptr, Oid nodeoid, int plan_id)
{
	OidAccessor *buf = hash_search(ptr, &nodeoid, HASH_FIND, NULL);
	if (buf == NULL)
	{
		ereport(ERROR,
				(errmsg("shared tuplestore is not exist for node %u in plan %d",
						nodeoid, plan_id)));
	}
	return buf->accessor;
}

static void EndSTSPut(void *ptr)
{
	HASH_SEQ_STATUS	seq;
	OidAccessor	   *buf;

	hash_seq_init(&seq, ptr);
	while((buf=hash_seq_search(&seq)) != NULL)
		sts_end_write(buf->accessor);
}