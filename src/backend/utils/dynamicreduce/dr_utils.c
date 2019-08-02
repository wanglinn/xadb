#include "postgres.h"

#include "access/parallel.h"
#include "access/session.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "libpq/pqformat.h"
#include "libpq/pqmq.h"
#include "miscadmin.h"
#include "storage/shm_toc.h"
#include "utils/builtins.h"
#include "utils/combocid.h"
#include "utils/memutils.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/snapmgr.h"

#include "utils/dynamicreduce.h"
#include "utils/dr_private.h"

#define DYNAMIC_REDUCE_MAGIC		0xf413bf2c
#define DR_KEY_FIXED_STATE			UINT64CONST(0xFFFFFFFFFFFF0001)
#define DR_KEY_CONNECT_INFO			UINT64CONST(0xFFFFFFFFFFFF0002)
#define DR_KEY_LIBRARY				UINT64CONST(0xFFFFFFFFFFFF0003)
#define DR_KEY_GUC					UINT64CONST(0xFFFFFFFFFFFF0004)
#define DR_KEY_COMBO_CID			UINT64CONST(0xFFFFFFFFFFFF0005)
#define DR_KEY_TRANSACTION_SNAPSHOT	UINT64CONST(0xFFFFFFFFFFFF0006)
#define DR_KEY_ACTIVE_SNAPSHOT		UINT64CONST(0xFFFFFFFFFFFF0007)
#define DR_KEY_TRANSACTION_STATE	UINT64CONST(0xFFFFFFFFFFFF0008)
#define DR_KEY_REINDEX_STATE		UINT64CONST(0xFFFFFFFFFFFF000B)

typedef struct DRFixedState
{
	PGPROC *master_proc;
	Oid		outer_user_id;
	Oid		current_user_id;
	Oid		temp_namespace_id;
	Oid		temp_toast_namespace_id;
	int		sec_context;
	bool	is_superuser;
}DRFixedState;

static dsm_segment *seg_utils = NULL;
static OidBuffer	cur_working_nodes = NULL;
static bool			session_attached = false;
static bool			in_parallel_transaction = false;

bool DynamicReduceHandleMessage(void *data, Size len)
{
	ErrorData		edata;
	StringInfoData	msg;
	char			msgtype;

	msg.data = data;
	msg.maxlen = msg.len = len;
	msg.cursor = 0;

	msgtype = pq_getmsgbyte(&msg);
	switch(msgtype)
	{
	case 'E':	/* ErrorResponse */
	case 'N':	/* NoticeResponse */
		/* Parse ErrorResponse or NoticeResponse. */
		pq_parse_errornotice(&msg, &edata);

		/* Death of a worker isn't enough justification for suicide. */
		edata.elevel = Min(edata.elevel, ERROR);

		if (edata.context)
			edata.context = psprintf("%s\n%s", edata.context, _("dynamic reduce"));
		else
			edata.context = pstrdup(_("dynamic reduce"));
		
		ThrowErrorData(&edata);
		return true;
	default:
		/* ignore other message for now */
		break;
	}
	return false;
}
/* ============ serialize and restore struct DynamicReduceNodeInfo ============= */
static void SerializeOneDynamicReduceNodeInfo(StringInfo buf, const DynamicReduceNodeInfo *info)
{
	appendBinaryStringInfoNT(buf, (const char*)info, offsetof(DynamicReduceNodeInfo, host));
	appendStringInfoString(buf, NameStr(info->host));
	buf->len++;	/* include '\0' */
	appendStringInfoString(buf, NameStr(info->name));
	buf->len++; /* same idea include '\0' */
}

static void RestoreOneDynamicReduceNodeInfo(StringInfo buf, DynamicReduceNodeInfo *info)
{
	pq_copymsgbytes(buf, (char*)info, offsetof(DynamicReduceNodeInfo, host));
	namestrcpy(&info->host, pq_getmsgrawstring(buf));
	namestrcpy(&info->name, pq_getmsgrawstring(buf));
}

void SerializeDynamicReduceNodeInfo(StringInfo buf, const DynamicReduceNodeInfo *info, uint32 count)
{
	uint32 i;
	if (count == 0)
		return;

	appendBinaryStringInfoNT(buf, (char*)&count, sizeof(count));
	for (i=0;i<count;++i)
		SerializeOneDynamicReduceNodeInfo(buf, &info[i]);
}

uint32 RestoreDynamicReduceNodeInfo(StringInfo buf, DynamicReduceNodeInfo **info)
{
	DynamicReduceNodeInfo *pinfo;
	uint32 count;
	uint32 i;

	pq_copymsgbytes(buf, (char*)&count, sizeof(count));
	pinfo = palloc0(sizeof(*pinfo)*count);

	for(i=0;i<count;++i)
		RestoreOneDynamicReduceNodeInfo(buf, &pinfo[i]);
	*info = pinfo;

	return count;
}

/* ================== serialize and restore TupleDesc =================================== */
void SerializeTupleDesc(StringInfo buf, TupleDesc desc)
{
	appendBinaryStringInfoNT(buf, (char*)&desc->natts, sizeof(desc->natts));
	appendStringInfoChar(buf, desc->tdhasoid);

	appendBinaryStringInfoNT(buf,
							 (char*)desc->attrs,
							 sizeof(desc->attrs[0]) * desc->natts);
}

TupleDesc RestoreTupleDesc(StringInfo buf)
{
	TupleDesc	desc;
	int			natts;
	bool		hasoid;

	pq_copymsgbytes(buf, (char*)&natts, sizeof(natts));
	hasoid = (bool)pq_getmsgbyte(buf);

	desc = CreateTemplateTupleDesc(natts, hasoid);
	pq_copymsgbytes(buf, (char*)desc->attrs, sizeof(desc->attrs[0]) * natts);

	return desc;
}

/* ===================== connect message =============================== */
void DynamicReduceConnectNet(const DynamicReduceNodeInfo *info, uint32 count)
{
	shm_toc_estimator	estimator;
	shm_toc			   *toc;
	dsm_segment		   *seg;
	char			   *ptr;
	DRFixedState	   *fs;
	dsm_handle			dsm_session;
	dsm_handle			dsm_seg;

	Size				library_len = 0;
	Size				guc_len = 0;
	Size				combocidlen = 0;
	Size				tsnaplen = 0;
	Size				asnaplen = 0;
	Size				tstatelen = 0;
	Size				reindexlen = 0;
	Size				infolen = 0;
	Size				segsize = 0;
	Snapshot			transaction_snapshot = GetTransactionSnapshot();
	Snapshot			active_snapshot = GetActiveSnapshot();
	StringInfoData		buf;
	uint32				i;

	DRCheckStarted();

	if (count < 2)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("dynamic reduce got %u node info,"
						"there should be at least 2", count)));

	seg = NULL;
	dsm_session = GetSessionDsmHandle();
	if (dsm_session == DSM_HANDLE_INVALID)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("can not alloc shared memory for dynamic reduce session")));
	}

	shm_toc_initialize_estimator(&estimator);

	/* Estimate space for various kinds of state sharing. */
	library_len = EstimateLibraryStateSpace();
	shm_toc_estimate_chunk(&estimator, library_len);
	guc_len = EstimateGUCStateSpace();
	shm_toc_estimate_chunk(&estimator, guc_len);
	combocidlen = EstimateComboCIDStateSpace();
	shm_toc_estimate_chunk(&estimator, combocidlen);
	tsnaplen = EstimateSnapshotSpace(transaction_snapshot);
	shm_toc_estimate_chunk(&estimator, tsnaplen);
	asnaplen = EstimateSnapshotSpace(active_snapshot);
	shm_toc_estimate_chunk(&estimator, asnaplen);
	tstatelen = EstimateTransactionStateSpace();
	shm_toc_estimate_chunk(&estimator, tstatelen);
	reindexlen = EstimateReindexStateSpace();
	shm_toc_estimate_chunk(&estimator, reindexlen);
	/* If you add more chunks here, you probably need to add keys. */
	shm_toc_estimate_keys(&estimator, 7);

	shm_toc_estimate_chunk(&estimator, sizeof(*fs));
	shm_toc_estimate_keys(&estimator, 1);

	infolen = sizeof(info[0])*count + sizeof(Size);
	shm_toc_estimate_chunk(&estimator, infolen);
	shm_toc_estimate_keys(&estimator, 1);

	/* create shared memory */
	segsize = shm_toc_estimate(&estimator);
	seg = dsm_create(segsize, 0);
	toc = shm_toc_create(DYNAMIC_REDUCE_MAGIC,
						 dsm_segment_address(seg),
						 segsize);

	/* Serialize shared libraries we have loaded. */
	ptr = shm_toc_allocate(toc, library_len);
	SerializeLibraryState(library_len, ptr);
	shm_toc_insert(toc, DR_KEY_LIBRARY, ptr);

	/* Serialize GUC settings. */
	ptr = shm_toc_allocate(toc, guc_len);
	SerializeGUCState(guc_len, ptr);
	shm_toc_insert(toc, DR_KEY_GUC, ptr);

	/* Serialize combo CID state. */
	ptr = shm_toc_allocate(toc, combocidlen);
	SerializeComboCIDState(combocidlen, ptr);
	shm_toc_insert(toc, DR_KEY_COMBO_CID, ptr);

	/* Serialize transaction snapshot and active snapshot. */
	ptr = shm_toc_allocate(toc, tsnaplen);
	SerializeSnapshot(transaction_snapshot, ptr);
	shm_toc_insert(toc, DR_KEY_TRANSACTION_SNAPSHOT, ptr);
	ptr = shm_toc_allocate(toc, asnaplen);
	SerializeSnapshot(active_snapshot, ptr);
	shm_toc_insert(toc, DR_KEY_ACTIVE_SNAPSHOT, ptr);

	/* Serialize transaction state. */
	ptr = shm_toc_allocate(toc, tstatelen);
	SerializeTransactionState(tstatelen, ptr);
	shm_toc_insert(toc, DR_KEY_TRANSACTION_STATE, ptr);

	/* Serialize reindex state. */
	ptr = shm_toc_allocate(toc, reindexlen);
	SerializeReindexState(reindexlen, ptr);
	shm_toc_insert(toc, DR_KEY_REINDEX_STATE, ptr);

	/* Serialize fixed state */
	fs = shm_toc_allocate(toc, sizeof(MyProc));
	fs->master_proc = MyProc;
	fs->outer_user_id = GetCurrentRoleId();
	GetUserIdAndSecContext(&fs->current_user_id, &fs->sec_context);
	GetTempNamespaceState(&fs->temp_namespace_id, &fs->temp_toast_namespace_id);
	fs->is_superuser = session_auth_is_superuser;
	shm_toc_insert(toc, DR_KEY_FIXED_STATE, fs);

	/* Serialize DynamicReduceNodeInfo */
	ptr = shm_toc_allocate(toc, infolen);
	*(Size*)ptr = count;
	memcpy(ptr + sizeof(Size), info, sizeof(info[0])*count);
	shm_toc_insert(toc, DR_KEY_CONNECT_INFO, ptr);

	dsm_seg = dsm_segment_handle(seg);
	initStringInfoExtend(&buf, 20);
	pq_sendbyte(&buf, ADB_DR_MQ_MSG_CONNECT);
	pq_sendbytes(&buf, (char*)&dsm_session, sizeof(dsm_session));
	pq_sendbytes(&buf, (char*)&dsm_seg, sizeof(dsm_seg));
	DRSendMsgToReduce(buf.data, buf.len, false);
	pfree(buf.data);

	if (cur_working_nodes == NULL)
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		cur_working_nodes = makeOidBuffer(OID_BUF_DEF_SIZE);
		MemoryContextSwitchTo(oldcontext);
	}
	resetOidBuffer(cur_working_nodes);
	for (i=0;i<count;++i)
		appendOidBufferOid(cur_working_nodes, info[i].node_oid);

	DRRecvConfirmFromReduce(false);
	dsm_detach(seg);
}

void DRConnectNetMsg(StringInfo msg)
{
	dsm_handle		dsm_session;
	dsm_handle		dsm_utils;
	shm_toc		   *toc;
	char		   *ptr;
	DRFixedState   *fs;
	DynamicReduceNodeInfo *info;
	ResourceOwner	oldowner;

	if (seg_utils != NULL)
	{
		ereport(ERROR,
				(errmsg("last connected dynamic shard memory in dynamic reduce is valid")));
	}
	if (MyDatabaseId == InvalidOid)
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("dynamic reduce not got startup message")));
	}

	pq_copymsgbytes(msg, (char*)&dsm_session, sizeof(dsm_session));
	pq_copymsgbytes(msg, (char*)&dsm_utils, sizeof(dsm_utils));
	pq_getmsgend(msg);

	oldowner = CurrentResourceOwner;
	CurrentResourceOwner = NULL;

	seg_utils = dsm_attach(dsm_utils);
	if (seg_utils == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not map dynamic shared memory segment")));
	toc = shm_toc_attach(DYNAMIC_REDUCE_MAGIC, dsm_segment_address(seg_utils));
	if (toc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("invalid magic number in dynamic shared memory segment")));

	/* Attach to the per-session DSM segment and contained objects. */
	AttachSession(dsm_session);
	session_attached = true;

	CurrentResourceOwner = oldowner;

	fs = shm_toc_lookup(toc, DR_KEY_FIXED_STATE, false);

	InitializingParallelWorker = true;

	/*
	 * Load libraries that were loaded by original backend.  We want to do
	 * this before restoring GUCs, because the libraries might define custom
	 * variables.
	 */
	RestoreLibraryState(shm_toc_lookup(toc, DR_KEY_LIBRARY, false));

	/* Restore GUC values from launching backend. */
	StartTransactionCommand();
	RestoreGUCState(shm_toc_lookup(toc, DR_KEY_GUC, false));
	CommitTransactionCommand();

	/* Crank up a transaction state appropriate to a parallel worker. */
	StartParallelWorkerTransaction(shm_toc_lookup(toc, DR_KEY_TRANSACTION_STATE, false));
	in_parallel_transaction = true;

	/* Restore combo CID state. */
	RestoreComboCIDState(shm_toc_lookup(toc, DR_KEY_COMBO_CID, false));

	/* Restore transaction snapshot. */
	ptr = shm_toc_lookup(toc, DR_KEY_TRANSACTION_SNAPSHOT, false);
	RestoreTransactionSnapshot(RestoreSnapshot(ptr),
							   fs->master_proc);

	/* Restore active snapshot. */
	PushActiveSnapshot(RestoreSnapshot(shm_toc_lookup(toc, DR_KEY_ACTIVE_SNAPSHOT, false)));

	/*
	 * We've changed which tuples we can see, and must therefore invalidate
	 * system caches.
	 */
	InvalidateSystemCaches();

	/*
	 * Restore current role id.  Skip verifying whether session user is
	 * allowed to become this role and blindly restore the leader's state for
	 * current role.
	 */
	SetCurrentRoleId(fs->outer_user_id, fs->is_superuser);

	/* Restore user ID and security context. */
	SetUserIdAndSecContext(fs->current_user_id, fs->sec_context);

	/* Restore temp-namespace state to ensure search path matches leader's. */
	SetTempNamespaceState(fs->temp_namespace_id,
						  fs->temp_toast_namespace_id);

	/* Restore reindex state. */
	RestoreReindexState(shm_toc_lookup(toc, DR_KEY_REINDEX_STATE, false));

	/*
	 * We've initialized all of our state now; nothing should change
	 * hereafter.
	 */
	InitializingParallelWorker = false;
	EnterParallelMode();

	ptr = shm_toc_lookup(toc, DR_KEY_CONNECT_INFO, false);
	info = (DynamicReduceNodeInfo*)(ptr + sizeof(Size));
	ConnectToAllNode(info, (uint32)*(Size*)ptr);
}

const Oid* DynamicReduceGetCurrentWorkingNodes(uint32 *count)
{
	if (cur_working_nodes == NULL)
	{
		if (count)
			*count = 0;
		return NULL;
	}

	if (count)
		*count = cur_working_nodes->len;
	return cur_working_nodes->oids;
}

void DRUtilsReset(void)
{
	InitializingParallelWorker = false;

	while (IsInParallelMode())
		ExitParallelMode();

	while (ActiveSnapshotSet())
		PopActiveSnapshot();
	
	if (in_parallel_transaction)
	{
		EndParallelWorkerTransaction();
		in_parallel_transaction = false;
	}

	if (session_attached)
	{
		DetachSession();
		session_attached = false;
	}

	if (seg_utils)
	{
		dsm_detach(seg_utils);
		seg_utils = NULL;
	}

	if (cur_working_nodes)
		resetOidBuffer(cur_working_nodes);
}

void DRUtilsAbort(void)
{
	DRUtilsReset();
}
