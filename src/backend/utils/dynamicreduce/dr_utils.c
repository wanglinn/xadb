#include "postgres.h"

#include "access/parallel.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "libpq/pqformat.h"
#include "libpq/pqmq.h"
#include "miscadmin.h"
#include "storage/shm_toc.h"
#include "utils/builtins.h"
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
#define DR_KEY_TRANSACTION_SNAPSHOT	UINT64CONST(0xFFFFFFFFFFFF0005)
#define DR_KEY_ACTIVE_SNAPSHOT		UINT64CONST(0xFFFFFFFFFFFF0006)
#define DR_KEY_TRANSACTION_STATE	UINT64CONST(0xFFFFFFFFFFFF0007)

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

static dsa_pointer	dsa_utils = InvalidDsaPointer;
static OidBuffer	cur_working_nodes = NULL;
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

/* ===================== connect message =============================== */
void DynamicReduceConnectNet(const DynamicReduceNodeInfo *info, uint32 count)
{
	shm_toc_estimator	estimator;
	shm_toc			   *toc;
	char			   *ptr;
	DRFixedState	   *fs;
	dsa_pointer			dsa;

	Size				library_len = 0;
	Size				guc_len = 0;
	Size				tsnaplen = 0;
	Size				asnaplen = 0;
	Size				tstatelen = 0;
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

	shm_toc_initialize_estimator(&estimator);

	/* Estimate space for various kinds of state sharing. */
	library_len = EstimateLibraryStateSpace();
	shm_toc_estimate_chunk(&estimator, library_len);
	guc_len = EstimateGUCStateSpace();
	shm_toc_estimate_chunk(&estimator, guc_len);
	tsnaplen = EstimateSnapshotSpace(transaction_snapshot);
	shm_toc_estimate_chunk(&estimator, tsnaplen);
	asnaplen = EstimateSnapshotSpace(active_snapshot);
	shm_toc_estimate_chunk(&estimator, asnaplen);
	tstatelen = EstimateTransactionStateSpace();
	shm_toc_estimate_chunk(&estimator, tstatelen);
	/* If you add more chunks here, you probably need to add keys. */
	shm_toc_estimate_keys(&estimator, 5);

	shm_toc_estimate_chunk(&estimator, sizeof(*fs));
	shm_toc_estimate_keys(&estimator, 1);

	infolen = sizeof(info[0])*count + sizeof(Size);
	shm_toc_estimate_chunk(&estimator, infolen);
	shm_toc_estimate_keys(&estimator, 1);

	/* create shared memory */
	segsize = shm_toc_estimate(&estimator);
	dsa = dsa_allocate0(dr_dsa, segsize);
	toc = shm_toc_create(DYNAMIC_REDUCE_MAGIC,
						 dsa_get_address(dr_dsa, dsa),
						 segsize);

	/* Serialize shared libraries we have loaded. */
	ptr = shm_toc_allocate(toc, library_len);
	SerializeLibraryState(library_len, ptr);
	shm_toc_insert(toc, DR_KEY_LIBRARY, ptr);

	/* Serialize GUC settings. */
	ptr = shm_toc_allocate(toc, guc_len);
	SerializeGUCState(guc_len, ptr);
	shm_toc_insert(toc, DR_KEY_GUC, ptr);

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

	/* Serialize fixed state */
	fs = shm_toc_allocate(toc, sizeof(*fs));
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

	initStringInfoExtend(&buf, 20);
	pq_sendbyte(&buf, ADB_DR_MQ_MSG_CONNECT);
	pq_sendbytes(&buf, (char*)&dsa, sizeof(dsa));
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
}

void DRConnectNetMsg(StringInfo msg)
{
	shm_toc		   *toc;
	char		   *ptr;
	DRFixedState   *fs;
	DynamicReduceNodeInfo *info;
	ResourceOwner	oldowner;

	if (DsaPointerIsValid(dsa_utils))
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

	pq_copymsgbytes(msg, (char*)&dsa_utils, sizeof(dsa_utils));
	pq_getmsgend(msg);

	oldowner = CurrentResourceOwner;
	CurrentResourceOwner = NULL;

	if (dsa_utils == InvalidDsaPointer)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not map dynamic shared memory pointer")));
	toc = shm_toc_attach(DYNAMIC_REDUCE_MAGIC, dsa_get_address(dr_dsa, dsa_utils));
	if (toc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("invalid magic number in dynamic shared memory segment")));

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
	{
		Oid temp_namespace_id;
		Oid temp_toast_namespace_id;
		GetTempNamespaceState(&temp_namespace_id, &temp_toast_namespace_id);
		if (OidIsValid(temp_namespace_id))
		{
			if (temp_namespace_id != fs->temp_namespace_id ||
				temp_toast_namespace_id != fs->temp_toast_namespace_id)
			{
				ereport(ERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("worng temp namespace")));
			}
		}else
		{
			SetTempNamespaceState(fs->temp_namespace_id,
								  fs->temp_toast_namespace_id);
		}
	}

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

	if (DsaPointerIsValid(dsa_utils))
	{
		dsa_free(dr_dsa, dsa_utils);
		dsa_utils = InvalidDsaPointer;
	}

	if (cur_working_nodes)
		resetOidBuffer(cur_working_nodes);
}

void DRUtilsAbort(void)
{
	DRUtilsReset();
}
