
#include "postgres.h"

#include "access/hash.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "storage/itemptr.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/rowid.h"
#include "utils/sortsupport.h"

#ifdef USE_SEQ_ROWID
	#ifdef ADB
		#error cluster not support rowid as sequence yet
	#else
		#define ROWID_BASE64_LEN	12	/* see function pg_base64_enc_len */
		#define DEFINE_ROWID_OP(name, op)				\
			Datum rowid_##name(PG_FUNCTION_ARGS)		\
			{											\
				PG_RETURN_BOOL((uint64)PG_GETARG_INT64(0) op (uint64)PG_GETARG_INT64(1));	\
			}extern int notexist
		#define DEFINE_ROWID_MINMAX(name, op)			\
			Datum rowid_##name(PG_FUNCTION_ARGS)		\
			{											\
				uint64 l = (uint64)PG_GETARG_INT64(0);	\
				uint64 r = (uint64)PG_GETARG_INT64(1);	\
				PG_RETURN_UINT64(l op r ? l:r);			\
			}extern int notexist

		Datum rowid_hash(PG_FUNCTION_ARGS)
		{
			return hashint8(fcinfo);
		}
		Datum rowid_hash_extended(PG_FUNCTION_ARGS)
		{
			return hashint8extended(fcinfo);
		}
		Datum rowid_cmp(PG_FUNCTION_ARGS)
		{
			uint64		a = (uint64)PG_GETARG_INT64(0);
			uint64		b = (uint64)PG_GETARG_INT64(1);

			if (a > b)
				PG_RETURN_INT32(1);
			else if (a == b)
				PG_RETURN_INT32(0);
			else
				PG_RETURN_INT32(-1);
		}
	#endif
#else /* USE_SEQ_ROWID */
	#ifdef ADB
		typedef struct OraRowID
		{
			int32			node_id;
			BlockNumber		block;
			OffsetNumber	offset;
		}OraRowID;
		static int32 rowid_compare(const OraRowID *l, const OraRowID *r);
		typedef OraRowID *RowIDPointer;
		#define ROWID_BASE64_LEN	(8+8+4)
	#else /* ADB */
		#define RowIDPointer ItemPointer
		#define rowid_compare ItemPointerCompare
		#define ROWID_BASE64_LEN	(4+4+4)
	#endif /* ADB */

	#define DatumGetRowIDPointer(X)		((RowIDPointer)DatumGetPointer(X))
	#define RowIDPointerGetDatum(X)		PointerGetDatum(X)
	#define PG_GETARG_ROWIDPOINTER(n)	DatumGetRowIDPointer(PG_GETARG_DATUM(n))
	#define PG_RETURN_ROWIDPOINTER(X)	return RowIDPointerGetDatum(X)
	#define DEFINE_ROWID_OP(name, op)								\
		Datum rowid_##name(PG_FUNCTION_ARGS)						\
		{															\
			PG_RETURN_BOOL(rowid_compare(PG_GETARG_ROWIDPOINTER(0),	\
						   PG_GETARG_ROWIDPOINTER(1)) op 0);		\
		}extern int notexist
	#define DEFINE_ROWID_MINMAX(name, op)							\
		Datum rowid_##name(PG_FUNCTION_ARGS)						\
		{															\
			RowIDPointer l = PG_GETARG_ROWIDPOINTER(0);				\
			RowIDPointer r = PG_GETARG_ROWIDPOINTER(1);				\
			PG_RETURN_ROWIDPOINTER(rowid_compare(l, r) op 0 ? l:r);	\
		}extern int notexist
	Datum rowid_hash(PG_FUNCTION_ARGS)
	{
		PG_RETURN_INT32(hash_any((unsigned char*)PG_GETARG_POINTER(0), ROWID_DATA_SIZE));
	}
	Datum rowid_hash_extended(PG_FUNCTION_ARGS)
	{
		return hash_any_extended((unsigned char*)PG_GETARG_POINTER(0),
								 ROWID_DATA_SIZE,
								 (uint64)PG_GETARG_INT64(1));
	}
	Datum rowid_cmp(PG_FUNCTION_ARGS)
	{
		PG_RETURN_INT32(rowid_compare(PG_GETARG_ROWIDPOINTER(0),
									  PG_GETARG_ROWIDPOINTER(1)));
	}
#endif /* else USE_SEQ_ROWID */

DEFINE_ROWID_OP(eq, ==);
DEFINE_ROWID_OP(ne, !=);
DEFINE_ROWID_OP(lt, <);
DEFINE_ROWID_OP(le, <=);
DEFINE_ROWID_OP(gt, >);
DEFINE_ROWID_OP(ge, >=);
DEFINE_ROWID_MINMAX(larger, >);
DEFINE_ROWID_MINMAX(smaller, <);

Datum rowid_in(PG_FUNCTION_ARGS)
{
	const char *str = PG_GETARG_CSTRING(0);
	Assert(str);
	if (strlen(str) != ROWID_BASE64_LEN)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid argument string length")));

#ifdef USE_SEQ_ROWID
	#ifdef ADB
		#error cluster not support rowid as sequence yet
	#else
		uint64 result;

		pg_base64_decode(str, ROWID_BASE64_LEN, (char*)&result);
		PG_RETURN_INT64(result);
	#endif /* ADB */
#else /* USE_SEQ_ROWID */
	#ifdef ADB
		OraRowID *rowid;
		StaticAssertStmt(offsetof(OraRowID, offset)+sizeof(OffsetNumber) == ROWID_DATA_SIZE, "please change pg_type");

		rowid = palloc(sizeof(*rowid));
		pg_base64_decode(str, 8, (char*)&(rowid->node_id));
		pg_base64_decode(str+8, 8, (char*)&(rowid->block));
		pg_base64_decode(str+16, 4, (char*)&(rowid->offset));

		PG_RETURN_POINTER(rowid);
	#else
		ItemPointer tid;
		StaticAssertStmt(sizeof(*tid) == ROWID_DATA_SIZE, "please change ROWID_DATA_SIZE");

		tid = palloc(sizeof(*tid));
		pg_base64_decode(str, 4, (char*)&(tid->ip_blkid.bi_hi));
		pg_base64_decode(str+4, 4, (char*)&(tid->ip_blkid.bi_lo));
		pg_base64_decode(str+8, 4, (char*)&(tid->ip_posid));
		PG_RETURN_POINTER(tid);
	#endif
#endif /* else USE_SEQ_ROWID */
}

Datum rowid_out(PG_FUNCTION_ARGS)
{
	char *output = palloc(ROWID_BASE64_LEN+1);
#ifdef USE_SEQ_ROWID
	#ifdef ADB
		#error cluster not support rowid as sequence yet
	#else
		uint64 value = PG_GETARG_INT64(0);
		uint32 len PG_USED_FOR_ASSERTS_ONLY;

		len = pg_base64_encode((char*)&value, sizeof(value), output);
		Assert(len == ROWID_BASE64_LEN);
	#endif
#else /* USE_SEQ_ROWID */
	#ifdef ADB
		OraRowID *rowid = (OraRowID*)PG_GETARG_POINTER(0);
		pg_base64_encode((char*)&(rowid->node_id), sizeof(rowid->node_id), output);
		pg_base64_encode((char*)&(rowid->block), sizeof(rowid->block), output+8);
		pg_base64_encode((char*)&(rowid->offset), sizeof(rowid->offset), output+16);
	#else
		ItemPointer tid = (ItemPointer)PG_GETARG_POINTER(0);
		pg_base64_encode((char*)&(tid->ip_blkid.bi_hi), 2, output);
		pg_base64_encode((char*)&(tid->ip_blkid.bi_lo), 2, output+4);
		pg_base64_encode((char*)&(tid->ip_posid), 2, output+8);
	#endif
#endif /* USE_SEQ_ROWID */

	output[ROWID_BASE64_LEN] = '\0';
	PG_RETURN_CSTRING(output);
}

Datum rowid_recv(PG_FUNCTION_ARGS)
{
#ifdef USE_SEQ_ROWID
	#ifdef ADB
		#error cluster not support rowid as sequence yet
	#else
		return int8recv(fcinfo);
	#endif
#else /* USE_SEQ_ROWID */
	#ifdef ADB
		StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);
		OraRowID *rowid = palloc(sizeof(OraRowID));

		rowid->node_id = pq_getmsgint(buf, sizeof(rowid->node_id));
		rowid->block = pq_getmsgint(buf, sizeof(rowid->block));
		rowid->offset = pq_getmsgint(buf, sizeof(rowid->offset));

		PG_RETURN_POINTER(rowid);
	#else
		return tidrecv(fcinfo);
	#endif
#endif /* USE_SEQ_ROWID */
}

Datum rowid_send(PG_FUNCTION_ARGS)
{
#ifdef USE_SEQ_ROWID
	#ifdef ADB
		#error cluster not support rowid as sequence yet
	#else
		return int8send(fcinfo);
	#endif
#else /* USE_SEQ_ROWID */
	#ifdef ADB
		StringInfoData buf;
		OraRowID *rowid = (OraRowID*)PG_GETARG_POINTER(0);

		pq_begintypsend(&buf);
		pq_sendint(&buf, rowid->node_id, sizeof(rowid->node_id));
		pq_sendint(&buf, rowid->block, sizeof(rowid->block));
		pq_sendint(&buf, rowid->offset, sizeof(rowid->offset));

		PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
	#else
		return tidsend(fcinfo);
	#endif
#endif /* USE_SEQ_ROWID */
}

static int
rowid_fastcmp(Datum x, Datum y, SortSupport ssup)
{
#ifdef USE_SEQ_ROWID
	#ifdef ADB
		#error cluster not support rowid as sequence yet
	#else
		uint64		a = DatumGetUInt64(x);
		uint64		b = DatumGetUInt64(y);

		if (a > b)
			return 1;
		else if (a == b)
			return 0;
		else
			return -1;
	#endif
#else
	return rowid_compare(DatumGetRowIDPointer(x),
						 DatumGetRowIDPointer(y));
#endif
}

Datum rowid_sortsupport(PG_FUNCTION_ARGS)
{
	SortSupport ssup = (SortSupport) PG_GETARG_POINTER(0);

	ssup->comparator = rowid_fastcmp;
	PG_RETURN_VOID();
}

#ifndef USE_SEQ_ROWID
Datum rowid_make(ADB_ONLY_ARG_COMMA(uint32 node_id) ItemPointer const tid)
{
#ifdef ADB
	OraRowID *rowid;
	AssertArg(tid);
	rowid = palloc(sizeof(*rowid));
	rowid->node_id = node_id;
	rowid->block = ItemPointerGetBlockNumber(tid);
	rowid->offset = ItemPointerGetOffsetNumber(tid);
#else
	ItemPointer *rowid = palloc(sizeof(*rowid));
	memcpy(rowid, tid, sizeof(*rowid));
#endif
	return PointerGetDatum(rowid);
}

#if defined(ADB)
/* save ctid to tid and return xc_node_id */
uint32 rowid_get_data(Datum arg, ItemPointer tid)
{
	OraRowID *rowid = (OraRowID*)DatumGetPointer(arg);

	ItemPointerSet(tid, rowid->block, rowid->offset);

	return rowid->node_id;
}
#else
/* save ctid to tid */
void rowid_get_data(Datum arg, ItemPointer tid)
{
	memcpy(tid, DatumGetPointer(arg), sizeof(*tid));
}
#endif

#ifdef ADB
static int32 rowid_compare(const OraRowID *l, const OraRowID *r)
{
	AssertArg(l && r);
	if(l->node_id < r->node_id)
		return -1;
	else if(l->node_id > r->node_id)
		return 1;
	else if(l->block < r->block)
		return -1;
	else if(l->block > r->block)
		return 1;
	else if(l->offset < r->offset)
		return -1;
	else if(l->offset > r->offset)
		return 1;
	return 0;
}
#endif /* ADB */

#else /* else !defined(USE_SEQ_ROWID) */
typedef struct SharedRowidCache
{
	uint64		nextRowid;				/* next ROWID to assign */
	uint64		flushedRowid;			/* last flush next ROWID in wal */
	uint64		flushingRowid;			/* flushing next ROWID in wal */
	XLogRecPtr	flushingPtr;			/* flushing next ROWID wal ptr */
	slock_t		mutexNext;				/* lock for nextRowid */
	slock_t		mutexFlushed;			/* lock for flushedRowid */
	slock_t		mutexFlushing;			/* lock for flushingRowid and flushingPtr */
}SharedRowidCache;

#define INVALID_COMPARE_WITH_ROWID	UINT64CONST(-1)
#define INVALID_SYNC_ROWID			UINT64CONST(0)
#define FirstNormalRowid			100

uint64 compare_with_rowid_id = INVALID_COMPARE_WITH_ROWID;
int default_with_rowid_id = -1;
bool default_with_rowids = false;

static bool registered_xact_callback = false;
static uint64 last_rowid = INVALID_SYNC_ROWID;
static SharedRowidCache *RowidCache = NULL;

void InitRowidShmem(void)
{
	bool found;
#ifdef USE_ASSERT_CHECKING
	if (default_with_rowid_id >= 0)
	{
		Assert(default_with_rowid_id < ROWID_NODE_MAX_VALUE);
		Assert((compare_with_rowid_id & ROWID_NODE_BITS_MASK) >> (64-ROWID_NODE_BITS_LENGTH) == default_with_rowid_id);
	}
#endif
	RowidCache = ShmemInitStruct("ROWID Data",
								 sizeof(SharedRowidCache),
								 &found);
	if (!found)
	{
		if (default_with_rowid_id >= 0)
			RowidCache->nextRowid = (compare_with_rowid_id | FirstNormalRowid);
		else
			RowidCache->nextRowid = INVALID_SYNC_ROWID;
		RowidCache->flushedRowid = RowidCache->nextRowid;
		RowidCache->flushingRowid = RowidCache->nextRowid;
		RowidCache->flushingPtr = InvalidXLogRecPtr;
		SpinLockInit(&RowidCache->mutexNext);
		SpinLockInit(&RowidCache->mutexFlushed);
		SpinLockInit(&RowidCache->mutexFlushing);
	}
}

Size SizeRowidShmem(void)
{
	return sizeof(SharedRowidCache);
}

static void rowid_xact_callback(XactEvent event, void *arg)
{
	if (last_rowid == INVALID_SYNC_ROWID)
		return;

	switch(event)
	{
	case XACT_EVENT_COMMIT:
	case XACT_EVENT_PARALLEL_COMMIT:
	case XACT_EVENT_PREPARE:
		SpinLockAcquire(&RowidCache->mutexFlushed);
		Assert(last_rowid <= RowidCache->nextRowid);
		Assert(RowidCache->flushedRowid <= RowidCache->nextRowid);
		if (RowidCache->flushedRowid < last_rowid)
			RowidCache->flushedRowid = last_rowid;
		SpinLockRelease(&RowidCache->mutexFlushed);
		last_rowid = INVALID_SYNC_ROWID;
		break;
	case XACT_EVENT_ABORT:
	case XACT_EVENT_PARALLEL_ABORT:
		last_rowid = INVALID_SYNC_ROWID;
		break;
	case XACT_EVENT_PRE_COMMIT:
	case XACT_EVENT_PARALLEL_PRE_COMMIT:
	case XACT_EVENT_PRE_PREPARE:
		/* is flushed? */
		SpinLockAcquire(&RowidCache->mutexFlushed);
		if (RowidCache->flushedRowid > last_rowid)
			last_rowid = INVALID_SYNC_ROWID;
		SpinLockRelease(&RowidCache->mutexFlushed);
		if (last_rowid == INVALID_SYNC_ROWID)
			break;

		/* is flushing? */
		SpinLockAcquire(&RowidCache->mutexFlushing);
		if (RowidCache->flushingRowid > last_rowid)
			last_rowid = INVALID_SYNC_ROWID;
		SpinLockRelease(&RowidCache->mutexFlushing);
		if (last_rowid == INVALID_SYNC_ROWID)
			break;

		/* update to global next rowid */
		SpinLockAcquire(&RowidCache->mutexNext);
		last_rowid = RowidCache->nextRowid;
		SpinLockRelease(&RowidCache->mutexNext);
		/*
		 * and flush next rowid,
		 * maybe more than one backend flushing this next rowid,
		 * however it bater than flush next rowid in each generate rowid
		 */
		XLogPutNextRowid(last_rowid);

		/*
		 * update flushing, but maybe other backend update it
		 */
		SpinLockAcquire(&RowidCache->mutexFlushing);
		if (RowidCache->flushingRowid < last_rowid)
			RowidCache->flushingRowid = last_rowid;
		SpinLockRelease(&RowidCache->mutexFlushing);
		break;
	}
}

uint64 GetNewRowid(void)
{
	uint64 newid;
	if (compare_with_rowid_id == INVALID_COMPARE_WITH_ROWID)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid value for parameter \"default_with_rowid_id\": %d", default_with_rowid_id),
				 errhint("before use rowid must set it to a valid value")));
	}

	SpinLockAcquire(&RowidCache->mutexNext);
	newid = RowidCache->nextRowid;

	if (RowidIsLocalInvalid(newid))
	{
		SpinLockRelease(&RowidCache->mutexNext);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid value for parameter \"default_with_rowid_id\": %d", default_with_rowid_id),
				 errhint("must restore \"default_with_rowid_id\" to old value: %d", (int)RowidGetNodeID(newid)),
				 errdetail("generating new rowid is: " UINT64_FORMAT, newid)));
	}
	if ((newid & ROWID_NODE_VALUE_MASK) == ROWID_NODE_VALUE_MASK)
	{
		SpinLockRelease(&RowidCache->mutexNext);
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("rowid out of range")));
	}

	/* update shared memory */
	++(RowidCache->nextRowid);
	SpinLockRelease(&RowidCache->mutexNext);

	/* remember we generated new rowid */
	if (unlikely(registered_xact_callback == false))
	{
		RegisterXactCallback(rowid_xact_callback, NULL);
		registered_xact_callback = true;
	}
	last_rowid = newid;

	return newid;
}

void GucAssignRowidNodeId(int newval, void *extra)
{
	if (newval < 0)
	{
		compare_with_rowid_id = INVALID_COMPARE_WITH_ROWID;
		return;
	}

	Assert(newval < ROWID_NODE_MAX_VALUE);
	compare_with_rowid_id = (uint64)newval << (64-ROWID_NODE_BITS_LENGTH);
}

Datum nextval_rowid(PG_FUNCTION_ARGS)
{
#if NOT_USE
	Oid		relid = PG_GETARG_OID(0);
#endif
	uint64	newrowid = GetNewRowid();

	/*
	 * we should check new value is exist, for now we not check
	 */

	PG_RETURN_UINT64(newrowid);
}

static void UpdateNextRowid(uint64 nextRowid)
{
	SpinLockAcquire(&RowidCache->mutexNext);
	if (nextRowid > RowidCache->nextRowid)
		RowidCache->nextRowid = nextRowid;
	SpinLockRelease(&RowidCache->mutexNext);

	SpinLockAcquire(&RowidCache->mutexFlushed);
	if (nextRowid > RowidCache->flushedRowid)
		RowidCache->flushedRowid = nextRowid;
	SpinLockRelease(&RowidCache->mutexFlushed);
}

void RedoNextRowid(void *ptr)
{
	uint64		nextRowid;
	memcpy(&nextRowid, ptr, sizeof(nextRowid));

	if (RowidIsLocalInvalid(nextRowid))
	{
		int old_rowid_id = (int)RowidGetNodeID(nextRowid);
		ereport(FATAL,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid value for parameter \"default_with_rowid_id\": %d", default_with_rowid_id),
				 errhint("must restore \"default_with_rowid_id\" to old value: %d", old_rowid_id),
				 errdetail("redo next rowid is: " UINT64_FORMAT, nextRowid)));
	}
	UpdateNextRowid(nextRowid);
}

void SetCheckpointRowid(int64 nextRowid)
{
	int old_rowid_id;

	if (default_with_rowid_id >= 0 &&
		nextRowid &&
		RowidIsLocalInvalid(nextRowid))
	{
		old_rowid_id = (int)RowidGetNodeID(nextRowid);
		ereport(FATAL,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid value for parameter \"default_with_rowid_id\": %d", default_with_rowid_id),
				 errhint("must restore \"default_with_rowid_id\" to old value: %d", old_rowid_id),
				 errdetail("checkpoint next rowid is: " UINT64_FORMAT, nextRowid)));
	}
	UpdateNextRowid(nextRowid);
}

int64 GetCheckpointRowid(void)
{
	uint64 nextRowid;
	SpinLockAcquire(&RowidCache->mutexNext);
	nextRowid = RowidCache->nextRowid;
	SpinLockRelease(&RowidCache->mutexNext);
	return nextRowid;
}

#endif /* !USE_SEQ_ROWID */
