
#include "postgres.h"

#ifdef ADB_GRAM_ORA

#include "access/hash.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "libpq/pqformat.h"
#include "storage/itemptr.h"
#include "utils/builtins.h"

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
#if defined(ADB) && defined(ADB_GRAM_ORA)
Datum rowid_make(uint32 node_id, ItemPointer const tid)
{
	OraRowID *rowid;
	AssertArg(tid);
	rowid = palloc(sizeof(*rowid));
	rowid->node_id = node_id;
	rowid->block = ItemPointerGetBlockNumber(tid);
	rowid->offset = ItemPointerGetOffsetNumber(tid);
	return PointerGetDatum(rowid);
}

/* save ctid to tid and return xc_node_id */
uint32 rowid_get_data(Datum arg, ItemPointer tid)
{
	OraRowID *rowid = (OraRowID*)DatumGetPointer(arg);

	ItemPointerSet(tid, rowid->block, rowid->offset);

	return rowid->node_id;
}
#elif defined(ADB_GRAM_ORA)
Datum rowid_make(ItemPointer const tid)
{
	ItemPointer *new_tid = palloc(sizeof(*new_tid));
	memcpy(new_tid, tid, sizeof(*new_tid));
	return PointerGetDatum(new_tid);
}

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

#endif /* ADB_GRAM_ORA */

#endif /* !USE_SEQ_ROWID */
