
#include "postgres.h"

#ifdef ADB_GRAM_ORA

#include "access/hash.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "libpq/pqformat.h"
#include "storage/itemptr.h"
#include "utils/builtins.h"

#ifdef ADB
typedef struct OraRowID
{
	int32			node_id;
	BlockNumber		block;
	OffsetNumber	offset;
}OraRowID;
static int32 rowid_compare(const OraRowID *l, const OraRowID *r);
typedef OraRowID *RowIDPointer;
#else
#define RowIDPointer ItemPointer
#define rowid_compare ItemPointerCompare
#endif

#define DatumGetRowIDPointer(X)		((RowIDPointer)DatumGetPointer(X))
#define RowIDPointerGetDatum(X)		PointerGetDatum(X)
#define PG_GETARG_ROWIDPOINTER(n)	DatumGetRowIDPointer(PG_GETARG_DATUM(n))
#define PG_RETURN_ROWIDPOINTER(X)	return RowIDPointerGetDatum(X)

Datum rowid_in(PG_FUNCTION_ARGS)
{
#ifdef ADB
	OraRowID *rowid;
	const char *str;
	StaticAssertStmt(offsetof(OraRowID, offset)+sizeof(OffsetNumber) == 10, "please change pg_type");

	str = PG_GETARG_CSTRING(0);
	Assert(str);
	if(strlen(str) != (8+8+4))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE)
			, errmsg("invalid argument string length")));

	rowid = palloc(sizeof(*rowid));
	pg_base64_decode(str, 8, (char*)&(rowid->node_id));
	pg_base64_decode(str+8, 8, (char*)&(rowid->block));
	pg_base64_decode(str+16, 4, (char*)&(rowid->offset));

	PG_RETURN_POINTER(rowid);
#else
	ItemPointer tid;
	const char *str = PG_GETARG_CSTRING(0);
	Assert(str);
	if (strlen(str) != (4+4+4))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE)
			, errmsg("invalid argument string length")));

	tid = palloc(sizeof(*tid));
	pg_base64_decode(str, 4, (char*)&(tid->ip_blkid.bi_hi));
	pg_base64_decode(str+4, 4, (char*)&(tid->ip_blkid.bi_lo));
	pg_base64_decode(str+8, 4, (char*)&(tid->ip_posid));
	PG_RETURN_POINTER(tid);
#endif
}

Datum rowid_out(PG_FUNCTION_ARGS)
{
#ifdef ADB
	OraRowID *rowid = (OraRowID*)PG_GETARG_POINTER(0);
	char *output = palloc(8+8+4+1);
	pg_base64_encode((char*)&(rowid->node_id), sizeof(rowid->node_id), output);
	pg_base64_encode((char*)&(rowid->block), sizeof(rowid->block), output+8);
	pg_base64_encode((char*)&(rowid->offset), sizeof(rowid->offset), output+16);
	output[8+8+4] = '\0';
	PG_RETURN_CSTRING(output);
#else
	ItemPointer tid = (ItemPointer)PG_GETARG_POINTER(0);
	char *output = palloc(4+4+4+1);
	pg_base64_encode((char*)&(tid->ip_blkid.bi_hi), 2, output);
	pg_base64_encode((char*)&(tid->ip_blkid.bi_lo), 2, output+4);
	pg_base64_encode((char*)&(tid->ip_posid), 2, output+8);
	output[4+4+4] = '\0';
	PG_RETURN_CSTRING(output);
#endif
}

Datum rowid_recv(PG_FUNCTION_ARGS)
{
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
}

Datum rowid_send(PG_FUNCTION_ARGS)
{
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
}

Datum rowid_eq(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(rowid_compare(PG_GETARG_ROWIDPOINTER(0),
								 PG_GETARG_ROWIDPOINTER(1)) == 0);
}

Datum rowid_ne(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(rowid_compare(PG_GETARG_ROWIDPOINTER(0),
								 PG_GETARG_ROWIDPOINTER(1)) != 0);
}

Datum rowid_lt(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(rowid_compare(PG_GETARG_ROWIDPOINTER(0),
								 PG_GETARG_ROWIDPOINTER(1)) < 0);
}

Datum rowid_le(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(rowid_compare(PG_GETARG_ROWIDPOINTER(0),
								 PG_GETARG_ROWIDPOINTER(1)) <= 0);
}

Datum rowid_gt(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(rowid_compare(PG_GETARG_ROWIDPOINTER(0),
								 PG_GETARG_ROWIDPOINTER(1)) > 0);
}

Datum rowid_ge(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(rowid_compare(PG_GETARG_ROWIDPOINTER(0),
								 PG_GETARG_ROWIDPOINTER(1)) >= 0);
}

Datum rowid_hash(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(hash_any((unsigned char*)PG_GETARG_POINTER(0), ROWID_DATA_SIZE));
}

Datum rowid_cmp(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(rowid_compare(PG_GETARG_ROWIDPOINTER(0),
								  PG_GETARG_ROWIDPOINTER(1)));
}

static int
rowid_fastcmp(Datum x, Datum y, SortSupport ssup)
{
	return rowid_compare(DatumGetRowIDPointer(x),
						 DatumGetRowIDPointer(y));
}

Datum rowid_sortsupport(PG_FUNCTION_ARGS)
{
	SortSupport ssup = (SortSupport) PG_GETARG_POINTER(0);

	ssup->comparator = rowid_fastcmp;
	PG_RETURN_VOID();
}

Datum rowid_larger(PG_FUNCTION_ARGS)
{
	RowIDPointer l = PG_GETARG_ROWIDPOINTER(0);
	RowIDPointer r = PG_GETARG_ROWIDPOINTER(1);
	PG_RETURN_ROWIDPOINTER(rowid_compare(l, r) > 0 ? l:r);
}

Datum rowid_smaller(PG_FUNCTION_ARGS)
{
	RowIDPointer l = PG_GETARG_ROWIDPOINTER(0);
	RowIDPointer r = PG_GETARG_ROWIDPOINTER(1);
	PG_RETURN_ROWIDPOINTER(rowid_compare(l, r) < 0 ? l:r);
}

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
