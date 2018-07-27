
#include "postgres.h"

#ifdef ADB_GRAM_ORA

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
static int rowid_compare(const OraRowID *l, const OraRowID *r);
#endif

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
	b64_decode(str, 8, (char*)&(rowid->node_id));
	b64_decode(str+8, 8, (char*)&(rowid->block));
	b64_decode(str+16, 4, (char*)&(rowid->offset));

	PG_RETURN_POINTER(rowid);
#else
	ItemPointer tid;
	const char *str = PG_GETARG_CSTRING(0);
	Assert(str);
	if (strlen(str) != (4+4+4))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE)
			, errmsg("invalid argument string length")));

	tid = palloc(sizeof(*tid));
	b64_decode(str, 4, (char*)&(tid->ip_blkid.bi_hi));
	b64_decode(str+4, 4, (char*)&(tid->ip_blkid.bi_lo));
	b64_decode(str+8, 4, (char*)&(tid->ip_posid));
	PG_RETURN_POINTER(tid);
#endif
}

Datum rowid_out(PG_FUNCTION_ARGS)
{
#ifdef ADB
	OraRowID *rowid = (OraRowID*)PG_GETARG_POINTER(0);
	char *output = palloc(8+8+4+1);
	b64_encode((char*)&(rowid->node_id), sizeof(rowid->node_id), output);
	b64_encode((char*)&(rowid->block), sizeof(rowid->block), output+8);
	b64_encode((char*)&(rowid->offset), sizeof(rowid->offset), output+16);
	output[8+8+4] = '\0';
	PG_RETURN_CSTRING(output);
#else
	ItemPointer tid = (ItemPointer)PG_GETARG_POINTER(0);
	char *output = palloc(4+4+4+1);
	b64_encode((char*)&(tid->ip_blkid.bi_hi), 2, output);
	b64_encode((char*)&(tid->ip_blkid.bi_lo), 2, output+4);
	b64_encode((char*)&(tid->ip_posid), 2, output+8);
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
#ifdef ADB
	OraRowID *l = (OraRowID*)PG_GETARG_POINTER(0);
	OraRowID *r = (OraRowID*)PG_GETARG_POINTER(1);
	PG_RETURN_BOOL(rowid_compare(l,r) == 0);
#else
	return tideq(fcinfo);
#endif
}

Datum rowid_ne(PG_FUNCTION_ARGS)
{
#ifdef ADB
	OraRowID *l = (OraRowID*)PG_GETARG_POINTER(0);
	OraRowID *r = (OraRowID*)PG_GETARG_POINTER(1);
	PG_RETURN_BOOL(rowid_compare(l,r) != 0);
#else
	return tidne(fcinfo);
#endif
}

Datum rowid_lt(PG_FUNCTION_ARGS)
{
#ifdef ADB
	OraRowID *l = (OraRowID*)PG_GETARG_POINTER(0);
	OraRowID *r = (OraRowID*)PG_GETARG_POINTER(1);
	PG_RETURN_BOOL(rowid_compare(l,r) < 0);
#else
	return tidlt(fcinfo);
#endif
}

Datum rowid_le(PG_FUNCTION_ARGS)
{
#ifdef ADB
	OraRowID *l = (OraRowID*)PG_GETARG_POINTER(0);
	OraRowID *r = (OraRowID*)PG_GETARG_POINTER(1);
	PG_RETURN_BOOL(rowid_compare(l,r) <= 0);
#else
	return tidle(fcinfo);
#endif
}

Datum rowid_gt(PG_FUNCTION_ARGS)
{
#ifdef ADB
	OraRowID *l = (OraRowID*)PG_GETARG_POINTER(0);
	OraRowID *r = (OraRowID*)PG_GETARG_POINTER(1);
	PG_RETURN_BOOL(rowid_compare(l,r) > 0);
#else
	return tidgt(fcinfo);
#endif
}

Datum rowid_ge(PG_FUNCTION_ARGS)
{
#ifdef ADB
	OraRowID *l = (OraRowID*)PG_GETARG_POINTER(0);
	OraRowID *r = (OraRowID*)PG_GETARG_POINTER(1);
	PG_RETURN_BOOL(rowid_compare(l,r) >= 0);
#else
	return tidge(fcinfo);
#endif
}

Datum rowid_larger(PG_FUNCTION_ARGS)
{
#ifdef ADB
	OraRowID *l = (OraRowID*)PG_GETARG_POINTER(0);
	OraRowID *r = (OraRowID*)PG_GETARG_POINTER(1);
	OraRowID *result = palloc(sizeof(OraRowID));
	memcpy(result, rowid_compare(l, r) > 0 ? l:r, sizeof(OraRowID));
	PG_RETURN_POINTER(result);
#else
	return tidlarger(fcinf);
#endif
}

Datum rowid_smaller(PG_FUNCTION_ARGS)
{
#ifdef ADB
	OraRowID *l = (OraRowID*)PG_GETARG_POINTER(0);
	OraRowID *r = (OraRowID*)PG_GETARG_POINTER(1);
	OraRowID *result = palloc(sizeof(OraRowID));
	memcpy(result, rowid_compare(l, r) < 0 ? l:r, sizeof(OraRowID));
	PG_RETURN_POINTER(result);
#else
	return tidsmaller(fcinfo);
#endif
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
static int rowid_compare(const OraRowID *l, const OraRowID *r)
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
