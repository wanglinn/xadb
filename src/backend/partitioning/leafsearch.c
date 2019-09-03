
#include "postgres.h"

#include "access/nbtree.h"
#include "access/htup_details.h"
#include "catalog/pg_type_d.h"
#include "fmgr.h"
#include "funcapi.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#include "nodes/relation.h"
#include "optimizer/reduceinfo.h"
#include "partitioning/partbounds.h"
#include "port/pg_crc32c.h"
#include "utils/arrayaccess.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/typcache.h"

typedef struct ListData
{
	Datum key;
	Datum value;
}ListData;
typedef struct CompareListData
{
	MemoryContext context;
	FmgrInfo	supfunc;
	pg_crc32c	crc_kv;
	Oid			collation;
	uint32		count;
	Datum		datum_key;
	Datum		datum_val;
	ListData	data[FLEXIBLE_ARRAY_MEMBER];
}CompareListData;

static int32
qsort_listdata_value_cmp(const void *a, const void *b, void *arg)
{
	CompareListData *compare = (CompareListData*)arg;
	return DatumGetInt32(FunctionCall2Coll(&compare->supfunc,
										   compare->collation,
										   ((ListData*)a)->key,
										   ((ListData*)b)->key));
}

/*
 * array_bsearch(val[], key[], comp_fn, comp_val)
 */
Datum array_bsearch(PG_FUNCTION_ARGS)
{
	CompareListData *my_extra = fcinfo->flinfo->fn_extra;
	ArrayType	   *arr_val = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType	   *arr_key = PG_GETARG_ARRAYTYPE_P(1);
	Datum			input = PG_GETARG_DATUM(3);
	Oid				collation = PG_GET_COLLATION();
	pg_crc32c		crc_kv;

	int				lo,
					hi,
					mid;

	INIT_CRC32C(crc_kv);
	COMP_CRC32C(crc_kv, (void*)arr_val, VARSIZE_ANY(arr_val));
	COMP_CRC32C(crc_kv, (void*)arr_key, VARSIZE_ANY(arr_key));
	FIN_CRC32C(crc_kv);

	if (my_extra == NULL ||
		!EQ_CRC32C(crc_kv, my_extra->crc_kv) ||
		my_extra->datum_val != PG_GETARG_DATUM(0) ||
		my_extra->datum_key != PG_GETARG_DATUM(1) ||
		my_extra->supfunc.fn_oid != PG_GETARG_OID(2) ||
		my_extra->collation != collation)
	{
		uint32		count;
		int16		elmlen;
		bool		elmbyval;
		char		elmalign;
		int			num_elems;
		Datum	   *elem_values;
		MemoryContext context;
		MemoryContext oldcontext;

		if (ARR_NDIM(arr_val) != 1 ||
			ARR_HASNULL(arr_val) ||
			ARR_NDIM(arr_key) != 1 ||
			ARR_HASNULL(arr_key))
		{
			elog(ERROR, "argument is not a 1-D not null array");
		}
		count = ARR_DIMS(arr_val)[0];
		if (count == 0)
			elog(ERROR, "argument array element is empty");
		if (count != ARR_DIMS(arr_key)[0])
			elog(ERROR, "argument array element count not same");

		if (my_extra)
			MemoryContextDelete(my_extra->context);
		context = AllocSetContextCreate(fcinfo->flinfo->fn_mcxt,
										"list_get_distribute_node",
										ALLOCSET_DEFAULT_SIZES);
		my_extra = MemoryContextAllocZero(context,
										  offsetof(CompareListData, data) +
										  sizeof(ListData)*count);
		fcinfo->flinfo->fn_extra = my_extra;
		my_extra->context = context;
		my_extra->count = count;
		my_extra->crc_kv = crc_kv;
		my_extra->datum_val = PG_GETARG_DATUM(0);
		my_extra->datum_key = PG_GETARG_DATUM(1);
		my_extra->collation = collation;

		oldcontext = MemoryContextSwitchTo(context);
		get_typlenbyvalalign(ARR_ELEMTYPE(arr_key),
							 &elmlen, &elmbyval, &elmalign);
		deconstruct_array(arr_key,
						  ARR_ELEMTYPE(arr_key),
						  elmlen, elmbyval, elmalign,
						  &elem_values, NULL, &num_elems);
		Assert(num_elems == count);
		while (num_elems > 0)
		{
			--num_elems;
			my_extra->data[num_elems].key = datumCopy(elem_values[num_elems], elmbyval, elmlen);
		}
		pfree(elem_values);

		get_typlenbyvalalign(ARR_ELEMTYPE(arr_val),
							 &elmlen, &elmbyval, &elmalign);
		deconstruct_array(arr_val,
						  ARR_ELEMTYPE(arr_val),
						  elmlen, elmbyval, elmalign,
						  &elem_values, NULL, &num_elems);
		Assert(num_elems == count);
		while (num_elems > 0)
		{
			--num_elems;
			my_extra->data[num_elems].value = datumCopy(elem_values[num_elems], elmbyval, elmlen);
		}
		pfree(elem_values);

		fmgr_info_cxt(PG_GETARG_OID(2), &my_extra->supfunc, fcinfo->flinfo->fn_mcxt);

		qsort_arg(my_extra->data, count, sizeof(my_extra->data[0]),
				  qsort_listdata_value_cmp, my_extra);

		MemoryContextSwitchTo(oldcontext);
	}

	input = PG_GETARG_DATUM(3);
	lo = -1;
	hi = my_extra->count - 1;
	while (lo < hi)
	{
		int32		compval;

		mid = (lo + hi + 1) / 2;
		compval = DatumGetInt32(FunctionCall2Coll(&my_extra->supfunc,
												  collation,
												  my_extra->data[mid].key,
												  input));
		if (compval < 0)
		{
			lo = mid;
		}else if(compval == 0)
		{
			PG_RETURN_DATUM(my_extra->data[mid].value);
		}else
		{
			hi = mid - 1;
		}
	}

	PG_RETURN_NULL();
}

#define	RANGE_DATUM_KIND_ATTNO	2
#define RANGE_DATUM_VALUE_ATTNO	1

static TupleDesc* create_range_bsearch_desc(ReduceInfo *rinfo, TupleDesc *range_desc)
{
	TupleDesc  *key_desc;
	TupleDesc	top_desc;
	TupleDesc	desc;
	uint32		i;

	top_desc = CreateTemplateTupleDesc(rinfo->nkey, false);
	key_desc = palloc(sizeof(key_desc[0])*rinfo->nkey);
	for (i=0;i<rinfo->nkey;++i)
	{
		/*
		 *	RECORD of range key
		 *	{
		 *		key		keytype;
		 *		kind	int16;	PartitionRangeDatumKind
		 *	}
		 */
		ReduceKeyInfo *key = &rinfo->keys[i];
		desc = CreateTemplateTupleDesc(2, false);
		TupleDescInitEntry(desc, RANGE_DATUM_KIND_ATTNO, NULL, INT2OID, -1, 0);
		TupleDescInitEntry(desc,
						   RANGE_DATUM_VALUE_ATTNO,
						   NULL,
						   exprType((Node*)key->key),
						   exprTypmod((Node*)key->key),
						   0);
		TupleDescInitEntryCollation(desc, RANGE_DATUM_VALUE_ATTNO, key->collation);
		key_desc[i] = BlessTupleDesc(desc);

		/*
		 * RECORD of range
		 * {
		 * 		RECORD of range key
		 * 		...    of range key
		 * }
		 */
		TupleDescInitEntry(top_desc,
						   i+1,
						   NULL,
						   RECORDOID,
						   key_desc[i]->tdtypmod,
						   0);
	}
	*range_desc = BlessTupleDesc(top_desc);
	return key_desc;
}

static Datum create_range_bsearch_key_item_datum(TupleDesc range_desc, TupleDesc *key_desc,
												 List *list, Datum *key_elems, bool *key_nulls)
{
	ListCell   *lc;
	HeapTuple	tuple;
	uint32		i;

	Datum		datum[2];
	bool		nulls[2];

	i = 0;
	foreach (lc, list)
	{
		PartitionRangeDatum *range = lfirst_node(PartitionRangeDatum, lc);
		nulls[RANGE_DATUM_KIND_ATTNO-1] = false;
		datum[RANGE_DATUM_KIND_ATTNO-1] = Int16GetDatum((int16)range->kind);
		if (range->kind == PARTITION_RANGE_DATUM_VALUE)
		{
			datum[RANGE_DATUM_VALUE_ATTNO-1] = castNode(Const, range->value)->constvalue;
			nulls[RANGE_DATUM_VALUE_ATTNO-1] = false;
		}else
		{
			nulls[RANGE_DATUM_VALUE_ATTNO-1] = true;
		}

		Assert(key_desc[i]->natts == 2);
		tuple = heap_form_tuple(key_desc[i], datum, nulls);
		key_elems[i] = HeapTupleGetDatum(tuple);
		key_nulls[i] = false;
	}

	tuple = heap_form_tuple(range_desc, key_elems, key_nulls);
	return HeapTupleGetDatum(tuple);
}

static Const* create_range_bsearch_key(ReduceInfo *rinfo)
{
	ListCell   *lc;
	TupleDesc  *key_desc;
	TupleDesc	range_desc;
	Datum	   *arr_elems;
	Datum	   *key_elems;
	bool	   *key_nulls;
	ArrayType  *arr_record;
	uint32		cnt;

	Assert(rinfo->type == REDUCE_TYPE_RANGE);
	key_desc = create_range_bsearch_desc(rinfo, &range_desc);

	arr_elems = palloc(list_length(rinfo->values) * sizeof(Datum) * 2);
	key_elems = palloc(sizeof(Datum) * rinfo->nkey);
	key_nulls = palloc(sizeof(bool) * rinfo->nkey);
	cnt = 0;
	foreach(lc, rinfo->values)
	{
		List *list2 = lfirst_node(List, lc);
		Assert(list_length(list2) == 2);
		arr_elems[cnt++] = create_range_bsearch_key_item_datum(range_desc,
															   key_desc,
															   linitial_node(List, list2),
															   key_elems,
															   key_nulls);
		arr_elems[cnt++] = create_range_bsearch_key_item_datum(range_desc,
															   key_desc,
															   llast_node(List, list2),
															   key_elems,
															   key_nulls);
	}
	Assert(list_length(rinfo->values)*2 == cnt);
	arr_record = construct_array(arr_elems, cnt, RECORDOID, -1, false, 'd');

	pfree(key_nulls);
	pfree(key_elems);
	pfree(arr_elems);
	for (cnt=0;cnt<rinfo->nkey;++cnt)
		FreeTupleDesc(key_desc[cnt]);
	pfree(key_desc);
	FreeTupleDesc(range_desc);

	return makeConst(RECORDARRAYOID,
					 -1,
					 InvalidOid,
					 -1,
					 PointerGetDatum(arr_record),
					 false,
					 false);
}

static Expr* create_range_bsearch_comp_val(ReduceInfo *rinfo, List *list)
{
	uint32		i;
	ListCell   *lc;
	RowExpr	   *row;

	if (list_length(list) != rinfo->nkey)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("bad argument for create_range_bsearch")));
	}

	/* check type*/
	i = 0;
	foreach(lc, list)
	{
		Oid typid = exprType((Node*)rinfo->keys[i].key);
		if (exprType(lfirst(lc)) != typid)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("bad argument for  create_range_bsearch")));
		}
	}

	row = makeNode(RowExpr);
	row->args = copyObject(list);
	row->row_typeid = RECORDOID;
	row->row_format = COERCE_IMPLICIT_CAST;
	row->colnames = NIL;
	row->location = -1;

	return (Expr*)row;
}

Expr* create_range_bsearch_expr(ReduceInfo *rinfo, List *search)
{
	oidvector  *oids;
	List	   *args;
	uint32		i;
	Assert(rinfo->type == REDUCE_TYPE_RANGE);

	/* val[] */
	oids = buildoidvector_from_list(rinfo->storage_nodes);
	args = list_make1(makeConst(OIDVECTOROID,
								-1,
								InvalidOid,
								-1,
								PointerGetDatum(oids),
								false,
								false));

	/* key[] */
	args = lappend(args, create_range_bsearch_key(rinfo));

	/* comp_fn[] */
	oids = buildoidvector(NULL, rinfo->nkey);
	oids->elemtype = REGPROCOID;
	for (i=0;i<rinfo->nkey;++i)
	{
		Oid typid = exprType((Node*)rinfo->keys[i].key);
		oids->values[i] = get_opfamily_proc(rinfo->keys[i].opfamily,
											typid,
											typid,
											BTORDER_PROC);
		if (!OidIsValid(oids->values[i]))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("operator class %u of access method %s is missing support function %d for type %s",
					 		rinfo->keys[i].opclass,
							"btree",
							BTORDER_PROC,
							format_type_be(typid))));
	}
	args = lappend(args, makeConst(REGPROCARRAYOID,
								   -1,
								   InvalidOid,
								   -1,
								   PointerGetDatum(oids),
								   false,
								   false));

	/* comp_coll */
#warning need a can convert vector
	oids = buildoidvector(NULL, rinfo->nkey);
	for (i=0;i<rinfo->nkey;++i)
		oids->values[i] = rinfo->keys[i].collation;
	args = lappend(args, makeConst(OIDVECTOROID,
								   -1,
								   InvalidOid,
								   -1,
								   PointerGetDatum(oids),
								   false,
								   false));

	/* comp_val */
	args = lappend(args, create_range_bsearch_comp_val(rinfo, search));

	/* not_found */
	args = lappend(args, makeNullConst(OIDOID, -1, InvalidOid));

	return (Expr*)makeFuncExpr(F_RANGE_BSEARCH,
							   OIDOID,
							   args,
							   InvalidOid,
							   InvalidOid,
							   COERCE_EXPLICIT_CALL);
}

typedef struct CompareRangeData
{
	MemoryContext	context;
	PartitionBoundInfoData	boundinfo;
	pg_crc32c		crc_args;
	int				partnatts;
	uint32			val_count;
	Datum			args[4];
	FmgrInfo	   *partsupfunc;					/* length partnatts */
	Oid			   *collation;						/* length partnatts */
	Oid			   *typid;							/* length partnatts */
	TupleDesc		comp_desc;
	Datum		   *comp_datum;
	bool		   *comp_nulls;
	Datum			val[FLEXIBLE_ARRAY_MEMBER];		/* length val_count */
}CompareRangeData;

static CompareRangeData* create_range_data(Datum val, MemoryContext parent)
{
	MemoryContext		oldcontext;
	MemoryContext		context;
	CompareRangeData   *result;
	AnyArrayType	   *arr_val = DatumGetAnyArrayP(val);
	array_iter			val_iter;
	uint32				i,count;
	int16				typlen;
	bool				typbyval;
	char				typalign;
	bool				isnull;

	if (AARR_NDIM(arr_val) != 1 ||
		AARR_HASNULL(arr_val))
		elog(ERROR, "argument val[] is not a 1-D not null array");

	count = AARR_DIMS(arr_val)[0];
	context = AllocSetContextCreate(parent,
									"range_bsearch",
									ALLOCSET_DEFAULT_SIZES);
	result = MemoryContextAllocZero(context, offsetof(CompareRangeData, val) + sizeof(Datum)*count);
	result->context = context;
	result->val_count = count;

	if (VARATT_IS_EXPANDED_HEADER(arr_val))
	{
		typlen = arr_val->xpn.typlen;
		typbyval = arr_val->xpn.typbyval;
		typalign = arr_val->xpn.typalign;
	}else
	{
		get_typlenbyvalalign(AARR_ELEMTYPE(arr_val), &typlen, &typbyval, &typalign);
	}

	oldcontext = CurrentMemoryContext;
	array_iter_setup(&val_iter, arr_val);
	for(i=0;i<count;++i)
	{
		result->val[i] = array_iter_next(&val_iter, &isnull, i, typlen, typbyval, typalign);
		MemoryContextSwitchTo(context);
		result->val[i] = datumCopy(result->val[i], typbyval, typlen);
		MemoryContextSwitchTo(oldcontext);
	}

	if ((Pointer)arr_val != DatumGetPointer(val))
		pfree(arr_val);
	
	return result;
}

static void fill_range_bsearch_part_support(CompareRangeData *range, Datum datum_fn, Datum datum_coll)
{
	AnyArrayType   *arr_fnid = DatumGetAnyArrayP(datum_fn);
	AnyArrayType   *arr_coll = DatumGetAnyArrayP(datum_coll);
	array_iter		fnid_iter;
	array_iter		coll_iter;
	uint32			i,count;
	bool			isnull;

	if (AARR_NDIM(arr_fnid) != 1 ||
		AARR_NDIM(arr_coll) != 1 ||
		AARR_HASNULL(arr_fnid) ||
		AARR_HASNULL(arr_coll))
		elog(ERROR, "argument comp_fn[] or comp_coll[] is not a 1-D not null array");
	
	count = AARR_DIMS(arr_fnid)[0];
	if (count != AARR_DIMS(arr_coll)[0])
		elog(ERROR, "argument comp_fn[] and comp_coll[] element count not equal");

	range->partnatts = count;
	range->partsupfunc = MemoryContextAlloc(range->context,
											sizeof(range->partsupfunc[0]) * count);
	range->collation = MemoryContextAlloc(range->context,
										  sizeof(range->collation[0]) * count);

	array_iter_setup(&fnid_iter, arr_fnid);
	array_iter_setup(&coll_iter, arr_coll);
	for(i=0;i<count;++i)
	{
		Datum datum = array_iter_next(&fnid_iter, &isnull, i, sizeof(Oid), true, 'i');
		fmgr_info_cxt(DatumGetObjectId(datum), &range->partsupfunc[i], range->context);
		
		datum = array_iter_next(&coll_iter, &isnull, i, sizeof(Oid), true, 'i');
		range->collation[i] = DatumGetObjectId(datum);
	}

	if ((Pointer)arr_fnid != DatumGetPointer(datum_fn))
		pfree(arr_fnid);
	if ((Pointer)arr_coll != DatumGetPointer(datum_coll))
		pfree(arr_coll);
}

static PartitionRangeBound* make_partition_rbound_from_datum(CompareRangeData *extra, Datum *datum, int index, bool lower)
{
	PartitionRangeBound *result;
	TupleDesc			desc;
	HeapTupleHeader		rec;
	HeapTupleData		tuple;
	Datum				fields[2];
	bool				nulls[2];
	uint32				i,count = extra->partnatts;
	int16				kind;
	bool				check;

	if (extra->typid == NULL)
	{
		check = false;
		extra->typid = MemoryContextAllocZero(extra->context, sizeof(Oid)*count);
	}else
	{
		check = true;
	}

	result = MemoryContextAllocZero(extra->context, sizeof(*result));
	result->index = index;
	result->datums = palloc0(count * sizeof(Datum));
	result->kind = palloc0(count * sizeof(PartitionRangeDatumKind));
	result->lower = lower;
	for (i=0;i<count;++i)
	{
		rec = DatumGetHeapTupleHeader(datum[i]);
		desc = lookup_rowtype_tupdesc(HeapTupleHeaderGetTypeId(rec),
									  HeapTupleHeaderGetTypMod(rec));
		if (desc->natts != 2)
			elog(ERROR, "bad argument for key[]");
		if (check)
		{
			if(extra->typid[i] != TupleDescAttr(desc, RANGE_DATUM_VALUE_ATTNO-1)->atttypid)
				elog(ERROR, "bad argument for key[]");
		}else
		{
			extra->typid[i] = TupleDescAttr(desc, RANGE_DATUM_VALUE_ATTNO-1)->atttypid;
		}

		tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
		ItemPointerSetInvalid(&(tuple.t_self));
		tuple.t_tableOid = InvalidOid;
		tuple.t_data = rec;
		heap_deform_tuple(&tuple, desc, fields, nulls);

		if (nulls[RANGE_DATUM_KIND_ATTNO-1])
			elog(ERROR, "bad argument for key[]");

		kind = DatumGetInt16(fields[RANGE_DATUM_KIND_ATTNO-1]);
		result->kind[i] = kind;
		if (kind == PARTITION_RANGE_DATUM_VALUE)
		{
			if (nulls[RANGE_DATUM_VALUE_ATTNO-1])
			{
				elog(ERROR, "bad argument for key[]");
			}else
			{
				Form_pg_attribute attr = TupleDescAttr(desc, RANGE_DATUM_VALUE_ATTNO);
				MemoryContext oldcontext = MemoryContextSwitchTo(extra->context);
				result->datums[i] = datumCopy(fields[RANGE_DATUM_VALUE_ATTNO-1], attr->attbyval, attr->attlen);
				MemoryContextSwitchTo(oldcontext);
			}
		}
		ReleaseTupleDesc(desc);
	}

	return result;
}

static bool simple_equal_desc(TupleDesc tupdesc1, TupleDesc tupdesc2)
{
	int i;

	if (tupdesc1 == tupdesc2)
		return true;

	if (tupdesc1->natts != tupdesc2->natts)
		return false;
	if (tupdesc1->tdhasoid != tupdesc2->tdhasoid)
		return false;

	for (i = 0; i < tupdesc1->natts; i++)
	{
		Form_pg_attribute attr1 = TupleDescAttr(tupdesc1, i);
		Form_pg_attribute attr2 = TupleDescAttr(tupdesc2, i);

		if (attr1->atttypid != attr2->atttypid)
			return false;
	}

	return true;
}

static int32
qsort_range_bound_cmp(const void *a, const void *b, void *arg)
{
	PartitionRangeBound *b1 = (*(PartitionRangeBound *const *) a);
	PartitionRangeBound *b2 = (*(PartitionRangeBound *const *) b);
	CompareRangeData *extra = (CompareRangeData*)arg;

	return partition_rbound_cmp(extra->partnatts,
								extra->partsupfunc,
								extra->collation,
								b1->datums,
								b1->kind,
								b1->lower,
								b2);
}

static PartitionRangeBound** restore_range_bsearch_keys(CompareRangeData *extra, Datum datum_keys)
{
	AnyArrayType   *arr_keys = DatumGetAnyArrayP(datum_keys);
	HeapTupleHeader	rec;
	array_iter		iter_keys;
	TupleDesc		desc_check = NULL;
	TupleDesc		desc;
	Datum			datum[PARTITION_MAX_KEYS];
	bool			nulls[PARTITION_MAX_KEYS];
	HeapTupleData	tuple;
	uint32			i,count;
	bool			isnull;

	PartitionRangeBound **all_bounds;

	if (AARR_NDIM(arr_keys) != 1 ||
		AARR_ELEMTYPE(arr_keys) != RECORDOID ||
		AARR_HASNULL(arr_keys))
		elog(ERROR, "argument key[] is not a 1-D not null array of RECORD");
	count = AARR_DIMS(arr_keys)[0];
	if (count != extra->val_count * 2)
		elog(ERROR, "argument key[] element count not right");

	all_bounds = palloc0(sizeof(all_bounds[0]) * count);
	array_iter_setup(&iter_keys, arr_keys);
	for (i=0;i<count;++i)
	{
		rec = DatumGetHeapTupleHeader(array_iter_next(&iter_keys, &isnull, i, -1, false, 'd'));
		if (rec->t_infomask & HEAP_HASNULL)
			elog(ERROR, "argument key[] has null element");
		desc = lookup_rowtype_tupdesc(HeapTupleHeaderGetTypeId(rec),
									  HeapTupleHeaderGetTypMod(rec));

		if (desc_check == NULL)
		{
			int j;
			if (desc->natts != extra->partnatts)
				elog(ERROR, "argument keys[] fields count not equal comp_fn[] element count");
			for (j=0;j<desc->natts;++j)
			{
				if (TupleDescAttr(desc, j)->atttypid != RECORDOID)
					elog(ERROR, "error argument keys[] type");
			}
			desc_check = desc;
			PinTupleDesc(desc_check);
		}else if (simple_equal_desc(desc_check, desc) == false)
		{
			elog(ERROR, "argument key[] RECORD type not all equale");
		}

		tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
		ItemPointerSetInvalid(&(tuple.t_self));
		tuple.t_tableOid = InvalidOid;
		tuple.t_data = rec;
		heap_deform_tuple(&tuple, desc, datum, nulls);

		all_bounds[i] = make_partition_rbound_from_datum(extra, datum, (i>>1)/* i/2 */, i%2==0 ? true:false);
		ReleaseTupleDesc(desc);
	}

	Assert(i==count);
	qsort_arg(all_bounds, count, sizeof(PartitionRangeBound*), qsort_range_bound_cmp, extra);

	if (desc_check)
		ReleaseTupleDesc(desc_check);

	return all_bounds;
}

static void fill_range_bsearch_keys(CompareRangeData *extra, Datum datum_keys)
{
	int					*mapping;
	PartitionRangeBound *prev;
	PartitionRangeBound **rbounds;
	PartitionRangeBound **all_bounds = restore_range_bsearch_keys(extra, datum_keys);

	uint32				ndatums = extra->val_count * 2;
	uint32				i,k;
	int					next_index;

	/* Save distinct bounds from all_bounds into rbounds. */
	rbounds = (PartitionRangeBound**)palloc(ndatums * sizeof(PartitionRangeBound*));
	k = 0;
	prev = NULL;
	for (i = 0; i < ndatums; i++)
	{
		PartitionRangeBound *cur = all_bounds[i];
		bool		is_distinct = false;
		int			j;

		/* Is the current bound distinct from the previous one? */
		for (j = 0; j < extra->partnatts; j++)
		{
			Datum		cmpval;
			if (prev == NULL || cur->kind[j] != prev->kind[j])
			{
				is_distinct = true;
				break;
			}

			/*
			 * If the bounds are both MINVALUE or MAXVALUE, stop now
			 * and treat them as equal, since any values after this
			 * point must be ignored.
			 */
			if (cur->kind[j] != PARTITION_RANGE_DATUM_VALUE)
				break;

			cmpval = FunctionCall2Coll(&extra->partsupfunc[j],
									   extra->collation[j],
									   cur->datums[j],
									   prev->datums[j]);
			if (DatumGetInt32(cmpval) != 0)
			{
				is_distinct = true;
				break;
			}
		}

		/*
		 * Only if the bound is distinct save it into a temporary
		 * array i.e. rbounds which is later copied into boundinfo
		 * datums array.
		 */
		if (is_distinct)
			rbounds[k++] = all_bounds[i];

		prev = cur;
	}

	/* Update ndatums to hold the count of distinct datums. */
	ndatums = k;

	extra->boundinfo.strategy = PARTITION_STRATEGY_RANGE;
	extra->boundinfo.default_index = -1;
	extra->boundinfo.ndatums = ndatums;
	extra->boundinfo.null_index = -1;
	extra->boundinfo.datums = (Datum **)MemoryContextAllocZero(extra->context,
															   ndatums * sizeof(Datum *));

	/* Initialize mapping array with invalid values */
	mapping = (int *) palloc(sizeof(int) * extra->val_count);
	for (i = 0; i < extra->val_count; i++)
		mapping[i] = -1;

	extra->boundinfo.kind = (PartitionRangeDatumKind**)
		MemoryContextAlloc(extra->context,
						   ndatums * sizeof(PartitionRangeDatumKind *));
	extra->boundinfo.indexes = MemoryContextAlloc(extra->context,
												  (ndatums+1) * sizeof(int));

	next_index = 0;
	for (i = 0; i < ndatums; i++)
	{
		int			j;

		extra->boundinfo.datums[i] = (Datum *) MemoryContextAlloc(extra->context,
																  extra->partnatts * sizeof(Datum));
		extra->boundinfo.kind[i] = (PartitionRangeDatumKind *)
			MemoryContextAlloc(extra->context, extra->partnatts * sizeof(PartitionRangeDatumKind));
		for (j = 0; j < extra->partnatts; j++)
		{
			if (rbounds[i]->kind[j] == PARTITION_RANGE_DATUM_VALUE)
				extra->boundinfo.datums[i][j] = rbounds[i]->datums[j];
			extra->boundinfo.kind[i][j] = rbounds[i]->kind[j];
		}

		/*
		 * There is no mapping for invalid indexes.
		 *
		 * Any lower bounds in the rbounds array have invalid
		 * indexes assigned, because the values between the
		 * previous bound (if there is one) and this (lower)
		 * bound are not part of the range of any existing
		 * partition.
		 */
		if (rbounds[i]->lower)
			extra->boundinfo.indexes[i] = -1;
		else
		{
			int			orig_index = rbounds[i]->index;

			/* If the old index has no mapping, assign one */
			if (mapping[orig_index] == -1)
				mapping[orig_index] = next_index++;

			extra->boundinfo.indexes[i] = mapping[orig_index];
		}
	}
	extra->boundinfo.indexes[i] = -1;

	/* swap val */
	{
		Datum *val = palloc(sizeof(Datum) * extra->val_count);
		memcpy(val, extra->val, sizeof(Datum) * extra->val_count);
		for (i=0;i<extra->val_count;++i)
			extra->val[mapping[i]] = val[i];
		pfree(val);
	}
	pfree(mapping);
}

/*
 * range_bsearch(val[],			--anyarray
 *				 key[],			--record array
 *				 comp_fn[],		--proc array
 *				 comp_coll[],	--oidvector
 *				 comp_val,		--record
 *				 not_found		--any, when not found in range(s) result is this argument
 *				 )
 */
Datum range_bsearch(PG_FUNCTION_ARGS)
{
	CompareRangeData   *my_extra = fcinfo->flinfo->fn_extra;
	HeapTupleHeader		rec;
	TupleDesc			desc;
	HeapTupleData		tuple;
	pg_crc32c			crc_args;
	int					i,index;
	bool				has_null;
	bool				is_equal;

	if (PG_ARGISNULL(0) ||
		PG_ARGISNULL(1) ||
		PG_ARGISNULL(2) ||
		PG_ARGISNULL(3))
		PG_RETURN_NULL();

	INIT_CRC32C(crc_args);
	COMP_CRC32C(crc_args, PG_GETARG_POINTER(0), VARSIZE_ANY(PG_GETARG_POINTER(0)));
	COMP_CRC32C(crc_args, PG_GETARG_POINTER(1), VARSIZE_ANY(PG_GETARG_POINTER(1)));
	COMP_CRC32C(crc_args, PG_GETARG_POINTER(2), VARSIZE_ANY(PG_GETARG_POINTER(2)));
	COMP_CRC32C(crc_args, PG_GETARG_POINTER(3), VARSIZE_ANY(PG_GETARG_POINTER(3)));
	FIN_CRC32C(crc_args);

	if (my_extra == NULL ||
		!EQ_CRC32C(my_extra->crc_args, crc_args) ||
		memcmp(fcinfo->arg, my_extra->args, sizeof(my_extra->args)) != 0)
	{
		MemoryContext	oldcontext;
		int				i;

		if (my_extra)
			MemoryContextDelete(my_extra->context);
		my_extra = create_range_data(PG_GETARG_DATUM(0), fcinfo->flinfo->fn_mcxt);
		fcinfo->flinfo->fn_extra = my_extra;
		my_extra->crc_args = crc_args;
		memcpy(my_extra->args, fcinfo->arg, sizeof(my_extra->args));

		fill_range_bsearch_part_support(my_extra, PG_GETARG_DATUM(2), PG_GETARG_DATUM(3));
		fill_range_bsearch_keys(my_extra, PG_GETARG_DATUM(1));

		oldcontext = MemoryContextSwitchTo(my_extra->context);
		my_extra->comp_desc = CreateTemplateTupleDesc(my_extra->partnatts, false);
		for (i=0;i<my_extra->partnatts;++i)
		{
			TupleDescInitEntry(my_extra->comp_desc,
							   i+1,
							   NULL,
							   my_extra->typid[i],
							   -1,
							   0);
		}
		my_extra->comp_datum = palloc(my_extra->partnatts * sizeof(Datum));
		my_extra->comp_nulls = palloc(my_extra->partnatts * sizeof(bool));
		MemoryContextSwitchTo(oldcontext);
	}

	rec = DatumGetHeapTupleHeader(PG_GETARG_DATUM(4));
	desc = lookup_rowtype_tupdesc(HeapTupleHeaderGetTypeId(rec),
								  HeapTupleHeaderGetTypMod(rec));
	if (simple_equal_desc(desc, my_extra->comp_desc) == false)
		elog(ERROR, "bad argument comp_val");

	tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
	ItemPointerSetInvalid(&(tuple.t_self));
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = rec;
	heap_deform_tuple(&tuple, desc, my_extra->comp_datum, my_extra->comp_nulls);
	ReleaseTupleDesc(desc);

	has_null = false;
	for (i=0;i<desc->natts;++i)
	{
		if (my_extra->comp_nulls[i])
		{
			has_null = true;
			break;
		}
	}
	if (has_null)
	{
		index = -1;
	}else
	{
		index = partition_range_datum_bsearch(my_extra->partsupfunc,
											  my_extra->collation,
											  &my_extra->boundinfo,
											  my_extra->partnatts,
											  my_extra->comp_datum,
											  &is_equal);
		/*
		 * The bound at bound_offset is less than or equal to the
		 * tuple value, so the bound at offset+1 is the upper
		 * bound of the partition we're looking for, if there
		 * actually exists one.
		 */
		Assert(index >= 0);
		Assert(index <= my_extra->boundinfo.ndatums);
		index = my_extra->boundinfo.indexes[index + 1];
	}

	if (index < 0)
	{
		if (PG_ARGISNULL(5))
			PG_RETURN_NULL();
		else
			PG_RETURN_DATUM(PG_GETARG_DATUM(5));
	}

	Assert(index < my_extra->val_count);
	PG_RETURN_DATUM(my_extra->val[index]);
}
