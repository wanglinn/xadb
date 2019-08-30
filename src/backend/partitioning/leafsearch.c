
#include "postgres.h"

#include "fmgr.h"
#include "port/pg_crc32c.h"
#include "utils/array.h"
#include "utils/datum.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

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