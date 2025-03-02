/*-------------------------------------------------------------------------
 *
 * hashfunc.c
 *	  Support functions for hash access method.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/hash/hashfunc.c
 *
 * NOTES
 *	  These functions are stored in pg_amproc.  For each operator class
 *	  defined for hash indexes, they compute the hash value of the argument.
 *
 *	  Additional hash functions appear in /utils/adt/ files for various
 *	  specialized datatypes.
 *
 *	  It is expected that every bit of a hash function's 32-bit result is
 *	  as random as every other; failure to ensure this is likely to lead
 *	  to poor performance of hash joins, for example.  In most cases a hash
 *	  function should use hash_any() or its variant hash_uint32().
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/hash.h"
#include "catalog/pg_collation.h"
#include "common/hashfn.h"
#include "utils/builtins.h"
#include "utils/pg_locale.h"
#ifdef ADB
#include "catalog/pg_type_d.h"
#include "utils/array.h"
#endif

/*
 * Datatype-specific hash functions.
 *
 * These support both hash indexes and hash joins.
 *
 * NOTE: some of these are also used by catcache operations, without
 * any direct connection to hash indexes.  Also, the common hash_any
 * routine is also used by dynahash tables.
 */

/* Note: this is used for both "char" and boolean datatypes */
Datum
hashchar(PG_FUNCTION_ARGS)
{
	return hash_uint32((int32) PG_GETARG_CHAR(0));
}

Datum
hashcharextended(PG_FUNCTION_ARGS)
{
	return hash_uint32_extended((int32) PG_GETARG_CHAR(0), PG_GETARG_INT64(1));
}

Datum
hashint2(PG_FUNCTION_ARGS)
{
	return hash_uint32((int32) PG_GETARG_INT16(0));
}

Datum
hashint2extended(PG_FUNCTION_ARGS)
{
	return hash_uint32_extended((int32) PG_GETARG_INT16(0), PG_GETARG_INT64(1));
}

Datum
hashint4(PG_FUNCTION_ARGS)
{
	return hash_uint32(PG_GETARG_INT32(0));
}

Datum
hashint4extended(PG_FUNCTION_ARGS)
{
	return hash_uint32_extended(PG_GETARG_INT32(0), PG_GETARG_INT64(1));
}

Datum
hashint8(PG_FUNCTION_ARGS)
{
	/*
	 * The idea here is to produce a hash value compatible with the values
	 * produced by hashint4 and hashint2 for logically equal inputs; this is
	 * necessary to support cross-type hash joins across these input types.
	 * Since all three types are signed, we can xor the high half of the int8
	 * value if the sign is positive, or the complement of the high half when
	 * the sign is negative.
	 */
	int64		val = PG_GETARG_INT64(0);
	uint32		lohalf = (uint32) val;
	uint32		hihalf = (uint32) (val >> 32);

	lohalf ^= (val >= 0) ? hihalf : ~hihalf;

	return hash_uint32(lohalf);
}

Datum
hashint8extended(PG_FUNCTION_ARGS)
{
	/* Same approach as hashint8 */
	int64		val = PG_GETARG_INT64(0);
	uint32		lohalf = (uint32) val;
	uint32		hihalf = (uint32) (val >> 32);

	lohalf ^= (val >= 0) ? hihalf : ~hihalf;

	return hash_uint32_extended(lohalf, PG_GETARG_INT64(1));
}

Datum
hashoid(PG_FUNCTION_ARGS)
{
	return hash_uint32((uint32) PG_GETARG_OID(0));
}

Datum
hashoidextended(PG_FUNCTION_ARGS)
{
	return hash_uint32_extended((uint32) PG_GETARG_OID(0), PG_GETARG_INT64(1));
}

Datum
hashenum(PG_FUNCTION_ARGS)
{
	return hash_uint32((uint32) PG_GETARG_OID(0));
}

Datum
hashenumextended(PG_FUNCTION_ARGS)
{
	return hash_uint32_extended((uint32) PG_GETARG_OID(0), PG_GETARG_INT64(1));
}

Datum
hashfloat4(PG_FUNCTION_ARGS)
{
	float4		key = PG_GETARG_FLOAT4(0);
	float8		key8;

	/*
	 * On IEEE-float machines, minus zero and zero have different bit patterns
	 * but should compare as equal.  We must ensure that they have the same
	 * hash value, which is most reliably done this way:
	 */
	if (key == (float4) 0)
		PG_RETURN_UINT32(0);

	/*
	 * To support cross-type hashing of float8 and float4, we want to return
	 * the same hash value hashfloat8 would produce for an equal float8 value.
	 * So, widen the value to float8 and hash that.  (We must do this rather
	 * than have hashfloat8 try to narrow its value to float4; that could fail
	 * on overflow.)
	 */
	key8 = key;

	return hash_any((unsigned char *) &key8, sizeof(key8));
}

Datum
hashfloat4extended(PG_FUNCTION_ARGS)
{
	float4		key = PG_GETARG_FLOAT4(0);
	uint64		seed = PG_GETARG_INT64(1);
	float8		key8;

	/* Same approach as hashfloat4 */
	if (key == (float4) 0)
		PG_RETURN_UINT64(seed);
	key8 = key;

	return hash_any_extended((unsigned char *) &key8, sizeof(key8), seed);
}

Datum
hashfloat8(PG_FUNCTION_ARGS)
{
	float8		key = PG_GETARG_FLOAT8(0);

	/*
	 * On IEEE-float machines, minus zero and zero have different bit patterns
	 * but should compare as equal.  We must ensure that they have the same
	 * hash value, which is most reliably done this way:
	 */
	if (key == (float8) 0)
		PG_RETURN_UINT32(0);

	return hash_any((unsigned char *) &key, sizeof(key));
}

Datum
hashfloat8extended(PG_FUNCTION_ARGS)
{
	float8		key = PG_GETARG_FLOAT8(0);
	uint64		seed = PG_GETARG_INT64(1);

	/* Same approach as hashfloat8 */
	if (key == (float8) 0)
		PG_RETURN_UINT64(seed);

	return hash_any_extended((unsigned char *) &key, sizeof(key), seed);
}

Datum
hashoidvector(PG_FUNCTION_ARGS)
{
	oidvector  *key = (oidvector *) PG_GETARG_POINTER(0);

	return hash_any((unsigned char *) key->values, key->dim1 * sizeof(Oid));
}

Datum
hashoidvectorextended(PG_FUNCTION_ARGS)
{
	oidvector  *key = (oidvector *) PG_GETARG_POINTER(0);

	return hash_any_extended((unsigned char *) key->values,
							 key->dim1 * sizeof(Oid),
							 PG_GETARG_INT64(1));
}

Datum
hashname(PG_FUNCTION_ARGS)
{
	char	   *key = NameStr(*PG_GETARG_NAME(0));

	return hash_any((unsigned char *) key, strlen(key));
}

Datum
hashnameextended(PG_FUNCTION_ARGS)
{
	char	   *key = NameStr(*PG_GETARG_NAME(0));

	return hash_any_extended((unsigned char *) key, strlen(key),
							 PG_GETARG_INT64(1));
}

Datum
hashtext(PG_FUNCTION_ARGS)
{
	text	   *key = PG_GETARG_TEXT_PP(0);
	Oid			collid = PG_GET_COLLATION();
	pg_locale_t mylocale = 0;
	Datum		result;

	if (!collid)
		ereport(ERROR,
				(errcode(ERRCODE_INDETERMINATE_COLLATION),
				 errmsg("could not determine which collation to use for string hashing"),
				 errhint("Use the COLLATE clause to set the collation explicitly.")));

	if (!lc_collate_is_c(collid) && collid != DEFAULT_COLLATION_OID)
		mylocale = pg_newlocale_from_collation(collid);

	if (!mylocale || mylocale->deterministic)
	{
		result = hash_any((unsigned char *) VARDATA_ANY(key),
						  VARSIZE_ANY_EXHDR(key));
	}
	else
	{
#ifdef USE_ICU
		if (mylocale->provider == COLLPROVIDER_ICU)
		{
			int32_t		ulen = -1;
			UChar	   *uchar = NULL;
			Size		bsize;
			uint8_t    *buf;

			ulen = icu_to_uchar(&uchar, VARDATA_ANY(key), VARSIZE_ANY_EXHDR(key));

			bsize = ucol_getSortKey(mylocale->info.icu.ucol,
									uchar, ulen, NULL, 0);
			buf = palloc(bsize);
			ucol_getSortKey(mylocale->info.icu.ucol,
							uchar, ulen, buf, bsize);

			result = hash_any(buf, bsize);

			pfree(buf);
		}
		else
#endif
			/* shouldn't happen */
			elog(ERROR, "unsupported collprovider: %c", mylocale->provider);
	}

	/* Avoid leaking memory for toasted inputs */
	PG_FREE_IF_COPY(key, 0);

	return result;
}

Datum
hashtextextended(PG_FUNCTION_ARGS)
{
	text	   *key = PG_GETARG_TEXT_PP(0);
	Oid			collid = PG_GET_COLLATION();
	pg_locale_t mylocale = 0;
	Datum		result;

	if (!collid)
		ereport(ERROR,
				(errcode(ERRCODE_INDETERMINATE_COLLATION),
				 errmsg("could not determine which collation to use for string hashing"),
				 errhint("Use the COLLATE clause to set the collation explicitly.")));

	if (!lc_collate_is_c(collid) && collid != DEFAULT_COLLATION_OID)
		mylocale = pg_newlocale_from_collation(collid);

	if (!mylocale || mylocale->deterministic)
	{
		result = hash_any_extended((unsigned char *) VARDATA_ANY(key),
								   VARSIZE_ANY_EXHDR(key),
								   PG_GETARG_INT64(1));
	}
	else
	{
#ifdef USE_ICU
		if (mylocale->provider == COLLPROVIDER_ICU)
		{
			int32_t		ulen = -1;
			UChar	   *uchar = NULL;
			Size		bsize;
			uint8_t    *buf;

			ulen = icu_to_uchar(&uchar, VARDATA_ANY(key), VARSIZE_ANY_EXHDR(key));

			bsize = ucol_getSortKey(mylocale->info.icu.ucol,
									uchar, ulen, NULL, 0);
			buf = palloc(bsize);
			ucol_getSortKey(mylocale->info.icu.ucol,
							uchar, ulen, buf, bsize);

			result = hash_any_extended(buf, bsize, PG_GETARG_INT64(1));

			pfree(buf);
		}
		else
#endif
			/* shouldn't happen */
			elog(ERROR, "unsupported collprovider: %c", mylocale->provider);
	}

	PG_FREE_IF_COPY(key, 0);

	return result;
}

/*
 * hashvarlena() can be used for any varlena datatype in which there are
 * no non-significant bits, ie, distinct bitpatterns never compare as equal.
 */
Datum
hashvarlena(PG_FUNCTION_ARGS)
{
	struct varlena *key = PG_GETARG_VARLENA_PP(0);
	Datum		result;

	result = hash_any((unsigned char *) VARDATA_ANY(key),
					  VARSIZE_ANY_EXHDR(key));

	/* Avoid leaking memory for toasted inputs */
	PG_FREE_IF_COPY(key, 0);

	return result;
}

Datum
hashvarlenaextended(PG_FUNCTION_ARGS)
{
	struct varlena *key = PG_GETARG_VARLENA_PP(0);
	Datum		result;

	result = hash_any_extended((unsigned char *) VARDATA_ANY(key),
							   VARSIZE_ANY_EXHDR(key),
							   PG_GETARG_INT64(1));

	PG_FREE_IF_COPY(key, 0);

	return result;
}

#ifdef ADB
Datum hash_combin_mod(PG_FUNCTION_ARGS)
{
	uint32 hashval;
	uint32 result;
	int i,count;

	if (get_fn_expr_variadic(fcinfo->flinfo))
	{
		ArrayType  *arr = PG_GETARG_ARRAYTYPE_P(1);
		uint32	   *ptr;
		count = ARR_DIMS(arr)[0];

		if (ARR_NDIM(arr) != 1 ||
			count < 1 ||
			ARR_HASNULL(arr) ||
			ARR_ELEMTYPE(arr) != INT4OID)
		{
			elog(ERROR, "argument is not a 1-D int4 array");
		}

		ptr = (uint32*) ARR_DATA_PTR(arr);

		hashval = ptr[0];
		for (i=1;i<count;++i)
			hashval = hash_combine(hashval, ptr[i]);
	}else
	{
		Assert(PG_NARGS() > 1);
		count = PG_NARGS();

		hashval = PG_GETARG_UINT32(1);
		for (i=2;i<count;++i)
			hashval = hash_combine(hashval, PG_GETARG_UINT32(i));
	}

	result = hashval % PG_GETARG_UINT32(0);

	PG_RETURN_UINT32(result);
}
#endif
