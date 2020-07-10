/*----------------------------------------------------------------------------
 *
 *     oraraw.c
 *     ORARAW type for PostgreSQL.
 *
 *----------------------------------------------------------------------------
 */

#include "postgres.h"

#include "c.h"
#include "fmgr.h"
#include "oraschema.h"
#include "access/tuptoaster.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type_d.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/sortsupport.h"
#include "utils/varlena.h"


#define VAL(CH)			((CH) - '0')
#define DIG(VAL)		((VAL) + '0')

typedef struct varlena Raw;

#define DatumGetRawPP(X)		((Raw *) PG_DETOAST_DATUM_PACKED(X))
#define PG_GETARG_RAW_PP(n)		DatumGetRawPP(PG_GETARG_DATUM(n))
#define PG_RETURN_RAW_P(x)		PG_RETURN_POINTER(x)

Datum
ora_raw_in(PG_FUNCTION_ARGS)
{
	char	   *inputText = PG_GETARG_CSTRING(0);
#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	int32		typmod = PG_GETARG_INT32(2);
	char	   *sp = inputText;
	char	   *tp;
	char	   *rp;
	int			bc;
	Raw		   *result;
	bool		add_zore = false;

	/* Only hexadecimal characters are allowed */
	for (; *sp; sp++)
	{
		if (*sp < '0' || 
		    (*sp > '9' && *sp < 'A') || 
			(*sp > 'F' && *sp < 'a') || 
			*sp > 'f')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						errmsg("\"%c\" is not a valid hexadecimal digit",
							*sp)));
	}

	/* Else, it's the traditional escaped style */
	for (bc = 0, tp = inputText; *tp != '\0'; bc++)
	{
		if (tp[0] != '\\')
			tp++;
		else if ((tp[0] == '\\') &&
				 (tp[1] >= '0' && tp[1] <= '3') &&
				 (tp[2] >= '0' && tp[2] <= '7') &&
				 (tp[3] >= '0' && tp[3] <= '7'))
			tp += 4;
		else if ((tp[0] == '\\') &&
				 (tp[1] == '\\'))
			tp += 2;
		else
		{
			/*
			 * one backslash, not followed by another or ### valid octal
			 */
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type %s", "bytea")));
		}
	}

	if (bc % 2 == 1)
	{
		add_zore = true;
		bc++;
	}
	if (typmod != -1 && bc > typmod * 2)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("Raw type length exceeds the maximum limit (current value: %d, maximum value: %d)", bc/2, typmod)));

	bc += VARHDRSZ;

	result = (Raw *) palloc(bc);
	SET_VARSIZE(result, bc);

	tp = inputText;
	rp = VARDATA(result);
	
	if (add_zore)
		*rp++ = '0';

	while (*tp != '\0')
	{
		if (tp[0] != '\\')
			*rp++ = tolower((unsigned char) *tp++);
		else if ((tp[0] == '\\') &&
				 (tp[1] >= '0' && tp[1] <= '3') &&
				 (tp[2] >= '0' && tp[2] <= '7') &&
				 (tp[3] >= '0' && tp[3] <= '7'))
		{
			bc = VAL(tp[1]);
			bc <<= 3;
			bc += VAL(tp[2]);
			bc <<= 3;
			*rp++ = bc + VAL(tp[3]);

			tp += 4;
		}
		else if ((tp[0] == '\\') &&
				 (tp[1] == '\\'))
		{
			*rp++ = '\\';
			tp += 2;
		}
		else
		{
			/*
			 * We should never get here. The first pass should not allow it.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type %s", "bytea")));
		}
	}

	PG_RETURN_RAW_P(result);
}


Datum
ora_raw_out(PG_FUNCTION_ARGS)
{
	Raw			*vlena = PG_GETARG_RAW_PP(0);
	char		*result;
	char		*rp;

	/* Print traditional escaped format */
	char	   *vp;
	int			len;
	int			i;

	len = 1;				/* empty string has 1 char */
	vp = VARDATA_ANY(vlena);
	for (i = VARSIZE_ANY_EXHDR(vlena); i != 0; i--, vp++)
	{
		if (*vp == '\\')
			len += 2;
		else if ((unsigned char) *vp < 0x20 || (unsigned char) *vp > 0x7e)
			len += 4;
		else
			len++;
	}

	if(len % 2 == 0)
	{
		len++;
		rp = result = (char *) palloc(len);
		*rp++ = '0';
	}
	else
		rp = result = (char *) palloc(len);

	vp = VARDATA_ANY(vlena);
	for (i = VARSIZE_ANY_EXHDR(vlena); i != 0; i--, vp++)
	{
		if (*vp == '\\')
		{
			*rp++ = '\\';
			*rp++ = '\\';
		}
		else if ((unsigned char) *vp < 0x20 || (unsigned char) *vp > 0x7e)
		{
			int			val;	/* holds unprintable chars */

			val = *vp;
			rp[0] = '\\';
			rp[3] = DIG(val & 07);
			val >>= 3;
			rp[2] = DIG(val & 07);
			val >>= 3;
			rp[1] = DIG(val & 03);
			rp += 4;
		}
		else
			*rp++ = toupper((unsigned char) *vp);
	}
	*rp = '\0';
	PG_RETURN_CSTRING(result);
}


Datum
raw_typmodin(PG_FUNCTION_ARGS)
{
	ArrayType  *ta = PG_GETARG_ARRAYTYPE_P(0);
	int			n;
	int32	   typmod = *(ArrayGetIntegerTypmods(ta, &n));

	if (typmod == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("Columns of length 0 are not allowed.")));
	}
	else if (typmod > 2000)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("The specified length is too long for the data type.")));

	PG_RETURN_INT32(typmod);
}


Datum
raw_typmodout(PG_FUNCTION_ARGS)
{
	int32		typmod = PG_GETARG_INT32(0);
	char	   *result;

	result = psprintf("(%d)", (int) typmod);

	PG_RETURN_CSTRING(result);
}


Datum
raweq(PG_FUNCTION_ARGS)
{
	Datum		arg1 = PG_GETARG_DATUM(0);
	Datum		arg2 = PG_GETARG_DATUM(1);
	bool		result;
	Size		len1,
				len2;

	/*
	 * We can use a fast path for unequal lengths, which might save us from
	 * having to detoast one or both values.
	 */
	len1 = toast_raw_datum_size(arg1);
	len2 = toast_raw_datum_size(arg2);
	if (len1 != len2)
		result = false;
	else
	{
		Raw	   *barg1 = DatumGetRawPP(arg1);
		Raw	   *barg2 = DatumGetRawPP(arg2);

		result = (memcmp(VARDATA_ANY(barg1), VARDATA_ANY(barg2),
						 len1 - VARHDRSZ) == 0);

		PG_FREE_IF_COPY(barg1, 0);
		PG_FREE_IF_COPY(barg2, 1);
	}

	PG_RETURN_BOOL(result);
}

Datum
rawne(PG_FUNCTION_ARGS)
{
	Datum		arg1 = PG_GETARG_DATUM(0);
	Datum		arg2 = PG_GETARG_DATUM(1);
	bool		result;
	Size		len1,
				len2;

	/*
	 * We can use a fast path for unequal lengths, which might save us from
	 * having to detoast one or both values.
	 */
	len1 = toast_raw_datum_size(arg1);
	len2 = toast_raw_datum_size(arg2);
	if (len1 != len2)
		result = true;
	else
	{
		Raw	   *barg1 = DatumGetRawPP(arg1);
		Raw	   *barg2 = DatumGetRawPP(arg2);

		result = (memcmp(VARDATA_ANY(barg1), VARDATA_ANY(barg2),
						 len1 - VARHDRSZ) != 0);

		PG_FREE_IF_COPY(barg1, 0);
		PG_FREE_IF_COPY(barg2, 1);
	}

	PG_RETURN_BOOL(result);
}

Datum
rawlt(PG_FUNCTION_ARGS)
{
	Raw	   *arg1 = PG_GETARG_RAW_PP(0);
	Raw	   *arg2 = PG_GETARG_RAW_PP(1);
	int			len1,
				len2;
	int			cmp;

	len1 = VARSIZE_ANY_EXHDR(arg1);
	len2 = VARSIZE_ANY_EXHDR(arg2);

	cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

	PG_FREE_IF_COPY(arg1, 0);
	PG_FREE_IF_COPY(arg2, 1);

	PG_RETURN_BOOL((cmp < 0) || ((cmp == 0) && (len1 < len2)));
}

Datum
rawle(PG_FUNCTION_ARGS)
{
	Raw	   *arg1 = PG_GETARG_RAW_PP(0);
	Raw	   *arg2 = PG_GETARG_RAW_PP(1);
	int			len1,
				len2;
	int			cmp;

	len1 = VARSIZE_ANY_EXHDR(arg1);
	len2 = VARSIZE_ANY_EXHDR(arg2);

	cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

	PG_FREE_IF_COPY(arg1, 0);
	PG_FREE_IF_COPY(arg2, 1);

	PG_RETURN_BOOL((cmp < 0) || ((cmp == 0) && (len1 <= len2)));
}

Datum
rawgt(PG_FUNCTION_ARGS)
{
	Raw	   *arg1 = PG_GETARG_RAW_PP(0);
	Raw	   *arg2 = PG_GETARG_RAW_PP(1);
	int			len1,
				len2;
	int			cmp;

	len1 = VARSIZE_ANY_EXHDR(arg1);
	len2 = VARSIZE_ANY_EXHDR(arg2);

	cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

	PG_FREE_IF_COPY(arg1, 0);
	PG_FREE_IF_COPY(arg2, 1);

	PG_RETURN_BOOL((cmp > 0) || ((cmp == 0) && (len1 > len2)));
}

Datum
rawge(PG_FUNCTION_ARGS)
{
	Raw	   *arg1 = PG_GETARG_RAW_PP(0);
	Raw	   *arg2 = PG_GETARG_RAW_PP(1);
	int			len1,
				len2;
	int			cmp;

	len1 = VARSIZE_ANY_EXHDR(arg1);
	len2 = VARSIZE_ANY_EXHDR(arg2);

	cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

	PG_FREE_IF_COPY(arg1, 0);
	PG_FREE_IF_COPY(arg2, 1);

	PG_RETURN_BOOL((cmp > 0) || ((cmp == 0) && (len1 >= len2)));
}

Datum
rawcmp(PG_FUNCTION_ARGS)
{
	Raw	   *arg1 = PG_GETARG_RAW_PP(0);
	Raw	   *arg2 = PG_GETARG_RAW_PP(1);
	int			len1,
				len2;
	int			cmp;

	len1 = VARSIZE_ANY_EXHDR(arg1);
	len2 = VARSIZE_ANY_EXHDR(arg2);

	cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));
	if ((cmp == 0) && (len1 != len2))
		cmp = (len1 < len2) ? -1 : 1;

	PG_FREE_IF_COPY(arg1, 0);
	PG_FREE_IF_COPY(arg2, 1);

	PG_RETURN_INT32(cmp);
}

Datum
raw_sortsupport(PG_FUNCTION_ARGS)
{
	SortSupport ssup = (SortSupport) PG_GETARG_POINTER(0);
	MemoryContext oldcontext;

	oldcontext = MemoryContextSwitchTo(ssup->ssup_cxt);

	/* Use generic string SortSupport, forcing "C" collation */
	varstr_sortsupport(ssup, ORACLE_RAWOID, C_COLLATION_OID);

	MemoryContextSwitchTo(oldcontext);

	PG_RETURN_VOID();
}

Datum
rawlike(PG_FUNCTION_ARGS)
{
	return(DirectFunctionCall2(bytealike,
							   PG_GETARG_DATUM(0),
							   PG_GETARG_DATUM(1)));
}

Datum
rawnlike(PG_FUNCTION_ARGS)
{
	return(DirectFunctionCall2(byteanlike,
							   PG_GETARG_DATUM(0),
							   PG_GETARG_DATUM(1)));
}

Datum
rawsend(PG_FUNCTION_ARGS)
{
	return byteasend(fcinfo);
}

Datum
rawrecv(PG_FUNCTION_ARGS)
{
	return bytearecv(fcinfo);
}

Datum
raw_hashvarlena(PG_FUNCTION_ARGS)
{
	return hashvarlena(fcinfo);
}

Datum
raw_hashvarlenaextended(PG_FUNCTION_ARGS)
{
	return hashvarlenaextended(fcinfo);
}