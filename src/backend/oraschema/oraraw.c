/*----------------------------------------------------------------------------
 *
 *     oraraw.c
 *     ORARAW type for PostgreSQL.
 *
 *----------------------------------------------------------------------------
 */

#include "postgres.h"
#include "oraschema.h"
#include "utils/builtins.h"


#define VAL(CH)			((CH) - '0')
#define DIG(VAL)		((VAL) + '0')

Datum
ora_raw_in(PG_FUNCTION_ARGS)
{
	char	   *inputText = PG_GETARG_CSTRING(0);
	char	   *sp = inputText;
	char	   *tp;
	char	   *rp;
	int			bc;
	bytea	   *result;

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

	bc += VARHDRSZ;

	result = (bytea *) palloc(bc);
	SET_VARSIZE(result, bc);

	tp = inputText;
	rp = VARDATA(result);
	while (*tp != '\0')
	{
		if (tp[0] != '\\')
			*rp++ = *tp++;
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

	PG_RETURN_BYTEA_P(result);
}


Datum
ora_raw_out(PG_FUNCTION_ARGS)
{
	bytea		*vlena = PG_GETARG_BYTEA_PP(0);
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