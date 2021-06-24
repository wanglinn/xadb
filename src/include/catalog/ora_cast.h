/*-------------------------------------------------------------------------
 *
 * ora_cast.h
 *	  definition of the oracle system "type casts" relation (ora_cast)
 *	  along with the relation's initial contents.
 *
 * Copyright (c) 2002-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 2014-2017, ADB Development Group
 *
 * IDENTIFICATION
 * 		src/include/catalog/ora_cast.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates.bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef ORA_CAST_H
#define ORA_CAST_H

#include "catalog/genbki.h"
#include "catalog/ora_cast_d.h"

/* ----------------
 *		ora_cast definition.  cpp turns this into
 *		typedef struct FormData_ora_cast
 * ----------------
 */

CATALOG(ora_cast,9121,OraCastRelationId)
{
	/* cast id */
	Oid			castid;

	/* source datatype for cast */
	Oid			castsource BKI_LOOKUP(pg_type);

	/* destination datatype for cast */
	Oid			casttarget BKI_LOOKUP(pg_type);

	/* cast function; 0 = binary coercible */
	regproc		castfunc BKI_LOOKUP(pg_proc);

	/* cast truncate function */
	regproc		casttruncfunc BKI_LOOKUP(pg_proc);

	/* contexts in which cast can be used */
	char		castcontext;
} FormData_ora_cast;

typedef FormData_ora_cast *Form_ora_cast;

DECLARE_UNIQUE_INDEX(ora_cast_id_index, 9016, on ora_cast using btree(castid oid_ops));
#define OraCastIdIndexId	9016
DECLARE_UNIQUE_INDEX(ora_cast_source_target_index, 9017, on ora_cast using btree(castsource oid_ops, casttarget oid_ops, castcontext char_ops));
#define OraCastSourceTargetIndexId  9017

#ifdef EXPOSE_TO_CLIENT_CODE
/*
 * Just like pg_cast.CoercionCodes, but it is more complex.
 */
typedef enum OraCoercionContext
{
	ORA_COERCE_DEFAULT = 'd',			/* default coerce context */
	ORA_COERCE_OPERATOR = 'o',			/* coerce with "operator" context */
	ORA_COERCE_COMMON_FUNCTION = 'f',	/* coerce with "common function" context */
	ORA_COERCE_SPECIAL_FUNCTION = 's',	/* coerce with "special function" context */
	ORA_COERCE_NOUSE = 'n'				/* disable coerce */
} OraCoercionContext;

#define OraCoercionContextIsValid(ctx)			\
	((ctx) == ORA_COERCE_DEFAULT ||				\
	 (ctx) == ORA_COERCE_OPERATOR ||			\
	 (ctx) == ORA_COERCE_COMMON_FUNCTION ||		\
	 (ctx) == ORA_COERCE_SPECIAL_FUNCTION ||	\
	 (ctx) == ORA_COERCE_NOUSE)

#endif							/* EXPOSE_TO_CLIENT_CODE */

#endif							/* ORA_CAST_H */
