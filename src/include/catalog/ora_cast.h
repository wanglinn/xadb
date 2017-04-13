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

#ifdef BUILD_BKI
#include "catalog/buildbki.h"
#else /* BUILD_BKI */
#include "catalog/genbki.h"
#endif /* BUILD_BKI */

/* ----------------
 *		ora_cast definition.  cpp turns this into
 *		typedef struct FormData_ora_cast
 * ----------------
 */
#define OraCastRelationId	4032

CATALOG(ora_cast,4032)
{
	Oid			castsource;		/* source datatype for cast */
	Oid			casttarget;		/* destination datatype for cast */
	Oid			castfunc;		/* cast function; 0 = binary coercible */
	Oid			casttruncfunc;	/* cast truncate function */
	char		castcontext;	/* contexts in which cast can be used */
} FormData_ora_cast;

typedef FormData_ora_cast *Form_ora_cast;

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

/* ----------------
 *		compiler constants for pg_cast
 * ----------------
 */
#define Natts_ora_cast					5
#define Anum_ora_cast_castsource		1
#define Anum_ora_cast_casttarget		2
#define Anum_ora_cast_castfunc			3
#define Anum_ora_cast_casttruncfunc		4
#define Anum_ora_cast_castcontext		5

DATA(insert (	25		700		5379	5379	d	));						/* text -> float4 */
DATA(insert (	25		701		5380	5380	d	));						/* text -> float8 */
DATA(insert (	25		1700	5385	5385	d	));						/* text -> numeric */
DATA(insert (	25		3997	5381	5381	d	));						/* text -> oracle.date */
DATA(insert (	25		1114	5382	5382	d	));						/* text -> timestamp */
DATA(insert (	25		1184	5383	5383	d	));						/* text -> timestamptz */
DATA(insert (	25		2275	5384	5384	d	));						/* text -> cstring */

DATA(insert (	1043	700		5379	5379	d	));						/* varchar -> float4 */
DATA(insert (	1043	701		5380	5380	d	));						/* varchar -> float8 */
DATA(insert (	1043	1700	5385	5385	d	));						/* varchar -> numeric */
DATA(insert (	1043	3997	5381	5381	d	));						/* varchar -> oracle.date */
DATA(insert (	1043	1114	5382	5382	d	));						/* varchar -> timestamp */
DATA(insert (	1043	1184	5383	5383	d	));						/* varchar -> timestamptz */
DATA(insert (	1043	2275	5384	5384	d	));						/* varchar -> cstring */

DATA(insert (	4001	700		5379	5379	d	));						/* varchar2 -> float4 */
DATA(insert (	4001	701		5380	5380	d	));						/* varchar2 -> float8 */
DATA(insert (	4001	1700	5385	5385	d	));						/* varchar2 -> numeric */
DATA(insert (	4001	3997	5381	5381	d	));						/* varchar2 -> oracle.date */
DATA(insert (	4001	1114	5382	5382	d	));						/* varchar2 -> timestamp */
DATA(insert (	4001	1184	5383	5383	d	));						/* varchar2 -> timestamptz */
DATA(insert (	4001	2275	5384	5384	d	));						/* varchar2 -> cstring */

DATA(insert (	4003	700		5379	5379	d	));						/* nvarchar2 -> float4 */
DATA(insert (	4003	701		5380	5380	d	));						/* nvarchar2 -> float8 */
DATA(insert (	4003	1700	5385	5385	d	));						/* nvarchar2 -> numeric */
DATA(insert (	4003	3997	5381	5381	d	));						/* nvarchar2 -> oracle.date */
DATA(insert (	4003	1114	5382	5382	d	));						/* nvarchar2 -> timestamp */
DATA(insert (	4003	1184	5383	5383	d	));						/* nvarchar2 -> timestamptz */
DATA(insert (	4003	2275	5384	5384	d	));						/* nvarchar2 -> cstring */

DATA(insert (	1042	700		5379	5379	d	));						/* bpchar -> float4 */
DATA(insert (	1042	701		5380	5380	d	));						/* bpchar -> float8 */
DATA(insert (	1042	1700	5385	5385	d	));						/* bpchar -> numeric */
DATA(insert (	1042	3997	5381	5381	d	));						/* bpchar -> oracle.date */
DATA(insert (	1042	1114	5382	5382	d	));						/* bpchar -> timestamp */
DATA(insert (	1042	1184	5383	5383	d	));						/* bpchar -> timestamptz */
DATA(insert (	1042	2275	5384	5384	d	));						/* bpchar -> cstring */


DATA(insert (	25		21		5385	5385	o	));						/* text -> int2 => numeric */
DATA(insert (	25		23		5385	5385	o	));						/* text -> int4 => numeric */
DATA(insert (	25		20		5385	5385	o	));						/* text -> int8 => numeric */

DATA(insert (	1043	21		5385	5385	o	));						/* varchar -> int2 => numeric */
DATA(insert (	1043	23		5385	5385	o	));						/* varchar -> int4 => numeric */
DATA(insert (	1043	20		5385	5385	o	));						/* varchar -> int8 => numeric */

DATA(insert (	4001	21		5385	5385	o	));						/* varchar2 -> int2 => numeric */
DATA(insert (	4001	23		5385	5385	o	));						/* varchar2 -> int4 => numeric */
DATA(insert (	4001	20		5385	5385	o	));						/* varchar2 -> int8 => numeric */

DATA(insert (	4003	21		5385	5385	o	));						/* nvarchar2 -> int2 => numeric */
DATA(insert (	4003	23		5385	5385	o	));						/* nvarchar2 -> int4 => numeric */
DATA(insert (	4003	20		5385	5385	o	));						/* nvarchar2 -> int8 => numeric */

DATA(insert (	1042	21		5385	5385	o	));						/* bpchar -> int2 => numeric */
DATA(insert (	1042	23		5385	5385	o	));						/* bpchar -> int4 => numeric */
DATA(insert (	1042	20		5385	5385	o	));						/* bpchar -> int8 => numeric */


DATA(insert (	25		21		5397	5376	f	));						/* trunc(text::numeric) -> int2 */
DATA(insert (	25		23		5398	5377	f	));						/* trunc(text::numeric) -> int4 */
DATA(insert (	25		20		5399	5378	f	));						/* trunc(text::numeric) -> int8 */

DATA(insert (	1043	21		5397	5376	f	));						/* trunc(varchar::numeric) -> int2 */
DATA(insert (	1043	23		5398	5377	f	));						/* trunc(varchar::numeric) -> int4 */
DATA(insert (	1043	20		5399	5378	f	));						/* trunc(varchar::numeric) -> int8 */

DATA(insert (	4001	21		5397	5376	f	));						/* trunc(varchar2::numeric) -> int2 */
DATA(insert (	4001	23		5398	5377	f	));						/* trunc(varchar2::numeric) -> int4 */
DATA(insert (	4001	20		5399	5378	f	));						/* trunc(varchar2::numeric) -> int8 */

DATA(insert (	4003	21		5397	5376	f	));						/* trunc(nvarchar2::numeric) -> int2 */
DATA(insert (	4003	23		5398	5377	f	));						/* trunc(nvarchar2::numeric) -> int4 */
DATA(insert (	4003	20		5399	5378	f	));						/* trunc(nvarchar2::numeric) -> int8 */

DATA(insert (	1042	21		5397	5376	f	));						/* trunc(bpchar::numeric) -> int2 */
DATA(insert (	1042	23		5398	5377	f	));						/* trunc(bpchar::numeric) -> int4 */
DATA(insert (	1042	20		5399	5378	f	));						/* trunc(bpchar::numeric) -> int8 */

DATA(insert (	1700	21		1783	5388	f	));						/* trunc(numeric) -> int2 */
DATA(insert (	1700	23		1744	5389	f	));						/* trunc(numeric) -> int4 */
DATA(insert (	1700	20		1779	5390	f	));						/* trunc(numeric) -> int8 */

DATA(insert (	700		21		238		5391	f	));						/* trunc(float4) -> int2 */
DATA(insert (	700		23		319		5392	f	));						/* trunc(float4) -> int4 */
DATA(insert (	700		20		653		5393	f	));						/* trunc(float4) -> int8 */

DATA(insert (	701		21		237		5394	f	));						/* trunc(float8) -> int2 */
DATA(insert (	701		23		317		5395	f	));						/* trunc(float8) -> int4 */
DATA(insert (	701		20		483		5396	f	));						/* trunc(float8) -> int8 */

DATA(insert (	21		25		5367	5367	f	));						/* int2 -> text */
DATA(insert (	23		25		5367	5367	f	));						/* int4 -> text */
DATA(insert (	20		25		5368	5368	f	));						/* int8 -> text */
DATA(insert (	700		25		5369	5369	f	));						/* float4 -> text */
DATA(insert (	701		25		5370	5370	f	));						/* float8 -> text */
DATA(insert (	1114	25		5373	5373	f	));						/* timestamp -> text */
DATA(insert (	1184	25		5374	5374	f	));						/* timestamptz -> text */
DATA(insert (	704		25		5375	5375	f	));						/* interval -> text */
DATA(insert (	1700	25		5371	5371	f	));						/* numeric -> text */

DATA(insert (	21		1043	5367	5367	f	));						/* int2 -> varchar */
DATA(insert (	23		1043	5367	5367	f	));						/* int4 -> varchar */
DATA(insert (	20		1043	5368	5368	f	));						/* int8 -> varchar */
DATA(insert (	700		1043	5369	5369	f	));						/* float4 -> varchar */
DATA(insert (	701		1043	5370	5370	f	));						/* float8 -> varchar */
DATA(insert (	1114	1043	5373	5373	f	));						/* timestamp -> varchar */
DATA(insert (	1184	1043	5374	5374	f	));						/* timestamptz -> varchar */
DATA(insert (	704		1043	5375	5375	f	));						/* interval -> varchar */
DATA(insert (	1700	1043	5371	5371	f	));						/* numeric -> varchar */

DATA(insert (	21		4001	5367	5367	f	));						/* int2 -> varchar2 */
DATA(insert (	23		4001	5367	5367	f	));						/* int4 -> varchar2 */
DATA(insert (	20		4001	5368	5368	f	));						/* int8 -> varchar2 */
DATA(insert (	700		4001	5369	5369	f	));						/* float4 -> varchar2 */
DATA(insert (	701		4001	5370	5370	f	));						/* float8 -> varchar2 */
DATA(insert (	1114	4001	5373	5373	f	));						/* timestamp -> varchar2 */
DATA(insert (	1184	4001	5374	5374	f	));						/* timestamptz -> varchar2 */
DATA(insert (	704		4001	5375	5375	f	));						/* interval -> varchar2 */
DATA(insert (	1700	4001	5371	5371	f	));						/* numeric -> varchar2 */

DATA(insert (	21		4003	5367	5367	f	));						/* int2 -> nvarchar2 */
DATA(insert (	23		4003	5367	5367	f	));						/* int4 -> nvarchar2 */
DATA(insert (	20		4003	5368	5368	f	));						/* int8 -> nvarchar2 */
DATA(insert (	700		4003	5369	5369	f	));						/* float4 -> nvarchar2 */
DATA(insert (	701		4003	5370	5370	f	));						/* float8 -> nvarchar2 */
DATA(insert (	1114	4003	5373	5373	f	));						/* timestamp -> nvarchar2 */
DATA(insert (	1184	4003	5374	5374	f	));						/* timestamptz -> nvarchar2 */
DATA(insert (	704		4003	5375	5375	f	));						/* interval -> nvarchar2 */
DATA(insert (	1700	4003	5371	5371	f	));						/* numeric -> nvarchar2 */

DATA(insert (	21		1042	5367	5367	f	));						/* int2 -> bpchar */
DATA(insert (	23		1042	5367	5367	f	));						/* int4 -> bpchar */
DATA(insert (	20		1042	5368	5368	f	));						/* int8 -> bpchar */
DATA(insert (	700		1042	5369	5369	f	));						/* float4 -> bpchar */
DATA(insert (	701		1042	5370	5370	f	));						/* float8 -> bpchar */
DATA(insert (	1114	1042	5373	5373	f	));						/* timestamp -> bpchar */
DATA(insert (	1184	1042	5374	5374	f	));						/* timestamptz -> bpchar */
DATA(insert (	704		1042	5375	5375	f	));						/* interval -> bpchar */
DATA(insert (	1700	1042	5371	5371	f	));						/* numeric -> bpchar */

DATA(insert (	21		1700	1782	1782	f	));						/* int2 -> numeric */
DATA(insert (	23		1700	1740	1740	f	));						/* int4 -> numeric */
DATA(insert (	20		1700	1781	1781	f	));						/* int8 -> numeric */
DATA(insert (	700		1700	1742	1742	f	));						/* float4 -> numeric */
DATA(insert (	701		1700	1743	1743	f	));						/* float8 -> numeric */

DATA(insert (	1700	700		1745	1745	f	));						/* numeric -> float4 */
DATA(insert (	1700	701		1746	1746	f	));						/* numeric -> float8 */

#endif /* ORA_CAST_H */
