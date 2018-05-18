/*-------------------------------------------------------------------------
 *
 * pg_aux_class.h
 *	  record auxiliary relation on column of master relation.
 *
 * Portions Copyright (c) 2018, ADB Development Group
 *
 * src/include/catalog/pg_aux_class.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_AUX_CLASS
#define PG_AUX_CLASS

#ifdef BUILD_BKI
#include "catalog/buildbki.h"
#else /* BUILD_BKI */
#include "catalog/genbki.h"
#endif /* BUILD_BKI */

#define AuxClassRelationId 5320

CATALOG(pg_aux_class,5320) BKI_WITHOUT_OIDS
{
	Oid		auxrelid;			/* Auxiliary table Oid */
	Oid		relid;				/* Parent table Oid */
	int16	attnum;				/* Auxiliary column number */
} FormData_pg_aux_class;

typedef FormData_pg_aux_class *Form_pg_aux_class;

#define Natts_pg_aux_class			3

#define Anum_pg_aux_class_auxrelid	1
#define Anum_pg_aux_class_relid		2
#define Anum_pg_aux_class_attnum	3

#endif
