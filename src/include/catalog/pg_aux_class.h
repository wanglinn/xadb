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

#include "catalog/genbki.h"
#include "catalog/pg_aux_class_d.h"

CATALOG(pg_aux_class,5320,AuxClassRelationId)
{
	/* Auxiliary table Oid */
	Oid			auxrelid;

	/* Parent table Oid */
	Oid			relid;

	/* Auxiliary column number */
	int16		attnum;
} FormData_pg_aux_class;

typedef FormData_pg_aux_class *Form_pg_aux_class;

#ifdef EXPOSE_TO_CLIENT_CODE

#define Natts_aux_table_class		3
#define Anum_aux_table_auxnodeid	1
#define Anum_aux_table_auxctid		2
#define Anum_aux_table_key			3

#endif							/* EXPOSE_TO_CLIENT_CODE */

#endif
