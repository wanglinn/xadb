/*-----------------------------------------------------------
 *
 * Portions Copyright (c) 2010-2013, Postgres-XC Development Group
 * Portions Copyright (c) 2014-2017, ADB Development Group
 *
 *-----------------------------------------------------------
 */
#ifndef PGXC_CLASS_H
#define PGXC_CLASS_H

#include "catalog/genbki.h"
#include "catalog/pgxc_class_d.h"

CATALOG(pgxc_class,9020,PgxcClassRelationId) BKI_WITHOUT_OIDS
{
	/* Table Oid */
	Oid			pcrelid;

	/* Type of distribution */
	char		pclocatortype;

	/* Column number of distribution */
	int16		pcattnum;

	/* Hashing algorithm */
	int16		pchashalgorithm;

	/* Number of buckets */
	int16		pchashbuckets;

	/* User-defined distribution function oid */
	Oid			pcfuncid;

	/* List of nodes used by table */
	oidvector	nodeoids;

#ifdef CATALOG_VARLEN

	/* List of column number of distribution */
	int2vector	pcfuncattnums;
#endif
} FormData_pgxc_class;

typedef FormData_pgxc_class *Form_pgxc_class;

#ifdef EXPOSE_TO_CLIENT_CODE

typedef enum PgxcClassAlterType
{
	PGXC_CLASS_ALTER_DISTRIBUTION,
	PGXC_CLASS_ALTER_NODES,
	PGXC_CLASS_ALTER_ALL
} PgxcClassAlterType;

#endif							/* EXPOSE_TO_CLIENT_CODE */

extern void PgxcClassCreate(Oid pcrelid,
							char pclocatortype,
							int pcattnum,
							int pchashalgorithm,
							int pchashbuckets,
							int numnodes,
							Oid *nodes,
							Oid pcfuncid,
							int numatts,
							int16 *pcfuncattnums);
extern void PgxcClassAlter(Oid pcrelid,
						   char pclocatortype,
						   int pcattnum,
						   int pchashalgorithm,
						   int pchashbuckets,
						   int numnodes,
						   Oid *nodes,
						   PgxcClassAlterType type,
						   Oid pcfuncid,
						   int numatts,
						   int16 *pcfuncattnums);
extern void RemovePgxcClass(Oid pcrelid);

extern void CreatePgxcRelationFuncDepend(Oid relid, Oid funcid);
extern void CreatePgxcRelationAttrDepend(Oid relid, AttrNumber attnum);

#endif   /* PGXC_CLASS_H */
