/*-------------------------------------------------------------------------
 *
 * pg_namespace.h
 *	  definition of the "namespace" system catalog (pg_namespace)
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_namespace.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_NAMESPACE_H
#define PG_NAMESPACE_H

#ifdef BUILD_BKI
#include "catalog/buildbki.h"
#else /* BUILD_BKI */
#include "catalog/genbki.h"
#endif /* BUILD_BKI */
#include "catalog/pg_namespace_d.h"
#include "utils/acl.h"

/* ----------------------------------------------------------------
 *		pg_namespace definition.
 *
 *		cpp turns this into typedef struct FormData_pg_namespace
 *
 *	nspname				name of the namespace
 *	nspowner			owner (creator) of the namespace
 *	nspacl				access privilege list
 * ----------------------------------------------------------------
 */
CATALOG(pg_namespace,2615,NamespaceRelationId)
{
	Oid			oid;			/* oid */

	NameData	nspname;
	Oid			nspowner BKI_DEFAULT(POSTGRES) BKI_LOOKUP(pg_authid);

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	aclitem		nspacl[1];
#endif
} FormData_pg_namespace;

/* ----------------
 *		Form_pg_namespace corresponds to a pointer to a tuple with
 *		the format of pg_namespace relation.
 * ----------------
 */
typedef FormData_pg_namespace *Form_pg_namespace;

DECLARE_TOAST(pg_namespace, 4163, 4164);

DECLARE_UNIQUE_INDEX(pg_namespace_nspname_index, 2684, on pg_namespace using btree(nspname name_ops));
#define NamespaceNameIndexId  2684
DECLARE_UNIQUE_INDEX_PKEY(pg_namespace_oid_index, 2685, on pg_namespace using btree(oid oid_ops));
#define NamespaceOidIndexId  2685

#ifdef EXPOSE_TO_CLIENT_CODE
/* I have no good idea for auto generate to main pg_*_d.h */
#define PG_MANAGER_NAMESPACE	9099
#endif							/* EXPOSE_TO_CLIENT_CODE */

/*
 * prototypes for functions in pg_namespace.c
 */
extern Oid	NamespaceCreate(const char *nspName, Oid ownerId, bool isTemp);

#endif							/* PG_NAMESPACE_H */
