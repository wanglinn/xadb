/*-------------------------------------------------------------------------
 *
 * pgxc_group.h
 *	  definition of the system "PGXC group" relation (pgxc_group)
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012, Postgres-XC Development Group
 * Portions Copyright (c) 2014-2017, ADB Development Group
 *
 * src/include/catalog/pgxc_group.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGXC_GROUP_H
#define PGXC_GROUP_H

#include "catalog/genbki.h"
#include "catalog/pgxc_group_d.h"

CATALOG(pgxc_group,9014,PgxcGroupRelationId) BKI_SHARED_RELATION
{
	Oid			oid;			/* oid */

	/* Group name */
	NameData	group_name;

	/* VARIABLE LENGTH FIELDS: */
	/* Group members */
	oidvector	group_members;
} FormData_pgxc_group;

typedef FormData_pgxc_group *Form_pgxc_group;

#endif	/* PGXC_GROUP_H */
