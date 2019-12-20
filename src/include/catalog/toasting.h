/*-------------------------------------------------------------------------
 *
 * toasting.h
 *	  This file provides some definitions to support creation of toast tables
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/toasting.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TOASTING_H
#define TOASTING_H

#ifdef BUILD_BKI
#include "catalog/buildbki.h"
#else /* BUILD_BKI */
#include "storage/lock.h"
#include "catalog/genbki.h"
#endif /* BUILD_BKI */

/*
 * toasting.c prototypes
 */
extern void NewRelationCreateToastTable(Oid relOid, Datum reloptions);
extern void NewHeapCreateToastTable(Oid relOid, Datum reloptions,
						LOCKMODE lockmode);
extern void AlterTableCreateToastTable(Oid relOid, Datum reloptions,
						   LOCKMODE lockmode);
extern void BootstrapToastTable(char *relName,
					Oid toastOid, Oid toastIndexOid);


/*
 * This macro is just to keep the C compiler from spitting up on the
 * upcoming commands for Catalog.pm.
 */
#ifndef BUILD_BKI
#define DECLARE_TOAST(name,toastoid,indexoid) extern int no_such_variable
#endif

/*
 * What follows are lines processed by genbki.pl to create the statements
 * the bootstrap parser will turn into BootstrapToastTable commands.
 * Each line specifies the system catalog that needs a toast table,
 * the OID to assign to the toast table, and the OID to assign to the
 * toast table's index.  The reason we hard-wire these OIDs is that we
 * need stable OIDs for shared relations, and that includes toast tables
 * of shared relations.
 */

/* normal catalogs */
DECLARE_TOAST(pg_attrdef, 2830, 2831);
DECLARE_TOAST(pg_constraint, 2832, 2833);
DECLARE_TOAST(pg_description, 2834, 2835);
DECLARE_TOAST(pg_proc, 2836, 2837);
DECLARE_TOAST(pg_rewrite, 2838, 2839);
DECLARE_TOAST(pg_seclabel, 3598, 3599);
DECLARE_TOAST(pg_statistic, 2840, 2841);
DECLARE_TOAST(pg_statistic_ext, 3439, 3440);
DECLARE_TOAST(pg_trigger, 2336, 2337);

/* shared catalogs */
DECLARE_TOAST(pg_shdescription, 2846, 2847);
#define PgShdescriptionToastTable 2846
#define PgShdescriptionToastIndex 2847
DECLARE_TOAST(pg_db_role_setting, 2966, 2967);
#define PgDbRoleSettingToastTable 2966
#define PgDbRoleSettingToastIndex 2967
DECLARE_TOAST(pg_shseclabel, 4060, 4061);
#define PgShseclabelToastTable 4060
#define PgShseclabelToastIndex 4061
#ifdef ADB
DECLARE_TOAST(adb_clean, 8894, 9117);
#define AdbCleanToastTable 8894
#define AdbCleanToastIndex 9117
#endif /* ADB */

#endif							/* TOASTING_H */
