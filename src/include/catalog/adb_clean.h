#ifndef ADB_CLEAN_H
#define ADB_CLEAN_H

#include "catalog/genbki.h"
#include "catalog/adb_clean_d.h"

CATALOG(adb_clean,9031,AdbCleanRelationId) BKI_SHARED_RELATION
{
	/* database Oid */
	Oid				clndb;

	/* relation Oid */
	Oid				clnrel;

	/* clean less then block number */
	int32			clnblocks;

	/* clean test expr */
	pg_node_tree	clnexpr BKI_FORCE_NOT_NULL;
}FormData_adb_clean;

typedef FormData_adb_clean *Form_adb_clean;

DECLARE_TOAST(adb_clean, 9321, 9117);
#define AdbCleanToastTable 9321
#define AdbCleanToastIndex 9117

DECLARE_UNIQUE_INDEX(adb_clean_index, 9032, on adb_clean using btree(clndb oid_ops, clnrel oid_ops));
#define AdbCleanIndexId				  9032

#endif							/* ADB_CLEAN_H */