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

#endif							/* ADB_CLEAN_H */