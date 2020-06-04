#ifndef ADB_PROC_H
#define ADB_PROC_H

#include "catalog/genbki.h"
#include "catalog/adb_proc_d.h"

CATALOG(adb_proc,9018,AdbProcRelationId)
{
	/* oid of pg_proc */
	Oid			proowner BKI_LOOKUP(pg_proc);

	/* cluster safe? see PROC_CLUSTER_XXX */
	char		proclustersafe;
	char		proslavesafe;
}FormData_adb_proc;

typedef FormData_adb_proc *Form_adb_proc;

#ifdef EXPOSE_TO_CLIENT_CODE

/* can run in coordinator or datanode */
#define PROC_CLUSTER_SAFE		's'
/* can run in coordinator only */
#define PROC_CLUSTER_RESTRICTED	'r'
/* can not run in cluster plan */
#define PROC_CLUSTER_UNSAFE		'u'

/* can run in datanode master or datanode slave*/
#define PROC_SLAVE_SAFE			's'
/* can run in datanode master only */
#define PROC_SLAVE_RESTRICTED	'r'
/* can not run in datanode slave */
#define PROC_SLAVE_UNSAFE		'u'

#endif							/* EXPOSE_TO_CLIENT_CODE */

#endif							/* ADB_PROC_H */
