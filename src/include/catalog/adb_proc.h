#ifndef ADB_PROC_H
#define ADB_PROC_H

#ifdef BUILD_BKI
#include "catalog/buildbki.h"
#else /* BUILD_BKI */
#include "catalog/genbki.h"
#endif /* BUILD_BKI */

#define AdbProcRelationId 9009

CATALOG(adb_proc,9009) BKI_WITHOUT_OIDS
{
	Oid		proowner;		/* oid of pg_proc */
	char	proclustersafe;	/* cluster safe? see PROC_CLUSTER_XXX */
}FormData_adb_proc;

typedef FormData_adb_proc *Form_adb_proc;

#define Natts_adb_proc					2
#define Anum_adb_proc_proowner			1
#define Anum_adb_proc_proclustersafe	2

#define PROC_CLUSTER_SAFE		's'		/* can run in coordinator or datanode */
#define PROC_CLUSTER_RESTRICTED	'r'		/* can run in coordinator only */
#define PROC_CLUSTER_UNSAFE		'u'		/* can not run in cluster plan */

/* pg_backend_pid() */
DATA(insert ( 2026 r));

/* pg_table_size(regclass) */
DATA(insert ( 2997 u));
/* pg_indexes_size(regclass) */
DATA(insert ( 2998 u));
/* pg_relation_size(regclass) */
DATA(insert ( 2332 u));
/* pg_total_relation_size(regclass) */
DATA(insert ( 2286 u));
/* pg_database_size(Oid) */
DATA(insert ( 2324 u));
/* pg_database_size(Name) */
DATA(insert ( 2168 u));

/* nextval(regclass) */
DATA(insert ( 1574 r));
/* currval(regclass) */
DATA(insert ( 1575 r));
/* setval(regclass,bigint) */
DATA(insert ( 1576 r));
/* setval(regclass,bigint,boolean) */
DATA(insert ( 1765 r));

#endif /* ADB_PROC_H */
