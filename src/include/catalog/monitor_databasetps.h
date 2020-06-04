
#ifndef MONITOR_DATABASETPS_H
#define MONITOR_DATABASETPS_H

#include "catalog/genbki.h"
#include "catalog/monitor_databasetps_d.h"

#include "utils/timestamp.h"

CATALOG(monitor_databasetps,4814,MdatabasetpsRelationId)
{
	/* monitor tps timestamp */
	timestamptz		monitor_databasetps_time;
	NameData		monitor_databasetps_dbname;
	int64			monitor_databasetps_tps;
	int64			monitor_databasetps_qps;
	int64			monitor_databasetps_runtime;
} FormData_monitor_databasetps;

/* ----------------
 *		Form_monitor_databasetps corresponds to a pointer to a tuple with
 *		the format of monitor_databasetps relation.
 * ----------------
 */
typedef FormData_monitor_databasetps *Form_monitor_databasetps;

#endif /* MONITOR_DATABASETPS_H */
