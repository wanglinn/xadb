
#ifndef MONITOR_VARPARM_H
#define MONITOR_VARPARM_H

#include "catalog/genbki.h"
#include "catalog/monitor_varparm_d.h"

CATALOG(monitor_varparm,9799,MonitorVarParmRelationId)
{
	Oid			oid;
	
	/* CPU threshold,More than this value will alarm */
	int16		mv_cpu_threshold;

	/* memory threshold,More than this value will alarm */
	int16		mv_mem_threshold;

	/* disk threshold,More than this value will alarm */
	int16		mv_disk_threshold;
} FormData_monitor_varparm;

/* ----------------
 *		Form_monitor_varparm corresponds to a pointer to a tuple with
 *	the format of monitor_varparm relation.
 * ----------------
 */
typedef FormData_monitor_varparm *Form_monitor_varparm;

DECLARE_UNIQUE_INDEX(monitor_varparm_oid_index, 9773, on monitor_varparm using btree(oid oid_ops));
#define MonitorVarparmOidIndexId 9773

#endif /* MONITOR_VARPARM_H */
