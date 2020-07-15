
#ifndef MONITOR_VARPARM_H
#define MONITOR_VARPARM_H

#include "catalog/genbki.h"
#include "catalog/monitor_varparm_d.h"

CATALOG(monitor_varparm,9799,MonitorVarParmRelationId)
{
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

#endif /* MONITOR_VARPARM_H */
