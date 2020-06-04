
#ifndef MONITOR_HOST_THRESHOLD_H
#define MONITOR_HOST_THRESHOLD_H

#include "catalog/genbki.h"
#include "catalog/monitor_host_threshold_d.h"

#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "utils/portal.h"
#include "utils/timestamp.h"

CATALOG(monitor_host_threshold,4811,MonitorHostThresholdRelationId)
{
	/* host alarm type */
	int16		mt_type;

	/*0 is '<', 1 is '>'*/
	int16		mt_direction;

	/* warning threshold, More than this value will alarm */
	int16		mt_warning_threshold;

	/* critical threshold, More than this value will alarm */
	int16		mt_critical_threshold;

	/* emergency threshold, More than this value will alarm */
	int16		mt_emergency_threshold;
} FormData_monitor_host_threshold;

/* ----------------
 *		FormData_monitor_host_threshold corresponds to a pointer to a tuple with
 *		the format of Form_monitor_host_threshold relation.
 * ----------------
 */
typedef FormData_monitor_host_threshold *Form_monitor_host_threshold;

#endif /* MONITOR_HOST_THRESHOLD_H */
