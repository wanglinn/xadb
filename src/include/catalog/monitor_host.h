
#ifndef MONITOR_HOST_H
#define MONITOR_HOST_H

#include "catalog/genbki.h"
#include "catalog/monitor_host_d.h"

#include "catalog/genbki.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "utils/portal.h"
#include "utils/timestamp.h"

CATALOG(monitor_host,9790,MonitorHostRelationId)
{
	Oid			oid;
	
	/* host name */
	NameData	hostname;

	/* host run state */
	int16		mh_run_state;

	/* host cuttent time */
	timestamptz	mh_current_time;

	/* host seconds since boot */
	int64		mh_seconds_since_boot;

	/* host cpu total cores */
	int16		mh_cpu_core_total;

	/* host cpu available cores */
	int16		mh_cpu_core_available;

#ifdef CATALOG_VARLEN

	/* host system */
	text		mh_system;

	/* host plateform type */
	text		mh_platform_type;
#endif
} FormData_monitor_host;

/* ----------------
 *		Form_mgr_host corresponds to a pointer to a tuple with
 *		the format of mgr_host relation.
 * ----------------
 */
typedef FormData_monitor_host *Form_monitor_host;

#endif /* MONITOR_HOST_H */
