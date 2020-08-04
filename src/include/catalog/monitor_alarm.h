
#ifndef MONITOR_ALARM_H
#define MONITOR_ALARM_H

#include "catalog/genbki.h"
#include "catalog/monitor_alarm_d.h"

#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "utils/portal.h"
#include "utils/timestamp.h"

CATALOG(monitor_alarm,5209,MonitorAlarmRelationId)
{
	Oid			oid;
	
	/* alarm level:1(warning),2(critical),3(emergency) */
	int16		ma_alarm_level;

	/* alarm type:1(host),2(database) */
	int16		ma_alarm_type;

	/* alarm time:timestamp with timezone */
	timestamptz	ma_alarm_timetz;

	/* alarm status: 1(unsolved),2(resolved) */
	int16		ma_alarm_status;

#ifdef CATALOG_VARLEN
/* alarm source: ip addr or other string */
	text		ma_alarm_source;

/* alarm text */
	text		ma_alarm_text;
#endif
} FormData_monitor_alarm;

/* ----------------
*		Form_mgr_alarm corresponds to a pointer to a tuple with
*		the format of FormData_monitor_alarm relation.
* ----------------
*/
typedef FormData_monitor_alarm *Form_monitor_alarm;

#endif /* MONITOR_ALARM_H */
