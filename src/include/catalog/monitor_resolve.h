
#ifndef MONITOR_RESOLVE_H
#define MONITOR_RESOLVE_H

#include "catalog/genbki.h"
#include "catalog/monitor_resolve_d.h"

CATALOG(monitor_resolve,8902,MonitorResolveRelationId)
{
	/* table monitor alarm oid */
	Oid		mr_alarm_oid;

	/* alarm resolve time:timestamp with timezone */
	timestamptz	mr_resolve_timetz;

#ifdef CATALOG_VARLEN

	/* alarm solution */
	text	mr_solution;
#endif
} FormData_monitor_resolve;

/* ----------------
*		FormData_monitor_resolve corresponds to a pointer to a tuple with
*		the format of Form_monitor_resolve relation.
* ----------------
*/
typedef FormData_monitor_resolve *Form_monitor_resolve;

#endif /* MONITOR_RESOLVE_H */
