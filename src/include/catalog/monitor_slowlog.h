
#ifndef MONITOR_SLOWLOG_H
#define MONITOR_SLOWLOG_H

#include "catalog/genbki.h"
#include "catalog/monitor_slowlog_d.h"

#include "utils/timestamp.h"

CATALOG(monitor_slowlog,4817,MslowlogRelationId)
{
	/*the database name*/
	NameData	slowlogdbname;

	NameData	slowloguser;

	/*single query one time*/
	float4		slowlogsingletime;

	/*how many num the query has runned*/
	int32		slowlogtotalnum;

	/* monitor tps timestamp */
	timestamptz	slowlogtime;

#ifdef CATALOG_VARLEN

	/*the query*/
	text		slowlogquery;

	/*plan for the query*/
	text		slowlogqueryplan;
#endif /* CATALOG_VARLEN */
} FormData_monitor_slowlog;

/* ----------------
 *		Form_monitor_slowlog corresponds to a pointer to a tuple with
 *		the format of monitor_slowlog relation.
 * ----------------
 */
typedef FormData_monitor_slowlog *Form_monitor_slowlog;

#endif /* MONITOR_SLOWLOG_H */
