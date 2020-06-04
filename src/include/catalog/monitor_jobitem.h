
#ifndef MONITOR_JOBITEM_H
#define MONITOR_JOBITEM_H

#include "catalog/genbki.h"
#include "catalog/monitor_jobitem_d.h"

#include "utils/timestamp.h"

CATALOG(monitor_jobitem,4804,MjobitemRelationId)
{
	NameData				jobitem_itemname;
#ifdef CATALOG_VARLEN
	text						jobitem_path;
	text						jobitem_desc;
#endif
} FormData_monitor_jobitemitem;

/* ----------------
 *		Form_monitor_jobitemitem corresponds to a pointer to a tuple with
 *		the format of monitor_jobitem relation.
 * ----------------
 */
typedef FormData_monitor_jobitemitem *Form_monitor_jobitemitem;

#endif /* MONITOR_JOBITEM_H */
