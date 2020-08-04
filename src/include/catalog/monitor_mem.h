
#ifndef MONITOR_MEM_H
#define MONITOR_MEM_H

#include "catalog/genbki.h"
#include "catalog/monitor_mem_d.h"

#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "utils/portal.h"
#include "utils/timestamp.h"

CATALOG(monitor_mem,4807,MonitorMemRelationId)
{
	Oid			oid;
	
	/* host name */
	NameData	hostname;

	/* monitor memory timestamp */
	timestamptz	mm_timestamptz;

	/* monitor memory total */
	int64		mm_total;

	/* monitor memory used */
	int64		mm_used;

	/* monitor memory usage */
	float4		mm_usage;
} FormData_monitor_mem;

/* ----------------
 *		Form_monitor_mem corresponds to a pointer to a tuple with
 *		the format of moniotr_mem relation.
 * ----------------
 */
typedef FormData_monitor_mem *Form_monitor_mem;

#endif /* MONITOR_MEM_H */
