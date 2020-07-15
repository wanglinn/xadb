
#ifndef MONITOR_CPU_H
#define MONITOR_CPU_H

#include "catalog/genbki.h"
#include "catalog/monitor_cpu_d.h"

#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "utils/portal.h"
#include "utils/timestamp.h"

CATALOG(monitor_cpu,9786,MonitorCpuRelationId)
{
	Oid			oid;
	
	/* host name */
	NameData	hostname;

	/* monitor cpu timestamptz */
	timestamptz	mc_timestamptz;

	/* monitor cpu usage */
	float4		mc_cpu_usage;

#ifdef CATALOG_VARLEN

/* monitor cpu frequency */
	text		mc_cpu_freq;
#endif
} FormData_monitor_cpu;

/* ----------------
 *		Form_monitor_cpu corresponds to a pointer to a tuple with
 *		the format of moniotr_cpu relation.
 * ----------------
 */
typedef FormData_monitor_cpu *Form_monitor_cpu;

#endif /* MONITOR_CPU_H */
