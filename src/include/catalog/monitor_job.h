
#ifndef MONITOR_JOB_H
#define MONITOR_JOB_H

#include "catalog/genbki.h"
#include "catalog/monitor_job_d.h"

#include "utils/timestamp.h"

CATALOG(monitor_job,9792,MjobRelationId)
{
	Oid				oid;
	NameData		name;
	timestamptz		next_time;
	int32			interval;
	bool			status;
#ifdef CATALOG_VARLEN
	text			command;
	text			description;
#endif
} FormData_monitor_job;

/* ----------------
 *		Form_monitor_job corresponds to a pointer to a tuple with
 *		the format of monitor_job relation.
 * ----------------
 */
typedef FormData_monitor_job *Form_monitor_job;

DECLARE_UNIQUE_INDEX(monitor_job_oid_index, 9778, on monitor_job using btree(oid oid_ops));
#define MonitorJobOidIndexId 9778

#ifdef EXPOSE_TO_CLIENT_CODE

#define MACRO_STAND_FOR_ALL_JOB  "*"

#endif							/* EXPOSE_TO_CLIENT_CODE */

#endif /* MONITOR_JOB_H */
