
#ifndef MONITOR_DISK_H
#define MONITOR_DISK_H

#include "catalog/genbki.h"
#include "catalog/monitor_disk_d.h"

#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "utils/portal.h"
#include "utils/timestamp.h"

CATALOG(monitor_disk,9789,MonitorDiskRelationId)
{
	Oid			oid;
	
	/* host name */
	NameData	hostname;

	/* monitor disk timestamp */
	timestamptz	md_timestamptz;

	/* monitor disk total size */
	int64		md_total;

	/* monitor disk available size */
	int64		md_used;

	/* monitor disk i/o read bytes */
	int64		md_io_read_bytes;

	/* monitor disk i/o read time */
	int64		md_io_read_time;

	/* monitor disk i/o write bytes */
	int64		md_io_write_bytes;

	/* monitor disk i/o wirte time */
	int64		md_io_write_time;
} FormData_monitor_disk;

/* ----------------
 *		Form_monitor_disk corresponds to a pointer to a tuple with
 *		the format of moniotr_disk relation.
 * ----------------
 */
typedef FormData_monitor_disk *Form_monitor_disk;

DECLARE_UNIQUE_INDEX(monitor_disk_oid_index, 9770, on monitor_disk using btree(oid oid_ops));
#define MonitorDiskOidIndexId 9770

#endif /* MONITOR_DISK_H */
