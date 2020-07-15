
#ifndef MONITOR_NET_H
#define MONITOR_NET_H

#include "catalog/genbki.h"
#include "catalog/monitor_net_d.h"

#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "utils/portal.h"
#include "utils/timestamp.h"

CATALOG(monitor_net,9795,MonitorNetRelationId)
{
	/* host name */
	NameData	hostname;

	/* monitor network timestamp */
	timestamptz	mn_timestamptz;

	/* monitor network sent speed */
	int64		mn_sent;

	/* monitor network recv speed */
	int64		mn_recv;
} FormData_monitor_net;

/* ----------------
 *		Form_monitor_net corresponds to a pointer to a tuple with
 *		the format of moniotr_net relation.
 * ----------------
 */
typedef FormData_monitor_net *Form_monitor_net;

#endif /* MONITOR_NET_H */
