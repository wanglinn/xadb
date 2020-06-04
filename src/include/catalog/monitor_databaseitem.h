
#ifndef MONITOR_MULTEITEM_H
#define MONITOR_MULTEITEM_H

#include "catalog/genbki.h"
#include "catalog/monitor_databaseitem_d.h"

#include "utils/timestamp.h"

CATALOG(monitor_databaseitem,4815,MdatabaseitemRelationId)
{
	/* monitor timestamp */
	timestamptz		monitor_databaseitem_time;
	NameData		monitor_databaseitem_dbname;
	int64			monitor_databaseitem_dbsize;
	bool			monitor_databaseitem_archivemode;
	bool			monitor_databaseitem_autovacuum;
	float4			monitor_databaseitem_heaphitrate;
	float4			monitor_databaseitem_commitrate;
	int64			monitor_databaseitem_dbage;
	int64			monitor_databaseitem_connectnum;
	int64			monitor_databaseitem_standbydelay;
	int64			monitor_databaseitem_locksnum;
	int64			monitor_databaseitem_longtransnum;
	int64			monitor_databaseitem_idletransnum;
	int64			monitor_databaseitem_preparenum;
	int64			monitor_databaseitem_unusedindexnum;
	int64			monitor_databaseitem_indexsize;
} FormData_monitor_databaseitem;

/* ----------------
 *		Form_monitor_databaseitem corresponds to a pointer to a tuple with
 *		the format of monitor_databaseitem relation.
 * ----------------
 */
typedef FormData_monitor_databaseitem *Form_monitor_databaseitem;

#endif /* MONITOR_MULTEITEM_H */
