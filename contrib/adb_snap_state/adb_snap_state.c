#include "postgres.h"

#include <math.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/hash.h"
#include "catalog/pg_authid.h"
#include "executor/instrument.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "parser/analyze.h"
#include "parser/parsetree.h"
#include "parser/scanner.h"
#include "parser/scansup.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/spin.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "pgxc/pgxc.h"
#include "replication/snapsender.h"
#include "replication/gxidreceiver.h"
#include "replication/gxidsender.h"
#include "replication/snapreceiver.h"


#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif
PG_FUNCTION_INFO_V1(adb_snap_state);
Datum adb_snap_state(PG_FUNCTION_ARGS);

#define ADB_SNAPSHOT_RESULT_NUM 2

Datum adb_snap_state(PG_FUNCTION_ARGS)
{
    StringInfoData buf;

    initStringInfo(&buf);
    if (IsGTMNode())
    {
        appendStringInfo(&buf, "SnapSender: \n");
        SnapSenderGetStat(&buf);

        appendStringInfo(&buf, "\nGxidSender: \n");
        GxidSenderGetStat(&buf);
    }
    else
    {
        appendStringInfo(&buf, "SnapReceiver: \n");
        SnapRcvGetStat(&buf);

        appendStringInfo(&buf, "\nGxidReceiver: \n");
        GxidRcvGetStat(&buf);
    }

    PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}