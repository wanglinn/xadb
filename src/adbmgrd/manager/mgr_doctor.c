#include "postgres.h"
#include "nodes/params.h"
#include "tcop/dest.h"
#include "lib/stringinfo.h"
#include "fmgr.h"
#include "utils/relcache.h"
#include "access/heapam.h"
#include "mgr/mgr_agent.h"
#include "utils/timestamp.h"
#include "../../interfaces/libpq/libpq-fe.h"
#include "catalog/mgr_node.h"
#include "utils/relcache.h"
#include "access/heapam.h"
#include "executor/spi.h"
#include "postgres.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/dsm.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "storage/spin.h"
#include "utils/memutils.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "access/xact.h"
#include "parser/mgr_node.h"
#include "commands/defrem.h"
#include "mgr/mgr_cmds.h"

Datum mgr_doctor_start(PG_FUNCTION_ARGS)
{
    int ret = SPI_connect();
    if (ret != SPI_OK_CONNECT)
    {
        ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
                        (errmsg("SPI_connect failed, connect return:%d", ret))));
        PG_RETURN_BOOL(false);
    }
    PG_TRY();
    {
        SPI_execute("select adb_doctor.adb_doctor_start()", 0, 0);
    }
    PG_CATCH();
    {
        ereport(ERROR, (errmsg("Failed to start doctor.\n Check if schema adb_doctor is set or params are out of bounds")));
        PG_RETURN_BOOL(false);
    }
    PG_END_TRY();
    SPI_finish();
    PG_RETURN_BOOL(true);
}

Datum mgr_doctor_stop(PG_FUNCTION_ARGS)
{
    int ret = SPI_connect();
    if (ret != SPI_OK_CONNECT)
    {
        ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
                        (errmsg("SPI_connect failed, connect return:%d", ret))));
        PG_RETURN_BOOL(false);
    }
    PG_TRY();
    {
        SPI_execute("select adb_doctor.adb_doctor_stop()", 0, 0);
    }
    PG_CATCH();
    {
        ereport(ERROR, (errmsg("Failed to stop doctor.\n Check if schema adb_doctor is set or params are out of bounds")));
        PG_RETURN_BOOL(false);
    }
    PG_END_TRY();
    SPI_finish();
    PG_RETURN_BOOL(true);
}

void mgr_doctor_set_param(MGRDoctorSet *node, ParamListInfo params, DestReceiver *dest)
{

    DirectFunctionCall1(mgr_doctor_param, PointerGetDatum(node->options));
    return;
}

Datum mgr_doctor_param(PG_FUNCTION_ARGS)
{
    int ret = SPI_connect();
    if (ret != SPI_OK_CONNECT)
    {
        ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
                        (errmsg("SPI_connect failed, connect return:%d", ret))));
        PG_RETURN_BOOL(false);
    }
    List *options = (List *)PG_GETARG_POINTER(0);
    ListCell *lc;
    DefElem *def;
    char *datalevelStr = NULL;
    char *probeintervalStr = NULL;
    foreach (lc, options)
    {
        def = lfirst(lc);
        Assert(def && IsA(def, DefElem));

        if (strcmp(def->defname, "datalevel") == 0)
        {
            datalevelStr = defGetString(def);
            if (pg_atoi(datalevelStr, 4, 0) < 0)
                ereport(ERROR, (errmsg("Invalid negative parameter value")));
        }
        else if (strcmp(def->defname, "probeinterval") == 0)
        {
            probeintervalStr = defGetString(def);
            if (pg_atoi(probeintervalStr, 4, 0) < 0)
                ereport(ERROR, (errmsg("Invalid negative parameter value")));
        }
        else
        {
            ereport(ERROR, (errmsg("Failed to set doctor parameters, no param %s", def->defname)));
        }
    }
    if (datalevelStr == NULL && probeintervalStr == NULL)
    {
        ereport(ERROR, (errmsg("Failed to set doctor parameters, nothing to set!")));
    }
    if (datalevelStr == NULL)
    {
        datalevelStr = palloc0(20);
        pg_ltoa(-1, datalevelStr);
    }

    if (probeintervalStr == NULL)
    {
        probeintervalStr = palloc0(20);
        pg_ltoa(-1, probeintervalStr);
    }

    PG_TRY();
    {
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf, "select adb_doctor.adb_doctor_param(%s, %s)",
                         datalevelStr, probeintervalStr);
        SPI_execute(buf.data, false, 0);
        pfree(buf.data);
    }
    PG_CATCH();
    {
        ereport(ERROR, (errmsg("Failed to set doctor parameters.\n Check if schema adb_doctor is set or params are out of bounds")));
        PG_RE_THROW();
    }
    PG_END_TRY();

    pfree(datalevelStr);
    pfree(probeintervalStr);
    pfree(options);

    SPI_finish();
    PG_RETURN_BOOL(true);
}