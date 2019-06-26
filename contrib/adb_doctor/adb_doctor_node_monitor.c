
#include "postgres.h"
#include "pgstat.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/procarray.h"
#include "storage/spin.h"
#include "utils/resowner.h"
#include "utils/builtins.h"
#include "../../src/interfaces/libpq/libpq-fe.h"
#include "../../src/interfaces/libpq/libpq-int.h"
#include "adb_doctor.h"

static void handleSigterm(SIGNAL_ARGS);
static void handleSigusr1(SIGNAL_ARGS);

static void nodeMonitorMainLoop(AdbDoctorNodeData *data);

static void attachNodeDataShm(Datum main_arg, AdbDoctorNodeData **dataP);

static PGconn *connectDB(AdbDoctorNodeData *data, int maxAttempt);
static bool probeDB(PGconn *conn, int maxAttempt);
// static void cure(AdbDoctorBgworkerData *data);

static AdbDoctorConfShm *confShm;
static AdbDoctorConf *confInLocal;

static volatile sig_atomic_t gotSigterm = false;
static volatile sig_atomic_t gotSigusr1 = false;

void adbDoctorNodeMonitorMain(Datum main_arg)
{
    pg_usleep(20 * 1000000);

    AdbDoctorNodeData *data;

    pqsignal(SIGTERM, handleSigterm);
    pqsignal(SIGUSR1, handleSigusr1);

    BackgroundWorkerUnblockSignals();

    attachNodeDataShm(main_arg, &data);

    notifyAdbDoctorRegistrant();

    confShm = attachAdbDoctorConfShm(data->header.commonShmHandle, MyBgworkerEntry->bgw_name);

    confInLocal = copyAdbDoctorConfFromShm(confShm);

    logAdbDoctorNodeData(data, psprintf("%s started", MyBgworkerEntry->bgw_name), LOG);

    nodeMonitorMainLoop(data);

    pfreeAdbDoctorConfShm(confShm);

    proc_exit(1);
}

static void nodeMonitorMainLoop(AdbDoctorNodeData *data)
{
    int rc;
    bool confChanged;
    long timeout;
    PGconn *conn;

    conn = connectDB(data, 3);
    if (conn == NULL)
        proc_exit(1);

    while (!gotSigterm)
    {
        CHECK_FOR_INTERRUPTS();

        timeout = confInLocal->probeinterval * 1000L;

        rc = WaitLatchOrSocket(MyLatch,
                               WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH | WL_SOCKET_MASK,
                               conn->sock,
                               timeout,
                               PG_WAIT_EXTENSION);
        /* Reset the latch, bail out if postmaster died, otherwise loop. */
        ResetLatch(&MyProc->procLatch);
        if (rc & WL_POSTMASTER_DEATH)
            proc_exit(1);

        if (gotSigusr1)
        {
            gotSigusr1 = false;
            confChanged = compareShmAndUpdateAdbDoctorConf(confInLocal, confShm);
            if (confChanged)
            {
                ereport(LOG,
                        (errmsg("AdbDoctorConf reload, datalevel: %d, probeinterval: %d",
                                confInLocal->datalevel, confInLocal->probeinterval)));
            }
        }

        probeDB(conn, 3);
    }
}

/*
 * When we receive a SIGTERM, we set InterruptPending and ProcDiePending just
 * like a normal backend.  The next CHECK_FOR_INTERRUPTS() will do the right
 * thing.
 */
static void handleSigterm(SIGNAL_ARGS)
{
    int save_errno = errno;

    gotSigterm = true;

    SetLatch(MyLatch);

    if (!proc_exit_inprogress)
    {
        InterruptPending = true;
        ProcDiePending = true;
    }
    errno = save_errno;
}
/*
 * When we receive a SIGUSR1, we set gotSigusr1 = true
 */
static void handleSigusr1(SIGNAL_ARGS)
{
    int save_errno = errno;

    gotSigusr1 = true;

    procsignal_sigusr1_handler(postgres_signal_arg);

    errno = save_errno;
}

static void attachNodeDataShm(Datum main_arg, AdbDoctorNodeData **dataP)
{
    dsm_segment *seg;
    AdbDoctorNodeData *dataInShm;
    AdbDoctorNodeData *data;
    Adb_Doctor_Bgworker_Type type;
    uint64 tocKey = 0;

    CurrentResourceOwner = ResourceOwnerCreate(NULL, MyBgworkerEntry->bgw_name);
    seg = dsm_attach(DatumGetUInt32(main_arg));
    if (seg == NULL)
        ereport(ERROR,
                (errmsg("unable to map individual dynamic shared memory segment")));

    shm_toc *toc = shm_toc_attach(ADB_DOCTOR_SHM_DATA_MAGIC, dsm_segment_address(seg));
    if (toc == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("bad magic number in dynamic shared memory segment")));

    dataInShm = shm_toc_lookup(toc, tocKey++, false);

    SpinLockAcquire(&dataInShm->header.mutex);

    type = dataInShm->header.type;
    Assert(type == ADB_DOCTOR_BGWORKER_TYPE_NODE_MONITOR);

    data = palloc0(sizeof(AdbDoctorNodeData));
    /* this shm will be detached, copy out all the data */
    memcpy(data, dataInShm, sizeof(AdbDoctorNodeData));

    data->wrapper = palloc0(sizeof(AdbMgrNodeWrapper));
    memcpy(data->wrapper, shm_toc_lookup(toc, tocKey++, false), sizeof(AdbMgrNodeWrapper));

    data->wrapper->nodepath = pstrdup(shm_toc_lookup(toc, tocKey++, false));
    data->wrapper->hostaddr = pstrdup(shm_toc_lookup(toc, tocKey++, false));

    /* if true, launcher know this worker is ready, and then detach this shm */
    dataInShm->header.ready = true;
    SpinLockRelease(&dataInShm->header.mutex);

    *dataP = data;

    dsm_detach(seg);
}

static PGconn *connectDB(AdbDoctorNodeData *data, int maxAttempt)
{
    StringInfoData conninfo;
    PGconn *conn;
    int attempted;

    initStringInfo(&conninfo);
    appendStringInfo(&conninfo,
                     "postgresql://%s@%s:%d/%s?connect_timeout=%d",
                     NameStr(data->wrapper->hostuser),
                     data->wrapper->hostaddr,
                     data->wrapper->fdmn.nodeport,
                     DEFAULT_DB,
                     5);
    for (attempted = 1; attempted <= maxAttempt; attempted++)
    {
        conn = PQconnectdb(conninfo.data);
        /* Check to see that the backend connection was successfully made */
        if (PQstatus(conn) != CONNECTION_OK)
        {
            PQfinish(conn);
            conn = NULL;
            ereport(LOG,
                    (errmsg("Connection to:%s,failed:%s,attempted:%d",
                            conninfo.data,
                            PQerrorMessage(conn),
                            attempted)));
        }
        else
        {
            ereport(LOG,
                    (errmsg("Connection to:%s,successed",
                            conninfo.data)));
            break;
        }
    }
    pfree(conninfo.data);

    return conn;
}

static bool probeDB(PGconn *conn, int maxAttempt)
{
    bool queryOk;
    int attempted;
    PGresult *res;

    for (attempted = 1; attempted <= maxAttempt; attempted++)
    {
        res = PQexec(conn, "SELECT 1");
        if (PQresultStatus(res) != PGRES_TUPLES_OK)
        {
            ereport(LOG,
                    (errmsg("Probe DB failed:%s,attempted:%d",
                            PQerrorMessage(conn),
                            attempted)));
            PQclear(res);
            res = NULL;
            queryOk = false;
        }
        else
        {
            PQclear(res);
            res = NULL;
            queryOk = true;
            break;
        }
    }
    return queryOk;
}

// static void cure(AdbDoctorBgworkerData *data)
// {
//     if (data->type == ADB_DOCTOR_BGWORKER_TYPE_HOST_MONITOR)
//     {
//     }
//     else if (data->type == ADB_DOCTOR_BGWORKER_TYPE_NODE_MONITOR)
//     {
//         // TODO:
//         // 1. save the data
//         // 2. failover
//         // 3. reboot
//     }
//     else if (data->type == ADB_DOCTOR_BGWORKER_TYPE_SWITCHER)
//     {
//         // TODO failover
//     }
//     else
//     {
//         ereport(ERROR, (errmsg("function heal unimplemented")));
//     }
//     // ereport(ERROR,
//     //         (errmsg("wait for cure facility.")));
// }