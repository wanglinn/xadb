
#include "postgres.h"
#include "pgstat.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/procarray.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "storage/spin.h"
#include "../../src/interfaces/libpq/libpq-fe.h"
#include "../../src/interfaces/libpq/libpq-int.h"
#include "mgr/mgr_agent.h"
#include "mgr/mgr_msg_type.h"
#include "mgr/mgr_cmds.h"
#include "adb_doctor.h"

#define AGENT_WAIT_EVENT_TIMEOUT_MS 1000
#define AGENT_RECONNECT_DELAY_MS 3000
#define AGENT_PENDING_DELAY_MS 30000
#define AGENT_RESTART_DELAY_MS 30000

typedef struct WaitEventData
{
    void (*fun)(WaitEvent *event);
} WaitEventData;

typedef enum ManagerAgentStatus
{
    MANAGER_AGENT_STATUS_NORMAL = 1,
    MANAGER_AGENT_STATUS_PENDING,
    MANAGER_AGENT_STATUS_CRASHED
} ManagerAgentStatus;

typedef struct ManagerAgentWrapper
{
    WaitEventData wed;
    int eventPosition; /* the id returned by AddWaitEventToSet */
    dlist_node list_node;
    ManagerAgent *agent;
    ManagerAgentStatus status;
    bool connected;
    TimestampTz tryConnectTime;
    TimestampTz activeTime;
    TimestampTz restartTime;
    AdbMgrHostWrapper *hostWrapper;
} ManagerAgentWrapper;

static void handleSigterm(SIGNAL_ARGS);
static void handleSigusr1(SIGNAL_ARGS);

static void hostMonitorMainLoop(AdbDoctorHostData *data);
static void attachHostDataShm(Datum main_arg, AdbDoctorHostData **dataP);

static void initializeWaitEventSet(int numberOfAgents);
static void addAgentWaitEventToSet(ManagerAgentWrapper *agentWrapper);
static void removeAgentWaitEvent(ManagerAgentWrapper *agentWrapper);
static void OnLatchSetEvent(WaitEvent *event);
static void OnPostmasterDeathEvent(WaitEvent *event);
static void OnAgentMsgEvent(WaitEvent *event);

/* each host run agent process, host monitor connect to these agents.  */
static void initializeAgentsConnection(AdbDoctorList *agentList);
static void connectToAgent(ManagerAgentWrapper *agentWrapper);
static void disconnectAgent(ManagerAgentWrapper *agentWrapper);
static void restartAgent(ManagerAgentWrapper *agentWrapper);
static void checkNormalAgentList();
static void checkPendingAgentList();
static void checkCrashedAgentList();
static void invalidEventPosition(ManagerAgentWrapper *agentWrapper);
static void agentStatusNormal(ManagerAgentWrapper *agentWrapper, bool inList);
static void agentStatusPending(ManagerAgentWrapper *agentWrapper, bool inList);
static void agentStatusCrashed(ManagerAgentWrapper *agentWrapper, bool inList);
static bool deleteAgentFromCurrentList(ManagerAgentWrapper *agentWrapper);

static void pfreeManagerAgentWrapper(ManagerAgentWrapper *agentWrapper);
static void pfreeAgentsInList(dlist_head *list);

static volatile sig_atomic_t gotSigterm = false;
static volatile sig_atomic_t gotSigusr1 = false;

static dlist_head normalAgentList = DLIST_STATIC_INIT(normalAgentList);
static dlist_head pendingAgentList = DLIST_STATIC_INIT(pendingAgentList);
static dlist_head crashedAgentList = DLIST_STATIC_INIT(crashedAgentList);

static AdbDoctorConfShm *confShm;
static AdbDoctorConf *confInLocal;

static WaitEventSet *agentWaitEventSet;
static WaitEvent *occurredEvents;
static int nOccurredEvents;

static const WaitEventData LatchSetEventData = {OnLatchSetEvent};
static const WaitEventData PostmasterDeathEventData = {OnPostmasterDeathEvent};

void adbDoctorHostMonitorMain(Datum main_arg)
{
    pg_usleep(20 * 1000000);
    AdbDoctorHostData *data;

    pqsignal(SIGTERM, handleSigterm);
    pqsignal(SIGUSR1, handleSigusr1);
    BackgroundWorkerUnblockSignals();

    attachHostDataShm(main_arg, &data);

    notifyAdbDoctorRegistrant();

    confShm = attachAdbDoctorConfShm(data->header.commonShmHandle,
                                     MyBgworkerEntry->bgw_name);

    confInLocal = copyAdbDoctorConfFromShm(confShm);

    logAdbDoctorHostData(data, psprintf("%s started", MyBgworkerEntry->bgw_name), LOG);

    hostMonitorMainLoop(data);

    pfreeAdbDoctorConfShm(confShm);

    pfreeAgentsInList(&normalAgentList);
    pfreeAgentsInList(&pendingAgentList);
    pfreeAgentsInList(&crashedAgentList);

    proc_exit(1);
}

static void hostMonitorMainLoop(AdbDoctorHostData *data)
{

    WaitEvent *event;
    WaitEventData *volatile wed = NULL;

    int rc, i;
    //    long timeout;

    initializeWaitEventSet(data->list->num);

    initializeAgentsConnection(data->list);

    while (!gotSigterm)
    {
        CHECK_FOR_INTERRUPTS();

        // timeout = confInLocal->probeinterval * 1000L;

        rc = WaitEventSetWait(agentWaitEventSet,
                              AGENT_WAIT_EVENT_TIMEOUT_MS,
                              occurredEvents,
                              nOccurredEvents,
                              PG_WAIT_CLIENT);
        if (rc > 0)
        {
            for (i = 0; i < rc; ++i)
            {
                event = &occurredEvents[i];
                wed = event->user_data;
                (*wed->fun)(event);
            }
        }

        checkNormalAgentList();
        checkPendingAgentList();
        checkCrashedAgentList();

        pg_usleep(20 * 1000000);
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

static void attachHostDataShm(Datum main_arg, AdbDoctorHostData **dataP)
{
    dsm_segment *seg;
    AdbDoctorHostData *dataInShm;
    AdbDoctorHostData *data;
    AdbDoctorLink *hostLink;
    AdbMgrHostWrapper *hostWrapper;
    Adb_Doctor_Bgworker_Type type;
    uint64 tocKey = 0;
    int i;

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
    Assert(type == ADB_DOCTOR_BGWORKER_TYPE_HOST_MONITOR);

    data = palloc0(sizeof(AdbDoctorHostData));
    /* this shm will be detached, copy out all the data */
    memcpy(data, dataInShm, sizeof(AdbDoctorHostData));

    data->list = newAdbDoctorList();
    memcpy(data->list, shm_toc_lookup(toc, tocKey++, false), sizeof(AdbDoctorList));
    dlist_init(&data->list->head);

    for (i = 0; i < data->list->num; i++)
    {
        hostLink = newAdbDoctorLink(NULL, NULL);
        memcpy(hostLink, shm_toc_lookup(toc, tocKey++, false), sizeof(AdbDoctorLink));
        dlist_push_tail(&data->list->head, &hostLink->wi_links);

        hostWrapper = palloc0(sizeof(AdbMgrHostWrapper));
        memcpy(hostWrapper, shm_toc_lookup(toc, tocKey++, false), sizeof(AdbMgrHostWrapper));
        hostLink->data = hostWrapper;
        hostLink->pfreeData = (void (*)(void *))pfreeAdbMgrHostWrapper;

        hostWrapper->hostaddr = pstrdup(shm_toc_lookup(toc, tocKey++, false));
        hostWrapper->hostadbhome = pstrdup(shm_toc_lookup(toc, tocKey++, false));
    }

    /* if true, launcher know this worker is ready, and then detach this shm */
    dataInShm->header.ready = true;
    SpinLockRelease(&dataInShm->header.mutex);

    *dataP = data;

    dsm_detach(seg);
}

static void initializeWaitEventSet(int numberOfAgents)
{
    nOccurredEvents = numberOfAgents + 2;
    agentWaitEventSet = CreateWaitEventSet(TopMemoryContext, nOccurredEvents);
    occurredEvents = palloc0(sizeof(WaitEvent) * nOccurredEvents);
    /* add latch */
    AddWaitEventToSet(agentWaitEventSet,
                      WL_LATCH_SET,
                      PGINVALID_SOCKET,
                      &MyProc->procLatch,
                      (void *)&LatchSetEventData);
    /* add postmaster death */
    AddWaitEventToSet(agentWaitEventSet,
                      WL_POSTMASTER_DEATH,
                      PGINVALID_SOCKET,
                      NULL,
                      (void *)&PostmasterDeathEventData);
}

static void addAgentWaitEventToSet(ManagerAgentWrapper *agentWrapper)
{
    int pos;
    if (agentWrapper->eventPosition >= 0)
        return;
    agentWrapper->wed.fun = OnAgentMsgEvent;
    pos = AddWaitEventToSet(agentWaitEventSet,
                            WL_SOCKET_READABLE,
                            ma_getsock(agentWrapper->agent),
                            NULL,
                            agentWrapper);
    agentWrapper->eventPosition = pos;
}

static void removeAgentWaitEvent(ManagerAgentWrapper *agentWrapper)
{
    if (agentWrapper->eventPosition >= 0)
    {
        RemoveWaitEvent(agentWaitEventSet, agentWrapper->eventPosition);
    }
    invalidEventPosition(agentWrapper);
}

static void OnLatchSetEvent(WaitEvent *event)
{
    bool confChanged;
    ResetLatch(&MyProc->procLatch);
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
}

static void OnPostmasterDeathEvent(WaitEvent *event)
{
    exit(1);
}

static void OnAgentMsgEvent(WaitEvent *event)
{
    ManagerAgentWrapper *agentWrapper = (ManagerAgentWrapper *)event->user_data;
    StringInfoData recvbuf;
    char msg_type;
    initStringInfo(&recvbuf);
    for (;;)
    {
        ereport(LOG, (errmsg("errno: %d", errno)));
        resetStringInfo(&recvbuf);
        msg_type = ma_get_message(agentWrapper->agent, &recvbuf);
        if (msg_type == AGT_MSG_IDLE)
        {
            /* message end */
            agentStatusNormal(agentWrapper, true);
            break;
        }
        else if (msg_type == '\0')
        {
            /* has an error */
            agentStatusCrashed(agentWrapper, true);
            break;
        }
        else if (msg_type == AGT_MSG_ERROR)
        {
            /* error message */
            ereport(LOG, (errmsg("receive msg: %s", ma_get_err_info(&recvbuf, AGT_MSG_RESULT))));
            agentStatusCrashed(agentWrapper, true);
            break;
        }
        else
        {
            // true;
            ereport(LOG, (errmsg("receive msg: %s", recvbuf.data)));
            agentStatusCrashed(agentWrapper, true);
            break;
        }
    }
    pfree(recvbuf.data);
}

/*
 * each host run agent process, host monitor connect to these agent,
 * link the data of type ManagerAgentWrapper into AdbDoctorList.
 */
static void initializeAgentsConnection(AdbDoctorList *agentList)
{
    TimestampTz currentTime;
    ManagerAgentWrapper *agentWrapper;
    dlist_iter iter;
    AdbDoctorLink *hostlink;

    dlist_foreach(iter, &agentList->head)
    {
        hostlink = dlist_container(AdbDoctorLink, wi_links, iter.cur);

        agentWrapper = palloc0(sizeof(ManagerAgentWrapper));
        invalidEventPosition(agentWrapper);
        agentWrapper->agent = NULL;
        /* set to invalid value */
        agentWrapper->status = -1;
        agentWrapper->connected = false;
        currentTime = GetCurrentTimestamp();
        agentWrapper->tryConnectTime = currentTime;
        agentWrapper->activeTime = currentTime;
        agentWrapper->restartTime = currentTime;
        agentWrapper->hostWrapper = hostlink->data;

        connectToAgent(agentWrapper);
        if (agentWrapper->connected)
        {
            agentStatusPending(agentWrapper, false);
        }
        else
        {
            agentStatusCrashed(agentWrapper, false);
        }
    }
    pfreeAdbDoctorList(agentList, false);
}

static void invalidEventPosition(ManagerAgentWrapper *agentWrapper)
{
    agentWrapper->eventPosition = -1;
}

static void connectToAgent(ManagerAgentWrapper *agentWrapper)
{
    ManagerAgent *agent;
    agentWrapper->tryConnectTime = GetCurrentTimestamp();
    PG_TRY();
    {
        agent = ma_connect(agentWrapper->hostWrapper->hostaddr, agentWrapper->hostWrapper->fdmh.hostagentport);
        agentWrapper->agent = agent;
        agentWrapper->connected = ma_isconnected(agent);
    }
    PG_CATCH();
    {
        agentWrapper->agent = NULL;
        agentWrapper->connected = false;
        EmitErrorReport();
    }
    PG_END_TRY();
}

static void disconnectAgent(ManagerAgentWrapper *agentWrapper)
{
    if (agentWrapper == NULL)
        return;
    if (agentWrapper->agent == NULL)
        return;
    PG_TRY();
    {
        ma_close(agentWrapper->agent);
        agentWrapper->agent = NULL;
    }
    PG_CATCH();
    {
        EmitErrorReport();
    }
    PG_END_TRY();
}

static void restartAgent(ManagerAgentWrapper *agentWrapper)
{
    agentWrapper->restartTime = GetCurrentTimestamp();
    PG_TRY();
    {
        mgr_start_agent_execute(&agentWrapper->hostWrapper->fdmh,
                                agentWrapper->hostWrapper->hostaddr,
                                agentWrapper->hostWrapper->hostadbhome,
                                "123", /* TODO, how to get passwo */
                                NULL);
    }
    PG_CATCH();
    {
        EmitErrorReport();
    }
    PG_END_TRY();
}

static void checkNormalAgentList()
{
    if (dlist_is_empty(&normalAgentList))
        return;

    bool sent;
    StringInfoData buf;
    ManagerAgentWrapper *agentWrapper;
    dlist_mutable_iter miter;

    dlist_foreach_modify(miter, &normalAgentList)
    {
        agentWrapper = dlist_container(ManagerAgentWrapper, list_node, miter.cur);
        /* send heartbeat to agent. */
        initStringInfo(&buf);
        ma_beginmessage(&buf, AGT_MSG_IDLE);
        ma_endmessage(&buf, agentWrapper->agent);
        sent = ma_flush(agentWrapper->agent, false);
        if (!sent)
        {
            dlist_delete(miter.cur);
            agentStatusCrashed(agentWrapper, false);
        }
    }
}

static void checkPendingAgentList()
{
    if (dlist_is_empty(&pendingAgentList))
        return;

    ManagerAgentWrapper *agentWrapper;
    dlist_mutable_iter miter;
    TimestampTz current_time = GetCurrentTimestamp();

    dlist_foreach_modify(miter, &pendingAgentList)
    {
        agentWrapper = dlist_container(ManagerAgentWrapper, list_node, miter.cur);
        if (TimestampDifferenceExceeds(agentWrapper->activeTime,
                                       current_time,
                                       AGENT_PENDING_DELAY_MS))
        {
            /* do not a waste of time, drop this agent to crashed list.  */
            dlist_delete(miter.cur);
            agentStatusCrashed(agentWrapper, false);
            break;
        }
        if (TimestampDifferenceExceeds(agentWrapper->tryConnectTime,
                                       current_time,
                                       AGENT_RECONNECT_DELAY_MS))
        {
            /* reconnect */
            disconnectAgent(agentWrapper);
            connectToAgent(agentWrapper);
            if (agentWrapper->connected)
            {
                agentStatusPending(agentWrapper, false);
            }
            else
            {
                dlist_delete(miter.cur);
                agentStatusCrashed(agentWrapper, false);
            }
            break;
        }
        /* wait for socket event next round.  */
    }
}

static void checkCrashedAgentList()
{
    if (dlist_is_empty(&crashedAgentList))
        return;

    ManagerAgentWrapper *agentWrapper;
    dlist_mutable_iter miter;
    TimestampTz current_time = GetCurrentTimestamp();

    dlist_foreach_modify(miter, &crashedAgentList)
    {
        agentWrapper = dlist_container(ManagerAgentWrapper, list_node, miter.cur);
        if (TimestampDifferenceExceeds(agentWrapper->restartTime,
                                       current_time,
                                       AGENT_RESTART_DELAY_MS))
        {
            restartAgent(agentWrapper);
        }
    }
}

static void agentStatusNormal(ManagerAgentWrapper *agentWrapper, bool inList)
{
    if (agentWrapper->status == MANAGER_AGENT_STATUS_NORMAL)
        return;
    if (inList)
    {
        deleteAgentFromCurrentList(agentWrapper);
    }
    agentWrapper->activeTime = GetCurrentTimestamp();
    addAgentWaitEventToSet(agentWrapper);
    agentWrapper->status = MANAGER_AGENT_STATUS_NORMAL;
    dlist_push_tail(&normalAgentList, &agentWrapper->list_node);
}

static void agentStatusPending(ManagerAgentWrapper *agentWrapper, bool inList)
{
    if (agentWrapper->status == MANAGER_AGENT_STATUS_PENDING)
        return;
    if (inList)
    {
        deleteAgentFromCurrentList(agentWrapper);
    }
    addAgentWaitEventToSet(agentWrapper);
    agentWrapper->status = MANAGER_AGENT_STATUS_PENDING;
    dlist_push_tail(&pendingAgentList, &agentWrapper->list_node);
}

static void agentStatusCrashed(ManagerAgentWrapper *agentWrapper, bool inList)
{
    if (agentWrapper->status == MANAGER_AGENT_STATUS_CRASHED)
        return;
    if (inList)
    {
        deleteAgentFromCurrentList(agentWrapper);
    }
    removeAgentWaitEvent(agentWrapper);
    disconnectAgent(agentWrapper);
    agentWrapper->status = MANAGER_AGENT_STATUS_CRASHED;
    dlist_push_tail(&crashedAgentList, &agentWrapper->list_node);
}

static bool deleteAgentFromCurrentList(ManagerAgentWrapper *agentWrapper)
{
    dlist_head *list;
    ManagerAgentStatus status;
    dlist_mutable_iter miter;
    ManagerAgentWrapper *data;

    status = agentWrapper->status;
    if (status == MANAGER_AGENT_STATUS_NORMAL)
        list = &normalAgentList;
    else if (status == MANAGER_AGENT_STATUS_PENDING)
        list = &pendingAgentList;
    else if (status == MANAGER_AGENT_STATUS_CRASHED)
        list = &crashedAgentList;
    else
        ereport(ERROR,
                (errmsg("unexpected ManagerAgentWrapper.status:%d", status)));

    if (dlist_is_empty(list))
        return true;
    dlist_foreach_modify(miter, list)
    {
        data = dlist_container(ManagerAgentWrapper, list_node, miter.cur);
        if (agentWrapper == data)
        {
            dlist_delete(miter.cur);
            return true;
        }
    }
    return false;
}

static void pfreeAgentsInList(dlist_head *list)
{
    dlist_mutable_iter miter;
    ManagerAgentWrapper *data;

    if (dlist_is_empty(list))
        return;
    dlist_foreach_modify(miter, list)
    {
        data = dlist_container(ManagerAgentWrapper, list_node, miter.cur);
        pfreeManagerAgentWrapper(data);
    }
}

static void pfreeManagerAgentWrapper(ManagerAgentWrapper *agentWrapper)
{
    if (agentWrapper == NULL)
        return;
    disconnectAgent(agentWrapper);
    pfreeAdbMgrHostWrapper(agentWrapper->hostWrapper);
    pfree(agentWrapper);
}