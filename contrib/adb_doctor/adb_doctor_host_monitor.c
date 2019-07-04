/*--------------------------------------------------------------------------
 * The Adb Doctor Launcher process launches this process， this process run
 * as a background worker. So don't worry about the process exiting abnormally
 * because the Launcher process will launch it again.
 * 
 * Each host runs an agent(mgr agent process), so we can monitor the running 
 * status of that agent. If the agent process dies, we restart it. 
 * This achieves the goal: to ensure that the agent process is always alive.
 *
 * Copyright (c) 2018-2019, Asiainfo Database Innovation Lab
 *
 * -------------------------------------------------------------------------
 */
#include <math.h>
#include "postgres.h"
#include "pgstat.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/procarray.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "storage/spin.h"
#include "executor/spi.h"
#include "../../src/interfaces/libpq/libpq-fe.h"
#include "../../src/interfaces/libpq/libpq-int.h"
#include "mgr/mgr_agent.h"
#include "mgr/mgr_msg_type.h"
#include "mgr/mgr_cmds.h"
#include "adb_doctor.h"

#define INVALID_TIMESTAMP -1
#define RESTART_FACTOR_MAX 5

/* 
 * Benchmark of the time interval, The following elements 
 * based on agentdeadlineMs, but have min and max value limit.
 * For more information, please refer to function newTolerationTime(). 
 */
typedef struct TolerationTime
{
    long agentdeadlineMs;
    long waitEventTimeoutMs;
    long connectTimeoutMs;
    long reconnectDelayMs;
    long heartbeatTimoutMs;
    long heartbeatIntervalMs;
    long restartDelayMs;
} TolerationTime;

typedef enum MonitorAgentError
{
    MONITOR_AGENT_ERROR_INVALID_VALUE = -1, /* illegal value. */
    MONITOR_AGENT_ERROR_CONNECT = 1,
    MONITOR_AGENT_ERROR_MESSAGE,
    MONITOR_AGENT_ERROR_HEARTBEAT
} MonitorAgentError;

typedef enum MonitorAgentStatus
{
    /* Pre-status can be NORMAL, PENDING */
    MONITOR_AGENT_STATUS_NORMAL = 1,
    /* Pre-status can be all status */
    MONITOR_AGENT_STATUS_PENDING,
    /* Pre-status can be PENDING, CRASHED */
    MONITOR_AGENT_STATUS_CRASHED
} MonitorAgentStatus;

typedef struct WaitEventData
{
    void (*fun)(WaitEvent *event);
} WaitEventData;

typedef struct ManagerAgentWrapper
{
    WaitEventData wed;
    int event_pos; /* the id returned by AddWaitEventToSet */
    dlist_node list_node;
    ManagerAgent *agent;
    MonitorAgentStatus previousStatus;
    MonitorAgentStatus currentStatus;
    TimestampTz connectTime;
    TimestampTz activeTime;
    TimestampTz restartTime;
    bool restarting;
    /* Use left shift, right shift operation to increase the delay time of restart. */
    int restartFactor;
    bool restartFactorIncrease;
    AdbMgrHostWrapper *hostWrapper;
    TimestampTz errorTime;
    MonitorAgentError *errors;
    int nerrors;
} ManagerAgentWrapper;

static void handleSigterm(SIGNAL_ARGS);
static void handleSigusr1(SIGNAL_ARGS);

static void hostMonitorMainLoop(AdbDoctorHostData *data);
static void attachHostDataShm(Datum main_arg, AdbDoctorHostData **dataP);
static TolerationTime *newTolerationTime(int agentdeadlineSecs);

static void initializeWaitEventSet(int nAgents);
static void initializeAgentsConnection(AdbDoctorList *agentList);

static void examineAgentsStatus(void);
static void transferToNormal(ManagerAgentWrapper *agentWrapper);
static void handleNormalStatus(ManagerAgentWrapper *agentWrapper, TimestampTz currentTime);
static void transferToPending(ManagerAgentWrapper *agentWrapper);
static void handlePendingStatus(ManagerAgentWrapper *agentWrapper, TimestampTz currentTime);
static void transferToCrashed(ManagerAgentWrapper *agentWrapper);
static void handleCrashedStatus(ManagerAgentWrapper *agentWrapper, TimestampTz currentTime);
static bool reachedErrorCriticalPoint(ManagerAgentWrapper *agentWrapper);
static bool reachedDeadline(ManagerAgentWrapper *agentWrapper, TimestampTz currentTime);
static void recordAgentError(ManagerAgentWrapper *agentWrapper, MonitorAgentError error);
static void clearAgentErrors(ManagerAgentWrapper *agentWrapper);
static bool haveConnectionToAgent(ManagerAgentWrapper *agentWrapper);
static bool startMonitorAgent(ManagerAgentWrapper *agentWrapper);
static void stopMonitorAgent(ManagerAgentWrapper *agentWrapper);
static bool restartMonitorAgent(ManagerAgentWrapper *agentWrapper);
static bool connectTimedOut(ManagerAgentWrapper *agentWrapper, TimestampTz currentTime);
static bool allowReconnectAgent(ManagerAgentWrapper *agentWrapper);
static bool allowRestartAgent(ManagerAgentWrapper *agentWrapper, TimestampTz currentTime);
static bool restartAgent(ManagerAgentWrapper *agentWrapper);
static bool heartbeatAgent(ManagerAgentWrapper *agentWrapper);
static void nextRestartFactor(ManagerAgentWrapper *agentWrapper);

static void invalidateEventPosition(ManagerAgentWrapper *agentWrapper);
static void addAgentWaitEventToSet(ManagerAgentWrapper *agentWrapper);
static void removeAgentWaitEvent(ManagerAgentWrapper *agentWrapper);
static void OnLatchSetEvent(WaitEvent *event);
static void OnPostmasterDeathEvent(WaitEvent *event);
static void OnAgentMsgEvent(WaitEvent *event);

static void pfreeManagerAgentWrapper(ManagerAgentWrapper *agentWrapper);
static void pfreeAgentsInList(dlist_head *list);

static volatile sig_atomic_t gotSigterm = false;
static volatile sig_atomic_t gotSigusr1 = false;

static dlist_head totalAgentList = DLIST_STATIC_INIT(totalAgentList);

static AdbDoctorConfShm *confShm;
static int agentdeadline;

static WaitEventSet *agentWaitEventSet;
static WaitEvent *occurredEvents;
static int nOccurredEvents;
static TolerationTime *tolerationTime;

static const WaitEventData LatchSetEventData = {OnLatchSetEvent};
static const WaitEventData PostmasterDeathEventData = {OnPostmasterDeathEvent};

/*
 * This is a backgroundworker process, it monitor all agents in all hosts,
 * if it exit abnormally, the adb doctor launcher process will detected this
 * event, and then that launcher process will launch this process again. 
 */
void adbDoctorHostMonitorMain(Datum main_arg)
{
    AdbDoctorHostData *data;
    //pg_usleep(20 * 1000000);
    pqsignal(SIGTERM, handleSigterm);
    pqsignal(SIGUSR1, handleSigusr1);
    BackgroundWorkerUnblockSignals();

    BackgroundWorkerInitializeConnection(ADBMGR_DBNAME, NULL, 0);

    attachHostDataShm(main_arg, &data);

    notifyAdbDoctorRegistrant();

    confShm = attachAdbDoctorConfShm(data->header.commonShmHandle,
                                     MyBgworkerEntry->bgw_name);

    LWLockAcquire(&confShm->confInShm->lock, LW_SHARED);
    agentdeadline = confShm->confInShm->agentdeadline;
    LWLockRelease(&confShm->confInShm->lock);

    tolerationTime = newTolerationTime(agentdeadline);

    logAdbDoctorHostData(data, psprintf("%s started", MyBgworkerEntry->bgw_name), LOG);

    hostMonitorMainLoop(data);

    pfreeAdbDoctorConfShm(confShm);

    pfreeAgentsInList(&totalAgentList);

    proc_exit(1);
}

/*
 * When received a SIGTERM, set InterruptPending and ProcDiePending just
 * like a normal backend. then CHECK_FOR_INTERRUPTS() will do the right thing.
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
 * When received a SIGUSR1, set gotSigusr1 = true
 */
static void handleSigusr1(SIGNAL_ARGS)
{
    int save_errno = errno;

    gotSigusr1 = true;

    procsignal_sigusr1_handler(postgres_signal_arg);

    errno = save_errno;
}

/* 
 * Connect to all of the agents, keep alive by send heartbeat message,
 * wait event of these socket, determine the status of agent.
 */
static void hostMonitorMainLoop(AdbDoctorHostData *data)
{
    WaitEvent *event;
    WaitEventData *volatile wed = NULL;

    int rc, i;

    initializeWaitEventSet(data->list->num);

    initializeAgentsConnection(data->list);

    while (!gotSigterm)
    {
        CHECK_FOR_INTERRUPTS();

        rc = WaitEventSetWait(agentWaitEventSet,
                              tolerationTime->waitEventTimeoutMs,
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
        /* Examine the status of all agents, determine whether it crashed.  */
        examineAgentsStatus();
    }
}

/* 
 * Launcher process set up a necessary individual data in shared memory(shm). 
 * attach this shm to get these data, if got, detach this shm.
 */
static void attachHostDataShm(Datum main_arg, AdbDoctorHostData **dataP)
{
    dsm_segment *seg;
    shm_toc *toc;
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
                (errmsg("unable to map individual dynamic shared memory segment.")));

    toc = shm_toc_attach(ADB_DOCTOR_SHM_DATA_MAGIC, dsm_segment_address(seg));
    if (toc == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("bad magic number in dynamic shared memory segment.")));

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

    /* If true, launcher process know this worker is ready. */
    dataInShm->header.ready = true;
    SpinLockRelease(&dataInShm->header.mutex);

    *dataP = data;

    dsm_detach(seg);
}

/* 
 * In order to simplify the configuration and also to avoid messing up these
 * various variables. and also in order to hide the details and provide convenience 
 * for the user, we only expose only one user settable parameter agentdeadline 
 * with unit second. The values of all other variables are calculated based on 
 * agentdeadline with unit transformed to millisecond, and each variable has its 
 * maximum and minimum value range. The value of agentdeadline should be set carefully.
 * If the setting is too small, it may lead to the wrong AGENT status determination.
 * If the setting is too large, it may cause a delayed AGENT status determination.
 * However, we avoid setting the value of the inappropriate agentdeadline by optimizing
 * the minimum and maximum these various variables.
 */
static TolerationTime *newTolerationTime(int agentdeadlineSecs)
{
    TolerationTime *tt;
    long deadlineMs;
    long minTimeMs;

    tt = palloc0(sizeof(TolerationTime));

    deadlineMs = safeAdbDoctorConf_agentdeadline(agentdeadlineSecs) * 1000L;
    tt->agentdeadlineMs = deadlineMs;
    tt->waitEventTimeoutMs = LIMIT_VALUE_RANGE(1000, 10000, floor(deadlineMs / 30));
    /* treat waitEventTimeoutMs as the minimum time unit, any time variable less then it is meaningless. */
    minTimeMs = tt->waitEventTimeoutMs;
    tt->connectTimeoutMs = LIMIT_VALUE_RANGE(Max(minTimeMs, 1000), 10000, floor(deadlineMs / 10));
    tt->reconnectDelayMs = LIMIT_VALUE_RANGE(minTimeMs, 10000, floor(deadlineMs / 10));
    tt->heartbeatTimoutMs = LIMIT_VALUE_RANGE(Max(minTimeMs, 3000), 180000, deadlineMs);
    tt->heartbeatIntervalMs = LIMIT_VALUE_RANGE(Max(minTimeMs, 1000), 60000, floor(tt->heartbeatTimoutMs / 3));
    tt->restartDelayMs = LIMIT_VALUE_RANGE(Max(minTimeMs, 10000), 120000, floor(deadlineMs / 2));
    return tt;
}

/* 
 * In order to quickly get events occurrex at agents you must first initialize
 * WaitEventSet, and also add events WL_LATCH_SET and WL_POSTMASTER_DEATH.
 */
static void initializeWaitEventSet(int nAgents)
{
    nOccurredEvents = nAgents + 2;
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

/*
 * Each host run an agent process, connect to all of these agents,
 * link them to a list to make it easier to maintain their status later.
 */
static void initializeAgentsConnection(AdbDoctorList *agentList)
{
    ManagerAgentWrapper *agentWrapper;
    dlist_iter iter;
    AdbDoctorLink *hostlink;

    dlist_foreach(iter, &agentList->head)
    {
        hostlink = dlist_container(AdbDoctorLink, wi_links, iter.cur);
        /* Initialize agent to normal status. */
        agentWrapper = palloc0(sizeof(ManagerAgentWrapper));
        agentWrapper->wed.fun = OnAgentMsgEvent;
        invalidateEventPosition(agentWrapper);
        agentWrapper->agent = NULL;
        agentWrapper->previousStatus = MONITOR_AGENT_STATUS_NORMAL;
        agentWrapper->currentStatus = MONITOR_AGENT_STATUS_PENDING;
        agentWrapper->connectTime = INVALID_TIMESTAMP;
        agentWrapper->activeTime = GetCurrentTimestamp();
        agentWrapper->restartTime = INVALID_TIMESTAMP;
        agentWrapper->restartFactor = 0;
        agentWrapper->restartFactorIncrease = true;
        agentWrapper->hostWrapper = hostlink->data;
        clearAgentErrors(agentWrapper);
        startMonitorAgent(agentWrapper);
        dlist_push_tail(&totalAgentList, &agentWrapper->list_node);
    }
    /* these hosts are already linked to the new list.
       so pfree the old list, but do not pfree data. */
    pfreeAdbDoctorList(agentList, false);
}

/**
 * All agent information are stored in a list, Iterate through this list and perform
 * different processing according to different statuses. Our goal is to find these 
 * crashed agnets as soon as possible.
 */
static void examineAgentsStatus(void)
{
    ManagerAgentWrapper *agentWrapper;
    MonitorAgentStatus status;
    dlist_iter iter;
    TimestampTz currentTime;

    if (dlist_is_empty(&totalAgentList))
        return;
    /* 
     * Because the delay of the loop processing, The lower the element in the list,
     * the larger the timestamp delay, it may mess up the status determination.
     * In order to prevent the situation mentioned above, the same current timestamp 
     * value is necessary.
     */
    currentTime = GetCurrentTimestamp();
    dlist_foreach(iter, &totalAgentList)
    {
        agentWrapper = dlist_container(ManagerAgentWrapper, list_node, iter.cur);
        status = agentWrapper->currentStatus;
        if (status == MONITOR_AGENT_STATUS_NORMAL)
        {
            handleNormalStatus(agentWrapper, currentTime);
        }
        else if (status == MONITOR_AGENT_STATUS_PENDING)
        {
            handlePendingStatus(agentWrapper, currentTime);
        }
        else if (status == MONITOR_AGENT_STATUS_CRASHED)
        {
            handleCrashedStatus(agentWrapper, currentTime);
        }
        else
        {
            ereport(ERROR,
                    (errmsg("Unexpected MonitorAgentStatus:%d.", status)));
        }
    }
}

/**
 * Transfer agent status to normal, refreshes activetime and clear the recorded errors occured.
 */
static void transferToNormal(ManagerAgentWrapper *agentWrapper)
{
    Assert(agentWrapper->currentStatus == MONITOR_AGENT_STATUS_NORMAL ||
           agentWrapper->currentStatus == MONITOR_AGENT_STATUS_PENDING);
    /* Is status changed? */
    if (agentWrapper->currentStatus != MONITOR_AGENT_STATUS_NORMAL)
    {
        agentWrapper->previousStatus = agentWrapper->currentStatus;
        agentWrapper->currentStatus = MONITOR_AGENT_STATUS_NORMAL;
        clearAgentErrors(agentWrapper);
        ereport(LOG, (errmsg("Host name:%s, address:%s, agent running normal!",
                             NameStr(agentWrapper->hostWrapper->fdmh.hostname),
                             agentWrapper->hostWrapper->hostaddr)));
    }
    /* Update activeTime regardless of whether the status changes. */
    agentWrapper->activeTime = GetCurrentTimestamp();
}

/*
 * When received heartbeat message from a agent, we think this is a status 
 * normal agent. refreshes activetime and clear the recorded errors occured,  
 * if the different between activetime and current timestamp exceed the heartbeat 
 * critical time point, treat it as an error occurred, transfer status to pending, 
 * if not, send heartbeat message to agent periodically. when an error occurred 
 * while send heartbeat, transfer status to pending.
 */
static void handleNormalStatus(ManagerAgentWrapper *agentWrapper, TimestampTz currentTime)
{
    Assert(agentWrapper->currentStatus == MONITOR_AGENT_STATUS_NORMAL);
    if (TimestampDifferenceExceeds(agentWrapper->activeTime,
                                   currentTime,
                                   tolerationTime->heartbeatTimoutMs))
    {
        recordAgentError(agentWrapper, MONITOR_AGENT_ERROR_HEARTBEAT);
        transferToPending(agentWrapper);
        handlePendingStatus(agentWrapper, currentTime);
    }
    else if (TimestampDifferenceExceeds(agentWrapper->activeTime,
                                        currentTime,
                                        tolerationTime->heartbeatIntervalMs))
    {
        /* send heartbeat to agent. */
        if (!heartbeatAgent(agentWrapper))
        {
            recordAgentError(agentWrapper, MONITOR_AGENT_ERROR_MESSAGE);
            transferToPending(agentWrapper);
            handlePendingStatus(agentWrapper, currentTime);
        }
        else
        {
            /* wait for event. */
        }
    }
    else
    {
        /* wait for event. */
    }
}

/**
 * Transfer agent status to pending.
 */
static void transferToPending(ManagerAgentWrapper *agentWrapper)
{
    Assert(agentWrapper->currentStatus == MONITOR_AGENT_STATUS_NORMAL ||
           agentWrapper->currentStatus == MONITOR_AGENT_STATUS_PENDING ||
           agentWrapper->currentStatus == MONITOR_AGENT_STATUS_CRASHED);
    /* Is status changed? */
    if (agentWrapper->currentStatus != MONITOR_AGENT_STATUS_PENDING)
    {
        agentWrapper->previousStatus = agentWrapper->currentStatus;
        agentWrapper->currentStatus = MONITOR_AGENT_STATUS_PENDING;
    }
}

/**
 * If the status of an agent is pending, it is not yet possible to determine 
 * the true status of that agent. Determine if the agent has crashed by judging 
 * whether the deadline is reached or whether the error has reached a critical 
 * point. If not, continue to examine the connection to agent.
 */
static void handlePendingStatus(ManagerAgentWrapper *agentWrapper, TimestampTz currentTime)
{
    Assert(agentWrapper->currentStatus == MONITOR_AGENT_STATUS_PENDING);
    if (agentWrapper->previousStatus == MONITOR_AGENT_STATUS_CRASHED)
    {
        if (connectTimedOut(agentWrapper, currentTime))
        {
            if (haveConnectionToAgent(agentWrapper))
            {
                /* Avoid duplicate recording errors */
                recordAgentError(agentWrapper, MONITOR_AGENT_ERROR_CONNECT);
            }
            transferToCrashed(agentWrapper);
            handleCrashedStatus(agentWrapper, currentTime);
        }
        else
        {
            /* wait for event. */
        }
    }
    else
    {
        if (reachedDeadline(agentWrapper, currentTime) ||
            reachedErrorCriticalPoint(agentWrapper))
        {
            transferToCrashed(agentWrapper);
            handleCrashedStatus(agentWrapper, currentTime);
        }
        else
        {
            if (connectTimedOut(agentWrapper, currentTime))
            {
                if (haveConnectionToAgent(agentWrapper))
                {
                    recordAgentError(agentWrapper, MONITOR_AGENT_ERROR_CONNECT);
                    /* Because new error have been added, 
                        examine again for whether agent crashed. */
                    if (reachedDeadline(agentWrapper, currentTime) ||
                        reachedErrorCriticalPoint(agentWrapper))
                    {
                        transferToCrashed(agentWrapper);
                        handleCrashedStatus(agentWrapper, currentTime);
                    }
                    else
                    {
                        restartMonitorAgent(agentWrapper);
                    }
                }
                else
                {
                    restartMonitorAgent(agentWrapper);
                }
            }
            else
            {
                /* wait for event. */
            }
        }
    }
}

/**
 * Transfer agent status to crashed.
 */
static void transferToCrashed(ManagerAgentWrapper *agentWrapper)
{
    Assert(agentWrapper->currentStatus == MONITOR_AGENT_STATUS_PENDING ||
           agentWrapper->currentStatus == MONITOR_AGENT_STATUS_CRASHED);
    /* Is status changed? */
    if (agentWrapper->currentStatus != MONITOR_AGENT_STATUS_CRASHED)
    {
        agentWrapper->previousStatus = agentWrapper->currentStatus;
        agentWrapper->currentStatus = MONITOR_AGENT_STATUS_CRASHED;
        ereport(LOG, (errmsg("Host name:%s, address:%s, agent crashed!",
                             NameStr(agentWrapper->hostWrapper->fdmh.hostname),
                             agentWrapper->hostWrapper->hostaddr)));
    }
    /* sentinel, ensure to free resources. */
    stopMonitorAgent(agentWrapper);
}

/**
 * An agent in crashed status means that the it has crashed, restart it by ssh as 
 * we can. In order to prevent frequent restarts, we set a "restart delay" variable.
 * the "restart delay" variable will dynamically increase or decrease according to  
 * the restart frequency, after restart it, try to start monitor it. 
 * we do not clear errors unless this agent become normal.
 */
static void handleCrashedStatus(ManagerAgentWrapper *agentWrapper, TimestampTz currentTime)
{
    Assert(agentWrapper->currentStatus == MONITOR_AGENT_STATUS_CRASHED);
    if (allowRestartAgent(agentWrapper, currentTime))
    {
        restartAgent(agentWrapper);
    }
    /* 
     * if an agent is treated as crashed, Regardless of whether the restart is 
     * successful, try to connect to that agent for monitor. Whether or not the 
     * connection is successful, the status of that agent transfer to pending. when 
     * we handle pending status, we need to consider this situation, prevent the   .
     * agent from becoming a crashed state before the connection times out status.
     */
    restartMonitorAgent(agentWrapper);
}

/* 
 * Examine if the errors reached the critical point.
 */
static bool reachedErrorCriticalPoint(ManagerAgentWrapper *agentWrapper)
{
    int nConnectErrors = 0;
    int nMessageErrors = 0;
    int nHeartbeatErrors = 0;
    /* If the number of errors that occurred have reached critical point? */
    if (agentWrapper->nerrors >= 3)
    {
        return true;
    }
    else
    {
        int i;
        for (i = 0; i < agentWrapper->nerrors; i++)
        {
            if (agentWrapper->errors[i] == MONITOR_AGENT_ERROR_CONNECT)
            {
                nConnectErrors++;
            }
            else if (agentWrapper->errors[i] == MONITOR_AGENT_ERROR_MESSAGE)
            {
                nMessageErrors++;
            }
            else if (agentWrapper->errors[i] == MONITOR_AGENT_ERROR_HEARTBEAT)
            {
                nHeartbeatErrors++;
            }
            else
            {
                ereport(LOG, (errmsg("Unexpected MonitorAgentError:%d.", agentWrapper->errors[i])));
            }
        }
        /* If the number of errors of the same type has reached critical point? */
        return nConnectErrors >= 3 || nMessageErrors >= 2 || nHeartbeatErrors >= 2;
    }
}

/* 
 * Examine if the difference of errortime and currenttimestamp reached deadline. 
 */
static bool reachedDeadline(ManagerAgentWrapper *agentWrapper, TimestampTz currentTime)
{
    if (agentWrapper->errorTime == INVALID_TIMESTAMP)
    {
        return false;
    }
    else
    {
        return TimestampDifferenceExceeds(agentWrapper->errorTime,
                                          currentTime,
                                          tolerationTime->agentdeadlineMs);
    }
}

/**
 * If there an error occurred on and agent, such as connection error, 
 * message error, heartbeat timeout error, etc. record this error.
 * transfer the status of that agent to pending. pending means unable
 * to determine the true state, further examination is required. on this
 * time, the errors have recorded is very important to determine the true 
 * status of that agent.
 */
static void recordAgentError(ManagerAgentWrapper *agentWrapper, MonitorAgentError error)
{
    MonitorAgentError *newErrors;
    int oldErrorNum;
    int newErrorNum;
    int oldMemSize;
    int newMemSize;

    oldErrorNum = agentWrapper->nerrors;
    if (oldErrorNum >= 20)
    {
        /* It is not necessary to record error. do not a waste of memory. */
        return;
    }
    if (oldErrorNum > 0)
    {
        /* if there have errors occurred already, append this new occurred error.  */
        newErrorNum = oldErrorNum + 1;
        oldMemSize = sizeof(MonitorAgentError) * oldErrorNum;
        newMemSize = sizeof(MonitorAgentError) * newErrorNum;
        newErrors = palloc(newMemSize);
        memcpy(newErrors, agentWrapper->errors, oldMemSize);
        newErrors[oldErrorNum] = error;
        pfree(agentWrapper->errors);
        agentWrapper->errors = newErrors;
        agentWrapper->nerrors = newErrorNum;
    }
    else
    {
        agentWrapper->errorTime = GetCurrentTimestamp();
        agentWrapper->errors = palloc(sizeof(MonitorAgentError));
        agentWrapper->errors[0] = error;
        agentWrapper->nerrors = 1;
    }
}

static void clearAgentErrors(ManagerAgentWrapper *agentWrapper)
{
    if (agentWrapper->errors != NULL)
    {
        pfree(agentWrapper->errors);
        agentWrapper->errors = NULL;
    }
    agentWrapper->errorTime = INVALID_TIMESTAMP;
    agentWrapper->nerrors = 0;
}

/**
 * Is There have a socket connection to the agent? 
 */
static bool haveConnectionToAgent(ManagerAgentWrapper *agentWrapper)
{
    if (agentWrapper->agent == NULL)
    {
        return false;
    }
    else
    {
        return ma_isconnected(agentWrapper->agent);
    }
}

/*
 * The agent is so called mgr agent process. try to connect to it,
 * if connected, add WaitEvent to WaitEventSet for further monitoring.
 * if not, record the error and then to pending.
 */
static bool startMonitorAgent(ManagerAgentWrapper *agentWrapper)
{
    ManagerAgent *agent;
    bool connected;

    agentWrapper->connectTime = GetCurrentTimestamp();
    agent = ma_connect_noblock(agentWrapper->hostWrapper->hostaddr, agentWrapper->hostWrapper->fdmh.hostagentport);
    agentWrapper->agent = agent;
    connected = ma_isconnected(agent);
    if (connected)
    {
        /* Connect agents in a non-blocking manner, require further event waiting. */
        addAgentWaitEventToSet(agentWrapper);
    }
    else
    {
        /* Add an error to help determine the true status of the agent. */
        recordAgentError(agentWrapper, MONITOR_AGENT_ERROR_CONNECT);
    }
    /* 
     * Regardless of whether the connection is successful,
     * further examination is needed to determine the true status of the agent.
     */
    transferToPending(agentWrapper);
    return connected;
}

/* 
 * Remove the WaitEvent from the WaitEventSet if setted.
 * Close the connection to the agent if connected.
 */
static void stopMonitorAgent(ManagerAgentWrapper *agentWrapper)
{
    /* Ensure to remove WaitEvent from the WaitEventSet. */
    removeAgentWaitEvent(agentWrapper);

    /* Ensure to disconnect from the agent. */
    if (agentWrapper->agent != NULL)
    {
        ma_close(agentWrapper->agent);
        agentWrapper->agent = NULL;
    }
}

static bool restartMonitorAgent(ManagerAgentWrapper *agentWrapper)
{
    if (allowReconnectAgent(agentWrapper))
    {
        stopMonitorAgent(agentWrapper);
        startMonitorAgent(agentWrapper);
        return true;
    }
    else
    {
        /* connect is not allowed, wait for next time. */
        return false;
    }
}

static bool connectTimedOut(ManagerAgentWrapper *agentWrapper, TimestampTz currentTime)
{
    return TimestampDifferenceExceeds(agentWrapper->connectTime,
                                      currentTime,
                                      tolerationTime->connectTimeoutMs);
}

static bool allowReconnectAgent(ManagerAgentWrapper *agentWrapper)
{
    return TimestampDifferenceExceeds(agentWrapper->connectTime,
                                      GetCurrentTimestamp(),
                                      tolerationTime->connectTimeoutMs +
                                          tolerationTime->reconnectDelayMs);
}

/**
 * Allow restart agent means that the difference between "restartTime" and "currentTime"
 * exceeds "restartDelay", and the configuration in table mgr_host shows allow to do it.
 */
static bool allowRestartAgent(ManagerAgentWrapper *agentWrapper, TimestampTz currentTime)
{
    int ret;
    long realRestartDelayMs;
    bool allow;
    AdbMgrHostWrapper *host;
    Oid hostOid;
    MemoryContext oldContext;
    /* We don't want to restart too often.  */
    realRestartDelayMs = tolerationTime->restartDelayMs * (1 << agentWrapper->restartFactor);
    allow = TimestampDifferenceExceeds(agentWrapper->restartTime,
                                       currentTime,
                                       realRestartDelayMs);
    if (!allow)
    {
        return false;
    }

    oldContext = CurrentMemoryContext;

    /* Double check to ensure that the host information in the mgr database has not changed. */
    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());
    ret = SPI_connect();
    if (ret != SPI_OK_CONNECT)
    {
        ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
                        (errmsg("SPI_connect failed, connect return:%d.", ret))));
    }

    hostOid = agentWrapper->hostWrapper->oid;
    host = SPI_selectMgrHostByOid(oldContext, hostOid);
    if (host == NULL)
    {
        ereport(LOG, (errmsg("Host name:%s, address:%s, oid:%u not exists in the table.",
                             NameStr(agentWrapper->hostWrapper->fdmh.hostname),
                             agentWrapper->hostWrapper->hostaddr,
                             hostOid)));
        allow = false;
    }
    else
    {
        allow = host->fdmh.allowcure;
        pfreeAdbMgrHostWrapper(host);
    }
    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();

    MemoryContextSwitchTo(oldContext);

    return allow;
}

static bool restartAgent(ManagerAgentWrapper *agentWrapper)
{
    bool done;
    char *retMessage = NULL;

    done = mgr_start_agent_execute(&agentWrapper->hostWrapper->fdmh,
                                   agentWrapper->hostWrapper->hostaddr,
                                   agentWrapper->hostWrapper->hostadbhome,
                                   "", /* TODO, how to get password? */
                                   &retMessage);
    /* The previous operations will take a while, we need precise time to prevent repeated restarts. */
    agentWrapper->restartTime = GetCurrentTimestamp();
    /* Modify the value of the variable that controls the restart frequency. */
    nextRestartFactor(agentWrapper);

    if (done)
    {
        ereport(LOG, (errmsg("Host name:%s, address:%s, restart agent completed.",
                             NameStr(agentWrapper->hostWrapper->fdmh.hostname),
                             agentWrapper->hostWrapper->hostaddr)));
    }
    else
    {
        ereport(LOG, (errmsg("Host name:%s, address:%s, restart agent failed:%s.",
                             NameStr(agentWrapper->hostWrapper->fdmh.hostname),
                             agentWrapper->hostWrapper->hostaddr,
                             retMessage)));
    }
    if (retMessage != NULL)
        pfree(retMessage);
    return done;
}

static bool heartbeatAgent(ManagerAgentWrapper *agentWrapper)
{
    StringInfoData buf;
    initStringInfo(&buf);
    /* send idle message, do it as heartbeat message. */
    ma_beginmessage(&buf, AGT_MSG_IDLE);
    ma_endmessage(&buf, agentWrapper->agent);
    return ma_flush(agentWrapper->agent, false);
}

/**
 * Use variable restartFactor to control the next restart time,
 * if it hit the maximum, decrease it,
 * if it hit the minimum, increase it.
 */
static void nextRestartFactor(ManagerAgentWrapper *agentWrapper)
{
    int restartFactor;
    bool restartFactorIncrease;

    restartFactor = agentWrapper->restartFactor;
    restartFactorIncrease = agentWrapper->restartFactorIncrease;
    if (restartFactorIncrease)
    {
        if (restartFactor >= RESTART_FACTOR_MAX)
        {
            /* hit the maximum, reverse. */
            restartFactor--;
            restartFactorIncrease = false;
        }
        else
        {
            restartFactor++;
        }
    }
    else
    {
        if (restartFactor <= 0)
        {
            /* hit the minimum, reverse. */
            restartFactor++;
            restartFactorIncrease = true;
        }
        else
        {
            restartFactor--;
        }
    }
    /* negative value is not allowed. */
    if (restartFactor < 0)
    {
        restartFactor = 0;
    }
    agentWrapper->restartFactor = restartFactor;
    agentWrapper->restartFactorIncrease = restartFactorIncrease;
}

/**
 * Ensure WaitEvent are not added or removed repeatedly, 
 * preventing mess up the WaitEventSet.
 */
static void invalidateEventPosition(ManagerAgentWrapper *agentWrapper)
{
    agentWrapper->event_pos = -1;
}

/**
 * Add a WaitEvent WL_SOCKET_READABLE to WaitEventSet.
 */
static void addAgentWaitEventToSet(ManagerAgentWrapper *agentWrapper)
{
    int pos;

    /* Sentinel, preventing being added to WaitEventSet repeatedly */
    if (agentWrapper->event_pos >= 0)
        return;
    pos = AddWaitEventToSet(agentWaitEventSet,
                            WL_SOCKET_READABLE,
                            ma_getsock(agentWrapper->agent),
                            NULL,
                            agentWrapper);
    agentWrapper->event_pos = pos;
}

/**
 * Remove the WaitEvent from WaitEventSet. Invalidate the event location to 
 * prevent the next mistaken addition to the WaitEventSet or the wrong deletion 
 * of the WaitEvent from the WaitEventSet. This will mess up the WaitEventSet. 
 * Because the WaitEventSet saves these WaitEvents in an array that referenced 
 * by a pointer. When a WaitEvent is deleted, the position of the other WaitEvents
 * in the array must be adjusted accordingly.
 */
static void removeAgentWaitEvent(ManagerAgentWrapper *agentWrapper)
{
    dlist_iter iter;
    ManagerAgentWrapper *element;
    int pos = agentWrapper->event_pos;

    if (pos >= 0)
    {
        RemoveWaitEvent(agentWaitEventSet, agentWrapper->event_pos);

        /* adjust position of other events. */
        dlist_foreach(iter, &totalAgentList)
        {
            element = dlist_container(ManagerAgentWrapper, list_node, iter.cur);
            if (element->event_pos > pos)
                --element->event_pos;
        }
    }
    /* Sentinel, prevent added to WaitEventSet repeatedly next time. */
    invalidateEventPosition(agentWrapper);
}

static void OnLatchSetEvent(WaitEvent *event)
{
    ResetLatch(&MyProc->procLatch);
    if (gotSigusr1)
    {
        gotSigusr1 = false;
        bool confChanged;
        int newAgentdeadline;

        /* check and update TolerationTime */
        LWLockAcquire(&confShm->confInShm->lock, LW_SHARED);
        newAgentdeadline = confShm->confInShm->agentdeadline;
        LWLockRelease(&confShm->confInShm->lock);
        confChanged = agentdeadline != newAgentdeadline;
        if (confChanged)
        {
            ereport(LOG,
                    (errmsg("Configuration changed, agentdeadline:old[%d], new[%d].",
                            agentdeadline, newAgentdeadline)));
            agentdeadline = newAgentdeadline;
            pfree(tolerationTime);
            tolerationTime = newTolerationTime(agentdeadline);
        }
    }
}

static void OnPostmasterDeathEvent(WaitEvent *event)
{
    exit(1);
}

/*
 * If received heartbeat message from agent, determine the running status
 * of that agent is normal,  when received error determine it have been crashed. 
 */
static void OnAgentMsgEvent(WaitEvent *event)
{
    ManagerAgentWrapper *agentWrapper = (ManagerAgentWrapper *)event->user_data;
    StringInfoData recvbuf;
    char msg_type;
    initStringInfo(&recvbuf);
    for (;;)
    {
        resetStringInfo(&recvbuf);
        msg_type = ma_get_message(agentWrapper->agent, &recvbuf);
        if (msg_type == AGT_MSG_IDLE)
        {
            ereport(LOG, (errmsg("Host name:%s, address:%s, agent idle.",
                                 NameStr(agentWrapper->hostWrapper->fdmh.hostname),
                                 agentWrapper->hostWrapper->hostaddr)));
            transferToNormal(agentWrapper);
            break;
        }
        else if (msg_type == '\0')
        {
            ereport(LOG, (errmsg("Host name:%s, address:%s, receive message failed.",
                                 NameStr(agentWrapper->hostWrapper->fdmh.hostname),
                                 agentWrapper->hostWrapper->hostaddr)));
            /* 
             * Running here means that the agent has been disconnected or other fatal error.
             * If doesn't stop monitor agent, then we will continue to receive this event.
             * We will run back here immediately, just like an infinite loop. So must stop
             * monitor this agent, wait for next delay time to reconnect to it.
             */
            stopMonitorAgent(agentWrapper);
            /* Add an error to help determine the true status of the agent. */
            recordAgentError(agentWrapper, MONITOR_AGENT_ERROR_MESSAGE);
            transferToPending(agentWrapper);
            break;
        }
        else if (msg_type == AGT_MSG_ERROR)
        {
            /* ignore error message */
            ereport(LOG, (errmsg("Host name:%s, address:%s, receive error message:%s",
                                 NameStr(agentWrapper->hostWrapper->fdmh.hostname),
                                 agentWrapper->hostWrapper->hostaddr,
                                 ma_get_err_info(&recvbuf, AGT_MSG_RESULT))));
            break;
        }
        else if (msg_type == AGT_MSG_NOTICE)
        {
            /* ignore notice message */
        }
        else if (msg_type == AGT_MSG_RESULT)
        {
            ereport(LOG, (errmsg("Host name:%s, address:%s, receive message:%s",
                                 NameStr(agentWrapper->hostWrapper->fdmh.hostname),
                                 agentWrapper->hostWrapper->hostaddr,
                                 recvbuf.data)));
            transferToNormal(agentWrapper);
            break;
        }
        else if (msg_type == AGT_MSG_COMMAND)
        {
            ereport(LOG, (errmsg("Host name:%s, address:%s, receive message:%s",
                                 NameStr(agentWrapper->hostWrapper->fdmh.hostname),
                                 agentWrapper->hostWrapper->hostaddr,
                                 recvbuf.data)));
            transferToNormal(agentWrapper);
            break;
        }
    }
    pfree(recvbuf.data);
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
    if (agentWrapper->agent != NULL)
    {
        ma_close(agentWrapper->agent);
        agentWrapper->agent = NULL;
    }
    pfreeAdbMgrHostWrapper(agentWrapper->hostWrapper);
    pfree(agentWrapper);
}