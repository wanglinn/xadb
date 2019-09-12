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
#include "mgr/mgr_helper.h"
#include "utils/memutils.h"
#include "adb_doctor.h"

/* 
 * Benchmark of the time interval, The following elements 
 * based on agentdeadlineMs, but have min and max value limit.
 * For more information, please refer to function newHostConfiguration(). 
 */
typedef struct HostConfiguration
{
	long agentdeadlineMs;
	long waitEventTimeoutMs;
	long connectTimeoutMs;
	long reconnectDelayMs;
	long heartbeatTimoutMs;
	long heartbeatIntervalMs;
	long restartDelayMs;
	int connectionErrorNumMax;
} HostConfiguration;

typedef enum MonitorAgentError
{
	AGENT_ERROR_CONNECT_FAIL = 1,
	AGENT_ERROR_MESSAGE_FAIL,
	AGENT_ERROR_HEARTBEAT_TIMEOUT
} MonitorAgentError;

/* Do not change the value of MonitorAgentError */
const static char *AGENT_ERROR_MSG[] =
	{"CONNECT_FAIL", "MESSAGE_FAIL", "HEARTBEAT_FAIL"};

typedef enum RestartAgentMode
{
	RESTART_AGENT_MODE_SSH = 1,
	RESTART_AGENT_MODE_SEND_MESSAGE
} RestartAgentMode;

typedef enum AgentConnectionStatus
{
	AGENT_CONNNECTION_STATUS_CONNECTING = 1,
	AGENT_CONNNECTION_STATUS_SUCCEEDED,
	AGENT_CONNNECTION_STATUS_BAD
} AgentConnectionStatus;

typedef enum AgentRunningStatus
{
	AGENT_RUNNING_STATUS_NORMAL = 1,
	AGENT_RUNNING_STATUS_CRASHED
} AgentRunningStatus;

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
	AgentConnectionStatus connectionStatus;
	AgentRunningStatus runningStatus;
	TimestampTz connectTime;
	TimestampTz sendMessageTime;
	TimestampTz activeTime;
	TimestampTz crashedTime;
	TimestampTz restartTime;
	MgrHostWrapper *mgrHost;
	AdbDoctorBounceNum *restartFactor;
	AdbDoctorErrorRecorder *errors;
} ManagerAgentWrapper;

static void hostMonitorMainLoop(void);
static void initializeWaitEventSet(void);
static void initializeAgentsConnection(void);

static void examineAgentsStatus(void);
static void resetHostMonitor(void);

static void handleConnectionStatusConnecting(ManagerAgentWrapper *agentWrapper, TimestampTz currentTime);
static void handleConnectionStatusSucceeded(ManagerAgentWrapper *agentWrapper, TimestampTz currentTime);
static void handleConnectionStatusBad(ManagerAgentWrapper *agentWrapper);
static void toConnectionStatusConnecting(ManagerAgentWrapper *agentWrapper);
static void toConnectionStatusSucceeded(ManagerAgentWrapper *agentWrapper);
static void toConnectionStatusBad(ManagerAgentWrapper *agentWrapper, MonitorAgentError error);

static void handleRunningStatusNormal(ManagerAgentWrapper *agentWrapper);
static void handleRunningStatusCrashed(ManagerAgentWrapper *agentWrapper);
static void toRunningStatusNormal(ManagerAgentWrapper *agentWrapper);
static void toRunningStatusCrashed(ManagerAgentWrapper *agentWrapper);

static bool reachedCrashedCondition(ManagerAgentWrapper *agentWrapper);
static bool startMonitorAgent(ManagerAgentWrapper *agentWrapper);
static void stopMonitorAgent(ManagerAgentWrapper *agentWrapper);
static bool allowRestartAgent(ManagerAgentWrapper *agentWrapper);
static bool restartAgent(ManagerAgentWrapper *agentWrapper, RestartAgentMode mode);
static bool restartAgentBySSH(ManagerAgentWrapper *agentWrapper);
static bool restartAgentByCmdMessage(ManagerAgentWrapper *agentWrapper);
static bool sendHeartbeatMessage(ManagerAgentWrapper *agentWrapper);
static bool sendStopAgentMessage(ManagerAgentWrapper *agentWrapper);
static bool sendResetAgentMessage(ManagerAgentWrapper *agentWrapper);
static bool beyondRestartDelay(ManagerAgentWrapper *agentWrapper);
static bool shouldResetRestartFactor(ManagerAgentWrapper *agentWrapper);
static void nextRestartFactor(ManagerAgentWrapper *agentWrapper);

static void invalidateEventPosition(ManagerAgentWrapper *agentWrapper);
static void addAgentWaitEventToSet(ManagerAgentWrapper *agentWrapper, uint32 events);
static void ModifyAgentWaitEventSet(ManagerAgentWrapper *agentWrapperevents, uint32 events);
static void removeAgentWaitEvent(ManagerAgentWrapper *agentWrapper);

static void OnLatchSetEvent(WaitEvent *event);
static void OnPostmasterDeathEvent(WaitEvent *event);
static void OnAgentConnectionEvent(WaitEvent *event);
static void OnAgentMessageEvent(WaitEvent *event);

static HostConfiguration *newHostConfiguration(AdbDoctorConf *conf);
static void getCheckMgrHostsForHostDoctor(dlist_head *hosts);

static void handleSigterm(SIGNAL_ARGS);
static void handleSigusr1(SIGNAL_ARGS);

static void pfreeManagerAgentWrapper(ManagerAgentWrapper *agentWrapper);
static void pfreeAgentsInList(dlist_head *list);

static volatile sig_atomic_t gotSigterm = false;
static volatile sig_atomic_t gotSigusr1 = false;

static dlist_head *cachedMgrHosts;
static dlist_head totalAgentList = DLIST_STATIC_INIT(totalAgentList);

static AdbDoctorConfShm *confShm;
static HostConfiguration *hostConfiguration = NULL;
static sigjmp_buf reset_host_monitor_sigjmp_buf;

static WaitEventSet *agentWaitEventSet = NULL;
static WaitEvent *occurredEvents = NULL;
static int nOccurredEvents = 0;

static const WaitEventData LatchSetEventData = {OnLatchSetEvent};
static const WaitEventData PostmasterDeathEventData = {OnPostmasterDeathEvent};

/*
 * This is a backgroundworker process, it monitor all agents in all hosts,
 * if it exit abnormally, the adb doctor launcher process will detected this
 * event, and then that launcher process will launch this process again. 
 */
void adbDoctorHostMonitorMain(Datum main_arg)
{
	ErrorData *edata = NULL;
	MemoryContext oldContext;
	AdbDoctorBgworkerData *bgworkerData;
	AdbDoctorConf *confInLocal;

	oldContext = CurrentMemoryContext;
	pqsignal(SIGTERM, handleSigterm);
	pqsignal(SIGUSR1, handleSigusr1);
	BackgroundWorkerUnblockSignals();
	BackgroundWorkerInitializeConnection(DEFAULT_DB, NULL, 0);

	PG_TRY();
	{
		bgworkerData = attachAdbDoctorBgworkerDataShm(main_arg,
													  MyBgworkerEntry->bgw_name);
		notifyAdbDoctorRegistrant();
		ereport(LOG,
				(errmsg("%s started",
						MyBgworkerEntry->bgw_name)));

		confShm = attachAdbDoctorConfShm(bgworkerData->commonShmHandle,
										 MyBgworkerEntry->bgw_name);
		confInLocal = copyAdbDoctorConfFromShm(confShm);
		hostConfiguration = newHostConfiguration(confInLocal);
		pfree(confInLocal);

		cachedMgrHosts = palloc0(sizeof(dlist_head));
		if (sigsetjmp(reset_host_monitor_sigjmp_buf, 1) != 0)
		{
			pfreeAgentsInList(&totalAgentList);
			dlist_init(&totalAgentList);
			if (agentWaitEventSet)
			{
				FreeWaitEventSet(agentWaitEventSet);
				agentWaitEventSet = NULL;
			}
		}

		dlist_init(cachedMgrHosts);
		getCheckMgrHostsForHostDoctor(cachedMgrHosts);
		hostMonitorMainLoop();
	}
	PG_CATCH();
	{
		/* Save error info in our stmt_mcontext */
		MemoryContextSwitchTo(oldContext);
		edata = CopyErrorData();
		FlushErrorState();
	}
	PG_END_TRY();

	pfreeAgentsInList(&totalAgentList);
	if (agentWaitEventSet)
		FreeWaitEventSet(agentWaitEventSet);
	pfreeAdbDoctorConfShm(confShm);
	if (edata)
		ReThrowError(edata);
	else
		proc_exit(1);
}

/* 
 * Connect to all of the agents, keep alive by send heartbeat message,
 * wait event of these socket, determine the status of agent.
 */
static void hostMonitorMainLoop()
{
	WaitEvent *event;
	WaitEventData *volatile wed = NULL;

	int rc, i;

	initializeWaitEventSet();

	initializeAgentsConnection();

	while (!gotSigterm)
	{
		CHECK_FOR_INTERRUPTS();

		rc = WaitEventSetWait(agentWaitEventSet,
							  hostConfiguration->waitEventTimeoutMs,
							  occurredEvents,
							  nOccurredEvents,
							  PG_WAIT_CLIENT);

		for (i = 0; i < rc; ++i)
		{
			event = &occurredEvents[i];
			wed = event->user_data;
			(*wed->fun)(event);
		}

		/* Examine the status of all agents, determine whether it crashed.  */
		examineAgentsStatus();
	}
}

/* 
 * In order to quickly get events occurred at agents you must first initialize
 * WaitEventSet, and also add events WL_LATCH_SET and WL_POSTMASTER_DEATH.
 */
static void initializeWaitEventSet()
{
	int nAgents = 0;
	dlist_iter iter;

	dlist_foreach(iter, cachedMgrHosts)
	{
		nAgents++;
	}
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
static void initializeAgentsConnection()
{
	ManagerAgentWrapper *agentWrapper;
	dlist_iter iter;
	MgrHostWrapper *mgrHost;

	dlist_foreach(iter, cachedMgrHosts)
	{
		mgrHost = dlist_container(MgrHostWrapper, link, iter.cur);
		/* Initialize agent to normal status. */
		agentWrapper = palloc0(sizeof(ManagerAgentWrapper));
		agentWrapper->wed.fun = NULL;
		invalidateEventPosition(agentWrapper);
		agentWrapper->agent = NULL;
		agentWrapper->connectionStatus = AGENT_CONNNECTION_STATUS_BAD;
		agentWrapper->runningStatus = AGENT_RUNNING_STATUS_NORMAL;
		agentWrapper->connectTime = 0;
		agentWrapper->sendMessageTime = 0;
		agentWrapper->activeTime = 0;
		agentWrapper->crashedTime = 0;
		agentWrapper->restartTime = 0;
		agentWrapper->mgrHost = mgrHost;
		agentWrapper->restartFactor = newAdbDoctorBounceNum(0, 5);
		agentWrapper->errors = newAdbDoctorErrorRecorder(100);

		startMonitorAgent(agentWrapper);

		dlist_push_tail(&totalAgentList, &agentWrapper->list_node);
	}
}

/**
 * All agent information are stored in a list, Iterate through this list and perform
 * different processing according to different statuses. Our goal is to find these 
 * crashed agnets as soon as possible.
 */
static void examineAgentsStatus(void)
{
	ManagerAgentWrapper *agentWrapper;
	AgentConnectionStatus connectionStatus;
	AgentRunningStatus runningStatus;
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
		connectionStatus = agentWrapper->connectionStatus;
		if (connectionStatus == AGENT_CONNNECTION_STATUS_CONNECTING)
		{
			handleConnectionStatusConnecting(agentWrapper, currentTime);
		}
		else if (connectionStatus == AGENT_CONNNECTION_STATUS_SUCCEEDED)
		{
			handleConnectionStatusSucceeded(agentWrapper, currentTime);
		}
		else if (connectionStatus == AGENT_CONNNECTION_STATUS_BAD)
		{
			handleConnectionStatusBad(agentWrapper);
		}
		else
		{
			ereport(ERROR,
					(errmsg("Unexpected AgentConnectionStatus:%d.",
							connectionStatus)));
		}

		runningStatus = agentWrapper->runningStatus;
		if (runningStatus == AGENT_RUNNING_STATUS_NORMAL)
		{
			handleRunningStatusNormal(agentWrapper);
		}
		else if (runningStatus == AGENT_RUNNING_STATUS_CRASHED)
		{
			handleRunningStatusCrashed(agentWrapper);
		}
		else
		{
			ereport(ERROR,
					(errmsg("Unexpected AgentRunningStatus:%d.",
							runningStatus)));
		}
	}
}

static void resetHostMonitor(void)
{
	ereport(LOG,
			(errmsg("%s, reset host monitor",
					MyBgworkerEntry->bgw_name)));

	siglongjmp(reset_host_monitor_sigjmp_buf, 1);
}

static void handleConnectionStatusConnecting(ManagerAgentWrapper *agentWrapper,
											 TimestampTz currentTime)
{
	if (TimestampDifferenceExceeds(agentWrapper->connectTime,
								   currentTime,
								   hostConfiguration->connectTimeoutMs))
	{
		toConnectionStatusBad(agentWrapper, AGENT_ERROR_CONNECT_FAIL);
		handleConnectionStatusBad(agentWrapper);
	}
	else
	{
		/* if connect successfuly, the event handler will transit status to succeeded  */
	}
}

/*
 * When received heartbeat message from a agent, we think this is a status 
 * normal agent. refreshes activetime and clear the recorded errors occured,  
 * if the different between activetime and current timestamp exceed the heartbeat 
 * timeout point, handle it as an error occurred, if not, send heartbeat message
 * to agent periodically. when an error occurred while send heartbeat, it an 
 * error too. If any error occurred, We think the connection is bad.
 */
static void handleConnectionStatusSucceeded(ManagerAgentWrapper *agentWrapper,
											TimestampTz currentTime)
{
	if ((agentWrapper->sendMessageTime > agentWrapper->activeTime) &&
		TimestampDifferenceExceeds(agentWrapper->sendMessageTime,
								   currentTime,
								   hostConfiguration->heartbeatTimoutMs))
	{
		toConnectionStatusBad(agentWrapper, AGENT_ERROR_HEARTBEAT_TIMEOUT);
		handleConnectionStatusBad(agentWrapper);
	}
	else
	{
		if (TimestampDifferenceExceeds(agentWrapper->sendMessageTime,
									   currentTime,
									   hostConfiguration->heartbeatIntervalMs))
		{
			/* send heartbeat to agent. */
			if (!sendHeartbeatMessage(agentWrapper))
			{
				toConnectionStatusBad(agentWrapper, AGENT_ERROR_MESSAGE_FAIL);
				handleConnectionStatusBad(agentWrapper);
			}
			else
			{
				agentWrapper->sendMessageTime = GetCurrentTimestamp();
			}
		}
		else
		{
			/* wait for event. */
		}
	}
}

static void handleConnectionStatusBad(ManagerAgentWrapper *agentWrapper)
{
	/* Try to reconnect to the agent. */
	if (TimestampDifferenceExceeds(agentWrapper->connectTime,
								   GetCurrentTimestamp(),
								   hostConfiguration->reconnectDelayMs))
	{
		stopMonitorAgent(agentWrapper);
		startMonitorAgent(agentWrapper);
	}
	else
	{
		ereport(DEBUG1,
				(errmsg("%s:%s, connect agent too often",
						NameStr(agentWrapper->mgrHost->form.hostname),
						agentWrapper->mgrHost->hostaddr)));
	}
}

static void toConnectionStatusConnecting(ManagerAgentWrapper *agentWrapper)
{
	ereport(DEBUG1,
			(errmsg("%s:%s, start connect agent",
					NameStr(agentWrapper->mgrHost->form.hostname),
					agentWrapper->mgrHost->hostaddr)));
	agentWrapper->connectionStatus = AGENT_CONNNECTION_STATUS_CONNECTING;
	agentWrapper->connectTime = GetCurrentTimestamp();
}

/**
 * Connection succeeded, refreshes activetime and clear the recorded errors occured.
 */
static void toConnectionStatusSucceeded(ManagerAgentWrapper *agentWrapper)
{
	if (agentWrapper->connectionStatus != AGENT_CONNNECTION_STATUS_SUCCEEDED)
	{
		ereport(DEBUG1,
				(errmsg("%s:%s, connection status succeeded",
						NameStr(agentWrapper->mgrHost->form.hostname),
						agentWrapper->mgrHost->hostaddr)));
		agentWrapper->connectionStatus = AGENT_CONNNECTION_STATUS_SUCCEEDED;
		/* clear all errors */
		resetAdbDoctorErrorRecorder(agentWrapper->errors);
	}
	/* Update activeTime regardless of whether the status changes. */
	agentWrapper->activeTime = GetCurrentTimestamp();
}

/**
 * If there an error occurred on and agent, such as connection error, 
 * message error, heartbeat timeout error, etc. record this error.
 * transit the connection status of that agent to bad. Connnection bad 
 * means that such agent may be crashed, but not necessarily a real crash.
 * To determine the true running state further examination is required. 
 * on this time, the errors have recorded is very useful to determine the
 * real running status of that agent.
 */
static void toConnectionStatusBad(ManagerAgentWrapper *agentWrapper, MonitorAgentError error)
{
	ereport(DEBUG1,
			(errmsg("%s:%s, error:%s",
					NameStr(agentWrapper->mgrHost->form.hostname),
					agentWrapper->mgrHost->hostaddr,
					AGENT_ERROR_MSG[(int)error - 1])));
	appendAdbDoctorErrorRecorder(agentWrapper->errors, (int)error);
	agentWrapper->connectionStatus = AGENT_CONNNECTION_STATUS_BAD;
	/* sentinel, ensure to free resources. */
	stopMonitorAgent(agentWrapper);
	ereport(DEBUG1,
			(errmsg("%s:%s, connection status bad",
					NameStr(agentWrapper->mgrHost->form.hostname),
					agentWrapper->mgrHost->hostaddr)));
}

static void handleRunningStatusNormal(ManagerAgentWrapper *agentWrapper)
{
	if (reachedCrashedCondition(agentWrapper))
	{
		toRunningStatusCrashed(agentWrapper);
		handleRunningStatusCrashed(agentWrapper);
	}
	else
	{
		/* Continuous monitoring error */
	}
}

/**
 * An agent in crashed status means that the it has crashed, restart it by ssh as 
 * we can. In order to prevent frequent restarts, we set a "restart delay" variable.
 * the "restart delay" variable will dynamically increase or decrease according to  
 * the restart frequency, after restart it, check if that agent become normal again.
 */
static void handleRunningStatusCrashed(ManagerAgentWrapper *agentWrapper)
{
	/**
	 * Here, we do not need to consider how to maintain the connection status,  
	 * The program will automatically detect the connection failure, then reconnect 
	 * and keep looping.So we judge whether the running status is back to normal by 
	 * whether an error has occurred and whether the connection is normal.
	 */
	if (agentWrapper->activeTime > agentWrapper->connectTime &&
		agentWrapper->activeTime > agentWrapper->crashedTime &&
		agentWrapper->errors->nerrors == 0 &&
		ma_isconnected(agentWrapper->agent))
	{
		toRunningStatusNormal(agentWrapper);
		handleRunningStatusNormal(agentWrapper);
	}
	else
	{
		if (agentWrapper->crashedTime > 0)
		{
			if (restartAgent(agentWrapper, RESTART_AGENT_MODE_SSH))
			{
				/* Get a brand new connection to agent just like 
				 * that agent is running normally.*/
				resetAdbDoctorErrorRecorder(agentWrapper->errors);
				stopMonitorAgent(agentWrapper);
				startMonitorAgent(agentWrapper);
			}
			else
			{
				/* Will still connect to agent, but there may be a delay */
			}
		}
		else
		{
			/* In initialized status, assume the agent is running normally. */
			toRunningStatusNormal(agentWrapper);
		}
	}
}

static void toRunningStatusNormal(ManagerAgentWrapper *agentWrapper)
{
	agentWrapper->runningStatus = AGENT_RUNNING_STATUS_NORMAL;
	resetAdbDoctorBounceNum(agentWrapper->restartFactor);
	agentWrapper->restartTime = 0;
	ereport(LOG,
			(errmsg("%s:%s, agent status normal",
					NameStr(agentWrapper->mgrHost->form.hostname),
					agentWrapper->mgrHost->hostaddr)));
}

static void toRunningStatusCrashed(ManagerAgentWrapper *agentWrapper)
{
	agentWrapper->runningStatus = AGENT_RUNNING_STATUS_CRASHED;
	agentWrapper->crashedTime = GetCurrentTimestamp();
	ereport(LOG,
			(errmsg("%s:%s, agent status crashed",
					NameStr(agentWrapper->mgrHost->form.hostname),
					agentWrapper->mgrHost->hostaddr)));
}

/**
 * If the status of an agent is pending, it is not yet possible to determine 
 * the running status of that agent. Determine if the agent has crashed by judging 
 * whether the deadline is reached or whether the error has reached a critical 
 * point. If not, continue to examine the connection to agent.
 */
static bool reachedCrashedCondition(ManagerAgentWrapper *agentWrapper)
{
	int errorCount;
	AdbDoctorError *firstError;
	AdbDoctorError *lastError;
	int *errornos;
	int nErrornos;
	bool res;
	if (agentWrapper->errors->nerrors < 1)
	{
		res = false;
	}
	else
	{
		nErrornos = 3;
		errornos = palloc(sizeof(int) * nErrornos);
		errornos[0] = (int)AGENT_ERROR_CONNECT_FAIL;
		errornos[1] = (int)AGENT_ERROR_MESSAGE_FAIL;
		errornos[2] = (int)AGENT_ERROR_HEARTBEAT_TIMEOUT;
		firstError = findFirstAdbDoctorError(agentWrapper->errors,
											 errornos, nErrornos);
		if (!firstError)
		{
			res = false;
		}
		else
		{
			lastError = findLastAdbDoctorError(agentWrapper->errors,
											   errornos, nErrornos);
			if (TimestampDifferenceExceeds(firstError->time,
										   lastError->time,
										   hostConfiguration->agentdeadlineMs))
			{
				/* reached dead line, agent crashed */
				res = true;
			}
			else
			{
				errorCount = countAdbDoctorErrorRecorder(agentWrapper->errors,
														 errornos, nErrornos);
				res = errorCount >= hostConfiguration->connectionErrorNumMax;
			}
		}
		pfree(errornos);
	}
	return res;
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

	agent = ma_connect_noblock(agentWrapper->mgrHost->hostaddr,
							   agentWrapper->mgrHost->form.hostagentport);
	agentWrapper->agent = agent;
	connected = ma_isconnected(agent);
	if (connected)
	{
		/* Connect agents in a non-blocking manner, require further event waiting. */
		agentWrapper->wed.fun = OnAgentConnectionEvent;
		addAgentWaitEventToSet(agentWrapper, WL_SOCKET_MASK);
		toConnectionStatusConnecting(agentWrapper);
	}
	else
	{
		/* Add an error to help determine the true status of the agent. */
		toConnectionStatusBad(agentWrapper, AGENT_ERROR_CONNECT_FAIL);
	}
	return connected;
}

/* 
 * Remove the WaitEvent from the WaitEventSet if setted.
 * Close the connection to the agent if connected.
 */
static void stopMonitorAgent(ManagerAgentWrapper *agentWrapper)
{
	agentWrapper->wed.fun = NULL;
	/* Ensure to remove WaitEvent from the WaitEventSet. */
	removeAgentWaitEvent(agentWrapper);

	/* Ensure to disconnect from the agent. */
	if (agentWrapper->agent)
	{
		ma_close(agentWrapper->agent);
		agentWrapper->agent = NULL;
	}
}

/**
 * Allow restart agent means that the difference between "restartTime" and "currentTime"
 * exceeds "restartDelay", and the configuration in table mgr_host shows allow to do it.
 */
static bool allowRestartAgent(ManagerAgentWrapper *agentWrapper)
{
	int ret;
	bool allowcure;
	MgrHostWrapper *hostDataInDB;
	MemoryContext oldContext;
	MemoryContext spiContext;

	/* We don't want to restart too often.  */
	if (!beyondRestartDelay(agentWrapper))
	{
		ereport(DEBUG1,
				(errmsg("%s:%s, restart agent too often",
						NameStr(agentWrapper->mgrHost->form.hostname),
						agentWrapper->mgrHost->hostaddr)));
		return false;
	}

	oldContext = CurrentMemoryContext;

	/* Double check to ensure that the host information in the mgr database has not changed. */
	SPI_CONNECT_TRANSACTIONAL_START(ret, true);
	spiContext = CurrentMemoryContext;
	MemoryContextSwitchTo(oldContext);
	hostDataInDB = selectMgrHostByOid(agentWrapper->mgrHost->oid, spiContext);
	SPI_FINISH_TRANSACTIONAL_COMMIT();
	if (hostDataInDB == NULL)
	{
		ereport(ERROR,
				(errmsg("%s:%s, oid:%u not exists in the table.",
						NameStr(agentWrapper->mgrHost->form.hostname),
						agentWrapper->mgrHost->hostaddr,
						agentWrapper->mgrHost->oid)));
	}
	allowcure = hostDataInDB->form.allowcure;
	if (!allowcure)
	{
		ereport(WARNING,
				(errmsg("%s, cure agent not allowed",
						MyBgworkerEntry->bgw_name)));
		pfreeMgrHostWrapper(hostDataInDB);
		resetHostMonitor();
	}
	if (!isIdenticalDoctorMgrHost(agentWrapper->mgrHost, hostDataInDB))
	{
		ereport(WARNING,
				(errmsg("%s:%s, oid:%u has changed in the table.",
						NameStr(agentWrapper->mgrHost->form.hostname),
						agentWrapper->mgrHost->hostaddr,
						agentWrapper->mgrHost->oid)));
		pfreeMgrHostWrapper(hostDataInDB);
		resetHostMonitor();
	}
	pfreeMgrHostWrapper(hostDataInDB);
	return allowcure;
}

static bool restartAgent(ManagerAgentWrapper *agentWrapper, RestartAgentMode mode)
{
	if (allowRestartAgent(agentWrapper))
	{
		agentWrapper->restartTime = GetCurrentTimestamp();
		/* Modify the value of the variable that controls the restart frequency. */
		nextRestartFactor(agentWrapper);

		if (mode == RESTART_AGENT_MODE_SSH)
		{
			return restartAgentBySSH(agentWrapper);
		}
		else if (mode == RESTART_AGENT_MODE_SEND_MESSAGE)
		{
			return restartAgentByCmdMessage(agentWrapper);
		}
		else
		{
			ereport(LOG,
					(errmsg("%s:%s, restart forbidden, unknow restart mode:%d.",
							NameStr(agentWrapper->mgrHost->form.hostname),
							agentWrapper->mgrHost->hostaddr,
							mode)));
			return false;
		}
	}
	else
	{
		ereport(DEBUG1,
				(errmsg("%s:%s, restart too often, wait for a while.",
						NameStr(agentWrapper->mgrHost->form.hostname),
						agentWrapper->mgrHost->hostaddr)));
		return false;
	}
}

static bool restartAgentBySSH(ManagerAgentWrapper *agentWrapper)
{
	bool done;
	GetAgentCmdRst *cmdRst;

	ereport(LOG,
			(errmsg("%s:%s, restart agent by ssh begin.",
					NameStr(agentWrapper->mgrHost->form.hostname),
					agentWrapper->mgrHost->hostaddr)));
	cmdRst = mgr_start_agent_execute(&agentWrapper->mgrHost->form,
									 agentWrapper->mgrHost->hostaddr,
									 agentWrapper->mgrHost->hostadbhome,
									 NULL);
	/* The previous operations will take a while, 
	 * we need precise time to prevent repeated restarts. */
	agentWrapper->restartTime = GetCurrentTimestamp();
	if (cmdRst->ret == 0)
	{
		done = true;
		ereport(LOG,
				(errmsg("%s:%s, restart agent by ssh successfully.",
						NameStr(agentWrapper->mgrHost->form.hostname),
						agentWrapper->mgrHost->hostaddr)));
	}
	else
	{
		done = false;
		ereport(LOG,
				(errmsg("%s:%s, restart agent by ssh failed:%s.",
						NameStr(agentWrapper->mgrHost->form.hostname),
						agentWrapper->mgrHost->hostaddr,
						cmdRst->description.data)));
	}
	pfree(cmdRst->description.data);
	pfree(cmdRst);
	return done;
}

static bool restartAgentByCmdMessage(ManagerAgentWrapper *agentWrapper)
{
	bool done;
	done = sendStopAgentMessage(agentWrapper) &&
		   sendResetAgentMessage(agentWrapper);
	/* The previous operations will take a while, we need precise
	 * time to prevent repeated restarts. */
	agentWrapper->restartTime = GetCurrentTimestamp();
	return done;
}

static bool sendHeartbeatMessage(ManagerAgentWrapper *agentWrapper)
{
	bool done;
	StringInfoData buf;
	/* send idle message, do it as heartbeat message. */
	ma_beginmessage(&buf, AGT_MSG_IDLE);
	ma_endmessage(&buf, agentWrapper->agent);
	done = ma_flush(agentWrapper->agent, false);
	if (done)
	{
		ereport(DEBUG1,
				(errmsg("%s:%s, sent message idle",
						NameStr(agentWrapper->mgrHost->form.hostname),
						agentWrapper->mgrHost->hostaddr)));
	}
	else
	{
		ereport(LOG,
				(errmsg("%s:%s, sent message idle failed.",
						NameStr(agentWrapper->mgrHost->form.hostname),
						agentWrapper->mgrHost->hostaddr)));
	}
	return done;
}

static bool sendStopAgentMessage(ManagerAgentWrapper *agentWrapper)
{
	bool done;
	StringInfoData buf;
	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, AGT_CMD_STOP_AGENT);
	ma_sendstring(&buf, "stop agent");
	ma_endmessage(&buf, agentWrapper->agent);
	done = ma_flush(agentWrapper->agent, false);
	if (done)
	{
		ereport(DEBUG1,
				(errmsg("%s:%s, sent message stop agent",
						NameStr(agentWrapper->mgrHost->form.hostname),
						agentWrapper->mgrHost->hostaddr)));
	}
	else
	{
		ereport(LOG,
				(errmsg("%s:%s, sent message stop agent failed.",
						NameStr(agentWrapper->mgrHost->form.hostname),
						agentWrapper->mgrHost->hostaddr)));
	}
	return done;
}

static bool sendResetAgentMessage(ManagerAgentWrapper *agentWrapper)
{
	bool done;
	StringInfoData buf;
	/* send idle message, do it as heartbeat message. */
	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, AGT_CMD_RESET_AGENT);
	ma_sendstring(&buf, "close socket then reset");
	ma_endmessage(&buf, agentWrapper->agent);
	done = ma_flush(agentWrapper->agent, false);
	if (done)
	{
		ereport(DEBUG1,
				(errmsg("%s:%s, sent message reset agent",
						NameStr(agentWrapper->mgrHost->form.hostname),
						agentWrapper->mgrHost->hostaddr)));
	}
	else
	{
		ereport(LOG,
				(errmsg("%s:%s, sent message reset agent failed.",
						NameStr(agentWrapper->mgrHost->form.hostname),
						agentWrapper->mgrHost->hostaddr)));
	}
	return done;
}

static bool beyondRestartDelay(ManagerAgentWrapper *agentWrapper)
{
	long realRestartDelayMs;
	if (shouldResetRestartFactor(agentWrapper))
	{
		resetAdbDoctorBounceNum(agentWrapper->restartFactor);
		return true;
	}
	else
	{
		/* We don't want to restart too often.  */
		realRestartDelayMs = hostConfiguration->restartDelayMs *
							 (1 << agentWrapper->restartFactor->num);
		return TimestampDifferenceExceeds(agentWrapper->restartTime,
										  GetCurrentTimestamp(),
										  realRestartDelayMs);
	}
}

static bool shouldResetRestartFactor(ManagerAgentWrapper *agentWrapper)
{
	int resetDelayMs;
	/* Reset to default value for more than 2 maximum delay cycles */
	resetDelayMs = hostConfiguration->restartDelayMs *
				   (1 << agentWrapper->restartFactor->max);
	resetDelayMs = resetDelayMs * 2;
	return TimestampDifferenceExceeds(agentWrapper->restartTime,
									  GetCurrentTimestamp(),
									  resetDelayMs);
}

/**
 * Use variable restartFactor to control the next restart time,
 */
static void nextRestartFactor(ManagerAgentWrapper *agentWrapper)
{
	if (shouldResetRestartFactor(agentWrapper))
	{
		resetAdbDoctorBounceNum(agentWrapper->restartFactor);
	}
	else
	{
		nextAdbDoctorBounceNum(agentWrapper->restartFactor);
	}
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
static void addAgentWaitEventToSet(ManagerAgentWrapper *agentWrapper, uint32 events)
{
	int pos;

	/* Sentinel, preventing being added to WaitEventSet repeatedly */
	if (agentWrapper->event_pos >= 0)
		return;
	pos = AddWaitEventToSet(agentWaitEventSet,
							events,
							ma_getsock(agentWrapper->agent),
							NULL,
							agentWrapper);
	agentWrapper->event_pos = pos;
}

static void ModifyAgentWaitEventSet(ManagerAgentWrapper *agentWrapper, uint32 events)
{
	if (agentWrapper->event_pos < 0)
		ereport(ERROR,
				(errmsg("%s:%s, modify WaitEventSet error",
						NameStr(agentWrapper->mgrHost->form.hostname),
						agentWrapper->mgrHost->hostaddr)));
	ModifyWaitEvent(agentWaitEventSet, agentWrapper->event_pos, events, NULL);
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
	AdbDoctorConf *confInLocal;
	dlist_head freshMgrHosts = DLIST_STATIC_INIT(freshMgrHosts);

	ResetLatch(&MyProc->procLatch);
	CHECK_FOR_INTERRUPTS();
	if (gotSigusr1)
	{
		gotSigusr1 = false;

		confInLocal = copyAdbDoctorConfFromShm(confShm);
		pfree(hostConfiguration);
		hostConfiguration = newHostConfiguration(confInLocal);
		pfree(confInLocal);

		ereport(LOG,
				(errmsg("%s Refresh configuration completed",
						MyBgworkerEntry->bgw_name)));

		getCheckMgrHostsForHostDoctor(&freshMgrHosts);
		if (isIdenticalDoctorMgrHosts(&freshMgrHosts, cachedMgrHosts))
		{
			pfreeMgrHostWrapperList(&freshMgrHosts, NULL);
		}
		else
		{
			pfreeMgrHostWrapperList(&freshMgrHosts, NULL);
			resetHostMonitor();
		}
	}
}

static void OnPostmasterDeathEvent(WaitEvent *event)
{
	exit(1);
}

static void OnAgentConnectionEvent(WaitEvent *event)
{
	ManagerAgentWrapper *agentWrapper;
	agentWrapper = (ManagerAgentWrapper *)event->user_data;
	ereport(DEBUG1,
			(errmsg("%s:%s, agent connection event",
					NameStr(agentWrapper->mgrHost->form.hostname),
					agentWrapper->mgrHost->hostaddr)));
	/* This not indicate that the agent connection is good, 
	 * should read from socket to determine the connection status,
	 * so listen the readable event then read from socket. */
	agentWrapper->wed.fun = OnAgentMessageEvent;
	ModifyAgentWaitEventSet(agentWrapper, WL_SOCKET_READABLE);
}

/*
 * If received heartbeat message from agent, determine the running status
 * of that agent is normal,  when received error determine it have been crashed. 
 */
static void OnAgentMessageEvent(WaitEvent *event)
{
	ManagerAgentWrapper *agentWrapper;
	StringInfoData recvbuf;
	char msg_type;
	agentWrapper = (ManagerAgentWrapper *)event->user_data;
	initStringInfo(&recvbuf);
	for (;;)
	{
		resetStringInfo(&recvbuf);
		msg_type = ma_get_message(agentWrapper->agent, &recvbuf);
		if (msg_type == AGT_MSG_IDLE)
		{
			ereport(DEBUG1,
					(errmsg("%s:%s, receive message idle",
							NameStr(agentWrapper->mgrHost->form.hostname),
							agentWrapper->mgrHost->hostaddr)));
			toConnectionStatusSucceeded(agentWrapper);
			break;
		}
		else if (msg_type == '\0')
		{
			/* 
             * Running here means that the agent has been disconnected or other fatal error.
             * If doesn't stop monitor agent, then we will continue to receive this event.
             * We will run back here immediately, just like an infinite loop. So must stop
             * monitor this agent, wait for next delay time to reconnect to it.
             */
			toConnectionStatusBad(agentWrapper, AGENT_ERROR_MESSAGE_FAIL);
			break;
		}
		else if (msg_type == AGT_MSG_ERROR)
		{
			/* ignore error message */
			ereport(LOG,
					(errmsg("%s:%s, receive error message:%s",
							NameStr(agentWrapper->mgrHost->form.hostname),
							agentWrapper->mgrHost->hostaddr,
							ma_get_err_info(&recvbuf, AGT_MSG_RESULT))));
			break;
		}
		else if (msg_type == AGT_MSG_NOTICE)
		{
			/* ignore notice message */
		}
		else if (msg_type == AGT_MSG_RESULT)
		{
			ereport(LOG,
					(errmsg("%s:%s, receive message:%s",
							NameStr(agentWrapper->mgrHost->form.hostname),
							agentWrapper->mgrHost->hostaddr,
							recvbuf.data)));
			toConnectionStatusSucceeded(agentWrapper);
			break;
		}
		else if (msg_type == AGT_MSG_COMMAND)
		{
			ereport(LOG,
					(errmsg("%s:%s, receive message:%s",
							NameStr(agentWrapper->mgrHost->form.hostname),
							agentWrapper->mgrHost->hostaddr,
							recvbuf.data)));
			toConnectionStatusSucceeded(agentWrapper);
			break;
		}
		else if (msg_type == AGT_MSG_EXIT)
		{
			ereport(LOG,
					(errmsg("%s:%s, receive message:%s",
							NameStr(agentWrapper->mgrHost->form.hostname),
							agentWrapper->mgrHost->hostaddr,
							recvbuf.data)));
			toConnectionStatusSucceeded(agentWrapper);
			/* Do not close this connection.use this connection to start agent main process. */
			if(restartAgent(agentWrapper, RESTART_AGENT_MODE_SEND_MESSAGE))
			{
				resetAdbDoctorBounceNum(agentWrapper->restartFactor);
			}
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
 * the minimum and maximum of these various variables.
 */
static HostConfiguration *newHostConfiguration(AdbDoctorConf *conf)
{
	HostConfiguration *hc;
	long deadlineMs;

	checkAdbDoctorConf(conf);

	hc = palloc0(sizeof(HostConfiguration));

	deadlineMs = conf->agentdeadline * 1000L;
	hc->agentdeadlineMs = deadlineMs;
	hc->waitEventTimeoutMs = LIMIT_VALUE_RANGE(500, 10000, floor(deadlineMs / 30));
	hc->connectTimeoutMs = LIMIT_VALUE_RANGE(conf->agent_connect_timeout_ms_min,
											 conf->agent_connect_timeout_ms_max,
											 floor(deadlineMs / 10));
	hc->reconnectDelayMs = LIMIT_VALUE_RANGE(conf->agent_reconnect_delay_ms_min,
											 conf->agent_reconnect_delay_ms_max,
											 floor(deadlineMs /
												   conf->agent_connection_error_num_max));
	hc->heartbeatTimoutMs = LIMIT_VALUE_RANGE(conf->agent_heartbeat_timeout_ms_min,
											  conf->agent_heartbeat_timeout_ms_max,
											  floor(deadlineMs / 5));
	hc->heartbeatIntervalMs = LIMIT_VALUE_RANGE(conf->agent_heartbeat_interval_ms_min,
												conf->agent_heartbeat_interval_ms_max,
												floor(deadlineMs / 5));
	hc->restartDelayMs = LIMIT_VALUE_RANGE(conf->agent_restart_delay_ms_min,
										   conf->agent_restart_delay_ms_max,
										   floor(deadlineMs / 2));
	hc->connectionErrorNumMax = conf->agent_connection_error_num_max;
	ereport(DEBUG1,
			(errmsg("%s configuration: "
					"agentdeadlineMs:%ld, waitEventTimeoutMs:%ld, "
					"connectTimeoutMs:%ld, reconnectDelayMs:%ld, "
					"heartbeatTimoutMs:%ld, heartbeatIntervalMs:%ld, "
					"restartDelayMs:%ld, connectionErrorNumMax:%d",
					MyBgworkerEntry->bgw_name,
					hc->agentdeadlineMs, hc->waitEventTimeoutMs,
					hc->connectTimeoutMs, hc->reconnectDelayMs,
					hc->heartbeatTimoutMs, hc->heartbeatIntervalMs,
					hc->restartDelayMs, hc->connectionErrorNumMax)));
	return hc;
}

static void getCheckMgrHostsForHostDoctor(dlist_head *hosts)
{
	MemoryContext oldContext;
	MemoryContext spiContext;
	int ret;

	oldContext = CurrentMemoryContext;
	SPI_CONNECT_TRANSACTIONAL_START(ret, true);
	spiContext = CurrentMemoryContext;
	MemoryContextSwitchTo(oldContext);
	selectMgrHostsForHostDoctor(spiContext, hosts);
	SPI_FINISH_TRANSACTIONAL_COMMIT();
	if (dlist_is_empty(hosts))
	{
		ereport(ERROR,
				(errmsg("%s There is no host data to monitor",
						MyBgworkerEntry->bgw_name)));
	}
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

static void pfreeManagerAgentWrapper(ManagerAgentWrapper *agentWrapper)
{
	if (agentWrapper)
	{
		if (agentWrapper->agent)
		{
			ma_close(agentWrapper->agent);
			agentWrapper->agent = NULL;
		}
		pfreeMgrHostWrapper(agentWrapper->mgrHost);
		agentWrapper->mgrHost = NULL;
		pfreeAdbDoctorBounceNum(agentWrapper->restartFactor);
		agentWrapper->restartFactor = NULL;
		pfreeAdbDoctorErrorRecorder(agentWrapper->errors);
		agentWrapper->errors = NULL;
		pfree(agentWrapper);
		agentWrapper = NULL;
	}
}