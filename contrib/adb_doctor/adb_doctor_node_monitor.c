/*--------------------------------------------------------------------------
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
#include "storage/spin.h"
#include "executor/spi.h"
#include "utils/resowner.h"
#include "utils/builtins.h"
#include "../../src/interfaces/libpq/libpq-fe.h"
#include "../../src/interfaces/libpq/libpq-int.h"
#include "mgr/mgr_agent.h"
#include "mgr/mgr_msg_type.h"
#include "mgr/mgr_cmds.h"
#include "adb_doctor.h"
#include "adb_doctor_utils.h"

/* 
 * Benchmark of the time interval, The following elements 
 * based on deadlineMs, but have min and max value limit.
 * For more information, please refer to function newNodeConfiguration(). 
 */
typedef struct NodeConfiguration
{
	long deadlineMs;
	long waitEventTimeoutMs;
	long connectTimeoutMs;
	long reconnectDelayMs;
	long queryTimoutMs;
	long queryIntervalMs;
	long restartDelayMs;
	long holdConnectionMs;
	int restartCrashedMaster;
	long restartMasterTimeoutMs;
	long shutdownTimeoutMs;
	int connectionErrorNumMax;

} NodeConfiguration;

typedef enum NodeError
{
	NODE_ERROR_CONNECT_TIMEDOUT = 1,
	NODE_ERROR_CONNECT_FAIL,
	/* If occurred increase MonitorNodeInfo->nQueryfails. */
	NODE_ERROR_QUERY_TIMEDOUT,
	/* If occurred, do not record it, handle this error instantly. */
	NODE_RUNNING_DANGEROUSLY,
	NODE_CANNOT_CONNECT_STARTUP,
	NODE_CANNOT_CONNECT_SHUTDOWN,
	NODE_CANNOT_CONNECT_RECOVERY,
	NODE_CANNOT_CONNECT_NOW,
	NODE_CANNOT_CONNECT_TOOMANY
} NodeError;

/* Do not change the value of NodeError */
const static char *NODE_ERROR_MSG[] =
	{"CONNECT_TIMEDOUT", "CONNECT_FAIL", "QUERY_TIMEDOUT",
	 "RUNNING_DANGEROUSLY", "CANNOT_CONNECT_STARTUP",
	 "CANNOT_CONNECT_SHUTDOWN", "CANNOT_CONNECT_RECOVERY",
	 "CANNOT_CONNECT_NOW", "CANNOT_CONNECT_TOOMANY"};

typedef enum NodeConnnectionStatus
{
	NODE_CONNNECTION_STATUS_CONNECTING = 1,
	NODE_CONNNECTION_STATUS_SUCCEEDED,
	NODE_CONNNECTION_STATUS_QUERYING,
	NODE_CONNNECTION_STATUS_BAD
} NodeConnnectionStatus;

typedef enum NodeRunningStatus
{
	NODE_RUNNING_STATUS_NORMAL = 1,
	NODE_RUNNING_STATUS_CRASHED,
	NODE_RUNNING_STATUS_PENDING
} NodeRunningStatus;

typedef enum NodeQuerySqlType
{
	SQL_TYPE_PG_IS_IN_RECOVERY
} NodeQuerySqlType;

typedef struct MonitorNodeInfo
{
	PGconn *conn;
	AdbMgrNodeWrapper *node;
	NodeConnnectionStatus connnectionStatus;
	NodeRunningStatus runningStatus;
	bool (*queryHandler)(struct MonitorNodeInfo *nodeInfo,
						 PGresult *pgResult);
	int waitEvents;
	int occurredEvents;
	int nRestarts;
	TimestampTz connectTime;
	TimestampTz queryTime;
	TimestampTz activeTime;
	TimestampTz crashedTime;
	TimestampTz restartTime;
	TimestampTz shutdownTime;
	TimestampTz recoveryTime;
	/* control the time interval when query timed out occurred. */
	int nQueryfails;
	/* control the time interval when restart node. */
	AdbDoctorBounceNum *restartFactor;
	AdbDoctorErrorRecorder *connectionErrors;
} MonitorNodeInfo;

static void nodeMonitorMainLoop(MonitorNodeInfo *nodeInfo);

static void examineAdbDoctorConf(void);
static void examineNodeStatus(MonitorNodeInfo *nodeInfo);

static void handleConnectionStatusConnecting(MonitorNodeInfo *nodeInfo);
static void handleConnectionStatusSucceeded(MonitorNodeInfo *nodeInfo);
static void handleConnectionStatusQuerying(MonitorNodeInfo *nodeInfo);
static void handleConnectionStatusBad(MonitorNodeInfo *nodeInfo);
static void toConnectionStatusConnecting(MonitorNodeInfo *nodeInfo);
static void toConnectionStatusSucceeded(MonitorNodeInfo *nodeInfo);
static void toConnectionStatusQuerying(MonitorNodeInfo *nodeInfo);
static void toConnectionStatusBad(MonitorNodeInfo *nodeInfo);

static void handleRunningStatusNormal(MonitorNodeInfo *nodeInfo);
static void handleRunningStatusCrashed(MonitorNodeInfo *nodeInfo);
static void handleRunningStatusPending(MonitorNodeInfo *nodeInfo);
static void toRunningStatusNormal(MonitorNodeInfo *nodeInfo);
static void toRunningStatusCrashed(MonitorNodeInfo *nodeInfo);
static void toRunningStatusPending(MonitorNodeInfo *nodeInfo);

static void handleNodeCrashed(MonitorNodeInfo *nodeInfo);
static void handleNodeDangerous(MonitorNodeInfo *nodeInfo);
static bool tryRestartNode(MonitorNodeInfo *nodeInfo);
static void nodeWaitSwitch(MonitorNodeInfo *nodeInfo);
static bool startupNode(MonitorNodeInfo *nodeInfo);
static bool shutdownNode(MonitorNodeInfo *nodeInfo, char *shutdownMode);

static void startConnection(MonitorNodeInfo *nodeInfo);
static void resetConnection(MonitorNodeInfo *nodeInfo);
static void closeConnection(MonitorNodeInfo *nodeInfo);
static void resetMonitor(MonitorNodeInfo *nodeInfo);
static bool startQuery(MonitorNodeInfo *nodeInfo,
					   NodeQuerySqlType sqlType);
static bool cancelQuery(MonitorNodeInfo *nodeInfo);
static bool PQflushAction(MonitorNodeInfo *nodeInfo);
static void PQgetResultUntilNull(PGconn *conn);
static bool pg_is_in_recovery_handler(MonitorNodeInfo *nodeInfo,
									  PGresult *pgResult);
static bool simple_print_query_handler(MonitorNodeInfo *nodeInfo,
									   PGresult *pgResult);
static bool PQgetResultAction(MonitorNodeInfo *nodeInfo);

static bool isConnectTimedOut(MonitorNodeInfo *nodeInfo);
static bool isQueryTimedOut(MonitorNodeInfo *nodeInfo);
static bool isMasterRestartTimedout(MonitorNodeInfo *nodeInfo);
static bool isNodeRunningNormally(MonitorNodeInfo *nodeInfo);
static bool isShutdownTimedout(MonitorNodeInfo *nodeInfo);
static bool isShouldResetConnection(MonitorNodeInfo *nodeInfo);
static bool beyondReconnectDelay(MonitorNodeInfo *nodeInfo);
static bool beyondQueryInterval(MonitorNodeInfo *nodeInfo);
static bool beyondRestartDelay(MonitorNodeInfo *nodeInfo);
static void nextRestartFactor(MonitorNodeInfo *nodeInfo);

static void occurredError(MonitorNodeInfo *nodeInfo, NodeError error);
static bool reachedCrashedCondition(MonitorNodeInfo *nodeInfo);
static int getLastNodeErrorno(MonitorNodeInfo *nodeInfo);

static AdbMgrHostWrapper *getAdbMgrHostData(AdbMgrNodeWrapper *node);
static void checkAndReplaceAdbMgrNode(AdbMgrNodeWrapper **nodeP);
static void checkAndUpdateCurestatus(AdbMgrNodeWrapper *node,
									 char *newCurestatus);
static void checkCurestatus(AdbMgrNodeWrapper *node);
static bool isMasterOrSlave(char nodetype);

static void attachNodeDataShm(Datum main_arg,
							  AdbDoctorNodeData **dataP);
static void handleSigterm(SIGNAL_ARGS);
static void handleSigusr1(SIGNAL_ARGS);

static NodeConfiguration *newNodeConfiguration(AdbDoctorConf *conf);
static MonitorNodeInfo *newMonitorNodeInfo(AdbMgrNodeWrapper *node);
static void pfreeMonitorNodeInfo(MonitorNodeInfo *nodeInfo);

static AdbDoctorConfShm *confShm;
static NodeConfiguration *nodeConfiguration;

static volatile sig_atomic_t gotSigterm = false;
static volatile sig_atomic_t gotSigusr1 = false;

void adbDoctorNodeMonitorMain(Datum main_arg)
{
	AdbDoctorNodeData *data;
	AdbDoctorConf *confInLocal;
	MonitorNodeInfo *nodeInfo = NULL;

	pqsignal(SIGTERM, handleSigterm);
	pqsignal(SIGUSR1, handleSigusr1);

	BackgroundWorkerUnblockSignals();

	PG_TRY();
	{
		BackgroundWorkerInitializeConnection(ADBMGR_DBNAME, NULL, 0);

		attachNodeDataShm(main_arg, &data);
		notifyAdbDoctorRegistrant();
		ereport(LOG,
				(errmsg("%s started",
						MyBgworkerEntry->bgw_name)));

		confShm = attachAdbDoctorConfShm(data->header.commonShmHandle,
										 MyBgworkerEntry->bgw_name);
		confInLocal = copyAdbDoctorConfFromShm(confShm);
		nodeConfiguration = newNodeConfiguration(confInLocal);
		pfree(confInLocal);

		nodeInfo = newMonitorNodeInfo(data->wrapper);

		/* This is the main loop */
		nodeMonitorMainLoop(nodeInfo);

		pfree(nodeConfiguration);
		pfreeMonitorNodeInfo(nodeInfo);
		pfreeAdbDoctorConfShm(confShm);
	}
	PG_CATCH();
	{
		pfree(nodeConfiguration);
		pfreeMonitorNodeInfo(nodeInfo);
		pfreeAdbDoctorConfShm(confShm);
		PG_RE_THROW();
	}
	PG_END_TRY();
	proc_exit(1);
}

static void nodeMonitorMainLoop(MonitorNodeInfo *nodeInfo)
{
	int rc;

	startConnection(nodeInfo);

	while (!gotSigterm)
	{
		CHECK_FOR_INTERRUPTS();
		rc = WaitLatchOrSocket(MyLatch,
							   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH |
								   nodeInfo->waitEvents,
							   PQsocket(nodeInfo->conn),
							   nodeConfiguration->waitEventTimeoutMs,
							   PG_WAIT_CLIENT);
		/* Reset the latch, bail out if postmaster died. */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
		/* Interrupted? */
		if (rc & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();
		}
		examineAdbDoctorConf();

		nodeInfo->occurredEvents = rc;
		nodeInfo->waitEvents = 0;

		examineNodeStatus(nodeInfo);
	}
}

static void examineAdbDoctorConf()
{
	AdbDoctorConf *confInLocal;
	if (gotSigusr1)
	{
		gotSigusr1 = false;

		confInLocal = copyAdbDoctorConfFromShm(confShm);
		pfree(nodeConfiguration);
		nodeConfiguration = newNodeConfiguration(confInLocal);
		pfree(confInLocal);

		ereport(LOG,
				(errmsg("%s, Refresh configuration completed",
						MyBgworkerEntry->bgw_name)));
	}
}

static void examineNodeStatus(MonitorNodeInfo *nodeInfo)
{
	NodeConnnectionStatus connnectionStatus;
	NodeRunningStatus runningStatus;

	connnectionStatus = nodeInfo->connnectionStatus;
	if (connnectionStatus == NODE_CONNNECTION_STATUS_CONNECTING)
	{
		handleConnectionStatusConnecting(nodeInfo);
	}
	else if (connnectionStatus == NODE_CONNNECTION_STATUS_SUCCEEDED)
	{
		handleConnectionStatusSucceeded(nodeInfo);
	}
	else if (connnectionStatus == NODE_CONNNECTION_STATUS_QUERYING)
	{
		handleConnectionStatusQuerying(nodeInfo);
	}
	else if (connnectionStatus == NODE_CONNNECTION_STATUS_BAD)
	{
		handleConnectionStatusBad(nodeInfo);
	}
	else
	{
		ereport(ERROR,
				(errmsg("Unexpected NodeConnnectionStatus:%d",
						connnectionStatus)));
	}

	runningStatus = nodeInfo->runningStatus;
	if (runningStatus == NODE_RUNNING_STATUS_NORMAL)
	{
		handleRunningStatusNormal(nodeInfo);
	}
	else if (runningStatus == NODE_RUNNING_STATUS_CRASHED)
	{
		handleRunningStatusCrashed(nodeInfo);
	}
	else if (runningStatus == NODE_RUNNING_STATUS_PENDING)
	{
		handleRunningStatusPending(nodeInfo);
	}
	else
	{
		ereport(ERROR,
				(errmsg("Unexpected NodeRunningStatus:%d",
						connnectionStatus)));
	}
}

static void handleConnectionStatusConnecting(MonitorNodeInfo *nodeInfo)
{
	PostgresPollingStatusType pollType;

	/* Loop thus: If PQconnectPoll(conn) last returned PGRES_POLLING_READING, 
     * wait until the socket is ready to read (as indicated by select(), poll(),
     * or similar system function). Then call PQconnectPoll(conn) again. Conversely,
     * if PQconnectPoll(conn) last returned PGRES_POLLING_WRITING, wait until the 
     * socket is ready to write, then call PQconnectPoll(conn) again. On the first
     * iteration, i.e. if you have yet to (have not) call PQconnectPoll, behave as if 
     * it last returned PGRES_POLLING_WRITING. Continue this loop until PQconnectPoll(conn) 
     * returns PGRES_POLLING_FAILED, indicating the connection procedure has failed,
     * or PGRES_POLLING_OK, indicating the connection has been successfully made. */
	pollType = PQconnectPoll(nodeInfo->conn);
	if (pollType == PGRES_POLLING_FAILED)
	{
		if (strcmp(nodeInfo->conn->last_sqlstate, "57P03") == 0)
		{
			/* See postmaster.c ERRCODE_CANNOT_CONNECT_NOW */
			if (strstr(PQerrorMessage(nodeInfo->conn), "starting up"))
			{
				occurredError(nodeInfo, NODE_CANNOT_CONNECT_STARTUP);
			}
			else if (strstr(PQerrorMessage(nodeInfo->conn), "shutting down"))
			{
				occurredError(nodeInfo, NODE_CANNOT_CONNECT_SHUTDOWN);
			}
			else if (strstr(PQerrorMessage(nodeInfo->conn), "recovery mode"))
			{
				occurredError(nodeInfo, NODE_CANNOT_CONNECT_RECOVERY);
			}
			else
			{
				/* the message may be localized, so can not recognize it. */
				occurredError(nodeInfo, NODE_CANNOT_CONNECT_NOW);
			}
		}
		else if (strcmp(nodeInfo->conn->last_sqlstate, "53300") == 0)
		{
			/* ERRCODE_TOO_MANY_CONNECTIONS */
			occurredError(nodeInfo, NODE_CANNOT_CONNECT_TOOMANY);
		}
		else
		{
			occurredError(nodeInfo, NODE_ERROR_CONNECT_FAIL);
		}
		return;
	}
	else if (pollType == PGRES_POLLING_READING)
	{
		nodeInfo->waitEvents |= WL_SOCKET_READABLE;
	}
	else if (pollType == PGRES_POLLING_WRITING)
	{
		nodeInfo->waitEvents |= WL_SOCKET_WRITEABLE;
	}
	else if (pollType == PGRES_POLLING_OK)
	{
		toConnectionStatusSucceeded(nodeInfo);
		/* query immediately to determine the node status. */
		startQuery(nodeInfo, SQL_TYPE_PG_IS_IN_RECOVERY);
		return;
	}
	else
	{
		ereport(ERROR,
				(errmsg("Unexpected PostgresPollingStatusType:%d",
						pollType)));
	}
	if (isConnectTimedOut(nodeInfo))
	{
		occurredError(nodeInfo, NODE_ERROR_CONNECT_TIMEDOUT);
	}
}

static void handleConnectionStatusSucceeded(MonitorNodeInfo *nodeInfo)
{
	/* may be connection closed */
	if (nodeInfo->occurredEvents & WL_SOCKET_READABLE)
	{
		if (PQconsumeInput(nodeInfo->conn) != 1)
		{
			occurredError(nodeInfo, NODE_ERROR_CONNECT_FAIL);
			return;
		}
	}
	if (isShouldResetConnection(nodeInfo))
	{
		resetConnection(nodeInfo);
	}
	else
	{
		startQuery(nodeInfo, SQL_TYPE_PG_IS_IN_RECOVERY);
		nodeInfo->waitEvents |= WL_SOCKET_READABLE;
	}
}

static void handleConnectionStatusQuerying(MonitorNodeInfo *nodeInfo)
{
	/* After sending any command or data on a nonblocking connection, call PQflush.
	 * If it returns 1, wait for the socket to become read- or write-ready.
	 * If it becomes write-ready, call PQflush again. If it becomes read-ready,
	 * call PQconsumeInput, then call PQflush again. Repeat until PQflush returns 0.
	 * (It is necessary to check for read-ready and drain the input with PQconsumeInput,
	 * because the server can block trying to send us data, e.g. NOTICE messages,
	 * and won't read our data until we read its.) Once PQflush returns 0,
	 * wait for the socket to be read-ready and then read the response */
	if (nodeInfo->occurredEvents & WL_SOCKET_WRITEABLE)
	{
		if (!PQflushAction(nodeInfo))
		{
			occurredError(nodeInfo, NODE_ERROR_CONNECT_FAIL);
			return;
		}
	}
	if (nodeInfo->occurredEvents & WL_SOCKET_READABLE)
	{
		if (PQconsumeInput(nodeInfo->conn) != 1)
		{
			occurredError(nodeInfo, NODE_ERROR_CONNECT_FAIL);
			return;
		}
		else
		{
			nodeInfo->waitEvents |= WL_SOCKET_READABLE;
		}
	}
	if (PQisBusy(nodeInfo->conn))
	{
		/* Returns 1 if a command is busy, that is, 
			 * PQgetResult would block waiting for input. */
		nodeInfo->waitEvents |= WL_SOCKET_READABLE;
	}
	else
	{
		/* Returns 0 indicates that PQgetResult can 
		 * be called with assurance of not blocking. */
		if (PQgetResultAction(nodeInfo))
		{
			toConnectionStatusSucceeded(nodeInfo);
			return;
		}
		else
		{
			/* go on to wait result */
		}
	}
	if (isQueryTimedOut(nodeInfo))
	{
		occurredError(nodeInfo, NODE_ERROR_QUERY_TIMEDOUT);
	}
	return;
}

static void handleConnectionStatusBad(MonitorNodeInfo *nodeInfo)
{
	/* Remove WaitEvents */
	nodeInfo->waitEvents = 0;
	/* Try to reconnect to the server. */
	if (beyondReconnectDelay(nodeInfo))
	{
		resetConnection(nodeInfo);
	}
	else
	{
		ereport(DEBUG1,
				(errmsg("%s, connect node too often",
						MyBgworkerEntry->bgw_name)));
	}
}

static void toConnectionStatusConnecting(MonitorNodeInfo *nodeInfo)
{
	ereport(DEBUG1,
			(errmsg("%s, start connect node",
					MyBgworkerEntry->bgw_name)));
	nodeInfo->connnectionStatus = NODE_CONNNECTION_STATUS_CONNECTING;
	nodeInfo->connectTime = GetCurrentTimestamp();
}

static void toConnectionStatusSucceeded(MonitorNodeInfo *nodeInfo)
{
	if (nodeInfo->connnectionStatus == NODE_CONNNECTION_STATUS_CONNECTING)
	{
		ereport(DEBUG1,
				(errmsg("%s, connect node succeeded",
						MyBgworkerEntry->bgw_name)));
	}
	else if (nodeInfo->connnectionStatus == NODE_CONNNECTION_STATUS_QUERYING)
	{
		ereport(DEBUG1,
				(errmsg("%s, query node succeeded",
						MyBgworkerEntry->bgw_name)));
		/* reset queryfail control factor */
		nodeInfo->nQueryfails = 0;
	}
	nodeInfo->queryHandler = NULL;
	nodeInfo->connnectionStatus = NODE_CONNNECTION_STATUS_SUCCEEDED;
	/* Of course, it is active. */
	nodeInfo->activeTime = GetCurrentTimestamp();
	nodeInfo->waitEvents |= WL_SOCKET_READABLE;
	/* Ensure all connection errors cleaned */
	resetAdbDoctorErrorRecorder(nodeInfo->connectionErrors);
}

static void toConnectionStatusQuerying(MonitorNodeInfo *nodeInfo)
{
	ereport(DEBUG1,
			(errmsg("%s, start query node",
					MyBgworkerEntry->bgw_name)));
	nodeInfo->queryTime = GetCurrentTimestamp();
	nodeInfo->connnectionStatus = NODE_CONNNECTION_STATUS_QUERYING;
}

static void toConnectionStatusBad(MonitorNodeInfo *nodeInfo)
{
	ereport(LOG,
			(errmsg("%s, connect node bad",
					MyBgworkerEntry->bgw_name)));
	nodeInfo->waitEvents = 0;
	nodeInfo->queryHandler = NULL;
	nodeInfo->connnectionStatus = NODE_CONNNECTION_STATUS_BAD;
}

static void handleRunningStatusNormal(MonitorNodeInfo *nodeInfo)
{
	int lastError;

	lastError = getLastNodeErrorno(nodeInfo);
	if (lastError == NODE_CANNOT_CONNECT_STARTUP ||
		lastError == NODE_CANNOT_CONNECT_NOW ||
		lastError == NODE_CANNOT_CONNECT_TOOMANY)
	{
		toRunningStatusPending(nodeInfo);
	}
	else if (lastError == NODE_CANNOT_CONNECT_SHUTDOWN)
	{
		nodeInfo->shutdownTime = GetCurrentTimestamp();
		toRunningStatusPending(nodeInfo);
	}
	else if (lastError == NODE_CANNOT_CONNECT_RECOVERY)
	{
		nodeInfo->recoveryTime = GetCurrentTimestamp();
		toRunningStatusPending(nodeInfo);
	}
	else
	{
		if (reachedCrashedCondition(nodeInfo))
		{
			toRunningStatusCrashed(nodeInfo);
			handleNodeCrashed(nodeInfo);
		}
		else
		{
			/*  */
		}
	}
}

static void handleRunningStatusCrashed(MonitorNodeInfo *nodeInfo)
{
	int lastError;
	if (isNodeRunningNormally(nodeInfo))
	{
		toRunningStatusNormal(nodeInfo);
	}
	else
	{
		lastError = getLastNodeErrorno(nodeInfo);
		if (lastError == NODE_CANNOT_CONNECT_STARTUP ||
			lastError == NODE_CANNOT_CONNECT_NOW ||
			lastError == NODE_CANNOT_CONNECT_TOOMANY)
		{
			toRunningStatusPending(nodeInfo);
		}
		else if (lastError == NODE_CANNOT_CONNECT_SHUTDOWN)
		{
			nodeInfo->shutdownTime = GetCurrentTimestamp();
			toRunningStatusPending(nodeInfo);
		}
		else if (lastError == NODE_CANNOT_CONNECT_RECOVERY)
		{
			nodeInfo->recoveryTime = GetCurrentTimestamp();
			toRunningStatusPending(nodeInfo);
		}
		else
		{
			handleNodeCrashed(nodeInfo);
		}
	}
}

static void handleRunningStatusPending(MonitorNodeInfo *nodeInfo)
{
	int lastError;
	if (isNodeRunningNormally(nodeInfo))
	{
		toRunningStatusNormal(nodeInfo);
	}
	else
	{
		lastError = getLastNodeErrorno(nodeInfo);
		if (lastError == NODE_CANNOT_CONNECT_SHUTDOWN)
		{
			if (isShutdownTimedout(nodeInfo))
			{
				ereport(LOG,
						(errmsg("%s, node is too long in shutdown mode",
								MyBgworkerEntry->bgw_name)));
				if (shutdownNode(nodeInfo, SHUTDOWN_I))
				{
					startupNode(nodeInfo);
				}
				resetMonitor(nodeInfo);
			}
			else
			{
				/*  */
			}
		}
		else if (lastError == NODE_CANNOT_CONNECT_STARTUP ||
				 lastError == NODE_CANNOT_CONNECT_RECOVERY ||
				 lastError == NODE_CANNOT_CONNECT_NOW ||
				 lastError == NODE_CANNOT_CONNECT_TOOMANY)
		{
			/*  */
		}
		else
		{
			if (reachedCrashedCondition(nodeInfo))
			{
				toRunningStatusCrashed(nodeInfo);
				handleNodeCrashed(nodeInfo);
			}
			else
			{
				/*  */
			}
		}
	}
}

static void toRunningStatusNormal(MonitorNodeInfo *nodeInfo)
{
	ereport(LOG,
			(errmsg("%s, node running normally",
					MyBgworkerEntry->bgw_name)));
	nodeInfo->runningStatus = NODE_RUNNING_STATUS_NORMAL;
	nodeInfo->nRestarts = 0;
	nodeInfo->crashedTime = 0;
	nodeInfo->shutdownTime = 0;
	nodeInfo->recoveryTime = 0;
	checkAndUpdateCurestatus(nodeInfo->node, CURE_STATUS_NORMAL);
}

static void toRunningStatusCrashed(MonitorNodeInfo *nodeInfo)
{
	ereport(LOG,
			(errmsg("%s, node crashed",
					MyBgworkerEntry->bgw_name)));
	nodeInfo->runningStatus = NODE_RUNNING_STATUS_CRASHED;
	nodeInfo->crashedTime = GetCurrentTimestamp();
}

static void toRunningStatusPending(MonitorNodeInfo *nodeInfo)
{
	ereport(LOG,
			(errmsg("%s, node pending",
					MyBgworkerEntry->bgw_name)));
	nodeInfo->runningStatus = NODE_RUNNING_STATUS_PENDING;
}

static void handleNodeCrashed(MonitorNodeInfo *nodeInfo)
{
	if (isMasterOrSlave(nodeInfo->node->fdmn.nodetype))
	{
		/* if this master node allow restart, try to restart it.
		 * if not, set it to "wait switch" */
		if (nodeConfiguration->restartCrashedMaster == 1)
		{
			if (nodeInfo->nRestarts > 0)
			{
				if (isMasterRestartTimedout(nodeInfo))
				{
					nodeWaitSwitch(nodeInfo);
				}
				else
				{
					/* wait */
				}
			}
			else
			{
				if (tryRestartNode(nodeInfo))
				{
					resetMonitor(nodeInfo);
				}
				else
				{
					nodeWaitSwitch(nodeInfo);
				}
			}
		}
		else
		{
			nodeWaitSwitch(nodeInfo);
		}
	}
	else
	{
		/* restart this slave node until succeeded */
		if (tryRestartNode(nodeInfo))
		{
			resetMonitor(nodeInfo);
		}
	}
}

static void handleNodeDangerous(MonitorNodeInfo *nodeInfo)
{
	ereport(LOG,
			(errmsg("%s, node is running dangerously",
					MyBgworkerEntry->bgw_name)));
	/* Check if monitor this node responsibility is still me. */
	checkAndReplaceAdbMgrNode(&nodeInfo->node);
	checkAndUpdateCurestatus(nodeInfo->node, CURE_STATUS_DANGER);
	closeConnection(nodeInfo);
	/* This node is running dangerously, shut it down */
	shutdownNode(nodeInfo, SHUTDOWN_F);
	notifyAdbDoctorRegistrant();
	/* I can't handle this situation, bye bye. */
	raise(SIGTERM);
}

static bool tryRestartNode(MonitorNodeInfo *nodeInfo)
{
	if (beyondRestartDelay(nodeInfo))
	{
		/* Double check if node changed? */
		checkAndReplaceAdbMgrNode(&nodeInfo->node);
		checkAndUpdateCurestatus(nodeInfo->node, CURE_STATUS_CURING);
		return startupNode(nodeInfo);
	}
	else
	{
		ereport(DEBUG1,
				(errmsg("%s, restart node too often",
						MyBgworkerEntry->bgw_name)));
		return false;
	}
}

static void nodeWaitSwitch(MonitorNodeInfo *nodeInfo)
{
	if (isMasterOrSlave(nodeInfo->node->fdmn.nodetype))
	{
		/* Double check if node changed? */
		checkAndReplaceAdbMgrNode(&nodeInfo->node);
		/* If it is a master node, signal to launcher that needs to be switched. */
		checkAndUpdateCurestatus(nodeInfo->node, CURE_STATUS_WAIT_SWITCH);
		notifyAdbDoctorRegistrant();
		/* I can't do the work of switching, I need to quit. */
		raise(SIGTERM);
	}
	else
	{
		ereport(ERROR,
				(errmsg("%s, can not do wait switch operation on a slave node.",
						MyBgworkerEntry->bgw_name)));
	}
}

static bool startupNode(MonitorNodeInfo *nodeInfo)
{
	bool ok;
	GetAgentCmdRst *volatile cmdRst = NULL;
	AdbMgrHostWrapper *volatile host = NULL;

	nodeInfo->nRestarts++;
	nodeInfo->restartTime = GetCurrentTimestamp();
	/* Modify the value of the variable that controls the restart frequency. */
	nextRestartFactor(nodeInfo);

	ereport(LOG,
			(errmsg("%s, startup node",
					MyBgworkerEntry->bgw_name)));
	PG_TRY();
	{
		host = getAdbMgrHostData(nodeInfo->node);
		cmdRst = mgr_start_node_execute(nodeInfo->node->fdmn.nodetype,
										nodeInfo->node->nodepath,
										host->hostaddr,
										host->fdmh.hostagentport);
		ok = cmdRst->ret;
		ereport(LOG,
				(errmsg("%s, startup node result:%d, message:%s",
						MyBgworkerEntry->bgw_name,
						cmdRst->ret,
						cmdRst->description.data)));
	}
	PG_CATCH();
	{
		ok = false;
		EmitErrorReport();
	}
	PG_END_TRY();
	if (host)
		pfree(host);
	if (cmdRst)
	{
		pfree(cmdRst->description.data);
		pfree(cmdRst);
	}
	nodeInfo->restartTime = GetCurrentTimestamp();
	return ok;
}

static bool shutdownNode(MonitorNodeInfo *nodeInfo, char *shutdownMode)
{
	bool ok;
	GetAgentCmdRst *volatile cmdRst = NULL;
	AdbMgrHostWrapper *volatile host = NULL;

	ereport(LOG,
			(errmsg("%s, shutdown node %s",
					MyBgworkerEntry->bgw_name,
					shutdownMode)));
	PG_TRY();
	{
		host = getAdbMgrHostData(nodeInfo->node);
		cmdRst = mgr_stop_node_execute(nodeInfo->node->fdmn.nodetype,
									   nodeInfo->node->nodepath,
									   host->hostaddr,
									   host->fdmh.hostagentport,
									   shutdownMode);
		ok = cmdRst->ret;
		ereport(LOG,
				(errmsg("%s, shutdown node result:%d, message:%s",
						MyBgworkerEntry->bgw_name,
						cmdRst->ret,
						cmdRst->description.data)));
	}
	PG_CATCH();
	{
		ok = false;
		EmitErrorReport();
	}
	PG_END_TRY();
	if (host)
		pfree(host);
	if (cmdRst)
	{
		pfree(cmdRst->description.data);
		pfree(cmdRst);
	}
	return ok;
}

static void startConnection(MonitorNodeInfo *nodeInfo)
{
	StringInfoData conninfo;

	initStringInfo(&conninfo);
	appendStringInfo(&conninfo,
					 "postgresql://%s@%s:%d/%s",
					 NameStr(nodeInfo->node->hostuser),
					 nodeInfo->node->hostaddr,
					 nodeInfo->node->fdmn.nodeport,
					 DEFAULT_DB);
	/* Ensure there is no connection to node. */
	if (nodeInfo->conn != NULL)
	{
		PQfinish(nodeInfo->conn);
		nodeInfo->conn = NULL;
	}
	toConnectionStatusConnecting(nodeInfo);
	/* Make a connection to the database server in a nonblocking manner. */
	nodeInfo->conn = PQconnectStart(conninfo.data);
	pfree(conninfo.data);
	if (nodeInfo->conn == NULL)
	{
		ereport(ERROR,
				(errmsg("%s, libpq has been unable to allocate a new PGconn structure",
						MyBgworkerEntry->bgw_name)));
	}
	if (PQstatus(nodeInfo->conn) == CONNECTION_BAD)
	{
		occurredError(nodeInfo, NODE_ERROR_CONNECT_FAIL);
	}
	else
	{
		nodeInfo->waitEvents |= WL_SOCKET_MASK;
	}
}

static void resetConnection(MonitorNodeInfo *nodeInfo)
{
	int res;

	toConnectionStatusConnecting(nodeInfo);
	/* We use this element to determine the connection error state,
	 * must reset it, otherwise may take wrong. */
	memset(nodeInfo->conn->last_sqlstate, 0, 6);
	res = PQresetStart(nodeInfo->conn);
	if (res > 0)
	{
		if (PQstatus(nodeInfo->conn) == CONNECTION_BAD)
		{
			occurredError(nodeInfo, NODE_ERROR_CONNECT_FAIL);
		}
		else
		{
			nodeInfo->waitEvents |= WL_SOCKET_MASK;
		}
	}
	else
	{
		occurredError(nodeInfo, NODE_ERROR_CONNECT_FAIL);
	}
}

static void closeConnection(MonitorNodeInfo *nodeInfo)
{
	toConnectionStatusBad(nodeInfo);
	ereport(LOG,
			(errmsg("%s, close node connection",
					MyBgworkerEntry->bgw_name)));
	PQfinish(nodeInfo->conn);
	nodeInfo->conn = NULL;
}

static void resetMonitor(MonitorNodeInfo *nodeInfo)
{
	resetAdbDoctorErrorRecorder(nodeInfo->connectionErrors);
	nodeInfo->nQueryfails = 0;
	closeConnection(nodeInfo);
	startConnection(nodeInfo);
}

static bool startQuery(MonitorNodeInfo *nodeInfo, NodeQuerySqlType sqlType)
{
	int res;

	if (nodeInfo->queryHandler != NULL)
	{
		ereport(LOG,
				(errmsg("%s, query is busy",
						MyBgworkerEntry->bgw_name)));
		return false;
	}
	if (!beyondQueryInterval(nodeInfo))
	{
		/* query too often */
		return false;
	}
	toConnectionStatusQuerying(nodeInfo);
	/* Prevent block waiting caused by sending output to the server and 
	 * achieve completely nonblocking database operation */
	res = PQsetnonblocking(nodeInfo->conn, 1);
	if (res != 0)
	{
		occurredError(nodeInfo, NODE_ERROR_CONNECT_FAIL);
		return false;
	}
	if (sqlType == SQL_TYPE_PG_IS_IN_RECOVERY)
	{
		nodeInfo->queryHandler = pg_is_in_recovery_handler;
		/* Non-blocking mode query */
		res = PQsendQuery(nodeInfo->conn, "SELECT PG_IS_IN_RECOVERY()");
		if (res != 1)
		{
			occurredError(nodeInfo, NODE_ERROR_CONNECT_FAIL);
			return false;
		}
	}
	else
	{
		ereport(ERROR,
				(errmsg("Unexpected NodeQuerySqlType:%d",
						sqlType)));
	}

	/* After sending any command or data on a nonblocking connection, call PQflush. */
	if (!PQflushAction(nodeInfo))
	{
		occurredError(nodeInfo, NODE_ERROR_CONNECT_FAIL);
		return false;
	}
	return true;
}

static bool cancelQuery(MonitorNodeInfo *nodeInfo)
{
	PGcancel *cancle;
	char *errbuf;
	int errbufsize;
	int ret;

	ereport(DEBUG1,
			(errmsg("%s, cancel query",
					MyBgworkerEntry->bgw_name)));
	cancle = PQgetCancel(nodeInfo->conn);
	if (cancle == NULL)
	{
		return false;
	}
	/* The return value is 1 if the cancel request was successfully dispatched and 0 if not.
	 * If not, errbuf is filled with an explanatory error message. 
	 * errbuf must be a char array of size errbufsize (the recommended size is 256 bytes). */
	errbufsize = 256;
	errbuf = palloc0(256);
	ret = PQcancel(cancle, errbuf, errbufsize) == 1;
	if (ret != 1)
	{
		ereport(LOG,
				(errmsg("%s, cancel query error:%s",
						MyBgworkerEntry->bgw_name,
						errbuf)));
	}
	pfree(errbuf);
	PQfreeCancel(cancle);
	return ret == 1;
}

/*
 * Invoke PQflush(), and set waitEvents.
 *
 * Returns: true if no error
 *			false if error (conn->errorMessage is set)
 */
static bool PQflushAction(MonitorNodeInfo *nodeInfo)
{
	int res;

	res = PQflush(nodeInfo->conn);
	if (res == 0)
	{
		/* Once PQflush returns 0, wait for the socket to be 
		 * read-ready and then read the response */
		nodeInfo->waitEvents |= WL_SOCKET_READABLE;
		return true;
	}
	else if (res == 1)
	{
		/* If it returns 1, wait for the socket to become read- or write-ready. */
		nodeInfo->waitEvents |= WL_SOCKET_READABLE;
		nodeInfo->waitEvents |= WL_SOCKET_WRITEABLE;
		return true;
	}
	else
	{
		/* -1 if it failed for some reason */
		return false;
	}
}

/* 
 * PQgetResult must be called repeatedly until it returns a null pointer, 
 * indicating that the command is done. 
 * Even when PQresultStatus indicates a fatal error, PQgetResult should be 
 * called until it returns a null pointer, to allow libpq to process the 
 * error information completely. 
 */
static void PQgetResultUntilNull(PGconn *conn)
{
	PGresult *pgResult;
	while (true)
	{
		pgResult = PQgetResult(conn);
		if (pgResult != NULL)
		{
			PQclear(pgResult);
		}
		else
		{
			break;
		}
	}
}

static bool pg_is_in_recovery_handler(MonitorNodeInfo *nodeInfo, PGresult *pgResult)
{
	bool pg_is_in_recovery;
	char *value;
	bool isMasterNode;

	value = PQgetvalue(pgResult, 0, 0);
	/* Boolean accepts these string representations for the “true” state:true,yes,on,1
	 * The datatype output function for type boolean always emits either t or f */
	pg_is_in_recovery = pg_strcasecmp(value, "t") == 0 ||
						pg_strcasecmp(value, "true") == 0 ||
						pg_strcasecmp(value, "yes") == 0 ||
						pg_strcasecmp(value, "on") == 0 ||
						pg_strcasecmp(value, "1") == 0;
	isMasterNode = isMasterOrSlave(nodeInfo->node->fdmn.nodetype);
	if (isMasterNode && pg_is_in_recovery)
	{
		occurredError(nodeInfo, NODE_RUNNING_DANGEROUSLY);
	}
	if (!isMasterNode && !pg_is_in_recovery)
	{
		occurredError(nodeInfo, NODE_RUNNING_DANGEROUSLY);
	}
	return true;
}

static bool simple_print_query_handler(MonitorNodeInfo *nodeInfo,
									   PGresult *pgResult)
{
	int nFields, i, j;
	/* first, print out the attribute names */
	nFields = PQnfields(pgResult);
	for (i = 0; i < nFields; i++)
		ereport(LOG,
				(errmsg("%-15s",
						PQfname(pgResult, i))));
	/* next, print out the rows */
	for (i = 0; i < PQntuples(pgResult); i++)
	{
		for (j = 0; j < nFields; j++)
			ereport(LOG,
					(errmsg("%-15s",
							PQgetvalue(pgResult, i, j))));
	}
	return true;
}

/*
 * Invoke PQgetResult().
 *
 * Returns: true if no connection error
 *			false if connection error (conn->errorMessage is set)
 */
static bool PQgetResultAction(MonitorNodeInfo *nodeInfo)
{
	PGresult *pgResult;
	char *sqlstate;
	bool handlerRetOK;

	pgResult = PQgetResult(nodeInfo->conn);
	if (pgResult == NULL)
	{
		return true;
	}
	else
	{
		if (PQresultStatus(pgResult) != PGRES_TUPLES_OK)
		{
			sqlstate = PQresultErrorField(pgResult, PG_DIAG_SQLSTATE);
			if (sqlstate != NULL)
			{
				/* Although the sql execution error, but still treat the node
				 * as running normally. If there a sqlstate can determine the
				 * node exception, handle that exception. */
				ereport(LOG,
						(errmsg("%s, sql execution error, sqlstate:%s, ErrorMessage:%s",
								MyBgworkerEntry->bgw_name,
								sqlstate,
								PQresultErrorMessage(pgResult))));
				PQclear(pgResult);
				return PQgetResultAction(nodeInfo);
			}
			else
			{
				PQclear(pgResult);
				PQgetResultUntilNull(nodeInfo->conn);
				return false;
			}
		}
		else
		{
			if (nodeInfo->queryHandler == NULL)
			{
				handlerRetOK = simple_print_query_handler(nodeInfo, pgResult);
			}
			else
			{
				/* Call the specific handler */
				handlerRetOK = nodeInfo->queryHandler(nodeInfo, pgResult);
			}
			/* Sentinal */
			PQclear(pgResult);
			if (handlerRetOK)
			{
				return PQgetResultAction(nodeInfo);
			}
			else
			{
				PQgetResultUntilNull(nodeInfo->conn);
				return false;
			}
		}
	}
}

static bool isConnectTimedOut(MonitorNodeInfo *nodeInfo)
{
	return TimestampDifferenceExceeds(nodeInfo->connectTime,
									  GetCurrentTimestamp(),
									  nodeConfiguration->connectTimeoutMs);
}

static bool isQueryTimedOut(MonitorNodeInfo *nodeInfo)
{
	return TimestampDifferenceExceeds(nodeInfo->queryTime,
									  GetCurrentTimestamp(),
									  nodeConfiguration->queryTimoutMs);
}

static bool isMasterRestartTimedout(MonitorNodeInfo *nodeInfo)
{
	return TimestampDifferenceExceeds(nodeInfo->restartTime,
									  GetCurrentTimestamp(),
									  nodeConfiguration->restartMasterTimeoutMs);
}

static bool isNodeRunningNormally(MonitorNodeInfo *nodeInfo)
{
	return nodeInfo->activeTime > nodeInfo->connectTime &&
		   nodeInfo->activeTime > nodeInfo->crashedTime &&
		   nodeInfo->connectionErrors->nerrors == 0 &&
		   PQstatus(nodeInfo->conn) == CONNECTION_OK;
}

static bool isShutdownTimedout(MonitorNodeInfo *nodeInfo)
{
	return nodeInfo->shutdownTime > 0 &&
		   TimestampDifferenceExceeds(nodeInfo->shutdownTime,
									  GetCurrentTimestamp(),
									  nodeConfiguration->shutdownTimeoutMs);
}

static bool isShouldResetConnection(MonitorNodeInfo *nodeInfo)
{
	return TimestampDifferenceExceeds(nodeInfo->connectTime,
									  GetCurrentTimestamp(),
									  nodeConfiguration->holdConnectionMs);
}

static bool beyondReconnectDelay(MonitorNodeInfo *nodeInfo)
{
	return TimestampDifferenceExceeds(nodeInfo->connectTime,
									  GetCurrentTimestamp(),
									  nodeConfiguration->reconnectDelayMs);
}

static bool beyondQueryInterval(MonitorNodeInfo *nodeInfo)
{
	long realQueryIntervalMs;
	if (nodeInfo->nQueryfails < 1)
	{
		realQueryIntervalMs = nodeConfiguration->queryIntervalMs;
	}
	else
	{
		realQueryIntervalMs = nodeConfiguration->queryIntervalMs *
							  (1 << Min(nodeInfo->nQueryfails, 8));
	}
	return TimestampDifferenceExceeds(nodeInfo->queryTime,
									  GetCurrentTimestamp(),
									  realQueryIntervalMs);
}

static bool beyondRestartDelay(MonitorNodeInfo *nodeInfo)
{
	long realRestartDelayMs;
	/* We don't want to restart too often.  */
	realRestartDelayMs = nodeConfiguration->restartDelayMs *
						 (1 << nodeInfo->restartFactor->num);
	return TimestampDifferenceExceeds(nodeInfo->restartTime,
									  GetCurrentTimestamp(),
									  realRestartDelayMs);
}

static void nextRestartFactor(MonitorNodeInfo *nodeInfo)
{
	int resetDelayMs;
	/* Reset to default value for more than 2 maximum delay cycles */
	resetDelayMs = nodeConfiguration->restartDelayMs *
				   (1 << nodeInfo->restartFactor->num);
	resetDelayMs = resetDelayMs * 2;
	if (TimestampDifferenceExceeds(nodeInfo->restartTime,
								   GetCurrentTimestamp(),
								   resetDelayMs))
	{
		resetAdbDoctorBounceNum(nodeInfo->restartFactor);
	}
	else
	{
		nextAdbDoctorBounceNum(nodeInfo->restartFactor);
	}
}

static void occurredError(MonitorNodeInfo *nodeInfo, NodeError error)
{
	ereport(LOG,
			(errmsg("%s, %s, PQerrorMessage:%s",
					MyBgworkerEntry->bgw_name,
					NODE_ERROR_MSG[(int)error - 1],
					PQerrorMessage(nodeInfo->conn))));
	if (error == NODE_ERROR_CONNECT_TIMEDOUT ||
		error == NODE_ERROR_CONNECT_FAIL ||
		error == NODE_CANNOT_CONNECT_STARTUP ||
		error == NODE_CANNOT_CONNECT_SHUTDOWN ||
		error == NODE_CANNOT_CONNECT_RECOVERY ||
		error == NODE_CANNOT_CONNECT_TOOMANY ||
		error == NODE_CANNOT_CONNECT_NOW)
	{
		appendAdbDoctorErrorRecorder(nodeInfo->connectionErrors, (int)error);
		toConnectionStatusBad(nodeInfo);
		handleConnectionStatusBad(nodeInfo);
	}
	else if (error == NODE_ERROR_QUERY_TIMEDOUT)
	{
		nodeInfo->nQueryfails++;
		cancelQuery(nodeInfo);
		toConnectionStatusBad(nodeInfo);
		handleConnectionStatusBad(nodeInfo);
	}
	else if (error == NODE_RUNNING_DANGEROUSLY)
	{
		/* Dangerous error, handle it whatever the situation. */
		handleNodeDangerous(nodeInfo);
	}
	else
	{
		ereport(ERROR,
				(errmsg("Unexpected NodeError:%d",
						error)));
	}
}

static bool reachedCrashedCondition(MonitorNodeInfo *nodeInfo)
{
	int errorCount;
	AdbDoctorError *firstError;
	AdbDoctorError *lastError;
	int *errornos;
	int nErrornos;
	bool res;
	if (nodeInfo->connectionErrors->nerrors < 1)
	{
		res = false;
	}
	else
	{
		nErrornos = 2;
		errornos = palloc(sizeof(int) * nErrornos);
		errornos[0] = (int)NODE_ERROR_CONNECT_TIMEDOUT;
		errornos[1] = (int)NODE_ERROR_CONNECT_FAIL;
		firstError = findFirstAdbDoctorError(nodeInfo->connectionErrors,
											 errornos, nErrornos);
		if (!firstError)
		{
			res = false;
		}
		else
		{
			lastError = findLastAdbDoctorError(nodeInfo->connectionErrors,
											   errornos, nErrornos);
			if (TimestampDifferenceExceeds(firstError->time,
										   lastError->time,
										   nodeConfiguration->deadlineMs))
			{
				/* reached dead line, node crashed */
				res = true;
			}
			else
			{
				errorCount = countAdbDoctorErrorRecorder(nodeInfo->connectionErrors,
														 errornos, nErrornos);
				res = errorCount >= nodeConfiguration->connectionErrorNumMax;
			}
		}
		pfree(errornos);
	}
	return res;
}

static int getLastNodeErrorno(MonitorNodeInfo *nodeInfo)
{
	AdbDoctorError *lastError;

	lastError = getLastAdbDoctorError(nodeInfo->connectionErrors);
	if (lastError)
	{
		return lastError->errorno;
	}
	else
	{
		return -1;
	}
}

static AdbMgrHostWrapper *getAdbMgrHostData(AdbMgrNodeWrapper *node)
{
	int ret;
	AdbMgrHostWrapper *host;
	MemoryContext oldContext;

	oldContext = CurrentMemoryContext;

	SPI_CONNECT_TRANSACTIONAL_START(ret);
	if (ret != SPI_OK_CONNECT)
	{
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 (errmsg("SPI_connect failed, connect return:%d",
						 ret))));
	}
	host = SPI_selectMgrHostByOid(oldContext, node->fdmn.nodehost);
	SPI_FINISH_TRANSACTIONAL_COMMIT();

	MemoryContextSwitchTo(oldContext);
	if (!host)
	{
		ereport(ERROR,
				(errmsg("%s get host info failed",
						MyBgworkerEntry->bgw_name)));
	}
	return host;
}

static void checkAndReplaceAdbMgrNode(AdbMgrNodeWrapper **nodeP)
{
	int ret;
	AdbMgrNodeWrapper *nodeDataInMem;
	AdbMgrNodeWrapper *nodeDataInDB;
	MemoryContext oldContext;

	nodeDataInMem = *nodeP;
	oldContext = CurrentMemoryContext;

	SPI_CONNECT_TRANSACTIONAL_START(ret);
	if (ret != SPI_OK_CONNECT)
	{
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 (errmsg("SPI_connect failed, connect return:%d",
						 ret))));
	}
	nodeDataInDB = SPI_selectMgrNodeByOid(oldContext, nodeDataInMem->oid);
	SPI_FINISH_TRANSACTIONAL_COMMIT();

	MemoryContextSwitchTo(oldContext);

	if (!nodeDataInDB)
	{
		ereport(ERROR,
				(errmsg("%s, node data not exists in database",
						MyBgworkerEntry->bgw_name)));
	}
	if (!nodeDataInDB->fdmn.allowcure)
	{
		ereport(ERROR,
				(errmsg("%s, cure node not allowed",
						MyBgworkerEntry->bgw_name)));
	}
	checkCurestatus(nodeDataInDB);
	if (!equalsAdbMgrNodeWrapper(nodeDataInMem, nodeDataInDB))
	{
		ereport(ERROR,
				(errmsg("%s, node data has changed in database",
						MyBgworkerEntry->bgw_name)));
	}
	pfreeAdbMgrNodeWrapper(nodeDataInMem);
	*nodeP = nodeDataInDB;
}

static void checkAndUpdateCurestatus(AdbMgrNodeWrapper *node, char *newCurestatus)
{
	int ret;
	MemoryContext oldContext;

	if (pg_strcasecmp(NameStr(node->fdmn.curestatus),
					  newCurestatus) == 0)
	{
		/* old status equals new status, no need to update. */
		return;
	}

	checkCurestatus(node);

	oldContext = CurrentMemoryContext;

	SPI_CONNECT_TRANSACTIONAL_START(ret);
	if (ret != SPI_OK_CONNECT)
	{
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 (errmsg("SPI_connect failed, connect return:%d",
						 ret))));
	}
	ret = SPI_updateMgrNodeCureStatus(node->oid,
									  NameStr(node->fdmn.curestatus),
									  newCurestatus);
	if (ret == 1)
	{
		SPI_FINISH_TRANSACTIONAL_COMMIT();
	}
	else
	{
		SPI_FINISH_TRANSACTIONAL_ABORT();
	}

	MemoryContextSwitchTo(oldContext);

	if (ret != 1)
	{
		ereport(ERROR,
				(errmsg("%s, can not transit to curestatus:%s",
						MyBgworkerEntry->bgw_name,
						newCurestatus)));
	}
	else
	{
		namestrcpy(&node->fdmn.curestatus, newCurestatus);
	}
}

static void checkCurestatus(AdbMgrNodeWrapper *node)
{
	if (pg_strcasecmp(NameStr(node->fdmn.curestatus), CURE_STATUS_NORMAL) != 0 &&
		pg_strcasecmp(NameStr(node->fdmn.curestatus), CURE_STATUS_CURING) != 0)
	{
		ereport(ERROR,
				(errmsg("%s, node curestatus:%s, it is not my responsibility",
						MyBgworkerEntry->bgw_name,
						NameStr(node->fdmn.curestatus))));
	}
}

/**
 * If is master node return true,
 * If is slave node return false,
 * otherwise complain.
 */
static bool isMasterOrSlave(char nodetype)
{
	switch (nodetype)
	{
	case CNDN_TYPE_COORDINATOR_MASTER:
		return true;
		break;
	case CNDN_TYPE_COORDINATOR_SLAVE:
		return false;
		break;
	case CNDN_TYPE_DATANODE_MASTER:
		return true;
		break;
	case CNDN_TYPE_DATANODE_SLAVE:
		return false;
		break;
	case GTM_TYPE_GTM_MASTER:
		return true;
		break;
	case GTM_TYPE_GTM_SLAVE:
		return false;
		break;
	default:
		ereport(ERROR,
				(errmsg("Unexpected nodetype:%c",
						nodetype)));
		break;
	}
}

static void attachNodeDataShm(Datum main_arg, AdbDoctorNodeData **dataP)
{
	dsm_segment *seg;
	shm_toc *toc;
	AdbDoctorNodeData *dataInShm;
	AdbDoctorNodeData *data;
	Adb_Doctor_Bgworker_Type type;
	uint64 tocKey = 0;

	CurrentResourceOwner = ResourceOwnerCreate(NULL, MyBgworkerEntry->bgw_name);
	seg = dsm_attach(DatumGetUInt32(main_arg));
	if (seg == NULL)
		ereport(ERROR,
				(errmsg("unable to map individual dynamic shared memory segment")));

	toc = shm_toc_attach(ADB_DOCTOR_SHM_DATA_MAGIC, dsm_segment_address(seg));
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

static NodeConfiguration *newNodeConfiguration(AdbDoctorConf *conf)
{
	NodeConfiguration *nc;
	long deadlineMs;
	long minTimeMs;

	checkAdbDoctorConf(conf);

	nc = palloc0(sizeof(NodeConfiguration));

	deadlineMs = conf->nodedeadline * 1000L;
	nc->deadlineMs = deadlineMs;
	nc->waitEventTimeoutMs = LIMIT_VALUE_RANGE(500, 10000,
											   floor(deadlineMs / 20));
	/* treat waitEventTimeoutMs as the minimum time unit, 
	 * any time variable less then it is meaningless. */
	minTimeMs = nc->waitEventTimeoutMs;
	nc->connectTimeoutMs = LIMIT_VALUE_RANGE(Max(minTimeMs,
												 conf->node_connect_timeout_ms_min),
											 conf->node_connect_timeout_ms_max,
											 floor(deadlineMs / 10));
	nc->reconnectDelayMs = LIMIT_VALUE_RANGE(Max(minTimeMs,
												 conf->node_reconnect_delay_ms_min),
											 conf->node_reconnect_delay_ms_max,
											 floor(deadlineMs / 20));
	nc->queryTimoutMs = LIMIT_VALUE_RANGE(Max(minTimeMs,
											  conf->node_query_timeout_ms_min),
										  conf->node_query_timeout_ms_max,
										  floor(deadlineMs / 5));
	nc->queryIntervalMs = LIMIT_VALUE_RANGE(Max(minTimeMs,
												conf->node_query_interval_ms_min),
											conf->node_query_interval_ms_max,
											floor(deadlineMs / 5));
	nc->restartDelayMs = LIMIT_VALUE_RANGE(Max(minTimeMs,
											   conf->node_restart_delay_ms_min),
										   conf->node_restart_delay_ms_max,
										   floor(deadlineMs / 2));
	nc->holdConnectionMs = LIMIT_VALUE_RANGE(nc->queryIntervalMs * 4,
											 (nc->queryTimoutMs + nc->queryIntervalMs) * 4,
											 deadlineMs);
	nc->restartCrashedMaster = conf->node_restart_crashed_master;
	nc->restartMasterTimeoutMs = conf->node_restart_master_timeout_ms;
	nc->shutdownTimeoutMs = conf->node_shutdown_timeout_ms;
	nc->connectionErrorNumMax = conf->node_connection_error_num_max;
	ereport(LOG,
			(errmsg("%s configuration: "
					"deadlineMs:%ld, waitEventTimeoutMs:%ld, "
					"connectTimeoutMs:%ld, reconnectDelayMs:%ld, "
					"queryTimoutMs:%ld, queryIntervalMs:%ld, "
					"restartDelayMs:%ld, holdConnectionMs:%ld, "
					"restartCrashedMaster:%d, restartMasterTimeoutMs:%ld, "
					"shutdownTimeoutMs:%ld, connectionErrorNumMax:%d",
					MyBgworkerEntry->bgw_name,
					nc->deadlineMs, nc->waitEventTimeoutMs,
					nc->connectTimeoutMs, nc->reconnectDelayMs,
					nc->queryTimoutMs, nc->queryIntervalMs,
					nc->restartDelayMs, nc->holdConnectionMs,
					nc->restartCrashedMaster, nc->restartMasterTimeoutMs,
					nc->shutdownTimeoutMs, nc->connectionErrorNumMax)));
	return nc;
}

static MonitorNodeInfo *newMonitorNodeInfo(AdbMgrNodeWrapper *node)
{
	MonitorNodeInfo *nodeInfo;

	nodeInfo = palloc(sizeof(MonitorNodeInfo));
	nodeInfo->conn = NULL;
	nodeInfo->node = node;
	nodeInfo->connnectionStatus = NODE_CONNNECTION_STATUS_CONNECTING;
	nodeInfo->runningStatus = NODE_RUNNING_STATUS_PENDING;
	nodeInfo->queryHandler = NULL;
	nodeInfo->waitEvents = 0;
	nodeInfo->occurredEvents = 0;
	nodeInfo->nRestarts = 0;
	nodeInfo->connectTime = 0;
	nodeInfo->queryTime = 0;
	nodeInfo->activeTime = 0;
	nodeInfo->crashedTime = 0;
	nodeInfo->restartTime = 0;
	nodeInfo->shutdownTime = 0;
	nodeInfo->recoveryTime = 0;
	nodeInfo->nQueryfails = 0;
	nodeInfo->restartFactor = newAdbDoctorBounceNum(5);
	nodeInfo->connectionErrors = newAdbDoctorErrorRecorder(100);
	return nodeInfo;
}

static void pfreeMonitorNodeInfo(MonitorNodeInfo *nodeInfo)
{
	if (nodeInfo)
	{
		if (nodeInfo->conn)
		{
			PQfinish(nodeInfo->conn);
			nodeInfo->conn = NULL;
		}
		pfreeAdbMgrNodeWrapper(nodeInfo->node);
		pfreeAdbDoctorBounceNum(nodeInfo->restartFactor);
		pfreeAdbDoctorErrorRecorder(nodeInfo->connectionErrors);
		pfree(nodeInfo);
		nodeInfo = NULL;
	}
}