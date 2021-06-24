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
#include "storage/latch.h"
#include "executor/spi.h"
#include "utils/resowner.h"
#include "utils/builtins.h"
#include "utils/ps_status.h"
#include "utils/memutils.h"
#include "../../src/interfaces/libpq/libpq-fe.h"
#include "../../src/interfaces/libpq/libpq-int.h"
#include "mgr/mgr_agent.h"
#include "mgr/mgr_msg_type.h"
#include "mgr/mgr_cmds.h"
#include "mgr/mgr_helper.h"
#include "mgr/mgr_switcher.h"
#include "adb_doctor.h"
#include "adb_doctor_log.h"

typedef struct SwitcherConfiguration
{
	long switchIntervalMs;
	bool forceSwitch;
	bool rewindOldMaster;
} SwitcherConfiguration;

typedef enum CoordinatorHoldMgrNode
{
	COORDINATOR_HOLD_MGRNODE_UNKNOWN = 0,
	COORDINATOR_HOLD_MGRNODE_YES,
	COORDINATOR_HOLD_MGRNODE_NO
} CoordinatorHoldMgrNode;

typedef struct SwitchMasterResult
{
	bool done;
	bool abortSwitch;
	NameData newMasterName;
} SwitchMasterResult;

static void switcherMainLoop(dlist_head *oldMasters);
static void checkAndSwitchSpecifiedTypeMaster(dlist_head *oldMasters, char nodetype);
static SwitchMasterResult *checkAndSwitchMaster(MgrNodeWrapper *oldMaster);
static bool checkIfOldMasterCanReign(MgrNodeWrapper *oldMaster,
									 bool forceSwitch);
static bool checkIfDataNodeOldMasterCanReign(MgrNodeWrapper *oldMaster,
											 bool forceSwitch,
											 MemoryContext spiContext);
static bool checkIfGtmCoordOldMasterCanReign(MgrNodeWrapper *oldMaster,
											 bool forceSwitch,
											 MemoryContext spiContext);
static void oldGtmCoordMasterContinueToReign(MgrNodeWrapper *oldMaster);
static void oldDataNodeMasterContinueToReign(MgrNodeWrapper *oldMaster);
static void failoverOldMaster(MgrNodeWrapper *oldMaster, Name newMasterName);
static bool isAllCoordinatorsHoldOldMaster(MgrNodeWrapper *oldMaster,
										   MemoryContext spiContext);
static bool isAllCoordinatorsHoldDataNodeMaster(MgrNodeWrapper *dataNodeMaster,
												MemoryContext spiContext);
static bool isAnyCoordinatorMayHoldGtmCoordMaster(MgrNodeWrapper *gtmCoordMaster,
												  MemoryContext spiContext);
static bool isAllCoordinatorsHoldGtmCoordMaster(MgrNodeWrapper *gtmCoordMaster,
												MemoryContext spiContext);
static CoordinatorHoldMgrNode isCoordinatorHoldMgrNode(MgrNodeWrapper *coordinator,
													   MgrNodeWrapper *mgrNode,
													   bool complain);
static bool isAnyCoordinatorsQueryOk(MemoryContext spiContext);
static bool isCoordinatorQueryOk(MgrNodeWrapper *coordinator);
static void checkMgrNodeDataInDB(MgrNodeWrapper *nodeDataInMem,
								 MemoryContext spiContext);
static void getCheckMgrNodesForSwitcher(dlist_head *nodes);
static void classifySlaveNodesByIfRunning(MgrNodeWrapper *masterNode,
										  MemoryContext spiContext,
										  dlist_head *runningSlaves,
										  dlist_head *failedSlaves);
static SwitcherConfiguration *newSwitcherConfiguration(AdbDoctorConf *conf);
static void examineAdbDoctorConf(dlist_head *oldMasters);
static void resetSwitcher(void);

static void handleSigterm(SIGNAL_ARGS);
static void handleSigusr1(SIGNAL_ARGS);

static AdbDoctorConfShm *confShm;
static SwitcherConfiguration *switcherConfiguration;
static sigjmp_buf reset_switcher_sigjmp_buf;

static volatile sig_atomic_t gotSigterm = false;
static volatile sig_atomic_t gotSigusr1 = false;

void adbDoctorSwitcherMain(Datum main_arg)
{
	AdbDoctorBgworkerData *bgworkerData;
	AdbDoctorConf *confInLocal;
	dlist_head mgrNodes = DLIST_STATIC_INIT(mgrNodes);

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
		switcherConfiguration = newSwitcherConfiguration(confInLocal);
		pfree(confInLocal);

		if (sigsetjmp(reset_switcher_sigjmp_buf, 1) != 0)
		{
			pfreeMgrNodeWrapperList(&mgrNodes, NULL);
		}
		dlist_init(&mgrNodes);

		getCheckMgrNodesForSwitcher(&mgrNodes);
		switcherMainLoop(&mgrNodes);
	}
	PG_CATCH();
	{
		PG_RE_THROW();
	}
	PG_END_TRY();
	proc_exit(1);
}

static void switcherMainLoop(dlist_head *oldMasters)
{
	int rc;

	while (!gotSigterm)
	{
		checkAndSwitchSpecifiedTypeMaster(oldMasters, CNDN_TYPE_GTM_COOR_MASTER);
		checkAndSwitchSpecifiedTypeMaster(oldMasters, CNDN_TYPE_DATANODE_MASTER);
		if (dlist_is_empty(oldMasters))
		{
			/* The switch task was completed, the process should exits */
			break;
		}
		set_ps_display("sleeping");
		rc = WaitLatchOrSocket(MyLatch,
							   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
							   PGINVALID_SOCKET,
							   switcherConfiguration->switchIntervalMs,
							   PG_WAIT_EXTENSION);
		/* Reset the latch, bail out if postmaster died. */
		if (rc & WL_POSTMASTER_DEATH)
		{
			ereport(ERROR,
					(errmsg("%s my postmaster dead, i need to exit",
							MyBgworkerEntry->bgw_name)));
		}
		/* Interrupted? */
		if (rc & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
		}
		CHECK_FOR_INTERRUPTS();
		examineAdbDoctorConf(oldMasters);
	}
}

static void checkAndSwitchSpecifiedTypeMaster(dlist_head *oldMasters, char nodetype)
{
	dlist_mutable_iter miter;
	MgrNodeWrapper *oldMaster;
	SwitchMasterResult *switchMasterResult;
	AdbDoctorLogRow *logRow;

	dlist_foreach_modify(miter, oldMasters)
	{
		oldMaster = dlist_container(MgrNodeWrapper, link, miter.cur);
		if (oldMaster->form.nodetype == nodetype)
		{
			logRow = beginAdbDoctorLog(NameStr(oldMaster->form.nodename),
									   ADBDOCTORLOG_STRATEGY_SWITCH);
			BEGIN_CATCH_ERR_MSG();
			switchMasterResult = checkAndSwitchMaster(oldMaster);
			END_CATCH_ERR_MSG();

			logRow->assistnode = NameStr(switchMasterResult->newMasterName);
			if (switchMasterResult->abortSwitch)
				logRow->strategy = ADBDOCTORLOG_STRATEGY_ABORT_SWITCH;

			if (switchMasterResult->done)
			{
				dlist_delete(miter.cur);
				pfreeMgrNodeWrapper(oldMaster);
				endAdbDoctorLog(logRow, true);
			}
			else
			{
				logRow->errormsg = ereport_message;
				endAdbDoctorLog(logRow, false);
			}
			pfree(switchMasterResult);

			CHECK_FOR_INTERRUPTS();
			examineAdbDoctorConf(oldMasters);
		}
	}
}

static SwitchMasterResult *checkAndSwitchMaster(MgrNodeWrapper *oldMaster)
{
	volatile bool done = false;
	MemoryContext oldContext;
	MemoryContext doctorContext;
	MgrNodeWrapper mgrNodeBackup;
	SwitchMasterResult *switchMasterResult = NULL;

	switchMasterResult = palloc0(sizeof(SwitchMasterResult));
	set_ps_display(NameStr(oldMaster->form.nodename));
	memcpy(&mgrNodeBackup, oldMaster, sizeof(MgrNodeWrapper));

	oldContext = CurrentMemoryContext;
	doctorContext = AllocSetContextCreate(oldContext,
										  "doctorContext",
										  ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(doctorContext);

	PG_TRY();
	{
		if (oldMaster->form.nodetype == CNDN_TYPE_DATANODE_MASTER ||
			oldMaster->form.nodetype == CNDN_TYPE_GTM_COOR_MASTER)
		{
			if (checkIfOldMasterCanReign(oldMaster,
										 switcherConfiguration->forceSwitch))
			{
				switchMasterResult->abortSwitch = true;
				if (oldMaster->form.nodetype == CNDN_TYPE_DATANODE_MASTER)
				{
					oldDataNodeMasterContinueToReign(oldMaster);
				}
				else
				{
					oldGtmCoordMasterContinueToReign(oldMaster);
				}
			}
			else
			{
				switchMasterResult->abortSwitch = false;
				failoverOldMaster(oldMaster, &switchMasterResult->newMasterName);
			}
		}
		else
		{
			/* mgr_node data may be changed in database */
			pg_usleep(5L * 1000000L);
			pfree(switchMasterResult);
			switchMasterResult = NULL;
			resetSwitcher();
		}
		done = true;
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(oldContext);
		done = false;
		EmitErrorReport();
		FlushErrorState();
	}
	PG_END_TRY();

	(void)MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(doctorContext);

	if (!done)
	{
		memcpy(oldMaster, &mgrNodeBackup, sizeof(MgrNodeWrapper));
	}
	switchMasterResult->done = done;
	return switchMasterResult;
}

static bool checkIfOldMasterCanReign(MgrNodeWrapper *oldMaster,
									 bool forceSwitch)
{
	ErrorData *edata = NULL;
	bool oldMasterCanReign = false;
	int spiRes;
	uint64 rows;
	MemoryContext oldContext;
	MemoryContext doctorContext;
	MemoryContext spiContext;

	oldContext = CurrentMemoryContext;
	doctorContext = AllocSetContextCreate(oldContext,
										  "doctorContext",
										  ALLOCSET_DEFAULT_SIZES);
	SPI_CONNECT_TRANSACTIONAL_START(spiRes, true);
	spiContext = CurrentMemoryContext;
	MemoryContextSwitchTo(doctorContext);

	PG_TRY();
	{
		checkMgrNodeDataInDB(oldMaster, spiContext);
		/* Sentinel, prevent other doctors from operating this node simultaneously. */
		rows = updateMgrNodeCurestatus(oldMaster,
									   CURE_STATUS_SWITCHING,
									   spiContext);
		if (rows != 1)
			ereport(ERROR,
					(errmsg("%s, curestatus can not transit form %s to:%s",
							NameStr(oldMaster->form.nodename),
							NameStr(oldMaster->form.curestatus),
							CURE_STATUS_SWITCHING)));
		if (isGtmCoordMgrNode(oldMaster->form.nodetype))
		{
			oldMasterCanReign = checkIfGtmCoordOldMasterCanReign(oldMaster,
																 forceSwitch,
																 spiContext);
		}
		else if (isDataNodeMgrNode(oldMaster->form.nodetype))
		{
			oldMasterCanReign = checkIfDataNodeOldMasterCanReign(oldMaster,
																 forceSwitch,
																 spiContext);
		}
		else
		{
			ereport(ERROR,
					(errmsg("unsupported node %s with nodetype %c",
							NameStr(oldMaster->form.nodename),
							oldMaster->form.nodetype)));
		}
	}
	PG_CATCH();
	{
		/* Save error info in our stmt_mcontext */
		MemoryContextSwitchTo(oldContext);
		edata = CopyErrorData();
		FlushErrorState();
	}
	PG_END_TRY();

	/* 
	* This function only check the oldMaster node before the actual operation, 
	* and does not really do things that affect the cluster, so the transaction
	* needs to be rolled back. 
	*/
	SPI_FINISH_TRANSACTIONAL_ABORT();
	(void)MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(doctorContext);

	if (edata)
	{
		/* 
		 * The current state of the cluster does not meet the conditions for 
		 * performing the master/slave switching.  
		 */
		ReThrowError(edata);
	}
	return oldMasterCanReign;
}

static bool checkIfDataNodeOldMasterCanReign(MgrNodeWrapper *oldMaster,
											 bool forceSwitch,
											 MemoryContext spiContext)
{
	bool oldMasterStatusOk = false;
	bool haveQualifiedCandidate = false;
	bool oldMasterCanReign = false;
	bool allCoordinatorsHoldOldMaster = false;
	PGconn *oldMasterConn = NULL;
	XLogRecPtr oldMasterWalLsn = InvalidXLogRecPtr;
	XLogRecPtr candidateWalLsn = InvalidXLogRecPtr;
	dlist_head failedSlaves = DLIST_STATIC_INIT(failedSlaves);
	dlist_head runningSlaves = DLIST_STATIC_INIT(runningSlaves);
	dlist_iter iter;
	SwitcherNodeWrapper *node;
	MgrNodeWrapper *gtmMaster = NULL;

	/* check if the relation of master/slave is changed in coordinators */
	allCoordinatorsHoldOldMaster =
		isAllCoordinatorsHoldOldMaster(oldMaster,
									   spiContext);
	if (!allCoordinatorsHoldOldMaster)
		goto end;

	ereport(LOG,
			(errmsg("old master %s exists in all coordinators's pgxc_node",
					NameStr(oldMaster->form.nodename))));

	gtmMaster = selectMgrGtmCoordNode(spiContext);
	if (!gtmMaster || !setGtmInfoInPGSqlConf(oldMaster, gtmMaster, false))
	{
		oldMasterStatusOk = false;
		goto end;
	}

	/* it is safe for starting up the old master */
	if (!pingNodeWaitinSeconds(oldMaster, PQPING_OK, 0))
	{
		if (!startupNodeWithinSeconds(oldMaster,
									  STARTUP_NODE_SECONDS,
									  true,
									  false))
		{
			oldMasterStatusOk = false;
			goto end;
		}
	}
	oldMasterConn = getNodeDefaultDBConnection(oldMaster, 10);
	if (oldMasterConn)
	{
		if (getNodeRunningMode(oldMasterConn) == NODE_RUNNING_MODE_MASTER)
		{
			oldMasterWalLsn = getNodeWalLsn(oldMasterConn,
											NODE_RUNNING_MODE_MASTER);
			ereport(LOG,
					(errmsg("old master %s wal lsn is " UINT64_FORMAT,
							NameStr(oldMaster->form.nodename),
							oldMasterWalLsn)));
			oldMasterStatusOk = oldMasterWalLsn > InvalidXLogRecPtr;
		}
		else
		{
			oldMasterStatusOk = false;
		}
	}
	else
	{
		oldMasterStatusOk = false;
	}
	if (!oldMasterStatusOk)
		goto end;

	ereport(LOG,
			(errmsg("old master %s running status ok",
					NameStr(oldMaster->form.nodename))));

	/* Check if there are some slave node has been promoted to master */
	classifySlaveNodesByIfRunning(oldMaster, spiContext,
								  &runningSlaves, &failedSlaves);
	if (dlist_is_empty(&runningSlaves))
	{
		ereport(LOG,
				(errmsg("cant find a normal running slave node of old master %s",
						NameStr(oldMaster->form.nodename))));
		haveQualifiedCandidate = false;
	}
	else
	{
		if (forceSwitch)
		{
			haveQualifiedCandidate = true;
		}
		else
		{
			dlist_foreach(iter, &runningSlaves)
			{
				node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
				node->runningMode = getNodeRunningMode(node->pgConn);
				/* It a dangerous situation, should compare the wal lsn with the old master */
				if (node->runningMode == NODE_RUNNING_MODE_MASTER)
				{
					candidateWalLsn = getNodeWalLsn(node->pgConn,
													node->runningMode);
					ereport(LOG,
							(errmsg("slave node %s is running on master mode, "
									"it's wal lsn is " UINT64_FORMAT,
									NameStr(node->mgrNode->form.nodename),
									candidateWalLsn)));
					/*
					 * When a slave node is running in the master mode, it indicates that 
					 * this node may be choosed as new master in the latest switch operation, 
					 * but due to some exceptions, the switch operation is not completely 
					 * successful. So when this node lsn is bigger than the old master, 
					 * we will continue to promote this node as the new master. 
					 */
					haveQualifiedCandidate = (oldMasterWalLsn > InvalidXLogRecPtr) &&
											 (candidateWalLsn >= oldMasterWalLsn);
					oldMasterStatusOk = oldMasterWalLsn >= candidateWalLsn;
				}
				else
				{
					haveQualifiedCandidate = false;
					continue;
				}
				if (haveQualifiedCandidate)
					break;
			}
		}
	}

	/* When use goto statement in PG_TRY block, 
	 * goto the outside of PG_TRY block would cause core dump. */
end:
	if (oldMasterConn)
		PQfinish(oldMasterConn);
	pfreeSwitcherNodeWrapperList(&failedSlaves, NULL);
	pfreeSwitcherNodeWrapperList(&runningSlaves, NULL);
	if (gtmMaster)
		pfreeMgrNodeWrapper(gtmMaster);

	oldMasterCanReign = oldMasterStatusOk &&
						allCoordinatorsHoldOldMaster &&
						!haveQualifiedCandidate;
	if (!oldMasterCanReign)
		shutdownNodeWithinSeconds(oldMaster,
								  SHUTDOWN_NODE_FAST_SECONDS,
								  SHUTDOWN_NODE_IMMEDIATE_SECONDS,
								  false);
	return oldMasterCanReign;
}

/*
 * When the gtmcoord master crashed, all coordinators can't get the query results.
 * But there is a situation, If a slave node have been promoted,
 * and then suddenly crashed, some coordinators may connect to this crashed node, 
 * these coordinators can't get the query results too. 
 */
static bool checkIfGtmCoordOldMasterCanReign(MgrNodeWrapper *oldMaster,
											 bool forceSwitch,
											 MemoryContext spiContext)
{
	bool oldMasterStatusOk = false;
	bool haveQualifiedCandidate = false;
	bool oldMasterCanReign = false;
	bool allCoordinatorsHoldOldMaster = false;
	PGconn *oldMasterConn = NULL;
	XLogRecPtr oldMasterWalLsn = InvalidXLogRecPtr;
	XLogRecPtr candidateWalLsn = InvalidXLogRecPtr;
	XLogRecPtr maxCandidateWalLsn = InvalidXLogRecPtr;
	dlist_head failedSlaves = DLIST_STATIC_INIT(failedSlaves);
	dlist_head runningSlaves = DLIST_STATIC_INIT(runningSlaves);
	dlist_iter iter;
	SwitcherNodeWrapper *node;
	char nodetypeBackup;

	classifySlaveNodesByIfRunning(oldMaster, spiContext,
								  &runningSlaves, &failedSlaves);
	if (forceSwitch)
	{
		if (!dlist_is_empty(&runningSlaves))
		{
			haveQualifiedCandidate = true;
			goto end;
		}
	}
	else
	{
		dlist_foreach(iter, &runningSlaves)
		{
			node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
			if (strcmp(NameStr(node->mgrNode->form.nodesync),
					   getMgrNodeSyncStateValue(SYNC_STATE_SYNC)) == 0)
			{
				haveQualifiedCandidate = true;
			}
			else
			{
				haveQualifiedCandidate = false;
				continue;
			}
			if (haveQualifiedCandidate)
				break;
		}
		if (haveQualifiedCandidate)
			goto end;

		dlist_foreach(iter, &runningSlaves)
		{
			node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
			node->runningMode = getNodeRunningMode(node->pgConn);
			/* may be this node has been promoted as a master node */
			if (node->runningMode == NODE_RUNNING_MODE_MASTER)
			{
				candidateWalLsn = getNodeWalLsn(node->pgConn,
												node->runningMode);
				if (candidateWalLsn > maxCandidateWalLsn)
					maxCandidateWalLsn = candidateWalLsn;

				ereport(LOG,
						(errmsg("slave node %s is running on master mode, "
								"it's wal lsn is " UINT64_FORMAT,
								NameStr(node->mgrNode->form.nodename),
								candidateWalLsn)));

				nodetypeBackup = node->mgrNode->form.nodetype;
				node->mgrNode->form.nodetype = getMgrMasterNodetype(nodetypeBackup);
				haveQualifiedCandidate =
					isAnyCoordinatorMayHoldGtmCoordMaster(node->mgrNode,
														  spiContext);
				node->mgrNode->form.nodetype = nodetypeBackup;
			}
			else
			{
				haveQualifiedCandidate = false;
				continue;
			}
			if (haveQualifiedCandidate)
				break;
		}
		if (haveQualifiedCandidate)
			goto end;

		if (isAnyCoordinatorsQueryOk(spiContext))
		{
			allCoordinatorsHoldOldMaster = false;
			goto end;
		}

		/* 
		 * may be some slave nodes has been promoted as a master node, 
		 * but suddenly crashed, so i can try to startup the old master 
		 * and judge the configuration in pgxc_node of coordinators.
		 */
		if (!pingNodeWaitinSeconds(oldMaster, PQPING_OK, 0))
		{
			if (!startupNodeWithinSeconds(oldMaster,
										  STARTUP_NODE_SECONDS,
										  true,
										  false))
			{
				oldMasterStatusOk = false;
				goto end;
			}
		}
		oldMasterConn = getNodeDefaultDBConnection(oldMaster, 10);
		if (oldMasterConn)
		{
			if (getNodeRunningMode(oldMasterConn) == NODE_RUNNING_MODE_MASTER)
			{
				oldMasterWalLsn = getNodeWalLsn(oldMasterConn,
												NODE_RUNNING_MODE_MASTER);
				ereport(LOG,
						(errmsg("old master %s wal lsn is " UINT64_FORMAT,
								NameStr(oldMaster->form.nodename),
								oldMasterWalLsn)));
				oldMasterStatusOk = (oldMasterWalLsn > InvalidXLogRecPtr) &&
									(oldMasterWalLsn >= maxCandidateWalLsn);
			}
			else
			{
				oldMasterStatusOk = false;
			}
		}
		else
		{
			oldMasterStatusOk = false;
		}
		if (!oldMasterStatusOk)
			goto end;

		ereport(LOG,
				(errmsg("old master %s running status ok",
						NameStr(oldMaster->form.nodename))));

		allCoordinatorsHoldOldMaster =
			isAllCoordinatorsHoldOldMaster(oldMaster,
										   spiContext);
		if (!allCoordinatorsHoldOldMaster)
			goto end;

		ereport(LOG,
				(errmsg("old master %s exists in all coordinators's pgxc_node",
						NameStr(oldMaster->form.nodename))));
	}

end:
	if (oldMasterConn)
		PQfinish(oldMasterConn);
	pfreeSwitcherNodeWrapperList(&failedSlaves, NULL);
	pfreeSwitcherNodeWrapperList(&runningSlaves, NULL);

	oldMasterCanReign = oldMasterStatusOk &&
						allCoordinatorsHoldOldMaster &&
						!haveQualifiedCandidate;
	if (!oldMasterCanReign)
		shutdownNodeWithinSeconds(oldMaster,
								  SHUTDOWN_NODE_FAST_SECONDS,
								  SHUTDOWN_NODE_IMMEDIATE_SECONDS,
								  false);
	return oldMasterCanReign;
}

static void oldGtmCoordMasterContinueToReign(MgrNodeWrapper *oldMaster)
{
	ErrorData *edata = NULL;
	int spiRes;
	dlist_head mgrNodes = DLIST_STATIC_INIT(mgrNodes);
	dlist_head slaveNodes = DLIST_STATIC_INIT(slaveNodes);
	dlist_iter iter;
	SwitcherNodeWrapper *slaveNode;
	MemoryContext oldContext;
	MemoryContext doctorContext;
	MemoryContext spiContext;

	oldContext = CurrentMemoryContext;
	doctorContext = AllocSetContextCreate(oldContext,
										  "doctorContext",
										  ALLOCSET_DEFAULT_SIZES);
	SPI_CONNECT_TRANSACTIONAL_START(spiRes, true);
	spiContext = CurrentMemoryContext;
	MemoryContextSwitchTo(doctorContext);

	PG_TRY();
	{
		ereport(LOG,
				(errmsg("old master %s returned to normal, try to abort switching",
						NameStr(oldMaster->form.nodename))));
		selectActiveMgrSlaveNodes(oldMaster->form.oid,
								  getMgrSlaveNodetype(oldMaster->form.nodetype),
								  spiContext,
								  &mgrNodes);
		selectIsolatedMgrSlaveNodes(oldMaster->form.oid,
									getMgrSlaveNodetype(oldMaster->form.nodetype),
									spiContext,
									&mgrNodes);
		mgrNodesToSwitcherNodes(&mgrNodes, &slaveNodes);
		/* Sentinel, prevent other doctors from operating this node simultaneously. */
		checkMgrNodeDataInDB(oldMaster, spiContext);
		updateCureStatusForSwitch(oldMaster,
								  CURE_STATUS_SWITCHING,
								  spiContext);
		dlist_foreach(iter, &slaveNodes)
		{
			slaveNode = dlist_container(SwitcherNodeWrapper, link, iter.cur);
			/* backup curestatus */
			memcpy(&slaveNode->oldCurestatus,
				   &slaveNode->mgrNode->form.curestatus, sizeof(NameData));
			updateCureStatusForSwitch(slaveNode->mgrNode,
									  CURE_STATUS_SWITCHING,
									  spiContext);
		}
		if (!isAllCoordinatorsHoldOldMaster(oldMaster,
											spiContext))
		{
			ereport(ERROR,
					(errmsg("old master %s cancel the reign, because the cluster state may have changed",
							NameStr(oldMaster->form.nodename))));
		}
		startupNodeWithinSeconds(oldMaster,
								 STARTUP_NODE_SECONDS,
								 true,
								 true);
		updateCureStatusForSwitch(oldMaster,
								  CURE_STATUS_SWITCHED,
								  spiContext);
		dlist_foreach(iter, &slaveNodes)
		{
			slaveNode = dlist_container(SwitcherNodeWrapper, link, iter.cur);
			if (pg_strcasecmp(NameStr(slaveNode->oldCurestatus),
							  CURE_STATUS_OLD_MASTER) == 0 ||
				pg_strcasecmp(NameStr(slaveNode->oldCurestatus),
							  CURE_STATUS_ISOLATED) == 0)
			{
				updateCureStatusForSwitch(slaveNode->mgrNode,
										  NameStr(slaveNode->oldCurestatus),
										  spiContext);
			}
			else
			{
				updateCureStatusForSwitch(slaveNode->mgrNode,
										  CURE_STATUS_FOLLOW_FAIL,
										  spiContext);
			}
		}
		ereport(LOG,
				(errmsg("old master %s returned to normal and begin to reign again, switching aborted",
						NameStr(oldMaster->form.nodename))));
	}
	PG_CATCH();
	{
		/* Save error info in our stmt_mcontext */
		MemoryContextSwitchTo(oldContext);
		edata = CopyErrorData();
		FlushErrorState();
	}
	PG_END_TRY();

	pfreeSwitcherNodeWrapperList(&slaveNodes, NULL);

	(void)MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(doctorContext);

	if (edata)
	{
		SPI_FINISH_TRANSACTIONAL_ABORT();
		shutdownNodeWithinSeconds(oldMaster,
								  SHUTDOWN_NODE_FAST_SECONDS,
								  SHUTDOWN_NODE_IMMEDIATE_SECONDS,
								  false);
		ReThrowError(edata);
	}
	else
	{
		SPI_FINISH_TRANSACTIONAL_COMMIT();
	}
}

static void oldDataNodeMasterContinueToReign(MgrNodeWrapper *oldMaster)
{
	ErrorData *edata = NULL;
	int spiRes;
	dlist_head mgrNodes = DLIST_STATIC_INIT(mgrNodes);
	dlist_head slaveNodes = DLIST_STATIC_INIT(slaveNodes);
	dlist_head coordinators = DLIST_STATIC_INIT(coordinators);
	dlist_head siblingMasters = DLIST_STATIC_INIT(siblingMasters);
	dlist_iter iter;
	SwitcherNodeWrapper *node;
	SwitcherNodeWrapper *gtmMaster = NULL;
	SwitcherNodeWrapper *holdLockCoordinator = NULL;
	MemoryContext oldContext;
	MemoryContext doctorContext;
	MemoryContext spiContext;
	PGconn *oldMasterConn = NULL;
	SwitcherNodeWrapper *switcherOldMaster = NULL;

	oldContext = CurrentMemoryContext;
	doctorContext = AllocSetContextCreate(oldContext,
										  "doctorContext",
										  ALLOCSET_DEFAULT_SIZES);
	SPI_CONNECT_TRANSACTIONAL_START(spiRes, true);
	spiContext = CurrentMemoryContext;
	MemoryContextSwitchTo(doctorContext);

	PG_TRY();
	{
		ereport(LOG,
				(errmsg("old master %s returned to normal, try to abort switching",
						NameStr(oldMaster->form.nodename))));

		checkGetMasterCoordinators(spiContext,
								   &coordinators,
								   true,
								   true);
		dlist_foreach(iter, &coordinators)
		{
			node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
			if (node->mgrNode->form.nodetype == CNDN_TYPE_GTM_COOR_MASTER)
			{
				gtmMaster = node;
			}
			if (!isMgrModeExistsInCoordinator(node->mgrNode,
											  node->pgConn,
											  true,
											  oldMaster,
											  true))
			{
				ereport(ERROR,
						(errmsg("old master %s cancel the reign, because the cluster state may have changed",
								NameStr(oldMaster->form.nodename))));
			}
		}

		selectActiveMgrSlaveNodes(oldMaster->form.oid,
								  getMgrSlaveNodetype(oldMaster->form.nodetype),
								  spiContext,
								  &mgrNodes);
		selectIsolatedMgrSlaveNodes(oldMaster->form.oid,
									getMgrSlaveNodetype(oldMaster->form.nodetype),
									spiContext,
									&mgrNodes);
		mgrNodesToSwitcherNodes(&mgrNodes, &slaveNodes);
		/* Sentinel, prevent other doctors from operating this node simultaneously. */
		checkMgrNodeDataInDB(oldMaster, spiContext);
		updateCureStatusForSwitch(oldMaster,
								  CURE_STATUS_SWITCHING,
								  spiContext);
		dlist_foreach(iter, &slaveNodes)
		{
			node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
			/* backup curestatus */
			memcpy(&node->oldCurestatus,
				   &node->mgrNode->form.curestatus, sizeof(NameData));
			updateCureStatusForSwitch(node->mgrNode,
									  CURE_STATUS_SWITCHING,
									  spiContext);
		}

		if (gtmMaster)
			setGtmInfoInPGSqlConf(oldMaster, gtmMaster->mgrNode, false);

		startupNodeWithinSeconds(oldMaster,
								 STARTUP_NODE_SECONDS,
								 true,
								 true);
		oldMasterConn = getNodeDefaultDBConnection(oldMaster, 10);
		if (!oldMasterConn)
			ereport(ERROR,
					(errmsg("old master %s cancel the reign, can not connect to it",
							NameStr(oldMaster->form.nodename))));

		if (!checkNodeRunningMode(oldMasterConn, true))
			ereport(ERROR,
					(errmsg("old master %s cancel the reign, it running improperly",
							NameStr(oldMaster->form.nodename))));
		switcherOldMaster = palloc0(sizeof(SwitcherNodeWrapper));
		switcherOldMaster->mgrNode = oldMaster;
		switcherOldMaster->pgConn = oldMasterConn;
		switcherOldMaster->runningMode = NODE_RUNNING_MODE_MASTER;

		{
			checkGetSiblingMasterNodes(spiContext,
									   switcherOldMaster,
									   &siblingMasters);
			tryLockCluster(&coordinators);
			holdLockCoordinator = getHoldLockCoordinator(&coordinators);
			Assert(holdLockCoordinator);
			diffPgxcNodesOfDataNode(holdLockCoordinator->pgConn,
									false,
									switcherOldMaster,
									&siblingMasters,
									spiContext,
									true);

			commitSwitcherNodeTransaction(holdLockCoordinator,
										  true);
			tryUnlockCluster(&coordinators, true);
		}

		updateCureStatusForSwitch(oldMaster,
								  CURE_STATUS_SWITCHED,
								  spiContext);
		dlist_foreach(iter, &slaveNodes)
		{
			node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
			if (pg_strcasecmp(NameStr(node->oldCurestatus),
							  CURE_STATUS_OLD_MASTER) == 0 ||
				pg_strcasecmp(NameStr(node->oldCurestatus),
							  CURE_STATUS_ISOLATED) == 0)
			{
				updateCureStatusForSwitch(node->mgrNode,
										  NameStr(node->oldCurestatus),
										  spiContext);
			}
			else
			{
				updateCureStatusForSwitch(node->mgrNode,
										  CURE_STATUS_FOLLOW_FAIL,
										  spiContext);
			}
		}

		ereport(LOG,
				(errmsg("old master %s returned to normal and begin to reign again, switching aborted",
						NameStr(oldMaster->form.nodename))));
	}
	PG_CATCH();
	{
		rollbackSwitcherNodeTransaction(holdLockCoordinator, false);
		tryUnlockCluster(&coordinators, false);

		/* Save error info in our stmt_mcontext */
		MemoryContextSwitchTo(oldContext);
		edata = CopyErrorData();
		FlushErrorState();
	}
	PG_END_TRY();

	pfreeSwitcherNodeWrapperList(&slaveNodes, NULL);
	pfreeSwitcherNodeWrapperList(&coordinators, NULL);
	pfreeSwitcherNodeWrapperList(&siblingMasters, switcherOldMaster);
	if (oldMasterConn)
		PQfinish(oldMasterConn);
	if (switcherOldMaster)
		pfree(switcherOldMaster);

	(void)MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(doctorContext);

	if (edata)
	{
		SPI_FINISH_TRANSACTIONAL_ABORT();
		shutdownNodeWithinSeconds(oldMaster,
								  SHUTDOWN_NODE_FAST_SECONDS,
								  SHUTDOWN_NODE_IMMEDIATE_SECONDS,
								  false);
		ReThrowError(edata);
	}
	else
	{
		SPI_FINISH_TRANSACTIONAL_COMMIT();
	}
}

static void failoverOldMaster(MgrNodeWrapper *oldMaster, Name newMasterName)
{
	ErrorData *edata = NULL;
	int spiRes;
	MemoryContext oldContext;
	oldContext = CurrentMemoryContext;
	SPI_CONNECT_TRANSACTIONAL_START(spiRes, true);

	PG_TRY();
	{
		if (isGtmCoordMgrNode(oldMaster->form.nodetype))
		{
			FailOverGtmCoordMaster(NameStr(oldMaster->form.nodename),
								 switcherConfiguration->forceSwitch,
								 !switcherConfiguration->rewindOldMaster,
								 newMasterName,
								 mgr_zone);
		}
		else if (isDataNodeMgrNode(oldMaster->form.nodetype))
		{
			FailOverDataNodeMaster(NameStr(oldMaster->form.nodename),
								 switcherConfiguration->forceSwitch,
								 !switcherConfiguration->rewindOldMaster,
								 newMasterName,
								 mgr_zone);
		}
		else
		{
			ereport(ERROR,
					(errmsg("unsupported node %s with nodetype %c",
							NameStr(oldMaster->form.nodename),
							oldMaster->form.nodetype)));
		}
		ereport(LOG,
				(errmsg("From now on, the master node %s begin to reign. "
						"The old master %s become slave and wait for rewind. "
						"Switching completed",
						NameStr(*newMasterName),
						NameStr(oldMaster->form.nodename))));
	}
	PG_CATCH();
	{
		/* Save error info in our stmt_mcontext */
		MemoryContextSwitchTo(oldContext);
		edata = CopyErrorData();
		FlushErrorState();
	}
	PG_END_TRY();

	(void)MemoryContextSwitchTo(oldContext);
	if (edata)
	{
		SPI_FINISH_TRANSACTIONAL_ABORT();
		ReThrowError(edata);
	}
	else
	{
		SPI_FINISH_TRANSACTIONAL_COMMIT();
	}
}

/**
 * If all coordinators keep the configuration of old master in their 
 * pgxc_node table, This means that no switching has been performed, 
 * or the last switching operation has failed absolutely.
 */
static bool isAllCoordinatorsHoldOldMaster(MgrNodeWrapper *oldMaster,
										   MemoryContext spiContext)
{
	if (isGtmCoordMgrNode(oldMaster->form.nodetype))
		return isAllCoordinatorsHoldGtmCoordMaster(oldMaster,
												   spiContext);
	else if (isDataNodeMgrNode(oldMaster->form.nodetype))
		return isAllCoordinatorsHoldDataNodeMaster(oldMaster,
												   spiContext);
	else
		ereport(ERROR,
				(errmsg("unsupported node %s with nodetype %c",
						NameStr(oldMaster->form.nodename),
						oldMaster->form.nodetype)));
}

static bool isAllCoordinatorsHoldDataNodeMaster(MgrNodeWrapper *dataNodeMaster,
												MemoryContext spiContext)
{
	dlist_head coordinators = DLIST_STATIC_INIT(coordinators);
	dlist_iter iter;
	MgrNodeWrapper *coordinator;
	bool allHold;
	CoordinatorHoldMgrNode hold;

	selectActiveMasterCoordinators(spiContext, &coordinators);
	if (dlist_is_empty(&coordinators))
	{
		ereport(ERROR,
				(errmsg("can't find any master coordinator")));
	}
	allHold = true;
	dlist_foreach(iter, &coordinators)
	{
		coordinator = dlist_container(MgrNodeWrapper, link, iter.cur);
		hold = isCoordinatorHoldMgrNode(coordinator,
										dataNodeMaster,
										true);
		if (hold != COORDINATOR_HOLD_MGRNODE_YES)
		{
			allHold = false;
			break;
		}
	}
	pfreeMgrNodeWrapperList(&coordinators, NULL);
	return allHold;
}

static bool isAnyCoordinatorMayHoldGtmCoordMaster(MgrNodeWrapper *gtmCoordMaster,
												  MemoryContext spiContext)
{
	dlist_head coordinators = DLIST_STATIC_INIT(coordinators);
	dlist_iter iter;
	MgrNodeWrapper *coordinator;
	bool mayHold;
	CoordinatorHoldMgrNode hold;

	selectActiveMgrNodeByNodetype(spiContext,
								  CNDN_TYPE_COORDINATOR_MASTER,
								  &coordinators);
	mayHold = false;
	dlist_foreach(iter, &coordinators)
	{
		coordinator = dlist_container(MgrNodeWrapper, link, iter.cur);
		hold = isCoordinatorHoldMgrNode(coordinator,
										gtmCoordMaster,
										false);
		if (hold != COORDINATOR_HOLD_MGRNODE_NO)
		{
			mayHold = true;
			break;
		}
	}
	pfreeMgrNodeWrapperList(&coordinators, NULL);
	return mayHold;
}

static bool isAllCoordinatorsHoldGtmCoordMaster(MgrNodeWrapper *gtmCoordMaster,
												MemoryContext spiContext)
{
	dlist_head coordinators = DLIST_STATIC_INIT(coordinators);
	dlist_iter iter;
	MgrNodeWrapper *coordinator;
	bool allHold;
	CoordinatorHoldMgrNode hold;

	selectActiveMgrNodeByNodetype(spiContext,
								  CNDN_TYPE_COORDINATOR_MASTER,
								  &coordinators);
	allHold = true;
	dlist_foreach(iter, &coordinators)
	{
		coordinator = dlist_container(MgrNodeWrapper, link, iter.cur);
		hold = isCoordinatorHoldMgrNode(coordinator,
										gtmCoordMaster,
										false);
		if (hold != COORDINATOR_HOLD_MGRNODE_YES)
		{
			allHold = false;
			break;
		}
	}
	pfreeMgrNodeWrapperList(&coordinators, NULL);
	return allHold;
}

static CoordinatorHoldMgrNode isCoordinatorHoldMgrNode(MgrNodeWrapper *coordinator,
													   MgrNodeWrapper *mgrNode,
													   bool complain)
{
	PGconn *pgconn = NULL;
	CoordinatorHoldMgrNode hold = COORDINATOR_HOLD_MGRNODE_UNKNOWN;

	pgconn = getNodeDefaultDBConnection(coordinator, 10);
	if (!pgconn)
	{
		ereport(complain ? ERROR : LOG,
				(errmsg("connect to coordinator %s failed",
						NameStr(coordinator->form.nodename))));
		hold = COORDINATOR_HOLD_MGRNODE_UNKNOWN;
	}
	else
	{
		if (getNodeRunningMode(pgconn) != NODE_RUNNING_MODE_MASTER)
		{
			if (pgconn)
			{
				PQfinish(pgconn);
				pgconn = NULL;
			}
			ereport(complain ? ERROR : LOG,
					(errmsg("coordinator %s configured as master, "
							"but actually did not running on that status",
							NameStr(coordinator->form.nodename))));
			hold = COORDINATOR_HOLD_MGRNODE_UNKNOWN;
		}
		else
		{
			if (isMgrModeExistsInCoordinator(coordinator,
											 pgconn,
											 true,
											 mgrNode,
											 complain))
			{
				hold = COORDINATOR_HOLD_MGRNODE_YES;
				ereport(LOG,
						(errmsg("%s exsits in pgxc_node of coordinator %s",
								NameStr(mgrNode->form.nodename),
								NameStr(coordinator->form.nodename))));
			}
			else
			{
				hold = COORDINATOR_HOLD_MGRNODE_NO;
				ereport(LOG,
						(errmsg("%s does not exsits in pgxc_node of coordinator %s",
								NameStr(mgrNode->form.nodename),
								NameStr(coordinator->form.nodename))));
			}
		}
	}

	if (pgconn)
	{
		PQfinish(pgconn);
		pgconn = NULL;
	}
	return hold;
}

static bool isAnyCoordinatorsQueryOk(MemoryContext spiContext)
{
	dlist_head coordinators = DLIST_STATIC_INIT(coordinators);
	dlist_iter iter;
	MgrNodeWrapper *coordinator;
	bool anyQueryOk;

	selectActiveMgrNodeByNodetype(spiContext,
								  CNDN_TYPE_COORDINATOR_MASTER,
								  &coordinators);
	anyQueryOk = false;
	dlist_foreach(iter, &coordinators)
	{
		coordinator = dlist_container(MgrNodeWrapper, link, iter.cur);
		if (isCoordinatorQueryOk(coordinator))
		{
			anyQueryOk = true;
			break;
		}
		else
		{
			anyQueryOk = false;
			continue;
		}
	}
	pfreeMgrNodeWrapperList(&coordinators, NULL);
	return anyQueryOk;
}

static bool isCoordinatorQueryOk(MgrNodeWrapper *coordinator)
{
	PGconn *pgconn = NULL;
	bool queryOk = false;

	pgconn = getNodeDefaultDBConnection(coordinator, 10);
	if (!pgconn)
	{
		queryOk = false;
	}
	else
	{
		queryOk = (getNodeRunningMode(pgconn) == NODE_RUNNING_MODE_MASTER);
	}

	if (pgconn)
	{
		PQfinish(pgconn);
		pgconn = NULL;
	}
	return queryOk;
}

static void checkMgrNodeDataInDB(MgrNodeWrapper *nodeDataInMem,
								 MemoryContext spiContext)
{
	MgrNodeWrapper *nodeDataInDB;

	nodeDataInDB = selectMgrNodeByOid(nodeDataInMem->form.oid, spiContext);
	if (!nodeDataInDB)
	{
		ereport(ERROR,
				(errmsg("%s %s, data not exists in database",
						MyBgworkerEntry->bgw_name,
						NameStr(nodeDataInDB->form.nodename))));
	}
	if (!nodeDataInDB->form.allowcure)
	{
		ereport(ERROR,
				(errmsg("%s %s, cure not allowed",
						MyBgworkerEntry->bgw_name,
						NameStr(nodeDataInDB->form.nodename))));
	}
	if (nodeDataInDB->form.nodetype != CNDN_TYPE_DATANODE_MASTER &&
		nodeDataInDB->form.nodetype != CNDN_TYPE_GTM_COOR_MASTER)
	{
		ereport(ERROR,
				(errmsg("only 'data node' or 'gtm coordinator' switching is supported")));
	}
	if (pg_strcasecmp(NameStr(nodeDataInDB->form.curestatus),
					  CURE_STATUS_WAIT_SWITCH) != 0 &&
		pg_strcasecmp(NameStr(nodeDataInDB->form.curestatus),
					  CURE_STATUS_SWITCHING) != 0)
	{
		ereport(ERROR,
				(errmsg("%s %s, curestatus:%s, it is not my duty",
						MyBgworkerEntry->bgw_name,
						NameStr(nodeDataInDB->form.nodename),
						NameStr(nodeDataInDB->form.curestatus))));
	}
	if (pg_strcasecmp(NameStr(nodeDataInMem->form.curestatus),
					  NameStr(nodeDataInDB->form.curestatus)) != 0)
	{
		ereport(ERROR,
				(errmsg("%s %s, curestatus not matched, in memory:%s, but in database:%s",
						MyBgworkerEntry->bgw_name,
						NameStr(nodeDataInDB->form.nodename),
						NameStr(nodeDataInMem->form.curestatus),
						NameStr(nodeDataInDB->form.curestatus))));
	}
	if (!isIdenticalDoctorMgrNode(nodeDataInMem, nodeDataInDB))
	{
		ereport(ERROR,
				(errmsg("%s %s, data has changed in database",
						MyBgworkerEntry->bgw_name,
						NameStr(nodeDataInDB->form.nodename))));
	}
	pfreeMgrNodeWrapper(nodeDataInDB);
	nodeDataInDB = NULL;
}

static void getCheckMgrNodesForSwitcher(dlist_head *nodes)
{
	MemoryContext oldContext;
	MemoryContext spiContext;
	int ret;

	oldContext = CurrentMemoryContext;
	SPI_CONNECT_TRANSACTIONAL_START(ret, true);
	spiContext = CurrentMemoryContext;
	MemoryContextSwitchTo(oldContext);
	selectMgrNodesForSwitcherDoctor(spiContext, nodes);
	SPI_FINISH_TRANSACTIONAL_COMMIT();
	if (dlist_is_empty(nodes))
	{
		ereport(ERROR,
				(errmsg("%s there are no nodes that need to perform master/slave switching",
						MyBgworkerEntry->bgw_name)));
	}
}

static void classifySlaveNodesByIfRunning(MgrNodeWrapper *masterNode,
										  MemoryContext spiContext,
										  dlist_head *runningSlaves,
										  dlist_head *failedSlaves)
{
	dlist_head mgrNodes = DLIST_STATIC_INIT(mgrNodes);
	dlist_head slaveNodes = DLIST_STATIC_INIT(slaveNodes);
	SwitcherNodeWrapper *node;
	dlist_mutable_iter miter;

	selectActiveMgrSlaveNodes(masterNode->form.oid,
							  getMgrSlaveNodetype(masterNode->form.nodetype),
							  spiContext,
							  &mgrNodes);

	mgrNodesToSwitcherNodes(&mgrNodes,
							&slaveNodes);

	dlist_foreach_modify(miter, &slaveNodes)
	{
		node = dlist_container(SwitcherNodeWrapper, link, miter.cur);
		node->pgConn = getNodeDefaultDBConnection(node->mgrNode, 10);
		if (node->pgConn)
		{
			ereport(DEBUG1,
					(errmsg("connect to node %s successfully",
							NameStr(node->mgrNode->form.nodename))));
			dlist_push_tail(runningSlaves, &node->link);
		}
		else
		{
			ereport(DEBUG1,
					(errmsg("connect to node %s failed",
							NameStr(node->mgrNode->form.nodename))));
			dlist_push_tail(failedSlaves, &node->link);
		}
	}
	/* add isolated slave node to failedSlaves */
	dlist_init(&mgrNodes);
	selectIsolatedMgrSlaveNodes(masterNode->form.oid,
								getMgrSlaveNodetype(masterNode->form.nodetype),
								spiContext,
								&mgrNodes);
	mgrNodesToSwitcherNodes(&mgrNodes,
							failedSlaves);
}

static SwitcherConfiguration *newSwitcherConfiguration(AdbDoctorConf *conf)
{
	SwitcherConfiguration *sc;

	checkAdbDoctorConf(conf);

	sc = palloc0(sizeof(SwitcherConfiguration));

	sc->switchIntervalMs = conf->switchinterval * 1000L;
	sc->forceSwitch = conf->forceswitch;
	sc->rewindOldMaster = conf->rewindoldmaster;
	ereport(DEBUG1,
			(errmsg("%s configuration: "
					"switchIntervalMs:%ld, "
					"forceSwitch:%d, "
					"rewindOldMaster:%d",
					MyBgworkerEntry->bgw_name,
					sc->switchIntervalMs,
					sc->forceSwitch,
					sc->rewindOldMaster)));
	return sc;
}

static void examineAdbDoctorConf(dlist_head *oldMasters)
{
	AdbDoctorConf *confInLocal;
	dlist_head freshMgrNodes = DLIST_STATIC_INIT(freshMgrNodes);
	if (gotSigusr1)
	{
		gotSigusr1 = false;

		confInLocal = copyAdbDoctorConfFromShm(confShm);
		pfree(switcherConfiguration);
		switcherConfiguration = newSwitcherConfiguration(confInLocal);
		pfree(confInLocal);

		ereport(LOG,
				(errmsg("%s, Refresh configuration completed",
						MyBgworkerEntry->bgw_name)));

		getCheckMgrNodesForSwitcher(&freshMgrNodes);
		if (isIdenticalDoctorMgrNodes(&freshMgrNodes, oldMasters))
		{
			pfreeMgrNodeWrapperList(&freshMgrNodes, NULL);
		}
		else
		{
			pfreeMgrNodeWrapperList(&freshMgrNodes, NULL);
			resetSwitcher();
		}
	}
}

static void resetSwitcher()
{
	ereport(LOG,
			(errmsg("%s, reset switcher",
					MyBgworkerEntry->bgw_name)));
	siglongjmp(reset_switcher_sigjmp_buf, 1);
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