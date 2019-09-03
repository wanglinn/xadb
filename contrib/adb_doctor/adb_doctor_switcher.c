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

typedef struct SwitcherConfiguration
{
	long switchIntervalMs;
	bool forceSwitch;
} SwitcherConfiguration;

static void switcherMainLoop(dlist_head *switcherNodes);
static bool checkAndSwitchMaster(SwitcherNodeWrapper *oldMaster);
static void checkAndSwitchDataNodeMaster(SwitcherNodeWrapper *oldMaster,
										 SwitcherNodeWrapper **newMasterP,
										 dlist_head *runningSlaves,
										 dlist_head *failedSlaves,
										 dlist_head *coordinators,
										 MemoryContext spiContext);
static void checkAndSwitchGtmCoordMaster(SwitcherNodeWrapper *oldMaster,
										 SwitcherNodeWrapper **newMasterP,
										 dlist_head *runningSlaves,
										 dlist_head *failedSlaves,
										 dlist_head *coordinators,
										 dlist_head *runningDataNodes,
										 dlist_head *failedDataNodes,
										 MemoryContext spiContext);
static bool isNewMasterEligible(SwitcherNodeWrapper *oldMaster,
								SwitcherNodeWrapper *newMaster);
static void checkMgrNodeDataInDB(MgrNodeWrapper *nodeDataInMem,
								 MemoryContext spiContext);
static void updateCurestatusToNormal(MgrNodeWrapper *node,
									 MemoryContext spiContext);
static void getCheckMgrNodesForSwitcher(dlist_head *nodes);
static SwitcherConfiguration *newSwitcherConfiguration(AdbDoctorConf *conf);
static void examineAdbDoctorConf(dlist_head *switcherNodes);
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
	dlist_head switcherNodes = DLIST_STATIC_INIT(switcherNodes);
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
			pfreeSwitcherNodeWrapperList(&switcherNodes, NULL);
		}
		dlist_init(&switcherNodes);
		dlist_init(&mgrNodes);

		getCheckMgrNodesForSwitcher(&mgrNodes);
		mgrNodesToSwitcherNodes(&mgrNodes, &switcherNodes);
		switcherMainLoop(&switcherNodes);
	}
	PG_CATCH();
	{
		PG_RE_THROW();
	}
	PG_END_TRY();
	proc_exit(1);
}

static void switcherMainLoop(dlist_head *switcherNodes)
{
	int rc;
	dlist_mutable_iter miter;
	SwitcherNodeWrapper *oldMaster;

	while (!gotSigterm)
	{
		dlist_foreach_modify(miter, switcherNodes)
		{
			oldMaster = dlist_container(SwitcherNodeWrapper, link, miter.cur);
			/* do switch */
			if (checkAndSwitchMaster(oldMaster))
			{
				dlist_delete(miter.cur);
				pfreeSwitcherNodeWrapper(oldMaster);
			}
			CHECK_FOR_INTERRUPTS();
			examineAdbDoctorConf(switcherNodes);
		}
		if (dlist_is_empty(switcherNodes))
		{
			/* The switch task was completed, the process should exits */
			break;
		}
		set_ps_display("sleeping", false);
		rc = WaitLatchOrSocket(MyLatch,
							   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
							   PGINVALID_SOCKET,
							   switcherConfiguration->switchIntervalMs,
							   PG_WAIT_EXTENSION);
		/* Reset the latch, bail out if postmaster died. */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
		/* Interrupted? */
		if (rc & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
		}
		CHECK_FOR_INTERRUPTS();
		examineAdbDoctorConf(switcherNodes);
	}
}

static bool checkAndSwitchMaster(SwitcherNodeWrapper *oldMaster)
{
	volatile bool done = false;
	int spiRes;
	SwitcherNodeWrapper *newMaster = NULL;
	dlist_head failedSlaves = DLIST_STATIC_INIT(failedSlaves);
	dlist_head runningSlaves = DLIST_STATIC_INIT(runningSlaves);
	dlist_head coordinators = DLIST_STATIC_INIT(coordinators);
	dlist_head runningDataNodes = DLIST_STATIC_INIT(runningDataNodes);
	dlist_head failedDataNodes = DLIST_STATIC_INIT(failedDataNodes);
	MemoryContext oldContext;
	MemoryContext switchContext;
	MemoryContext spiContext;

	set_ps_display(NameStr(oldMaster->mgrNode->form.nodename), false);

	oldContext = CurrentMemoryContext;
	switchContext = AllocSetContextCreate(oldContext,
										  "checkAndSwitchMaster",
										  ALLOCSET_DEFAULT_SIZES);
	SPI_CONNECT_TRANSACTIONAL_START(spiRes, true);
	spiContext = CurrentMemoryContext;
	MemoryContextSwitchTo(switchContext);

	PG_TRY();
	{
		if (oldMaster->mgrNode->form.nodetype == CNDN_TYPE_DATANODE_MASTER)
		{
			checkAndSwitchDataNodeMaster(oldMaster,
										 &newMaster,
										 &runningSlaves,
										 &failedSlaves,
										 &coordinators,
										 spiContext);
		}
		else if (oldMaster->mgrNode->form.nodetype == CNDN_TYPE_GTM_COOR_MASTER)
		{
			checkAndSwitchGtmCoordMaster(oldMaster,
										 &newMaster,
										 &runningSlaves,
										 &failedSlaves,
										 &coordinators,
										 &runningDataNodes,
										 &failedDataNodes,
										 spiContext);
		}
		else
		{
			/* mgr_node data may be changed in database */
			resetSwitcher();
		}
		done = true;
	}
	PG_CATCH();
	{
		done = false;
		EmitErrorReport();
		FlushErrorState();

		revertClusterSetting(&coordinators, oldMaster, newMaster);
		/* do not throw this exception */
	}
	PG_END_TRY();

	pfreeSwitcherNodeWrapperList(&failedSlaves, NULL);
	pfreeSwitcherNodeWrapperList(&runningSlaves, NULL);
	/* When switching gtm, newMaster will be added in coordinators. */
	pfreeSwitcherNodeWrapperList(&coordinators, newMaster);
	pfreeSwitcherNodeWrapperList(&runningDataNodes, NULL);
	pfreeSwitcherNodeWrapperList(&failedDataNodes, NULL);
	pfreeSwitcherNodeWrapperPGconn(oldMaster);
	pfreeSwitcherNodeWrapper(newMaster);

	(void)MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(switchContext);

	if (done)
	{
		SPI_FINISH_TRANSACTIONAL_COMMIT();
	}
	else
	{
		SPI_FINISH_TRANSACTIONAL_ABORT();
	}

	return done;
}

static void checkAndSwitchDataNodeMaster(SwitcherNodeWrapper *oldMaster,
										 SwitcherNodeWrapper **newMasterP,
										 dlist_head *runningSlaves,
										 dlist_head *failedSlaves,
										 dlist_head *coordinators,
										 MemoryContext spiContext)
{
	checkMgrNodeDataInDB(oldMaster->mgrNode, spiContext);
	checkSwitchDataNodePrerequisite(oldMaster,
									runningSlaves,
									failedSlaves,
									coordinators,
									spiContext,
									switcherConfiguration->forceSwitch);
	oldMaster->pgConn = getNodeDefaultDBConnection(oldMaster->mgrNode, 10);
	if (oldMaster->pgConn &&
		checkNodeRunningMode(oldMaster->pgConn, true))
	{
		oldMaster->runningMode = NODE_RUNNING_MODE_MASTER;
		oldMaster->walLsn = getNodeWalLsn(oldMaster->pgConn,
										  oldMaster->runningMode);
		chooseNewMasterNode(oldMaster,
							newMasterP,
							runningSlaves,
							failedSlaves,
							spiContext,
							switcherConfiguration->forceSwitch);
		if (isNewMasterEligible(oldMaster, (*newMasterP)))
		{
			switchToDataNodeNewMaster(oldMaster,
									  (*newMasterP),
									  runningSlaves,
									  failedSlaves,
									  coordinators,
									  spiContext,
									  false);
		}
		else
		{
			ereport(LOG,
					(errmsg("%s %s old master back to normal, abort switch",
							MyBgworkerEntry->bgw_name,
							NameStr(oldMaster->mgrNode->form.nodename))));
			updateCurestatusToNormal(oldMaster->mgrNode, spiContext);
			callAgentStartNode(oldMaster->mgrNode, true, false);
		}
	}
	else
	{
		chooseNewMasterNode(oldMaster,
							newMasterP,
							runningSlaves,
							failedSlaves,
							spiContext,
							switcherConfiguration->forceSwitch);
		switchToDataNodeNewMaster(oldMaster,
								  (*newMasterP),
								  runningSlaves,
								  failedSlaves,
								  coordinators,
								  spiContext,
								  false);
	}
}

static void checkAndSwitchGtmCoordMaster(SwitcherNodeWrapper *oldMaster,
										 SwitcherNodeWrapper **newMasterP,
										 dlist_head *runningSlaves,
										 dlist_head *failedSlaves,
										 dlist_head *coordinators,
										 dlist_head *runningDataNodes,
										 dlist_head *failedDataNodes,
										 MemoryContext spiContext)
{
	checkMgrNodeDataInDB(oldMaster->mgrNode, spiContext);
	checkSwitchGtmCoordPrerequisite(oldMaster,
									runningSlaves,
									failedSlaves,
									coordinators,
									runningDataNodes,
									failedDataNodes,
									spiContext,
									switcherConfiguration->forceSwitch);
	oldMaster->pgConn = getNodeDefaultDBConnection(oldMaster->mgrNode, 10);
	if (oldMaster->pgConn &&
		checkNodeRunningMode(oldMaster->pgConn, true))
	{
		oldMaster->runningMode = NODE_RUNNING_MODE_MASTER;
		oldMaster->walLsn = getNodeWalLsn(oldMaster->pgConn,
										  oldMaster->runningMode);
		chooseNewMasterNode(oldMaster,
							newMasterP,
							runningSlaves,
							failedSlaves,
							spiContext,
							switcherConfiguration->forceSwitch);

		if (isNewMasterEligible(oldMaster, (*newMasterP)))
		{
			switchToGtmCoordNewMaster(oldMaster,
									  (*newMasterP),
									  runningSlaves,
									  failedSlaves,
									  coordinators,
									  runningDataNodes,
									  spiContext,
									  false);
		}
		else
		{
			ereport(LOG,
					(errmsg("%s %s old master back to normal, abort switch",
							MyBgworkerEntry->bgw_name,
							NameStr(oldMaster->mgrNode->form.nodename))));
			updateCurestatusToNormal(oldMaster->mgrNode, spiContext);
			callAgentStartNode(oldMaster->mgrNode, true, false);
		}
	}
	else
	{
		chooseNewMasterNode(oldMaster,
							newMasterP,
							runningSlaves,
							failedSlaves,
							spiContext,
							switcherConfiguration->forceSwitch);
		switchToGtmCoordNewMaster(oldMaster,
								  (*newMasterP),
								  runningSlaves,
								  failedSlaves,
								  coordinators,
								  runningDataNodes,
								  spiContext,
								  false);
	}
}

/*
 * When a slave node is running in the master mode, it indicates that 
 * this node may be choosed as new master in the latest switch operation, 
 * but due to some exceptions, the switch operation is not completely 
 * successful. So when this node lsn is bigger than the old master, 
 * we will continue to promote this node as the new master. 
 */
static bool isNewMasterEligible(SwitcherNodeWrapper *oldMaster,
								SwitcherNodeWrapper *newMaster)
{
	return newMaster != NULL &&
		   newMaster->runningMode == NODE_RUNNING_MODE_MASTER &&
		   newMaster->walLsn >= oldMaster->walLsn &&
		   newMaster->walLsn > InvalidXLogRecPtr;
}

static void checkMgrNodeDataInDB(MgrNodeWrapper *nodeDataInMem,
								 MemoryContext spiContext)
{
	MgrNodeWrapper *nodeDataInDB;

	nodeDataInDB = selectMgrNodeByOid(nodeDataInMem->oid, spiContext);
	if (!nodeDataInDB)
	{
		ereport(ERROR,
				(errmsg("%s %s, data not exists in database",
						MyBgworkerEntry->bgw_name,
						NameStr(nodeDataInDB->form.nodename))));
	}
	if (!nodeDataInDB->form.allowcure)
	{
		pfreeMgrNodeWrapper(nodeDataInDB);
		ereport(ERROR,
				(errmsg("%s %s, cure not allowed",
						MyBgworkerEntry->bgw_name,
						NameStr(nodeDataInDB->form.nodename))));
	}
	if (nodeDataInDB->form.nodetype != CNDN_TYPE_DATANODE_MASTER &&
		nodeDataInDB->form.nodetype != CNDN_TYPE_GTM_COOR_MASTER)
	{
		pfreeMgrNodeWrapper(nodeDataInDB);
		ereport(ERROR,
				(errmsg("only 'data node' or 'gtm coordinator' switching is supported")));
	}
	if (pg_strcasecmp(NameStr(nodeDataInDB->form.curestatus),
					  CURE_STATUS_WAIT_SWITCH) != 0 &&
		pg_strcasecmp(NameStr(nodeDataInDB->form.curestatus),
					  CURE_STATUS_SWITCHING) != 0)
	{
		pfreeMgrNodeWrapper(nodeDataInDB);
		ereport(ERROR,
				(errmsg("%s %s, curestatus:%s, it is not my duty",
						MyBgworkerEntry->bgw_name,
						NameStr(nodeDataInDB->form.nodename),
						NameStr(nodeDataInDB->form.curestatus))));
	}
	if (pg_strcasecmp(NameStr(nodeDataInMem->form.curestatus),
					  NameStr(nodeDataInDB->form.curestatus)) != 0)
	{
		pfreeMgrNodeWrapper(nodeDataInDB);
		ereport(ERROR,
				(errmsg("%s %s, curestatus not matched, in memory:%s, but in database:%s",
						MyBgworkerEntry->bgw_name,
						NameStr(nodeDataInDB->form.nodename),
						NameStr(nodeDataInMem->form.curestatus),
						NameStr(nodeDataInDB->form.curestatus))));
	}
	if (!isIdenticalDoctorMgrNode(nodeDataInMem, nodeDataInDB))
	{
		pfreeMgrNodeWrapper(nodeDataInDB);
		ereport(ERROR,
				(errmsg("%s %s, data has changed in database",
						MyBgworkerEntry->bgw_name,
						NameStr(nodeDataInDB->form.nodename))));
	}
	pfreeMgrNodeWrapper(nodeDataInDB);
	nodeDataInDB = NULL;
}

static void updateCurestatusToNormal(MgrNodeWrapper *node,
									 MemoryContext spiContext)
{
	int rows;
	char *newCurestatus;

	newCurestatus = CURE_STATUS_NORMAL;

	rows = updateMgrNodeCurestatus(node, newCurestatus, spiContext);
	if (rows != 1)
	{
		ereport(ERROR,
				(errmsg("%s, curestatus can not transit to:%s",
						NameStr(node->form.nodename),
						newCurestatus)));
	}
	else
	{
		namestrcpy(&node->form.curestatus, newCurestatus);
	}
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
				(errmsg("%s There is no node to switch",
						MyBgworkerEntry->bgw_name)));
	}
}

static SwitcherConfiguration *newSwitcherConfiguration(AdbDoctorConf *conf)
{
	SwitcherConfiguration *sc;

	checkAdbDoctorConf(conf);

	sc = palloc0(sizeof(SwitcherConfiguration));

	sc->switchIntervalMs = conf->switchinterval * 1000L;
	sc->forceSwitch = conf->forceswitch;
	ereport(LOG,
			(errmsg("%s configuration: "
					"switchIntervalMs:%ld, forceSwitch:%d",
					MyBgworkerEntry->bgw_name,
					sc->switchIntervalMs, sc->forceSwitch)));
	return sc;
}

static void examineAdbDoctorConf(dlist_head *switcherNodes)
{
	AdbDoctorConf *confInLocal;
	dlist_head freshMgrNodes = DLIST_STATIC_INIT(freshMgrNodes);
	dlist_head staleMgrNodes = DLIST_STATIC_INIT(staleMgrNodes);
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
		switcherNodesToMgrNodes(switcherNodes, &staleMgrNodes);
		if (isIdenticalDoctorMgrNodes(&freshMgrNodes, &staleMgrNodes))
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