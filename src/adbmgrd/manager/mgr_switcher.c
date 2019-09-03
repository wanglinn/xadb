/*--------------------------------------------------------------------------
 *
 * Copyright (c) 2018-2019, Asiainfo Database Innovation Lab
 *
 * Switching is not allowed to go back, always going forward.
 * 
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "mgr/mgr_switcher.h"
#include "mgr/mgr_msg_type.h"
#include "mgr/mgr_cmds.h"
#include "access/xlog.h"
#include "access/htup_details.h"
#include "executor/spi.h"
#include "utils/memutils.h"
#include "utils/builtins.h"
#include "../../src/interfaces/libpq/libpq-fe.h"
#include "../../src/interfaces/libpq/libpq-int.h"
#include "catalog/pgxc_node.h"

#define IS_EMPTY_STRING(str) (str == NULL || strlen(str) == 0)

static SwitcherNodeWrapper *checkGetDataNodeOldMaster(char *oldMasterName,
													  MemoryContext spiContext);
static SwitcherNodeWrapper *checkGetGtmCoordOldMaster(char *oldMasterName,
													  MemoryContext spiContext);
static void checkGetMasterCoordinators(MemoryContext spiContext,
									   dlist_head *coordinators,
									   bool includeGtmCoord,
									   bool checkRunningMode);
static void checkGetAllDataNodes(dlist_head *runningDataNodes,
								 dlist_head *failedDataNodes,
								 MemoryContext spiContext);
static void checkGetSlaveNodesRunningStatus(SwitcherNodeWrapper *masterNode,
											MemoryContext spiContext,
											bool forceSwitch,
											dlist_head *failedSlaves,
											dlist_head *runningSlaves);
static void precheckPromotionNode(dlist_head *runningSlaves, bool forceSwitch);
static SwitcherNodeWrapper *getBestWalLsnSlaveNode(dlist_head *runningSlaves,
												   dlist_head *failedSlaves,
												   bool forceSwitch);
static void sortNodesByWalLsnDesc(dlist_head *nodes);
static void restoreCoordinatorSetting(SwitcherNodeWrapper *coordinator);
static bool tryConnectNode(SwitcherNodeWrapper *node, int connectTimeout);
static void checkSlavesRunningStatus(dlist_head *slaveNodes,
									 dlist_head *failedSlaves,
									 dlist_head *runningSlaves);
static void runningSlavesFollowMaster(SwitcherNodeWrapper *masterNode,
									  dlist_head *runningSlaves,
									  MgrNodeWrapper *gtmMaster);
static void appendSyncStandbyNames(MgrNodeWrapper *masterNode,
								   MgrNodeWrapper *slaveNode,
								   PGconn *masterPGconn);
static int walLsnDesc(const void *node1, const void *node2);
static void checkSet_pool_release_to_idle_timeout(SwitcherNodeWrapper *node);
static void waitForNodeRunningOk(MgrNodeWrapper *mgrNode,
								 bool isMaster,
								 PGconn **pgConnP,
								 NodeRunningMode *runningModeP);
static void refreshOldMasterBeforeSwitch(SwitcherNodeWrapper *oldMaster,
										 MemoryContext spiContext);
static void refreshSlaveNodesBeforeSwitch(SwitcherNodeWrapper *newMaster,
										  dlist_head *runningSlaves,
										  dlist_head *failedSlaves,
										  MemoryContext spiContext);
static void refreshMgrNodeAfterSwitch(SwitcherNodeWrapper *oldMaster,
									  SwitcherNodeWrapper *newMaster,
									  dlist_head *runningSlaves,
									  dlist_head *failedSlaves,
									  MemoryContext spiContext,
									  bool kickOutOldMaster);
static void refreshMgrUpdateparmAfterSwitch(MgrNodeWrapper *oldMaster,
											MgrNodeWrapper *newMaster,
											MemoryContext spiContext,
											bool kickOutOldMaster);
static void refreshPgxcNodesAfterSwitchDataNode(dlist_head *coordinators,
												MgrNodeWrapper *oldMaster,
												MgrNodeWrapper *newMaster);
static void refreshPgxcNodesAfterSwitchGtmCoord(dlist_head *coordinators,
												SwitcherNodeWrapper *newMaster);
static void updateCureStatusForSwitch(MgrNodeWrapper *mgrNode,
									  char *newCurestatus,
									  MemoryContext spiContext);
static void updateMgrNodeAfterSwitch(MgrNodeWrapper *mgrNode,
									 char *newCurestatus,
									 MemoryContext spiContext);
static void deleteMgrUpdateparmByNodenameType(char *updateparmnodename,
											  char updateparmnodetype,
											  MemoryContext spiContext);
static void updateMgrUpdateparmNodetype(char *nodename, char nodetype,
										MemoryContext spiContext);
static bool existsPgxcNodeDataNodeMaster(PGconn *activeCoordinatorCoon,
										 char *coordinatorName,
										 bool localExecute,
										 char *dataNodeName);
static bool deletePgxcNodeDataNodeSlaves(PGconn *activeCoordinatorCoon,
										 char *coordinatorName,
										 bool localExecute);
static bool updatePgxcNodeAfterSwitchDataNode(PGconn *activeCoordinatorCoon,
											  char *coordinatorName,
											  bool localExecute,
											  MgrNodeWrapper *oldMaster,
											  MgrNodeWrapper *newMaster,
											  bool complain);
static bool updatePgxcNodeAfterSwitchGtmCoord(PGconn *activeCoordinatorCoon,
											  char *coordinatorName,
											  SwitcherNodeWrapper *newGtmMaster,
											  bool complain);
static bool updateAdbSlot(SwitcherNodeWrapper *coordinator,
						  MgrNodeWrapper *oldMaster,
						  MgrNodeWrapper *newMaster,
						  bool complain);
static bool pgxcPoolReload(PGconn *activeCoordinatorCoon,
						   char *coordinatorName,
						   bool localExecute,
						   bool complain);
static SwitcherNodeWrapper *getHoldLockCoordinator(dlist_head *coordinators);
static SwitcherNodeWrapper *getGtmCoordMaster(dlist_head *coordinators);
static void batchSetGtmInfoOnNodes(MgrNodeWrapper *gtmMaster,
								   dlist_head *nodes,
								   SwitcherNodeWrapper *ignoreNode);
static void batchCheckGtmInfoOnNodes(MgrNodeWrapper *gtmMaster,
									 dlist_head *nodes,
									 SwitcherNodeWrapper *ignoreNode,
									 int checkSeconds);
static void batchSetCheckGtmInfoOnNodes(MgrNodeWrapper *gtmMaster,
										dlist_head *nodes,
										SwitcherNodeWrapper *ignoreNode);

/**
 * system function of failover data node
 */
Datum mgr_failover_one_dn(PG_FUNCTION_ARGS)
{
	HeapTuple tup_result;
	char *nodename;
	bool force;
	NameData newMasterName;

	nodename = PG_GETARG_CSTRING(0);
	force = PG_GETARG_BOOL(1);

	if (RecoveryInProgress())
		ereport(ERROR,
				(errmsg("cannot assign TransactionIds during recovery")));

	switchDataNodeMaster(nodename, force, false, &newMasterName);

	tup_result = build_common_command_tuple(&newMasterName,
											1, "promotion success");
	return HeapTupleGetDatum(tup_result);
}

/**
 * system function of failover gtm node
 */
Datum mgr_failover_gtm(PG_FUNCTION_ARGS)
{
	HeapTuple tup_result;
	char *nodename;
	bool force;
	NameData newMasterName;

	nodename = PG_GETARG_CSTRING(0);
	force = PG_GETARG_BOOL(1);

	if (RecoveryInProgress())
		ereport(ERROR,
				(errmsg("cannot assign TransactionIds during recovery")));

	switchGtmCoordMaster(nodename, force, false, &newMasterName);

	tup_result = build_common_command_tuple(&newMasterName,
											1, "promotion success");
	return HeapTupleGetDatum(tup_result);
}

/**
 * If the switch is forced, it means that data loss can be tolerated,
 * Though it may cause data loss, but that is acceptable.
 * If not force, allow switching without losing any data, In other words,
 * all slave nodes must running normally, and then pick the one which 
 * hold the biggest wal lsn as the new master.
 */
void switchDataNodeMaster(char *oldMasterName,
						  bool forceSwitch,
						  bool kickOutOldMaster,
						  Name newMasterName)
{
	int spiRes;
	SwitcherNodeWrapper *oldMaster = NULL;
	SwitcherNodeWrapper *newMaster = NULL;
	dlist_head coordinators = DLIST_STATIC_INIT(coordinators);
	dlist_head runningSlaves = DLIST_STATIC_INIT(runningSlaves);
	dlist_head failedSlaves = DLIST_STATIC_INIT(failedSlaves);
	MemoryContext oldContext;
	MemoryContext switchContext;
	MemoryContext spiContext;
	ErrorData *edata = NULL;

	oldContext = CurrentMemoryContext;
	switchContext = AllocSetContextCreate(oldContext,
										  "switchDataNodeMaster",
										  ALLOCSET_DEFAULT_SIZES);
	spiRes = SPI_connect();
	if (spiRes != SPI_OK_CONNECT)
	{
		ereport(ERROR,
				(errmsg("SPI_connect failed, connect return:%d",
						spiRes)));
	}
	spiContext = CurrentMemoryContext;
	MemoryContextSwitchTo(switchContext);

	PG_TRY();
	{
		oldMaster = checkGetDataNodeOldMaster(oldMasterName,
											  spiContext);
		checkSwitchDataNodePrerequisite(oldMaster,
										&runningSlaves,
										&failedSlaves,
										&coordinators,
										spiContext,
										forceSwitch);
		chooseNewMasterNode(oldMaster,
							&newMaster,
							&runningSlaves,
							&failedSlaves,
							spiContext,
							forceSwitch);
		switchToDataNodeNewMaster(oldMaster,
								  newMaster,
								  &runningSlaves,
								  &failedSlaves,
								  &coordinators,
								  spiContext,
								  kickOutOldMaster);
		namestrcpy(newMasterName,
				   NameStr(newMaster->mgrNode->form.nodename));
	}
	PG_CATCH();
	{
		/* Save error info in our stmt_mcontext */
		MemoryContextSwitchTo(oldContext);
		edata = CopyErrorData();
		FlushErrorState();

		revertClusterSetting(&coordinators, oldMaster, newMaster);
	}
	PG_END_TRY();

	/* pfree data and close PGconn */
	pfreeSwitcherNodeWrapperList(&failedSlaves, NULL);
	pfreeSwitcherNodeWrapperList(&runningSlaves, NULL);
	pfreeSwitcherNodeWrapperList(&coordinators, NULL);
	pfreeSwitcherNodeWrapper(oldMaster);
	pfreeSwitcherNodeWrapper(newMaster);

	(void)MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(switchContext);

	SPI_finish();

	if (edata)
		ReThrowError(edata);
}

void switchGtmCoordMaster(char *oldMasterName,
						  bool forceSwitch,
						  bool kickOutOldMaster,
						  Name newMasterName)
{
	int spiRes;
	SwitcherNodeWrapper *oldMaster = NULL;
	SwitcherNodeWrapper *newMaster = NULL;
	dlist_head coordinators = DLIST_STATIC_INIT(coordinators);
	dlist_head runningSlaves = DLIST_STATIC_INIT(runningSlaves);
	dlist_head failedSlaves = DLIST_STATIC_INIT(failedSlaves);
	dlist_head runningDataNodes = DLIST_STATIC_INIT(runningDataNodes);
	dlist_head failedDataNodes = DLIST_STATIC_INIT(failedDataNodes);
	MemoryContext oldContext;
	MemoryContext switchContext;
	MemoryContext spiContext;
	ErrorData *edata = NULL;

	oldContext = CurrentMemoryContext;
	switchContext = AllocSetContextCreate(oldContext,
										  "switchGtmCoordMaster",
										  ALLOCSET_DEFAULT_SIZES);
	spiRes = SPI_connect();
	if (spiRes != SPI_OK_CONNECT)
	{
		ereport(ERROR,
				(errmsg("SPI_connect failed, connect return:%d",
						spiRes)));
	}
	spiContext = CurrentMemoryContext;
	MemoryContextSwitchTo(switchContext);

	PG_TRY();
	{
		oldMaster = checkGetGtmCoordOldMaster(oldMasterName,
											  spiContext);
		checkSwitchGtmCoordPrerequisite(oldMaster,
										&runningSlaves,
										&failedSlaves,
										&coordinators,
										&runningDataNodes,
										&failedDataNodes,
										spiContext,
										forceSwitch);
		chooseNewMasterNode(oldMaster,
							&newMaster,
							&runningSlaves,
							&failedSlaves,
							spiContext,
							forceSwitch);
		switchToGtmCoordNewMaster(oldMaster,
								  newMaster,
								  &runningSlaves,
								  &failedSlaves,
								  &coordinators,
								  &runningDataNodes,
								  spiContext,
								  kickOutOldMaster);
		namestrcpy(newMasterName,
				   NameStr(newMaster->mgrNode->form.nodename));
	}
	PG_CATCH();
	{
		/* Save error info in our stmt_mcontext */
		MemoryContextSwitchTo(oldContext);
		edata = CopyErrorData();
		FlushErrorState();

		revertClusterSetting(&coordinators, oldMaster, newMaster);
	}
	PG_END_TRY();

	/* pfree data and close PGconn */
	pfreeSwitcherNodeWrapperList(&failedSlaves, NULL);
	pfreeSwitcherNodeWrapperList(&runningSlaves, NULL);
	/* When switching gtm, newMaster will be added in coordinators. */
	pfreeSwitcherNodeWrapperList(&coordinators, newMaster);
	pfreeSwitcherNodeWrapperList(&runningDataNodes, NULL);
	pfreeSwitcherNodeWrapperList(&failedDataNodes, NULL);
	pfreeSwitcherNodeWrapper(oldMaster);
	pfreeSwitcherNodeWrapper(newMaster);

	(void)MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(switchContext);

	SPI_finish();

	if (edata)
		ReThrowError(edata);
}

void checkSwitchDataNodePrerequisite(SwitcherNodeWrapper *oldMaster,
									 dlist_head *runningSlaves,
									 dlist_head *failedSlaves,
									 dlist_head *coordinators,
									 MemoryContext spiContext,
									 bool forceSwitch)
{
	checkGetSlaveNodesRunningStatus(oldMaster,
									spiContext,
									forceSwitch,
									failedSlaves,
									runningSlaves);
	precheckPromotionNode(runningSlaves, forceSwitch);
	checkGetMasterCoordinators(spiContext, coordinators, true, true);
}

void checkSwitchGtmCoordPrerequisite(SwitcherNodeWrapper *oldMaster,
									 dlist_head *runningSlaves,
									 dlist_head *failedSlaves,
									 dlist_head *coordinators,
									 dlist_head *runningDataNodes,
									 dlist_head *failedDataNodes,
									 MemoryContext spiContext,
									 bool forceSwitch)
{
	checkGetSlaveNodesRunningStatus(oldMaster,
									spiContext,
									forceSwitch,
									failedSlaves,
									runningSlaves);
	precheckPromotionNode(runningSlaves, forceSwitch);
	checkGetMasterCoordinators(spiContext, coordinators, false, false);
	checkGetAllDataNodes(runningDataNodes, failedDataNodes, spiContext);
}

/**
 * Switch data node oldMaster to the standby running mode, 
 * and switch data node newMaster to the master running mode, 
 * The runningSlaves follow this newMaster, and the other failedSlaves
 * will update curestatus to followfail, then the operation of following
 * new master will handed to node doctor(see adb_doctor_node_monitor.c). 
 * If any error occurred, will complain.
 */
void switchToDataNodeNewMaster(SwitcherNodeWrapper *oldMaster,
							   SwitcherNodeWrapper *newMaster,
							   dlist_head *runningSlaves,
							   dlist_head *failedSlaves,
							   dlist_head *coordinators,
							   MemoryContext spiContext,
							   bool kickOutOldMaster)
{
	SwitcherNodeWrapper *gtmMaster;

	if (!dlist_is_empty(failedSlaves))
	{
		ereport(WARNING,
				(errmsg("There are some slave nodes failed, "
						"force switch may cause data loss")));
	}

	/* Prevent other doctor processes from manipulating these nodes simultaneously */
	refreshSlaveNodesBeforeSwitch(newMaster,
								  runningSlaves,
								  failedSlaves,
								  spiContext);

	gtmMaster = getGtmCoordMaster(coordinators);
	if (gtmMaster)
		/* ensure gtm info is correct */
		setCheckGtmInfoInPGSqlConf(gtmMaster->mgrNode,
								   newMaster->mgrNode,
								   newMaster->pgConn,
								   true, 10);

	/* Ready to start switching, during the switching process, 
	 * failure is not allowed. If there it is, complain it. */
	tryLockCluster(coordinators);
	/* If the lastest switch failed, it is possible that a standby node 
	 * has been promoted to the master, so it's not need to promote again. */
	if (newMaster->runningMode != NODE_RUNNING_MODE_MASTER)
	{
		/* The old connection may mistake the judgment of whether new master 
		* is running normally. So close it first, we will reconnect this node 
		* after promotion completed. */
		pfreeSwitcherNodeWrapperPGconn(newMaster);
		callAgentPromoteNode(newMaster->mgrNode, true);
	}
	ereport(NOTICE,
			(errmsg("%s was successfully promoted to the new master",
					NameStr(newMaster->mgrNode->form.nodename))));
	ereport(LOG,
			(errmsg("%s was successfully promoted to the new master",
					NameStr(newMaster->mgrNode->form.nodename))));

	waitForNodeRunningOk(newMaster->mgrNode,
						 true,
						 &newMaster->pgConn,
						 &newMaster->runningMode);

	setCheckSynchronousStandbyNames(newMaster->mgrNode,
									newMaster->pgConn,
									"",
									CHECK_SYNC_STANDBY_NAMES_SECONDS);

	/* The better slave node is in front of the list */
	sortNodesByWalLsnDesc(runningSlaves);
	runningSlavesFollowMaster(newMaster, runningSlaves,
							  gtmMaster == NULL ? NULL : gtmMaster->mgrNode);

	refreshPgxcNodesAfterSwitchDataNode(coordinators,
										oldMaster->mgrNode,
										newMaster->mgrNode);

	refreshMgrNodeAfterSwitch(oldMaster,
							  newMaster,
							  runningSlaves,
							  failedSlaves,
							  spiContext,
							  kickOutOldMaster);
	refreshMgrUpdateparmAfterSwitch(oldMaster->mgrNode,
									newMaster->mgrNode,
									spiContext,
									kickOutOldMaster);

	tryUnlockCluster(coordinators, true);

	ereport(NOTICE,
			(errmsg("Switch the data node master from %s to %s "
					"has been successfully completed",
					NameStr(oldMaster->mgrNode->form.nodename),
					NameStr(newMaster->mgrNode->form.nodename))));
	ereport(LOG,
			(errmsg("Switch the data node master from %s to %s "
					"has been successfully completed",
					NameStr(oldMaster->mgrNode->form.nodename),
					NameStr(newMaster->mgrNode->form.nodename))));
}

/**
 * Switch gtm coordinator oldMaster to the standby running mode, 
 * and switch gtm coordinator newMaster to the master running mode, 
 * The runningSlaves follow this newMaster, and the other failedSlaves
 * will update curestatus to followfail, then the operation of following 
 * new master will handed to node doctor(see adb_doctor_node_monitor.c). 
 * If any error occurred, will complain.
 */
void switchToGtmCoordNewMaster(SwitcherNodeWrapper *oldMaster,
							   SwitcherNodeWrapper *newMaster,
							   dlist_head *runningSlaves,
							   dlist_head *failedSlaves,
							   dlist_head *coordinators,
							   dlist_head *runningDataNodes,
							   MemoryContext spiContext,
							   bool kickOutOldMaster)
{
	dlist_iter iter;
	SwitcherNodeWrapper *node;

	if (!dlist_is_empty(failedSlaves))
	{
		ereport(WARNING,
				(errmsg("There are some slave nodes failed, "
						"force switch may cause data loss")));
	}

	/* new master also is a coordinator */
	dlist_push_head(coordinators, &newMaster->link);

	/* Prevent other doctor processes from manipulating these nodes simultaneously */
	refreshSlaveNodesBeforeSwitch(newMaster,
								  runningSlaves,
								  failedSlaves,
								  spiContext);
	dlist_foreach(iter, coordinators)
	{
		node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		if (node != newMaster)
			updateCureStatusForSwitch(node->mgrNode,
									  CURE_STATUS_SWITCHING,
									  spiContext);
	}
	dlist_foreach(iter, runningDataNodes)
	{
		node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		updateCureStatusForSwitch(node->mgrNode,
								  CURE_STATUS_SWITCHING,
								  spiContext);
	}

	setCheckGtmInfoInPGSqlConf(newMaster->mgrNode,
							   newMaster->mgrNode,
							   newMaster->pgConn,
							   true, 10);

	/* If the lastest switch failed, it is possible that a standby node 
	 * has been promoted to the master, so it's not need to promote again. */
	if (newMaster->runningMode != NODE_RUNNING_MODE_MASTER)
	{
		/* The old connection may mistake the judgment of whether new master 
		* is running normally. So close it first, we will reconnect this node 
		* after promotion completed. */
		pfreeSwitcherNodeWrapperPGconn(newMaster);
		callAgentPromoteNode(newMaster->mgrNode, true);
	}
	ereport(NOTICE,
			(errmsg("%s was successfully promoted to the new master",
					NameStr(newMaster->mgrNode->form.nodename))));
	ereport(LOG,
			(errmsg("%s was successfully promoted to the new master",
					NameStr(newMaster->mgrNode->form.nodename))));

	waitForNodeRunningOk(newMaster->mgrNode,
						 true,
						 &newMaster->pgConn,
						 &newMaster->runningMode);

	setCheckSynchronousStandbyNames(newMaster->mgrNode,
									newMaster->pgConn,
									"",
									CHECK_SYNC_STANDBY_NAMES_SECONDS);

	/* The better slave node is in front of the list */
	sortNodesByWalLsnDesc(runningSlaves);
	runningSlavesFollowMaster(newMaster, runningSlaves, NULL);

	batchSetCheckGtmInfoOnNodes(newMaster->mgrNode,
								coordinators,
								newMaster);

	refreshPgxcNodesAfterSwitchGtmCoord(coordinators, newMaster);

	/* Ready to start switching, during the switching process, 
	 * failure is not allowed. If there it is, complain it. */
	tryLockCluster(coordinators);

	batchSetCheckGtmInfoOnNodes(newMaster->mgrNode,
								runningDataNodes,
								newMaster);

	refreshMgrNodeAfterSwitch(oldMaster,
							  newMaster,
							  runningSlaves,
							  failedSlaves,
							  spiContext,
							  kickOutOldMaster);
	refreshMgrUpdateparmAfterSwitch(oldMaster->mgrNode,
									newMaster->mgrNode,
									spiContext,
									kickOutOldMaster);
	dlist_foreach(iter, coordinators)
	{
		node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		if (node != newMaster)
			updateCureStatusForSwitch(node->mgrNode,
									  CURE_STATUS_SWITCHED,
									  spiContext);
	}
	dlist_foreach(iter, runningDataNodes)
	{
		node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		updateCureStatusForSwitch(node->mgrNode,
								  CURE_STATUS_SWITCHED,
								  spiContext);
	}

	tryUnlockCluster(coordinators, true);

	ereport(NOTICE,
			(errmsg("Switch the GTM master from %s to %s "
					"has been successfully completed",
					NameStr(oldMaster->mgrNode->form.nodename),
					NameStr(newMaster->mgrNode->form.nodename))));
	ereport(LOG,
			(errmsg("Switch the GTM master from %s to %s "
					"has been successfully completed",
					NameStr(oldMaster->mgrNode->form.nodename),
					NameStr(newMaster->mgrNode->form.nodename))));
}

static void precheckPromotionNode(dlist_head *runningSlaves, bool forceSwitch)
{
	SwitcherNodeWrapper *node = NULL;
	SwitcherNodeWrapper *syncNode = NULL;
	dlist_mutable_iter miter;

	if (dlist_is_empty(runningSlaves))
	{
		ereport(ERROR,
				(errmsg("Can't find any slave node that can be promoted")));
	}
	if (!forceSwitch)
	{
		/* check sync slave node */
		dlist_foreach_modify(miter, runningSlaves)
		{
			node = dlist_container(SwitcherNodeWrapper, link, miter.cur);
			if (strcmp(NameStr(node->mgrNode->form.nodesync),
					   getMgrNodeSyncStateValue(SYNC_STATE_SYNC)) == 0)
			{
				syncNode = node;
				break;
			}
			else
			{
				continue;
			}
		}
		if (syncNode == NULL)
		{
			ereport(ERROR,
					(errmsg("Can't find a Synchronous standby node, "
							"Abort switching to avoid data loss")));
		}
	}
}

/**
 * Shut the oldMaster down, so as to prevent disturb the determination 
 * of wal lsn. Choose one node which has the best wal lsn from runningSlaves.
 * This choosed node is the expected newMaster.
 */
void chooseNewMasterNode(SwitcherNodeWrapper *oldMaster,
						 SwitcherNodeWrapper **newMasterP,
						 dlist_head *runningSlaves,
						 dlist_head *failedSlaves,
						 MemoryContext spiContext,
						 bool forceSwitch)
{
	SwitcherNodeWrapper *newMaster;

	/* Prevent other doctor processe from manipulating this node simultaneously */
	refreshOldMasterBeforeSwitch(oldMaster, spiContext);

	/* Sentinel, ensure to shut down old master */
	shutdownNodeWithinSeconds(oldMaster->mgrNode,
							  SHUTDOWN_NODE_FAST_SECONDS,
							  SHUTDOWN_NODE_IMMEDIATE_SECONDS,
							  true);

	newMaster = getBestWalLsnSlaveNode(runningSlaves,
									   failedSlaves,
									   forceSwitch);
	*newMasterP = newMaster;
}

void revertClusterSetting(dlist_head *coordinators,
						  SwitcherNodeWrapper *oldMaster,
						  SwitcherNodeWrapper *newMaster)
{
	dlist_iter iter;
	SwitcherNodeWrapper *coordinator;
	SwitcherNodeWrapper *holdLockCoordinator = NULL;
	bool execOk = true;
	PGconn *activeCoordinatorCoon;
	bool localExecute;
	bool revertPgxcNode;

	holdLockCoordinator = getHoldLockCoordinator(coordinators);

	dlist_foreach(iter, coordinators)
	{
		coordinator = dlist_container(SwitcherNodeWrapper, link, iter.cur);

		if (holdLockCoordinator == NULL)
		{
			/* When cluster already unlocked, all connection become active */
			activeCoordinatorCoon = coordinator->pgConn;
			localExecute = true;
		}
		else
		{
			/* When cluster is locked, the connection which 
	 		 * execute the lock command is the only active connection */
			activeCoordinatorCoon = holdLockCoordinator->pgConn;
			localExecute = holdLockCoordinator == coordinator;
		}

		if (coordinator->coordPgxcNodeChanged)
		{
			if (oldMaster->mgrNode->form.nodetype == CNDN_TYPE_GTM_COOR_MASTER ||
				oldMaster->mgrNode->form.nodetype == CNDN_TYPE_GTM_COOR_SLAVE)
			{
				revertPgxcNode =
					updatePgxcNodeAfterSwitchGtmCoord(activeCoordinatorCoon,
													  NameStr(coordinator->mgrNode->form.nodename),
													  newMaster,
													  false);
			}
			else
			{

				revertPgxcNode =
					updatePgxcNodeAfterSwitchDataNode(activeCoordinatorCoon,
													  NameStr(coordinator->mgrNode->form.nodename),
													  localExecute,
													  newMaster->mgrNode,
													  oldMaster->mgrNode,
													  false);
			}
			if (revertPgxcNode)
			{
				coordinator->coordPgxcNodeChanged = false;
			}
			else
			{
				ereport(NOTICE,
						(errmsg("%s revert pgxc_node failed",
								NameStr(coordinator->mgrNode->form.nodename))));
				ereport(LOG,
						(errmsg("%s revert pgxc_node failed",
								NameStr(coordinator->mgrNode->form.nodename))));
				execOk = false;
			}
		}
		if (coordinator->adbSlotChanged)
		{
			if (updateAdbSlot(coordinator, newMaster->mgrNode,
							  oldMaster->mgrNode, false))
			{
				coordinator->adbSlotChanged = false;
			}
			else
			{
				ereport(NOTICE,
						(errmsg("%s revert adb slot failed",
								NameStr(coordinator->mgrNode->form.nodename))));
				ereport(LOG,
						(errmsg("%s revert adb slot failed",
								NameStr(coordinator->mgrNode->form.nodename))));
				execOk = false;
			}
		}
	}
	if (!tryUnlockCluster(coordinators, false))
	{
		execOk = false;
	}
	if (execOk)
	{
		ereport(LOG,
				(errmsg("revert cluster setting successfully")));
	}
	else
	{
		ereport(WARNING,
				(errmsg("revert cluster setting operation failed")));
	}
}

/*
 * Executes an "pause cluster" operation on master coordinator. 
 * If any of the sub-operations fails, the execution of the remaining 
 * sub-operations is abandoned, and the function returns false. 
 * Finally, if operation failed, may should call revertClusterSetting() to 
 * revert the settings that was changed during execution.
 */
void tryLockCluster(dlist_head *coordinators)
{
	dlist_iter iter;
	SwitcherNodeWrapper *coordinator;
	SwitcherNodeWrapper *holdLockCoordinator = NULL;

	if (dlist_is_empty(coordinators))
	{
		ereport(ERROR,
				(errmsg("There is no master coordinator, lock cluster failed")));
	}

	dlist_foreach(iter, coordinators)
	{
		coordinator = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		checkSet_pool_release_to_idle_timeout(coordinator);
		deletePgxcNodeDataNodeSlaves(coordinator->pgConn,
									 NameStr(coordinator->mgrNode->form.nodename),
									 true);
	}

	/* gtm_coord is the first priority */
	holdLockCoordinator = getGtmCoordMaster(coordinators);

	/* When cluster is locked, the connection which 
	 * execute the lock command is the only active connection */
	if (holdLockCoordinator)
	{
		holdLockCoordinator->holdClusterLock =
			exec_pg_pause_cluster(coordinator->pgConn, false);
	}
	else
	{
		dlist_foreach(iter, coordinators)
		{
			coordinator = dlist_container(SwitcherNodeWrapper, link, iter.cur);
			coordinator->holdClusterLock =
				exec_pg_pause_cluster(coordinator->pgConn, false);
			holdLockCoordinator = coordinator;
			break;
		}
	}
	if (holdLockCoordinator->holdClusterLock)
	{
		ereport(NOTICE,
				(errmsg("%s try lock cluster successfully",
						NameStr(holdLockCoordinator->mgrNode->form.nodename))));
		ereport(LOG,
				(errmsg("%s try lock cluster successfully",
						NameStr(holdLockCoordinator->mgrNode->form.nodename))));
	}
	else
	{
		ereport(ERROR,
				(errmsg("%s try lock cluster failed",
						NameStr(holdLockCoordinator->mgrNode->form.nodename))));
	}
}

bool tryUnlockCluster(dlist_head *coordinators, bool complain)
{
	dlist_iter iter;
	SwitcherNodeWrapper *coordinator;
	bool execOk = true;

	dlist_foreach(iter, coordinators)
	{
		coordinator = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		if (coordinator->holdClusterLock)
		{
			if (exec_pg_unpause_cluster(coordinator->pgConn, complain))
			{
				coordinator->holdClusterLock = false;
			}
			else
			{
				execOk = false;
			}
			if (execOk)
			{
				ereport(NOTICE,
						(errmsg("%s try unlock cluster successfully",
								NameStr(coordinator->mgrNode->form.nodename))));
				ereport(LOG,
						(errmsg("%s try unlock cluster successfully",
								NameStr(coordinator->mgrNode->form.nodename))));
			}
			else
			{
				ereport(complain ? ERROR : WARNING,
						(errmsg("%s try unlock cluster failed",
								NameStr(coordinator->mgrNode->form.nodename))));
			}
			break;
		}
		else
		{
			continue;
		}
	}

	dlist_foreach(iter, coordinators)
	{
		coordinator = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		restoreCoordinatorSetting(coordinator);
	}
	return execOk;
}

void mgrNodesToSwitcherNodes(dlist_head *mgrNodes,
							 dlist_head *switcherNodes)
{
	SwitcherNodeWrapper *switcherNode;
	MgrNodeWrapper *mgrNode;
	dlist_iter iter;

	if (mgrNodes == NULL || dlist_is_empty(mgrNodes))
	{
		return;
	}
	dlist_foreach(iter, mgrNodes)
	{
		mgrNode = dlist_container(MgrNodeWrapper, link, iter.cur);
		switcherNode = palloc0(sizeof(SwitcherNodeWrapper));
		switcherNode->mgrNode = mgrNode;
		dlist_push_tail(switcherNodes, &switcherNode->link);
	}
}

void switcherNodesToMgrNodes(dlist_head *switcherNodes,
							 dlist_head *mgrNodes)
{
	SwitcherNodeWrapper *switcherNode;
	MgrNodeWrapper *mgrNode;
	dlist_iter iter;

	if (switcherNodes == NULL || dlist_is_empty(switcherNodes))
	{
		return;
	}
	dlist_foreach(iter, switcherNodes)
	{
		switcherNode = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		mgrNode = switcherNode->mgrNode;
		dlist_push_tail(mgrNodes, &mgrNode->link);
	}
}

void appendSlaveNodeFollowMaster(MgrNodeWrapper *masterNode,
								 MgrNodeWrapper *slaveNode,
								 PGconn *masterPGconn)
{
	setPGHbaTrustSlaveReplication(masterNode, slaveNode);

	setSynchronousStandbyNames(slaveNode, "");

	setSlaveNodeRecoveryConf(masterNode, slaveNode);

	shutdownNodeWithinSeconds(slaveNode,
							  SHUTDOWN_NODE_FAST_SECONDS,
							  SHUTDOWN_NODE_IMMEDIATE_SECONDS,
							  true);

	callAgentStartNode(slaveNode, true, true);

	waitForNodeRunningOk(slaveNode, false, NULL, NULL);

	appendSyncStandbyNames(masterNode, slaveNode, masterPGconn);

	ereport(LOG,
			(errmsg("%s has followed master %s",
					NameStr(slaveNode->form.nodename),
					NameStr(masterNode->form.nodename))));
}

/**
 * Compare all nodes's wal lsn, find a node which has the best wal lsn, 
 * called it "bestNode", we think this bestNode can be promoted, also 
 * this node will be deleted in runningSlaves. In  subsequent operations, 
 * If the bestNode is promoted as new master node, the remaining nodes in 
 * runningSlaves should follow this bestNode.
 */
static SwitcherNodeWrapper *getBestWalLsnSlaveNode(dlist_head *runningSlaves,
												   dlist_head *failedSlaves,
												   bool forceSwitch)
{
	SwitcherNodeWrapper *node;
	SwitcherNodeWrapper *bestNode = NULL;
	dlist_mutable_iter miter;

	if (dlist_is_empty(runningSlaves))
	{
		ereport(ERROR,
				(errmsg("Can't find any slave node that can be promoted")));
	}

	dlist_foreach_modify(miter, runningSlaves)
	{
		node = dlist_container(SwitcherNodeWrapper, link, miter.cur);

		node->walLsn = getNodeWalLsn(node->pgConn, node->runningMode);
		if (node->walLsn <= InvalidXLogRecPtr)
		{
			dlist_delete(miter.cur);
			dlist_push_tail(failedSlaves, &node->link);
			ereport(WARNING,
					(errmsg("%s get wal lsn failed",
							NameStr(node->mgrNode->form.nodename))));
		}
		else
		{
			if (bestNode == NULL)
			{
				bestNode = node;
			}
			else
			{
				if (node->walLsn > bestNode->walLsn)
				{
					bestNode = node;
				}
				else if (node->walLsn == bestNode->walLsn)
				{
					if (strcmp(NameStr(bestNode->mgrNode->form.nodesync),
							   getMgrNodeSyncStateValue(SYNC_STATE_SYNC)) == 0)
					{
						continue;
					}
					else
					{
						bestNode = node;
					}
				}
				else
				{
					continue;
				}
			}
		}
	}
	if (!forceSwitch && !dlist_is_empty(failedSlaves))
	{
		ereport(ERROR,
				(errmsg("There are some slave nodes failed, "
						"Abort switching to avoid data loss")));
	}
	if (!forceSwitch && (strcmp(NameStr(bestNode->mgrNode->form.nodesync),
								getMgrNodeSyncStateValue(SYNC_STATE_SYNC)) != 0))
	{
		ereport(ERROR,
				(errmsg("Candidate %s is not a Synchronous standby node, "
						"Abort switching to avoid data loss",
						NameStr(bestNode->mgrNode->form.nodename))));
	}
	if (bestNode != NULL)
	{
		dlist_foreach_modify(miter, runningSlaves)
		{
			node = dlist_container(SwitcherNodeWrapper, link, miter.cur);
			if (node == bestNode)
			{
				dlist_delete(miter.cur);
			}
		}
		ereport(NOTICE,
				(errmsg("%s have the best wal lsn, "
						"choose it as a candidate for promotion",
						NameStr(bestNode->mgrNode->form.nodename))));
		ereport(LOG,
				(errmsg("%s have the best wal lsn, "
						"choose it as a candidate for promotion",
						NameStr(bestNode->mgrNode->form.nodename))));
	}
	else
	{
		ereport(ERROR,
				(errmsg("Can't find a qualified slave node that can be promoted")));
	}
	return bestNode;
}

/*
 * If any of the sub-operations failed to execute, the funtion returns 
 * false, but ignores any execution failures and continues to execute 
 * the remaining sub-operations.
 */
static void restoreCoordinatorSetting(SwitcherNodeWrapper *coordinator)
{
	if (coordinator->temporaryHbaItems)
	{
		if (callAgentDeletePGHbaConf(coordinator->mgrNode,
									 coordinator->temporaryHbaItems,
									 false))
		{
			pfreePGHbaItem(coordinator->temporaryHbaItems);
			coordinator->temporaryHbaItems = NULL;
		}
		else
		{
			ereport(NOTICE,
					(errmsg("%s restore pg_hba.conf failed",
							NameStr(coordinator->mgrNode->form.nodename))));
			ereport(LOG,
					(errmsg("%s restore pg_hba.conf failed",
							NameStr(coordinator->mgrNode->form.nodename))));
		}
	}
	if (coordinator->originalParameterItems)
	{
		if (callAgentRefreshPGSqlConfReload(coordinator->mgrNode,
											coordinator->originalParameterItems,
											false))
		{
			pfreePGConfParameterItem(coordinator->originalParameterItems);
			coordinator->originalParameterItems = NULL;
		}
		else
		{
			ereport(NOTICE,
					(errmsg("%s restore postgresql.conf failed",
							NameStr(coordinator->mgrNode->form.nodename))));
			ereport(LOG,
					(errmsg("%s restore postgresql.conf failed",
							NameStr(coordinator->mgrNode->form.nodename))));
		}
	}

	callAgentReloadNode(coordinator->mgrNode, false);

	if (!exec_pool_close_idle_conn(coordinator->pgConn, false))
	{
		ereport(NOTICE,
				(errmsg("%s exec pool_close_idle_conn failed",
						NameStr(coordinator->mgrNode->form.nodename))));
		ereport(LOG,
				(errmsg("%s exec pool_close_idle_conn failed",
						NameStr(coordinator->mgrNode->form.nodename))));
	}
}

static void checkGetSlaveNodesRunningStatus(SwitcherNodeWrapper *masterNode,
											MemoryContext spiContext,
											bool forceSwitch,
											dlist_head *failedSlaves,
											dlist_head *runningSlaves)
{
	char slaveNodetype;
	dlist_head mgrNodes = DLIST_STATIC_INIT(mgrNodes);
	dlist_head slaveNodes = DLIST_STATIC_INIT(slaveNodes);

	slaveNodetype = getMgrSlaveNodetype(masterNode->mgrNode->form.nodetype);
	selectMgrSlaveNodes(masterNode->mgrNode->oid,
						slaveNodetype, spiContext, &mgrNodes);
	mgrNodesToSwitcherNodes(&mgrNodes, &slaveNodes);
	checkSlavesRunningStatus(&slaveNodes, failedSlaves, runningSlaves);
	if (!forceSwitch && !dlist_is_empty(failedSlaves))
	{
		ereport(ERROR,
				(errmsg("There are some slave nodes failed, "
						"Abort switching to avoid data loss")));
	}
}

static void checkSlavesRunningStatus(dlist_head *slaveNodes,
									 dlist_head *failedSlaves,
									 dlist_head *runningSlaves)
{
	SwitcherNodeWrapper *node;
	dlist_mutable_iter miter;

	dlist_foreach_modify(miter, slaveNodes)
	{
		node = dlist_container(SwitcherNodeWrapper, link, miter.cur);
		if (pg_strcasecmp(NameStr(node->mgrNode->form.curestatus),
						  CURE_STATUS_NORMAL) != 0 &&
			pg_strcasecmp(NameStr(node->mgrNode->form.curestatus),
						  CURE_STATUS_SWITCHED) != 0)
		{
			dlist_delete(miter.cur);
			dlist_push_tail(failedSlaves, &node->link);
			ereport(WARNING,
					(errmsg("%s illegal curestatus:%s",
							NameStr(node->mgrNode->form.nodename),
							NameStr(node->mgrNode->form.curestatus))));
		}
		else
		{
			if (tryConnectNode(node, 10))
			{
				node->runningMode = getNodeRunningMode(node->pgConn);
				if (node->runningMode == NODE_RUNNING_MODE_UNKNOW)
				{
					dlist_delete(miter.cur);
					dlist_push_tail(failedSlaves, &node->link);
					ereport(WARNING,
							(errmsg("%s running status unknow",
									NameStr(node->mgrNode->form.nodename))));
				}
				else
				{
					dlist_delete(miter.cur);
					dlist_push_tail(runningSlaves, &node->link);
				}
			}
			else
			{
				dlist_delete(miter.cur);
				dlist_push_tail(failedSlaves, &node->link);
				ereport(WARNING,
						(errmsg("%s connection failed",
								NameStr(node->mgrNode->form.nodename))));
			}
		}
	}
}

/**
 * Sort new slave nodes (exclude new master node) by walReceiveLsn.
 * The larger the walReceiveLsn, the more in front of the dlist.
 */
static void sortNodesByWalLsnDesc(dlist_head *nodes)
{
	dlist_mutable_iter miter;
	SwitcherNodeWrapper *node;
	SwitcherNodeWrapper **sortItems;
	int i, numOfNodes;

	numOfNodes = 0;
	/* Exclude new master node from old slave nodes */
	dlist_foreach_modify(miter, nodes)
	{
		numOfNodes++;
	}
	if (numOfNodes > 1)
	{
		sortItems = palloc(sizeof(SwitcherNodeWrapper *) * numOfNodes);
		i = 0;
		dlist_foreach_modify(miter, nodes)
		{
			node = dlist_container(SwitcherNodeWrapper, link, miter.cur);
			sortItems[i] = node;
			i++;
		}
		/* order by wal lsn desc */
		qsort(sortItems, numOfNodes, sizeof(SwitcherNodeWrapper *), walLsnDesc);
		/* add to dlist in order */
		dlist_init(nodes);
		for (i = 0; i < numOfNodes; i++)
		{
			dlist_push_tail(nodes, &sortItems[i]->link);
		}
	}
	else
	{
		/* No need to sort */
	}
}

static void checkGetMasterCoordinators(MemoryContext spiContext,
									   dlist_head *coordinators,
									   bool includeGtmCoord,
									   bool checkRunningMode)
{
	dlist_head mgrNodes = DLIST_STATIC_INIT(mgrNodes);
	SwitcherNodeWrapper *node;
	dlist_iter iter;

	if (includeGtmCoord)
		selectMgrAllMasterCoordinators(spiContext, &mgrNodes);
	else
		selectMgrNodeByNodetype(spiContext,
								CNDN_TYPE_COORDINATOR_MASTER,
								&mgrNodes);
	mgrNodesToSwitcherNodes(&mgrNodes, coordinators);

	if (dlist_is_empty(coordinators))
	{
		ereport(ERROR,
				(errmsg("can't find any master coordinator")));
	}
	dlist_foreach(iter, coordinators)
	{
		node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		if (!tryConnectNode(node, 10))
		{
			ereport(ERROR,
					(errmsg("connect to coordinator %s failed",
							NameStr(node->mgrNode->form.nodename))));
		}
		if (checkRunningMode)
		{
			node->runningMode = getNodeRunningMode(node->pgConn);
			if (node->runningMode != NODE_RUNNING_MODE_MASTER)
			{
				ereport(ERROR,
						(errmsg("coordinator %s configured as master, "
								"but actually did not run in that expected state",
								NameStr(node->mgrNode->form.nodename))));
			}
		}
		else
		{
			node->runningMode = NODE_RUNNING_MODE_UNKNOW;
		}
	}
}

static void checkGetAllDataNodes(dlist_head *runningDataNodes,
								 dlist_head *failedDataNodes,
								 MemoryContext spiContext)
{
	dlist_head mgrNodes = DLIST_STATIC_INIT(mgrNodes);
	dlist_head allDataNodes = DLIST_STATIC_INIT(allDataNodes);
	SwitcherNodeWrapper *node;
	dlist_mutable_iter iter;

	selectMgrAllDataNodes(spiContext, &mgrNodes);
	mgrNodesToSwitcherNodes(&mgrNodes, &allDataNodes);

	if (dlist_is_empty(&allDataNodes))
	{
		ereport(LOG,
				(errmsg("can't find any data node")));
	}
	dlist_foreach_modify(iter, &allDataNodes)
	{
		node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		if (tryConnectNode(node, 10))
		{
			dlist_delete(&node->link);
			dlist_push_tail(runningDataNodes, &node->link);
		}
		else
		{
			ereport(WARNING,
					(errmsg("connect to data node %s failed, "
							"you can manually fix it's GTM information, "
							"or enable the doctor extension to fix it automatically",
							NameStr(node->mgrNode->form.nodename))));
			dlist_delete(&node->link);
			dlist_push_tail(failedDataNodes, &node->link);
		}
	}
}

/**
 * connectTimeout: Maximum wait for connection, in seconds 
 * (write as a decimal integer, e.g. 10). 
 * Zero, negative, or not specified means wait indefinitely. 
 * If get connection successfully, the PGconn is saved in node->pgConn.
 */
static bool tryConnectNode(SwitcherNodeWrapper *node, int connectTimeout)
{
	/* ensure to close obtained connection */
	pfreeSwitcherNodeWrapperPGconn(node);
	node->pgConn = getNodeDefaultDBConnection(node->mgrNode, connectTimeout);
	return node->pgConn != NULL;
}

static SwitcherNodeWrapper *checkGetDataNodeOldMaster(char *oldMasterName,
													  MemoryContext spiContext)
{
	MgrNodeWrapper *mgrNode;
	SwitcherNodeWrapper *oldMaster;

	mgrNode = selectMgrNodeByNodenameType(oldMasterName,
										  CNDN_TYPE_DATANODE_MASTER,
										  spiContext);
	if (!mgrNode)
	{
		ereport(ERROR,
				(errmsg("%s does not exist or is not a master data node",
						oldMasterName)));
	}
	if (mgrNode->form.nodetype != CNDN_TYPE_DATANODE_MASTER)
	{
		ereport(ERROR,
				(errmsg("%s is not a master data node",
						oldMasterName)));
	}
	if (!mgrNode->form.nodeinited)
	{
		ereport(ERROR,
				(errmsg("%s has not be initialized",
						oldMasterName)));
	}
	if (!mgrNode->form.nodeincluster)
	{
		ereport(ERROR,
				(errmsg("%s has been kicked out of the cluster",
						oldMasterName)));
	}
	oldMaster = palloc0(sizeof(SwitcherNodeWrapper));
	oldMaster->mgrNode = mgrNode;
	return oldMaster;
}

static SwitcherNodeWrapper *checkGetGtmCoordOldMaster(char *oldMasterName,
													  MemoryContext spiContext)
{
	MgrNodeWrapper *mgrNode;
	SwitcherNodeWrapper *oldMaster;

	mgrNode = selectMgrNodeByNodenameType(oldMasterName,
										  CNDN_TYPE_GTM_COOR_MASTER,
										  spiContext);
	if (!mgrNode)
	{
		ereport(ERROR,
				(errmsg("%s does not exist or is not a master GTM coordinator",
						oldMasterName)));
	}
	if (mgrNode->form.nodetype != CNDN_TYPE_GTM_COOR_MASTER)
	{
		ereport(ERROR,
				(errmsg("%s is not a master GTM coordinator",
						oldMasterName)));
	}
	if (!mgrNode->form.nodeinited)
	{
		ereport(ERROR,
				(errmsg("%s has not be initialized",
						oldMasterName)));
	}
	if (!mgrNode->form.nodeincluster)
	{
		ereport(ERROR,
				(errmsg("%s has been kicked out of the cluster",
						oldMasterName)));
	}
	oldMaster = palloc0(sizeof(SwitcherNodeWrapper));
	oldMaster->mgrNode = mgrNode;
	return oldMaster;
}

static void runningSlavesFollowMaster(SwitcherNodeWrapper *masterNode,
									  dlist_head *runningSlaves,
									  MgrNodeWrapper *gtmMaster)
{
	dlist_mutable_iter iter;
	SwitcherNodeWrapper *slaveNode;
	dlist_head mgrNodes = DLIST_STATIC_INIT(mgrNodes);

	/* config these slave node to follow new master and restart them */
	dlist_foreach_modify(iter, runningSlaves)
	{
		slaveNode = dlist_container(SwitcherNodeWrapper,
									link, iter.cur);
		setPGHbaTrustSlaveReplication(masterNode->mgrNode,
									  slaveNode->mgrNode);
		setSynchronousStandbyNames(slaveNode->mgrNode, "");
		setSlaveNodeRecoveryConf(masterNode->mgrNode,
								 slaveNode->mgrNode);
		pfreeSwitcherNodeWrapperPGconn(slaveNode);
	}

	if (gtmMaster)
		batchSetGtmInfoOnNodes(gtmMaster, runningSlaves, NULL);

	switcherNodesToMgrNodes(runningSlaves, &mgrNodes);

	batchShutdownNodesWithinSeconds(&mgrNodes,
									SHUTDOWN_NODE_FAST_SECONDS,
									SHUTDOWN_NODE_IMMEDIATE_SECONDS,
									true);

	batchStartupNodesWithinSeconds(&mgrNodes,
								   STARTUP_NODE_SECONDS,
								   true);

	dlist_foreach_modify(iter, runningSlaves)
	{
		slaveNode = dlist_container(SwitcherNodeWrapper,
									link, iter.cur);
		waitForNodeRunningOk(slaveNode->mgrNode,
							 false,
							 &slaveNode->pgConn,
							 &slaveNode->runningMode);
		appendSyncStandbyNames(masterNode->mgrNode,
							   slaveNode->mgrNode,
							   masterNode->pgConn);
		ereport(LOG,
				(errmsg("%s has followed master %s",
						NameStr(slaveNode->mgrNode->form.nodename),
						NameStr(masterNode->mgrNode->form.nodename))));
	}
	if (gtmMaster)
		batchCheckGtmInfoOnNodes(gtmMaster,
								 runningSlaves,
								 NULL,
								 CHECK_GTM_INFO_SECONDS);
}

/**
 * This function will modify slaveNode's field form.nodesync.
 */
static void appendSyncStandbyNames(MgrNodeWrapper *masterNode,
								   MgrNodeWrapper *slaveNode,
								   PGconn *masterPGconn)
{
	char *oldSyncNames = NULL;
	char *newSyncNames = NULL;
	char *syncNodes = NULL;
	char *backupSyncNodes = NULL;
	char *buf = NULL;
	char *temp = NULL;
	bool contained = false;
	int i;

	temp = showNodeParameter(masterPGconn,
							 "synchronous_standby_names", true);
	ereport(DEBUG1,
			(errmsg("%s synchronous_standby_names is %s",
					NameStr(masterNode->form.nodename),
					temp)));
	oldSyncNames = trimString(temp);
	pfree(temp);
	temp = NULL;
	if (IS_EMPTY_STRING(oldSyncNames))
	{
		newSyncNames = psprintf("FIRST %d (%s)",
								1,
								NameStr(slaveNode->form.nodename));
		namestrcpy(&slaveNode->form.nodesync,
				   getMgrNodeSyncStateValue(SYNC_STATE_SYNC));
	}
	else
	{
		buf = palloc0(strlen(oldSyncNames) + 1);
		/* "FIRST 1 (nodename2,nodename4)" will get result nodename2,nodename4 */
		sscanf(oldSyncNames, "%*[^(](%[^)]", buf);
		syncNodes = trimString(buf);
		pfree(buf);
		if (IS_EMPTY_STRING(syncNodes))
		{
			ereport(ERROR,
					(errmsg("%s unexpected synchronous_standby_names %s",
							NameStr(masterNode->form.nodename),
							oldSyncNames)));
		}

		contained = false;
		i = 0;
		/* function "strtok" will scribble on the input argument */
		backupSyncNodes = psprintf("%s", syncNodes);
		temp = strtok(syncNodes, ",");
		while (temp)
		{
			i++;
			if (equalsAfterTrim(temp, NameStr(slaveNode->form.nodename)))
			{
				contained = true;
				break;
			}
			temp = strtok(NULL, ",");
		}
		if (contained)
		{
			ereport(LOG,
					(errmsg("%s synchronous_standby_names "
							"already contained %s, no need to change it",
							NameStr(masterNode->form.nodename),
							NameStr(slaveNode->form.nodename))));
			newSyncNames = NULL;
			if (i == 1)
				namestrcpy(&slaveNode->form.nodesync,
						   getMgrNodeSyncStateValue(SYNC_STATE_SYNC));
			else
				namestrcpy(&slaveNode->form.nodesync,
						   getMgrNodeSyncStateValue(SYNC_STATE_POTENTIAL));
		}
		else
		{
			newSyncNames = psprintf("FIRST %d (%s,%s)",
									1,
									backupSyncNodes,
									NameStr(slaveNode->form.nodename));
			namestrcpy(&slaveNode->form.nodesync,
					   getMgrNodeSyncStateValue(SYNC_STATE_POTENTIAL));
		}
	}
	if (newSyncNames)
	{
		ereport(LOG,
				(errmsg("%s try to change synchronous_standby_names from '%s' to '%s'",
						NameStr(masterNode->form.nodename),
						oldSyncNames,
						newSyncNames)));
		setCheckSynchronousStandbyNames(masterNode,
										masterPGconn,
										newSyncNames,
										CHECK_SYNC_STANDBY_NAMES_SECONDS);
		pfree(newSyncNames);
	}
	if (backupSyncNodes)
		pfree(backupSyncNodes);
}

static int walLsnDesc(const void *node1, const void *node2)
{
	return (*(SwitcherNodeWrapper **)node2)->walLsn -
		   (*(SwitcherNodeWrapper **)node1)->walLsn;
}

static void checkSet_pool_release_to_idle_timeout(SwitcherNodeWrapper *node)
{
	char *parameterName;
	char *expectValue;
	char *originalValue;
	PGConfParameterItem *originalItems;
	PGConfParameterItem *expectItems;
	bool execOk;
	int nTrys;

	parameterName = "pool_release_to_idle_timeout";
	expectValue = "-1";
	originalValue = showNodeParameter(node->pgConn, parameterName, true);
	if (strcmp(originalValue, expectValue) == 0)
	{
		ereport(LOG, (errmsg("node %s parameter %s already is %s,"
							 " no need to set",
							 NameStr(node->mgrNode->form.nodename),
							 parameterName, expectValue)));
		pfree(originalValue);
		return;
	}

	originalItems = newPGConfParameterItem(parameterName,
										   originalValue, true);
	pfree(originalValue);

	/* We will revert these settings after switching is complete. */
	node->originalParameterItems = originalItems;
	expectItems = newPGConfParameterItem(parameterName,
										 expectValue, true);
	execOk = callAgentRefreshPGSqlConfReload(node->mgrNode,
											 expectItems, false);
	pfreePGConfParameterItem(expectItems);
	if (execOk)
	{
		for (nTrys = 0; nTrys < 10; nTrys++)
		{
			/*sleep 0.1s*/
			pg_usleep(100000L);
			/* check the param */
			execOk = equalsNodeParameter(node->pgConn,
										 parameterName,
										 expectValue);
			if (execOk)
				break;
		}
	}
	if (execOk)
		ereport(LOG, (errmsg("%s set %s = %s successful",
							 NameStr(node->mgrNode->form.nodename),
							 parameterName, expectValue)));
	else
		ereport(ERROR, (errmsg("%s set %s = %s failed",
							   NameStr(node->mgrNode->form.nodename),
							   parameterName, expectValue)));
}

static void waitForNodeRunningOk(MgrNodeWrapper *mgrNode,
								 bool isMaster,
								 PGconn **pgConnP,
								 NodeRunningMode *runningModeP)
{
	NodeConnectionStatus connStatus;
	NodeRunningMode runningMode;
	NodeRunningMode expectedRunningMode;
	PGconn *pgConn = NULL;
	int networkFailures = 0;
	int maxNetworkFailures = 60;
	int runningModeFailures = 0;
	int maxRunningModeFailures = 60;

	while (networkFailures < maxNetworkFailures)
	{
		connStatus = connectNodeDefaultDB(mgrNode, 10, &pgConn);
		if (connStatus == NODE_CONNECTION_STATUS_SUCCESS)
		{
			break;
		}
		else if (connStatus == NODE_CONNECTION_STATUS_BUSY ||
				 connStatus == NODE_CONNECTION_STATUS_STARTING_UP)
		{
			/* wait for start up */
			networkFailures = 0;
		}
		else
		{
			networkFailures++;
		}
		fputs(_("."), stdout);
		fflush(stdout);
		pg_usleep(1 * 1000000L);
	}
	if (connStatus != NODE_CONNECTION_STATUS_SUCCESS)
	{
		ereport(ERROR,
				(errmsg("%s connection failed, may crashed",
						NameStr(mgrNode->form.nodename))));
	}

	expectedRunningMode = getExpectedNodeRunningMode(isMaster);
	while (runningModeFailures < maxRunningModeFailures)
	{
		runningMode = getNodeRunningMode(pgConn);
		if (runningMode == expectedRunningMode)
		{
			runningModeFailures = 0;
			break;
		}
		else if (runningMode == NODE_RUNNING_MODE_UNKNOW)
		{
			runningModeFailures++;
		}
		else
		{
			break;
		}
		fputs(_("."), stdout);
		fflush(stdout);
		pg_usleep(1 * 1000000L);
	}

	if (pgConnP)
	{
		if (*pgConnP)
		{
			PQfinish(*pgConnP);
			*pgConnP = NULL;
		}
		*pgConnP = pgConn;
	}
	else
	{
		if (pgConn)
		{
			PQfinish(pgConn);
			pgConn = NULL;
		}
	}
	if (runningModeP)
	{
		*runningModeP = runningMode;
	}
	if (runningMode == expectedRunningMode)
	{
		ereport(NOTICE,
				(errmsg("%s running on correct status",
						NameStr(mgrNode->form.nodename))));
		ereport(LOG,
				(errmsg("%s running on correct status",
						NameStr(mgrNode->form.nodename))));
	}
	else
	{
		ereport(ERROR,
				(errmsg("%s recovery status error",
						NameStr(mgrNode->form.nodename))));
	}
}

static void refreshOldMasterBeforeSwitch(SwitcherNodeWrapper *oldMaster,
										 MemoryContext spiContext)
{
	char *newCurestatus;
	/* Use CURE_STATUS_SWITCHING as a tag to prevent the node doctor from 
	 * operating on this node simultaneously. */
	newCurestatus = CURE_STATUS_SWITCHING;

	/* backup curestatus */
	memcpy(&oldMaster->oldCurestatus,
		   &oldMaster->mgrNode->form.curestatus, sizeof(NameData));
	updateCureStatusForSwitch(oldMaster->mgrNode,
							  newCurestatus,
							  spiContext);
}

static void refreshSlaveNodesBeforeSwitch(SwitcherNodeWrapper *newMaster,
										  dlist_head *runningSlaves,
										  dlist_head *failedSlaves,
										  MemoryContext spiContext)
{
	dlist_iter iter;
	SwitcherNodeWrapper *node;
	char *newCurestatus;
	/* Use CURE_STATUS_SWITCHING as a tag to prevent the node doctor from 
	 * operating on this node simultaneously. */
	newCurestatus = CURE_STATUS_SWITCHING;

	memcpy(&newMaster->oldCurestatus,
		   &newMaster->mgrNode->form.curestatus, sizeof(NameData));
	updateCureStatusForSwitch(newMaster->mgrNode, newCurestatus,
							  spiContext);

	dlist_foreach(iter, runningSlaves)
	{
		node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		memcpy(&node->oldCurestatus,
			   &node->mgrNode->form.curestatus, sizeof(NameData));
		updateCureStatusForSwitch(node->mgrNode, newCurestatus,
								  spiContext);
	}
	dlist_foreach(iter, failedSlaves)
	{
		node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		memcpy(&node->oldCurestatus,
			   &node->mgrNode->form.curestatus, sizeof(NameData));
		updateCureStatusForSwitch(node->mgrNode, newCurestatus,
								  spiContext);
	}
}

static void refreshMgrNodeAfterSwitch(SwitcherNodeWrapper *oldMaster,
									  SwitcherNodeWrapper *newMaster,
									  dlist_head *runningSlaves,
									  dlist_head *failedSlaves,
									  MemoryContext spiContext,
									  bool kickOutOldMaster)
{
	dlist_iter iter;
	SwitcherNodeWrapper *node;

	namestrcpy(&oldMaster->mgrNode->form.nodesync, "");
	/* Mark the data group to which the old master belongs */
	oldMaster->mgrNode->form.nodemasternameoid = newMaster->mgrNode->oid;
	if (kickOutOldMaster)
	{
		oldMaster->mgrNode->form.nodeinited = false;
		oldMaster->mgrNode->form.nodeincluster = false;
		oldMaster->mgrNode->form.allowcure = false;
		/* Kick the old master out of the cluster */
		updateMgrNodeAfterSwitch(oldMaster->mgrNode, CURE_STATUS_OLD_MASTER,
								 spiContext);
	}
	else
	{
		oldMaster->mgrNode->form.nodetype =
			getMgrSlaveNodetype(oldMaster->mgrNode->form.nodetype);
		/* Update Old master follow the new master, 
		 * Then, the task of pg_rewind this old master is handled to the node doctor. */
		updateMgrNodeAfterSwitch(oldMaster->mgrNode, CURE_STATUS_OLD_MASTER,
								 spiContext);
	}

	/* Admit the reign of the new master */
	newMaster->mgrNode->form.nodetype =
		getMgrMasterNodetype(newMaster->mgrNode->form.nodetype);
	namestrcpy(&newMaster->mgrNode->form.nodesync, "");
	newMaster->mgrNode->form.nodemasternameoid = 0;
	/* Why is there a curestatus of CURE_STATUS_SWITCHED? 
	 * Because we can use this field as a tag to prevent the node doctor 
	 * from operating on this node simultaneously. */
	updateMgrNodeAfterSwitch(newMaster->mgrNode, CURE_STATUS_SWITCHED,
							 spiContext);

	dlist_foreach(iter, runningSlaves)
	{
		node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		/* nodesync field was set in other function */
		node->mgrNode->form.nodemasternameoid = newMaster->mgrNode->oid;
		/* Admit the reign of new master */
		updateMgrNodeAfterSwitch(node->mgrNode, CURE_STATUS_SWITCHED,
								 spiContext);
	}
	dlist_foreach(iter, failedSlaves)
	{
		node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		namestrcpy(&node->mgrNode->form.nodesync, "");
		node->mgrNode->form.nodemasternameoid = newMaster->mgrNode->oid;
		/* Update other failure slave node follow the new master, 
		 * Then, The node "follow the new master node" task is handed over 
		 * to the node doctor. */
		if (pg_strcasecmp(NameStr(node->oldCurestatus),
						  CURE_STATUS_OLD_MASTER) == 0)
		{
			updateMgrNodeAfterSwitch(node->mgrNode,
									 CURE_STATUS_OLD_MASTER,
									 spiContext);
		}
		else
		{
			updateMgrNodeAfterSwitch(node->mgrNode,
									 CURE_STATUS_FOLLOW_FAIL,
									 spiContext);
		}
	}
}

static void refreshMgrUpdateparmAfterSwitch(MgrNodeWrapper *oldMaster,
											MgrNodeWrapper *newMaster,
											MemoryContext spiContext,
											bool kickOutOldMaster)
{
	if (kickOutOldMaster)
	{
		deleteMgrUpdateparmByNodenameType(NameStr(oldMaster->form.nodename),
										  getMgrMasterNodetype(oldMaster->form.nodetype),
										  spiContext);
	}
	else
	{
		/* old master update to slave */
		updateMgrUpdateparmNodetype(NameStr(oldMaster->form.nodename),
									getMgrSlaveNodetype(oldMaster->form.nodetype),
									spiContext);
	}

	/* new master update to master */
	updateMgrUpdateparmNodetype(NameStr(newMaster->form.nodename),
								getMgrMasterNodetype(newMaster->form.nodetype),
								spiContext);
}

static void refreshPgxcNodesAfterSwitchDataNode(dlist_head *coordinators,
												MgrNodeWrapper *oldMaster,
												MgrNodeWrapper *newMaster)
{
	dlist_iter iter;
	SwitcherNodeWrapper *holdLockCoordinator = NULL;
	SwitcherNodeWrapper *coordinator;
	bool localExecute;

	holdLockCoordinator = getHoldLockCoordinator(coordinators);

	if (!holdLockCoordinator)
		ereport(ERROR, (errmsg("System error, can not find a "
							   "coordinator that hode the cluster lock")));

	/* Adb_slot only needs to be updated once */
	updateAdbSlot(holdLockCoordinator, oldMaster, newMaster, true);

	/* When cluster is locked, the connection which 
	 * execute the lock command is the only active connection */
	dlist_foreach(iter, coordinators)
	{
		coordinator = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		localExecute = holdLockCoordinator == coordinator;
		coordinator->coordPgxcNodeChanged =
			updatePgxcNodeAfterSwitchDataNode(holdLockCoordinator->pgConn,
											  NameStr(coordinator->mgrNode->form.nodename),
											  localExecute,
											  oldMaster,
											  newMaster,
											  true);
		pgxcPoolReload(holdLockCoordinator->pgConn,
					   NameStr(coordinator->mgrNode->form.nodename),
					   localExecute, true);
	}
}

static void refreshPgxcNodesAfterSwitchGtmCoord(dlist_head *coordinators,
												SwitcherNodeWrapper *newMaster)
{
	dlist_iter iter;
	SwitcherNodeWrapper *coordinator;

	/* When cluster is locked, the connection which 
	 * execute the lock command is the only active connection */
	dlist_foreach(iter, coordinators)
	{
		coordinator = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		coordinator->coordPgxcNodeChanged =
			updatePgxcNodeAfterSwitchGtmCoord(coordinator->pgConn,
											  NameStr(coordinator->mgrNode->form.nodename),
											  newMaster,
											  true);
		pgxcPoolReload(coordinator->pgConn,
					   NameStr(coordinator->mgrNode->form.nodename),
					   true, true);
	}
}

/**
 * update curestatus can avoid adb doctor monitor this node
 */
static void updateCureStatusForSwitch(MgrNodeWrapper *mgrNode,
									  char *newCurestatus,
									  MemoryContext spiContext)
{
	int spiRes;
	MemoryContext oldCtx;
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "UPDATE pg_catalog.mgr_node \n"
					 "SET curestatus = '%s' \n"
					 "WHERE oid = %u \n"
					 "AND curestatus = '%s' \n"
					 "AND nodetype = '%c' \n",
					 newCurestatus,
					 mgrNode->oid,
					 NameStr(mgrNode->form.curestatus),
					 mgrNode->form.nodetype);
	oldCtx = MemoryContextSwitchTo(spiContext);
	spiRes = SPI_execute(sql.data, false, 0);
	MemoryContextSwitchTo(oldCtx);

	pfree(sql.data);
	if (spiRes != SPI_OK_UPDATE)
	{
		ereport(ERROR,
				(errmsg("SPI_execute failed: error code %d",
						spiRes)));
	}
	if (SPI_processed != 1)
	{
		ereport(ERROR,
				(errmsg("SPI_execute failed: expected rows:%d, actually:%lu",
						1,
						SPI_processed)));
	}
	namestrcpy(&mgrNode->form.curestatus, newCurestatus);
}

static void updateMgrNodeAfterSwitch(MgrNodeWrapper *mgrNode,
									 char *newCurestatus,
									 MemoryContext spiContext)
{
	int spiRes;
	MemoryContext oldCtx;
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "UPDATE pg_catalog.mgr_node \n"
					 "SET nodetype = '%c', \n"
					 "nodesync = '%s', \n"
					 "nodemasternameoid = %u, \n"
					 "nodeinited =%d::boolean, \n"
					 "nodeincluster =%d::boolean, \n"
					 "allowcure =%d::boolean, \n"
					 "curestatus = '%s' \n"
					 "WHERE oid = %u \n"
					 "AND curestatus = '%s' \n",
					 mgrNode->form.nodetype,
					 NameStr(mgrNode->form.nodesync),
					 mgrNode->form.nodemasternameoid,
					 mgrNode->form.nodeinited,
					 mgrNode->form.nodeincluster,
					 mgrNode->form.allowcure,
					 newCurestatus,
					 mgrNode->oid,
					 NameStr(mgrNode->form.curestatus));
	oldCtx = MemoryContextSwitchTo(spiContext);
	spiRes = SPI_execute(sql.data, false, 0);
	MemoryContextSwitchTo(oldCtx);

	pfree(sql.data);
	if (spiRes != SPI_OK_UPDATE)
	{
		ereport(ERROR,
				(errmsg("SPI_execute failed: error code %d",
						spiRes)));
	}
	if (SPI_processed != 1)
	{
		ereport(ERROR,
				(errmsg("SPI_execute failed: expected rows:%d, actually:%lu",
						1,
						SPI_processed)));
	}
	namestrcpy(&mgrNode->form.curestatus, newCurestatus);
}

static void deleteMgrUpdateparmByNodenameType(char *updateparmnodename,
											  char updateparmnodetype,
											  MemoryContext spiContext)
{
	int spiRes;
	MemoryContext oldCtx;
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "DELETE FROM mgr_updateparm \n"
					 "WHERE updateparmnodename = '%s' \n"
					 "AND updateparmnodetype = '%c' \n",
					 updateparmnodename,
					 updateparmnodetype);
	oldCtx = MemoryContextSwitchTo(spiContext);
	spiRes = SPI_execute(sql.data, false, 0);
	MemoryContextSwitchTo(oldCtx);

	pfree(sql.data);
	if (spiRes != SPI_OK_UPDATE)
	{
		ereport(ERROR,
				(errmsg("SPI_execute failed: error code %d",
						spiRes)));
	}
}

static void updateMgrUpdateparmNodetype(char *updateparmnodename,
										char updateparmnodetype,
										MemoryContext spiContext)
{
	int spiRes;
	MemoryContext oldCtx;
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "UPDATE mgr_updateparm \n"
					 "SET updateparmnodetype = '%c' \n"
					 "WHERE updateparmnodename = '%s' \n",
					 updateparmnodetype,
					 updateparmnodename);
	oldCtx = MemoryContextSwitchTo(spiContext);
	spiRes = SPI_execute(sql.data, false, 0);
	MemoryContextSwitchTo(oldCtx);

	pfree(sql.data);
	if (spiRes != SPI_OK_UPDATE)
	{
		ereport(ERROR,
				(errmsg("SPI_execute failed: error code %d",
						spiRes)));
	}
}

static bool existsPgxcNodeDataNodeMaster(PGconn *activeCoordinatorCoon,
										 char *coordinatorName,
										 bool localExecute,
										 char *dataNodeName)
{
	char *sql;
	bool exists;
	if (localExecute)
		sql = psprintf("select count(*) "
					   "from pgxc_node "
					   "where node_name = '%s' "
					   "and node_type = '%c';",
					   dataNodeName,
					   PGXC_NODE_DATANODE);
	else
		sql = psprintf("EXECUTE DIRECT ON (\"%s\") "
					   "'select count(*) "
					   "from pgxc_node "
					   "where node_name = ''%s'' "
					   "and node_type = ''%c'' ;'",
					   coordinatorName,
					   dataNodeName,
					   PGXC_NODE_DATANODE);
	exists = PQexecCountSql(activeCoordinatorCoon, sql, true) > 0;
	pfree(sql);
	return exists;
}

static bool deletePgxcNodeDataNodeSlaves(PGconn *activeCoordinatorCoon,
										 char *coordinatorName,
										 bool localExecute)
{
	char *sql;
	bool execOk;
	if (localExecute)
		sql = psprintf("set FORCE_PARALLEL_MODE = off; "
					   "delete from pgxc_node "
					   "where node_type = '%c';",
					   PGXC_NODE_DATANODESLAVE);
	else
		sql = psprintf("set FORCE_PARALLEL_MODE = off; "
					   "EXECUTE DIRECT ON (\"%s\") "
					   "'delete from pgxc_node "
					   "where node_type = ''%c'' ;'",
					   coordinatorName,
					   PGXC_NODE_DATANODESLAVE);
	execOk = PQexecCommandSql(activeCoordinatorCoon, sql, false);
	pfree(sql);
	if (!execOk)
	{
		ereport(LOG,
				(errmsg("%s delete data node slaves from pgxc_node failed",
						coordinatorName)));
	}
	return execOk;
}

static bool updatePgxcNodeAfterSwitchDataNode(PGconn *activeCoordinatorCoon,
											  char *coordinatorName,
											  bool localExecute,
											  MgrNodeWrapper *oldMaster,
											  MgrNodeWrapper *newMaster,
											  bool complain)
{
	char *sql;
	bool execOk;

	if (existsPgxcNodeDataNodeMaster(activeCoordinatorCoon,
									 coordinatorName,
									 localExecute,
									 NameStr(newMaster->form.nodename)))
	{
		ereport(LOG,
				(errmsg("master node %s already exists %s pgxc_node, no need to update",
						NameStr(newMaster->form.nodename),
						coordinatorName)));
		execOk = true;
	}
	else
	{
		sql = psprintf("alter node \"%s\" with(name='%s', host='%s', port=%d) on (\"%s\");",
					   NameStr(oldMaster->form.nodename),
					   NameStr(newMaster->form.nodename),
					   newMaster->host->hostaddr,
					   newMaster->form.nodeport,
					   coordinatorName);
		execOk = PQexecCommandSql(activeCoordinatorCoon, sql, false);
		pfree(sql);
		if (execOk)
		{
			ereport(LOG,
					(errmsg("%s refresh pgxc_node successfully",
							coordinatorName)));
		}
		else
		{
			ereport(complain ? ERROR : WARNING,
					(errmsg("%s refresh pgxc_node failed",
							coordinatorName)));
		}
	}
	return execOk;
}

static bool updatePgxcNodeAfterSwitchGtmCoord(PGconn *activeCoordinatorCoon,
											  char *coordinatorName,
											  SwitcherNodeWrapper *newMaster,
											  bool complain)
{
	char *sql;
	char *gtmPgxcNodeName;
	bool execOk;
	bool isGtmMaster;

	gtmPgxcNodeName = showNodeParameter(newMaster->pgConn,
										"pgxc_node_name",
										true);
	if (IS_EMPTY_STRING(gtmPgxcNodeName))
	{
		pfree(gtmPgxcNodeName);
		execOk = false;
		ereport(complain ? ERROR : WARNING,
				(errmsg("%s get pgxc_node_name failed",
						coordinatorName)));
	}
	/* This is a strange situation, all gtm nodes have the same pgxc_node_name value. */
	isGtmMaster = strcmp(coordinatorName, NameStr(newMaster->mgrNode->form.nodename)) == 0;

	sql = psprintf("alter node \"%s\" with( host='%s', port=%d) on (\"%s\");",
				   gtmPgxcNodeName,
				   newMaster->mgrNode->host->hostaddr,
				   newMaster->mgrNode->form.nodeport,
				   isGtmMaster ? gtmPgxcNodeName : coordinatorName);
	execOk = PQexecCommandSql(activeCoordinatorCoon, sql, false);
	pfree(gtmPgxcNodeName);
	pfree(sql);
	if (execOk)
	{
		ereport(LOG,
				(errmsg("%s refresh pgxc_node successfully",
						coordinatorName)));
	}
	else
	{
		ereport(complain ? ERROR : WARNING,
				(errmsg("%s refresh pgxc_node failed",
						coordinatorName)));
	}
	return execOk;
}

static bool updateAdbSlot(SwitcherNodeWrapper *coordinator,
						  MgrNodeWrapper *oldMaster,
						  MgrNodeWrapper *newMaster,
						  bool complain)
{
	bool execOk = true;
	int numberOfSlots;

	numberOfSlots = PQexecCountSql(coordinator->pgConn,
								   SELECT_ADB_SLOT_TABLE_COUNT,
								   false);
	if (numberOfSlots < 0)
	{
		ereport(complain ? ERROR : WARNING,
				(errmsg("refresh the node %s information in adb_slot failed",
						NameStr(newMaster->form.nodename))));
	}

	/* update adb_slot */
	if (numberOfSlots > 0)
	{
		ereport(LOG, (errmsg("refresh node %s slot information",
							 NameStr(newMaster->form.nodename))));
		ereport(NOTICE, (errmsg("refresh node %s slot information",
								NameStr(newMaster->form.nodename))));
		hexp_alter_slotinfo_nodename_noflush(coordinator->pgConn,
											 NameStr(oldMaster->form.nodename),
											 NameStr(newMaster->form.nodename),
											 false,
											 complain);
		coordinator->adbSlotChanged = true;

		if (!PQexecCommandSql(coordinator->pgConn, "flush slot;", false))
		{
			ereport(complain ? ERROR : WARNING,
					(errmsg("flush node %s slot information failed",
							NameStr(newMaster->form.nodename))));
		}
	}
	return execOk;
}

static bool pgxcPoolReload(PGconn *activeCoordinatorCoon,
						   char *coordinatorName,
						   bool localExecute,
						   bool complain)
{
	char *sql;
	bool res;
	if (localExecute)
		sql = psprintf("set FORCE_PARALLEL_MODE = off; "
					   "select pgxc_pool_reload();");
	else
		sql = psprintf("set FORCE_PARALLEL_MODE = off; "
					   "EXECUTE DIRECT ON (\"%s\") "
					   "'select pgxc_pool_reload();'",
					   coordinatorName);
	res = PQexecBoolQuery(activeCoordinatorCoon, sql, true, complain);
	pfree(sql);
	if (res)
		ereport(LOG,
				(errmsg("%s execute pgxc_pool_reload() successfully",
						coordinatorName)));
	return res;
}

static SwitcherNodeWrapper *getHoldLockCoordinator(dlist_head *coordinators)
{
	dlist_iter iter;
	SwitcherNodeWrapper *coordinator;

	dlist_foreach(iter, coordinators)
	{
		coordinator = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		if (coordinator->holdClusterLock)
		{
			return coordinator;
		}
	}
	return NULL;
}

static SwitcherNodeWrapper *getGtmCoordMaster(dlist_head *coordinators)
{
	dlist_iter iter;
	SwitcherNodeWrapper *node;

	dlist_foreach(iter, coordinators)
	{
		node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		if (node->mgrNode->form.nodetype == CNDN_TYPE_GTM_COOR_MASTER)
		{
			return node;
		}
	}
	return NULL;
}

static void batchSetGtmInfoOnNodes(MgrNodeWrapper *gtmMaster,
								   dlist_head *nodes,
								   SwitcherNodeWrapper *ignoreNode)
{
	dlist_mutable_iter iter;
	SwitcherNodeWrapper *node;
	char *agtm_host;
	char snapsender_port[12] = {0};
	char gxidsender_port[12] = {0};

	EXTRACT_GTM_INFOMATION(gtmMaster, agtm_host,
						   snapsender_port, gxidsender_port);

	dlist_foreach_modify(iter, nodes)
	{
		node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		if (node != ignoreNode)
		{
			setGtmInfoInPGSqlConf(node->mgrNode,
								  agtm_host,
								  snapsender_port,
								  gxidsender_port,
								  true);
		}
	}
}

static void batchCheckGtmInfoOnNodes(MgrNodeWrapper *gtmMaster,
									 dlist_head *nodes,
									 SwitcherNodeWrapper *ignoreNode,
									 int checkSeconds)
{
	dlist_mutable_iter iter;
	SwitcherNodeWrapper *node;
	SwitcherNodeWrapper *copyOfNode;
	dlist_head copyOfNodes = DLIST_STATIC_INIT(copyOfNodes);
	int seconds;
	bool execOk = false;
	char *agtm_host;
	char snapsender_port[12] = {0};
	char gxidsender_port[12] = {0};

	dlist_foreach_modify(iter, nodes)
	{
		node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		if (node != ignoreNode)
		{
			copyOfNode = palloc(sizeof(SwitcherNodeWrapper));
			memcpy(copyOfNode, node, sizeof(SwitcherNodeWrapper));
			dlist_push_tail(&copyOfNodes, &copyOfNode->link);
		}
	}

	EXTRACT_GTM_INFOMATION(gtmMaster, agtm_host,
						   snapsender_port, gxidsender_port);

	for (seconds = 0; seconds < checkSeconds; seconds++)
	{
		dlist_foreach_modify(iter, &copyOfNodes)
		{
			node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
			execOk = checkGtmInfoInPGSqlConf(node->pgConn,
											 NameStr(node->mgrNode->form.nodename),
											 true,
											 agtm_host,
											 snapsender_port,
											 gxidsender_port);
			if (execOk)
			{
				ereport(NOTICE,
						(errmsg("the GTM information on %s is correct",
								NameStr(node->mgrNode->form.nodename))));
				ereport(LOG,
						(errmsg("the GTM information on %s is correct",
								NameStr(node->mgrNode->form.nodename))));
				dlist_delete(iter.cur);
				pfree(node);
			}
		}
		if (dlist_is_empty(&copyOfNodes))
		{
			break;
		}
		else
		{
			if (seconds < checkSeconds - 1)
				pg_usleep(1000000L);
		}
	}

	if (!dlist_is_empty(&copyOfNodes))
	{
		dlist_foreach_modify(iter, &copyOfNodes)
		{
			node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
			ereport(ERROR,
					(errmsg("check GTM information on %s failed",
							NameStr(node->mgrNode->form.nodename))));
			dlist_delete(iter.cur);
			pfree(node);
		}
	}
}

static void batchSetCheckGtmInfoOnNodes(MgrNodeWrapper *gtmMaster,
										dlist_head *nodes,
										SwitcherNodeWrapper *ignoreNode)
{
	batchSetGtmInfoOnNodes(gtmMaster, nodes, ignoreNode);
	batchCheckGtmInfoOnNodes(gtmMaster, nodes, ignoreNode,
							 CHECK_GTM_INFO_SECONDS);
}