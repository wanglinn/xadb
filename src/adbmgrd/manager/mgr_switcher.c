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

static SwitcherNodeWrapper *checkGetDatanodeOldMaster(char *oldMasterNodename,
													  MemoryContext spiContext);
static bool tryConnectNode(SwitcherNodeWrapper *node, int connectTimeout);
static void checkSlavesRunningStatus(dlist_head *slaveNodes,
									 dlist_head *failedSlaves,
									 dlist_head *runningSlaves);
static void runningSlavesFollowMaster(SwitcherNodeWrapper *masterNode,
									  dlist_head *runningSlaves);
static void appendSyncStandbyNames(MgrNodeWrapper *masterNode,
								   MgrNodeWrapper *slaveNode,
								   PGconn *masterPGconn);
static int walLsnDesc(const void *node1, const void *node2);
static bool checkSet_pool_release_to_idle_timeout(SwitcherNodeWrapper *node);
static void waitForNodeRunningOk(MgrNodeWrapper *mgrNode,
								 bool isMaster,
								 PGconn **pgConnP,
								 NodeRunningMode *runningModeP);
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
static void refreshPgxcNodesAfterSwitch(dlist_head *coordinators,
										MgrNodeWrapper *oldMaster,
										MgrNodeWrapper *newMaster);
static void updateMgrNodeBeforeSwitch(MgrNodeWrapper *mgrNode,
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
static bool deletePgxcNodeDatanodeSlaves(SwitcherNodeWrapper *coordinator);
static char *getAlterNodeSql(char *coordinatorName,
							 MgrNodeWrapper *oldNode,
							 MgrNodeWrapper *newNode);
static bool updatePgxcNode(SwitcherNodeWrapper *coordinator,
						   MgrNodeWrapper *oldMaster,
						   MgrNodeWrapper *newMaster,
						   bool complain);
static bool updateAdbSlot(SwitcherNodeWrapper *coordinator,
						  MgrNodeWrapper *oldMaster,
						  MgrNodeWrapper *newMaster,
						  bool complain);

/**
 * system function of failover datanode
 */
Datum mgr_failover_one_dn(PG_FUNCTION_ARGS)
{
	HeapTuple tup_result;
	char *nodename;
	bool force;
	NameData newMasterNodename;

	nodename = PG_GETARG_CSTRING(0);
	force = PG_GETARG_BOOL(1);

	if (RecoveryInProgress())
		ereport(ERROR,
				(errmsg("cannot assign TransactionIds during recovery")));

	switchDatanodeMaster(nodename, force, false, &newMasterNodename);

	tup_result = build_common_command_tuple(&newMasterNodename,
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
void switchDatanodeMaster(char *oldMasterNodename,
						  bool forceSwitch,
						  bool kickOutOldMaster,
						  Name newMasterNodename)
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
										  "switchMasterDatanode",
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
		oldMaster = checkGetDatanodeOldMaster(oldMasterNodename,
											  spiContext);

		checkGetSlaveNodesRunningStatus(oldMaster,
										spiContext,
										forceSwitch,
										&failedSlaves,
										&runningSlaves);

		precheckPromotionNode(&runningSlaves, forceSwitch);

		checkGetMasterCoordinators(spiContext, &coordinators);

		switchDataNodeOperation(oldMaster,
								&newMaster,
								&runningSlaves,
								&failedSlaves,
								&coordinators,
								spiContext,
								forceSwitch,
								kickOutOldMaster);

		namestrcpy(newMasterNodename,
				   NameStr(newMaster->mgrNode->form.nodename));
	}
	PG_CATCH();
	{
		/* Save error info in our stmt_mcontext */
		MemoryContextSwitchTo(oldContext);
		edata = CopyErrorData();
		FlushErrorState();

		revertClusterSetting(&coordinators, oldMaster, newMaster, false);
	}
	PG_END_TRY();

	/* pfree data and close PGconn */
	pfreeSwitcherNodeWrapper(oldMaster);
	pfreeSwitcherNodeWrapper(newMaster);
	pfreeSwitcherNodeWrapperList(&failedSlaves, NULL);
	pfreeSwitcherNodeWrapperList(&runningSlaves, NULL);
	pfreeSwitcherNodeWrapperList(&coordinators, NULL);

	(void)MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(switchContext);

	SPI_finish();

	if (edata)
		ReThrowError(edata);
}

/**
 * Switch oldMaster to the standby and switch newMaster to the master, 
 * other running slaves follow this newMaster.
 * If any error happened, will complain.
 * If all operations successfully completed, return the newMaster.
 */
void switchDataNodeOperation(SwitcherNodeWrapper *oldMaster,
							 SwitcherNodeWrapper **newMasterP,
							 dlist_head *runningSlaves,
							 dlist_head *failedSlaves,
							 dlist_head *coordinators,
							 MemoryContext spiContext,
							 bool forceSwitch,
							 bool kickOutOldMaster)
{
	SwitcherNodeWrapper *newMaster = NULL;

	/* Prevent other doctor processe from manipulating this node simultaneously */
	refreshOldMasterBeforeSwitch(oldMaster,
								 spiContext);

	/* Sentinel, ensure to shut down old master */
	shutdownNodeWithinSeconds(oldMaster->mgrNode, 10, 50, true);
	// callAgentPingAndStopNode(oldMaster->mgrNode,
	// 						 SHUTDOWN_I);

	newMaster = choosePromotionNode(runningSlaves,
									forceSwitch,
									failedSlaves);
	*newMasterP = newMaster;

	switchToDataNodeNewMaster(oldMaster,
							  newMaster,
							  runningSlaves,
							  failedSlaves,
							  coordinators,
							  spiContext,
							  kickOutOldMaster);
}

void switchToDataNodeNewMaster(SwitcherNodeWrapper *oldMaster,
							   SwitcherNodeWrapper *newMaster,
							   dlist_head *runningSlaves,
							   dlist_head *failedSlaves,
							   dlist_head *coordinators,
							   MemoryContext spiContext,
							   bool kickOutOldMaster)
{
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

	/* The better slave node is in front of the list */
	sortNodesByWalLsnDesc(runningSlaves);

	/* Ready to start switching, during the switching process, 
	 * failure is not allowed. If there it is, complain it. */
	tryLockCluster(coordinators, true);
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

	waitForNodeRunningOk(newMaster->mgrNode, true,
						 &newMaster->pgConn, &newMaster->runningMode);

	setCheckSynchronousStandbyNames(newMaster->mgrNode,
									newMaster->pgConn, "", 10);

	runningSlavesFollowMaster(newMaster, runningSlaves);

	refreshPgxcNodesAfterSwitch(coordinators, oldMaster->mgrNode,
								newMaster->mgrNode);

	refreshMgrNodeAfterSwitch(oldMaster, newMaster, runningSlaves,
							  failedSlaves, spiContext, kickOutOldMaster);
	refreshMgrUpdateparmAfterSwitch(oldMaster->mgrNode,
									newMaster->mgrNode,
									spiContext,
									kickOutOldMaster);

	tryUnlockCluster(coordinators, true);

	ereport(NOTICE,
			(errmsg("Switch the master node from %s to %s "
					"has been successfully completed",
					NameStr(oldMaster->mgrNode->form.nodename),
					NameStr(newMaster->mgrNode->form.nodename))));
	ereport(LOG,
			(errmsg("Switch the master node from %s to %s "
					"has been successfully completed",
					NameStr(oldMaster->mgrNode->form.nodename),
					NameStr(newMaster->mgrNode->form.nodename))));
}

bool revertClusterSetting(dlist_head *coordinators,
						  SwitcherNodeWrapper *oldMaster,
						  SwitcherNodeWrapper *newMaster,
						  bool complain)
{
	dlist_iter iter;
	SwitcherNodeWrapper *coordinator;
	bool execOk = true;

	dlist_foreach(iter, coordinators)
	{
		coordinator = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		if (coordinator->coordinatorPgxcNodeChanged)
		{
			if (updatePgxcNode(coordinator, newMaster->mgrNode,
							   oldMaster->mgrNode, complain))
			{
				coordinator->coordinatorPgxcNodeChanged = false;
			}
			else
			{
				execOk = false;
			}
		}
		if (coordinator->adbSlotChanged)
		{
			if (updateAdbSlot(coordinator, newMaster->mgrNode,
							  oldMaster->mgrNode, complain))
			{
				coordinator->adbSlotChanged = false;
			}
			else
			{
				execOk = false;
			}
		}
		if (!unlockCoordinator(coordinator, complain))
		{
			execOk = false;
		}
		if (execOk)
		{
			ereport(NOTICE,
					(errmsg("%s revert cluster setting successfully",
							NameStr(coordinator->mgrNode->form.nodename))));
			ereport(LOG,
					(errmsg("%s revert cluster setting successfully",
							NameStr(coordinator->mgrNode->form.nodename))));
		}
		else
		{
			ereport(complain ? ERROR : WARNING,
					(errmsg("%s revert cluster setting operation failed",
							NameStr(coordinator->mgrNode->form.nodename))));
		}
	}
	return execOk;
}

void precheckPromotionNode(dlist_head *runningSlaves, bool forceSwitch)
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
 * Before using this function, it is recommended to shutdown the old master 
 * first, so as to prevent disturb the determination of wal lsn. 
 * If a node is choosed as a candidate for promotion, this node will also 
 * be deleted in runningSlaves. In subsequent operations, the remaining nodes 
 * in runningSlaves should follow the candidate node.
 */
SwitcherNodeWrapper *choosePromotionNode(dlist_head *runningSlaves,
										 bool forceSwitch,
										 dlist_head *failedSlaves)
{
	SwitcherNodeWrapper *node;
	SwitcherNodeWrapper *promotionNode = NULL;
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
			if (promotionNode == NULL)
			{
				promotionNode = node;
			}
			else
			{
				if (node->walLsn > promotionNode->walLsn)
				{
					promotionNode = node;
				}
				else if (node->walLsn == promotionNode->walLsn)
				{
					if (strcmp(NameStr(promotionNode->mgrNode->form.nodesync),
							   getMgrNodeSyncStateValue(SYNC_STATE_SYNC)) == 0)
					{
						continue;
					}
					else
					{
						promotionNode = node;
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
	if (!forceSwitch && (strcmp(NameStr(promotionNode->mgrNode->form.nodesync),
								getMgrNodeSyncStateValue(SYNC_STATE_SYNC)) != 0))
	{
		ereport(ERROR,
				(errmsg("Candidate %s is not a Synchronous standby node, "
						"Abort switching to avoid data loss",
						NameStr(promotionNode->mgrNode->form.nodename))));
	}
	if (promotionNode != NULL)
	{
		dlist_foreach_modify(miter, runningSlaves)
		{
			node = dlist_container(SwitcherNodeWrapper, link, miter.cur);
			if (node == promotionNode)
			{
				dlist_delete(miter.cur);
			}
		}
		ereport(NOTICE,
				(errmsg("%s have the best wal lsn, "
						"choose it as a candidate for promotion",
						NameStr(promotionNode->mgrNode->form.nodename))));
		ereport(LOG,
				(errmsg("%s have the best wal lsn, "
						"choose it as a candidate for promotion",
						NameStr(promotionNode->mgrNode->form.nodename))));
	}
	else
	{
		ereport(ERROR,
				(errmsg("Can't find a qualified slave node that can be promoted")));
	}
	return promotionNode;
}

/**
 * connectTimeout: Maximum wait for connection, in seconds 
 * (write as a decimal integer, e.g. 10). 
 * Zero, negative, or not specified means wait indefinitely. 
 * If get connection successfully, the PGconn is saved in node->pgConn.
 */
bool tryConnectNode(SwitcherNodeWrapper *node, int connectTimeout)
{
	/* ensure to close obtained connection */
	pfreeSwitcherNodeWrapperPGconn(node);
	node->pgConn = getNodeDefaultDBConnection(node->mgrNode, connectTimeout);
	return node->pgConn != NULL;
}

bool tryLockCluster(dlist_head *coordinators, bool complain)
{
	dlist_iter iter;
	SwitcherNodeWrapper *coordinator;
	bool execOk = true;

	if (dlist_is_empty(coordinators))
	{
		ereport(complain ? ERROR : WARNING,
				(errmsg("There is no master coordinator, lock cluster failed")));
		return false;
	}

	dlist_foreach(iter, coordinators)
	{
		coordinator = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		if (!lockCoordinator(coordinator, complain))
		{
			execOk = false;
		}
	}
	return execOk;
}

/*
 * Executes an "pause cluster" operation on master coordinator. 
 * If any of the sub-operations fails, the execution of the remaining 
 * sub-operations is abandoned, and the function returns false. 
 * Finally, if operation failed, may should call tryUnlockCluster() to 
 * revert the settings that was changed during execution.
 */
bool lockCoordinator(SwitcherNodeWrapper *coordinator, bool complain)
{
	bool execOk;
	/* Execution to here, it means that connect to node successfully */
	execOk = checkSet_pool_release_to_idle_timeout(coordinator);
	if (!execOk)
	{
		goto end;
	}
	/* execute the pause cluster command */
	execOk = exec_pg_pause_cluster(coordinator->pgConn, false);
	coordinator->coordinatorPaused = execOk;

end:
	if (execOk)
	{
		ereport(NOTICE,
				(errmsg("%s try lock cluster successfully",
						NameStr(coordinator->mgrNode->form.nodename))));
		ereport(LOG,
				(errmsg("%s try lock cluster successfully",
						NameStr(coordinator->mgrNode->form.nodename))));
	}
	else
	{
		ereport(complain ? ERROR : WARNING,
				(errmsg("%s try lock cluster failed",
						NameStr(coordinator->mgrNode->form.nodename))));
	}
	return execOk;
}

bool tryUnlockCluster(dlist_head *coordinators, bool complain)
{
	dlist_iter iter;
	SwitcherNodeWrapper *coordinator;
	bool execOk = true;

	dlist_foreach(iter, coordinators)
	{
		coordinator = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		if (!unlockCoordinator(coordinator, complain))
		{
			execOk = false;
		}
	}
	return execOk;
}

/*
 * Executes an "unpause cluster" operation on master coordinator.
 * If any of the sub-operations failed to execute, the funtion returns 
 * false, but ignores any execution failures and continues to execute 
 * the remaining sub-operations.
 */
bool unlockCoordinator(SwitcherNodeWrapper *coordinator, bool complain)
{
	bool execOk = true;
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
			execOk = false;
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
			execOk = false;
		}
	}
	callAgentReloadNode(coordinator->mgrNode, false);

	if (!exec_pool_close_idle_conn(coordinator->pgConn, false))
		execOk = false;
	if (coordinator->coordinatorPaused)
	{
		if (exec_pg_unpause_cluster(coordinator->pgConn, false))
		{
			coordinator->coordinatorPaused = false;
		}
		else
		{
			execOk = false;
		}
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
	return execOk;
}

void checkGetSlaveNodesRunningStatus(SwitcherNodeWrapper *masterNode,
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
void sortNodesByWalLsnDesc(dlist_head *nodes)
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

void checkGetMasterCoordinators(MemoryContext spiContext,
								dlist_head *coordinators)
{
	dlist_head mgrNodes = DLIST_STATIC_INIT(mgrNodes);
	SwitcherNodeWrapper *node;
	dlist_iter iter;

	selectMgrMasterCoordinators(spiContext, &mgrNodes);
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
		node->runningMode = getNodeRunningMode(node->pgConn);
		if (node->runningMode != NODE_RUNNING_MODE_MASTER)
		{
			ereport(ERROR,
					(errmsg("coordinator %s configured as master, "
							"but actually did not run in that expected state",
							NameStr(node->mgrNode->form.nodename))));
		}
	}
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

	shutdownNodeWithinSeconds(slaveNode, 10, 50, true);
	//callAgentPingAndStopNode(slaveNode, SHUTDOWN_I);

	callAgentStartNode(slaveNode, true);

	waitForNodeRunningOk(slaveNode, false, NULL, NULL);

	appendSyncStandbyNames(masterNode, slaveNode, masterPGconn);

	ereport(LOG,
			(errmsg("%s has followed master %s",
					NameStr(slaveNode->form.nodename),
					NameStr(masterNode->form.nodename))));
}

static SwitcherNodeWrapper *checkGetDatanodeOldMaster(char *oldMasterNodename,
													  MemoryContext spiContext)
{
	MgrNodeWrapper *mgrNode;
	SwitcherNodeWrapper *oldMaster;

	mgrNode = selectMgrNodeByNodenameType(oldMasterNodename,
										  CNDN_TYPE_DATANODE_MASTER,
										  spiContext);
	if (!mgrNode)
	{
		ereport(ERROR,
				(errmsg("%s does not exist or is not a master datanode",
						oldMasterNodename)));
	}
	if (mgrNode->form.nodetype != CNDN_TYPE_DATANODE_MASTER)
	{
		ereport(ERROR,
				(errmsg("%s is not a master datanode",
						oldMasterNodename)));
	}
	if (!mgrNode->form.nodeinited)
	{
		ereport(ERROR,
				(errmsg("%s has not be initialized",
						oldMasterNodename)));
	}
	if (!mgrNode->form.nodeincluster)
	{
		ereport(ERROR,
				(errmsg("%s has been kicked out of the cluster",
						oldMasterNodename)));
	}
	oldMaster = palloc0(sizeof(SwitcherNodeWrapper));
	oldMaster->mgrNode = mgrNode;
	return oldMaster;
}

static void runningSlavesFollowMaster(SwitcherNodeWrapper *masterNode,
									  dlist_head *runningSlaves)
{
	dlist_iter iter;
	SwitcherNodeWrapper *slaveNode;

	/* config these slave node to follow new master and restart them */
	dlist_foreach(iter, runningSlaves)
	{
		slaveNode = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		/* ensure to close obtained connection */
		pfreeSwitcherNodeWrapperPGconn(slaveNode);
		appendSlaveNodeFollowMaster(masterNode->mgrNode,
									slaveNode->mgrNode,
									masterNode->pgConn);
	}
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
		/* "FIRST 1 (datanode2,datanode4)" will get result datanode2,datanode4 */
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
										masterPGconn, newSyncNames, 10);
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

static bool checkSet_pool_release_to_idle_timeout(SwitcherNodeWrapper *node)
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
		return true;
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
	return execOk;
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

	ereport(NOTICE,
			(errmsg("waiting for %s can accept connections...",
					NameStr(mgrNode->form.nodename))));

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

	pg_usleep(100000L);

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

void refreshOldMasterBeforeSwitch(SwitcherNodeWrapper *oldMaster,
								  MemoryContext spiContext)
{
	char *newCurestatus;
	/* Use CURE_STATUS_SWITCHING as a tag to prevent the node doctor from 
	 * operating on this node simultaneously. */
	newCurestatus = CURE_STATUS_SWITCHING;

	/* backup curestatus */
	memcpy(&oldMaster->oldCurestatus,
		   &oldMaster->mgrNode->form.curestatus, sizeof(NameData));
	updateMgrNodeBeforeSwitch(oldMaster->mgrNode,
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
	updateMgrNodeBeforeSwitch(newMaster->mgrNode, newCurestatus,
							  spiContext);

	dlist_foreach(iter, runningSlaves)
	{
		node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		memcpy(&node->oldCurestatus,
			   &node->mgrNode->form.curestatus, sizeof(NameData));
		updateMgrNodeBeforeSwitch(node->mgrNode, newCurestatus,
								  spiContext);
	}
	dlist_foreach(iter, failedSlaves)
	{
		node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		memcpy(&node->oldCurestatus,
			   &node->mgrNode->form.curestatus, sizeof(NameData));
		updateMgrNodeBeforeSwitch(node->mgrNode, newCurestatus,
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

static void refreshPgxcNodesAfterSwitch(dlist_head *coordinators,
										MgrNodeWrapper *oldMaster,
										MgrNodeWrapper *newMaster)
{
	dlist_iter iter;
	SwitcherNodeWrapper *coordinator;
	bool slotUpdated = false;

	dlist_foreach(iter, coordinators)
	{
		coordinator = dlist_container(SwitcherNodeWrapper, link, iter.cur);

		deletePgxcNodeDatanodeSlaves(coordinator);
		updatePgxcNode(coordinator, oldMaster, newMaster, true);
		if (!slotUpdated)
		{
			/* Adb_slot only needs to be updated once */
			slotUpdated = updateAdbSlot(coordinator, oldMaster,
										newMaster, true);
		}
		exec_pgxc_pool_reload(coordinator->pgConn, true);
	}
}

/**
 * update curestatus can avoid adb doctor monitor this node
 */
static void updateMgrNodeBeforeSwitch(MgrNodeWrapper *mgrNode,
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

static bool deletePgxcNodeDatanodeSlaves(SwitcherNodeWrapper *coordinator)
{
	char *sql;
	bool execOk;
	sql = psprintf("delete from pgxc_node where node_type = '%c';",
				   PGXC_NODE_DATANODESLAVE);
	execOk = PQexecCommandSql(coordinator->pgConn, sql, false);
	pfree(sql);
	if (!execOk)
	{
		ereport(WARNING,
				(errmsg("%s delete pgxc_node datanode slaves failed",
						NameStr(coordinator->mgrNode->form.nodename))));
	}
	return execOk;
}

static char *getAlterNodeSql(char *coordinatorName,
							 MgrNodeWrapper *oldNode,
							 MgrNodeWrapper *newNode)
{
	return psprintf("alter node \"%s\" with(name='%s', host='%s', port=%d) on (\"%s\");",
					NameStr(oldNode->form.nodename),
					NameStr(newNode->form.nodename),
					newNode->host->hostaddr,
					newNode->form.nodeport,
					coordinatorName);
}
static bool updatePgxcNode(SwitcherNodeWrapper *coordinator,
						   MgrNodeWrapper *oldMaster,
						   MgrNodeWrapper *newMaster,
						   bool complain)
{
	char *sql;
	bool execOk;

	sql = getAlterNodeSql(NameStr(coordinator->mgrNode->form.nodename),
						  oldMaster,
						  newMaster);
	execOk = PQexecCommandSql(coordinator->pgConn, sql, false);
	pfree(sql);
	if (execOk)
	{
		coordinator->coordinatorPgxcNodeChanged = true;
	}
	else
	{
		ereport(complain ? ERROR : WARNING,
				(errmsg("%s refresh pgxc_node failed",
						NameStr(coordinator->mgrNode->form.nodename))));
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
				(errmsg("refresh the new master %s information in adb_slot failed",
						NameStr(newMaster->form.nodename))));
	}

	/* update adb_slot */
	if (numberOfSlots > 0)
	{
		ereport(LOG, (errmsg("refresh new master %s slot information",
							 NameStr(newMaster->form.nodename))));
		ereport(NOTICE, (errmsg("refresh new master %s slot information",
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
					(errmsg("flush new master %s slot information failed",
							NameStr(newMaster->form.nodename))));
		}
	}
	return execOk;
}
