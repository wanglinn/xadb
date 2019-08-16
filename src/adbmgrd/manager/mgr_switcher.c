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

static SwitcherNodeWrapper *checkGetDatanodeOldMaster(char *oldMasterNodename,
													  MemoryContext spiContext);

static void masterNodeAcceptSlaveNodes(SwitcherNodeWrapper *masterNode,
									   dlist_head *runningNodes,
									   dlist_head *failedSlaves);
static void slaveNodesFollowMasterNode(SwitcherNodeWrapper *masterNode,
									   dlist_head *slaveNodes);

static bool setPGHbaTrustMe(SwitcherNodeWrapper *node, char *myAddress);
static void setPGHbaTrustSlaveReplication(SwitcherNodeWrapper *masterNode,
										  SwitcherNodeWrapper *slaveNode);
static void setSynchronousStandbyNames(SwitcherNodeWrapper *masterNode,
									   char *value);
static void setSlaveNodeRecoveryConf(SwitcherNodeWrapper *masterNode,
									 SwitcherNodeWrapper *slaveNode);
static int walLsnDesc(const void *node1, const void *node2);
static bool checkSet_pool_release_to_idle_timeout(SwitcherNodeWrapper *node);
static void waitForNodeRunningOk(SwitcherNodeWrapper *node, bool isMaster);

static void refreshMgrNodeBeforeSwitch(SwitcherNodeWrapper *oldMaster,
									   SwitcherNodeWrapper *newMaster,
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
											MemoryContext spiContext);
static void refreshPgxcNodesAfterSwitch(dlist_head *coordinators,
										MgrNodeWrapper *oldMaster,
										MgrNodeWrapper *newMaster);
static bool updateMgrNodeBeforeSwitch(MgrNodeWrapper *mgrNode,
									  char *newCureStatus,
									  MemoryContext spiContext,
									  bool complain);
static bool updateMgrNodeAfterSwitch(MgrNodeWrapper *mgrNode,
									 char *newCureStatus,
									 MemoryContext spiContext,
									 bool complain);
static bool updateMgrUpdateparmNodetype(char *nodename, char nodetype,
										MemoryContext spiContext,
										bool complain);
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

		checkGetSlaveNodes(oldMaster,
						   spiContext,
						   forceSwitch,
						   &failedSlaves,
						   &runningSlaves);
		newMaster = choosePromotionNode(&runningSlaves,
										forceSwitch,
										&failedSlaves);
		/* The better slave node is in front of the list */
		sortNodesByWalLsnDesc(&runningSlaves,
							  newMaster->mgrNode->nodeOid);

		checkGetMasterCoordinators(spiContext, &coordinators);

		switchDataNodeOperation(oldMaster,
								newMaster,
								&runningSlaves,
								&failedSlaves,
								&coordinators,
								spiContext,
								kickOutOldMaster);

		namestrcpy(newMasterNodename,
				   NameStr(newMaster->mgrNode->form.nodename));

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
 */
void switchDataNodeOperation(SwitcherNodeWrapper *oldMaster,
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
	refreshMgrNodeBeforeSwitch(oldMaster, newMaster, runningSlaves,
							   failedSlaves, spiContext);
	/* Try to shut down old master, but ignore operation failure. */
	callAgentStopNode(oldMaster->mgrNode, SHUTDOWN_I, false);
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

	waitForNodeRunningOk(newMaster, true);

	setSynchronousStandbyNames(newMaster, "");

	masterNodeAcceptSlaveNodes(newMaster, runningSlaves, failedSlaves);

	slaveNodesFollowMasterNode(newMaster, runningSlaves);

	refreshPgxcNodesAfterSwitch(coordinators, oldMaster->mgrNode,
								newMaster->mgrNode);

	refreshMgrNodeAfterSwitch(oldMaster, newMaster, runningSlaves,
							  failedSlaves, spiContext, kickOutOldMaster);
	refreshMgrUpdateparmAfterSwitch(oldMaster->mgrNode,
									newMaster->mgrNode, spiContext);

	tryUnlockCluster(coordinators, true);
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

SwitcherNodeWrapper *choosePromotionNode(dlist_head *slaveNodes,
										 bool allowFailed,
										 dlist_head *failedSlaves)
{
	SwitcherNodeWrapper *node;
	SwitcherNodeWrapper *promotionNode = NULL;
	dlist_mutable_iter miter;

	dlist_foreach_modify(miter, slaveNodes)
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
			/* choose the biggest wal lsn */
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
					if (node->runningMode == NODE_RUNNING_MODE_MASTER)
					{
						promotionNode = node;
					}
					else
					{
						continue;
					}
				}
				else
				{
					continue;
				}
			}
		}
	}
	if (!allowFailed && !dlist_is_empty(failedSlaves))
	{
		ereport(ERROR,
				(errmsg("There are some slave nodes failed, "
						"Abort switching to avoid data loss")));
	}
	if (promotionNode != NULL)
	{
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
	NodeConnectionStatus connStatus;
	NameData myAddress;
	int nTrys;

	/* ensure to close obtained connection */
	pfreeSwitcherNodeWrapperPGconn(node);

	connStatus = connectNodeDefaultDB(node->mgrNode, 10, &node->pgConn);
	if (connStatus == NODE_CONNECTION_STATUS_SUCCESS)
	{
		return true;
	}
	else if (connStatus == NODE_CONNECTION_STATUS_BUSY ||
			 connStatus == NODE_CONNECTION_STATUS_STARTING_UP)
	{
		return false;
	}
	else
	{
		/* may be is the reason of hba, try to get myself 
		 * addreess and refresh node's pg_hba.conf */
		memset(myAddress.data, 0, NAMEDATALEN);
		if (!mgr_get_self_address(node->mgrNode->host->hostaddr,
								  node->mgrNode->form.nodeport,
								  &myAddress))
		{
			ereport(WARNING,
					(errmsg("on ADB Manager get local address fail, "
							"this may be caused by cannot get the "
							"connection to node %s",
							NameStr(node->mgrNode->form.nodename))));
			return false;
		}
		if (!setPGHbaTrustMe(node, NameStr(myAddress)))
		{
			ereport(WARNING,
					(errmsg("set node %s trust me failed, "
							"this may be caused by network error",
							NameStr(node->mgrNode->form.nodename))));
			return false;
		}
		for (nTrys = 0; nTrys < 10; nTrys++)
		{
			CHECK_FOR_INTERRUPTS();
			/*sleep 0.1s*/
			pg_usleep(100000L);
			/* ensure to close obtained connection */
			pfreeSwitcherNodeWrapperPGconn(node);

			connStatus = connectNodeDefaultDB(node->mgrNode,
											  10,
											  &node->pgConn);
			if (connStatus == NODE_CONNECTION_STATUS_SUCCESS)
			{
				return true;
			}
			else
			{
				continue;
			}
		}
		return false;
	}
}

bool tryLockCluster(dlist_head *coordinators, bool complain)
{
	dlist_iter iter;
	SwitcherNodeWrapper *coordinator;
	bool execOk = true;

	if (dlist_is_empty(coordinators))
	{
		ereport(complain ? ERROR : WARNING,
				(errmsg("no master coordinator, lock cluster failed")));
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

void checkGetSlaveNodes(SwitcherNodeWrapper *masterNode,
						MemoryContext spiContext,
						bool allowFailed,
						dlist_head *failedSlaves,
						dlist_head *runningSlaves)
{
	char slaveNodetype;
	dlist_head mgrNodes = DLIST_STATIC_INIT(mgrNodes);
	dlist_head slaveNodes = DLIST_STATIC_INIT(slaveNodes);

	slaveNodetype = getMgrSlaveNodetype(masterNode->mgrNode->form.nodetype);
	selectMgrSlaveNodes(masterNode->mgrNode->nodeOid,
						slaveNodetype, spiContext, &mgrNodes);
	mgrNodesToSwitcherNodes(&mgrNodes, &slaveNodes);
	checkNodesRunningStatus(&slaveNodes, failedSlaves, runningSlaves);
	if (!allowFailed && !dlist_is_empty(failedSlaves))
	{
		ereport(ERROR,
				(errmsg("There are some slave nodes failed, "
						"Abort switching to avoid data loss")));
	}
}

void checkNodesRunningStatus(dlist_head *nodes,
							 dlist_head *failedNodes,
							 dlist_head *runningNodes)
{
	SwitcherNodeWrapper *node;
	dlist_mutable_iter miter;

	dlist_foreach_modify(miter, nodes)
	{
		node = dlist_container(SwitcherNodeWrapper, link, miter.cur);
		if (tryConnectNode(node, 10))
		{
			node->runningMode = getNodeRunningMode(node->pgConn);
			if (node->runningMode == NODE_RUNNING_MODE_UNKNOW)
			{
				dlist_delete(miter.cur);
				dlist_push_tail(failedNodes, &node->link);
				ereport(WARNING,
						(errmsg("node %s status unknow",
								NameStr(node->mgrNode->form.nodename))));
			}
			else
			{
				dlist_delete(miter.cur);
				dlist_push_tail(runningNodes, &node->link);
			}
		}
		else
		{
			dlist_delete(miter.cur);
			dlist_push_tail(failedNodes, &node->link);
			ereport(WARNING,
					(errmsg("%s connection failed",
							NameStr(node->mgrNode->form.nodename))));
		}
	}
}

/**
 * Sort new slave nodes (exclude new master node) by walReceiveLsn.
 * The larger the walReceiveLsn, the more in front of the dlist.
 */
void sortNodesByWalLsnDesc(dlist_head *nodes,
						   Oid deleteNodeOid)
{
	dlist_mutable_iter miter;
	SwitcherNodeWrapper *node;
	SwitcherNodeWrapper **sortItems;
	int i, numOfNodes;
	dlist_head tempList = DLIST_STATIC_INIT(tempList);

	numOfNodes = 0;
	/* Exclude new master node from old slave nodes */
	dlist_foreach_modify(miter, nodes)
	{
		node = dlist_container(SwitcherNodeWrapper, link, miter.cur);
		if (node->mgrNode->nodeOid != deleteNodeOid)
		{
			dlist_delete(miter.cur);
			dlist_push_tail(&tempList, &node->link);
			numOfNodes++;
		}
	}
	if (numOfNodes > 1)
	{
		sortItems = palloc(sizeof(SwitcherNodeWrapper *) * numOfNodes);
		i = 0;
		dlist_foreach_modify(miter, &tempList)
		{
			node = dlist_container(SwitcherNodeWrapper, link, miter.cur);
			sortItems[i] = node;
			i++;
		}
		/* order by wal lsn desc */
		qsort(sortItems, numOfNodes, sizeof(SwitcherNodeWrapper *), walLsnDesc);
		/* add to dlist in order */
		dlist_init(&tempList);
		for (i = 0; i < numOfNodes; i++)
		{
			dlist_push_tail(&tempList, &sortItems[i]->link);
		}
	}
	else
	{
		/* No need to sort */
	}

	dlist_init(nodes);
	dlist_foreach_modify(miter, &tempList)
	{
		node = dlist_container(SwitcherNodeWrapper, link, miter.cur);
		dlist_delete(miter.cur);
		dlist_push_tail(nodes, &node->link);
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
	dlist_mutable_iter miter;

	if (mgrNodes == NULL || dlist_is_empty(mgrNodes))
	{
		return;
	}
	dlist_foreach_modify(miter, mgrNodes)
	{
		mgrNode = dlist_container(MgrNodeWrapper, link, miter.cur);
		switcherNode = palloc0(sizeof(SwitcherNodeWrapper));
		switcherNode->mgrNode = mgrNode;
		dlist_delete(miter.cur);
		dlist_push_tail(switcherNodes, &switcherNode->link);
	}
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
				(errmsg("%s does not exist",
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

/**
 * return the sync slave datanode name
 */
static void masterNodeAcceptSlaveNodes(SwitcherNodeWrapper *masterNode,
									   dlist_head *runningNodes,
									   dlist_head *failedSlaves)
{
	dlist_iter iter;
	SwitcherNodeWrapper *node;
	StringInfoData syncNodenames;
	char *syncValue;
	int nRunningSlaves = 0;

	initStringInfo(&syncNodenames);
	dlist_foreach(iter, runningNodes)
	{
		nRunningSlaves++;
		node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		if (syncNodenames.len == 0)
		{
			appendStringInfo(&syncNodenames, "%s",
							 NameStr(node->mgrNode->form.nodename));
		}
		else
		{
			appendStringInfo(&syncNodenames, ",%s",
							 NameStr(node->mgrNode->form.nodename));
		}
		/* config master pg_hba.conf to trust slave */
		setPGHbaTrustSlaveReplication(masterNode, node);
		/* prepare data to update mgr_node */
		if (nRunningSlaves == 1)
		{
			namestrcpy(&node->mgrNode->form.nodesync,
					   getMgrNodeSyncStateValue(SYNC_STATE_SYNC));
		}
		else
		{
			namestrcpy(&node->mgrNode->form.nodesync,
					   getMgrNodeSyncStateValue(SYNC_STATE_POTENTIAL));
		}
	}
	dlist_foreach(iter, failedSlaves)
	{
		node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		/* config master pg_hba.conf to trust slave */
		setPGHbaTrustSlaveReplication(masterNode, node);
	}
	if (nRunningSlaves > 0)
	{
		/* config master allow synchronous streaming replication */
		syncValue = psprintf("FIRST %d (%s)", 1, syncNodenames.data);
		setSynchronousStandbyNames(masterNode, syncValue);
		pfree(syncValue);
	}
	pfree(syncNodenames.data);
}

static void slaveNodesFollowMasterNode(SwitcherNodeWrapper *masterNode,
									   dlist_head *slaveNodes)
{
	dlist_iter iter;
	SwitcherNodeWrapper *node;

	/* config these slave node to follow new master and restart them */
	dlist_foreach(iter, slaveNodes)
	{
		node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		setSynchronousStandbyNames(node, "");
		setSlaveNodeRecoveryConf(masterNode, node);
		/* ensure to close obtained connection */
		pfreeSwitcherNodeWrapperPGconn(node);
		callAgentStopNode(node->mgrNode, SHUTDOWN_I, true);
		callAgentStartNode(node->mgrNode, true);
	}
	dlist_foreach(iter, slaveNodes)
	{
		/* config and restart slave node */
		node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		waitForNodeRunningOk(node, false);
		ereport(LOG,
				(errmsg("%s has followed new master %s",
						NameStr(node->mgrNode->form.nodename),
						NameStr(masterNode->mgrNode->form.nodename))));
	}
}

static bool setPGHbaTrustMe(SwitcherNodeWrapper *node, char *myAddress)
{
	PGHbaItem *hbaItems;
	bool execOk;

	hbaItems = newPGHbaItem(CONNECT_HOST, DEFAULT_DB,
							NameStr(node->mgrNode->host->form.hostuser),
							myAddress, 32, "trust");

	execOk = callAgentRefreshPGHbaConf(node->mgrNode, hbaItems, false);
	pfreePGHbaItem(hbaItems);
	if (!execOk)
		return execOk;
	execOk = callAgentReloadNode(node->mgrNode, false);
	return execOk;
}

static void setPGHbaTrustSlaveReplication(SwitcherNodeWrapper *masterNode,
										  SwitcherNodeWrapper *slaveNode)
{
	PGHbaItem *hbaItems;

	hbaItems = newPGHbaItem(CONNECT_HOST, "replication",
							NameStr(slaveNode->mgrNode->host->form.hostuser),
							slaveNode->mgrNode->host->hostaddr,
							32, "trust");
	callAgentRefreshPGHbaConf(masterNode->mgrNode, hbaItems, true);
	callAgentReloadNode(masterNode->mgrNode, true);
	pfreePGHbaItem(hbaItems);
}

static void setSynchronousStandbyNames(SwitcherNodeWrapper *masterNode,
									   char *value)
{
	PGConfParameterItem *syncStandbys;

	syncStandbys = newPGConfParameterItem("synchronous_standby_names",
										  value, true);
	callAgentRefreshPGSqlConfReload(masterNode->mgrNode,
									syncStandbys,
									true);
	pfreePGConfParameterItem(syncStandbys);
}

static void setSlaveNodeRecoveryConf(SwitcherNodeWrapper *masterNode,
									 SwitcherNodeWrapper *slaveNode)
{
	PGConfParameterItem *items;
	char *primary_conninfo_value;
	char *slavePGUser;

	items = newPGConfParameterItem("recovery_target_timeline", "latest", false);
	items->next = newPGConfParameterItem("standby_mode", "on", false);

	slavePGUser = getNodePGUser(slaveNode->mgrNode->form.nodetype,
								NameStr(slaveNode->mgrNode->host->form.hostuser));
	primary_conninfo_value = psprintf("host=%s port=%d user=%s application_name=%s",
									  masterNode->mgrNode->host->hostaddr,
									  masterNode->mgrNode->form.nodeport,
									  slavePGUser,
									  NameStr(slaveNode->mgrNode->form.nodename));
	items->next = newPGConfParameterItem("primary_conninfo",
										 primary_conninfo_value, true);
	pfree(slavePGUser);
	pfree(primary_conninfo_value);

	callAgentRefreshRecoveryConf(slaveNode->mgrNode, items, true);
	pfreePGConfParameterItem(items);
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
	originalValue = showNodeParameter(node->pgConn, parameterName);
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
			CHECK_FOR_INTERRUPTS();
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

static void waitForNodeRunningOk(SwitcherNodeWrapper *node, bool isMaster)
{
	NodeConnectionStatus connStatus;
	NodeRunningMode expectMode;
	int networkFailures;
	int maxNetworkFailures = 60;

	ereport(NOTICE,
			(errmsg("waiting for %s can accept connections...",
					NameStr(node->mgrNode->form.nodename))));

	networkFailures = 0;
	while (networkFailures < maxNetworkFailures)
	{
		connStatus = connectNodeDefaultDB(node->mgrNode, 10, &node->pgConn);
		if (connStatus == NODE_CONNECTION_STATUS_SUCCESS)
		{
			networkFailures = 0;
			break;
		}
		else if (connStatus == NODE_CONNECTION_STATUS_BUSY ||
				 connStatus == NODE_CONNECTION_STATUS_STARTING_UP)
		{
			networkFailures = 0;
		}
		else
		{
			networkFailures++;
		}

		CHECK_FOR_INTERRUPTS();

		fputs(_("."), stdout);
		fflush(stdout);
		pg_usleep(1 * 1000000L);
	}
	if (connStatus != NODE_CONNECTION_STATUS_SUCCESS)
	{
		ereport(ERROR,
				(errmsg("%s connection failed, may crashed",
						NameStr(node->mgrNode->form.nodename))));
	}

	expectMode = getExpectedNodeRunningMode(isMaster);

	pg_usleep(100000L);

	/* wait for recovery finished */
	node->runningMode = getNodeRunningMode(node->pgConn);
	if (node->runningMode == expectMode)
	{
		ereport(LOG,
				(errmsg("%s running normally",
						NameStr(node->mgrNode->form.nodename))));
	}
	else
	{
		ereport(ERROR,
				(errmsg("select pg_is_in_recovery from node %s failed",
						NameStr(node->mgrNode->form.nodename))));
	}
}

static void refreshMgrNodeBeforeSwitch(SwitcherNodeWrapper *oldMaster,
									   SwitcherNodeWrapper *newMaster,
									   dlist_head *runningSlaves,
									   dlist_head *failedSlaves,
									   MemoryContext spiContext)
{
	dlist_iter iter;
	SwitcherNodeWrapper *node;
	char *newCureStatus;
	/* Use CURE_STATUS_SWITCHING as a tag to prevent the node doctor from 
	 * operating on this node simultaneously. */
	newCureStatus = CURE_STATUS_SWITCHING;
	updateMgrNodeBeforeSwitch(oldMaster->mgrNode, newCureStatus,
							  spiContext, true);
	updateMgrNodeBeforeSwitch(newMaster->mgrNode, newCureStatus,
							  spiContext, true);

	dlist_foreach(iter, runningSlaves)
	{
		node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		updateMgrNodeBeforeSwitch(node->mgrNode, newCureStatus,
								  spiContext, true);
	}
	dlist_foreach(iter, failedSlaves)
	{
		node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		updateMgrNodeBeforeSwitch(node->mgrNode, newCureStatus,
								  spiContext, true);
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
	oldMaster->mgrNode->form.nodemasternameoid = newMaster->mgrNode->nodeOid;
	if (kickOutOldMaster)
	{
		oldMaster->mgrNode->form.nodeinited = false;
		oldMaster->mgrNode->form.nodeincluster = false;
		oldMaster->mgrNode->form.allowcure = false;
		/* Kick the old master out of the cluster */
		updateMgrNodeAfterSwitch(oldMaster->mgrNode, CURE_STATUS_NORMAL,
								 spiContext, true);
	}
	else
	{
		oldMaster->mgrNode->form.nodetype =
			getMgrSlaveNodetype(oldMaster->mgrNode->form.nodetype);
		/* Update Old master follow the new master, 
		 * Then, the task of pg_rewind this old master is handled to the node doctor. */
		updateMgrNodeAfterSwitch(oldMaster->mgrNode, CURE_STATUS_OLD_MASTER,
								 spiContext, true);
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
							 spiContext, true);

	dlist_foreach(iter, runningSlaves)
	{
		node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		/* nodesync field was set in other function */
		node->mgrNode->form.nodemasternameoid = newMaster->mgrNode->nodeOid;
		/* Admit the reign of new master */
		updateMgrNodeAfterSwitch(node->mgrNode, CURE_STATUS_SWITCHED,
								 spiContext, true);
	}
	dlist_foreach(iter, failedSlaves)
	{
		node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		namestrcpy(&node->mgrNode->form.nodesync, "");
		node->mgrNode->form.nodemasternameoid = newMaster->mgrNode->nodeOid;
		/* Update other failure slave node follow the new master, 
		 * Then, The node "follow the new master node" task is handed over 
		 * to the node doctor. */
		updateMgrNodeAfterSwitch(node->mgrNode, CURE_STATUS_FOLLOW_FAIL,
								 spiContext, true);
	}
}

static void refreshMgrUpdateparmAfterSwitch(MgrNodeWrapper *oldMaster,
											MgrNodeWrapper *newMaster,
											MemoryContext spiContext)
{
	/* old master update to slave */
	updateMgrUpdateparmNodetype(NameStr(oldMaster->form.nodename),
								newMaster->form.nodetype,
								spiContext, true);
	/* new master update to master */
	updateMgrUpdateparmNodetype(NameStr(newMaster->form.nodename),
								oldMaster->form.nodetype,
								spiContext, true);
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
static bool updateMgrNodeBeforeSwitch(MgrNodeWrapper *mgrNode,
									  char *newCureStatus,
									  MemoryContext spiContext,
									  bool complain)
{
	int spiRes;
	MemoryContext oldCtx;
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "UPDATE pg_catalog.mgr_node \n"
					 "SET curestatus = '%s' \n"
					 "WHERE oid = %u \n"
					 "AND curestatus = '%s' \n",
					 newCureStatus,
					 mgrNode->nodeOid,
					 NameStr(mgrNode->form.curestatus));
	oldCtx = MemoryContextSwitchTo(spiContext);
	spiRes = SPI_execute(sql.data, false, 0);
	MemoryContextSwitchTo(oldCtx);

	pfree(sql.data);
	if (spiRes != SPI_OK_UPDATE)
	{
		ereport(complain ? ERROR : WARNING,
				(errmsg("SPI_execute failed: error code %d",
						spiRes)));
		return false;
	}
	if (SPI_processed != 1)
	{
		ereport(complain ? ERROR : WARNING,
				(errmsg("SPI_execute failed: expected rows:%d, actually:%lu",
						1,
						SPI_processed)));
		return false;
	}
	namestrcpy(&mgrNode->form.curestatus, newCureStatus);
	return true;
}

static bool updateMgrNodeAfterSwitch(MgrNodeWrapper *mgrNode,
									 char *newCureStatus,
									 MemoryContext spiContext,
									 bool complain)
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
					 newCureStatus,
					 mgrNode->nodeOid,
					 NameStr(mgrNode->form.curestatus));
	oldCtx = MemoryContextSwitchTo(spiContext);
	spiRes = SPI_execute(sql.data, false, 0);
	MemoryContextSwitchTo(oldCtx);

	pfree(sql.data);
	if (spiRes != SPI_OK_UPDATE)
	{
		ereport(complain ? ERROR : WARNING,
				(errmsg("SPI_execute failed: error code %d",
						spiRes)));
		return false;
	}
	if (SPI_processed != 1)
	{
		ereport(complain ? ERROR : WARNING,
				(errmsg("SPI_execute failed: expected rows:%d, actually:%lu",
						1,
						SPI_processed)));
		return false;
	}
	namestrcpy(&mgrNode->form.curestatus, newCureStatus);
	return true;
}

static bool updateMgrUpdateparmNodetype(char *updateparmnodename,
										char updateparmnodetype,
										MemoryContext spiContext,
										bool complain)
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
		ereport(complain ? ERROR : WARNING,
				(errmsg("SPI_execute failed: error code %d",
						spiRes)));
		return false;
	}
	return true;
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
