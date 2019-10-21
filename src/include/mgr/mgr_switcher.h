/*--------------------------------------------------------------------------
 *
 * Copyright (c) 2018-2019, Asiainfo Database Innovation Lab
 *
 * -------------------------------------------------------------------------
 */

#ifndef MGR_SWITCHER_H
#define MGR_SWITCHER_H

#include "lib/ilist.h"
#include "../../interfaces/libpq/libpq-fe.h"
#include "mgr/mgr_agent.h"
#include "mgr/mgr_helper.h"

#define CHECK_SYNC_STANDBY_NAMES_SECONDS 60
#define CHECK_GTM_INFO_SECONDS 60
#define SHUTDOWN_NODE_FAST_SECONDS 5
#define SHUTDOWN_NODE_IMMEDIATE_SECONDS 90
#define STARTUP_NODE_SECONDS 90

typedef struct SwitcherNodeWrapper
{
	MgrNodeWrapper *mgrNode;
	dlist_node link;
	PGconn *pgConn;
	XLogRecPtr walLsn;
	NodeRunningMode runningMode;
	NameData oldCurestatus;
	PGHbaItem *temporaryHbaItems;
	PGConfParameterItem *originalParameterItems;
	bool pgxcNodeChanged;
	bool holdClusterLock;
	bool adbSlotChanged;
	bool startupAfterException;
	bool inTransactionBlock;
	NameData pgxcNodeName;
	bool gtmInfoChanged;
} SwitcherNodeWrapper;

static inline void pfreeSwitcherNodeWrapperPGconn(SwitcherNodeWrapper *obj)
{
	if (obj && obj->pgConn)
	{
		PQfinish(obj->pgConn);
		obj->pgConn = NULL;
	}
}

static inline void pfreeSwitcherNodeWrapper(SwitcherNodeWrapper *obj)
{
	if (obj)
	{
		if (obj->mgrNode)
		{
			pfreeMgrNodeWrapper(obj->mgrNode);
			obj->mgrNode = NULL;
		}
		if (obj->temporaryHbaItems)
		{
			pfreePGHbaItem(obj->temporaryHbaItems);
			obj->temporaryHbaItems = NULL;
		}
		if (obj->originalParameterItems)
		{
			pfreePGConfParameterItem(obj->originalParameterItems);
			obj->originalParameterItems = NULL;
		}
		pfreeSwitcherNodeWrapperPGconn(obj);
		pfree(obj);
		obj = NULL;
	}
}

static inline void pfreeSwitcherNodeWrapperList(dlist_head *nodes,
												SwitcherNodeWrapper *exclude)
{
	dlist_mutable_iter miter;
	SwitcherNodeWrapper *node;

	dlist_foreach_modify(miter, nodes)
	{
		node = dlist_container(SwitcherNodeWrapper, link, miter.cur);
		dlist_delete(miter.cur);
		if (node != exclude)
		{
			pfreeSwitcherNodeWrapper(node);
		}
	}
}

extern void switchDataNodeMaster(char *oldMasterName,
								 bool forceSwitch,
								 bool kickOutOldMaster,
								 Name newMasterName);
extern void switchGtmCoordMaster(char *oldMasterName,
								 bool forceSwitch,
								 bool kickOutOldMaster,
								 Name newMasterName);
extern void switchoverDataNode(char *newMasterName, bool forceSwitch);
extern void switchoverGtmCoord(char *newMasterName, bool forceSwitch);
extern void chooseNewMasterNode(SwitcherNodeWrapper *oldMaster,
								SwitcherNodeWrapper **newMasterP,
								dlist_head *runningSlaves,
								dlist_head *failedSlaves,
								MemoryContext spiContext,
								bool forceSwitch);
extern void tryLockCluster(dlist_head *coordinators);
extern bool tryUnlockCluster(dlist_head *coordinators, bool complain);
extern void mgrNodesToSwitcherNodes(dlist_head *mgrNodes,
									dlist_head *switcherNodes);
extern void switcherNodesToMgrNodes(dlist_head *switcherNodes,
									dlist_head *mgrNodes);
extern void appendSlaveNodeFollowMaster(MgrNodeWrapper *masterNode,
										MgrNodeWrapper *slaveNode,
										PGconn *masterPGconn);
extern void checkGetMasterCoordinators(MemoryContext spiContext,
									   dlist_head *coordinators,
									   bool includeGtmCoord,
									   bool checkRunningMode);
extern SwitcherNodeWrapper *getHoldLockCoordinator(dlist_head *coordinators);
extern void updateCureStatusForSwitch(MgrNodeWrapper *mgrNode,
									  char *newCurestatus,
									  MemoryContext spiContext);
extern void checkGetSiblingMasterNodes(MemoryContext spiContext,
									   SwitcherNodeWrapper *masterNode,
									   dlist_head *siblingMasters);
extern void diffPgxcNodesOfDataNode(PGconn *pgconn,
									bool localExecute,
									SwitcherNodeWrapper *dataNodeMaster,
									dlist_head *siblingMasters,
									MemoryContext spiContext,
									bool complain);
extern void diffAdbSlotOfDataNodes(SwitcherNodeWrapper *coordinator,
								   SwitcherNodeWrapper *dataNodeMaster,
								   dlist_head *siblingMasters,
								   MemoryContext spiContext,
								   bool complain);
extern void beginSwitcherNodeTransaction(SwitcherNodeWrapper *switcherNode,
										 bool complain);
extern void commitSwitcherNodeTransaction(SwitcherNodeWrapper *switcherNode,
										  bool complain);
extern void rollbackSwitcherNodeTransaction(SwitcherNodeWrapper *switcherNode,
											bool complain);

#endif /* MGR_SWITCHER_H */
