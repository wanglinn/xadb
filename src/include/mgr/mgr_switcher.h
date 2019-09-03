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
	bool coordPgxcNodeChanged;
	bool holdClusterLock;
	bool adbSlotChanged;
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
extern void checkSwitchDataNodePrerequisite(SwitcherNodeWrapper *oldMaster,
											dlist_head *runningSlaves,
											dlist_head *failedSlaves,
											dlist_head *coordinators,
											MemoryContext spiContext,
											bool forceSwitch);
extern void checkSwitchGtmCoordPrerequisite(SwitcherNodeWrapper *oldMaster,
											dlist_head *runningSlaves,
											dlist_head *failedSlaves,
											dlist_head *coordinators,
											dlist_head *runningDataNodes,
											dlist_head *failedDataNodes,
											MemoryContext spiContext,
											bool forceSwitch);
extern void switchToDataNodeNewMaster(SwitcherNodeWrapper *oldMaster,
									  SwitcherNodeWrapper *newMaster,
									  dlist_head *runningSlaves,
									  dlist_head *failedSlaves,
									  dlist_head *coordinators,
									  MemoryContext spiContext,
									  bool kickOutOldMaster);
extern void switchToGtmCoordNewMaster(SwitcherNodeWrapper *oldMaster,
									  SwitcherNodeWrapper *newMaster,
									  dlist_head *runningSlaves,
									  dlist_head *failedSlaves,
									  dlist_head *coordinators,
									  dlist_head *runningDataNodes,
									  MemoryContext spiContext,
									  bool kickOutOldMaster);
extern void chooseNewMasterNode(SwitcherNodeWrapper *oldMaster,
								SwitcherNodeWrapper **newMasterP,
								dlist_head *runningSlaves,
								dlist_head *failedSlaves,
								MemoryContext spiContext,
								bool forceSwitch);
extern void revertClusterSetting(dlist_head *coordinators,
								 SwitcherNodeWrapper *oldMaster,
								 SwitcherNodeWrapper *newMaster);
extern void tryLockCluster(dlist_head *coordinators);
extern bool tryUnlockCluster(dlist_head *coordinators, bool complain);
extern void mgrNodesToSwitcherNodes(dlist_head *mgrNodes,
									dlist_head *switcherNodes);
extern void switcherNodesToMgrNodes(dlist_head *switcherNodes,
									dlist_head *mgrNodes);
extern void appendSlaveNodeFollowMaster(MgrNodeWrapper *masterNode,
										MgrNodeWrapper *slaveNode,
										PGconn *masterPGconn);

#endif /* MGR_SWITCHER_H */
