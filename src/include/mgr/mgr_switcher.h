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
	bool coordinatorPgxcNodeChanged;
	bool coordinatorPaused;
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

extern void switchDatanodeMaster(char *oldMasterNodename,
								 bool forceSwitch,
								 bool kickOutOldMaster,
								 Name newMasterNodename);
extern void switchDataNodeOperation(SwitcherNodeWrapper *oldMaster,
									SwitcherNodeWrapper **newMasterP,
									dlist_head *runningSlaves,
									dlist_head *failedSlaves,
									dlist_head *coordinators,
									MemoryContext spiContext,
									bool forceSwitch,
									bool kickOutOldMaster);
extern void switchToDataNodeNewMaster(SwitcherNodeWrapper *oldMaster,
									  SwitcherNodeWrapper *newMaster,
									  dlist_head *runningSlaves,
									  dlist_head *failedSlaves,
									  dlist_head *coordinators,
									  MemoryContext spiContext,
									  bool kickOutOldMaster);
extern bool revertClusterSetting(dlist_head *coordinators,
								 SwitcherNodeWrapper *oldMaster,
								 SwitcherNodeWrapper *newMaster,
								 bool complain);
extern void precheckPromotionNode(dlist_head *runningSlaves,
								  bool forceSwitch);
extern SwitcherNodeWrapper *choosePromotionNode(dlist_head *runningSlaves,
												bool forceSwitch,
												dlist_head *failedSlaves);
extern bool tryLockCluster(dlist_head *coordinators, bool complain);
extern bool lockCoordinator(SwitcherNodeWrapper *coordinator, bool complain);
extern bool tryUnlockCluster(dlist_head *coordinators, bool complain);
extern bool unlockCoordinator(SwitcherNodeWrapper *coordinator, bool complain);
extern void checkGetSlaveNodesRunningStatus(SwitcherNodeWrapper *masterNode,
											MemoryContext spiContext,
											bool forceSwitch,
											dlist_head *failedSlaves,
											dlist_head *runningSlaves);
extern void sortNodesByWalLsnDesc(dlist_head *nodes);
extern void checkGetMasterCoordinators(MemoryContext spiContext,
									   dlist_head *coordinators);
extern void mgrNodesToSwitcherNodes(dlist_head *mgrNodes,
									dlist_head *switcherNodes);
extern void switcherNodesToMgrNodes(dlist_head *switcherNodes,
									dlist_head *mgrNodes);
extern void appendSlaveNodeFollowMaster(MgrNodeWrapper *masterNode,
										MgrNodeWrapper *slaveNode,
										PGconn *masterPGconn);
extern void refreshOldMasterBeforeSwitch(SwitcherNodeWrapper *oldMaster,
										 MemoryContext spiContext);

#endif /* MGR_SWITCHER_H */
