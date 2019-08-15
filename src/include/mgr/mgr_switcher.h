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
extern SwitcherNodeWrapper *choosePromotionNode(dlist_head *slaveNodes,
												bool allowFailed,
												dlist_head *failedSlaves);
extern bool tryConnectNode(SwitcherNodeWrapper *node, int connectTimeout);
extern bool tryLockCluster(dlist_head *coordinators, bool complain);
extern bool lockCoordinator(SwitcherNodeWrapper *coordinator, bool complain);
extern bool tryUnlockCluster(dlist_head *coordinators, bool complain);
extern bool unlockCoordinator(SwitcherNodeWrapper *coordinator, bool complain);
extern void checkGetSlaveNodes(SwitcherNodeWrapper *masterNode,
							   MemoryContext spiContext,
							   bool allowFailed,
							   dlist_head *failedSlaves,
							   dlist_head *runningSlaves);

extern void checkNodesRunningStatus(dlist_head *nodes,
									dlist_head *failedNodes,
									dlist_head *runningNodes);
extern void sortNodesByWalLsnDesc(dlist_head *nodes,
								  Oid deleteNodeOid);
extern void checkGetMasterCoordinators(MemoryContext spiContext,
									   dlist_head *coordinators);
extern void mgrNodesToSwitcherNodes(dlist_head *mgrNodes,
									dlist_head *switcherNodes);
#endif /* MGR_SWITCHER_H */
