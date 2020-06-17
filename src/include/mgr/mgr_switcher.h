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
	bool pgxcNodeChanged;
	bool holdClusterLock;
	bool adbSlotChanged;
	bool startupAfterException;
	bool inTransactionBlock;
	bool gtmInfoChanged;
} SwitcherNodeWrapper;

typedef struct ZoneOverGtm
{
	SwitcherNodeWrapper *oldMaster;
	SwitcherNodeWrapper *newMaster;
	SwitcherNodeWrapper *holdLockCoordinator;
	dlist_head 			coordinators;
	dlist_head 			coordinatorSlaves;
	dlist_head 			runningSlaves;
	dlist_head 			runningSlavesSecond;
	dlist_head 			failedSlaves;
	dlist_head 			failedSlavesSecond;
	dlist_head 			dataNodes;
}ZoneOverGtm;
 
typedef struct ZoneOverCoord
{
	SwitcherNodeWrapper *oldMaster;
	SwitcherNodeWrapper *newMaster;
	dlist_head 			runningSlaves;
	dlist_head 			failedSlaves;
}ZoneOverCoord;

typedef struct ZoneOverCoordWrapper
{
	ZoneOverCoord 		*zoCoord;	
	dlist_node 			link;
}ZoneOverCoordWrapper;

typedef struct ZoneOverDN
{
	SwitcherNodeWrapper *oldMaster;
	SwitcherNodeWrapper *newMaster;
	dlist_head 			runningSlaves;
	dlist_head 			runningSlavesSecond;
	dlist_head 			failedSlaves;
	dlist_head 			failedSlavesSecond;
}ZoneOverDN;

typedef struct ZoneOverDNWrapper
{
	ZoneOverDN 	    *zoDN;
	dlist_node 		link;
}ZoneOverDNWrapper;

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

static inline void pfreeSwitcherNodeWrapperListEx(dlist_head *nodes,
												SwitcherNodeWrapper *exclude, 
												SwitcherNodeWrapper *exclude2)
{
	dlist_mutable_iter miter;
	SwitcherNodeWrapper *node;

	dlist_foreach_modify(miter, nodes)
	{
		node = dlist_container(SwitcherNodeWrapper, link, miter.cur);
		dlist_delete(miter.cur);
		if (node != exclude && node != exclude2)
		{
			pfreeSwitcherNodeWrapper(node);
		}
	}
}
extern void FailOverDataNodeMaster(char *oldMasterName,
								 bool forceSwitch,
								 bool kickOutOldMaster,
								 Name newMasterName,
								 char *curZone);
void FailOverCoordMaster(char *oldMasterName,
						  bool forceSwitch,
						  bool kickOutOldMaster,
						  Name newMasterName,
						  char *curZone);								 
extern void FailOverGtmCoordMaster(char *oldMasterName,
								 bool forceSwitch,
								 bool kickOutOldMaster,
								 Name newMasterName,
								 char *curZone);
extern void switcherGtmCoordMasterFunc(MemoryContext spiContext,
										char *oldMasterName,
										bool forceSwitch,
										bool kickOutOldMaster,
										Name newMasterName,
										char* curZone,
										ErrorData **edata);								 
extern void switchoverDataNode(char *newMasterName, bool forceSwitch, char *curZone, int maxTrys);
extern void switchoverGtmCoord(char *newMasterName, bool forceSwitch, char *curZone, int maxTrys);
extern void switchoverCoord(char *newMasterName, bool forceSwitch, char *curZone);
extern void chooseNewMasterNode(SwitcherNodeWrapper *oldMaster,
								SwitcherNodeWrapper **newMasterP,
								dlist_head *runningSlaves,
								dlist_head *failedSlaves,
								MemoryContext spiContext,
								bool forceSwitch,
								char *newMasterName,
								char *curZone);
extern void PrintMgrNodeList(MemoryContext spiContext);
extern void chooseNewMasterNodeForZone(SwitcherNodeWrapper *oldMaster,
										SwitcherNodeWrapper **newMasterP,
										dlist_head *runningSlaves,
										bool forceSwitch,
										char *curZone);								
extern void tryLockCluster(dlist_head *coordinators);
extern bool tryUnlockCluster(dlist_head *coordinators, bool complain);
extern void mgrNodesToSwitcherNodes(dlist_head *mgrNodes,
									dlist_head *switcherNodes);
extern void switcherNodesToMgrNodes(dlist_head *switcherNodes,
									dlist_head *mgrNodes);
extern void appendSlaveNodeFollowMaster(MgrNodeWrapper *masterNode,
										MgrNodeWrapper *slaveNode,
										dlist_head *siblingSlaveNodes,
										PGconn *masterPGconn,
										MemoryContext spiContext);
extern void appendSlaveNodeFollowMasterEx(MemoryContext spiContext,
										SwitcherNodeWrapper *master,
										SwitcherNodeWrapper *slave,
										dlist_head *siblingSlaveNodes,
										bool complain);								
extern void checkGetMasterCoordinators(MemoryContext spiContext,
									   dlist_head *coordinators,
									   bool includeGtmCoord,
									   bool checkRunningMode);
extern void checkGetSlaveCoordinators(MemoryContext spiContext,
								dlist_head *coordinators,
								bool checkRunningMode);
extern void checkGetMasterDataNodes(MemoryContext spiContext,
								dlist_head *dataNodes,
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

extern void beginSwitcherNodeTransaction(SwitcherNodeWrapper *switcherNode,
										 bool complain);
extern void commitSwitcherNodeTransaction(SwitcherNodeWrapper *switcherNode,
										  bool complain);
extern void rollbackSwitcherNodeTransaction(SwitcherNodeWrapper *switcherNode,
											bool complain);
extern void MgrChildNodeFollowParentNode(MemoryContext spiContext, 
										Form_mgr_node childMgrNode, 
										Oid childNodeOid, 
										Form_mgr_node parentMgrNode, 
										Oid parentOid);

#endif /* MGR_SWITCHER_H */
