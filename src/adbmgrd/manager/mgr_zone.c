/*
 * commands of zone
 */
#include <netdb.h>
#include <arpa/inet.h>
#include <unistd.h>
#include<pwd.h>

#include "postgres.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/mgr_host.h"
#include "catalog/pg_authid.h"
#include "catalog/mgr_node.h"
#include "catalog/mgr_updateparm.h"
#include "catalog/mgr_parm.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "fmgr.h"
#include "mgr/mgr_cmds.h"
#include "mgr/mgr_agent.h"
#include "mgr/mgr_msg_type.h"
#include "mgr/mgr_helper.h"
#include "mgr/mgr_switcher.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "parser/mgr_node.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/acl.h"
#include "utils/relcache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "funcapi.h"
#include "fmgr.h"
#include "utils/lsyscache.h"
#include "executor/spi.h"
#include "../../interfaces/libpq/libpq-fe.h"
#include "nodes/makefuncs.h"
#include "access/xlog.h"
#include "nodes/nodes.h"
#include "lib/ilist.h"

char *mgr_zone;

#define MGR_PGEXEC_DIRECT_EXE_UTI_RET_COMMAND_OK	0 


static void MgrFailoverCheck(MemoryContext spiContext, char *currentZone);
static void MgrSwitchoverCheck(MemoryContext spiContext, char *currentZone);
static void MgrCheckMasterHasSlave(MemoryContext spiContext, 
									char *currentZone, 
									char *overType);
static void MgrCheckMasterHasSlaveCnDn(MemoryContext spiContext, 
										char *currentZone, 
										char nodeType,
										char *overType);
static void MgrMakesureAllSlaveRunning(void);
static void MgrMakesureZoneAllSlaveRunning(char *zone);
static void SetZoneOverGtm(ZoneOverGtm *zoGtm);
static void MgrCheckAllSlaveNum(MemoryContext spiContext, 
								char *currentZone);
static void MgrCheckSlaveNum(MemoryContext spiContext, 
							char *currentZone,
							char masterType);
static void MgrGetNodeAndChildsInZone(MemoryContext spiContext,
										MgrNodeWrapper *mgrNode,
										char *zone,
										dlist_head *slaveNodes);
static bool MgrCheckHasSlaveZoneNode(MemoryContext spiContext, 
									char *zone);				
static void MgrShutdownNodesNotZone(MemoryContext spiContext,
									char *currentZone);		
static void MgrSetNodesNotZoneSwitched(MemoryContext spiContext,
										char *currentZone);	
static void MgrGetMasterZoneName(MemoryContext spiContext, 
								NameData *zoneName);											
static void MgrGetSlaveNodeZoneName(MemoryContext spiContext,
									NameData *masterZoneName, 
									NameData *zoneName);
static void MgrInitAllNodes(char *zone);
static void MgrAppendNode(MgrNodeWrapper  *node, 
							int *num, 
							int total);
static void MgrInitChildNodes(MemoryContext spiContext, 
								MgrNodeWrapper *mgrNode, 
								char *zone,
								int *num,
								int total);								
static int MgrGetNotActiveCount(MemoryContext spiContext, 
								char *zone);
static void MgrAddHbaToNodes(MemoryContext spiContext, char *zone);
static void MgrAddMgrHba(MemoryContext spiContext);
static void MgrAddGtmCoordHba(MemoryContext spiContext, char *zone);
static void MgrAddHbaToActiveNodes(MemoryContext spiContext, char *addIp);


Datum mgr_zone_failover(PG_FUNCTION_ARGS)
{
	HeapTuple 		tupResult = NULL;
	NameData 		name;
	char 			*currentZone;
	bool 			force = false;
	int 			maxTrys = 10;
	int 			spiRes = 0;	
	MemoryContext 	oldContext;
	MemoryContext 	spiContext = NULL;
	MemoryContext   switchContext = NULL;
	ZoneOverGtm 	zoGtm;
	ErrorData 		*edata = NULL;
	dlist_head 		zoCoordList = DLIST_STATIC_INIT(zoCoordList);
	dlist_head 		zoDNList = DLIST_STATIC_INIT(zoDNList);

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot do the command during recovery")));

	currentZone  = PG_GETARG_CSTRING(0);
	force 		 = PG_GETARG_INT32(1);
	Assert(currentZone);
	if (strcmp(currentZone, mgr_zone) != 0)
		ereport(ERROR, (errmsg("the given zone name \"%s\" is not same with guc parameter mgr_zone \"%s\" in postgresql.conf", currentZone, mgr_zone)));	

	namestrcpy(&name, "ZONE FAILOVER");
	SetZoneOverGtm(&zoGtm);

	oldContext = CurrentMemoryContext;
	switchContext = AllocSetContextCreate(CurrentMemoryContext, "mgr_zone_switchover", ALLOCSET_DEFAULT_SIZES);
	if ((spiRes = SPI_connect()) != SPI_OK_CONNECT){
		ereport(ERROR, (errmsg("SPI_connect failed, connect return:%d",	spiRes)));
	}
	spiContext = CurrentMemoryContext;
	MemoryContextSwitchTo(switchContext);

	PG_TRY();
	{
		ereportNoticeLog(errmsg("======== ZONE FAILOVER %s begin ========.", currentZone));
		MgrFailoverCheck(spiContext, currentZone);

		MgrAddHbaToNodes(spiContext, currentZone);

	 	PrintReplicationInfoOfMasterZone(spiContext, currentZone);
		
		MgrShutdownNodesNotZone(spiContext, currentZone);

		ereportNoticeLog(errmsg("======== ZONE FAILOVER %s, step1:failover gtmcoord slave in %s ========.", currentZone, currentZone));
		MgrZoneFailoverGtm(spiContext, 
							currentZone,
							force,
							maxTrys, 
							&zoGtm);

		ereportNoticeLog(errmsg("======== ZONE FAILOVER %s, step2:failover coordinator slave in %s ========.", currentZone, currentZone));
		MgrZoneFailoverCoord(spiContext,
							currentZone, 
							force, 
							maxTrys,
							&zoGtm, 
							&zoCoordList);

		ereportNoticeLog(errmsg("======== ZONE FAILOVER %s, step3:failover datanode slave in %s ========.", currentZone, currentZone));
		MgrZoneFailoverDN(spiContext, 
							currentZone,
							force,
							maxTrys,
							&zoGtm,
							&zoDNList);
		MgrRefreshAllPgxcNode(spiContext,
							&zoGtm,
							&zoCoordList, 
							&zoDNList);
		MgrSetNodesNotZoneSwitched(spiContext, currentZone);
		ereportNoticeLog(errmsg("======== ZONE FAILOVER %s end ========.", currentZone));
	}PG_CATCH();
	{
		ereportNoticeLog(errmsg("============ ZONE FAILOVER %s failed, revert it begin ============", currentZone));
		RevertZoneFailover(spiContext, 
							&zoGtm, 
							&zoCoordList,
							&zoDNList);
		if (zoGtm.holdLockCoordinator != NULL)					
			RefreshGtmAdbCheckSyncNextid(zoGtm.holdLockCoordinator->mgrNode, ADB_CHECK_SYNC_NEXTID_ON);					
		ZoneSwitchoverFree(&zoGtm, 
							&zoCoordList, 
							&zoDNList);
		ereportNoticeLog(errmsg("============ ZONE FAILOVER %s failed, revert it end ============", currentZone));
		
		(void)MemoryContextSwitchTo(oldContext);
		MemoryContextDelete(switchContext);
		SPI_finish();

		edata = CopyErrorData();
		FlushErrorState();
		if (edata)
			ReThrowError(edata);

		ereport(ERROR, (errmsg(" ZONE FAILOVER %s failed.", currentZone)));
	}PG_END_TRY();

	callAgentRestartNode(zoGtm.holdLockCoordinator->mgrNode, SHUTDOWN_F, true);

	if (zoGtm.holdLockCoordinator != NULL)
		RefreshGtmAdbCheckSyncNextid(zoGtm.holdLockCoordinator->mgrNode, ADB_CHECK_SYNC_NEXTID_ON);
	ZoneSwitchoverFree(&zoGtm, &zoCoordList, &zoDNList);

	MgrCheckAllSlaveNum(spiContext, currentZone);

	(void)MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(switchContext);
	SPI_finish();

	ereportNoticeLog(errmsg("the command of \"ZONE FAILOVER %s\" result is %s, description is %s", currentZone,"true", "success"));
	tupResult = build_common_command_tuple(&name, true, "success");
	return HeapTupleGetDatum(tupResult);
}

Datum mgr_zone_switchover(PG_FUNCTION_ARGS)
{
	HeapTuple 		tupResult = NULL;
	NameData 		name;
	char 			*currentZone;
	bool 			force = false;
	int 			maxTrys = 0;
	int 			spiRes = 0;	
	MemoryContext 	oldContext;
	MemoryContext 	spiContext = NULL;
	MemoryContext   switchContext = NULL;
	ZoneOverGtm 	zoGtm;
	dlist_head 		zoCoordList = DLIST_STATIC_INIT(zoCoordList);
	dlist_head 		zoDNList = DLIST_STATIC_INIT(zoDNList);
	ErrorData 		*edata = NULL;

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot do the command during recovery")));

	currentZone = PG_GETARG_CSTRING(0);
	force 		= PG_GETARG_INT32(1);
	maxTrys 	= PG_GETARG_INT32(2);
	
	Assert(currentZone);
	if (strcmp(currentZone, mgr_zone) != 0){
		ereport(ERROR, (errmsg("the given zone name \"%s\" is not same with guc parameter mgr_zone \"%s\" in postgresql.conf", currentZone, mgr_zone)));
	}
	
	if (maxTrys < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("the value of maxTrys must be positive")));
	if (maxTrys < 10)
		maxTrys = 10;			 
		
	namestrcpy(&name, "ZONE SWITCHOVER");
	
	SetZoneOverGtm(&zoGtm);

	oldContext = CurrentMemoryContext;
	switchContext = AllocSetContextCreate(CurrentMemoryContext, "mgr_zone_switchover", ALLOCSET_DEFAULT_SIZES);
	if ((spiRes = SPI_connect()) != SPI_OK_CONNECT){
		ereport(ERROR, (errmsg("SPI_connect failed, connect return:%d",	spiRes)));
	}
	spiContext = CurrentMemoryContext;
	MemoryContextSwitchTo(switchContext);
	
	PG_TRY();
	{
		ereportNoticeLog(errmsg("======== ZONE SWITCHOVER %s begin ========.", currentZone));
		MgrSwitchoverCheck(spiContext, currentZone);

		MgrAddHbaToNodes(spiContext, currentZone);

		ereportNoticeLog(errmsg("============ ZONE SWITCHOVER %s, step1:switchover gtmcoord slave in %s ============", currentZone, currentZone));
		MgrZoneSwitchoverGtm(spiContext, 
							currentZone,
							force,
							maxTrys, 
							&zoGtm);

		ereportNoticeLog(errmsg("============ ZONE SWITCHOVER %s, step2:switchover coordinator slave in %s ============", currentZone, currentZone));
		MgrZoneSwitchoverCoord(spiContext, 
								currentZone, 
								force,
								maxTrys,
								&zoGtm, 
								&zoCoordList);

		ereportNoticeLog(errmsg("============ ZONE SWITCHOVER %s, step3:switchover datanode slave in %s ============", currentZone, currentZone));
		MgrZoneSwitchoverDataNode(spiContext, 
									currentZone, 
									force,
									maxTrys,
									&zoGtm, 
									&zoDNList);
		refreshAsyncToSync(spiContext, &zoGtm.runningSlaveOfNewMaster);
		ereportNoticeLog(errmsg("======== ZONE SWITCHOVER %s end ========.", currentZone));							
	}PG_CATCH();
	{
		ereportNoticeLog(errmsg("============ ZONE SWITCHOVER %s failed, revert it begin ============", currentZone));
		RevertZoneSwitchover(spiContext, &zoGtm, &zoCoordList, &zoDNList);

		if (zoGtm.holdLockCoordinator != NULL)	
			RefreshGtmAdbCheckSyncNextid(zoGtm.holdLockCoordinator->mgrNode, ADB_CHECK_SYNC_NEXTID_ON);
		ZoneSwitchoverFree(&zoGtm, &zoCoordList, &zoDNList);
		ereportNoticeLog(errmsg("============ ZONE SWITCHOVER %s failed, revert it end ============", currentZone));

		(void)MemoryContextSwitchTo(oldContext);
		MemoryContextDelete(switchContext);
		SPI_finish();

		edata = CopyErrorData();
		FlushErrorState();
		if (edata)
			ReThrowError(edata);

		ereport(ERROR, (errmsg(" ZONE SWITCHOVER %s failed.", currentZone)));
	}PG_END_TRY();

	tryUnlockCluster(&zoGtm.coordinators, true);

	if (zoGtm.holdLockCoordinator != NULL)	
		RefreshGtmAdbCheckSyncNextid(zoGtm.holdLockCoordinator->mgrNode, ADB_CHECK_SYNC_NEXTID_ON);
	
	ZoneSwitchoverFree(&zoGtm, &zoCoordList, &zoDNList);

	MgrCheckAllSlaveNum(spiContext, currentZone);

	(void)MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(switchContext);
	SPI_finish();

	ereportNoticeLog(errmsg("the command of \"ZONE SWITCHOVER %s\" result is %s, description is %s", currentZone,"true", "success"));
	tupResult = build_common_command_tuple(&name, true, "success");
	return HeapTupleGetDatum(tupResult);
}
Datum mgr_zone_clear(PG_FUNCTION_ARGS)
{
	int 				spiRes;
	char 				*zone = NULL;
	dlist_iter 			iter;
	MgrNodeWrapper 		*mgrNode = NULL;
	MemoryContext 		oldContext;
	MemoryContext 		spiContext = NULL;
	MemoryContext   	switchContext = NULL;
	HeapTuple 			tupResult = NULL;
	NameData 			name;
	dlist_head 			mgrNodes = DLIST_STATIC_INIT(mgrNodes);
	dlist_head 			mgrNodesNew = DLIST_STATIC_INIT(mgrNodesNew);

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot do the command during recovery")));

	zone  = PG_GETARG_CSTRING(0);
	Assert(zone);
	namestrcpy(&name, "DROP ZONE");

	oldContext = CurrentMemoryContext;
	switchContext = AllocSetContextCreate(CurrentMemoryContext, "mgr_zone_clear", ALLOCSET_DEFAULT_SIZES);
	if ((spiRes = SPI_connect()) != SPI_OK_CONNECT){
		ereport(ERROR, (errmsg("SPI_connect failed, connect return:%d",	spiRes)));
	}
	spiContext = CurrentMemoryContext;
	MemoryContextSwitchTo(switchContext);

	PG_TRY();
	{
		if(MgrCheckHasSlaveZoneNode(spiContext, zone)){
			ereport(ERROR, (errmsg("zone %s has slave node in other zone, so can't drop zone %s.",	zone, zone)));
		}
		dlist_init(&mgrNodes);
		selectChildNodes(spiContext,
							0,
							&mgrNodes);
		dlist_foreach(iter, &mgrNodes)
		{
			mgrNode = dlist_container(MgrNodeWrapper, link, iter.cur);
			Assert(mgrNode);
			MgrGetNodeAndChildsInZone(spiContext,
									mgrNode,
									zone,
									&mgrNodesNew);		
		}
		mgr_drop_all_nodes(&mgrNodesNew);
	}PG_CATCH();
	{
		(void)MemoryContextSwitchTo(oldContext);
		MemoryContextDelete(switchContext);
		SPI_finish();
		PG_RE_THROW();
	}PG_END_TRY();

	(void)MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(switchContext);
	SPI_finish();

	ereportNoticeLog(errmsg("DROP ZONE %s success", zone));
	tupResult = build_common_command_tuple(&name, true, "success");
	return HeapTupleGetDatum(tupResult);
}
Datum mgr_zone_init(PG_FUNCTION_ARGS)
{
	char 			*currentZone= NULL;
	NameData 		name;
	HeapTuple 		tupResult = NULL;

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot do the command during recovery")));

	namestrcpy(&name, "zone init");
	currentZone = PG_GETARG_CSTRING(0);
	
	PG_TRY();
	{
		CheckZoneNodesBeforeInitAll();
		mgr_check_rewind_dir_exist(currentZone);

		MgrInitAllNodes(currentZone);
	}PG_CATCH();
	{
		PG_RE_THROW();
	}PG_END_TRY();

	tupResult = build_common_command_tuple(&name, true, "success");
	return HeapTupleGetDatum(tupResult);
}

static void MgrCheckMasterHasSlaveCnDn(MemoryContext spiContext, 
										char *currentZone, 
										char nodeType,
										char *overType)
{
	dlist_head 			masterList = DLIST_STATIC_INIT(masterList);
	dlist_head 			slaveList  = DLIST_STATIC_INIT(slaveList);
	dlist_iter 			iter;
	MgrNodeWrapper      *mgrNode = NULL;
	Assert(spiContext);
	Assert(currentZone);

	PG_TRY();
	{
		MgrGetOldDnMasterNotZone(spiContext, 
								currentZone, 
								nodeType, 
								&masterList,
								overType);
		dlist_foreach(iter, &masterList)
		{
			mgrNode = dlist_container(MgrNodeWrapper, link, iter.cur);
			dlist_init(&slaveList);
			selectActiveMgrSlaveNodesInZone(mgrNode->form.oid, getMgrSlaveNodetype(mgrNode->form.nodetype), currentZone, spiContext, &slaveList);
			if (dlist_is_empty(&slaveList)){
				ereport(ERROR, (errmsg("because %s node(%s) has no slave node in zone(%s), please add slave node first.", 
					NameStr(mgrNode->form.nodename), mgr_get_nodetype_desc(mgrNode->form.nodetype), currentZone)));
			}
			pfreeMgrNodeWrapperList(&slaveList, NULL);
		}
	}PG_CATCH();
	{
		pfreeMgrNodeWrapperList(&masterList, NULL);
		pfreeMgrNodeWrapperList(&slaveList, NULL);
		PG_RE_THROW();
	}PG_END_TRY();
	pfreeMgrNodeWrapperList(&masterList, NULL);
}
static void MgrCheckMasterHasSlave(MemoryContext spiContext, 
									char *currentZone, 
									char *overType)
{
	MgrNodeWrapper 	*oldMaster = NULL;
	dlist_head 		activeNodes = DLIST_STATIC_INIT(activeNodes);
	Assert(spiContext);
	Assert(currentZone);

	oldMaster = MgrGetOldGtmMasterNotZone(spiContext, currentZone, overType);
	Assert(oldMaster);
	selectActiveMgrSlaveNodesInZone(oldMaster->form.oid, getMgrSlaveNodetype(oldMaster->form.nodetype), currentZone, spiContext, &activeNodes);
	if (dlist_is_empty(&activeNodes)){
		ereport(ERROR, (errmsg("because no gtmcoord slave in zone(%s),  please add slave node first.", currentZone)));
	}

	MgrCheckMasterHasSlaveCnDn(spiContext, currentZone, CNDN_TYPE_COORDINATOR_MASTER, overType);
	MgrCheckMasterHasSlaveCnDn(spiContext, currentZone, CNDN_TYPE_DATANODE_MASTER, overType);	

    pfreeMgrNodeWrapperList(&activeNodes, NULL);
}
static void MgrFailoverCheck(MemoryContext spiContext, char *currentZone)
{
	MgrCheckMasterHasSlave(spiContext, currentZone, OVERTYPE_FAILOVER);
	MgrMakesureZoneAllSlaveRunning(currentZone);
}
static void MgrSwitchoverCheck(MemoryContext spiContext, char *currentZone)
{
	MgrCheckMasterHasSlave(spiContext, currentZone, OVERTYPE_SWITCHOVER);
	MgrMakesureAllSlaveRunning();
}
static void MgrShutdownNodeNotZone(MemoryContext spiContext,
									char *currentZone,
									char nodeType)
{
	dlist_head 			nodeList = DLIST_STATIC_INIT(nodeList);
	dlist_iter 			iter;
	MgrNodeWrapper 		*mgrNode = NULL;

	selectNodeNotZoneForFailover(spiContext, currentZone, nodeType, &nodeList);
	dlist_foreach(iter, &nodeList)
	{
		mgrNode = dlist_container(MgrNodeWrapper, link, iter.cur);
		Assert(mgrNode);
		if(mgr_check_agent_running(mgrNode->form.nodehost))
		{
			shutdownNodeWithinSeconds(mgrNode,
									SHUTDOWN_NODE_FAST_SECONDS,
									SHUTDOWN_NODE_IMMEDIATE_SECONDS,
									false);

			mgrNode->form.nodeinited    = false;
			mgrNode->form.nodeincluster = false;
			updateMgrNodeAfterSwitch(mgrNode, CURE_STATUS_SWITCHING, spiContext);
			ereport(LOG, (errmsg("failover node(%s) is set to not inited, not incluster. nodezone(%s)", 
				NameStr(mgrNode->form.nodename), NameStr(mgrNode->form.nodezone))));
		}							
	}
		
	pfreeMgrNodeWrapperList(&nodeList, NULL);
}
static void MgrShutdownNodesNotZone(MemoryContext spiContext,
									char *currentZone)
{	
	MgrShutdownNodeNotZone(spiContext, currentZone, CNDN_TYPE_GTM_COOR_MASTER);
	MgrShutdownNodeNotZone(spiContext, currentZone, CNDN_TYPE_GTM_COOR_SLAVE);
	MgrShutdownNodeNotZone(spiContext, currentZone, CNDN_TYPE_COORDINATOR_MASTER);
	MgrShutdownNodeNotZone(spiContext, currentZone, CNDN_TYPE_COORDINATOR_SLAVE);
	MgrShutdownNodeNotZone(spiContext, currentZone, CNDN_TYPE_DATANODE_MASTER);
	MgrShutdownNodeNotZone(spiContext, currentZone, CNDN_TYPE_DATANODE_SLAVE);
}
static void MgrSetNodeNotZoneSwitched(MemoryContext spiContext,
									char *currentZone,
									char nodeType)
{
	dlist_head 			nodeList = DLIST_STATIC_INIT(nodeList);
	dlist_iter 			iter;
	MgrNodeWrapper 		*mgrNode = NULL;

	selectNodeNotZoneForFailover(spiContext, currentZone, nodeType, &nodeList);
	dlist_foreach(iter, &nodeList)
	{
		mgrNode = dlist_container(MgrNodeWrapper, link, iter.cur);
		Assert(mgrNode);
		updateMgrNodeAfterSwitch(mgrNode, CURE_STATUS_SWITCHED, spiContext);
	}
		
	pfreeMgrNodeWrapperList(&nodeList, NULL);
}
static void MgrSetNodesNotZoneSwitched(MemoryContext spiContext,
										char *currentZone)
{	
	MgrSetNodeNotZoneSwitched(spiContext, currentZone, CNDN_TYPE_GTM_COOR_MASTER);
	MgrSetNodeNotZoneSwitched(spiContext, currentZone, CNDN_TYPE_GTM_COOR_SLAVE);
	MgrSetNodeNotZoneSwitched(spiContext, currentZone, CNDN_TYPE_COORDINATOR_MASTER);
	MgrSetNodeNotZoneSwitched(spiContext, currentZone, CNDN_TYPE_COORDINATOR_SLAVE);
	MgrSetNodeNotZoneSwitched(spiContext, currentZone, CNDN_TYPE_DATANODE_MASTER);
	MgrSetNodeNotZoneSwitched(spiContext, currentZone, CNDN_TYPE_DATANODE_SLAVE);
}
MgrNodeWrapper *MgrGetOldGtmMasterNotZone(MemoryContext spiContext,
											char *currentZone, 
											char *overType)
{
	dlist_head 			masterList = DLIST_STATIC_INIT(masterList);
	dlist_iter 			iter;
	MgrNodeWrapper 		*oldMaster = NULL;
	int                 gtmMasterNum = 0;
	dlist_node 			*node = NULL; 

	if (pg_strcasecmp(overType, OVERTYPE_SWITCHOVER) == 0){
		selectNodeNotZone(spiContext, currentZone, CNDN_TYPE_GTM_COOR_MASTER, &masterList);
	}
	else{
		selectNodeNotZoneForFailover(spiContext, currentZone, CNDN_TYPE_GTM_COOR_MASTER, &masterList);
	} 

	if (dlist_is_empty(&masterList)){
		ereport(ERROR, (errmsg("current zone is %s, because no gtmcoord master in other zone, so can't switchover or failover.", currentZone)));
	}

	dlist_foreach(iter, &masterList)
	{
		oldMaster = dlist_container(MgrNodeWrapper, link, iter.cur);
		Assert(oldMaster);
		gtmMasterNum++;	
	}	
	if (gtmMasterNum != 1){
		ereport(ERROR, (errmsg("because gtmcoord master number(%d) is not be equal to 1 in not zone(%s), so can't switchover or failover.", 
				gtmMasterNum, currentZone)));
	}

	node = dlist_tail_node(&masterList);
	oldMaster = dlist_container(MgrNodeWrapper, link, node);
	return oldMaster;
}
/*
* mgr_checknode_in_currentzone
* 
* check given tuple oid, if tuple is in the current zone return true, else return false;
*
*/
bool mgr_checknode_in_currentzone(const char *zone, const Oid TupleOid)
{
	Relation relNode;
	HeapTuple tuple;
	TableScanDesc relScan;
	ScanKeyData key[1];
	bool res = false;

	Assert(zone);
	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodezone
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,CStringGetDatum(zone));

	relNode = table_open(NodeRelationId, AccessShareLock);
	relScan = table_beginscan_catalog(relNode, 1, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		if (TupleOid == ((Form_mgr_node)GETSTRUCT(tuple))->oid)
		{
			res = true;
			break;
		}

	}
	table_endscan(relScan);
	table_close(relNode, AccessShareLock);

	return res;
}
/* mgr_node_has_slave_inzone
* 
* check the oid has been used by slave in given zone
*/
bool mgr_node_has_slave_inzone(Relation rel, char *zone, Oid mastertupleoid)
{
	ScanKeyData key[2];
	HeapTuple tuple;
	TableScanDesc scan;

	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodemasternameoid
		,BTEqualStrategyNumber
		,F_OIDEQ
		,ObjectIdGetDatum(mastertupleoid));
	ScanKeyInit(&key[1]
		,Anum_mgr_node_nodezone
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,CStringGetDatum(zone));
	scan = table_beginscan_catalog(rel, 2, key);
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		table_endscan(scan);
		return true;
	}
	table_endscan(scan);
	return false;
}
static void MgrMakesureAllSlaveRunning(void)
{
	mgr_make_sure_all_running(CNDN_TYPE_GTM_COOR_MASTER, NULL);
	mgr_make_sure_all_running(CNDN_TYPE_COORDINATOR_MASTER, NULL);
	mgr_make_sure_all_running(CNDN_TYPE_DATANODE_MASTER, NULL);
	mgr_make_sure_all_running(CNDN_TYPE_GTM_COOR_SLAVE, NULL);
	mgr_make_sure_all_running(CNDN_TYPE_COORDINATOR_SLAVE, NULL);
	mgr_make_sure_all_running(CNDN_TYPE_DATANODE_SLAVE, NULL);
}
static void MgrMakesureZoneAllSlaveRunning(char *zone)
{
	mgr_make_sure_all_running(CNDN_TYPE_GTM_COOR_SLAVE, zone);
	mgr_make_sure_all_running(CNDN_TYPE_COORDINATOR_SLAVE, zone);
	mgr_make_sure_all_running(CNDN_TYPE_DATANODE_SLAVE, zone);
}
static void SetZoneOverGtm(ZoneOverGtm *zoGtm)
{
	zoGtm->oldMaster          = NULL;
	zoGtm->newMaster          = NULL;	
	zoGtm->holdLockCoordinator= NULL;
	dlist_init(&zoGtm->coordinators);
	dlist_init(&zoGtm->coordinatorSlaves);
	dlist_init(&zoGtm->runningSlaves);
	dlist_init(&zoGtm->runningSlavesSecond);
	dlist_init(&zoGtm->failedSlaves);
	dlist_init(&zoGtm->failedSlavesSecond);
	dlist_init(&zoGtm->dataNodes);	
	dlist_init(&zoGtm->runningSlaveOfNewMaster);	
}
static void MgrCheckAllSlaveNum(MemoryContext spiContext, 
								char *currentZone)
{
	MgrCheckSlaveNum(spiContext, currentZone, CNDN_TYPE_GTM_COOR_MASTER);
	MgrCheckSlaveNum(spiContext, currentZone, CNDN_TYPE_DATANODE_MASTER);
}
static void MgrCheckSlaveNum(MemoryContext spiContext, 
							char *currentZone,
							char masterType)
{
	dlist_iter 		iter;
	char 			slaveType;
	MgrNodeWrapper  *mgrNode;
	dlist_head 		masterMgrNodes = DLIST_STATIC_INIT(masterMgrNodes);

	selectActiveMgrNodeByNodetype(spiContext,
									masterType,
									&masterMgrNodes);	
	dlist_foreach(iter, &masterMgrNodes)
	{
		mgrNode = dlist_container(MgrNodeWrapper, link, iter.cur);
		Assert(mgrNode);
		slaveType = getMgrSlaveNodetype(mgrNode->form.nodetype);
		if (GetSlaveNodeNumInZone(spiContext, mgrNode, slaveType, currentZone) == 0){
			ereport(WARNING, (errmsg("%s %s has no %s node in %s, it not highly available, please append slave node for the node.", 
				mgr_get_nodetype_desc(mgrNode->form.nodetype), NameStr(mgrNode->form.nodename), 
				mgr_get_nodetype_desc(slaveType), currentZone)));
		}
	}

	pfreeMgrNodeWrapperList(&masterMgrNodes, NULL);
}
static void MgrGetNodeAndChildsInZone(MemoryContext spiContext,
									MgrNodeWrapper *mgrNode,
									char *zone,
									dlist_head *slaveNodes)
{
	dlist_head 			mgrNodes = DLIST_STATIC_INIT(mgrNodes);
	dlist_iter 			iter;
	MgrNodeWrapper 		*slaveNode = NULL;
	MgrNodeWrapper 		*mgrNodeTmp = NULL;

	Assert(mgrNode);

    if (pg_strcasecmp(NameStr(mgrNode->form.nodezone), zone) == 0){
		mgrNodeTmp = (MgrNodeWrapper *)palloc0(sizeof(MgrNodeWrapper));
		memcpy(mgrNodeTmp, mgrNode, sizeof(MgrNodeWrapper));
		dlist_push_head(slaveNodes, &mgrNodeTmp->link);
	}

	dlist_init(&mgrNodes);
	selectChildNodesInZone(spiContext,
						   mgrNode->form.oid,
						   zone,
						   &mgrNodes);
	dlist_foreach(iter, &mgrNodes)
	{
		slaveNode = dlist_container(MgrNodeWrapper, link, iter.cur);
		Assert(slaveNode);
		MgrGetNodeAndChildsInZone(spiContext,
								  slaveNode,
								  zone,
								  slaveNodes);
	}
}
static bool MgrCheckHasSlaveZoneNode(MemoryContext spiContext, char *zone)
{
	dlist_head 			mgrNodes = DLIST_STATIC_INIT(mgrNodes);
	dlist_head 			childNodes = DLIST_STATIC_INIT(childNodes);
	MgrNodeWrapper 		*mgrNode = NULL;
	MgrNodeWrapper 		*childNode = NULL;
	dlist_iter 			iter;
	dlist_iter 			childIter;
	
    selectAllNodesInZone(spiContext, zone, &mgrNodes);
	dlist_foreach(iter, &mgrNodes)
	{
		mgrNode = dlist_container(MgrNodeWrapper, link, iter.cur);
		Assert(mgrNode);
		selectChildNodes(spiContext,
                        mgrNode->form.oid,
						&childNodes);
		dlist_foreach(childIter, &childNodes)
		{
			childNode = dlist_container(MgrNodeWrapper, link, childIter.cur);
			Assert(childNode);
			if (pg_strcasecmp(NameStr(childNode->form.nodezone), NameStr(mgrNode->form.nodezone)) != 0){
				ereport(LOG, (errmsg("node(%s) zone(%s) is not equal to node(%s) zone(%s).",
				    NameStr(childNode->form.nodename), 	NameStr(childNode->form.nodezone),
					NameStr(mgrNode->form.nodename), NameStr(mgrNode->form.nodezone))));
				return true;	
			}
		}
	}
	return false;
}
void CheckZoneNodesBeforeInitAll(void)
{
	MemoryContext 	oldContext;
	MemoryContext 	spiContext = NULL;
	MemoryContext   switchContext = NULL;
	NameData 		slaveZone = {{0}};
	NameData 		masterZone = {{0}};
	dlist_head 	    masterList = DLIST_STATIC_INIT(masterList);
	dlist_head 	    slaveList = DLIST_STATIC_INIT(slaveList);
	MgrNodeWrapper  *masterNode = NULL;
	dlist_iter		masterIter;
	int 			spiRes;

	oldContext = CurrentMemoryContext;
	switchContext = AllocSetContextCreate(CurrentMemoryContext, "mgr_zone_init", ALLOCSET_DEFAULT_SIZES);
	if ((spiRes = SPI_connect()) != SPI_OK_CONNECT){
		ereport(ERROR, (errmsg("SPI_connect failed, connect return:%d",	spiRes)));
	}
	spiContext = CurrentMemoryContext;
	MemoryContextSwitchTo(switchContext);
	PG_TRY();
	{
		MgrGetMasterZoneName(spiContext, &masterZone);
		MgrGetSlaveNodeZoneName(spiContext, &masterZone, &slaveZone);
		if (strlen(NameStr(slaveZone)) > 0)
		{
			selectChildNodesInZone(spiContext, 0, NameStr(masterZone), &masterList);
			dlist_foreach(masterIter, &masterList)
			{
				masterNode = dlist_container(MgrNodeWrapper, link, masterIter.cur);
				Assert(masterNode);
				dlist_init(&slaveList);
				selectChildNodesInZone(spiContext, masterNode->form.oid, NameStr(slaveZone), &slaveList);
				if (dlist_is_empty(&slaveList)){
					ereport(ERROR, (errmsg("%s %s has no slave node in zone %s, please add slave node first.",	
						 mgr_get_nodetype_desc(masterNode->form.nodetype), NameStr(masterNode->form.nodename), NameStr(slaveZone))));
				}
			}
		}
	}PG_CATCH();
	{
		(void)MemoryContextSwitchTo(oldContext);
		MemoryContextDelete(switchContext);
		SPI_finish();
		PG_RE_THROW();
	}PG_END_TRY();

	(void)MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(switchContext);
	SPI_finish();
}
static bool MgrGetSlaveNodeZoneNameByNodeType(MemoryContext spiContext, 
											char 	nodeType, 
											NameData *masterZoneName,
											NameData *zoneName)
{
	dlist_head 			resultList = DLIST_STATIC_INIT(resultList);
	dlist_node 			*node = NULL; 
	MgrNodeWrapper 		*mgrNode = NULL;

	selectNodeNotZoneForFailover(spiContext, 
								NameStr(*masterZoneName),
								nodeType, 
								&resultList);
	if (!dlist_is_empty(&resultList))
	{
		node = dlist_tail_node(&resultList);
		mgrNode = dlist_container(MgrNodeWrapper, link, node);
		namestrcpy(zoneName, NameStr(mgrNode->form.nodezone));
		return true;
	}
	return false;
}
static void MgrGetMasterZoneName(MemoryContext spiContext, 
								NameData *zoneName)
{
	dlist_head 			resultList = DLIST_STATIC_INIT(resultList);
	dlist_node 			*node = NULL; 
	MgrNodeWrapper 		*mgrNode = NULL;

	selectMgrNodeByNodetypeEx(spiContext, CNDN_TYPE_GTM_COOR_MASTER, &resultList);
	if (!dlist_is_empty(&resultList))
	{
		node = dlist_tail_node(&resultList);
		mgrNode = dlist_container(MgrNodeWrapper, link, node);
		namestrcpy(zoneName, NameStr(mgrNode->form.nodezone));
	}
}
static void MgrGetSlaveNodeZoneName(MemoryContext spiContext, NameData *masterZoneName, NameData *zoneName)
{
	if (MgrGetSlaveNodeZoneNameByNodeType(spiContext, CNDN_TYPE_GTM_COOR_SLAVE, masterZoneName, zoneName)){
		return;
	}

	if (MgrGetSlaveNodeZoneNameByNodeType(spiContext, CNDN_TYPE_COORDINATOR_SLAVE, masterZoneName, zoneName)){
		return;
	}

	if (MgrGetSlaveNodeZoneNameByNodeType(spiContext, CNDN_TYPE_DATANODE_SLAVE, masterZoneName, zoneName)){
		return;
	}
}
static void MgrInitAllNodes(char *zone)
{
	MemoryContext 	oldContext;
	MemoryContext 	spiContext = NULL;
	MemoryContext   switchContext = NULL;
	dlist_head 	    masterList = DLIST_STATIC_INIT(masterList);
	dlist_head 	    slaveList = DLIST_STATIC_INIT(slaveList);
	MgrNodeWrapper  *masterNode = NULL;
	dlist_iter		masterIter;
	int 			spiRes;
	int             num = 1;
	int             total = 0;

	oldContext = CurrentMemoryContext;
	switchContext = AllocSetContextCreate(CurrentMemoryContext, "MgrInitAllNodes", ALLOCSET_DEFAULT_SIZES);
	if ((spiRes = SPI_connect()) != SPI_OK_CONNECT){
		ereport(ERROR, (errmsg("SPI_connect failed, connect return:%d",	spiRes)));
	}
	spiContext = CurrentMemoryContext;
	MemoryContextSwitchTo(switchContext);

	PG_TRY();
	{
		total = MgrGetNotActiveCount(spiContext, zone);

		selectChildNodes(spiContext, 0, &masterList);
		dlist_foreach(masterIter, &masterList)
		{
			masterNode = dlist_container(MgrNodeWrapper, link, masterIter.cur);
			Assert(masterNode);
			MgrInitChildNodes(spiContext,
							  masterNode, 
							  zone,
							  &num,
							  total);
		}
	}PG_CATCH();
	{
		(void)MemoryContextSwitchTo(oldContext);
		MemoryContextDelete(switchContext);
		SPI_finish();
		PG_RE_THROW();
	}PG_END_TRY();

	(void)MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(switchContext);
	SPI_finish();
}
static void MgrInitChildNodes(MemoryContext spiContext, 
								MgrNodeWrapper *mgrNode, 
								char *zone,
								int *num,
								int total)
{
	dlist_head 			slaveNodes = DLIST_STATIC_INIT(slaveNodes);
	dlist_iter 			iter;
	MgrNodeWrapper 		*slaveNode = NULL;

	dlist_init(&slaveNodes);
	selectChildNodesInZone(spiContext,
							mgrNode->form.oid,
							zone,
							&slaveNodes);
	dlist_foreach(iter, &slaveNodes)
	{
		slaveNode = dlist_container(MgrNodeWrapper, link, iter.cur);
		Assert(slaveNode);
		if (!slaveNode->form.nodeincluster){
			MgrAppendNode(slaveNode, num, total);
		}
		MgrInitChildNodes(spiContext, 
						  slaveNode, 
						  zone,
						  num,
						  total);
	}
}
static void MgrAppendNode(MgrNodeWrapper  *node, 
							int *num, 
							int total)
{
	char            *coordMaster = NULL;
	Form_mgr_node   mgrNode = NULL;
	StringInfoData 	strerr;

	Assert(node);
	mgrNode = &(node->form);
	Assert(mgrNode);
	
	if (mgrNode->nodetype == CNDN_TYPE_GTM_COOR_SLAVE)
	{
		if (mgr_append_agtm_slave_func(NameStr(mgrNode->nodename), false)){
			ereportNoticeLog(errmsg("append gtmcoord slave %s success, progress is %d/%d.", NameStr(mgrNode->nodename), *num, total));		
		}
		else{
			ereportWarningLog(errmsg("append gtmcoord slave %s failed, progress is %d/%d.", NameStr(mgrNode->nodename), *num, total));		
		}
	}
	else if (mgrNode->nodetype == CNDN_TYPE_COORDINATOR_SLAVE)
	{
		initStringInfo(&strerr);
		coordMaster = mgr_get_mastername_by_nodename_type(NameStr(mgrNode->nodename), CNDN_TYPE_COORDINATOR_SLAVE);
		if (mgr_append_coord_slave_func(coordMaster, NameStr(mgrNode->nodename), &strerr)){
			ereportNoticeLog(errmsg("append coordinator slave %s success, progress is %d/%d.", NameStr(mgrNode->nodename), *num, total));		
		}
		else{
			ereportWarningLog(errmsg("append coordinator slave %s failed, progress is %d/%d.", NameStr(mgrNode->nodename), *num, total));
		}
		MgrFree(coordMaster);
	}
	else if (mgrNode->nodetype == CNDN_TYPE_DATANODE_SLAVE)
	{
		if (mgr_append_dn_slave_func(NameStr(mgrNode->nodename), false)){
			ereportNoticeLog(errmsg("append datanode slave %s success, progress is %d/%d.", NameStr(mgrNode->nodename), *num, total));		
		}
		else{
			ereportWarningLog(errmsg("append datanode slave %s failed, progress is %d/%d.", NameStr(mgrNode->nodename), *num, total));
		}
	}
	(*num)++;
}
static int MgrGetNotActiveCount(MemoryContext spiContext, 
								char *zone)
{
	dlist_head 			slaveNodes = DLIST_STATIC_INIT(slaveNodes);
	dlist_iter 			iter;
	MgrNodeWrapper 		*slaveNode = NULL;
	int                 count = 0;

	selectNotActiveChildInZone(spiContext, 
								zone, 
								&slaveNodes);
	dlist_foreach(iter, &slaveNodes)
	{
		slaveNode = dlist_container(MgrNodeWrapper, link, iter.cur);
		Assert(slaveNode);
		count++;
	}
	return count;
}
static void MgrAddHbaToNodes(MemoryContext spiContext, char *zone)
{
	MgrAddMgrHba(spiContext);
	MgrAddGtmCoordHba(spiContext, zone);									
}
static void MgrAddMgrHba(MemoryContext spiContext)
{
	NameData local_ip;
	if (!get_local_ip(&local_ip))
		ereport(ERROR, (errmsg("get adb manager local ip.")));
	MgrAddHbaToActiveNodes(spiContext, NameStr(local_ip));
}
static void MgrAddGtmCoordHba(MemoryContext spiContext, char *zone)
{
	dlist_head 			masterGtm = DLIST_STATIC_INIT(masterGtm);
	dlist_head 			gtmSlaves = DLIST_STATIC_INIT(gtmSlaves);
	dlist_iter 			iter;
	MgrNodeWrapper      *mgrNode;
	MgrNodeWrapper 	    *gtmMaster = NULL;
	Assert(spiContext);
	Assert(zone);

	selectMgrNodeByNodetype(spiContext, CNDN_TYPE_GTM_COOR_MASTER, &masterGtm);
	gtmMaster = selectMgrGtmCoordNode(spiContext);
	Assert(gtmMaster);
	selectActiveMgrSlaveNodesInZone(gtmMaster->form.oid,
									CNDN_TYPE_GTM_COOR_SLAVE,
									zone,
									spiContext,
									&gtmSlaves);	
	dlist_foreach(iter, &gtmSlaves)
	{
		mgrNode = dlist_container(MgrNodeWrapper, link, iter.cur);
		Assert(mgrNode);
		MgrAddHbaToActiveNodes(spiContext, mgrNode->host->hostaddr);
	}	
}
static void MgrAddHbaToActiveNodes(MemoryContext spiContext, char *addIp)
{
	dlist_head nodes = DLIST_STATIC_INIT(nodes);
	dlist_iter iter;
	MgrNodeWrapper *node;
	AppendNodeInfo nodeinfo;
	bool is_exist = false;
	bool is_running = false; 
	StringInfoData send_hba_msg;
	NameData local_ip;
	GetAgentCmdRst getAgentCmdRst;

	initStringInfo(&send_hba_msg);
	initStringInfo(&(getAgentCmdRst.description));
	
	if (!get_local_ip(&local_ip))
		ereport(ERROR, (errmsg("get adb manager local ip.")));
	
	selectActiveMgrNode(spiContext, &nodes);
	dlist_foreach(iter, &nodes)
	{
		node = dlist_container(MgrNodeWrapper, link, iter.cur);
		memset(&nodeinfo, 0, sizeof(AppendNodeInfo));
		is_exist = false;
		is_running = false;
		get_nodeinfo(NameStr(node->form.nodename), node->form.nodetype, &is_exist, &is_running, &nodeinfo);
		if (is_exist && is_running)
		{
			resetStringInfo(&send_hba_msg);
			resetStringInfo(&(getAgentCmdRst.description));
			
			/*send adb manager ip to coordinator pg_hba.conf file*/
			mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", "all", addIp, 32, "trust", &send_hba_msg);
			mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF
									,nodeinfo.nodepath
									,&send_hba_msg
									,nodeinfo.nodehost
									,&getAgentCmdRst);
			if (!getAgentCmdRst.ret)
				ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

			/*execute pgxc_ctl reload to take effect for the new value in the pg_hba.conf  */
			mgr_reload_conf(nodeinfo.nodehost, nodeinfo.nodepath);
		}
	}

}

