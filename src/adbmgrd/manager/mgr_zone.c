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
#include "utils/tqual.h"
#include "funcapi.h"
#include "fmgr.h"
#include "utils/lsyscache.h"
#include "executor/spi.h"
#include "../../interfaces/libpq/libpq-fe.h"
#include "nodes/makefuncs.h"
#include "access/xlog.h"
#include "nodes/nodes.h"

char *mgr_zone;

#define MGR_PGEXEC_DIRECT_EXE_UTI_RET_COMMAND_OK	0 

static void MgrZoneFailoverGtm(MemoryContext spiContext, char *currentZone);
static void MgrZoneFailoverCoord(MemoryContext spiContext, char *currentZone);
static void MgrZoneFailoverDN(MemoryContext spiContext, char *currentZone);
static MgrNodeWrapper *MgrGetOldGtmMasterNotZone(MemoryContext spiContext, char *currentZone);
static void MgrFailoverCheck(MemoryContext spiContext, char *currentZone);
static void MgrCheckMasterHasSlave(MemoryContext spiContext, char *currentZone);
static void MgrCheckMasterHasSlaveCnDn(MemoryContext spiContext, char *currentZone, char nodeType);
static void MgrMakesureAllSlaveRunning(void);

Datum mgr_zone_failover(PG_FUNCTION_ARGS)
{
	HeapTuple 		tupResult = NULL;
	NameData 		name;
	char 			*currentZone;
	int 			spiRes = 0;	
	MemoryContext 	spiContext = NULL;

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot do the command during recovery")));

	currentZone  = PG_GETARG_CSTRING(0);
	Assert(currentZone);
	if (strcmp(currentZone, mgr_zone) != 0)
		ereport(ERROR, (errmsg("the given zone name \"%s\" is not the same wtih guc parameter mgr_zone \"%s\" in postgresql.conf", currentZone, mgr_zone)));	

	namestrcpy(&name, "ZONE FAILOVER");
	PG_TRY();
	{
		if ((spiRes = SPI_connect()) != SPI_OK_CONNECT){
			ereport(ERROR, (errmsg("SPI_connect failed, connect return:%d",	spiRes)));
		}
		spiContext = CurrentMemoryContext; 		
		MgrFailoverCheck(spiContext, currentZone);

		ereportNoticeLog(errmsg("======== ZONE FAILOVER %s, step1:failover gtmcoord slave in %s ========.", currentZone, currentZone));
		MgrZoneFailoverGtm(spiContext, currentZone);

		ereportNoticeLog(errmsg("======== ZONE FAILOVER %s, step2:failover coordinator slave in %s ========.", currentZone, currentZone));
		MgrZoneFailoverCoord(spiContext, currentZone);

		ereportNoticeLog(errmsg("======== ZONE FAILOVER %s, step3:failover datanode slave in %s ========.", currentZone, currentZone));
		MgrZoneFailoverDN(spiContext, currentZone);
	}PG_CATCH();
	{
		SPI_finish();
		ereport(ERROR, (errmsg(" ZONE FAILOVER zone(%s) failed.", currentZone)));
		PG_RE_THROW();
	}PG_END_TRY();

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
	int 			spiRes = 0;	
	MemoryContext 	spiContext = NULL;

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot do the command during recovery")));

	currentZone  = PG_GETARG_CSTRING(0);
	Assert(currentZone);
	if (strcmp(currentZone, mgr_zone) != 0){
		ereport(ERROR, (errmsg("the given zone name \"%s\" is not the same wtih guc parameter mgr_zone \"%s\" in postgresql.conf", currentZone, mgr_zone)));
	}
		
	namestrcpy(&name, "ZONE SWITCHOVER");
	PG_TRY();
	{
		if ((spiRes = SPI_connect()) != SPI_OK_CONNECT){
			ereport(ERROR, (errmsg("SPI_connect failed, connect return:%d",	spiRes)));
		}
		spiContext = CurrentMemoryContext; 		
		MgrFailoverCheck(spiContext, currentZone);

		ereportNoticeLog(errmsg("======== ZONE SWITCHOVER %s, step1:switchover gtmcoord slave in %s ========.", currentZone, currentZone));
		MgrZoneSwitchoverGtm(spiContext, currentZone);

		ereportNoticeLog(errmsg("======== ZONE SWITCHOVER %s, step2:switchover coordinator slave in %s ========.", currentZone, currentZone));
		MgrZoneSwitchoverCoord(spiContext, currentZone);

		ereportNoticeLog(errmsg("======== ZONE SWITCHOVER %s, step3:switchover datanode slave in %s ========.", currentZone, currentZone));
		MgrZoneSwitchoverDataNode(spiContext, currentZone);
	}PG_CATCH();
	{
		SPI_finish();
		EmitErrorReport();
		FlushErrorState();
		ereport(ERROR, (errmsg(" ZONE SWITCHOVER %s failed.", currentZone)));
	}PG_END_TRY();

	SPI_finish();

	ereportNoticeLog(errmsg("the command of \"ZONE SWITCHOVER %s\" result is %s, description is %s", currentZone,"true", "success"));
	tupResult = build_common_command_tuple(&name, true, "success");
	return HeapTupleGetDatum(tupResult);
}
/*
* mgr_zone_clear
* clear the tuple which is not in the current zone
*/
Datum mgr_zone_clear(PG_FUNCTION_ARGS)
{
	Relation 		relNode = NULL;
	HeapScanDesc 	relScan  = NULL;
	ScanKeyData 	key[2];
	HeapTuple 		tuple = NULL;
	Form_mgr_node 	mgrNode = NULL;
	char 			*zone = NULL;

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot do the command during recovery")));

	zone  = PG_GETARG_CSTRING(0);
	Assert(zone);

	if (pg_strcasecmp(zone, mgr_zone) != 0)
		ereport(ERROR, (errmsg("the given zone name \"%s\" is not the same wtih guc parameter mgr_zone \"%s\" in postgresql.conf", zone, mgr_zone)));

	ereportNoticeLog(errmsg("drop node if the node is not inited or not in this zone(%s).", zone));

	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(false));
	ScanKeyInit(&key[1]
			,Anum_mgr_node_nodezone
			,BTEqualStrategyNumber
			,F_NAMENE
			,CStringGetDatum(zone));

	PG_TRY();
	{
		relNode = heap_open(NodeRelationId, RowExclusiveLock);
		relScan = heap_beginscan_catalog(relNode, 2, key);
		while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
		{
			mgrNode = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgrNode);
			ereport(LOG, (errmsg("zone clear %s, drop node %s.", zone, NameStr(mgrNode->nodename))));

			if(HeapTupleIsValid(tuple))
			{
				simple_heap_delete(relNode, &(tuple->t_self));
			}
		}
		EndScan(relScan);
	}PG_CATCH();
	{
		EndScan(relScan);	
		heap_close(relNode, RowExclusiveLock);	
		PG_RE_THROW();
	}PG_END_TRY();

	heap_close(relNode, RowExclusiveLock);
	PG_RETURN_BOOL(true);
}

Datum mgr_zone_init(PG_FUNCTION_ARGS)
{
	Relation 		relNode  = NULL;
	HeapTuple 		tuple    = NULL;
	HeapTuple 		tupResult= NULL;
	HeapScanDesc 	relScan  = NULL;
	char            *coordMaster = NULL;
	char 			*currentZone= NULL;
	NameData 		name;
	Form_mgr_node   mgrNode;
	ScanKeyData 	key[3];
	StringInfoData 	strerr;
	bool            res = true;
	int             total = 0;
	int             num = 0;

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot do the command during recovery")));

	namestrcpy(&name, "zone init");
	currentZone = PG_GETARG_CSTRING(0);
	initStringInfo(&strerr);
	
	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(false));
	ScanKeyInit(&key[1]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(false));
	ScanKeyInit(&key[2]
			,Anum_mgr_node_nodezone
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,CStringGetDatum(currentZone));
	PG_TRY();
	{
		relNode = heap_open(NodeRelationId, RowExclusiveLock);
		relScan = heap_beginscan_catalog(relNode, 3, key);
		while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
		{
			mgrNode = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgrNode);
			total++;
		}
		EndScan(relScan);

		relScan = heap_beginscan_catalog(relNode, 3, key);
		while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
		{
			mgrNode = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgrNode);
			num++;
			
			if (mgrNode->nodetype == CNDN_TYPE_GTM_COOR_SLAVE)
			{
				if (mgr_append_agtm_slave_func(NameStr(mgrNode->nodename))){
					ereportNoticeLog(errmsg("append gtmcoord slave %s success, progress is %d/%d.", NameStr(mgrNode->nodename), num, total));		
				}
				else{
					res = false;
					ereportWarningLog(errmsg("append gtmcoord slave %s failed, progress is %d/%d.", NameStr(mgrNode->nodename), num, total));		
				}
			}
			else if (mgrNode->nodetype == CNDN_TYPE_COORDINATOR_SLAVE)
			{
				coordMaster = mgr_get_mastername_by_nodename_type(NameStr(mgrNode->nodename), CNDN_TYPE_COORDINATOR_SLAVE);
				if (mgr_append_coord_slave_func(coordMaster, NameStr(mgrNode->nodename), true, &strerr)){
					ereportNoticeLog(errmsg("append coordinator slave %s success, progress is %d/%d.", NameStr(mgrNode->nodename), num, total));		
				}
				else{
					res = false;
					ereportWarningLog(errmsg("append coordinator slave %s failed, progress is %d/%d.", NameStr(mgrNode->nodename), num, total));
				}
				MgrFree(coordMaster);
			}
			else if (mgrNode->nodetype == CNDN_TYPE_DATANODE_SLAVE)
			{
				if (mgr_append_dn_slave_func(NameStr(mgrNode->nodename))){
					ereportNoticeLog(errmsg("append datanode slave %s success, progress is %d/%d.", NameStr(mgrNode->nodename), num, total));		
				}
				else{
					res = false;
					ereportWarningLog(errmsg("append datanode slave %s failed, progress is %d/%d.", NameStr(mgrNode->nodename), num, total));
				}
			}		
		}
	}PG_CATCH();
	{
		MgrFree(strerr.data);
		EndScan(relScan);
		heap_close(relNode, RowExclusiveLock);
		PG_RE_THROW();
	}PG_END_TRY();

	MgrFree(strerr.data);
	EndScan(relScan);
	heap_close(relNode, RowExclusiveLock);
	tupResult = build_common_command_tuple(&name, res, res ? "success" : "failed");
	return HeapTupleGetDatum(tupResult);
}

static void MgrCheckMasterHasSlaveCnDn(MemoryContext spiContext, char *currentZone, char nodeType)
{
	dlist_head 			masterList = DLIST_STATIC_INIT(masterList);
	dlist_head 			slaveList  = DLIST_STATIC_INIT(slaveList);
	dlist_iter 			iter;
	MgrNodeWrapper      *mgrNode = NULL;
	Assert(spiContext);
	Assert(currentZone);

	PG_TRY();
	{
		MgrGetOldDnMasterNotZone(spiContext, currentZone, nodeType, &masterList);
		dlist_foreach(iter, &masterList)
		{
			mgrNode = dlist_container(MgrNodeWrapper, link, iter.cur);
			dlist_init(&slaveList);
			selectActiveMgrSlaveNodesInZone(mgrNode->oid, getMgrSlaveNodetype(mgrNode->form.nodetype), currentZone, spiContext, &slaveList);
			if (dlist_is_empty(&slaveList)){
				ereport(ERROR, (errmsg("because %s node(%s) has no slave node in zone(%s), so can't switchover or failover.",  NameStr(mgrNode->form.nodename), mgr_nodetype_str(mgrNode->form.nodetype), currentZone)));
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
static void MgrCheckMasterHasSlave(MemoryContext spiContext, char *currentZone)
{
	MgrNodeWrapper 	*oldMaster = NULL;
	dlist_head 		activeNodes = DLIST_STATIC_INIT(activeNodes);
	Assert(spiContext);
	Assert(currentZone);

	oldMaster = MgrGetOldGtmMasterNotZone(spiContext, currentZone);
	Assert(oldMaster);
	selectActiveMgrSlaveNodesInZone(oldMaster->oid, getMgrSlaveNodetype(oldMaster->form.nodetype), currentZone, spiContext, &activeNodes);
	if (dlist_is_empty(&activeNodes)){
		ereport(ERROR, (errmsg("because no gtmcoord slave in zone(%s), so can't switchover or failover.", currentZone)));
	}

	MgrCheckMasterHasSlaveCnDn(spiContext, currentZone, CNDN_TYPE_COORDINATOR_MASTER);
	MgrCheckMasterHasSlaveCnDn(spiContext, currentZone, CNDN_TYPE_DATANODE_MASTER);	

    pfreeMgrNodeWrapperList(&activeNodes, NULL);
}
static void MgrFailoverCheck(MemoryContext spiContext, char *currentZone)
{
	MgrCheckMasterHasSlave(spiContext, currentZone);
	MgrMakesureAllSlaveRunning();
}
static MgrNodeWrapper *MgrGetOldGtmMasterNotZone(MemoryContext spiContext, char *currentZone)
{
	dlist_head 			masterList = DLIST_STATIC_INIT(masterList);
	dlist_iter 			iter;
	MgrNodeWrapper 		*oldMaster = NULL;
	int                 gtmMasterNum = 0;
	dlist_node 			*node = NULL; 

	selectNodeNotZone(spiContext, currentZone, CNDN_TYPE_GTM_COOR_MASTER, &masterList);
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

static void MgrZoneFailoverGtm(MemoryContext spiContext, char *currentZone)
{
	MgrNodeWrapper 	*oldGtmMaster = NULL;
	NameData       	newMasterName = {{0}};

	Assert(spiContext);
	Assert(currentZone);

	oldGtmMaster = MgrGetOldGtmMasterNotZone(spiContext, currentZone);
	Assert(oldGtmMaster);
	
	PG_TRY();
	{
		FailOverGtmCoordMaster(NameStr(oldGtmMaster->form.nodename), true, true, &newMasterName, currentZone);	
	}
	PG_CATCH();
	{
		pfreeMgrNodeWrapper(oldGtmMaster);
		PG_RE_THROW();
	}PG_END_TRY();
	
	pfreeMgrNodeWrapper(oldGtmMaster);
	return;
}
static void MgrZoneFailoverCoord(MemoryContext spiContext, char *currentZone)
{
	dlist_head 			masterList = DLIST_STATIC_INIT(masterList);
	MgrNodeWrapper 		*mgrNode;
	dlist_iter 			iter;
	NameData 			newMasterName = {{0}};
	
	PG_TRY();
	{
		MgrGetOldDnMasterNotZone(spiContext, currentZone, CNDN_TYPE_COORDINATOR_MASTER, &masterList);
		dlist_foreach(iter, &masterList)
		{
			mgrNode = dlist_container(MgrNodeWrapper, link, iter.cur);
			Assert(mgrNode);
            memset(&newMasterName, 0x00, sizeof(NameData));
			FailOverCoordMaster(NameStr(mgrNode->form.nodename), true, true, &newMasterName, currentZone);
		}
	}
	PG_CATCH();
	{
		pfreeMgrNodeWrapperList(&masterList, NULL);
		PG_RE_THROW();
	}PG_END_TRY();
	
	pfreeMgrNodeWrapperList(&masterList, NULL);
	return;
}
static void MgrZoneFailoverDN(MemoryContext spiContext, char *currentZone)
{
	dlist_head 			masterList = DLIST_STATIC_INIT(masterList);
	MgrNodeWrapper 		*mgrNode;
	dlist_iter 			iter;
	NameData 			newMasterName = {{0}};

	PG_TRY();
	{
		MgrGetOldDnMasterNotZone(spiContext, currentZone, CNDN_TYPE_DATANODE_MASTER, &masterList);
		dlist_foreach(iter, &masterList)
		{
			mgrNode = dlist_container(MgrNodeWrapper, link, iter.cur);
			Assert(mgrNode);
			memset(&newMasterName, 0x00, sizeof(NameData));
			FailOverDataNodeMaster(NameStr(mgrNode->form.nodename), true, true, &newMasterName, currentZone);
		}
	}PG_CATCH();
	{
		pfreeMgrNodeWrapperList(&masterList, NULL);
		PG_RE_THROW();
	}PG_END_TRY();

	pfreeMgrNodeWrapperList(&masterList, NULL);
	return;
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
	HeapScanDesc relScan;
	ScanKeyData key[1];
	bool res = false;

	Assert(zone);
	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodezone
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,CStringGetDatum(zone));

	relNode = heap_open(NodeRelationId, AccessShareLock);
	relScan = heap_beginscan_catalog(relNode, 1, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		if (TupleOid == HeapTupleGetOid(tuple))
		{
			res = true;
			break;
		}

	}
	heap_endscan(relScan);
	heap_close(relNode, AccessShareLock);

	return res;
}
/*
* mgr_get_nodetuple_by_name_zone
*
* get the tuple of node according to nodename and zone
*/
HeapTuple mgr_get_nodetuple_by_name_zone(Relation rel, char *nodename, char *nodezone)
{
	ScanKeyData key[2];
	HeapScanDesc rel_scan;
	HeapTuple tuple = NULL;
	HeapTuple tupleret = NULL;
	NameData nodenamedata;
	NameData nodezonedata;

	Assert(nodename);
	Assert(nodezone);
	namestrcpy(&nodenamedata, nodename);
	namestrcpy(&nodezonedata, nodezone);
	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodename
		,BTEqualStrategyNumber, F_NAMEEQ
		,NameGetDatum(&nodenamedata));
	ScanKeyInit(&key[1]
		,Anum_mgr_node_nodezone
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,NameGetDatum(&nodezonedata));
	rel_scan = heap_beginscan_catalog(rel, 2, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		break;
	}
	tupleret = heap_copytuple(tuple);
	heap_endscan(rel_scan);
	return tupleret;
}
/*
* mgr_node_has_slave_inzone
* check the oid has been used by slave in given zone
*/
bool mgr_node_has_slave_inzone(Relation rel, char *zone, Oid mastertupleoid)
{
	ScanKeyData key[2];
	HeapTuple tuple;
	HeapScanDesc scan;

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
	scan = heap_beginscan_catalog(rel, 2, key);
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		heap_endscan(scan);
		return true;
	}
	heap_endscan(scan);
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


