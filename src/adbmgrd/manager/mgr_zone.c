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

static bool mgr_promote_node_in_zone(Relation relNode, int keyNum, ScanKeyData key[2], char nodeTypeList[8], char **agtmHost, int *agtmPort, StringInfoData *resultmsg);
static bool mgr_check_node_allow_connect(Relation relNode, int keyNum, ScanKeyData key[2], char *currentZone);
static bool mgr_zone_modify_allnodes_agtm_host_port(const char *zone, int agtmPort, char *agtmHost);
static bool mgr_zone_has_node(const char *zonename, char nodetype);
static char *mgr_zone_get_node_type(char nodetype);
static void mgr_zone_has_all_masternode(char *currentZone);
static void mgr_make_sure_allmaster_running(void);
static void mgr_make_sure_allslave_running(void);
static void mgr_zone_update_allcoord_xcnode(PGconn *pgConn, Relation relNode, ScanKeyData key[3], char *currentZone);
static char mgr_zone_get_restart_cmd(char nodetype);
static void mgr_zone_restart_master_node(Relation relNode, ScanKeyData key[3], char *currentZone, StringInfoData *resultmsg);
static void mgr_zone_clear_sync_masternameoid(Relation relNode, char *currentZone);
static void mgr_zone_update_otherzone_mgrnode(Relation relNode, char *currentZone);
static void mgr_shutdown_otherzone_node(Relation relNode, char *currentZone);

/*
* mgr_zone_promote
* make one zone as sub-center, all of nodes in sub-center as slave nodes, when we promote the sub-center to
* master conter, we should promote the nodes to master which whose master nodename not in the given zone
*/
Datum mgr_zone_promote(PG_FUNCTION_ARGS)
{
	Relation 		relNode   = NULL;
	HeapTuple 		tupResult = NULL;
	StringInfoData 	resultmsg;
	ScanKeyData 	key[2];
	NameData 		name;	
	bool 			bres = true;
	char 			*currentZone;
	char 			*agtmHost = NULL;
	int 			agtmPort = 0;
	char 			nodeTypeList[8] = {0};

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot do the command during recovery")));

	currentZone  = PG_GETARG_CSTRING(0);
	Assert(currentZone);
	if (strcmp(currentZone, mgr_zone) != 0)
		ereport(ERROR, (errmsg("the given zone name \"%s\" is not the same wtih guc parameter mgr_zone \"%s\" in postgresql.conf", currentZone, mgr_zone)));

	ereportNoticeLog(errmsg("in zone \"%s\"", currentZone));
	mgr_make_sure_allslave_running();
	initStringInfo(&resultmsg);
	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	ScanKeyInit(&key[1]
			,Anum_mgr_node_nodezone
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,CStringGetDatum(currentZone));	

	PG_TRY();
	{
		relNode = heap_open(NodeRelationId, RowExclusiveLock);
		ereportNoticeLog(errmsg("ZONE PROMOTE %s, step1--promote gtm slave in zone(%s).", currentZone, currentZone));		
		nodeTypeList[0] = CNDN_TYPE_GTM_COOR_SLAVE;
		bres = mgr_promote_node_in_zone(relNode, 2, key, nodeTypeList, &agtmHost, &agtmPort, &resultmsg);
		if (NULL == agtmHost)
			ereport(ERROR, (errmsg("gtm slave does not exist in zone %s", currentZone)));
		
		if (bres)
		{
			ereportNoticeLog(errmsg("ZONE PROMOTE %s, step2--refresh agtm_host, agtm_port to all nodes in zone(%s)", currentZone, currentZone));
			if (!(bres = mgr_zone_modify_allnodes_agtm_host_port(currentZone, agtmPort, agtmHost)))
			{
				ereport(WARNING, (errmsg("refresh agtm_host and agtm_port in zone \"%s\" fail", currentZone)));
				appendStringInfo(&resultmsg, "refresh agtm_host, agtm_port in zone %s fail, check agtm_port=%d, \
					agtm_host=%s in postgresql.conf of all coordinators and datanodes\n", currentZone, agtmPort, agtmHost);
			}
			MgrFree(agtmHost);

			ereportNoticeLog(errmsg("ZONE PROMOTE %s, step3--promote coordinators and datanodes in zone(%s)", currentZone, currentZone));
			nodeTypeList[0] = CNDN_TYPE_COORDINATOR_SLAVE;
			nodeTypeList[1] = CNDN_TYPE_DATANODE_SLAVE;
			bres = mgr_promote_node_in_zone(relNode, 2, key, nodeTypeList, &agtmHost, &agtmPort, &resultmsg);

			ereportNoticeLog(errmsg("ZONE PROMOTE %s, step4--check the node has allow connect in zone(%s)", currentZone, currentZone));
			bres = mgr_check_node_allow_connect(relNode, 2, key, currentZone);
		}
	}PG_CATCH();
	{
		MgrFree(resultmsg.data);
		MgrFree(agtmHost);	
		heap_close(relNode, RowExclusiveLock);	
		PG_RE_THROW();
	}PG_END_TRY();

    MgrFree(agtmHost);
	heap_close(relNode, RowExclusiveLock);

	ereportNoticeLog(errmsg("the command of \"ZONE PROMOTE %s\" result is %s, description is %s", currentZone
		,bres ? "true":"false", resultmsg.len == 0 ? "success":resultmsg.data));
	namestrcpy(&name, "zone promote");
	tupResult = build_common_command_tuple(&name, bres, resultmsg.len == 0 ? "success":resultmsg.data);
	MgrFree(resultmsg.data);
	return HeapTupleGetDatum(tupResult);
}
/*
* mgr_zone_config_all
* refresh pgxc_node on all coordinators in the given zone 
*/
Datum mgr_zone_config_all(PG_FUNCTION_ARGS)
{
	Relation 		relNode  = NULL;
	HeapTuple 		tupResult= NULL;
	StringInfoData 	resultmsg;
	ScanKeyData 	key[3];
	NameData 		name;	
	char 			*currentZone= NULL;
	bool 			bres 	    = true;
	PGconn 			*gtm_conn   = NULL;
	Oid 			cnoid;
	
	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot do the command during recovery")));
	
	namestrcpy(&name, "zone config");
	currentZone  = PG_GETARG_CSTRING(0);
	Assert(currentZone);

	if (strcmp(currentZone, mgr_zone) !=0)
		ereport(ERROR, (errmsg("the given zone name \"%s\" is not the same wtih guc parameter mgr_zone \"%s\" in postgresql.conf", currentZone, mgr_zone)));
	
	mgr_zone_has_all_masternode(currentZone);
    mgr_make_sure_allmaster_running();
	
	initStringInfo(&resultmsg);
	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	ScanKeyInit(&key[1]
			,Anum_mgr_node_nodezone
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,CStringGetDatum(currentZone));

	PG_TRY();
	{
		relNode = heap_open(NodeRelationId, RowExclusiveLock);
		mgr_get_gtmcoord_conn(MgrGetDefDbName(), &gtm_conn, &cnoid);
		hexp_pqexec_direct_execute_utility(gtm_conn, SQL_BEGIN_TRANSACTION, MGR_PGEXEC_DIRECT_EXE_UTI_RET_COMMAND_OK);
		ereportNoticeLog(errmsg("ZONE CONFIG %s, step1:update master node pgxc_node.", currentZone));
		mgr_zone_update_allcoord_xcnode(gtm_conn, relNode, key, currentZone);

		ereportNoticeLog(errmsg("ZONE CONFIG %s, step2:update node info of mgr node table.", currentZone));
		mgr_zone_clear_sync_masternameoid(relNode, currentZone);

		ereportNoticeLog(errmsg("ZONE CONFIG %s, step3:update other zone's node info of mgr node table.", currentZone));
		mgr_zone_update_otherzone_mgrnode(relNode, currentZone);
 	    hexp_pqexec_direct_execute_utility(gtm_conn, SQL_COMMIT_TRANSACTION, MGR_PGEXEC_DIRECT_EXE_UTI_RET_COMMAND_OK);
		
        ereportNoticeLog(errmsg("ZONE CONFIG %s, step4:shutdown other zone's gtmcoord, coordinator, datanode.", currentZone));							  
        mgr_shutdown_otherzone_node(relNode, currentZone); 	

		ereportNoticeLog(errmsg("ZONE CONFIG %s, step5:update PGSQLCONF pgxc_node_name, then restart node.", currentZone));
		mgr_zone_restart_master_node(relNode, key, currentZone, &resultmsg);
	}PG_CATCH();
	{
		ClosePgConn(gtm_conn);
		heap_close(relNode, RowExclusiveLock);
		MgrFree(resultmsg.data);
		PG_RE_THROW();
	}PG_END_TRY();

	ClosePgConn(gtm_conn);
	heap_close(relNode, RowExclusiveLock);	
	tupResult = build_common_command_tuple(&name, bres, resultmsg.len == 0 ? "success":resultmsg.data);
	MgrFree(resultmsg.data);
	ereport(LOG, (errmsg("the command of \"ZONE CONFIG %s\" result is %s, description is %s", currentZone
		,bres ? "true":"false", resultmsg.len == 0 ? "success":resultmsg.data)));
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
	Form_mgr_node 	mgr_node = NULL;
	char 			*zone = NULL;

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot do the command during recovery")));

	zone  = PG_GETARG_CSTRING(0);
	Assert(zone);

	if (strcmp(zone, mgr_zone) !=0)
		ereport(ERROR, (errmsg("the given zone name \"%s\" is not the same wtih guc parameter mgr_zone \"%s\" in postgresql.conf", zone, mgr_zone)));

	ereportNoticeLog(errmsg("make the special node as master type and set its master name is null, sync_state is null on node table in zone \"%s\"", zone));
	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	ScanKeyInit(&key[1]
			,Anum_mgr_node_nodezone
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,CStringGetDatum(zone));

	PG_TRY();
	{
		relNode = heap_open(NodeRelationId, RowExclusiveLock);
		relScan = heap_beginscan_catalog(relNode, 2, key);
		while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			if (mgr_checknode_in_currentzone(zone, mgr_node->nodemasternameoid))
				continue;
			ereportNoticeLog(errmsg("make the node \"%s\" as master type on node table in zone \"%s\"", NameStr(mgr_node->nodename), zone));
			mgr_node->nodetype = mgr_get_master_type(mgr_node->nodetype);
			namestrcpy(&(mgr_node->nodesync), "");
			mgr_node->nodemasternameoid = 0;
			heap_inplace_update(relNode, tuple);
		}
		EndScan(relScan);

		ereportNoticeLog(errmsg("on node table, drop the node which is not in zone \"%s\"", zone));
		mgr_zone_update_otherzone_mgrnode(relNode, zone);
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
		relScan = heap_beginscan_catalog(relNode, 1, key);
		while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
		{
			mgrNode = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgrNode);
			if (mgrNode->nodetype == CNDN_TYPE_GTM_COOR_SLAVE)
			{
				if (mgr_append_agtm_slave_func(NameStr(mgrNode->nodename))){
					ereportNoticeLog(errmsg("append gtmcoord slave %s success.", NameStr(mgrNode->nodename)));		
				}
				else{
					res = false;
					ereportWarningLog(errmsg("append gtmcoord slave %s failed.", NameStr(mgrNode->nodename)));		
				}
			}
			else if (mgrNode->nodetype == CNDN_TYPE_COORDINATOR_SLAVE)
			{
				coordMaster = mgr_get_mastername_by_nodename_type(NameStr(mgrNode->nodename), CNDN_TYPE_COORDINATOR_SLAVE);
				if (mgr_append_coord_slave_func(coordMaster, NameStr(mgrNode->nodename), &strerr)){
					ereportNoticeLog(errmsg("append coordinator slave %s success.", NameStr(mgrNode->nodename)));		
				}
				else{
					res = false;
					ereportWarningLog(errmsg("append coordinator slave %s failed.", NameStr(mgrNode->nodename)));
				}
				MgrFree(coordMaster);
			}
			else if (mgrNode->nodetype == CNDN_TYPE_DATANODE_SLAVE)
			{
				if (mgr_append_dn_slave_func(NameStr(mgrNode->nodename))){
					ereportNoticeLog(errmsg("append datanode slave %s success.", NameStr(mgrNode->nodename)));		
				}
				else{
					res = false;
					ereportWarningLog(errmsg("append datanode slave %s failed.", NameStr(mgrNode->nodename)));
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


static bool mgr_promote_node_in_zone(Relation relNode, int keyNum, ScanKeyData key[2], char nodeTypeList[8], 
										char **agtmHost, int *agtmPort, StringInfoData *resultmsg)
{
	HeapScanDesc    relScan = NULL;
	HeapTuple 		tuple   = NULL;
	Form_mgr_node 	mgr_node=NULL;
	bool			bres 	= true;
	StringInfoData 	infosendmsg;
	StringInfoData 	restmsg;
	bool 			isNull = true;
	Datum 			datumPath;
	char 			*nodePath = NULL;

	Assert(relNode);
	Assert(key);
	Assert(nodeTypeList);
	Assert(agtmPort);
	Assert(resultmsg);
	initStringInfo(&infosendmsg);
	initStringInfo(&restmsg);

    PG_TRY();
	{
		relScan = heap_beginscan_catalog(relNode, keyNum, key);
		while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);				
			if(mgr_check_nodetype_exist(mgr_node->nodetype, nodeTypeList))
			{
				/* change slave type to master */
				mgr_node->nodetype = mgr_get_master_type(mgr_node->nodetype);
				heap_inplace_update(relNode, tuple);
				*agtmPort = mgr_node->nodeport;
				MgrFree(*agtmHost);
				*agtmHost = get_hostaddress_from_hostoid(mgr_node->nodehost);
				isNull = true;
				datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(relNode), &isNull);
				if(isNull){
					ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node"), errmsg("column nodepath is null")));
				}
				nodePath = TextDatumGetCString(datumPath);
				resetStringInfo(&infosendmsg);
				resetStringInfo(&restmsg);
				appendStringInfo(&infosendmsg, " promote -D %s", nodePath);
				mgr_ma_send_cmd_get_original_result(AGT_CMD_DN_FAILOVER, infosendmsg.data, mgr_node->nodehost, &restmsg, AGENT_RESULT_LOG);
				if (restmsg.len != 0 && strstr(restmsg.data, "server promoted") != NULL){
					ereportNoticeLog((errmsg("promote \"%s\" success.", NameStr(mgr_node->nodename))));
				}
				else{
					bres = false;
					ereport(WARNING, (errmsg("promote \"%s\" %s", NameStr(mgr_node->nodename), restmsg.len != 0 ? restmsg.data:"fail")));
					appendStringInfo(resultmsg, "promote \"%s\" fail, params(%s)\n", NameStr(mgr_node->nodename), restmsg.data);
				}
			}
		}
	}PG_CATCH();
	{
		MgrFree(infosendmsg.data);
		MgrFree(restmsg.data);
		MgrFree(*agtmHost);	
		EndScan(relScan);
		PG_RE_THROW();
	}PG_END_TRY();

    MgrFree(infosendmsg.data);
	MgrFree(restmsg.data);
	EndScan(relScan);
	return bres;
}
static bool mgr_check_node_allow_connect(Relation relNode, int keyNum, ScanKeyData key[2], char *currentZone)
{
	HeapScanDesc   	relScan = NULL;
	HeapTuple 		tuple   = NULL;
	Form_mgr_node 	mgr_node=NULL;
	int             i = 0;
	bool 			bres = true;
	char 			*hostAddr = NULL;
	char 			nodePortBuf[10];
	char 			*userName = NULL;
	StringInfoData  resultmsg;

    Assert(relNode);
	Assert(key);
	Assert(currentZone);
	initStringInfo(&resultmsg);

	PG_TRY();
	{
		relScan = heap_beginscan_catalog(relNode, keyNum, key);
		while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			if (mgr_checknode_in_currentzone(currentZone, mgr_node->nodemasternameoid))
				continue;
			hostAddr = get_hostaddress_from_hostoid(mgr_node->nodehost);
			i = 0;
			while(i++<3)
			{
				if (mgr_check_node_recovery_finish(mgr_node->nodetype, mgr_node->nodehost, mgr_node->nodeport, hostAddr))
					break;
				pg_usleep(1 * 1000000L);
			}
			if (mgr_check_node_recovery_finish(mgr_node->nodetype, mgr_node->nodehost, mgr_node->nodeport, hostAddr))
			{
				memset(nodePortBuf, 0, 10);
				sprintf(nodePortBuf, "%d", mgr_node->nodeport);
				userName = get_hostuser_from_hostoid(mgr_node->nodehost);
				i = 0;
				while(i++<3)
				{
					if (pingNode_user(hostAddr, nodePortBuf, userName) == 0)
						break;
					pg_usleep(1 * 1000000L);
				}

				if (pingNode_user(hostAddr, nodePortBuf, userName) != 0)
				{
					bres = false;
					ereport(WARNING, (errmsg("the node \"%s\" is master type, but not running normal", NameStr(mgr_node->nodename))));
					appendStringInfo(&resultmsg, "the node \"%s\" is master type, but not running normal\n", NameStr(mgr_node->nodename));
				}
			}
			else
			{
				bres = false;
				ereport(WARNING, (errmsg("the node \"%s\" is not master type, execute \"select pg_is_in_recovery()\" on the node to check", NameStr(mgr_node->nodename))));
				appendStringInfo(&resultmsg, "the node \"%s\" is not master type, execute \"select pg_is_in_recovery()\" on the node to check\n", NameStr(mgr_node->nodename));
			}
			MgrFree(hostAddr);
		}
	}PG_CATCH();
	{
		MgrFree(hostAddr);
		MgrFree(resultmsg.data);
		EndScan(relScan);
		PG_RE_THROW();
	}PG_END_TRY();

	MgrFree(resultmsg.data);
	EndScan(relScan);
	return bres;
}
static char *mgr_zone_get_node_type(char nodetype)
{
	if (nodetype == CNDN_TYPE_GTM_COOR_MASTER)
		return "gtm master";
	else if (nodetype == CNDN_TYPE_COORDINATOR_MASTER)
		return "coordinator master";
	else
		return "datanode master";
}
static void mgr_zone_has_all_masternode(char *currentZone)
{
	if (!mgr_zone_has_node(currentZone, CNDN_TYPE_GTM_COOR_MASTER))
		ereport(ERROR, (errmsg("the zone \"%s\" has not GTMCOORD MASTER in cluster", currentZone)));
	if (!mgr_zone_has_node(currentZone, CNDN_TYPE_COORDINATOR_MASTER))
		ereport(ERROR, (errmsg("the zone \"%s\" has not COORDINATOR MASTER in cluster", currentZone)));
	if (!mgr_zone_has_node(currentZone, CNDN_TYPE_DATANODE_MASTER))
		ereport(ERROR, (errmsg("the zone \"%s\" has not DATANODE MASTER in cluster", currentZone)));
}
static void mgr_make_sure_allmaster_running(void)
{
	mgr_make_sure_all_running(CNDN_TYPE_GTM_COOR_MASTER);
	mgr_make_sure_all_running(CNDN_TYPE_COORDINATOR_MASTER);
	mgr_make_sure_all_running(CNDN_TYPE_DATANODE_MASTER);
}
static void mgr_make_sure_allslave_running(void)
{
	mgr_make_sure_all_running(CNDN_TYPE_GTM_COOR_SLAVE);
	mgr_make_sure_all_running(CNDN_TYPE_COORDINATOR_SLAVE);
	mgr_make_sure_all_running(CNDN_TYPE_DATANODE_SLAVE);
}
static void mgr_zone_update_allcoord_xcnode(PGconn *pgConn, Relation relNode, ScanKeyData key[3], char *currentZone)
{
	HeapScanDesc 	relScanout = NULL;
	HeapScanDesc 	relScanin  = NULL;
	HeapTuple 		tupleout = NULL;
	Form_mgr_node	mgrNodeOut = NULL;
	Form_mgr_node 	mgrNodeIn = NULL;
	Form_mgr_node 	mgr_nodeM = NULL;
	HeapTuple 		tuplein = NULL;
	char 			*hostAddress = NULL;
	HeapTuple 		masterTuple = NULL;
	StringInfoData 	infosendmsg;

	Assert(relNode);
	Assert(key);
	Assert(currentZone);

    PG_TRY();
	{
		initStringInfo(&infosendmsg);
		relScanout = heap_beginscan_catalog(relNode, 2, key);
		while((tupleout = heap_getnext(relScanout, ForwardScanDirection)) != NULL)
		{
			mgrNodeOut = (Form_mgr_node)GETSTRUCT(tupleout);
			Assert(mgrNodeOut);

			ereport(LOG, (errmsg("mgr_zone_config_all  nodename(%s) nodetype(%c) nodemasternameoid(%d).", 
				NameStr(mgrNodeOut->nodename), mgrNodeOut->nodetype, mgrNodeOut->nodemasternameoid)));

			if ((!IsGTMMaster(mgrNodeOut->nodetype)) && (!IsCOORDMaster(mgrNodeOut->nodetype)))
				continue;		
			if (mgr_checknode_in_currentzone(currentZone, mgrNodeOut->nodemasternameoid)){
				continue;	
			}

			ereport(LOG, (errmsg("mgr_zone_config_all nodename(%s).", NameStr(mgrNodeOut->nodename))));

			resetStringInfo(&infosendmsg);
			relScanin = heap_beginscan_catalog(relNode, 2, key);
			while((tuplein = heap_getnext(relScanin, ForwardScanDirection)) != NULL)
			{
				mgrNodeIn = (Form_mgr_node)GETSTRUCT(tuplein);
				Assert(mgrNodeIn);
				if ((!IsGTMMaster(mgrNodeIn->nodetype)) && (!IsCOORDMaster(mgrNodeIn->nodetype)) && (!IsDNMaster(mgrNodeIn->nodetype)))
					continue;
				if (mgr_checknode_in_currentzone(currentZone, mgrNodeIn->nodemasternameoid))
					continue;

				hostAddress = get_hostaddress_from_hostoid(mgrNodeIn->nodehost);
				masterTuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(mgrNodeIn->nodemasternameoid));
				if(!HeapTupleIsValid(masterTuple))
				{				
					ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
						,errmsg("cache lookup failed for the master of \"%s\" in zone \"%s\"", NameStr(mgrNodeIn->nodename), currentZone)));
				}
				mgr_nodeM = (Form_mgr_node)GETSTRUCT(masterTuple);
				Assert(mgr_nodeM);
				appendStringInfo(&infosendmsg
					,"alter node \"%s\" with(name='%s', host='%s', port=%d, preferred=false) on (\"%s\");"
					,NameStr(mgr_nodeM->nodename), NameStr(mgrNodeIn->nodename)
					,hostAddress, mgrNodeIn->nodeport, NameStr(mgrNodeOut->nodename));
					
				ereport(LOG, (errmsg("alter node \"%s\" with(name='%s', host='%s', port=%d, preferred=false) on (\"%s\");"
					,NameStr(mgr_nodeM->nodename), NameStr(mgrNodeIn->nodename)
					,hostAddress, mgrNodeIn->nodeport, NameStr(mgrNodeOut->nodename))));
				
				ReleaseSysCache(masterTuple);
				MgrFree(hostAddress);
			}
			hexp_pqexec_direct_execute_utility(pgConn, infosendmsg.data, MGR_PGEXEC_DIRECT_EXE_UTI_RET_COMMAND_OK);
			EndScan(relScanin);
		}
		EndScan(relScanout);
	}PG_CATCH();
	{
		MgrFree(infosendmsg.data);
		MgrFree(hostAddress);
		EndScan(relScanin);
		EndScan(relScanout);		
		PG_RE_THROW();
	}PG_END_TRY();

	MgrFree(infosendmsg.data);
	MgrFree(hostAddress);
	return;
}
static char mgr_zone_get_restart_cmd(char nodetype)
{
	if (nodetype == CNDN_TYPE_GTM_COOR_MASTER)
		return AGT_CMD_AGTM_RESTART;
	else if (nodetype == CNDN_TYPE_COORDINATOR_MASTER)	
		return AGT_CMD_CN_RESTART;
	else
		return AGT_CMD_DN_RESTART;
}
static void mgr_zone_restart_master_node(Relation relNode, ScanKeyData key[3], char *currentZone, StringInfoData *resultmsg)
{
	HeapScanDesc 	relScan = NULL;	
    HeapTuple 		tuple;
	Form_mgr_node 	mgr_node;
	StringInfoData  infosendmsg;
	GetAgentCmdRst  getAgentCmdRst;
	bool 			isNull = false;
	Datum 			datumPath;
	char 			*cndnPath = NULL;

	Assert(relNode);
	Assert(key);
	Assert(currentZone);
	Assert(resultmsg);

    initStringInfo(&infosendmsg);
	initStringInfo(&(getAgentCmdRst.description));

	PG_TRY();
	{
		relScan = heap_beginscan_catalog(relNode, 2, key);
		while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			if ((!IsGTMMaster(mgr_node->nodetype)) && (!IsCOORDMaster(mgr_node->nodetype)) && (!IsDNMaster(mgr_node->nodetype)))
				continue;

			ereportNoticeLog(errmsg("in zone %s set pgxc_node_name='%s' for %s \"%s\"",
				currentZone, NameStr(mgr_node->nodename), mgr_zone_get_node_type(mgr_node->nodetype), NameStr(mgr_node->nodename)));

			resetStringInfo(&infosendmsg);
			resetStringInfo(&(getAgentCmdRst.description));
			datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(relNode), &isNull);
			if(isNull)
			{
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
					, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
					, errmsg("column cndnpath is null")));
			}

			cndnPath = TextDatumGetCString(datumPath);
			mgr_append_pgconf_paras_str_quotastr("pgxc_node_name", NameStr(mgr_node->nodename), &infosendmsg);
			mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, cndnPath, &infosendmsg, mgr_node->nodehost, &getAgentCmdRst);
			if (!getAgentCmdRst.ret)
			{
				appendStringInfo(resultmsg, "in zone %s set pgxc_node_name='%s' for %s \"%s\" fail %s\n"
					, currentZone, NameStr(mgr_node->nodename), mgr_zone_get_node_type(mgr_node->nodetype)
					, NameStr(mgr_node->nodename), getAgentCmdRst.description.data);
				ereport(WARNING, (errmsg("in zone %s set pgxc_node_name='%s' for %s \"%s\" fail %s\n"
					, currentZone, NameStr(mgr_node->nodename), mgr_zone_get_node_type(mgr_node->nodetype)
					, NameStr(mgr_node->nodename), getAgentCmdRst.description.data)));
				break;			
			}
			MgrFree(cndnPath);

			resetStringInfo(&(getAgentCmdRst.description));
			mgr_runmode_cndn_get_result(mgr_zone_get_restart_cmd(mgr_node->nodetype), &getAgentCmdRst, relNode, tuple, SHUTDOWN_F);
			if(!getAgentCmdRst.ret)
			{
				appendStringInfo(resultmsg, "in zone %s restart %s \"%s\" fail %s\n"
					, currentZone, mgr_zone_get_node_type(mgr_node->nodetype)
					, NameStr(mgr_node->nodename), getAgentCmdRst.description.data);
				ereport(WARNING, (errmsg("in zone %s restart %s \"%s\" fail %s\n"
					, currentZone, mgr_zone_get_node_type(mgr_node->nodetype)
					, NameStr(mgr_node->nodename), getAgentCmdRst.description.data)));
				break;	
			}
			else{
				ereport(LOG, (errmsg("in zone %s restart %s \"%s\" success.\n"
					,currentZone, mgr_zone_get_node_type(mgr_node->nodetype), NameStr(mgr_node->nodename))));
			}
		}
	}PG_CATCH();
	{
		MgrFree(cndnPath);
		MgrFree(infosendmsg.data);	
		MgrFree(getAgentCmdRst.description.data);	
		EndScan(relScan);
		PG_RE_THROW();
	}PG_END_TRY();

	MgrFree(cndnPath);
	MgrFree(infosendmsg.data);	
	MgrFree(getAgentCmdRst.description.data);
	EndScan(relScan);
	return;
}
static void mgr_zone_clear_sync_masternameoid(Relation relNode, char *currentZone)
{
	ScanKeyData 	key[1];
	HeapScanDesc 	relScan = NULL;
	HeapTuple 		tuple;
	Form_mgr_node 	mgr_node;

	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodezone
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,CStringGetDatum(currentZone));

	PG_TRY();
	{
		relScan = heap_beginscan_catalog(relNode, 1, key);
		while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			if (IsGTMMaster(mgr_node->nodetype)	|| IsCOORDMaster(mgr_node->nodetype) || IsDNMaster(mgr_node->nodetype))
			{
				namestrcpy(&(mgr_node->nodesync), "");
				mgr_node->nodemasternameoid = 0;
				heap_inplace_update(relNode, tuple);
				ereport(LOG, (errmsg("in zone(%s), make the node(%s) of mgr node table: the column nodemasternameoid is null, sync_stat is null", 
					currentZone, NameStr(mgr_node->nodename))));
			}
		}
		EndScan(relScan);
	}PG_CATCH();
	{
		EndScan(relScan);	
		PG_RE_THROW();
	}PG_END_TRY();
}
static void mgr_shutdown_otherzone_node(Relation relNode, char *currentZone)
{
	HeapScanDesc 	relScan = NULL;
	HeapTuple 		tuple;
	Form_mgr_node 	mgr_node;
	MgrNodeWrapper 	*mgrNodeWrapper = NULL;
	Datum 			datumPath;
	bool 			isNull = false;
	MgrHostWrapper  host;

	PG_TRY();
	{
		mgrNodeWrapper = (MgrNodeWrapper*)palloc0(sizeof(MgrNodeWrapper));
		memset(&host, 0x00, sizeof(MgrHostWrapper));
		mgrNodeWrapper->host = &host;
		relScan = heap_beginscan_catalog(relNode, 0, NULL);
		while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			if (strcasecmp(NameStr(mgr_node->nodezone), currentZone) == 0)
				continue; 

			memcpy(&mgrNodeWrapper->form, mgr_node, sizeof(FormData_mgr_node));
			datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(relNode), &isNull);
			if(isNull){
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), err_generic_string(PG_DIAG_TABLE_NAME, "mgr_nodetmp"), errmsg("column nodepath is null")));
			}
			mgrNodeWrapper->nodepath = TextDatumGetCString(datumPath);

			datumPath = heap_getattr(tuple, Anum_mgr_node_nodehost, RelationGetDescr(relNode), &isNull);
			if(isNull){
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), err_generic_string(PG_DIAG_TABLE_NAME, "mgr_nodetmp"), errmsg("column nodehost is null")));
			}		
			get_hostinfo_from_hostoid(DatumGetObjectId(datumPath), &host);

			if(shutdownNodeWithinSeconds(mgrNodeWrapper, SHUTDOWN_NODE_FAST_SECONDS, SHUTDOWN_NODE_IMMEDIATE_SECONDS, false)){
				ereportNoticeLog(errmsg("shutdown %s success in zone \"%s\"", NameStr(mgr_node->nodename), NameStr(mgr_node->nodezone)));		
			}
			else{
				ereportWarningLog(errmsg("shutdown %s failed in zone \"%s\"", NameStr(mgr_node->nodename), NameStr(mgr_node->nodezone)));
			}			
		}
	}PG_CATCH();
	{
		MgrFree(mgrNodeWrapper);
		EndScan(relScan);		
		PG_RE_THROW();
	}PG_END_TRY();	

	MgrFree(mgrNodeWrapper);
	EndScan(relScan);
}
static void mgr_zone_update_otherzone_mgrnode(Relation relNode, char *currentZone)
{
	HeapScanDesc 	relScan = NULL;
	HeapTuple 		tuple;
	Form_mgr_node 	mgr_node;

	PG_TRY();
	{
		relScan = heap_beginscan_catalog(relNode, 0, NULL);
		while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);			
			if (strcasecmp(NameStr(mgr_node->nodezone), currentZone) == 0)
				continue;            
			namestrcpy(&(mgr_node->nodesync), "");
			mgr_node->nodetype      = getMgrSlaveNodetype(mgr_node->nodetype);	
			mgr_node->nodeinited    = false;
			mgr_node->nodeincluster = false;			
			heap_inplace_update(relNode, tuple);
			ereport(LOG, (errmsg("set %s to not inited, not incluster on mgr node table in zone \"%s\"", NameStr(mgr_node->nodename), NameStr(mgr_node->nodezone))));
		}
		EndScan(relScan);
	}PG_CATCH();
	{
		EndScan(relScan);		
		PG_RE_THROW();
	}PG_END_TRY();
}
static bool mgr_zone_modify_allnodes_agtm_host_port(const char *zone, int agtmPort, char *agtmHost)
{
	Relation 		relNode =  NULL;
	HeapTuple 		tuple 	= NULL;
	HeapScanDesc 	relScan = NULL;
	StringInfoData 	infosendmsg;
	GetAgentCmdRst 	getAgentCmdRst;
	Datum 			datumPath;
	ScanKeyData 	key[2];
	Form_mgr_node 	mgr_node= NULL;
	bool 			isNull 	= true;
	bool 			res 	= true;
	char 			*cndnPath;

    Assert(zone);
	Assert(agtmHost);
	initStringInfo(&infosendmsg);
	initStringInfo(&(getAgentCmdRst.description));
	mgr_append_pgconf_paras_str_quotastr("agtm_host", agtmHost, &infosendmsg);
	mgr_append_pgconf_paras_str_int("agtm_port", agtmPort, &infosendmsg);
	
	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodezone
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,CStringGetDatum(zone));
	ScanKeyInit(&key[1]
		,Anum_mgr_node_nodeincluster
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	PG_TRY();
	{
		relNode = heap_open(NodeRelationId, AccessShareLock);
		relScan = heap_beginscan_catalog(relNode, 2, key);
		while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			resetStringInfo(&(getAgentCmdRst.description));
			datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(relNode), &isNull);
			if(isNull){	
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node"), errmsg("column cndnpath is null")));
			}
			cndnPath = TextDatumGetCString(datumPath);
			mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, cndnPath, &infosendmsg, mgr_node->nodehost, &getAgentCmdRst);
			if (getAgentCmdRst.ret){
				ereport(LOG, (errmsg("on %s modify agtm_host, agtm_port success.", NameStr(mgr_node->nodename))));
			}
			else{
				res = false;
				ereport(LOG, (errmsg("on %s modify agtm_host, agtm_port fail.", NameStr(mgr_node->nodename))));
				ereport(WARNING, (errmsg("on %s modify agtm_host, agtm_port fail.", NameStr(mgr_node->nodename))));			
			}
		}
	}PG_CATCH();
	{
		MgrFree(infosendmsg.data);
		MgrFree(getAgentCmdRst.description.data);
		EndScan(relScan);
		heap_close(relNode, AccessShareLock);	
		PG_RE_THROW();
	}PG_END_TRY();

	MgrFree(infosendmsg.data);
	MgrFree(getAgentCmdRst.description.data);
	EndScan(relScan);
	heap_close(relNode, AccessShareLock);	
	return res;
}
/*
* mgr_zone_has_node
* check the zone has given the type of node in cluster
*/
static bool mgr_zone_has_node(const char *zonename, char nodetype)
{
	bool bres = false;
	Relation relNode;
	HeapScanDesc relScan;
	ScanKeyData key[3];
	HeapTuple tuple =NULL;

	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	ScanKeyInit(&key[1]
			,Anum_mgr_node_nodezone
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,CStringGetDatum(zonename));
	ScanKeyInit(&key[2]
		,Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(nodetype));
	relNode = heap_open(NodeRelationId, AccessShareLock);
	relScan = heap_beginscan_catalog(relNode, 3, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{	
		bres = true;
		break;
	}
	heap_endscan(relScan);
	heap_close(relNode, AccessShareLock);

	return bres;
}

