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

static bool mgr_zone_modify_conf_agtm_host_port(const char *zone, int agtmPort, char *agtmHost);
static bool mgr_zone_has_node(const char *zonename, char nodetype);
/*
* mgr_zone_promote
* make one zone as sub-center, all of nodes in sub-center as slave nodes, when we promote the sub-center to
* master conter, we should promote the nodes to master which whose master nodename not in the given zone
*/
Datum mgr_zone_promote(PG_FUNCTION_ARGS)
{
	Relation relNode;
	HeapTuple tuple;
	HeapTuple tupResult;
	HeapScanDesc relScan;
	Form_mgr_node mgr_node;
	Datum datumPath;
	StringInfoData infosendmsg;
	StringInfoData restmsg;
	StringInfoData resultmsg;
	ScanKeyData key[2];
	NameData name;
	bool isNull;
	bool bres = true;
	char *nodePath;
	char *hostAddr;
	char *userName;
	char *currentZone;
	char *agtmHost = NULL;
	char nodePortBuf[10];
	int i = 0;
	int agtmPort = 0;

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot do the command during recovery")));

	currentZone  = PG_GETARG_CSTRING(0);
	Assert(currentZone);

	if (strcmp(currentZone, mgr_zone) !=0)
		ereport(ERROR, (errmsg("the given zone name \"%s\" is not the same wtih guc parameter mgr_zone \"%s\" in postgresql.conf", currentZone, mgr_zone)));

	mgr_make_sure_all_running(GTM_TYPE_GTM_SLAVE);
	mgr_make_sure_all_running(CNDN_TYPE_COORDINATOR_SLAVE);
	mgr_make_sure_all_running(CNDN_TYPE_DATANODE_SLAVE);

	ereport(LOG, (errmsg("in zone \"%s\"", currentZone)));
	ereport(NOTICE, (errmsg("in zone \"%s\"", currentZone)));
	initStringInfo(&infosendmsg);
	initStringInfo(&restmsg);
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

	relNode = heap_open(NodeRelationId, RowExclusiveLock);
	relScan = heap_beginscan_catalog(relNode, 2, key);
	PG_TRY();
	{
		while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			/* check the node's master in the same zone */
			if (mgr_checknode_in_currentzone(currentZone, mgr_node->nodemasternameoid))
				continue;
			if (mgr_node->nodetype != GTM_TYPE_GTM_SLAVE)
				continue;
			/* change slave type to master */
			mgr_node->nodetype = mgr_get_master_type(mgr_node->nodetype);
			heap_inplace_update(relNode, tuple);
			agtmPort = mgr_node->nodeport;
			agtmHost = get_hostaddress_from_hostoid(mgr_node->nodehost);
			datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(relNode), &isNull);
			if(isNull)
			{
				heap_endscan(relScan);
				heap_close(relNode, RowExclusiveLock);
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
					, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
					, errmsg("column nodepath is null")));
			}
			nodePath = TextDatumGetCString(datumPath);
			resetStringInfo(&infosendmsg);
			resetStringInfo(&restmsg);
			appendStringInfo(&infosendmsg, " promote -D %s", nodePath);
			mgr_ma_send_cmd_get_original_result(AGT_CMD_DN_FAILOVER, infosendmsg.data, mgr_node->nodehost, &restmsg, AGENT_RESULT_LOG);
			if (restmsg.len != 0 && strstr(restmsg.data, "server promoted") != NULL)
			{
				ereport(LOG, (errmsg("promote \"%s\" %s", NameStr(mgr_node->nodename), restmsg.data)));
			}
			else
			{
				bres = false;
				ereport(WARNING, (errmsg("promote \"%s\" %s", NameStr(mgr_node->nodename), restmsg.len != 0 ? restmsg.data:"fail")));
				appendStringInfo(&resultmsg, "promote \"%s\" fail %s\n", NameStr(mgr_node->nodename), restmsg.data);
			}
		}

		if (!agtmHost)
			ereport(ERROR, (errmsg("gtm slave does not exist in zone %s", currentZone)));

		ereport(LOG, (errmsg("refresh agtm_host, agtm_port in zone \"%s\"", currentZone)));
		ereport(NOTICE, (errmsg("refresh agtm_host, agtm_port in zone \"%s\"", currentZone)));
		if (!mgr_zone_modify_conf_agtm_host_port(currentZone, agtmPort, agtmHost))
		{
			ereport(WARNING, (errmsg("refresh agtm_host, agtm_port in zone \"%s\" fail", currentZone)));
			appendStringInfo(&resultmsg, "refresh agtm_host, agtm_port in zone %s fail, check agtm_port=%d, \
			agtm_host=%s in postgresql.conf of all coordinators and datanodes\n"
			, currentZone, agtmPort, agtmHost);
		}

		heap_endscan(relScan);
		relScan = heap_beginscan_catalog(relNode, 2, key);
		while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			if (mgr_checknode_in_currentzone(currentZone, mgr_node->nodemasternameoid))
				continue;
			if (mgr_node->nodetype == GTM_TYPE_GTM_MASTER)
				continue;
			mgr_node->nodetype = mgr_get_master_type(mgr_node->nodetype);
			heap_inplace_update(relNode, tuple);
			/* get node path */
			datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(relNode), &isNull);
			if(isNull)
			{
				heap_endscan(relScan);
				heap_close(relNode, RowExclusiveLock);
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
					, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
					, errmsg("column nodepath is null")));
			}
			nodePath = TextDatumGetCString(datumPath);
			resetStringInfo(&infosendmsg);
			resetStringInfo(&restmsg);
			appendStringInfo(&infosendmsg, " promote -D %s", nodePath);
			mgr_ma_send_cmd_get_original_result(AGT_CMD_DN_FAILOVER, infosendmsg.data, mgr_node->nodehost, &restmsg, AGENT_RESULT_LOG);
			if (restmsg.len != 0 && strstr(restmsg.data, "server promoted") != NULL)
			{
				ereport(LOG, (errmsg("promote \"%s\" %s", NameStr(mgr_node->nodename), restmsg.data)));
			}
			else
			{
				bres = false;
				ereport(WARNING, (errmsg("promote \"%s\" %s", NameStr(mgr_node->nodename), restmsg.len != 0 ? restmsg.data:"fail")));
				appendStringInfo(&resultmsg, "promote \"%s\" fail %s\n", NameStr(mgr_node->nodename), restmsg.data);
			}
		}

		/* check the node has allow connect */
		heap_endscan(relScan);
		relScan = heap_beginscan_catalog(relNode, 2, key);
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
				if (mgr_node->nodetype != GTM_TYPE_GTM_MASTER && mgr_node->nodetype != GTM_TYPE_GTM_SLAVE)
					userName = get_hostuser_from_hostoid(mgr_node->nodehost);
				else
					userName = NULL;
				i = 0;
				while(i++<3)
				{
					if (pingNode_user(hostAddr, nodePortBuf, userName == NULL ? AGTM_USER : userName) == 0)
						break;
					pg_usleep(1 * 1000000L);
				}

				if (pingNode_user(hostAddr, nodePortBuf, userName == NULL ? AGTM_USER : userName) != 0)
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

			pfree(hostAddr);
		}
	}PG_CATCH();
	{
		pfree(infosendmsg.data);
		pfree(restmsg.data);
		pfree(resultmsg.data);
		if (agtmHost)
			pfree(agtmHost);
		PG_RE_THROW();
	}PG_END_TRY();

	heap_endscan(relScan);
	heap_close(relNode, RowExclusiveLock);

	pfree(infosendmsg.data);
	pfree(restmsg.data);
	if (agtmHost)
		pfree(agtmHost);

	ereport(LOG, (errmsg("the command of \"ZONE PROMOTE %s\" result is %s, description is %s", currentZone
		, bres ? "true":"false", resultmsg.len == 0 ? "success":resultmsg.data)));
	namestrcpy(&name, "promote master node");
	tupResult = build_common_command_tuple(&name, bres, resultmsg.len == 0 ? "success":resultmsg.data);
	pfree(resultmsg.data);

	return HeapTupleGetDatum(tupResult);
}

/*
* mgr_zone_config_all
*
* refresh pgxc_node on all coordinators in the given zone 
*
*/
Datum mgr_zone_config_all(PG_FUNCTION_ARGS)
{
	Relation relNode;
	HeapTuple tuple;
	HeapTuple tupleout;
	HeapTuple tuplein;
	HeapTuple masterTuple;
	HeapTuple tupResult;
	HeapScanDesc relScan;
	HeapScanDesc relScanout;
	HeapScanDesc relScanin;
	Form_mgr_node mgr_node;
	Form_mgr_node mgr_node_out;
	Form_mgr_node mgr_node_in;
	Form_mgr_node mgr_nodeM;
	StringInfoData infosendmsg;
	StringInfoData infosendmsgdn;
	StringInfoData infosendmsgdns;
	StringInfoData restmsg;
	StringInfoData resultmsg;
	GetAgentCmdRst getAgentCmdRst;
	ScanKeyData key[3];
	NameData name;
	NameData cnName;
	NameData cnNameM;
	NameData selfAddress;
	Datum datumPath;
	List *newNameList = NIL;
	List *oldNameList = NIL;
	ListCell   *newceil;
	ListCell   *oldceil;
	PGconn *pgConn;
	char *cnUserName;
	char *hostAddress;
	char *cnHostAddress;
	char *currentZone;
	char *cndnPath;
	char *newname;
	char *oldname;
	char coordPortBuf[10];
	int i = 0;
	Oid coordHostOid;
	bool bres = true;
	bool isNull = false;
	bool bDnMaster = false;
	bool bgetAddress = false;
	bool bexpandStatus = false;

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot do the command during recovery")));

	currentZone  = PG_GETARG_CSTRING(0);
	Assert(currentZone);

	if (strcmp(currentZone, mgr_zone) !=0)
		ereport(ERROR, (errmsg("the given zone name \"%s\" is not the same wtih guc parameter mgr_zone \"%s\" in postgresql.conf", currentZone, mgr_zone)));
	if (!mgr_zone_has_node(currentZone, GTM_TYPE_GTM_MASTER))
		ereport(ERROR, (errmsg("the zone \"%s\" has not GTM MASTER in cluster", currentZone)));
	if (!mgr_zone_has_node(currentZone, CNDN_TYPE_COORDINATOR_MASTER))
		ereport(ERROR, (errmsg("the zone \"%s\" has not COORDINATOR MASTER in cluster", currentZone)));
	if (!mgr_zone_has_node(currentZone, CNDN_TYPE_DATANODE_MASTER))
		ereport(ERROR, (errmsg("the zone \"%s\" has not DATANODE MASTER in cluster", currentZone)));

	mgr_make_sure_all_running(GTM_TYPE_GTM_MASTER);
	mgr_make_sure_all_running(CNDN_TYPE_COORDINATOR_MASTER);
	mgr_make_sure_all_running(CNDN_TYPE_DATANODE_MASTER);

	ereport(LOG, (errmsg("in zone \"%s\"", currentZone)));
	ereport(NOTICE, (errmsg("in zone \"%s\"", currentZone)));

	initStringInfo(&infosendmsg);
	initStringInfo(&infosendmsgdn);
	initStringInfo(&infosendmsgdns);
	initStringInfo(&restmsg);
	initStringInfo(&resultmsg);
	initStringInfo(&(getAgentCmdRst.description));

	ereport(LOG, (errmsg("in zone %s refresh pgxc_node on all coordinators", currentZone)));
	ereport(NOTICE, (errmsg("in zone %s refresh pgxc_node on all coordinators", currentZone)));
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

	relNode = heap_open(NodeRelationId, RowExclusiveLock);
	relScanout = heap_beginscan_catalog(relNode, 2, key);
	PG_TRY();
	{
		mgr_get_active_node(&cnName, CNDN_TYPE_COORDINATOR_MASTER, InvalidOid);
		tuple = mgr_get_tuple_node_from_name_type(relNode, NameStr(cnName));
		if (!HeapTupleIsValid(tuple))
		{
			heap_endscan(relScanout);
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				,errmsg("get tuple information of coordinator \"%s\" fail", NameStr(cnName))));
		}
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		coordHostOid = mgr_node->nodehost;
		cnUserName = get_hostuser_from_hostoid(coordHostOid);
		cnHostAddress = get_hostaddress_from_hostoid(coordHostOid);
		sprintf(coordPortBuf, "%d", mgr_node->nodeport);

		masterTuple = SearchSysCache1(NODENODEOID
			, ObjectIdGetDatum(mgr_node->nodemasternameoid));
		if(!HeapTupleIsValid(masterTuple))
		{
			heap_endscan(relScanout);
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				, errmsg("cache lookup failed for the master of \"%s\" in zone \"%s\""
					, NameStr(mgr_node->nodename), currentZone)));
		}
		mgr_nodeM = (Form_mgr_node)GETSTRUCT(masterTuple);
		Assert(mgr_nodeM);
		namestrcpy(&cnNameM, NameStr(mgr_nodeM->nodename));
		ReleaseSysCache(masterTuple);

		pgConn = PQsetdbLogin(cnHostAddress, coordPortBuf, NULL, NULL, DEFAULT_DB, cnUserName, NULL);
		if (PQstatus((PGconn*)pgConn) != CONNECTION_OK)
		{
			PQfinish(pgConn);
			resetStringInfo(&infosendmsg);
			resetStringInfo(&(getAgentCmdRst.description));
			memset(selfAddress.data, 0, NAMEDATALEN);
			datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(relNode), &isNull);
			cndnPath = TextDatumGetCString(datumPath);
			bgetAddress = mgr_get_self_address(cnHostAddress, mgr_node->nodeport, &selfAddress);
			if (!bgetAddress)
				ereport(ERROR, (errmsg("on ADB Manager get local address fail")));
			mgr_add_oneline_info_pghbaconf(CONNECT_HOST, DEFAULT_DB, cnUserName, selfAddress.data
				, 32, "trust", &infosendmsg);
			mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF, cndnPath, &infosendmsg
				, coordHostOid, &getAgentCmdRst);
			mgr_reload_conf(coordHostOid, cndnPath);
			if (!getAgentCmdRst.ret)
			{
				ereport(ERROR, (errmsg("set ADB Manager ip \"%s\" to %s coordinator %s/pg_hba,conf fail %s"
				, selfAddress.data, cnHostAddress, cndnPath, getAgentCmdRst.description.data)));
			}
			pgConn = PQsetdbLogin(cnHostAddress, coordPortBuf, NULL, NULL, DEFAULT_DB, cnUserName, NULL);
			if (PQstatus((PGconn*)pgConn) != CONNECTION_OK)
				ereport(ERROR, (errmsg("mgr connect to active coordinator master \"%s\" fail", NameStr(cnName))));
		}
		if(hexp_check_select_result_count(pgConn, SELECT_ADB_SLOT_TABLE_COUNT))
			bexpandStatus = true;

		hexp_pqexec_direct_execute_utility(pgConn,SQL_BEGIN_TRANSACTION
			, MGR_PGEXEC_DIRECT_EXE_UTI_RET_COMMAND_OK);

		/* update coordinator pgxc_node */
		resetStringInfo(&infosendmsg);
		while((tupleout = heap_getnext(relScanout, ForwardScanDirection)) != NULL)
		{
			mgr_node_out = (Form_mgr_node)GETSTRUCT(tupleout);
			Assert(mgr_node_out);
			if (mgr_node_out->nodetype != CNDN_TYPE_COORDINATOR_MASTER
				&& mgr_node_out->nodetype != CNDN_TYPE_DATANODE_MASTER)
				continue;
			if (mgr_checknode_in_currentzone(currentZone, mgr_node_out->nodemasternameoid))
				continue;
			hostAddress = get_hostaddress_from_hostoid(mgr_node_out->nodehost);
			masterTuple = SearchSysCache1(NODENODEOID
				, ObjectIdGetDatum(mgr_node_out->nodemasternameoid));
			if(!HeapTupleIsValid(masterTuple))
			{
				heap_endscan(relScanout);
				ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					, errmsg("cache lookup failed for the master of \"%s\" in zone \"%s\"", NameStr(mgr_node_out->nodename), currentZone)));
			}
			mgr_nodeM = (Form_mgr_node)GETSTRUCT(masterTuple);
			Assert(mgr_nodeM);
			if (CNDN_TYPE_DATANODE_MASTER == mgr_node_out->nodetype)
			{
				newNameList = lappend(newNameList, NameStr(mgr_node_out->nodename));
				oldNameList = lappend(oldNameList, NameStr(mgr_nodeM->nodename));
			}
			appendStringInfo(&infosendmsg
				, "alter node \"%s\" with(name='%s', host='%s', port=%d, preferred=false) on (\"%s\");"
				,NameStr(mgr_nodeM->nodename), NameStr(mgr_node_out->nodename)
				, hostAddress, mgr_node_out->nodeport
				,NameStr(cnNameM));
			ReleaseSysCache(masterTuple);
			pfree(hostAddress);

		}
		heap_endscan(relScanout);
		/* execute command */
		hexp_pqexec_direct_execute_utility(pgConn
			, infosendmsg.data, MGR_PGEXEC_DIRECT_EXE_UTI_RET_COMMAND_OK);

		/* out the other coordinator */
		relScanout = heap_beginscan_catalog(relNode, 2, key);
		while((tupleout = heap_getnext(relScanout, ForwardScanDirection)) != NULL)
		{
			mgr_node_out = (Form_mgr_node)GETSTRUCT(tupleout);
			Assert(mgr_node_out);
			if (mgr_node_out->nodetype != CNDN_TYPE_COORDINATOR_MASTER)
				continue;
			if (strcmp(NameStr(mgr_node_out->nodename), NameStr(cnName)) == 0)
				continue;
			if (mgr_checknode_in_currentzone(currentZone, mgr_node_out->nodemasternameoid))
				continue;
			resetStringInfo(&infosendmsg);
			relScanin = heap_beginscan_catalog(relNode, 2, key);
			while((tuplein = heap_getnext(relScanin, ForwardScanDirection)) != NULL)
			{
				mgr_node_in = (Form_mgr_node)GETSTRUCT(tuplein);
				Assert(mgr_node_in);
				if (mgr_node_in->nodetype != CNDN_TYPE_COORDINATOR_MASTER
					&& mgr_node_in->nodetype != CNDN_TYPE_DATANODE_MASTER)
					continue;
				if (mgr_checknode_in_currentzone(currentZone, mgr_node_in->nodemasternameoid))
					continue;
				hostAddress = get_hostaddress_from_hostoid(mgr_node_in->nodehost);
				masterTuple = SearchSysCache1(NODENODEOID
					, ObjectIdGetDatum(mgr_node_in->nodemasternameoid));
				if(!HeapTupleIsValid(masterTuple))
				{
					heap_endscan(relScanin);
					ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
						, errmsg("cache lookup failed for the master of \"%s\" in zone \"%s\"", NameStr(mgr_node_in->nodename), currentZone)));
				}
				mgr_nodeM = (Form_mgr_node)GETSTRUCT(masterTuple);
				Assert(mgr_nodeM);
				appendStringInfo(&infosendmsg
					, "alter node \"%s\" with(name='%s', host='%s', port=%d, preferred=false) on (\"%s\");"
					,NameStr(mgr_nodeM->nodename), NameStr(mgr_node_in->nodename)
					, hostAddress, mgr_node_in->nodeport
					,NameStr(mgr_node_out->nodename));
				ReleaseSysCache(masterTuple);
				pfree(hostAddress);
			}
			heap_endscan(relScanin);
			hexp_pqexec_direct_execute_utility(pgConn
				, infosendmsg.data, MGR_PGEXEC_DIRECT_EXE_UTI_RET_COMMAND_OK);
		}
		heap_endscan(relScanout);

		/* update datanode master pgxc_node */
		if (!bexpandStatus)
		{
			relScanin = heap_beginscan_catalog(relNode, 2, key);
			while((tuplein = heap_getnext(relScanin, ForwardScanDirection)) != NULL)
			{
				mgr_node_in = (Form_mgr_node)GETSTRUCT(tuplein);
				Assert(mgr_node_in);
				if (mgr_node_in->nodetype != CNDN_TYPE_DATANODE_MASTER)
					continue;
				if (mgr_checknode_in_currentzone(currentZone, mgr_node_in->nodemasternameoid))
					continue;
				resetStringInfo(&infosendmsg);
				hostAddress = get_hostaddress_from_hostoid(mgr_node_in->nodehost);
				masterTuple = SearchSysCache1(NODENODEOID
					, ObjectIdGetDatum(mgr_node_in->nodemasternameoid));
				if(!HeapTupleIsValid(masterTuple))
				{
					heap_endscan(relScanin);
					ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
						, errmsg("cache lookup failed for the master of \"%s\" in zone \"%s\"", NameStr(mgr_node_in->nodename), currentZone)));
				}
				mgr_nodeM = (Form_mgr_node)GETSTRUCT(masterTuple);
				Assert(mgr_nodeM);
				appendStringInfo(&infosendmsg
					, "alter node \"%s\" with(name='%s', host='%s', port=%d, preferred=false) on (\"%s\");"
					,NameStr(mgr_nodeM->nodename), NameStr(mgr_node_in->nodename)
					, hostAddress, mgr_node_in->nodeport
					,NameStr(mgr_node_in->nodename));
				ReleaseSysCache(masterTuple);
				pfree(hostAddress);
				hexp_pqexec_direct_execute_utility(pgConn
					, infosendmsg.data, MGR_PGEXEC_DIRECT_EXE_UTI_RET_COMMAND_OK);
			}
			heap_endscan(relScanin);
		}
		else
		{
			relScanout = heap_beginscan_catalog(relNode, 2, key);
			while((tupleout = heap_getnext(relScanout, ForwardScanDirection)) != NULL)
			{
				mgr_node_out = (Form_mgr_node)GETSTRUCT(tupleout);
				Assert(mgr_node_out);
				if (mgr_node_out->nodetype != CNDN_TYPE_DATANODE_MASTER)
					continue;
				if (strcmp(NameStr(mgr_node_out->nodename), NameStr(cnName)) == 0)
					continue;
				if (mgr_checknode_in_currentzone(currentZone, mgr_node_out->nodemasternameoid))
					continue;
				resetStringInfo(&infosendmsg);
				relScanin = heap_beginscan_catalog(relNode, 2, key);
				while((tuplein = heap_getnext(relScanin, ForwardScanDirection)) != NULL)
				{
					mgr_node_in = (Form_mgr_node)GETSTRUCT(tuplein);
					Assert(mgr_node_in);
					if (mgr_node_in->nodetype != CNDN_TYPE_DATANODE_MASTER)
						continue;
					if (mgr_checknode_in_currentzone(currentZone, mgr_node_in->nodemasternameoid))
						continue;
					hostAddress = get_hostaddress_from_hostoid(mgr_node_in->nodehost);
					masterTuple = SearchSysCache1(NODENODEOID
						, ObjectIdGetDatum(mgr_node_in->nodemasternameoid));
					if(!HeapTupleIsValid(masterTuple))
					{
						heap_endscan(relScanin);
						ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
							, errmsg("cache lookup failed for the master of \"%s\" in zone \"%s\"", NameStr(mgr_node_in->nodename), currentZone)));
					}
					mgr_nodeM = (Form_mgr_node)GETSTRUCT(masterTuple);
					Assert(mgr_nodeM);
					appendStringInfo(&infosendmsg
						, "alter node \"%s\" with(name='%s', host='%s', port=%d, preferred=false) on (\"%s\");"
						,NameStr(mgr_nodeM->nodename), NameStr(mgr_node_in->nodename)
						, hostAddress, mgr_node_in->nodeport
						,NameStr(mgr_node_out->nodename));
					ReleaseSysCache(masterTuple);
					pfree(hostAddress);
				}
				heap_endscan(relScanin);
				hexp_pqexec_direct_execute_utility(pgConn
					, infosendmsg.data, MGR_PGEXEC_DIRECT_EXE_UTI_RET_COMMAND_OK);
			}
			heap_endscan(relScanout);
		}

		hexp_pqexec_direct_execute_utility(pgConn,SQL_COMMIT_TRANSACTION
			, MGR_PGEXEC_DIRECT_EXE_UTI_RET_COMMAND_OK);
		PQfinish(pgConn);

		ereport(LOG, (errmsg("in zone %s refresh pgxc_node_name on all coordinator masters and datanode masters then restart", currentZone)));
		ereport(NOTICE, (errmsg("in zone %s refresh pgxc_node_name on all coordinator masters and datanode masters  then restart", currentZone)));
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
		relScan = heap_beginscan_catalog(relNode, 2, key);
		while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			if (mgr_node->nodetype != CNDN_TYPE_COORDINATOR_MASTER
				&& mgr_node->nodetype != CNDN_TYPE_DATANODE_MASTER)
				continue;
			if (mgr_node->nodetype != CNDN_TYPE_DATANODE_MASTER)
				bDnMaster = false;
			else
				bDnMaster = true;
			ereport(LOG, (errmsg("in zone %s set pgxc_node_name='%s' for %s master \"%s\""
				, currentZone, NameStr(mgr_node->nodename)
				, bDnMaster ? "datanode" : "coordinator"
				, NameStr(mgr_node->nodename))));
			ereport(NOTICE, (errmsg("in zone %s set pgxc_node_name='%s' for %s master \"%s\""
				, currentZone, NameStr(mgr_node->nodename)
				, bDnMaster ? "datanode" : "coordinator"
				, NameStr(mgr_node->nodename))));
			resetStringInfo(&infosendmsg);
			resetStringInfo(&(getAgentCmdRst.description));
			datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(relNode), &isNull);
			if(isNull)
			{
				bres = false;
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
					, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
					, errmsg("column cndnpath is null")));
				continue;
			}
			cndnPath = TextDatumGetCString(datumPath);
			mgr_append_pgconf_paras_str_quotastr("pgxc_node_name", NameStr(mgr_node->nodename), &infosendmsg);
			mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, cndnPath, &infosendmsg
				, mgr_node->nodehost, &getAgentCmdRst);
			if (!getAgentCmdRst.ret)
			{
				bres = false;
				ereport(WARNING, (errmsg("in zone %s set pgxc_node_name='%s' for %s master \"%s\" fail %s"
					, currentZone, NameStr(mgr_node->nodename)
					, bDnMaster ? "datanode" : "coordinator"
					, NameStr(mgr_node->nodename)
					, getAgentCmdRst.description.data)));
				appendStringInfo(&resultmsg, "in zone %s set pgxc_node_name='%s' for %s master \"%s\" fail %s\n"
					, currentZone, NameStr(mgr_node->nodename)
					, bDnMaster ? "datanode" : "coordinator"
					, NameStr(mgr_node->nodename)
					, getAgentCmdRst.description.data);
			}

			/* restart the coordinator/datanode master */
			ereport(LOG, (errmsg("in zone %s, pg_ctl restart %s master \"%s\""
				, currentZone
				, mgr_node->nodetype == CNDN_TYPE_COORDINATOR_MASTER ? "coordinator":"datanode"
				, NameStr(mgr_node->nodename))));
			ereport(NOTICE, (errmsg("in zone %s, pg_ctl restart %s master \"%s\""
				, currentZone
				, bDnMaster ? "datanode" : "coordinator"
				, NameStr(mgr_node->nodename))));
			resetStringInfo(&(getAgentCmdRst.description));
			mgr_runmode_cndn_get_result(bDnMaster ? AGT_CMD_DN_RESTART : AGT_CMD_CN_RESTART, &getAgentCmdRst, relNode, tuple, SHUTDOWN_F);
			if(!getAgentCmdRst.ret)
			{
				bres = false;
				ereport(WARNING, (errmsg("in zone %s restart %s master \"%s\" fail %s"
					, currentZone
					, bDnMaster ? "datanode" : "coordinator"
					, NameStr(mgr_node->nodename)
					, getAgentCmdRst.description.data)));
				appendStringInfo(&resultmsg, "in zone %s restart %s master \"%s\" fail %s\n"
					, currentZone
					, bDnMaster ? "datanode" : "coordinator"
					, NameStr(mgr_node->nodename)
					, getAgentCmdRst.description.data);
			}
		}
		heap_endscan(relScan);

		if(bexpandStatus)
		{
			ereport(LOG, (errmsg("flush adb_slot on coordinator master \"%s\"", cnName.data)));
			ereport(NOTICE, (errmsg("flush adb_slot on coordinator master \"%s\"", cnName.data)));
			i = 0;
			while(i++<3)
			{
				pgConn = PQsetdbLogin(cnHostAddress, coordPortBuf, NULL, NULL, DEFAULT_DB, cnUserName, NULL);
				if (PQstatus((PGconn*)pgConn) == CONNECTION_OK)
					break;
				else
					PQfinish(pgConn);
				if (i == 3)
					ereport(ERROR, (errmsg("mgr connect to active coordinator master \"%s\" fail", NameStr(cnName))));
			}
			forboth(newceil, newNameList, oldceil, oldNameList)
			{
				newname = (char *)lfirst(newceil);
				oldname = (char *)lfirst(oldceil);
				hexp_alter_slotinfo_nodename_noflush(pgConn, oldname, newname, true, true);
			}
			hexp_pqexec_direct_execute_utility(pgConn, "flush slot;", MGR_PGEXEC_DIRECT_EXE_UTI_RET_COMMAND_OK);
			list_free(newNameList);
			list_free(oldNameList);
			pfree(cnUserName);
			pfree(cnHostAddress);
			PQfinish(pgConn);
		}

		if (bres)
		{
			ereport(LOG, (errmsg("in zone %s make the node of master type clear: the column mastername is null, sync_stat is null", currentZone)));
			ereport(NOTICE, (errmsg("in zone %s make the node of master type clear: the column mastername is null, sync_stat is null", currentZone)));
			ScanKeyInit(&key[0]
				,Anum_mgr_node_nodezone
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,CStringGetDatum(currentZone));
			relScan = heap_beginscan_catalog(relNode, 1, key);
			while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
			{
				mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
				Assert(mgr_node);
				if (CNDN_TYPE_COORDINATOR_MASTER == mgr_node->nodetype 
					|| CNDN_TYPE_DATANODE_MASTER == mgr_node->nodetype
					|| GTM_TYPE_GTM_MASTER == mgr_node->nodetype)
				{
					namestrcpy(&(mgr_node->nodesync), "");
					mgr_node->nodemasternameoid = 0;
					heap_inplace_update(relNode, tuple);
				}
			}

			heap_endscan(relScan);
			ereport(LOG, (errmsg("on node table, drop the node which is not in zone \"%s\"", currentZone)));
			ereport(NOTICE, (errmsg("on node table, drop the node which is not in zone \"%s\"", currentZone)));
			relScan = heap_beginscan_catalog(relNode, 0, NULL);
			while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
			{
				mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
				Assert(mgr_node);
				if (strcasecmp(NameStr(mgr_node->nodezone), currentZone) == 0)
					continue;
				CatalogTupleDelete(relNode, &(tuple->t_self));
			}
			heap_endscan(relScan);
		}
	}PG_CATCH();
	{
		heap_close(relNode, RowExclusiveLock);
		pfree(infosendmsg.data);
		pfree(infosendmsgdn.data);
		pfree(infosendmsgdns.data);
		pfree(restmsg.data);
		pfree(resultmsg.data);
		pfree(getAgentCmdRst.description.data);
		PG_RE_THROW();
	}PG_END_TRY();

	heap_close(relNode, RowExclusiveLock);
	pfree(infosendmsg.data);
	pfree(infosendmsgdn.data);
	pfree(infosendmsgdns.data);
	pfree(restmsg.data);
	pfree(getAgentCmdRst.description.data);

	ereport(LOG, (errmsg("the command of \"ZONE CONFIG %s\" result is %s, description is %s", currentZone
		,bres ? "true":"false", resultmsg.len == 0 ? "success":resultmsg.data)));
	namestrcpy(&name, "config all");
	tupResult = build_common_command_tuple(&name, bres, resultmsg.len == 0 ? "success":resultmsg.data);
	pfree(resultmsg.data);

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
* mgr_zone_modify_conf_agtm_host_port
* 
* refresh agtm_port, agtm_host in all the node in zone
*
*/
static bool mgr_zone_modify_conf_agtm_host_port(const char *zone, int agtmPort, char *agtmHost)
{
	Relation relNode;
	HeapTuple tuple;
	HeapScanDesc relScan;
	StringInfoData infosendmsg;
	GetAgentCmdRst getAgentCmdRst;
	Datum datumPath;
	ScanKeyData key[2];
	Form_mgr_node mgr_node;
	bool isNull;
	bool res = true;
	char *cndnPath;

	Assert(agtmHost);
	initStringInfo(&infosendmsg);
	/* get agtm_host, agtm_port */
	mgr_append_pgconf_paras_str_quotastr("agtm_host", agtmHost, &infosendmsg);
	mgr_append_pgconf_paras_str_int("agtm_port", agtmPort, &infosendmsg);
	
	initStringInfo(&(getAgentCmdRst.description));
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
	relNode = heap_open(NodeRelationId, AccessShareLock);
	relScan = heap_beginscan_catalog(relNode, 2, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if (GTM_TYPE_GTM_MASTER == mgr_node->nodetype || GTM_TYPE_GTM_SLAVE == mgr_node->nodetype)
			continue;
		resetStringInfo(&(getAgentCmdRst.description));
		datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(relNode), &isNull);
		if(isNull)
		{
			pfree(infosendmsg.data);
			pfree(getAgentCmdRst.description.data);
			heap_endscan(relScan);
			heap_close(relNode, AccessShareLock);
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
				, errmsg("column cndnpath is null")));
		}
		cndnPath = TextDatumGetCString(datumPath);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, cndnPath, &infosendmsg, mgr_node->nodehost, &getAgentCmdRst);
		if (!getAgentCmdRst.ret)
		{
			res = false;
			ereport(WARNING, (errmsg("on %s set agtm_host, agtm_port fail %s", NameStr(mgr_node->nodename), getAgentCmdRst.description.data)));
		}
	}
	heap_endscan(relScan);
	heap_close(relNode, AccessShareLock);
	pfree(infosendmsg.data);
	pfree(getAgentCmdRst.description.data);
	
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
* mgr_zone_clear
*
* clear the tuple which is not in the current zone
*/

Datum mgr_zone_clear(PG_FUNCTION_ARGS)
{
	Relation relNode;
	HeapScanDesc relScan;
	ScanKeyData key[2];
	HeapTuple tuple =NULL;
	Form_mgr_node mgr_node;
	char *zone;
	char *nodetypestr;

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot do the command during recovery")));

	zone  = PG_GETARG_CSTRING(0);
	Assert(zone);

	if (strcmp(zone, mgr_zone) !=0)
		ereport(ERROR, (errmsg("the given zone name \"%s\" is not the same wtih guc parameter mgr_zone \"%s\" in postgresql.conf", zone, mgr_zone)));

	ereport(LOG, (errmsg("make the special node as master type and set its master name is null, sync_state is null on node table in zone \"%s\"", zone)));
	ereport(NOTICE, (errmsg("make the special node as master type and set its master name is null, sync_state is null on node table in zone \"%s\"", zone)));
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

	relNode = heap_open(NodeRelationId, RowExclusiveLock);
	relScan = heap_beginscan_catalog(relNode, 2, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if (mgr_checknode_in_currentzone(zone, mgr_node->nodemasternameoid))
			continue;
		ereport(LOG, (errmsg("make the node \"%s\" as master type on node table in zone \"%s\"", NameStr(mgr_node->nodename), zone)));
		ereport(NOTICE, (errmsg("make the node \"%s\" as master type on node table in zone \"%s\"", NameStr(mgr_node->nodename), zone)));
		mgr_node->nodetype = mgr_get_master_type(mgr_node->nodetype);
		namestrcpy(&(mgr_node->nodesync), "");
		mgr_node->nodemasternameoid = 0;
		heap_inplace_update(relNode, tuple);
	}
	heap_endscan(relScan);

	ereport(LOG, (errmsg("on node table, drop the node which is not in zone \"%s\"", zone)));
	ereport(NOTICE, (errmsg("on node table, drop the node which is not in zone \"%s\"", zone)));
	relScan = heap_beginscan_catalog(relNode, 0, NULL);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if (strcmp(NameStr(mgr_node->nodezone), zone) == 0)
			continue;
		nodetypestr = mgr_nodetype_str(mgr_node->nodetype);
		ereport(LOG, (errmsg("drop %s \"%s\" on node table in zone \"%s\"", nodetypestr, NameStr(mgr_node->nodename), NameStr(mgr_node->nodezone))));
		ereport(NOTICE, (errmsg("drop %s \"%s\" on node table in zone \"%s\"", nodetypestr, NameStr(mgr_node->nodename), NameStr(mgr_node->nodezone))));
		pfree(nodetypestr);
		CatalogTupleDelete(relNode, &(tuple->t_self));
	}

	heap_endscan(relScan);
	heap_close(relNode, RowExclusiveLock);

	PG_RETURN_BOOL(true);
}

/*
* mgr_node_has_slave_inzone
*
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

/*
* mgr_zone_has_node
*
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
