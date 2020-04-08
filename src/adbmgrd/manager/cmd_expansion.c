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
#include "catalog/pgxc_node.h"
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
#include <stdlib.h>
#include "access/sysattr.h"
#include "access/xlog.h"

char *MGRDatabaseName = NULL;

#define ExpandStatusExpanding  	"Expanding"
#define ExpandStatusOnline  	"Online"
#define SELECT_LAST_LSN 		"SELECT pg_last_wal_replay_lsn();"
#define SELECT_CUR_LSN			"SELECT pg_current_wal_lsn();"
#define SELECT_DBNAME_ADBCLEAN  "SELECT DISTINCT datname FROM pg_database,adb_clean WHERE pg_database.oid=adb_clean.clndb;"

typedef struct DN_STATUS
{
	NameData	nodename;
	Oid			tid;
	Oid			nodemasternameoid;
	bool		nodeincluster;
	NameData 	node_status;
	NameData	pgxc_node_name;
} DN_STATUS;

typedef struct SRC_DST_NODENAME
{
	NameData	srcnodename;
	NameData	dstnodename;
	bool        used;
}SRC_DST_NODENAME; 

/*hot expansion definition end*/
static bool hexp_get_nodeinfo_from_table(char *node_name, char node_type, AppendNodeInfo *nodeinfo);
static void hexp_create_dm_on_all_coord(PGconn *pg_conn, AppendNodeInfo *nodeinfo);
static void hexp_create_dm_on_itself(PGconn *pg_conn, AppendNodeInfo *nodeinfo);
static void hexp_set_expended_node_state(char *nodename, bool search_init, bool search_incluster, bool value_init, bool value_incluster, Oid src_oid);
static bool hexp_get_nodeinfo_from_table_byoid(Oid tupleOid, AppendNodeInfo *nodeinfo);
static void hexp_mgr_pqexec_getlsn(PGconn *pg_conn, char *sqlstr, int* phvalue, int* plvalue);
static void hexp_parse_pair_lsn(char* strvalue, int* phvalue, int* plvalue);
static List *hexp_get_all_dn_status(void);
static void hexp_get_dn_status(Form_mgr_node mgr_node, Oid tuple_id, DN_STATUS* pdn_status, char* cnpath);
static void hexp_get_dn_conn(PGconn **pg_conn, Form_mgr_node mgr_node, char* cnpath);
static void hexp_update_conf_pgxc_node_name(AppendNodeInfo *node, char* newname);
static void hexp_restart_node(AppendNodeInfo *node);
static void hexp_pgxc_pool_reload_on_all_node(PGconn *pg_conn);
static void hexp_get_allnodes_serialize(StringInfoData *pserialize);
static void hexp_check_expand(void);
static void MgrSetAppendNodeInfo(AppendNodeInfo  *nodeinfo, Form_mgr_node mgr_node, HeapTuple tuple, InitNodeInfo *info);
static List* MgrGetAllAppendNodeInfo(void);
static bool MgrGetConn(char* database, AppendNodeInfo *node, PGconn **pg_conn, StringInfoData  *infosendmsg);
static void MgrCheckSynaLsn(PGconn * src_pg_conn, PGconn * dst_pg_conn);
static void MgrCheckRestartRunning(AppendNodeInfo *dst_node);
static List* MgrActivateStep1(char* database, List *nodeinfo_list);
static void MgrCreateGtmSql(List *src_dst_list, char *nodes_slq, int len);
static void MgrActivateStep2(PGconn *gtm_conn, List *dst_node_list, List *src_dst_list);
static HeapTuple MgrGetTupleResult(List *nodeinfo_list);
static void MgrFreeNodeList(List *dst_node_list, List *src_dst_list);
static List* MgrGetAdbCleanDbName(PGconn *pg_conn);
static void MgrFreeDBName(List *dbname_list);
static void MgrFreeClean(PGconn *co_pg_conn, PGconn *other_conn, List *dbname_list);
/*
 * expand sourcenode to destnode
 */
Datum mgr_expand_activate_dnmaster(PG_FUNCTION_ARGS)
{
	NameData 	nodename;
	HeapTuple 	tup_result = NULL;
	char		*database ;
	List 		*dst_node_list = NIL;
	List 	    *src_dst_list  = NIL;
	PGconn 		*gtm_conn = NULL;
	Oid 		cnoid;
	int			finish_try = 3;

	namestrcpy(&nodename, "all node");

    if(0!=strcmp(MGRDatabaseName,""))
		database = MGRDatabaseName;
	else
		database = DEFAULT_DB;

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot execute this command during recovery")));

	mgr_make_sure_all_running(CNDN_TYPE_COORDINATOR_MASTER);
	mgr_make_sure_all_running(CNDN_TYPE_DATANODE_MASTER);

	hexp_check_expand();

	if ((dst_node_list = MgrGetAllAppendNodeInfo()) == NIL)
	{
		tup_result = build_common_command_tuple(&nodename, true, " no node need activate");
		return HeapTupleGetDatum(tup_result);
	}
retry:
	/* lock cluster */
	PG_TRY();
	{
		mgr_lock_cluster_involve_gtm_coord(&gtm_conn, &cnoid);
		/* finish active client backend connect */		
		MgrSendFinishActiveBackendToGtm(gtm_conn);
	}
	PG_CATCH();
	{
		mgr_unlock_cluster_involve_gtm_coord(&gtm_conn);
		if (finish_try)
		{
			ereport(LOG, (errmsg("Failed to execute \"FINISH ACTIVE BACKEND\", Try again now.")));
			finish_try --;
			pg_usleep(2000000L);
			goto retry;
		}
		ereport(ERROR,
			(errmsg("End active backend failed, please try to reactivate the expansion.")));
	}
	PG_END_TRY();

	if ((src_dst_list = MgrActivateStep1(database, dst_node_list)) == NIL)
	{
		tup_result = build_common_command_tuple(&nodename, true, " no node need activate");
		return HeapTupleGetDatum(tup_result);
	}
	
	MgrActivateStep2(gtm_conn, dst_node_list, src_dst_list);	

	tup_result = MgrGetTupleResult(dst_node_list);
	/* unlock cluster */
	mgr_unlock_cluster_involve_gtm_coord(&gtm_conn);
	MgrFreeNodeList(dst_node_list, src_dst_list);

	return HeapTupleGetDatum(tup_result);
}
/*
 * expand sourcenode to destnode
 */
Datum mgr_expand_activate_recover_promote_suc(PG_FUNCTION_ARGS)
{
	AppendNodeInfo appendnodeinfo;
	AppendNodeInfo srcnodeinfo;
	StringInfoData  infosendmsg;
	NameData nodename;
	const int max_pingtry = 60;
	char nodeport_buf[10];
	HeapTuple tup_result;
	GetAgentCmdRst getAgentCmdRst;
	bool result = true;
	bool findtuple = false;
	PGconn * co_pg_conn = NULL;
	Oid cnoid;

	char step1_msg[100];
	strcpy(step1_msg, "step1--if this step fails, nothing to revoke. step command:");

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot execute this command during recovery")));

	memset(&appendnodeinfo, 0, sizeof(AppendNodeInfo));

	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsg);
	appendnodeinfo.nodename = PG_GETARG_CSTRING(0);
	Assert(appendnodeinfo.nodename);

	namestrcpy(&nodename, appendnodeinfo.nodename);

	PG_TRY();
	{
		//phase 1. if errors occur, doesn't need rollback.
		ereport(INFO, (errmsg("%s%s", step1_msg, "check src node and dst node status.")));
		
		findtuple = hexp_get_nodeinfo_from_table(appendnodeinfo.nodename, CNDN_TYPE_DATANODE_MASTER, &appendnodeinfo);
		if(!findtuple)
			ereport(ERROR, (errmsg("The node does not exist.")));

		if(!((appendnodeinfo.init) && (!appendnodeinfo.incluster)))
			ereport(ERROR, (errmsg("The node status is error. It should be initialized and not in cluster.")));

		/*	1. check src node status.	*/
		findtuple = hexp_get_nodeinfo_from_table_byoid(appendnodeinfo.nodemasteroid, &srcnodeinfo);
		if(!findtuple)
			ereport(ERROR, (errmsg("The node does not exist.tuple id is %d", appendnodeinfo.nodemasteroid)));

		/*	2. check all dn and co are running.*/
		ereport(INFO, (errmsg("%s%s", step1_msg, "check all dn and co are running.")));
		mgr_make_sure_all_running(CNDN_TYPE_COORDINATOR_MASTER);
		mgr_make_sure_all_running(CNDN_TYPE_DATANODE_MASTER);

		mgr_get_gtmcoord_conn(MgrGetDefDbName(), &co_pg_conn, &cnoid);

		/*	3.add dst node to all other node's pgxc_node. */
		ereport(INFO, (errmsg("add dst node to all other node's pgxc_node.if this step fails, do it by hand.")));		
		hexp_pqexec_direct_execute_utility(co_pg_conn,SQL_BEGIN_TRANSACTION , MGR_PGEXEC_DIRECT_EXE_UTI_RET_COMMAND_OK);		
		//create new node on all node which is initilized and incluster
		hexp_create_dm_on_all_coord(co_pg_conn, &appendnodeinfo);
		hexp_create_dm_on_itself(co_pg_conn, &appendnodeinfo);
		hexp_pqexec_direct_execute_utility(co_pg_conn,SQL_COMMIT_TRANSACTION , MGR_PGEXEC_DIRECT_EXE_UTI_RET_COMMAND_OK);

		mgr_unlock_cluster_involve_gtm_coord(&co_pg_conn);

		/* 4.update dst node init and in cluster, and expend node is empty. */
		ereport(INFO, (errmsg("update dst node init and in cluster, and expend node is empty.")));
		hexp_set_expended_node_state(appendnodeinfo.nodename, true, false,  true, true, 0);

		PQfinish(co_pg_conn);
		co_pg_conn = NULL;

	}PG_CATCH();
	{
		if(co_pg_conn)
		{
			PQfinish(co_pg_conn);
			co_pg_conn = NULL;
		}
		PG_RE_THROW();
	}PG_END_TRY();

	/*wait the node can accept connections*/
	sprintf(nodeport_buf, "%d", appendnodeinfo.nodeport);
	if (!mgr_try_max_pingnode(appendnodeinfo.nodeaddr, nodeport_buf, appendnodeinfo.nodeusername, max_pingtry))
	{
		if (!result)
			appendStringInfoCharMacro(&(getAgentCmdRst.description), '\n');
		result = false;
		appendStringInfo(&(getAgentCmdRst.description), "waiting %d seconds for the new node can accept connections failed", max_pingtry);
	}

	tup_result = build_common_command_tuple(&nodename, result, getAgentCmdRst.description.data);

	pfree(getAgentCmdRst.description.data);
	pfree_AppendNodeInfo(appendnodeinfo);

	return HeapTupleGetDatum(tup_result);
}

/*
 * expand sourcenode to destnode
 */
Datum mgr_expand_dnmaster(PG_FUNCTION_ARGS)
{
	AppendNodeInfo destnodeinfo;
	AppendNodeInfo sourcenodeinfo;
	AppendNodeInfo agtm_m_nodeinfo;
	bool agtm_m_is_exist, agtm_m_is_running; /* agtm master status */
	bool sn_is_exist, sn_is_running; /*src node status */
	bool result = true;
	StringInfoData  infosendmsg;
	StringInfoData recorderr;
	NameData nodename;
	NameData gtmMasterNameData;
	HeapTuple tup_result;
	GetAgentCmdRst getAgentCmdRst;
	const int max_pingtry = 60;
	char nodeport_buf[10];
	bool findtuple;
	char step1_msg[100];
	char step2_msg[100];
	char step3_msg[256];
	char *gtmMasterName;

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot execute this command during recovery")));

	strcpy(step1_msg, "step1--if this step failed, there's nothing need to do. the command:");
	strcpy(step2_msg, "step2--If this step failed, use the command 'EXPAND RECOVER BASEBACKUP FAIL SRC TO DST'.");
	strcpy(step3_msg, "step3--if this step failed, use the command 'EXPAND RECOVER BASEBACKUP SUCCESS SRC TO DST' to recover.");

	memset(&destnodeinfo, 0, sizeof(AppendNodeInfo));
	memset(&sourcenodeinfo, 0, sizeof(AppendNodeInfo));
	memset(&agtm_m_nodeinfo, 0, sizeof(AppendNodeInfo));

	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsg);
	sourcenodeinfo.nodename = PG_GETARG_CSTRING(0);
	destnodeinfo.nodename = PG_GETARG_CSTRING(1);
	Assert(sourcenodeinfo.nodename);
	Assert(destnodeinfo.nodename);

	namestrcpy(&nodename, destnodeinfo.nodename);

	PG_TRY();
	{
		//1.check src node and dst node status. if the process can start.
		ereport(INFO, (errmsg("%s %s", step1_msg, "check src node and dst node status.")));
		
		/*1.1 check src node state.src node is initialized and in cluster.*/
		get_nodeinfo_byname(sourcenodeinfo.nodename, CNDN_TYPE_DATANODE_MASTER,
							&sn_is_exist, &sn_is_running, &sourcenodeinfo);
		if (!sn_is_running)
			ereport(ERROR, (errmsg("source datanode master \"%s\" is not running", sourcenodeinfo.nodename)));

		if (!sn_is_exist)
			ereport(ERROR, (errmsg("source datanode master \"%s\" is not initialized", sourcenodeinfo.nodename)));

		/*1.2 check dst node state.it exists and is not inicilized nor in cluster*/
		findtuple = hexp_get_nodeinfo_from_table(destnodeinfo.nodename, CNDN_TYPE_DATANODE_MASTER, &destnodeinfo);
		if(!findtuple)
		{
			ereport(ERROR, (errmsg("The node %s does not exist.", destnodeinfo.nodename)));
		}
		if(!((!destnodeinfo.incluster)&&(!destnodeinfo.init)))
		{
			ereport(ERROR, (errmsg("The node %s status is error. It should be not initialized and not in cluster.", destnodeinfo.nodename)));
		}

		/*1.3 all dn and co are running.*/
		mgr_make_sure_all_running(CNDN_TYPE_COORDINATOR_MASTER);
		mgr_make_sure_all_running(CNDN_TYPE_DATANODE_MASTER);

		//check src node status
		hexp_check_expand();

		/*2. check gtmcoord status and add dst info into gtmcoord hba.*/
		ereport(LOG, (errmsg("%s %s", step1_msg, "check gtm status and add dst info into gtmcoord hba.")));

		gtmMasterName = mgr_get_agtm_name();
		namestrcpy(&gtmMasterNameData, gtmMasterName);
		pfree(gtmMasterName);
		get_nodeinfo(gtmMasterNameData.data, CNDN_TYPE_GTM_COOR_MASTER, &agtm_m_is_exist, &agtm_m_is_running, &agtm_m_nodeinfo);
		if (agtm_m_is_exist)
		{
			if (agtm_m_is_running)
			{
				/* append "host all postgres  ip/32" for agtm master pg_hba.conf and reload it. */
				mgr_add_hbaconf(CNDN_TYPE_GTM_COOR_MASTER, "all", destnodeinfo.nodeaddr);
			}else{	
				ereport(ERROR, (errmsg("gtmcoord master is not running")));
			}
		}else{	
			ereport(ERROR, (errmsg("gtmcoord master is not initialized")));
		}

		mgr_add_hbaconf(CNDN_TYPE_GTM_COOR_SLAVE, "all", destnodeinfo.nodeaddr);

		/*3.add dst node ip and account into src node hba*/
		ereport(LOG, (errmsg("%s %s", step1_msg, "add dst node ip and account into src node hba.")));
		resetStringInfo(&infosendmsg);
		mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "replication", destnodeinfo.nodeusername, destnodeinfo.nodeaddr, 32, "trust", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
								sourcenodeinfo.nodepath,
								&infosendmsg,
								sourcenodeinfo.nodehost,
								&getAgentCmdRst);

		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));
		mgr_reload_conf(sourcenodeinfo.nodehost, sourcenodeinfo.nodepath);

		/*	4.check dst node basebackup dir does not exist.	*/
		ereport(LOG, (errmsg("%s %s", step1_msg, "check dst node basebackup dir does not exist.if this step fails , you should check the dir.")));
		mgr_check_dir_exist_and_priv(destnodeinfo.nodehost, destnodeinfo.nodepath);

		/* 5.basebackup	*/
		ereport(INFO, (errmsg("%s %s", step2_msg, "this step is basebackup, if the command failed, you must delete dst directory by hand.")));
		ereport(INFO, (errmsg("step2--basebackup begin, please wait for a moment.")));
		mgr_pgbasebackup(CNDN_TYPE_DATANODE_MASTER, &destnodeinfo, &sourcenodeinfo);
		ereport(INFO, (errmsg("step2--basebackup suceess.")));

		/*6.update dst node postgres.conf*/
		ereport(LOG, (errmsg("%s %s", step3_msg, "this step is update dst node postgres.conf.")));
		resetStringInfo(&infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("archive_command", "", &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("log_directory", "pg_log", &infosendmsg);
		mgr_add_parm(destnodeinfo.nodename, CNDN_TYPE_DATANODE_MASTER, &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
		mgr_append_pgconf_paras_str_str("hot_standby", "on", &infosendmsg);
		mgr_append_pgconf_paras_str_int("port", destnodeinfo.nodeport, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF,
								destnodeinfo.nodepath,
								&infosendmsg,
								destnodeinfo.nodehost,
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/*7. update dst node recovery.conf*/
		ereport(LOG, (errmsg("%s %s", step3_msg, "this step is update dst node recovery.conf.")));
		resetStringInfo(&infosendmsg);

		mgr_append_pgconf_paras_str_quotastr("standby_mode", "on", &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("recovery_target_timeline", "latest", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_RECOVERCONF,
								destnodeinfo.nodepath,
								&infosendmsg,
								destnodeinfo.nodehost,
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/*
		  async rep, don't need to update src node postgres.conf
		  8. start datanode
		*/
		ereport(LOG, (errmsg("%s %s", step3_msg, "this step is start datanode.")));
		mgr_start_node(CNDN_TYPE_DATANODE_MASTER, destnodeinfo.nodepath, destnodeinfo.nodehost);

		/*9.update node status initialized but not in cluster.*/
		ereport(INFO, (errmsg("%s %s", step3_msg, "the last step to update mgr info.")));
		hexp_set_expended_node_state(destnodeinfo.nodename, false, false,  true, false, sourcenodeinfo.tupleoid);

		ereport(INFO, (errmsg("expend success.")));

	}PG_CATCH();
	{
		PG_RE_THROW();
	}PG_END_TRY();

	/*wait the node can accept connections*/
	sprintf(nodeport_buf, "%d", destnodeinfo.nodeport);
	initStringInfo(&recorderr);
	if (!mgr_try_max_pingnode(destnodeinfo.nodeaddr, nodeport_buf, destnodeinfo.nodeusername, max_pingtry))
	{
		result = false;
		appendStringInfo(&recorderr, "waiting %d seconds for the new node can accept connections failed", max_pingtry);
	}
	if (result){
		tup_result = build_common_command_tuple(&nodename, true, "success");
	}
	else{
		tup_result = build_common_command_tuple(&nodename, result, recorderr.data);
	}

	pfree(recorderr.data);
	pfree_AppendNodeInfo(destnodeinfo);
	pfree_AppendNodeInfo(sourcenodeinfo);
	pfree_AppendNodeInfo(agtm_m_nodeinfo);

	return HeapTupleGetDatum(tup_result);
}

Datum mgr_expand_recover_backup_suc(PG_FUNCTION_ARGS)
{
	AppendNodeInfo destnodeinfo;
	AppendNodeInfo sourcenodeinfo;
	bool sn_is_exist, sn_is_running; /*src node status */
	bool result = true;
	StringInfoData  infosendmsg;
	StringInfoData primary_conninfo_value;
	StringInfoData recorderr;
	NameData nodename;
	HeapTuple tup_result;
	GetAgentCmdRst getAgentCmdRst;
	const int max_pingtry = 60;
	char nodeport_buf[10];
	bool findtuple;
	char step1_msgs[100];
	char step3_msgs[100];

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot execute this command during recovery")));

	strcpy(step1_msgs, "phase1--if this step fails, nothing to revoke. step command:");
	strcpy(step3_msgs, "phase3--if this step fails, use XXX. step command:");

	memset(&destnodeinfo, 0, sizeof(AppendNodeInfo));
	memset(&sourcenodeinfo, 0, sizeof(AppendNodeInfo));

	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsg);
	sourcenodeinfo.nodename = PG_GETARG_CSTRING(0);
	destnodeinfo.nodename = PG_GETARG_CSTRING(1);
	Assert(sourcenodeinfo.nodename);
	Assert(destnodeinfo.nodename);

	namestrcpy(&nodename, destnodeinfo.nodename);

	PG_TRY();
	{
		//phase 1. if errors occur, doesn't need rollback.

		//1.check src node and dst node status. if the process can start.
		ereport(INFO, (errmsg("%s.%s", step1_msgs, "check src node and dst node status.")));
		/*
		1.1 check src node state.src node is initialized and in cluster.
		*/
		get_nodeinfo_byname(sourcenodeinfo.nodename, CNDN_TYPE_DATANODE_MASTER,
							&sn_is_exist, &sn_is_running, &sourcenodeinfo);
		if (!sn_is_running)
			ereport(ERROR, (errmsg("source datanode master \"%s\" is not running", sourcenodeinfo.nodename)));

		if (!sn_is_exist)
			ereport(ERROR, (errmsg("source datanode master \"%s\" is not initialized", sourcenodeinfo.nodename)));

		/*
		1.2 check dst node state.it exists and is not inicilized nor in cluster
		*/
		findtuple = hexp_get_nodeinfo_from_table(destnodeinfo.nodename, CNDN_TYPE_DATANODE_MASTER, &destnodeinfo);
		if(!findtuple)
		{
			ereport(ERROR, (errmsg("The node does not exist.")));
		}
		if(!((!destnodeinfo.incluster)&&(!destnodeinfo.init)))
		{
			ereport(ERROR, (errmsg("The node status is error. It should be not initialized and not in cluster.")));
		}

		/*
		1.3 all dn and co are running.
		*/
		mgr_make_sure_all_running(CNDN_TYPE_COORDINATOR_MASTER);
		mgr_make_sure_all_running(CNDN_TYPE_DATANODE_MASTER);

		//phase 3. if errors occur, redo those.
		/*
		7.update dst node postgres.conf
		*/
		ereport(INFO, (errmsg("%s.%s", step3_msgs, "update dst node postgres.conf.")));
		resetStringInfo(&infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("archive_command", "", &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("log_directory", "pg_log", &infosendmsg);
		mgr_add_parm(destnodeinfo.nodename, CNDN_TYPE_DATANODE_MASTER, &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
		mgr_append_pgconf_paras_str_str("hot_standby", "on", &infosendmsg);
		mgr_append_pgconf_paras_str_int("port", destnodeinfo.nodeport, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF,
								destnodeinfo.nodepath,
								&infosendmsg,
								destnodeinfo.nodehost,
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/*
		8. update dst node recovery.conf
		*/
		ereport(INFO, (errmsg("%s.%s", step3_msgs, "update dst node recovery.conf.")));
		resetStringInfo(&infosendmsg);
		initStringInfo(&primary_conninfo_value);

		appendStringInfo(&primary_conninfo_value, "host=%s port=%d user=%s ",
						get_hostaddress_from_hostoid(sourcenodeinfo.nodehost),
						sourcenodeinfo.nodeport,
						get_hostuser_from_hostoid(sourcenodeinfo.nodehost));

		mgr_append_pgconf_paras_str_quotastr("standby_mode", "on", &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("recovery_target_timeline", "latest", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_RECOVERCONF,
								destnodeinfo.nodepath,
								&infosendmsg,
								destnodeinfo.nodehost,
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		//async rep, don't need to update src node postgres.conf

		/*
		9. start datanode
		*/
		ereport(INFO, (errmsg("%s.%s", step3_msgs, "start datanode.")));
		mgr_start_node(CNDN_TYPE_DATANODE_MASTER, destnodeinfo.nodepath, destnodeinfo.nodehost);

		/*
		10.update node status initialized but not in cluster.
		*/
		ereport(INFO, (errmsg("last step to update mgr info.if failed, can update by ***")));
		hexp_set_expended_node_state(destnodeinfo.nodename, false, false,  true, false, sourcenodeinfo.tupleoid);

		ereport(INFO, (errmsg("expend success.")));

	}PG_CATCH();
	{
		PG_RE_THROW();
	}PG_END_TRY();

	/*wait the node can accept connections*/
	sprintf(nodeport_buf, "%d", destnodeinfo.nodeport);
	initStringInfo(&recorderr);
	if (!mgr_try_max_pingnode(destnodeinfo.nodeaddr, nodeport_buf, destnodeinfo.nodeusername, max_pingtry))
	{
		result = false;
		appendStringInfo(&recorderr, "waiting %d seconds for the new node can accept connections failed", max_pingtry);
	}
	if (result)
		tup_result = build_common_command_tuple(&nodename, true, "success");
	else
	{
		tup_result = build_common_command_tuple(&nodename, result, recorderr.data);
	}

	pfree(recorderr.data);
	pfree_AppendNodeInfo(destnodeinfo);
	pfree_AppendNodeInfo(sourcenodeinfo);

	return HeapTupleGetDatum(tup_result);
}

Datum mgr_expand_recover_backup_fail(PG_FUNCTION_ARGS)
{
	AppendNodeInfo destnodeinfo;
	AppendNodeInfo sourcenodeinfo;
	bool sn_is_exist, sn_is_running; /*src node status */
	StringInfoData  infosendmsg;
	NameData nodename;
	HeapTuple tup_result;
	GetAgentCmdRst getAgentCmdRst;
	bool findtuple;
	char step1_msgs[100];
	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot execute this command during recovery")));

	strcpy(step1_msgs, "if this step fails, nothing to revoke. step command:");

	memset(&destnodeinfo, 0, sizeof(AppendNodeInfo));
	memset(&sourcenodeinfo, 0, sizeof(AppendNodeInfo));

	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsg);
	sourcenodeinfo.nodename = PG_GETARG_CSTRING(0);
	destnodeinfo.nodename = PG_GETARG_CSTRING(1);
	Assert(sourcenodeinfo.nodename);
	Assert(destnodeinfo.nodename);

	namestrcpy(&nodename, destnodeinfo.nodename);

	PG_TRY();
	{
		//1.check src node and dst node status. if the process can start.
		ereport(INFO, (errmsg("%s.%s", step1_msgs, "check src node and dst node status.")));
		/*
		1.1 check src node state.src node is initialized and in cluster.
		*/
		get_nodeinfo_byname(sourcenodeinfo.nodename, CNDN_TYPE_DATANODE_MASTER,
							&sn_is_exist, &sn_is_running, &sourcenodeinfo);
		if (!sn_is_running)
			ereport(ERROR, (errmsg("source datanode master \"%s\" is not running", sourcenodeinfo.nodename)));

		if (!sn_is_exist)
			ereport(ERROR, (errmsg("source datanode master \"%s\" is not initialized", sourcenodeinfo.nodename)));

		/*
		1.2 check dst node state.it exists and is not inicilized nor in cluster
		*/
		findtuple = hexp_get_nodeinfo_from_table(destnodeinfo.nodename, CNDN_TYPE_DATANODE_MASTER, &destnodeinfo);
		if(!findtuple)
		{
			ereport(ERROR, (errmsg("The node does not exist.")));
		}
		if(!((!destnodeinfo.incluster)&&(!destnodeinfo.init)))
		{
			ereport(ERROR, (errmsg("The node status is error. It should be not initialized and not in cluster.")));
		}
	}PG_CATCH();
	{
		PG_RE_THROW();
	}PG_END_TRY();


	tup_result = build_common_command_tuple(&nodename, true, "success");

	pfree_AppendNodeInfo(destnodeinfo);
	pfree_AppendNodeInfo(sourcenodeinfo);

	return HeapTupleGetDatum(tup_result);
}
Datum mgr_expand_clean(PG_FUNCTION_ARGS)
{	
	PGconn *co_pg_conn = NULL;
	PGconn *other_conn = NULL;
	Oid cnoid;
	HeapTuple tup_result = NULL;
	char ret_msg[100];
	Name dbname;
	NameData nodename;
	List *dbname_list = NIL;
	ListCell	*lc;
	
	strcpy(nodename.data, "---");
	strcpy(ret_msg, "expand clean success.");
	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot execute this command during recovery")));

	PG_TRY();
	{
		mgr_get_gtmcoord_conn(MgrGetDefDbName(), &co_pg_conn, &cnoid);
		Assert(cnoid);
		if (MgrGetAdbcleanNum(co_pg_conn) > 0)
		{
			if ((dbname_list = MgrGetAdbCleanDbName(co_pg_conn)) == NIL)
				ereport(ERROR, (errmsg("No database need expand clean.")));

			foreach (lc, dbname_list)
			{
				dbname = (Name)lfirst(lc);
				mgr_get_gtmcoord_conn(dbname->data, &other_conn, &cnoid);
				MgrSendDataCleanToGtm(other_conn);
				PQfinish(other_conn);
				other_conn = NULL;	
			}	
		}	
	}PG_CATCH();
	{
		MgrFreeClean(co_pg_conn, other_conn, dbname_list);
		PG_RE_THROW();
	}PG_END_TRY();

    MgrFreeClean(co_pg_conn, other_conn, dbname_list);
	tup_result = build_common_command_tuple(&nodename, true, ret_msg);
	return HeapTupleGetDatum(tup_result);
}

Datum mgr_checkout_dnslave_status(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	InitNodeInfo *info;
	ScanKeyData key[1];
	HeapTuple tuple_node;
	HeapTuple tuple_result;
	Form_mgr_node mgr_node;
	NameData agent_addr;
	NameData node_type_str;
	int agent_port;
	int32 node_port;
	char *node_user;
	ManagerAgent *ma;
	GetAgentCmdRst getAgentCmdRst;
	StringInfoData sendstrmsg;
	StringInfoData buf;
	bool execok;
	int ret = 0;
	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot execute this command during recovery")));

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		info = palloc(sizeof(*info));
		ScanKeyInit(&key[0]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
		info->rel_node = heap_open(NodeRelationId, AccessShareLock);
		info->rel_scan = heap_beginscan_catalog(info->rel_node, 1, key);
		info->lcp =NULL;
		/* save info */
		funcctx->user_fctx = info;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	Assert(funcctx);
	info = funcctx->user_fctx;
	Assert(info);
	/*select the datanode slave node from cluster*/
	while(1)
	{
		tuple_node = heap_getnext(info->rel_scan, ForwardScanDirection);
		if(tuple_node == NULL)
		{
			/* end of row */
			heap_endscan(info->rel_scan);
			heap_close(info->rel_node, AccessShareLock);
			pfree(info);
			SRF_RETURN_DONE(funcctx);
		}

		mgr_node = (Form_mgr_node)GETSTRUCT(tuple_node);
		Assert(mgr_node);
		/*find the type is slave and the node is datanode*/
		if ((mgr_node->nodemasternameoid != 0)
			&& (CNDN_TYPE_DATANODE_SLAVE == mgr_node->nodetype
			|| CNDN_TYPE_DATANODE_MASTER == mgr_node->nodetype))
			break;
	}

	/*get the datanode info*/
	node_port = mgr_node->nodeport;
	node_user = get_hostuser_from_hostoid(mgr_node->nodehost);

	/*get agent info to connect */
	get_agent_info_from_hostoid(ObjectIdGetDatum(mgr_node->nodehost), NameStr(agent_addr), &agent_port);

	/*check node is running */
	execok = is_node_running(NameStr(agent_addr), node_port, node_user, mgr_node->nodetype);
	if (!execok)
	{
		get_node_type_str(mgr_node->nodetype, &node_type_str);
		ereport(ERROR, (errmsg("%s \"%s\" is not running", NameStr(node_type_str), NameStr(mgr_node->nodename))));
	}
	/* connect to agent and send msg */
	initStringInfo(&sendstrmsg);
	initStringInfo(&(getAgentCmdRst.description));
	appendStringInfo(&sendstrmsg, "%s", NameStr(agent_addr));
	appendStringInfoChar(&sendstrmsg, '\0');
	appendStringInfo(&sendstrmsg, "%d", node_port);
	appendStringInfoChar(&sendstrmsg, '\0');
	appendStringInfo(&sendstrmsg, "%s", node_user);
	appendStringInfoChar(&sendstrmsg, '\0');

	ma = ma_connect(NameStr(agent_addr), agent_port);;
	if (!ma_isconnected(ma))
	{
		/*report error message*/
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		ereport(ERROR, (errmsg("could not connect socket for agent \"%s\".",
						NameStr(agent_addr))));
	}
	getAgentCmdRst.ret = false;
	initStringInfo(&buf);
	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, AGT_CMD_CHECKOUT_NODE);
	mgr_append_infostr_infostr(&buf, &sendstrmsg);
	pfree(sendstrmsg.data);
	ma_endmessage(&buf, ma);
	if (! ma_flush(ma, true))
	{
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		return -1;
	}
	/*check the receive msg*/
	mgr_recv_msg_for_monitor(ma, &execok, &getAgentCmdRst.description);
	ma_close(ma);
	if (!execok)
	{
		ereport(WARNING, (errmsg("execute checkout datanode slave by agent(host=%s port=%d) fail.\n \"%s\"",
			NameStr(agent_addr), agent_port, getAgentCmdRst.description.data)));
	}
	if (getAgentCmdRst.description.len == 1)
		ret = getAgentCmdRst.description.data[0];
	else
		ereport(ERROR, (errmsg("receive msg from agent \"%s\" error.", NameStr(agent_addr))));

	/*return */
	tuple_result = build_common_command_tuple_four_col(
				&(mgr_node->nodename)
				,mgr_node->nodetype
				,ret == 't' ? true : false
				,"pg_is_in_recovery");

	pfree(getAgentCmdRst.description.data);
	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple_result));
}

Datum mgr_expand_check_status(PG_FUNCTION_ARGS)
{
	StringInfoData serialize;
	NameData 	nodename;
	PGconn 		*pg_conn = NULL;
	Oid 		cnoid;
	HeapTuple 	tup_result;
	int         count = 0;

	strcpy(nodename.data, "---");
	initStringInfo(&serialize);

	PG_TRY();
	{
		mgr_get_gtmcoord_conn(MgrGetDefDbName(), &pg_conn, &cnoid);
		Assert(cnoid);

		appendStringInfo(&serialize,"pgxc node info in cluster is consistent.\n");

		if ((count = MgrGetAdbcleanNum(pg_conn)) > 0)
		{
			ereport(ERROR, (errmsg("cluster status is expanding, can't expand again. adb_clean count(%d).", count)));
		}
		
		hexp_get_allnodes_serialize(&serialize);

		if(pg_conn)
			PQfinish(pg_conn);
		pg_conn = NULL;
	}PG_CATCH();
	{
		if(pg_conn)
			PQfinish(pg_conn);
		pg_conn = NULL;
		PG_RE_THROW();
	}PG_END_TRY();

	tup_result = build_common_command_tuple(&nodename, true, serialize.data);
	return HeapTupleGetDatum(tup_result);
}

Datum mgr_expand_show_status(PG_FUNCTION_ARGS)
{
	PGconn 	 *pg_conn = NULL;
	Oid 	 cnoid;
	HeapTuple 	tup_result;
	StringInfoData serialize;
	NameData 	   nodename;
	int            count = 0;

	strcpy(nodename.data, "---");
	initStringInfo(&serialize);

	PG_TRY();
	{
		mgr_get_gtmcoord_conn(MgrGetDefDbName(), &pg_conn, &cnoid);
		Assert(cnoid);

		hexp_pgxc_pool_reload_on_all_node(pg_conn);

		appendStringInfo(&serialize,"pgxc node info in cluster is consistent.\n");

		if ((count = MgrGetAdbcleanNum(pg_conn)) > 0)
		{
			appendStringInfo(&serialize,"cluster status is vacuum, can't expand now. adb_clean count(%d).\n", count);
		}

        hexp_get_allnodes_serialize(&serialize);

		if(pg_conn)
			PQfinish(pg_conn);
		pg_conn = NULL;
	}PG_CATCH();
	{
		if(pg_conn)
			PQfinish(pg_conn);
		pg_conn = NULL;
		PG_RE_THROW();
	}PG_END_TRY();

	tup_result = build_common_command_tuple(&nodename, true, serialize.data);
	return HeapTupleGetDatum(tup_result);
}

static void MgrSetAppendNodeInfo(AppendNodeInfo  *nodeinfo, Form_mgr_node mgr_node, HeapTuple tuple, InitNodeInfo *info)
{
	bool is_null = false;
	Datum datumPath;

	CheckNull(nodeinfo);
	CheckNull(mgr_node);
	CheckNull(tuple);
	CheckNull(info);

	nodeinfo->nodename = pstrdup(NameStr(mgr_node->nodename));
	nodeinfo->nodetype = mgr_node->nodetype;
	nodeinfo->nodeaddr = get_hostaddress_from_hostoid(mgr_node->nodehost);
	nodeinfo->nodeusername = get_hostuser_from_hostoid(mgr_node->nodehost);
	nodeinfo->nodeport = mgr_node->nodeport;
	nodeinfo->nodehost = mgr_node->nodehost;
	nodeinfo->nodemasteroid = mgr_node->nodemasternameoid;
	nodeinfo->init = mgr_node->nodeinited;
	nodeinfo->incluster = mgr_node->nodeincluster;

	/*get nodepath from tuple*/
	datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(info->rel_node), &is_null);
	if (is_null)
	{
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, AccessShareLock);
		pfree(info);
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
			, errmsg("column nodepath is null")));
	}
	nodeinfo->nodepath = pstrdup(TextDatumGetCString(datumPath));
	return;
}	
static List* MgrGetAllAppendNodeInfo(void)
{
	InitNodeInfo *info;
	ScanKeyData key[3];
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	AppendNodeInfo  *nodeinfo = NULL;
	List 			*nodeinfo_list = NIL;

	ScanKeyInit(&key[0]
			,Anum_mgr_node_nodetype
			,BTEqualStrategyNumber
			,F_CHAREQ
			,CharGetDatum(CNDN_TYPE_DATANODE_MASTER));
	ScanKeyInit(&key[1]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	ScanKeyInit(&key[2]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(false));

	info = palloc(sizeof(*info));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);
	info->rel_scan = heap_beginscan_catalog(info->rel_node, 3, key);
	info->lcp = NULL;

	while ((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);

		nodeinfo = (AppendNodeInfo *) palloc(sizeof(AppendNodeInfo));
		MgrSetAppendNodeInfo(nodeinfo, mgr_node, tuple, info);
		nodeinfo_list = lappend(nodeinfo_list, nodeinfo);	
	}

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);

	return nodeinfo_list;
}

static bool MgrGetConn(char* database, AppendNodeInfo *node, PGconn **pg_conn, StringInfoData  *infosendmsg)
{
	bool is_addhba = false;
	char port_buf[10] = {0};

    CheckNullRetrunRet(database,false);
	CheckNullRetrunRet(node,false);
	CheckNullRetrunRet(infosendmsg,false);

	sprintf(port_buf, "%d", node->nodeport);
	*pg_conn = PQsetdbLogin(node->nodeaddr,
					port_buf,
					NULL, NULL,database,
					node->nodeusername,NULL);					
	if (*pg_conn == NULL || PQstatus((PGconn*)*pg_conn) != CONNECTION_OK)
	{
		/* update pg_hba.conf */
		is_addhba = AddHbaIsValid(node, infosendmsg);
		*pg_conn = PQsetdbLogin(node->nodeaddr,
					port_buf,
					NULL, NULL,database,
					node->nodeusername,NULL);
		if (*pg_conn == NULL || PQstatus((PGconn*)*pg_conn) != CONNECTION_OK)
			ereport(ERROR,
				(errmsg("Fail to connect to expend datanode %s", PQerrorMessage((PGconn*)*pg_conn)),
					errhint("info(host=%s port=%d dbname=%s user=%s)",
					node->nodeaddr, node->nodeport, DEFAULT_DB, node->nodeusername)));
	}

	return is_addhba;
}
static void MgrCheckSynaLsn(PGconn *src_pg_conn, PGconn *dst_pg_conn)
{
	int src_lsn_high = 0;
	int src_lsn_low = 0;
	int dst_lsn_high = 0;
	int dst_lsn_low = 0;
	int try = 0;

	CheckNull(src_pg_conn);
	CheckNull(dst_pg_conn);
	
	/*	2.4 wait 20s for sync */
	try=20;
	for(;;)
	{
		hexp_mgr_pqexec_getlsn(dst_pg_conn, SELECT_LAST_LSN, &dst_lsn_high, &dst_lsn_low);
		hexp_mgr_pqexec_getlsn(src_pg_conn, SELECT_CUR_LSN, &src_lsn_high, &src_lsn_low);
		if((src_lsn_high==dst_lsn_high) && (src_lsn_low==dst_lsn_low))
			break;

		pg_usleep(1000000L);
		try--;
		if(try==0)
			break;
	}
	if(!((src_lsn_high==dst_lsn_high) && (src_lsn_low==dst_lsn_low)))
		ereport(ERROR, (errmsg("expend src node and dst node can not sync in %d seconds", try)));
	
	return;
}
static void MgrCheckRestartRunning(AppendNodeInfo *dst_node)
{
	int try=60;
	CheckNull(dst_node);

	hexp_restart_node(dst_node);	
	for(;;)
	{
		if (is_node_running(dst_node->nodeaddr, dst_node->nodeport, dst_node->nodeusername, dst_node->nodetype))
			break;
		pg_usleep(1000000L);
		try--;
		if(try==0)
			break;
	}
	
	if (!is_node_running(dst_node->nodeaddr, dst_node->nodeport, dst_node->nodeusername, dst_node->nodetype))
		ereport(ERROR, (errmsg("expend dst node %s can not restart in %d seconds",dst_node->nodename, 60)));

	return;
}
static List* MgrActivateStep1(char* database, List *nodeinfo_list)
{
	AppendNodeInfo  src_node;
	AppendNodeInfo	*dst_node = NULL;
	StringInfoData  sendmsg_src;
	StringInfoData  sendmsg_dst;
	bool 		is_addhba_src = false;
	bool 		is_addhba_dst = false;
	PGconn 		*src_pg_conn = NULL;
	PGconn 		*dst_pg_conn = NULL;
	int 		src_lsn_high = 0;
	int 		src_lsn_low = 0;
	int 		dst_lsn_high = 0;
	int 		dst_lsn_low = 0;
	char 		step1_msg[100];
	ListCell	*lc;
	List 	    *src_dst_list = NIL;
	SRC_DST_NODENAME *src_dst_nodename = NULL;
	
	CheckNullRetrunRet(database, NIL);
	CheckNullRetrunRet(nodeinfo_list, NIL);

	strcpy(step1_msg, "step1--if this step failed, there's nothing need to do. the command:");
	
	PG_TRY();
	{		
		foreach (lc, nodeinfo_list)
		{
			initStringInfo(&sendmsg_src);
			initStringInfo(&sendmsg_dst);

			dst_node = (AppendNodeInfo *)lfirst(lc);
			ereport(INFO, (errmsg("%s%s", step1_msg, "check src node and dst node status.")));

			if(!((dst_node->init) && (!dst_node->incluster)))
				ereport(ERROR, (errmsg("The dst node %s status is error. It should be initialized and not in cluster.", dst_node->nodename)));

			if(!hexp_get_nodeinfo_from_table_byoid(dst_node->nodemasteroid, &src_node))
				ereport(ERROR, (errmsg("The src node %s does not exist.tuple id is %d", dst_node->nodename, dst_node->nodemasteroid)));
			
			src_dst_nodename = (SRC_DST_NODENAME *)palloc(sizeof(SRC_DST_NODENAME));
			namestrcpy(&src_dst_nodename->srcnodename, src_node.nodename);
			namestrcpy(&src_dst_nodename->dstnodename, dst_node->nodename);
			src_dst_nodename->used = false;
			src_dst_list = lappend(src_dst_list, src_dst_nodename);

			ereport(LOG, (errmsg("%s%s", step1_msg, "get dst lsn.")));
			is_addhba_dst = MgrGetConn(database, dst_node, &dst_pg_conn, &sendmsg_dst);
			if (dst_pg_conn != NULL)
				hexp_mgr_pqexec_getlsn(dst_pg_conn, SELECT_LAST_LSN, &dst_lsn_high, &dst_lsn_low);

			ereport(LOG, (errmsg("%s%s", step1_msg, "get src lsn.")));
			is_addhba_src = MgrGetConn(database, &src_node, &src_pg_conn, &sendmsg_src);
			if (src_pg_conn != NULL)
				hexp_mgr_pqexec_getlsn(src_pg_conn, SELECT_CUR_LSN, &src_lsn_high, &src_lsn_low);

			if(!((src_lsn_high==dst_lsn_high) && ((src_lsn_low-dst_lsn_low)>=0) &&((src_lsn_low-dst_lsn_low)<=(8*1024*1024))))
				ereport(ERROR, (errmsg("the lsn lag between src node(%s) and dst node(%s) is longer than 8M.src lsn is %x/%x, dst lsn is %x/%x", 
				src_node.nodename, dst_node->nodename, src_lsn_high,src_lsn_low,dst_lsn_high,dst_lsn_low)));
			
			MgrCheckSynaLsn(src_pg_conn, dst_pg_conn);

			/* 3.promote&check connect	*/
			ereport(INFO, (errmsg("step1--promote dst node. if it fails, check dst status by hand. you have to drop the node and directory, then do expand from beginning.")));
			mgr_failover_one_dn_inner_func(dst_node->nodename, AGT_CMD_DN_MASTER_PROMOTE, CNDN_TYPE_DATANODE_MASTER, true, false);

			if (is_addhba_src)
				RemoveHba(dst_node, &sendmsg_src);
			if (is_addhba_dst)
				RemoveHba(dst_node, &sendmsg_dst);

			ClosePgConn(dst_pg_conn);
			ClosePgConn(src_pg_conn);
			MgrFree(sendmsg_src.data);
			MgrFree(sendmsg_dst.data);
		}
	}PG_CATCH();
	{
		ClosePgConn(dst_pg_conn);
		ClosePgConn(src_pg_conn);
		MgrFree(sendmsg_src.data);
		MgrFree(sendmsg_dst.data);

		PG_RE_THROW();
	}PG_END_TRY();

	return src_dst_list;
}
static void MgrCreateGtmSql(List *src_dst_list, char *nodes_slq, int len)
{
	ListCell*			lc;
	ListCell*			la;
	SRC_DST_NODENAME*	nodename = NULL;
	SRC_DST_NODENAME*	nodename2 = NULL;
	char 				one_node[512] = {0};
	int 				offset_total = 0;
	int 				offset_one = 0;

	CheckNull(src_dst_list);
	CheckNull(nodes_slq);
	
	foreach (lc, src_dst_list)
	{		
		nodename = (SRC_DST_NODENAME*)lfirst(lc);
		if (nodename->used){
			continue;
		}

		offset_one = 0;
		memset(one_node, 0x00, sizeof(one_node));
		offset_one = sprintf(one_node, "%s TO (%s", NameStr(nodename->srcnodename), NameStr(nodename->dstnodename));
		nodename->used = true;
		foreach (la, src_dst_list)
		{
			nodename2 = (SRC_DST_NODENAME*)lfirst(la);
			if ((0 == strcasecmp(NameStr(nodename->srcnodename), NameStr(nodename2->srcnodename))) && (!nodename2->used))
			{
				if (offset_one + 1 + strlen(NameStr(nodename2->dstnodename)) < sizeof(one_node))
				{
					offset_one += sprintf(one_node, "%s,%s", one_node, NameStr(nodename2->dstnodename));	
					nodename2->used = true;
				}						
			}
		}

		if (offset_one + 1 < sizeof(one_node)){
			offset_one += sprintf(one_node, "%s)", one_node);
		}

		if (offset_total + strlen(one_node) < len)
		{
			if (0 == offset_total){
				offset_total += sprintf(nodes_slq, "%s,", one_node);		
			}else{
				offset_total += sprintf(nodes_slq, "%s%s,", nodes_slq, one_node);		
			}
		}
	}
	
	if (strlen(nodes_slq) > 0){
		nodes_slq[strlen(nodes_slq)-1] = '\0';
	}
    
	return;
}
static void MgrActivateStep2(PGconn *gtm_conn, List *dst_node_list, List *src_dst_list)
{
	AppendNodeInfo	*dst_node = NULL;
	ListCell		*lc;
	char 			step2_msg[256];
	char            nodes_slq[2048] = {0};
	strcpy(step2_msg, "step2--if this step failed, use the command 'EXPAND ACTIVATE RECOVER DOPROMOTE SUCCESS DST' to recover.");

	PG_TRY();
	{
		foreach (lc, dst_node_list)
		{
			dst_node = (AppendNodeInfo *)lfirst(lc);
			/* update pgxc node name in postgresql.conf in dst node. */
			ereport(LOG, (errmsg("update pgxc node name in (%s) postgresql.conf in dst node.if this step fails, do it by hand, then restart the node", dst_node->nodename)));
			hexp_update_conf_pgxc_node_name(dst_node, dst_node->nodename);

			/* wait 60s for restart */
			ereport(INFO, (errmsg("step2--restart dst node(%s). if this step fails, do it by hand.", dst_node->nodename)));
			MgrCheckRestartRunning(dst_node);
		}	

		MgrCreateGtmSql(src_dst_list, nodes_slq, sizeof(nodes_slq));
		/* add dst node to all other node's pgxc_node. */
		ereport(INFO, (errmsg("%s %s", step2_msg, "add dst node to all other node's pgxc_node.")));
		foreach (lc, dst_node_list)
		{
			hexp_pqexec_direct_execute_utility(gtm_conn, SQL_BEGIN_TRANSACTION , MGR_PGEXEC_DIRECT_EXE_UTI_RET_COMMAND_OK);
			dst_node = (AppendNodeInfo *)lfirst(lc);
			hexp_create_dm_on_all_coord(gtm_conn, dst_node);
			hexp_create_dm_on_itself(gtm_conn, dst_node);
			hexp_pgxc_pool_reload_on_all_node(gtm_conn);
			hexp_pqexec_direct_execute_utility(gtm_conn, SQL_COMMIT_TRANSACTION , MGR_PGEXEC_DIRECT_EXE_UTI_RET_COMMAND_OK);
		}		
		
		MgrSendAlterNodeDataToGtm(gtm_conn, nodes_slq);
		
		//hexp_pqexec_direct_execute_utility(gtm_conn, SQL_COMMIT_TRANSACTION , MGR_PGEXEC_DIRECT_EXE_UTI_RET_COMMAND_OK);

		/* update dst node init and in cluster. */
		ereport(INFO, (errmsg("%s %s", step2_msg, "update dst node init and in cluster.")));
		foreach (lc, dst_node_list)
		{
			dst_node = (AppendNodeInfo *)lfirst(lc);
			hexp_set_expended_node_state(dst_node->nodename, true, false,  true, true, 0);
		}
	}PG_CATCH();
	{
		PG_RE_THROW();
	}PG_END_TRY();
	return;
}

static HeapTuple MgrGetTupleResult(List *nodeinfo_list)
{
	ListCell		*lc;
	AppendNodeInfo	*dst_node = NULL;
	char 			nodeport_buf[10];
	bool			result = true;
	GetAgentCmdRst  success_rst;
	GetAgentCmdRst  fail_rst;
	const int 		max_pingtry = 60;
	NameData 		nodename;
	HeapTuple 		tup_result = NULL;

    initStringInfo(&(success_rst.description));
	initStringInfo(&(fail_rst.description));
	appendStringInfo(&(success_rst.description), "expand activate success, the nodes:");	
	namestrcpy(&nodename, "");

	foreach (lc, nodeinfo_list)
	{		
		dst_node = (AppendNodeInfo *)lfirst(lc);
		sprintf(nodeport_buf, "%d", dst_node->nodeport);
		if (!mgr_try_max_pingnode(dst_node->nodeaddr, nodeport_buf, dst_node->nodeusername, max_pingtry))
		{
			result = false;
			namestrcpy(&nodename, dst_node->nodename);
			appendStringInfo(&(fail_rst.description), "waiting %d seconds for the new node can accept connections failed", max_pingtry);
			break;
		}
		appendStringInfo(&(success_rst.description), "%s ", dst_node->nodename);		
	}

    if (result){
		tup_result = build_common_command_tuple(&nodename, result, success_rst.description.data);
	}else{
		tup_result = build_common_command_tuple(&nodename, result, fail_rst.description.data);
	}

	MgrFree(success_rst.description.data);
	MgrFree(fail_rst.description.data);
	return tup_result;
}
static void MgrFreeNodeList(List *dst_node_list, List *src_dst_list)
{
	ListCell		*lc;
	AppendNodeInfo	*dst_node = NULL;

	if (NIL == dst_node_list)
		return;

	foreach (lc, dst_node_list)
	{		
		dst_node = (AppendNodeInfo *)lfirst(lc);
		pfree_AppendNodeInfo(*dst_node);
	}

    if (NIL == src_dst_list)
		return;
    list_free(src_dst_list);
}
static void hexp_get_allnodes_serialize(StringInfoData *pserialize)
{
	ListCell	*lc;
	List 		*dn_status_list;
	DN_STATUS	*dn_status;
	
	Assert(pserialize);

	dn_status_list = hexp_get_all_dn_status();
	foreach (lc, dn_status_list)
	{
		dn_status = (DN_STATUS *)lfirst(lc);
		appendStringInfo(pserialize,
			"name=%s-status=%s-masterid=%d-incluster=%d\n"
			,NameStr(dn_status->nodename),
			NameStr(dn_status->node_status),
			dn_status->nodemasternameoid,
			dn_status->nodeincluster);
	}
	return;
}
static void hexp_update_conf_pgxc_node_name(AppendNodeInfo *node, char* newname)
{
	GetAgentCmdRst getAgentCmdRst;
	StringInfoData infosendmsg;

	initStringInfo(&infosendmsg);
	initStringInfo(&(getAgentCmdRst.description));

	mgr_append_pgconf_paras_str_quotastr("pgxc_node_name", newname, &infosendmsg);
	mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, node->nodepath, &infosendmsg, node->nodehost, &getAgentCmdRst);

	if (!getAgentCmdRst.ret)
	{
		ereport(ERROR, (errmsg("update datanode %s's pgxc_node_name param fail\n", newname)));
	}
}

static void hexp_restart_node(AppendNodeInfo *node)
{
	GetAgentCmdRst getAgentCmdRst;
	StringInfoData infosendmsg;
	StringInfoData buf;
	ManagerAgent *ma;
	bool exec_result;

	initStringInfo(&buf);
	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsg);

	appendStringInfo(&infosendmsg, " restart -D %s", node->nodepath);
	appendStringInfo(&infosendmsg, " -Z datanode -m fast -o -i -w -c -l %s/logfile", node->nodepath);

	/* connection agent */
	ma = ma_connect_hostoid(node->nodehost);
	if(!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}

	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, AGT_CMD_DN_RESTART);
	ma_sendstring(&buf,infosendmsg.data);
	ma_endmessage(&buf, ma);
	if (! ma_flush(ma, true))
	{
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}

	/*check the receive msg*/
	exec_result = mgr_recv_msg(ma, &getAgentCmdRst);
	ma_close(ma);

	if(buf.data)
		pfree(buf.data);
	if(getAgentCmdRst.description.data)
		pfree(getAgentCmdRst.description.data);
	if(infosendmsg.data)
		pfree(infosendmsg.data);

	if (!exec_result)
	{
		ereport(ERROR, (errmsg("restart %s fail\n", node->nodename)));
	}
}

static void hexp_get_dn_conn(PGconn **pg_conn, Form_mgr_node mgr_node, char* cnpath)
{
	Oid coordhostoid;
	int32 coordport;
	char *coordhost;
	char coordport_buf[10];
	char *connect_user;
	int try = 0;
	NameData self_address;
	GetAgentCmdRst getAgentCmdRst;
	StringInfoData infosendmsg;
	char* database ;
	
	bool breload = false;
	
	if(0!=strcmp(MGRDatabaseName,""))
		database = MGRDatabaseName;
	else
		database = DEFAULT_DB;

	coordhostoid = mgr_node->nodehost;
	coordport = mgr_node->nodeport;
	coordhost = get_hostaddress_from_hostoid(coordhostoid);
	connect_user = get_hostuser_from_hostoid(coordhostoid);

	/*get the adbmanager ip*/
	if (!mgr_get_self_address(coordhost, coordport, &self_address))
	{
		ereport(ERROR,
				(errmsg("can not connect node %s, is it running?", NameStr(mgr_node->nodename))));
	}

	/*set adbmanager ip to the coordinator if need*/
	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsg);

	sprintf(coordport_buf, "%d", coordport);
	for (try = 0; try < 2; try++)
	{
		*pg_conn = PQsetdbLogin(coordhost
								,coordport_buf
								,NULL, NULL
								,database
								,connect_user
								,NULL);
		if (try != 0)
			break;
		if (PQstatus((PGconn*)*pg_conn) != CONNECTION_OK)
		{
			breload = true;
			PQfinish((PGconn*)*pg_conn);
			*pg_conn = NULL;
			resetStringInfo(&infosendmsg);
			mgr_add_oneline_info_pghbaconf(CONNECT_HOST, DEFAULT_DB, connect_user, self_address.data, 31, "trust", &infosendmsg);
			mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF, cnpath, &infosendmsg, coordhostoid, &getAgentCmdRst);
			mgr_reload_conf(coordhostoid, cnpath);
			if (!getAgentCmdRst.ret)
			{
				pfree(infosendmsg.data);
				ereport(ERROR, (errmsg("set ADB Manager ip \"%s\" to %s coordinator %s/pg_hba,conf fail %s", self_address.data, coordhost, cnpath, getAgentCmdRst.description.data)));
			}
		}
		else
			break;
	}
	try = 0;
	if ((PGconn*)*pg_conn == NULL || PQstatus((PGconn*)*pg_conn) != CONNECTION_OK)
	{
		pfree(infosendmsg.data);
		pfree(getAgentCmdRst.description.data);
		ereport(ERROR,
			(errmsg("Fail to connect to coordinator %s", PQerrorMessage((PGconn*)*pg_conn)),
			errhint("coordinator info(host=%s port=%d dbname=%s user=%s)",
				coordhost, coordport, DEFAULT_DB, connect_user)));
	}

	/*remove the add line from coordinator pg_hba.conf*/
	if (breload)
	{
		resetStringInfo(&(getAgentCmdRst.description));
		mgr_send_conf_parameters(AGT_CMD_CNDN_DELETE_PGHBACONF
								,cnpath
								,&infosendmsg
								,coordhostoid
								,&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(WARNING, (errmsg("remove ADB Manager ip \"%s\" from %s coordinator %s/pg_hba,conf fail %s", self_address.data, coordhost, cnpath, getAgentCmdRst.description.data)));
		mgr_reload_conf(coordhostoid, cnpath);
	}

	pfree(coordhost);
	pfree(connect_user);
	pfree(infosendmsg.data);
	pfree(getAgentCmdRst.description.data);
}

static void hexp_get_dn_status(Form_mgr_node mgr_node, Oid tuple_id, DN_STATUS* pdn_status, char* cnpath)
{
	PGconn *dn_pg_conn = NULL;

	Assert(mgr_node);
	Assert(pdn_status);
	Assert(cnpath);

	namestrcpy(&pdn_status->node_status, "");
	namecpy(&pdn_status->nodename, &mgr_node->nodename);
	pdn_status->tid = tuple_id;
	pdn_status->nodemasternameoid = mgr_node->nodemasternameoid;
	pdn_status->nodeincluster = mgr_node->nodeincluster;

	PG_TRY();
	{
		hexp_get_dn_conn((PGconn**)&dn_pg_conn, mgr_node, cnpath);
        if(MgrGetAdbcleanNum(dn_pg_conn) > 0){
			namestrcpy(&pdn_status->node_status, ExpandStatusExpanding);
		}
		else{
			namestrcpy(&pdn_status->node_status, ExpandStatusOnline);
		}

		PQfinish(dn_pg_conn);
		dn_pg_conn = NULL;
	}PG_CATCH();
	{
		if((dn_pg_conn))
		{
			PQfinish((dn_pg_conn));
			(dn_pg_conn) = NULL;
		}
		PG_RE_THROW();
	}PG_END_TRY();
}

static List *
hexp_get_all_dn_status(void)
{
	InitNodeInfo *info;
	ScanKeyData key[2];
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	bool isNull;
	char cnpath[1024];
	Datum datumPath;
	DN_STATUS *dn_status = NULL;
	List *dn_status_list = NIL;

	//select all inicialized node
	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));

	info = palloc(sizeof(*info));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);
	info->rel_scan = heap_beginscan_catalog(info->rel_node, 1, key);
	info->lcp = NULL;

	while ((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);

		if (mgr_node->nodetype != CNDN_TYPE_DATANODE_MASTER)
			continue;

		/*get nodepath from tuple*/
		datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(info->rel_node), &isNull);
		if (isNull)
		{
			heap_endscan(info->rel_scan);
			heap_close(info->rel_node, AccessShareLock);
			pfree(info);

			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
				, errmsg("column nodepath is null")));
		}
		strncpy(cnpath, TextDatumGetCString(datumPath), 1024);

		dn_status = (DN_STATUS *) palloc(sizeof(DN_STATUS));
		hexp_get_dn_status(mgr_node, HeapTupleGetOid(tuple), dn_status, cnpath);
		dn_status_list = lappend(dn_status_list, dn_status);
	}

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
	return dn_status_list;
}

static void hexp_check_expand(void)
{
	PGconn *pg_conn = NULL;
	Oid cnoid;
	int count = 0;

	mgr_get_gtmcoord_conn(MgrGetDefDbName(), &pg_conn, &cnoid);

	if ((count = MgrGetAdbcleanNum(pg_conn)) > 0)
	{
		ereport(ERROR, (errmsg("There is also data num(%d) in the table adb_clean, and the cleaning operation has not been completed since the last expansion, so you cann't start expand operation again.", count)));
	}
    return;
}

static void hexp_parse_pair_lsn(char* strvalue, int* phvalue, int* plvalue)
{
    char* t = NULL;
    char* d = "/";

    t = strtok(strvalue, d);
	CheckNull(t);
    sscanf(t, "%x", phvalue);
    t = strtok(NULL, d);
	CheckNull(t);
    sscanf(t, "%x", plvalue);
}
static void hexp_mgr_pqexec_getlsn(PGconn *pg_conn, char *sqlstr, int* phvalue, int* plvalue)
{
	PGresult *res = NULL;
	char pair_value[50]= {0};

	CheckNull(pg_conn);
	CheckNull(sqlstr);
	CheckNull(phvalue);
	CheckNull(plvalue);

	res = PQexec(pg_conn, sqlstr);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		ereport(ERROR, (errmsg("%s runs error", sqlstr)));
	if (0==PQntuples(res))
		ereport(ERROR, (errmsg("%s runs. resutl is null. the lsn info in replication is not valid. wait a minute, try again.", sqlstr)));
	strcpy(pair_value,PQgetvalue(res, 0, 0));

	hexp_parse_pair_lsn(pair_value, phvalue, plvalue);

	PQclear(res);
	return;
}
void hexp_pqexec_direct_execute_utility(PGconn *pg_conn, char *sqlstr, int ret_type)
{
	PGresult *res;

	Assert((pg_conn)!= 0);

	res = PQexec(pg_conn, sqlstr);

	switch(ret_type)
	{
		case MGR_PGEXEC_DIRECT_EXE_UTI_RET_COMMAND_OK:
			if (PQresultStatus(res) != PGRES_COMMAND_OK)
			{
				PQclear(res);
				ereport(ERROR, (errmsg("%s runs. result is %s.", sqlstr, PQresultErrorMessage(res))));
			}
			break;
		case MGR_PGEXEC_DIRECT_EXE_UTI_RET_TUPLES_TRUE:
			if (PQresultStatus(res) != PGRES_TUPLES_OK)
			{
				PQclear(res);
				ereport(ERROR, (errmsg("%s runs. result is %s.", sqlstr, PQresultErrorMessage(res))));
			}
			if (0==PQntuples(res))
			{
				PQclear(res);
				ereport(ERROR, (errmsg("%s runs. resutl is null.", sqlstr)));
			}
			if (strcasecmp("t", PQgetvalue(res, 0, 0)) != 0)
			{
				PQclear(res);
				ereport(ERROR, (errmsg("%s runs. result is %s.", sqlstr, PQgetvalue(res, 0, 0))));
			}
			break;
		default:
			ereport(ERROR, (errmsg("ret type is error.")));
			break;
	}

	PQclear(res);
	res = NULL;
	return;
}

static void hexp_create_dm_on_itself(PGconn *pg_conn, AppendNodeInfo *nodeinfo)
{
	FormData_mgr_node mgr_node;

	memset(&mgr_node, 0, sizeof(FormData_mgr_node));
	namestrcpy(&mgr_node.nodename, nodeinfo->nodename);
	mgr_node.nodetype = nodeinfo->nodetype;

	if(!mgr_manipulate_pgxc_node_on_node(&pg_conn,
										 1,
										 nodeinfo,
										 &mgr_node,
										 false,
										 PGXC_NODE_MANIPULATE_TYPE_CREATE,
										 NULL))
	{
		ereport(ERROR, (errmsg("on coordinator \"%s\" create node \"%s\" fail", NameStr(mgr_node.nodename), nodeinfo->nodename)));
	}
}

static void hexp_create_dm_on_all_coord(PGconn *pg_conn, AppendNodeInfo *nodeinfo)
{
	InitNodeInfo *info;
	ScanKeyData key[2];
	HeapTuple tuple;
	Form_mgr_node mgr_node;

	//select all inicialized and incluster node
	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));

	ScanKeyInit(&key[1]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));

	info = palloc(sizeof(*info));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);
	info->rel_scan = heap_beginscan_catalog(info->rel_node, 2, key);
	info->lcp = NULL;

	//todo rollback
	while ((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);

		if(!((mgr_node->nodetype==CNDN_TYPE_COORDINATOR_MASTER) ||
			 (mgr_node->nodetype==CNDN_TYPE_GTM_COOR_MASTER)))
			continue;

		if(!mgr_manipulate_pgxc_node_on_node(&pg_conn,
											 1,
											 nodeinfo,
											 mgr_node,
											 false,
											 PGXC_NODE_MANIPULATE_TYPE_CREATE,
											 NULL))
		{
			ereport(ERROR, (errmsg("on coordinator \"%s\" create node \"%s\" fail", NameStr(mgr_node->nodename), nodeinfo->nodename)));
		}
	}

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
}

static void hexp_pgxc_pool_reload_on_all_node(PGconn *pg_conn)
{
	InitNodeInfo *info;
	ScanKeyData key[2];
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	StringInfoData psql_cmd;

	initStringInfo(&psql_cmd);
	//select all inicialized and incluster node
	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));

	ScanKeyInit(&key[1]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));

	info = palloc(sizeof(*info));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);
	info->rel_scan = heap_beginscan_catalog(info->rel_node, 2, key);
	info->lcp = NULL;

	while ((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);

		if(!((mgr_node->nodetype==CNDN_TYPE_DATANODE_MASTER)
			||(mgr_node->nodetype==CNDN_TYPE_COORDINATOR_MASTER)))
			continue;

		initStringInfo(&psql_cmd);
		appendStringInfo(&psql_cmd, " EXECUTE DIRECT ON (%s) ", NameStr(mgr_node->nodename));
		appendStringInfo(&psql_cmd, " 'select pgxc_pool_reload();'");
		hexp_pqexec_direct_execute_utility(pg_conn, psql_cmd.data, MGR_PGEXEC_DIRECT_EXE_UTI_RET_TUPLES_TRUE);
	}

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
}

static bool hexp_get_nodeinfo_from_table(char *node_name, char node_type, AppendNodeInfo *nodeinfo)
{
	InitNodeInfo *info;
	ScanKeyData key[2];
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	Datum datumPath;
	bool isNull = false;

	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodetype
				,BTEqualStrategyNumber
				,F_CHAREQ
				,CharGetDatum(node_type));

	ScanKeyInit(&key[1]
				,Anum_mgr_node_nodename
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,CStringGetDatum(node_name));

	info = (InitNodeInfo *)palloc0(sizeof(InitNodeInfo));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);
	info->rel_scan = heap_beginscan_catalog(info->rel_node, 2, key);
	info->lcp =NULL;

	if ((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) == NULL)
	{
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, AccessShareLock);
		pfree(info);

		return false;
	}

	mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
	Assert(mgr_node);

	nodeinfo->nodename = pstrdup(NameStr(mgr_node->nodename));
	nodeinfo->nodetype = mgr_node->nodetype;
	nodeinfo->nodeaddr = get_hostaddress_from_hostoid(mgr_node->nodehost);
	nodeinfo->nodeusername = get_hostuser_from_hostoid(mgr_node->nodehost);
	nodeinfo->nodeport = mgr_node->nodeport;
	nodeinfo->nodehost = mgr_node->nodehost;
	nodeinfo->nodemasteroid = mgr_node->nodemasternameoid;
	nodeinfo->init = mgr_node->nodeinited;
	nodeinfo->incluster = mgr_node->nodeincluster;


	/*get nodepath from tuple*/
	datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(info->rel_node), &isNull);
	if (isNull)
	{
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, AccessShareLock);
		pfree(info);

		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
			, errmsg("column nodepath is null")));
	}
	nodeinfo->nodepath = pstrdup(TextDatumGetCString(datumPath));

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
	return true;
}

static bool hexp_get_nodeinfo_from_table_byoid(Oid tupleOid, AppendNodeInfo *nodeinfo)
{
	InitNodeInfo *info;
	ScanKeyData key[1];
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	Datum datumPath;
	bool isNull = false;

	ScanKeyInit(&key[0]
		,ObjectIdAttributeNumber
		,BTEqualStrategyNumber
		,F_OIDEQ
		,ObjectIdGetDatum(tupleOid));

	info = (InitNodeInfo *)palloc0(sizeof(InitNodeInfo));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);
	info->rel_scan = heap_beginscan_catalog(info->rel_node, 1, key);
	info->lcp =NULL;

	if ((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) == NULL)
	{
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, AccessShareLock);
		pfree(info);

		return false;
	}

	mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
	Assert(mgr_node);

	nodeinfo->nodename = pstrdup(NameStr(mgr_node->nodename));
	nodeinfo->nodetype = mgr_node->nodetype;
	nodeinfo->nodeaddr = get_hostaddress_from_hostoid(mgr_node->nodehost);
	nodeinfo->nodeusername = get_hostuser_from_hostoid(mgr_node->nodehost);
	nodeinfo->nodeport = mgr_node->nodeport;
	nodeinfo->nodehost = mgr_node->nodehost;
	nodeinfo->nodemasteroid = mgr_node->nodemasternameoid;
	nodeinfo->init = mgr_node->nodeinited;
	nodeinfo->incluster = mgr_node->nodeincluster;

	/*get nodepath from tuple*/
	datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(info->rel_node), &isNull);
	if (isNull)
	{
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, AccessShareLock);
		pfree(info);

		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
			, errmsg("column nodepath is null")));
	}
	nodeinfo->nodepath = pstrdup(TextDatumGetCString(datumPath));

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
	return true;
}

static void hexp_set_expended_node_state(char *nodename, bool search_init, bool search_incluster, bool value_init, bool value_incluster, Oid src_oid)
{
	InitNodeInfo *info;
	ScanKeyData key[4];
	HeapTuple tuple;
	Form_mgr_node mgr_node;

	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodename
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,CStringGetDatum(nodename));

	ScanKeyInit(&key[1]
				,Anum_mgr_node_nodetype
				,BTEqualStrategyNumber
				,F_CHAREQ
				,CharGetDatum(CNDN_TYPE_DATANODE_MASTER));

	ScanKeyInit(&key[2]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(search_init));

	ScanKeyInit(&key[3]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(search_incluster));

	info = palloc(sizeof(*info));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);
	info->rel_scan = heap_beginscan_catalog(info->rel_node, 4, key);
	info->lcp =NULL;

	tuple = heap_getnext(info->rel_scan, ForwardScanDirection);
	if(tuple == NULL)
	{
		ereport(ERROR, (errmsg("The node can not be found in last step.")));

		/* end of row */
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, AccessShareLock);
		pfree(info);
		return ;
	}

	mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
	Assert(mgr_node);

	mgr_node->nodeinited = value_init;
	mgr_node->nodeincluster = value_incluster;
	if(value_init&&value_incluster)
	{
		mgr_node->nodemasternameoid = 0;
		mgr_node->allowcure = true;
		namestrcpy(&mgr_node->curestatus, CURE_STATUS_NORMAL);
	}

	if(value_init&&(!value_incluster))
		mgr_node->nodemasternameoid = src_oid;

	heap_inplace_update(info->rel_node, tuple);

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
}

bool get_agent_info_from_hostoid(const Oid hostOid, char *agent_addr, int *agent_port)
{
	Relation rel;
	HeapTuple tuple;
	Datum datum_addr;
	Datum datum_port;
	bool isNull = false;
	Assert(agent_addr);
	rel = heap_open(HostRelationId, AccessShareLock);
	tuple = SearchSysCache1(HOSTHOSTOID, ObjectIdGetDatum(hostOid));
	/*check the host exists*/
	if (!HeapTupleIsValid(tuple))
	{
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION)
		,errmsg("cache lookup failed for relation %u", hostOid)));
	}
	datum_addr = heap_getattr(tuple, Anum_mgr_host_hostaddr, RelationGetDescr(rel), &isNull);
	if(isNull)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
			, errmsg("column hostaddr is null")));
	sprintf(agent_addr, "%s", TextDatumGetCString(datum_addr));
	datum_port = heap_getattr(tuple, Anum_mgr_host_hostagentport, RelationGetDescr(rel), &isNull);
	if(isNull)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
			, errmsg("column agentport is null")));
	*agent_port = DatumGetInt32(datum_port);

	ReleaseSysCache(tuple);
	heap_close(rel, AccessShareLock);
	return true;
}

static struct enum_sync_state sync_state_tab[] =
{
	{SYNC_STATE_SYNC, "sync"},
	{SYNC_STATE_ASYNC, "async"},
	{SYNC_STATE_POTENTIAL, "potential"},
	{-1, NULL}
};

/*
* inner function, userd for node failover
*/
Datum mgr_failover_one_dn_inner_func(char *nodename, char cmdtype, char nodetype, bool nodetypechange, bool bforce)
{
	Relation rel_node;
	HeapTuple aimtuple;
	HeapTuple tup_result;
	GetAgentCmdRst getAgentCmdRst;
	char *nodestring;
	char *host_addr;
	char *user;
	Form_mgr_node mgr_node;
	StringInfoData port;
	int ret;

	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	nodestring = mgr_nodetype_str(nodetype);
	//aimtuple = mgr_get_tuple_node_from_name_type(rel_node, nodename, nodetype);
	aimtuple = mgr_get_tuple_node_from_name_type(rel_node, nodename);
	if (!HeapTupleIsValid(aimtuple))
	{
		heap_close(rel_node, RowExclusiveLock);
		ereport(ERROR, (errmsg("%s \"%s\" does not exist", nodestring, nodename)));
	}
	/*check node is running normal and sync*/
	if (!nodetypechange)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(aimtuple);
		Assert(mgr_node);
		if ((!bforce) && strcmp(NameStr(mgr_node->nodesync), sync_state_tab[SYNC_STATE_SYNC].name) != 0)
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
				,errmsg("%s \"%s\" is async mode", nodestring, nodename)
				,errhint("you can add \'force\' at the end, and enforcing execute failover")));
		}
		/*check running normal*/
		host_addr = get_hostaddress_from_hostoid(mgr_node->nodehost);
		initStringInfo(&port);
		appendStringInfo(&port, "%d", mgr_node->nodeport);
		user = get_hostuser_from_hostoid(mgr_node->nodehost);
		ret = pingNode_user(host_addr, port.data, user);
		pfree(user);
		pfree(port.data);
		pfree(host_addr);
		if(ret != 0)
			ereport(ERROR, (errmsg("%s \"%s\" is not running normal", nodestring, nodename)));
	}
	pfree(nodestring);
	initStringInfo(&(getAgentCmdRst.description));
	mgr_runmode_cndn_get_result(cmdtype, &getAgentCmdRst, rel_node, aimtuple, TAKEPLAPARM_N);
	heap_freetuple(aimtuple);
	namestrcpy(&(getAgentCmdRst.nodename),nodename);
	tup_result = build_common_command_tuple(
		&(getAgentCmdRst.nodename)
		, getAgentCmdRst.ret
		, getAgentCmdRst.description.data);
	ereport(LOG, (errmsg("the command for failover:\nresult is: %s\ndescription is: %s\n", getAgentCmdRst.ret == true ? "true" : "false", getAgentCmdRst.description.data)));
	pfree(getAgentCmdRst.description.data);
	heap_close(rel_node, RowExclusiveLock);
	return HeapTupleGetDatum(tup_result);
}
static List* MgrGetAdbCleanDbName(PGconn *pg_conn)
{
	int loop = 0;
	ExecStatusType status;
	PGresult *res;
	NameData *dbname;
	List 	 *dbname_list = NIL;

	CheckNullRetrunRet(pg_conn, NIL);

	res = PQexec(pg_conn, SELECT_DBNAME_ADBCLEAN);
	status = PQresultStatus(res);
	switch(status)
	{
		case PGRES_TUPLES_OK:
			break;
		default:
			ereport(ERROR, (errmsg("\"%s\" runs error. result is %s.", SELECT_DBNAME_ADBCLEAN, PQresultErrorMessage(res))));
	}

	if (0==PQntuples(res))
	{
		PQclear(res);
		ereport(ERROR, (errmsg("No database need expand clean. \"%s\" runs result is null.", SELECT_DBNAME_ADBCLEAN)));
	}
    for (loop=0; loop<PQntuples(res); loop++)
	{
		dbname = (Name)palloc0(sizeof(NameData));
		namestrcpy(dbname, PQgetvalue(res, loop, 0));
		dbname_list = lappend(dbname_list, dbname);
	}

	PQclear(res);
    return dbname_list;
}
static void MgrFreeDBName(List *dbname_list)
{
	ListCell	*lc;
	Name 		dbname;

	foreach (lc, dbname_list)
	{
		dbname = (Name)lfirst(lc);
		MgrFree(dbname);
	}
}
static void MgrFreeClean(PGconn *co_pg_conn, PGconn *other_conn, List *dbname_list)
{
	ClosePgConn(co_pg_conn);
	ClosePgConn(other_conn);
	MgrFreeDBName(dbname_list);
}
char *MgrGetDefDbName(void)
{
	char* database;
	if(0!=strcmp(MGRDatabaseName,""))
		database = MGRDatabaseName;
	else
		database = DEFAULT_DB;
	return database;
}