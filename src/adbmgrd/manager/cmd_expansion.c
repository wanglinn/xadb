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
char *DefaultDatabaseName = DEFAULT_DB;


/*hot expansion definition begin*/
#define SlotStatusInvalid		-1
#define SlotStatusOnlineInDB	1
#define SlotStatusMoveInDB		2
#define SlotStatusCleanInDB		3
#define SlotStatusExpand		4
#define SlotStatusMoveHalfWay	5

char* 	MsgSlotStatus[5] =
	{
	"Online",
	"Move",
	"Clean",
	"Expand",
	"MoveHalfWay"
	};


typedef struct DN_STATUS
{
	NameData	nodename;
	Oid			tid;

	int			online_count;
	int			move_count;
	int			clean_count;

	bool 		enable_mvcc;

	Oid			nodemasternameoid;
	bool		nodeincluster;

	bool 		checked;

	int 		node_status;

	NameData	pgxc_node_name;
} DN_STATUS;

typedef struct DN_NODE
{
	NameData	nodename;
	NameData	port;
	NameData	host;
} DN_NODE;

#define INVALID_ID	-1
#define VALID_ID	1

/*
ADBSQL
*/
/*adb schema*/
#define IS_ADB_SCHEMA_EXISTS						"select count(*) from pg_namespace where nspname = 'adb';"
#define CREATE_SCHEMA 								"create schema if not exists adb;"


#define ADB_CLEAN_TABLE						        "adb_clean"
#define IS_ADB_CLEAN_TABLE_EXISTS 				    "select count(*) from pg_class pgc, pg_namespace pgn where pgn.nspname = 'adb' and pgc.relname = 'adb_clean' and pgc.relnamespace = pgn.oid;"

#define SELECT_PGXC_NODE_THROUGH_COOR 				"execute direct on(%s) 'select node_name from pgxc_node n where node_type=''D'' order by node_name';"
#define SELECT_PGXC_NODE 							"select node_name, node_host, node_port from pgxc_node n where node_type='D' order by node_name;"

/*postgres.conf*/
#define SHOW_ADB_SLOT_ENABLE_MVCC 					"show adb_slot_enable_mvcc;"
#define SHOW_PGXC_NODE_NAME 						"show pgxc_node_name;"

/*import*/
#define SELECT_HASH_TABLE_COUNT						"select count(*) from pg_class pg, pgxc_class xc where pg.oid=xc.pcrelid and pclocatortype = 'B';"
#define SELECT_HASH_TABLE_COUNT_THROUGH_CO			"execute direct on (%s) 'select count(*) from pg_class pg, pgxc_class xc where pg.oid=xc.pcrelid and pclocatortype = ''B''';"
#define SELECT_HASH_TABLE_DETAIL 					"select nspname, relname, pclocatortype, pcattnum, pchashalgorithm, pchashbuckets, nodeoids from pg_class pg, pgxc_class xc , pg_namespace pgn where pg.oid=xc.pcrelid and pclocatortype = 'B' and pg.relnamespace = pgn.oid;"
#define SELECT_HASH_TABLE_DETAIL_THROUGH_CO 		"execute direct on (d1m) 'select nspname, relname, pclocatortype, pcattnum, pchashalgorithm, pchashbuckets, nodeoids from pg_class pg, pgxc_class xc , pg_namespace pgn where pg.oid=xc.pcrelid and pclocatortype = ''B'' and pg.relnamespace = pgn.oid;';"
#define SELECT_REL_ID_TROUGH_CO						"execute direct on (%s) 'select pgc.oid from pg_class pgc, pg_namespace pgn where pgc.relnamespace = pgn.oid and nspname = ''%s'' and relname = ''%s''';"
#define INSERT_PGXC_CLASS_THROUGH_CO 				"execute direct on (%s)'INSERT INTO pgxc_class(pcrelid, pclocatortype, pcattnum, pchashalgorithm, pchashbuckets, nodeoids, pcfuncid, pcfuncattnums) VALUES (%s, ''%s'', %s, %s, %s, ''%s'', 0, ''0'');';"

#define SELECT_HASH_TABLE_FOR_MATCH 				"select nspname, relname, pcattnum from pg_class pg, pgxc_class xc , pg_namespace pgn where pg.oid=xc.pcrelid and pclocatortype = 'B' and pg.relnamespace = pgn.oid;"
#define SELECT_HASH_TABLE_FOR_MATCH_THROUGH_CO		"execute direct on (%s) 'select count(*) from pg_class pg, pgxc_class xc , pg_namespace pgn where pg.oid=xc.pcrelid and pclocatortype = ''B'' and pg.relnamespace = pgn.oid and nspname = ''%s'' and relname = ''%s'' and pcattnum = %s;';"

#define SQL_XC_MAINTENANCE_MODE_ON 					"set xc_maintenance_mode = on;"
#define SQL_XC_MAINTENANCE_MODE_OFF 				"set xc_maintenance_mode = off;"


/*hot expansion definition end*/
static bool hexp_get_nodeinfo_from_table(char *node_name, char node_type, AppendNodeInfo *nodeinfo);
static void hexp_create_dm_on_all_node(PGconn *pg_conn, AppendNodeInfo *nodeinfo);
static void hexp_create_dm_on_itself(PGconn *pg_conn, AppendNodeInfo *nodeinfo);
static void hexp_set_expended_node_state(char *nodename, bool search_init, bool search_incluster, bool value_init, bool value_incluster, Oid src_oid);
static bool hexp_get_nodeinfo_from_table_byoid(Oid tupleOid, AppendNodeInfo *nodeinfo);
static void hexp_get_coordinator_conn(PGconn **pg_conn, Oid *cnoid);
static void hexp_mgr_pqexec_getlsn(PGconn **pg_conn, char *sqlstr, int* phvalue, int* plvalue);
static void hexp_parse_pair_lsn(char* strvalue, int* phvalue, int* plvalue);
static void hexp_check_dn_pgxcnode_info(PGconn *pg_conn, char *nodename, StringInfoData* pnode_list_exists);
static void hexp_check_cluster_pgxcnode(void);
static List *hexp_get_all_dn_status(void);
static void hexp_get_dn_status(Form_mgr_node mgr_node, Oid tuple_id, DN_STATUS* pdn_status, char* cnpath);
static void hexp_get_dn_conn(PGconn **pg_conn, Form_mgr_node mgr_node, char* cnpath);
static int 	hexp_find_dn_nodes(List *dn_node_list, char* nodename);
static List *hexp_init_dn_nodes(PGconn *pg_conn);
static int 	hexp_select_result_count(PGconn *pg_conn, char* sql);
static void hexp_check_expand();
static void hexp_update_conf_pgxc_node_name(AppendNodeInfo node, char* newname);
static void hexp_restart_node(AppendNodeInfo node);
static void hexp_pgxc_pool_reload_on_all_node(PGconn *pg_conn);
static Datum hexp_expand_check_show_status(bool check);
static bool hexp_check_cluster_status_internal(List **pdn_status_list, StringInfo pserialize, bool check);
static void hexp_execute_cmd_get_reloid(PGconn *pg_conn, char *sqlstr, char* ret);
static void hexp_import_hash_meta(PGconn *pgconn, PGconn *pgconn_dn, char* node_name);
static void hexp_check_hash_meta(void);
static void hexp_check_hash_meta_dn(PGconn *pgconn, PGconn *pgconn_dn, char* node_name);

/*
 * expand sourcenode to destnode
 */
Datum mgr_expand_activate_dnmaster(PG_FUNCTION_ARGS)
{
	AppendNodeInfo appendnodeinfo;
	AppendNodeInfo srcnodeinfo;
	StringInfoData  infosendmsgsrc;
	StringInfoData  infosendmsgdst;
	StringInfoData  strinfo;
	NameData nodename;
	const int max_pingtry = 60;
	char nodeport_buf[10];
	HeapTuple tup_result;
	char srcport_buf[10];
	char dstport_buf[10];
	GetAgentCmdRst getAgentCmdRst;
	bool result = true;
	bool findtuple = false;
	bool isAddHbaSrc = false;
	bool isAddHbaDst = false;
	PGconn * src_pg_conn = NULL;
	PGconn * dst_pg_conn = NULL;
	PGconn * co_pg_conn = NULL;
	int src_lsn_high = 0;
	int src_lsn_low = 0;
	int dst_lsn_high = 0;
	int dst_lsn_low = 0;
	int try = 0;
	Oid cnoid;

	char phase1_msg[100];
	char phase3_msg[256];

	char* database ;
	if(0!=strcmp(MGRDatabaseName,""))
		database = MGRDatabaseName;
	else
		database = DEFAULT_DB;

	strcpy(phase1_msg, "phase1--if this command failed, there's nothing need to do. the command:");
	strcpy(phase3_msg, "phase3--if this command failed, use 'expand activate recover promote success' to recover. the command:");

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot execute this command during recovery")));

	memset(&appendnodeinfo, 0, sizeof(AppendNodeInfo));

	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsgsrc);
	initStringInfo(&infosendmsgdst);
	initStringInfo(&strinfo);
	appendnodeinfo.nodename = PG_GETARG_CSTRING(0);
	Assert(appendnodeinfo.nodename);

	namestrcpy(&nodename, appendnodeinfo.nodename);

	PG_TRY();
	{
		ereport(INFO, (errmsg("%s%s", phase1_msg, "check src node and dst node status.")));
		/*
		1.1 check dst node status.it exists and is inicilized but not in cluster
		*/
		findtuple = hexp_get_nodeinfo_from_table(appendnodeinfo.nodename, CNDN_TYPE_DATANODE_MASTER, &appendnodeinfo);
		if(!findtuple)
			ereport(ERROR, (errmsg("The node %s does not exist.", appendnodeinfo.nodename)));

		if(!((appendnodeinfo.init) && (!appendnodeinfo.incluster)))
			ereport(ERROR, (errmsg("The node %s status is error. It should be initialized and not in cluster.", appendnodeinfo.nodename)));

		/*
		1.2 check src node status.
		*/
		findtuple = hexp_get_nodeinfo_from_table_byoid(appendnodeinfo.nodemasteroid, &srcnodeinfo);
		if(!findtuple)
			ereport(ERROR, (errmsg("The node %s does not exist.tuple id is %d", appendnodeinfo.nodename, appendnodeinfo.nodemasteroid)));

		/*
		1.3 check all dn and co are running.
		*/
		ereport(INFO, (errmsg("%s%s", phase1_msg, "check all dn and co are running.")));
		mgr_make_sure_all_running(CNDN_TYPE_COORDINATOR_MASTER);
		mgr_make_sure_all_running(CNDN_TYPE_DATANODE_MASTER);

		//check global and node status
		ereport(INFO, (errmsg("%s%s", phase1_msg, "expand check status.")));
		hexp_check_expand();

		/*
		2.1 get dst lsn
		*/
		ereport(INFO, (errmsg("%s%s", phase1_msg, "get dst lsn.")));
		sprintf(dstport_buf, "%d", appendnodeinfo.nodeport);
		dst_pg_conn = PQsetdbLogin(appendnodeinfo.nodeaddr,
						dstport_buf,
						NULL, NULL,database,
						appendnodeinfo.nodeusername,NULL);
		if (dst_pg_conn == NULL || PQstatus((PGconn*)dst_pg_conn) != CONNECTION_OK)
		{
			/* update dst pg_hba.conf */
			isAddHbaSrc = AddHbaIsValid(&appendnodeinfo, &infosendmsgdst);
			dst_pg_conn = PQsetdbLogin(appendnodeinfo.nodeaddr,
						dstport_buf,
						NULL, NULL,database,
						appendnodeinfo.nodeusername,NULL);
			if (dst_pg_conn == NULL || PQstatus((PGconn*)dst_pg_conn) != CONNECTION_OK)
				ereport(ERROR,
					(errmsg("Fail to connect to expend dst datanode %s", PQerrorMessage((PGconn*)dst_pg_conn)),
						errhint("info(host=%s port=%d dbname=%s user=%s)",
						appendnodeinfo.nodeaddr, appendnodeinfo.nodeport, DEFAULT_DB, appendnodeinfo.nodeusername)));
		}

		hexp_mgr_pqexec_getlsn(&dst_pg_conn, "select pg_last_wal_replay_lsn();",&dst_lsn_high, &dst_lsn_low);

		/*
		2.2get src lsn
		*/
		ereport(INFO, (errmsg("%s%s", phase1_msg, "get src lsn.")));
		sprintf(srcport_buf, "%d", srcnodeinfo.nodeport);
		src_pg_conn = PQsetdbLogin(srcnodeinfo.nodeaddr,
						srcport_buf,
						NULL, NULL,database,
						srcnodeinfo.nodeusername,NULL);
		if (src_pg_conn == NULL || PQstatus((PGconn*)src_pg_conn) != CONNECTION_OK)
		{
			/* update src pg_hba.conf */
			isAddHbaSrc = AddHbaIsValid(&srcnodeinfo, &infosendmsgsrc);
			src_pg_conn = PQsetdbLogin(srcnodeinfo.nodeaddr,
						srcport_buf,
						NULL, NULL,database,
						srcnodeinfo.nodeusername,NULL);
			if (src_pg_conn == NULL || PQstatus((PGconn*)src_pg_conn) != CONNECTION_OK)
				ereport(ERROR,
					(errmsg("Fail to connect to expend src datanode %s", PQerrorMessage((PGconn*)src_pg_conn)),
						errhint("info(host=%s port=%d dbname=%s user=%s)",
						srcnodeinfo.nodeaddr, srcnodeinfo.nodeport, DEFAULT_DB, srcnodeinfo.nodeusername)));
		}
		hexp_mgr_pqexec_getlsn(&src_pg_conn, "select pg_current_wal_lsn();",&src_lsn_high, &src_lsn_low);

		/*
		2.3 check lsn lag between src and dst is 8M.
		*/
		ereport(INFO, (errmsg("%s%s", phase1_msg, "check lsn lag between src and dst is 8M.")));
		if(!((src_lsn_high==dst_lsn_high) && ((src_lsn_low-dst_lsn_low)>=0) &&((src_lsn_low-dst_lsn_low)<=8388608)))
			ereport(ERROR, (errmsg("the lsn lag between src node and dst node is longer than 8M.src lsn is %x/%x, dst lsn is %x/%x", src_lsn_high,src_lsn_low,dst_lsn_high,dst_lsn_low)));

		//check global and node status againt cluster lock
		ereport(INFO, (errmsg("%s%s", phase1_msg, "expand check status.")));
		hexp_check_expand();

		/*
		2.4 wait 20s for sync
		*/
		ereport(INFO, (errmsg("%s%s", phase1_msg, "lock cluster and wait 20s for sync.")));
		try=20;
		for(;;)
		{
			if((src_lsn_high==dst_lsn_high) && (src_lsn_low==dst_lsn_low))
				break;
			hexp_mgr_pqexec_getlsn(&dst_pg_conn, "select pg_last_wal_replay_lsn();",&dst_lsn_high, &dst_lsn_low);
			hexp_mgr_pqexec_getlsn(&src_pg_conn, "select pg_current_wal_lsn();",&src_lsn_high, &src_lsn_low);

			pg_usleep(1000000L);
			try--;
			if(try==0)
				break;
		}

		if(!((src_lsn_high==dst_lsn_high) && (src_lsn_low==dst_lsn_low)))
			ereport(ERROR, (errmsg("expend src node and dst node can not sync in %d seconds", try)));
		/*
		3.promote&check connect
		*/
		ereport(INFO, (errmsg("promote dst node. if it fails, check dst status by hand.it cann't be revoked if promotion really fails.you have to drop the node and directory, then do expand from beginning.")));
		mgr_failover_one_dn_inner_func(appendnodeinfo.nodename,
			AGT_CMD_DN_MASTER_PROMOTE,
			CNDN_TYPE_DATANODE_MASTER,
			true, false);

		/*
		4.update pgxc node name in postgresql.conf in dst node.
		*/
		ereport(INFO, (errmsg("update pgxc node name in postgresql.conf in dst node.if this step fails, do it by hand, then restart the node")));
		hexp_update_conf_pgxc_node_name(appendnodeinfo, appendnodeinfo.nodename);

		//wait 60s for restart
		ereport(INFO, (errmsg("restart dst node. if this step fails, do it by hand.")));
		hexp_restart_node(appendnodeinfo);
		try=60;
		for(;;)
		{
			if (is_node_running(appendnodeinfo.nodeaddr, appendnodeinfo.nodeport, appendnodeinfo.nodeusername, appendnodeinfo.nodetype))
				break;
			pg_usleep(1000000L);
			try--;
			if(try==0)
				break;
		}
		if (!is_node_running(appendnodeinfo.nodeaddr, appendnodeinfo.nodeport, appendnodeinfo.nodeusername, appendnodeinfo.nodetype))
			ereport(ERROR, (errmsg("expend dst node %s can not restart in %d seconds",appendnodeinfo.nodename, 60)));

		/*
		5.add dst node to all other node's pgxc_node.
		*/
		mgr_lock_cluster_involve_gtm_coord(&co_pg_conn, &cnoid);
	 	hexp_pqexec_direct_execute_utility(co_pg_conn,SQL_BEGIN_TRANSACTION , MGR_PGEXEC_DIRECT_EXE_UTI_RET_COMMAND_OK);
		
		ereport(INFO, (errmsg("add dst node to all other node's pgxc_node.if this step fails, use 'expand activate recover promote success dst' to recover.")));
		//create new node on all node which is initilized and incluster
		hexp_create_dm_on_all_node(co_pg_conn, &appendnodeinfo);
		hexp_create_dm_on_itself(co_pg_conn, &appendnodeinfo);
		hexp_pgxc_pool_reload_on_all_node(co_pg_conn);
		
		MgrSendAlterNodeDataToGtm(co_pg_conn, srcnodeinfo.nodename, appendnodeinfo.nodename);
		
		hexp_pqexec_direct_execute_utility(co_pg_conn,SQL_COMMIT_TRANSACTION , MGR_PGEXEC_DIRECT_EXE_UTI_RET_COMMAND_OK);
       	mgr_unlock_cluster_involve_gtm_coord(&co_pg_conn);

		//5.update dst node init and in cluster, and parent node is empty.
		ereport(INFO, (errmsg("update dst node init and in cluster, and parent node is empty.if this step fails, use 'expand activate recover promote success dst' to recover.")));
		hexp_set_expended_node_state(appendnodeinfo.nodename, true, false,  true, true, 0);

		PQfinish(dst_pg_conn);
		dst_pg_conn = NULL;
		PQfinish(src_pg_conn);
		src_pg_conn = NULL;
	}PG_CATCH();
	{
		if(dst_pg_conn)
		{
			PQfinish(dst_pg_conn);
			dst_pg_conn = NULL;
		}
		if(src_pg_conn)
		{
			PQfinish(src_pg_conn);
			src_pg_conn = NULL;
		}
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

    if (result)
	{
		tup_result = build_common_command_tuple(&nodename, true, "success");
	}
	else
	{
		tup_result = build_common_command_tuple(&nodename, result, getAgentCmdRst.description.data);
	}

	if (isAddHbaSrc)
		RemoveHba(&appendnodeinfo, &infosendmsgsrc);
	if (isAddHbaDst)
		RemoveHba(&appendnodeinfo, &infosendmsgdst);
	pfree(infosendmsgsrc.data);
	pfree(infosendmsgdst.data);
	pfree(getAgentCmdRst.description.data);
	pfree_AppendNodeInfo(appendnodeinfo);
	pfree(strinfo.data);

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

	char phase1_msg[100];
	strcpy(phase1_msg, "phase1--if this step fails, nothing to revoke. step command:");

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

		//1.check src node and dst node status.
		ereport(INFO, (errmsg("%s%s", phase1_msg, "check src node and dst node status.")));
		/*
		1.1 check dst node status.it exists and is inicilized but not in cluster
		*/
		findtuple = hexp_get_nodeinfo_from_table(appendnodeinfo.nodename, CNDN_TYPE_DATANODE_MASTER, &appendnodeinfo);
		if(!findtuple)
			ereport(ERROR, (errmsg("The node does not exist.")));

		if(!((appendnodeinfo.init) && (!appendnodeinfo.incluster)))
			ereport(ERROR, (errmsg("The node status is error. It should be initialized and not in cluster.")));

		/*
		1.2 check src node status.
		*/
		findtuple = hexp_get_nodeinfo_from_table_byoid(appendnodeinfo.nodemasteroid, &srcnodeinfo);
		if(!findtuple)
			ereport(ERROR, (errmsg("The node does not exist.tuple id is %d", appendnodeinfo.nodemasteroid)));

		/*
		1.3 check all dn and co are running.
		*/
		ereport(INFO, (errmsg("%s%s", phase1_msg, "check all dn and co are running.")));
		mgr_make_sure_all_running(CNDN_TYPE_COORDINATOR_MASTER);
		mgr_make_sure_all_running(CNDN_TYPE_DATANODE_MASTER);



		//phase2 recover
		hexp_get_coordinator_conn(&co_pg_conn, &cnoid);

		/*
		5.add dst node to all other node's pgxc_node.
		*/
		ereport(INFO, (errmsg("add dst node to all other node's pgxc_node.if this step fails, do it by hand.")));
		hexp_pqexec_direct_execute_utility(co_pg_conn,SQL_XC_MAINTENANCE_MODE_ON , MGR_PGEXEC_DIRECT_EXE_UTI_RET_COMMAND_OK);
		//create new node on all node which is initilized and incluster
		hexp_create_dm_on_all_node(co_pg_conn, &appendnodeinfo);
		hexp_create_dm_on_itself(co_pg_conn, &appendnodeinfo);
		hexp_pqexec_direct_execute_utility(co_pg_conn,SQL_XC_MAINTENANCE_MODE_OFF , MGR_PGEXEC_DIRECT_EXE_UTI_RET_COMMAND_OK);


		//flush slot info in all nodes(includes new node)
		hexp_pqexec_direct_execute_utility(co_pg_conn, "flush slot;", MGR_PGEXEC_DIRECT_EXE_UTI_RET_COMMAND_OK);
		mgr_unlock_cluster_involve_gtm_coord(&co_pg_conn);


		//5.update dst node init and in cluster, and parent node is empty.
		ereport(INFO, (errmsg("update dst node init and in cluster, and parent node is empty.")));
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
	StringInfoData primary_conninfo_value;
	StringInfoData recorderr;
	NameData nodename;
	NameData gtmMasterNameData;
	HeapTuple tup_result;
	GetAgentCmdRst getAgentCmdRst;
	const int max_pingtry = 60;
	char nodeport_buf[10];
	bool findtuple;
	PGconn * co_pg_conn = NULL;
	char phase1_msg[100];
	char phase3_msg[256];
	char *gtmMasterName;

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot execute this command during recovery")));

	strcpy(phase1_msg, "phase1--if this command failed, there's nothing need to do. the command:");
	strcpy(phase3_msg, "phase3--if this command failed, use 'expand recover basebackup success src to dst' to recover. the command:");

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
		ereport(INFO, (errmsg("%s.%s", phase1_msg, "check src node and dst node status.")));
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
			ereport(ERROR, (errmsg("The node %s does not exist.", destnodeinfo.nodename)));
		}
		if(!((!destnodeinfo.incluster)&&(!destnodeinfo.init)))
		{
			ereport(ERROR, (errmsg("The node %s status is error. It should be not initialized and not in cluster.", destnodeinfo.nodename)));
		}

		/*
		1.3 all dn and co are running.
		*/
		mgr_make_sure_all_running(CNDN_TYPE_COORDINATOR_MASTER);
		mgr_make_sure_all_running(CNDN_TYPE_DATANODE_MASTER);

		//check src node status
		hexp_check_expand();

		/*
		2. check gtm status and add dst info into gtm hba.
		*/
		ereport(INFO, (errmsg("%s.%s", phase1_msg, "check gtm status and add dst info into gtm hba.")));

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
			}
			else
				{	ereport(ERROR, (errmsg("gtm master is not running")));}
		}
		else
		{	ereport(ERROR, (errmsg("gtm master is not initialized")));}

		/* for gtm slave */
		mgr_add_hbaconf(CNDN_TYPE_GTM_COOR_SLAVE, "all", destnodeinfo.nodeaddr);

		/*
		3.add dst node ip and account into src node hba
		*/
		ereport(INFO, (errmsg("%s.%s", phase1_msg, "add dst node ip and account into src node hba.")));
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

		/*
		4.check dst node basebackup dir does not exist.
		*/
		ereport(INFO, (errmsg("%s.%s", phase1_msg, "check dst node basebackup dir does not exist.if this step fails , you should check the dir.")));
		mgr_check_dir_exist_and_priv(destnodeinfo.nodehost, destnodeinfo.nodepath);

		/*
		6.basebackup
		*/
		ereport(INFO, (errmsg("phase2--basebackup.If the command failed, you must delete directory by hand.If you don't see the rollback success message, you can do it by expand recover basebackup fail src to dst.")));
		mgr_pgbasebackup(CNDN_TYPE_DATANODE_MASTER, &destnodeinfo, &sourcenodeinfo);
		ereport(INFO, (errmsg("phase2--basebackup suceess.")));

		//phase 3. if errors occur, redo those.
		/*
		7.update dst node postgres.conf
		*/
		ereport(INFO, (errmsg("%s.%s", phase3_msg, "update dst node postgres.conf.")));
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
		ereport(INFO, (errmsg("%s.%s", phase3_msg, "update dst node recovery.conf.")));
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
		ereport(INFO, (errmsg("%s.%s", phase3_msg, "start datanode.")));
		mgr_start_node(CNDN_TYPE_DATANODE_MASTER, destnodeinfo.nodepath, destnodeinfo.nodehost);

		/*
		10.update node status initialized but not in cluster.
		*/
		ereport(INFO, (errmsg("last step to update mgr info.if failed, you can stop the node, and use 'expand recover basebackup success src to dst' recover")));
		hexp_set_expended_node_state(destnodeinfo.nodename, false, false,  true, false, sourcenodeinfo.tupleoid);

		ereport(INFO, (errmsg("expend success.")));

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
	PGconn * co_pg_conn = NULL;
	char phase1_msg[100];
	char phase3_msg[100];

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot execute this command during recovery")));

	strcpy(phase1_msg, "phase1--if this step fails, nothing to revoke. step command:");
	strcpy(phase3_msg, "phase3--if this step fails, use XXX. step command:");

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
		ereport(INFO, (errmsg("%s.%s", phase1_msg, "check src node and dst node status.")));
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
		ereport(INFO, (errmsg("%s.%s", phase3_msg, "update dst node postgres.conf.")));
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
		ereport(INFO, (errmsg("%s.%s", phase3_msg, "update dst node recovery.conf.")));
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
		ereport(INFO, (errmsg("%s.%s", phase3_msg, "start datanode.")));
		mgr_start_node(CNDN_TYPE_DATANODE_MASTER, destnodeinfo.nodepath, destnodeinfo.nodehost);

		/*
		10.update node status initialized but not in cluster.
		*/
		ereport(INFO, (errmsg("last step to update mgr info.if failed, can update by ***")));
		hexp_set_expended_node_state(destnodeinfo.nodename, false, false,  true, false, sourcenodeinfo.tupleoid);

		ereport(INFO, (errmsg("expend success.")));

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
	PGconn * co_pg_conn = NULL;
	char phase1_msg[100];
	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot execute this command during recovery")));

	strcpy(phase1_msg, "if this step fails, nothing to revoke. step command:");

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
		ereport(INFO, (errmsg("%s.%s", phase1_msg, "check src node and dst node status.")));
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
		if(co_pg_conn)
		{
			PQfinish(co_pg_conn);
			co_pg_conn = NULL;
		}
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
	Oid cnoid;
	HeapTuple tup_result = NULL;
	char ret_msg[100];
	NameData nodename;
	bool is_vacuum_state = false;
	
	strcpy(nodename.data, "---");
	strcpy(ret_msg, "expand clean success.");
	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot execute this command during recovery")));

	PG_TRY();
	{
		mgr_get_gtmcoord_conn(&co_pg_conn, &cnoid);
		Assert(cnoid);

		MgrSendDataCleanToGtm(co_pg_conn);
		
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

	tup_result = build_common_command_tuple(&nodename, true, ret_msg);
	return HeapTupleGetDatum(tup_result);
}

Datum mgr_cluster_pgxcnode_check(PG_FUNCTION_ARGS)
{
	HeapTuple tup_result;
	char ret_msg[100];
	NameData nodename;

	strcpy(nodename.data, "---");
	strcpy(ret_msg, "pgxc node info in cluster is consistent.this info in all datanode is same with coordinator's.");
	hexp_check_cluster_pgxcnode();

	tup_result = build_common_command_tuple(&nodename, true, ret_msg);
	return HeapTupleGetDatum(tup_result);
}

Datum mgr_import_hash_meta(PG_FUNCTION_ARGS)
{
	HeapTuple tup_result;
	NameData nodename;
	char 	ret_msg[100];
	char* 	node_name;
	char 	sql[300];
	PGconn *pg_conn = NULL;
	PGconn *pg_conn_dn = NULL;
	Oid 	cnoid;
	Oid 	cnoid_dn;
	List	*dn_node_list = NIL;

	node_name = PG_GETARG_CSTRING(0);
	Assert(node_name);
	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot execute this command during recovery")));

	strcpy(nodename.data, "---");
	sprintf(ret_msg, "import coordinator hash table meta info to %s.", node_name);

	PG_TRY();
	{
		hexp_get_coordinator_conn(&pg_conn, &cnoid);

		//1.check dn exists
		dn_node_list = hexp_init_dn_nodes(pg_conn);
		if(INVALID_ID==hexp_find_dn_nodes(dn_node_list, node_name))
		{
			PQfinish(pg_conn);
			pg_conn = NULL;
			ereport(ERROR, (errmsg("%s's doesn't exist.", node_name)));
		}

		//2.check dn's hash table meta empty
		sprintf(sql, SELECT_HASH_TABLE_COUNT_THROUGH_CO, node_name);
		if(hexp_check_select_result_count(pg_conn, sql))
		{
			PQfinish(pg_conn);
			pg_conn = NULL;
			ereport(ERROR, (errmsg("%s's hash table meta info isn't empty.", node_name)));
		}

		//3.import
		hexp_get_coordinator_conn(&pg_conn_dn, &cnoid_dn);

		hexp_pqexec_direct_execute_utility(pg_conn_dn,SQL_XC_MAINTENANCE_MODE_ON, MGR_PGEXEC_DIRECT_EXE_UTI_RET_COMMAND_OK);
		hexp_import_hash_meta(pg_conn, pg_conn_dn, node_name);
		hexp_pqexec_direct_execute_utility(pg_conn_dn,SQL_XC_MAINTENANCE_MODE_OFF, MGR_PGEXEC_DIRECT_EXE_UTI_RET_COMMAND_OK);

		PQfinish(pg_conn_dn);
		pg_conn_dn= NULL;

		PQfinish(pg_conn);
		pg_conn = NULL;

	}PG_CATCH();
	{
		if(pg_conn_dn)
		{
			PQfinish(pg_conn_dn);
			pg_conn_dn = NULL;
		}
		if(pg_conn)
		{
			PQfinish(pg_conn);
			pg_conn = NULL;
		}
		PG_RE_THROW();
	}PG_END_TRY();

	tup_result = build_common_command_tuple(&nodename, true, ret_msg);
	return HeapTupleGetDatum(tup_result);
}

Datum mgr_cluster_hash_meta_check(PG_FUNCTION_ARGS)
{
	HeapTuple tup_result;
	char ret_msg[100];
	NameData nodename;

	strcpy(nodename.data, "---");
	strcpy(ret_msg, "check hash table meta info consistency in cluster.");
	hexp_check_hash_meta();

	tup_result = build_common_command_tuple(&nodename, true, ret_msg);
	return HeapTupleGetDatum(tup_result);
}

Datum mgr_expand_check_status(PG_FUNCTION_ARGS)
{
	return hexp_expand_check_show_status(true);
}

Datum mgr_expand_show_status(PG_FUNCTION_ARGS)
{
	return hexp_expand_check_show_status(false);
}

/*
	check datanode slave status
*/
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
		info->rel_node = table_open(NodeRelationId, AccessShareLock);
		info->rel_scan = table_beginscan_catalog(info->rel_node, 1, key);
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

Datum hexp_expand_check_show_status(bool check)
{
	HeapTuple tup_result;
	NameData nodename;
	StringInfoData serialize;
	List *dn_status_list;

	strcpy(nodename.data, "---");
	initStringInfo(&serialize);

	hexp_check_cluster_status_internal(&dn_status_list, &serialize, check);

	tup_result = build_common_command_tuple(&nodename, true, serialize.data);
	return HeapTupleGetDatum(tup_result);
}

static void hexp_update_conf_pgxc_node_name(AppendNodeInfo node, char* newname)
{
	GetAgentCmdRst getAgentCmdRst;
	StringInfoData infosendmsg;

	initStringInfo(&infosendmsg);
	initStringInfo(&(getAgentCmdRst.description));

	mgr_append_pgconf_paras_str_quotastr("pgxc_node_name", newname, &infosendmsg);
	mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, node.nodepath, &infosendmsg, node.nodehost, &getAgentCmdRst);

	if (!getAgentCmdRst.ret)
	{
		ereport(ERROR, (errmsg("update datanode %s's pgxc_node_name param fail\n", newname)));
	}
}

static void hexp_restart_node(AppendNodeInfo node)
{
	GetAgentCmdRst getAgentCmdRst;
	StringInfoData infosendmsg;
	StringInfoData buf;
	ManagerAgent *ma;
	bool exec_result;

	initStringInfo(&buf);
	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsg);

	appendStringInfo(&infosendmsg, " restart -D %s", node.nodepath);
	appendStringInfo(&infosendmsg, " -Z datanode -m fast -o -i -w -c -l %s/logfile", node.nodepath);

	/* connection agent */
	ma = ma_connect_hostoid(node.nodehost);
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
		ereport(ERROR, (errmsg("restart %s fail\n", node.nodename)));
	}

}

static void hexp_execute_cmd_get_reloid(PGconn *pg_conn, char *sqlstr, char* ret)
{
	PGresult *res;
	res = PQexec(pg_conn, sqlstr);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		PQclear(res);
		ereport(ERROR, (errmsg("get table id error, %s runs error.", SELECT_HASH_TABLE_DETAIL)));
	}
	if (PQgetisnull(res, 0, 0))
	{
		PQclear(res);
		ereport(ERROR, (errmsg("cann't find table id, %s runs error.", SELECT_HASH_TABLE_DETAIL)));
	}

	strcpy(ret, PQgetvalue(res, 0, 0));
	return;
}

static void hexp_check_hash_meta_dn(PGconn *pgconn, PGconn *pgconn_dn, char* node_name)
{
	PGresult* res;
	ExecStatusType status;
	int i=0;
	char sql[300];
	char pcrelid1[20];
	char* pcattnum3;
	char* pcrelname;
	char* pschema;
	int		c_count;
	int 	d_count;

	c_count = hexp_select_result_count(pgconn, SELECT_HASH_TABLE_COUNT);

	sprintf(sql,SELECT_HASH_TABLE_COUNT_THROUGH_CO,node_name);
	d_count = hexp_select_result_count(pgconn_dn, sql);

	if(c_count!=d_count)
		ereport(ERROR, (errmsg("coor's hash table number doesn't match %s's.", node_name)));

	//1.get cor's hash table meta
	res = PQexec(pgconn, SELECT_HASH_TABLE_FOR_MATCH);
	status = PQresultStatus(res);
	switch(status)
	{
		case PGRES_TUPLES_OK:
			break;
		default:
			ereport(ERROR, (errmsg("%s runs error.", SELECT_HASH_TABLE_DETAIL)));
	}

    for (i = 0; i < PQntuples(res); i++)
    {
		pschema = 			PQgetvalue(res, i, 0);
		pcrelname = 		PQgetvalue(res, i, 1);
		pcattnum3 = 		PQgetvalue(res, i, 2);

		//2.1 get table oid
		sprintf(sql, SELECT_HASH_TABLE_FOR_MATCH_THROUGH_CO,node_name, pschema, pcrelname, pcattnum3);
		hexp_execute_cmd_get_reloid(pgconn_dn, sql, pcrelid1);

		if(1!=hexp_select_result_count(pgconn_dn, sql))
			ereport(ERROR, (errmsg("cann't find %s-%s-%s on %s.", pschema, pcrelname, pcattnum3, node_name)));
	}
    PQclear(res);
}

static void hexp_check_hash_meta(void)
{
	PGconn *pg_conn = NULL;
	PGconn *pg_conn_dn = NULL;
	Oid 	cnoid;
	Oid 	cnoid_dn;
	DN_NODE	*dn_node;
	List	*dn_node_list = NIL;
	ListCell	*lc;

	PG_TRY();
	{
		hexp_get_coordinator_conn(&pg_conn, &cnoid);
		hexp_get_coordinator_conn(&pg_conn_dn, &cnoid_dn);

		dn_node_list = hexp_init_dn_nodes(pg_conn);

		foreach (lc, dn_node_list)
		{	
			dn_node = (DN_NODE *)lfirst(lc);
			hexp_check_hash_meta_dn(pg_conn, pg_conn_dn, NameStr(dn_node->nodename));
		}

		PQfinish(pg_conn_dn);
		pg_conn_dn= NULL;

		PQfinish(pg_conn);
		pg_conn = NULL;
	}PG_CATCH();
	{
		if(pg_conn_dn)
		{
			PQfinish(pg_conn_dn);
			pg_conn_dn = NULL;
		}
		if(pg_conn)
		{
			PQfinish(pg_conn);
			pg_conn = NULL;
		}
		PG_RE_THROW();
	}PG_END_TRY();

}

static void hexp_import_hash_meta(PGconn *pgconn, PGconn *pgconn_dn, char* node_name)
{
	PGresult* res;
	ExecStatusType status;
	int i=0;
	char sql[300];
	char pcrelid1[20];
	char* pclocatortype2;
	char* pcattnum3;
	char* pchashalgorithm4;
	char* pchashbuckets5;
	char* nodeoids6;
	char* pcrelname;
	char* pschema;


	//1.get cor's hash table meta
	res = PQexec(pgconn, SELECT_HASH_TABLE_DETAIL);
	status = PQresultStatus(res);
	switch(status)
	{
		case PGRES_TUPLES_OK:
			break;
		default:
			ereport(ERROR, (errmsg("%s runs error.", SELECT_HASH_TABLE_DETAIL)));
	}

    for (i = 0; i < PQntuples(res); i++)
    {
		pschema = 			PQgetvalue(res, i, 0);
		pcrelname = 		PQgetvalue(res, i, 1);
		pclocatortype2 = 	PQgetvalue(res, i, 2);;
		pcattnum3 = 		PQgetvalue(res, i, 3);
		pchashalgorithm4 = 	PQgetvalue(res, i, 4);
		pchashbuckets5 = 	PQgetvalue(res, i, 5);
		nodeoids6 = 		PQgetvalue(res, i, 6);

		//2.1 get table oid
		sprintf(sql, SELECT_REL_ID_TROUGH_CO,node_name, pschema, pcrelname);
		hexp_execute_cmd_get_reloid(pgconn_dn, sql, pcrelid1);

		//2.2 insert into pgxc_class
		sprintf(sql, INSERT_PGXC_CLASS_THROUGH_CO,
				node_name, pcrelid1,pclocatortype2,pcattnum3, pchashalgorithm4,pchashbuckets5, nodeoids6);
		hexp_pqexec_direct_execute_utility(pgconn_dn,sql, MGR_PGEXEC_DIRECT_EXE_UTI_RET_COMMAND_OK);
	}
    PQclear(res);
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

	pdn_status->checked = false;
	pdn_status->node_status = SlotStatusInvalid;


	namestrcpy(&pdn_status->pgxc_node_name, "");
	namecpy(&pdn_status->nodename, &mgr_node->nodename);
	pdn_status->tid = tuple_id;

	pdn_status->nodemasternameoid = mgr_node->nodemasternameoid;
	pdn_status->nodeincluster = mgr_node->nodeincluster;

	PG_TRY();
	{
		hexp_get_dn_conn((PGconn**)&dn_pg_conn, mgr_node, cnpath);
		//hexp_get_dn_slot_param_status(dn_pg_conn, pdn_status);
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
	info->rel_node = table_open(NodeRelationId, AccessShareLock);
	info->rel_scan = table_beginscan_catalog(info->rel_node, 1, key);
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
		hexp_get_dn_status(mgr_node, mgr_node->oid, dn_status, cnpath);
		dn_status_list = lappend(dn_status_list, dn_status);
	}

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
	return dn_status_list;
}

static int hexp_select_result_count(PGconn *pg_conn, char* sql)
{
	ExecStatusType status;
	PGresult *res;
	int count;

	Assert((pg_conn)!= 0);

	res = PQexec(pg_conn, sql);
	status = PQresultStatus(res);

	switch(status)
	{
		case PGRES_TUPLES_OK:
			break;
		default:
			ereport(ERROR, (errmsg("%s runs error. result is %s.", sql, PQresultErrorMessage(res))));
	}

	if (0==PQntuples(res))
	{
		PQclear(res);
		ereport(ERROR, (errmsg("%s runs error. result is null.", sql)));
	}

	count = atoi(PQgetvalue(res, 0, 0));
	PQclear(res);

	return count;
}

/*
return false if result is 0.
return true  if result is not 0.
*/
bool hexp_check_select_result_count(PGconn *pg_conn, char* sql)
{
	ExecStatusType status;
	PGresult *res;
	int count;

	Assert((pg_conn)!= 0);

	res = PQexec(pg_conn, sql);
	status = PQresultStatus(res);

	switch(status)
	{
		case PGRES_TUPLES_OK:
			break;
		default:
			ereport(ERROR, (errmsg("%s runs error. result is %s.", sql, PQresultErrorMessage(res))));
	}

	if (0==PQntuples(res))
	{
		PQclear(res);
		ereport(ERROR, (errmsg("%s runs error. result is null.", sql)));
	}

	count = atoi(PQgetvalue(res, 0, 0));
	PQclear(res);

	if (0 == count)
		return false;
	else
		return true;

}

static void hexp_check_dn_pgxcnode_info(PGconn *pg_conn, char *nodename, StringInfoData* pcoor_node_list)
{
	PGresult *res;
	StringInfoData psql_cmd;
	StringInfoData node_list_self;
	ExecStatusType status;
	int i;

	Assert((pg_conn)!= 0);

	initStringInfo(&psql_cmd);
	initStringInfo(&node_list_self);

	//check dn info consistent with coor's.
	appendStringInfo(&psql_cmd, SELECT_PGXC_NODE_THROUGH_COOR ,nodename);

	res = PQexec(pg_conn, psql_cmd.data);
	status = PQresultStatus(res);

	switch(status)
	{
		case PGRES_TUPLES_OK:
			break;
		default:
			ereport(ERROR, (errmsg("%s runs error. result is %s.", psql_cmd.data, PQresultErrorMessage(res))));
	}

	if (0==PQntuples(res))
	{
		PQclear(res);
		ereport(ERROR, (errmsg("%s hasn't any datanode in pgxc_class. %s runs error. resutl is null.", nodename, psql_cmd.data)));
	}


    for (i = 0; i < PQntuples(res); i++)
		appendStringInfo(&node_list_self, "%s", PQgetvalue(res, i, 0));

	PQclear(res);

	if(0 != strcmp(pcoor_node_list->data, node_list_self.data))
		ereport(ERROR, (errmsg("pgxc_node info is inconsistent in %s.", nodename)));

	pfree(psql_cmd.data);
	pfree(node_list_self.data);
}

static int hexp_find_dn_nodes(List *dn_node_list, char* nodename)
{
	ListCell	*lc;
	DN_NODE		*dn_node;

	foreach (lc, dn_node_list)
	{
		dn_node = (DN_NODE *)lfirst(lc);
		if(0==strcmp(NameStr(dn_node->nodename), nodename))
			return VALID_ID;
	}

	return INVALID_ID;
}
static List *
hexp_init_dn_nodes(PGconn *pg_conn)
{
	PGresult *res;
	ExecStatusType status;
	int i;
	List *dn_node_list = NIL;

	//get coor dn info
	res = PQexec(pg_conn, SELECT_PGXC_NODE);
	status = PQresultStatus(res);
	switch(status)
	{
		case PGRES_TUPLES_OK:
			break;
		default:
			ereport(ERROR, (errmsg("%s runs error. result is %s.", SELECT_PGXC_NODE, PQresultErrorMessage(res))));
	}
	if (0==PQntuples(res))
	{
		PQclear(res);
		ereport(ERROR, (errmsg("%s runs error. result is null.", SELECT_PGXC_NODE)));
	}
    for (i = 0; i < PQntuples(res); i++)
    {
		DN_NODE *dn_node = (DN_NODE *) palloc(sizeof(DN_NODE));
		strcpy(NameStr(dn_node->nodename), PQgetvalue(res, i, 0));
		strcpy(NameStr(dn_node->host), PQgetvalue(res, i, 1));
		strcpy(NameStr(dn_node->port), PQgetvalue(res, i, 2));
		dn_node_list = lappend(dn_node_list, dn_node);
	}
	PQclear(res);
	return dn_node_list;
}

static void hexp_check_cluster_pgxcnode(void)
{
	InitNodeInfo *info;
	ScanKeyData key[2];
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	PGconn *pg_conn = NULL;
	Oid cnoid;

	StringInfoData coor_node_list;

	int 		mgr_count=0;
	List		*dn_node_list = NIL;
	ListCell	*lc;
	DN_NODE		*dn_node;

	PG_TRY();
	{
		hexp_get_coordinator_conn(&pg_conn, &cnoid);
		//get coor dn info
		initStringInfo(&coor_node_list);

		dn_node_list = hexp_init_dn_nodes(pg_conn);
		foreach (lc, dn_node_list)
		{
			dn_node = (DN_NODE *)lfirst(lc);
			appendStringInfo(&coor_node_list, "%s", NameStr(dn_node->nodename));
		}

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
		info->rel_node = table_open(NodeRelationId, AccessShareLock);
		info->rel_scan = table_beginscan_catalog(info->rel_node, 2, key);
		info->lcp = NULL;

		while ((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);

			if(mgr_node->nodetype!=CNDN_TYPE_DATANODE_MASTER)
				continue;
			//check if this dn's pgxc_node equals coor's.
			hexp_check_dn_pgxcnode_info(pg_conn, NameStr(mgr_node->nodename), &coor_node_list);
			mgr_count++;
		}

		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, AccessShareLock);
		pfree(info);

		if(dn_node_list->length != mgr_count)
			ereport(ERROR, (errmsg("pgxc_node' count doesn't match mgr's.")));

		if(pg_conn)
			PQfinish(pg_conn);
		pg_conn = NULL;
	}PG_CATCH();
	{
		if(pg_conn)
		{
			PQfinish(pg_conn);
			pg_conn = NULL;
		}
		PG_RE_THROW();
	}PG_END_TRY();

}
static void hexp_check_expand()
{
	StringInfoData serialize;
	bool is_vacuum_state = false;
	List *dn_status_list;

	//check slot vacuum status
	initStringInfo(&serialize);
	is_vacuum_state = hexp_check_cluster_status_internal(&dn_status_list, &serialize, true);
	if(is_vacuum_state)
		ereport(ERROR, (errmsg("%s exists, expand cann't be started.", ADB_CLEAN_TABLE)));
}

bool hexp_check_cluster_status_internal(List **pdn_status_list, StringInfo pserialize, bool check)
{
	PGconn *pg_conn = NULL;
	Oid cnoid;
	bool is_vacuum_state = false;
	ListCell	*lc;
	DN_STATUS	*dn_status;

	PG_TRY();
	{
		Assert(pserialize);

		hexp_get_coordinator_conn(&pg_conn, &cnoid);

		//call pgxc_pool_reload on all nodes in expand show status cmd
		if(!check)
			hexp_pgxc_pool_reload_on_all_node(pg_conn);

		//check pgxc node info is consistent
		appendStringInfo(pserialize,"pgxc node info in cluster is consistent.\n");

		//get slot vacuum status
		is_vacuum_state = hexp_check_select_result_count(pg_conn, IS_ADB_CLEAN_TABLE_EXISTS);
		if(is_vacuum_state)
			appendStringInfo(pserialize,"cluster status is vacuum\n");

		//get all dn info
		*pdn_status_list = hexp_get_all_dn_status();

		//check each node
		if(check&&is_vacuum_state)
			ereport(ERROR, (errmsg("cluster status is slot vacuum, but ClusterSlotStatus is not clean.")));
		
		foreach (lc, *pdn_status_list)
		{
			dn_status = (DN_STATUS *)lfirst(lc);
			appendStringInfo(pserialize,
				"name=%s-masterid=%d-incluster=%d\n"
				,NameStr(dn_status->nodename),
				dn_status->nodemasternameoid,
				dn_status->nodeincluster);
		}
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
	return is_vacuum_state;
}

static void hexp_get_coordinator_conn(PGconn **pg_conn, Oid *cnoid)
{
	Oid coordhostoid;
	int32 coordport;
	char *coordhost;
	char coordport_buf[10];
	char *connect_user;
	char cnpath[1024];
	int try = 0;
	NameData self_address;
	NameData nodename;
	GetAgentCmdRst getAgentCmdRst;
	StringInfoData infosendmsg;
	Datum datumPath;
	Relation rel_node;
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	bool isNull;
	char* database ;
	bool breload = false;

	if(0!=strcmp(MGRDatabaseName,""))
		database = MGRDatabaseName;
	else
		database = DEFAULT_DB;

	/*get active coordinator to connect*/
	if (!mgr_get_active_node(&nodename, CNDN_TYPE_COORDINATOR_MASTER, 0))
		ereport(ERROR, (errmsg("can not get active coordinator in cluster")));
	rel_node = table_open(NodeRelationId, AccessShareLock);
	//tuple = mgr_get_tuple_node_from_name_type(rel_node, nodename.data, CNDN_TYPE_COORDINATOR_MASTER);
	tuple = mgr_get_tuple_node_from_name_type(rel_node, nodename.data);
	if(!(HeapTupleIsValid(tuple)))
	{
		ereport(ERROR, (errmsg("coordinator \"%s\" does not exist", nodename.data)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
			, errcode(ERRCODE_UNDEFINED_OBJECT)));
	}
	mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
	coordhostoid = mgr_node->nodehost;
	coordport = mgr_node->nodeport;
	coordhost = get_hostaddress_from_hostoid(coordhostoid);
	connect_user = get_hostuser_from_hostoid(coordhostoid);
	*cnoid = mgr_node->oid;

	/*get the adbmanager ip*/
	mgr_get_self_address(coordhost, coordport, &self_address);

	/*set adbmanager ip to the coordinator if need*/
	datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(rel_node), &isNull);
	if (isNull)
	{
		heap_freetuple(tuple);
		heap_close(rel_node, AccessShareLock);
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
			, errmsg("column nodepath is null")));
	}
	strncpy(cnpath, TextDatumGetCString(datumPath), 1024);
	heap_freetuple(tuple);
	heap_close(rel_node, AccessShareLock);
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
			PQfinish(*pg_conn);
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
	if (*pg_conn == NULL || PQstatus((PGconn*)*pg_conn) != CONNECTION_OK)
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

static void hexp_parse_pair_lsn(char* strvalue, int* phvalue, int* plvalue)
{
    char* t;
    char* d = "/";
    t = strtok(strvalue, d);
    Assert(t!= 0);
    sscanf(t, "%x", phvalue);
    t = strtok(NULL, d);
    Assert(t!= 0);
    sscanf(t, "%x", plvalue);
}

/*
* execute the sql, the result of sql is lsn
*/
static void hexp_mgr_pqexec_getlsn(PGconn **pg_conn, char *sqlstr, int* phvalue, int* plvalue)
{
	PGresult *res;
	char pair_value[50];

	Assert((*pg_conn)!= 0);

	res = PQexec(*pg_conn, sqlstr);
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

static void hexp_create_dm_on_all_node(PGconn *pg_conn, AppendNodeInfo *nodeinfo)
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
	info->rel_node = table_open(NodeRelationId, AccessShareLock);
	info->rel_scan = table_beginscan_catalog(info->rel_node, 2, key);
	info->lcp = NULL;

	//todo rollback
	while ((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);

		if(!((mgr_node->nodetype==CNDN_TYPE_DATANODE_MASTER) ||
			 (mgr_node->nodetype==CNDN_TYPE_COORDINATOR_MASTER) ||
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
	info->rel_node = table_open(NodeRelationId, AccessShareLock);
	info->rel_scan = table_beginscan_catalog(info->rel_node, 2, key);
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
	info->rel_node = table_open(NodeRelationId, AccessShareLock);
	info->rel_scan = table_beginscan_catalog(info->rel_node, 2, key);
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
		,Anum_mgr_node_oid
		,BTEqualStrategyNumber
		,F_OIDEQ
		,ObjectIdGetDatum(tupleOid));

	info = (InitNodeInfo *)palloc0(sizeof(InitNodeInfo));
	info->rel_node = table_open(NodeRelationId, AccessShareLock);
	info->rel_scan = table_beginscan_catalog(info->rel_node, 1, key);
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
	info->rel_node = table_open(NodeRelationId, AccessShareLock);
	info->rel_scan = table_beginscan_catalog(info->rel_node, 4, key);
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
	rel = table_open(HostRelationId, AccessShareLock);
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

	rel_node = table_open(NodeRelationId, RowExclusiveLock);
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



