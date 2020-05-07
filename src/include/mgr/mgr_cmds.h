
#ifndef MGR_CMDS_H
#define MGR_CMDS_H

#include "parser/mgr_node.h"
#include "nodes/params.h"
#include "tcop/dest.h"
#include "lib/stringinfo.h"
#include "fmgr.h"
#include "utils/relcache.h"
#include "access/heapam.h"
#include "mgr/mgr_agent.h"
#include "mgr/mgr_helper.h"
#include "utils/timestamp.h"
#include "../../interfaces/libpq/libpq-fe.h"
#include "catalog/mgr_node.h"
#include "catalog/mgr_host.h"
#include "utils/relcache.h"
#include "access/heapam.h"

#define run_success "success"
#define PRIV_GRANT        'G'
#define PRIV_REVOKE       'R'
#define MONITOR_CLUSTERSTR "cluster"

#define SQL_BEGIN_TRANSACTION	"begin transaction;"
#define SQL_COMMIT_TRANSACTION	"commit;"
#define SQL_ROLLBACK_TRANSACTION	"rollback;"

#define MGR_PGEXEC_DIRECT_EXE_UTI_RET_COMMAND_OK	0
#define MGR_PGEXEC_DIRECT_EXE_UTI_RET_TUPLES_TRUE	1

#define ClosePgConn(co_pg_conn)\
{\
	if(co_pg_conn)\
	{\
		PQfinish(co_pg_conn);\
		co_pg_conn = NULL;\
	}\
}

#define MgrFree(val)\
{\
	if (val != NULL)\
	{\
		pfree(val);\
		val = NULL;\
	}\
}

#define CheckNull(val)\
{\
	if (NULL == val)\
	{\
	    ereport(LOG, (errmsg("NULL == val.")));\
		return;\
	}\
}

#define CheckNullRetrunRet(val, ret)\
{\
	if (NULL == val)\
	{\
	    ereport(LOG, (errmsg("NULL == val, and return.")));\
		return ret;\
	}\
}

#define IsGTMMaster(nodetype)  	(nodetype == CNDN_TYPE_GTM_COOR_MASTER)
#define IsCOORDMaster(nodetype) (nodetype == CNDN_TYPE_COORDINATOR_MASTER)
#define IsDNMaster(nodetype)  	(nodetype == CNDN_TYPE_DATANODE_MASTER)

#define EndScan(relScan)\
{\
	if (relScan != NULL){\
		heap_endscan(relScan);\
		relScan = NULL;\
	}\
}

typedef struct GetAgentCmdRst
{
	NameData nodename;
	int ret;
	StringInfoData description;
}GetAgentCmdRst;

typedef struct AppendNodeInfo
{
	char *nodename;
	char *nodepath;
	char  nodetype;
	Oid   nodehost;
	char *nodeaddr;
	int32 nodeport;
	Oid   nodemasteroid;
	char *nodeusername;
	Oid		tupleoid;
	NameData sync_state;
	bool init;
	bool incluster;
}AppendNodeInfo;

/* for table: monitor_alarm */
typedef struct Monitor_Alarm
{
	int16			alarm_level;
	int16			alarm_type;
	StringInfoData	alarm_timetz;
	int16			alarm_status;
	StringInfoData	alarm_source;
	StringInfoData	alarm_text;
}Monitor_Alarm;

/* for table: monitor_alarm */
typedef struct Monitor_Threshold
{
	int16			threshold_warning;
	int16			threshold_critical;
	int16			threshold_emergency;
}Monitor_Threshold;

/*cmd flag for create/drop extension*/
typedef enum
{
	EXTENSION_CREATE,
	EXTENSION_DROP
}extension_operator;

/* agent result message type */
typedef enum AGENT_RESULT_MsgTYPE 
{
	AGENT_RESULT_LOG = 0,
	AGENT_RESULT_DEBUG = 1,
	AGENT_RESULT_MESSAGE = 2
}AGENT_RESULT_MsgTYPE;

typedef struct InitNodeInfo
{
	Relation rel_node;
	HeapScanDesc rel_scan;
	ListCell  **lcp;
}InitNodeInfo;

typedef struct InitAclInfo
{
	Relation rel_authid;
	HeapScanDesc rel_scan;
	ListCell  **lcp;
}InitAclInfo;

struct tuple_cndn
{
	List *coordiantor_list;
	List *datanode_list;
};

/*see the content : insert into mgr.dbthreshold in adbmgr_init.sql*/
typedef enum DbthresholdObject
{
	OBJECT_NODE_HEAPHIT = 11,
	OBJECT_NODE_COMMITRATE,
	OBJECT_NODE_STANDBYDELAY,
	OBJECT_NODE_LOCKS,
	OBJECT_NODE_CONNECT,
	OBJECT_NODE_LONGTRANS,
	OBJECT_NODE_UNUSEDINDEX,
	OBJECT_CLUSTER_HEAPHIT = 21,
	OBJECT_CLUSTER_COMMITRATE,
	OBJECT_CLUSTER_STANDBYDELAY,
	OBJECT_CLUSTER_LOCKS,
	OBJECT_CLUSTER_CONNECT,
	OBJECT_CLUSTER_LONGTRANS,
	OBJECT_CLUSTER_UNUSEDINDEX
}DbthresholdObject;

typedef enum AlarmLevel
{
	ALARM_WARNING = 1,
	ALARM_CRITICAL,
	ALARM_EMERGENCY
}AlarmLevel;

typedef enum PGXC_NODE_MANIPULATE_TYPE
{
	PGXC_NODE_MANIPULATE_TYPE_CREATE = 1,
	PGXC_NODE_MANIPULATE_TYPE_DROP
} PGXC_NODE_MANIPULATE_TYPE;

typedef struct CmdTypeName
{
	int     	cmdtype;
	char		cmdname[NAMEDATALEN];
}CmdTypeName;

typedef List * (*nodenames_supplier) (PG_FUNCTION_ARGS, char nodetype);
typedef void (nodenames_consumer)(List *nodenames, char nodetype);
/* host commands, in cmd_host.c */

extern void mgr_add_host(MGRAddHost *node, ParamListInfo params, DestReceiver *dest);
extern void mgr_drop_host(MGRDropHost *node, ParamListInfo params, DestReceiver *dest);
extern void mgr_alter_host(MGRAlterHost *node, ParamListInfo params, DestReceiver *dest);
extern bool check_node_running_by_socket(char *host, int port);
extern bool port_occupancy_test(const char *ip_address, const int port);
extern bool get_node_type_str(int node_type, Name node_type_str);

extern Datum mgr_start_agent_all(PG_FUNCTION_ARGS);
extern GetAgentCmdRst *mgr_start_agent_execute(Form_mgr_host mgr_host,char* hostaddr,char *hostadbhome, char *password);
extern Datum mgr_start_agent_hostnamelist(PG_FUNCTION_ARGS);

extern Datum mgr_deploy_all(PG_FUNCTION_ARGS);
extern Datum mgr_deploy_hostnamelist(PG_FUNCTION_ARGS);

extern Datum mgr_drop_host_func(PG_FUNCTION_ARGS);
extern Datum mgr_alter_host_func(PG_FUNCTION_ARGS);

extern Datum mgr_add_updateparm_func(PG_FUNCTION_ARGS);
extern Datum mgr_reset_updateparm_func(PG_FUNCTION_ARGS);
extern void mgr_stop_agent(MGRStopAgent *node,  ParamListInfo params, DestReceiver *dest);
extern void mgr_monitor_agent(MGRMonitorAgent *node,  ParamListInfo params, DestReceiver *dest);
extern int ssh2_start_agent(const char *hostname,
							unsigned short port,
					 		const char *username,
					 		const char *password,
					 		const char *commandline,
					 		StringInfo message);
extern bool ssh2_deplory_tar(const char *hostname,
							unsigned short port,
							const char *username,
							const char *password,
							const char *path,
							FILE *tar,
							StringInfo message);
extern bool mgr_check_cluster_stop(Name nodename, Name nodetypestr);

/*parm commands, in cmd_parm.c*/
extern void mgr_alter_parm(MGRAlterParm *node, ParamListInfo params, DestReceiver *dest);

/*in cmd_node.c */
extern void mgr_reload_conf(Oid hostoid, char *nodepath);
extern bool get_active_node_info(const char node_type, const char *node_name, AppendNodeInfo *nodeinfo);
/*coordinator datanode parse cmd*/
extern Datum mgr_init_gtmcoord_master(PG_FUNCTION_ARGS);
extern Datum mgr_start_gtmcoord_master(PG_FUNCTION_ARGS);
extern Datum mgr_stop_one_gtm_master(PG_FUNCTION_ARGS);
extern Datum mgr_stop_gtmcoord_master(PG_FUNCTION_ARGS);
extern Datum mgr_init_gtmcoord_slave(PG_FUNCTION_ARGS);
extern Datum mgr_start_gtmcoord_slave(PG_FUNCTION_ARGS);
extern Datum mgr_stop_gtmcoord_slave(PG_FUNCTION_ARGS);
extern Datum mgr_failover_gtm(PG_FUNCTION_ARGS);
extern Datum mgr_failover_gtm_deprecated(PG_FUNCTION_ARGS);
extern Datum mgr_failover_one_dn(PG_FUNCTION_ARGS);
extern Datum mgr_failover_one_dn_deprecated(PG_FUNCTION_ARGS);
extern void mgr_add_node(MGRAddNode *node, ParamListInfo params, DestReceiver *dest);
extern void mgr_alter_node(MGRAlterNode *node, ParamListInfo params, DestReceiver *dest);
extern void mgr_drop_node(MGRDropNode *node, ParamListInfo params, DestReceiver *dest);
extern Datum mgr_drop_node_func(PG_FUNCTION_ARGS);
extern Datum mgr_init_all(PG_FUNCTION_ARGS);
extern Datum mgr_init_cn_master(PG_FUNCTION_ARGS);
extern void mgr_runmode_cndn_get_result(const char cmdtype, GetAgentCmdRst *getAgentCmdRst, Relation noderel, HeapTuple aimtuple, const char *shutdown_mode);
extern Datum mgr_init_dn_master(PG_FUNCTION_ARGS);
extern Datum mgr_init_dn_slave_all(PG_FUNCTION_ARGS);
extern void mgr_init_dn_slave_get_result(const char cmdtype, GetAgentCmdRst *getAgentCmdRst, Relation noderel, HeapTuple aimtuple, char *masterhostaddress, uint32 masterport, char *mastername);

extern Datum mgr_boottime_nodetype_all(PG_FUNCTION_ARGS);;

extern Datum mgr_start_cn_master(PG_FUNCTION_ARGS);
extern Datum mgr_start_cn_slave(PG_FUNCTION_ARGS);
extern Datum mgr_stop_cn_master(PG_FUNCTION_ARGS);
extern Datum mgr_stop_cn_slave(PG_FUNCTION_ARGS);
extern Datum mgr_start_dn_master(PG_FUNCTION_ARGS);
extern Datum mgr_stop_dn_master(PG_FUNCTION_ARGS);
extern Datum mgr_stop_dn_master_all(PG_FUNCTION_ARGS);
extern Datum mgr_start_dn_slave(PG_FUNCTION_ARGS);
extern Datum mgr_stop_dn_slave(PG_FUNCTION_ARGS);
extern Datum mgr_stop_dn_slave_all(PG_FUNCTION_ARGS);
extern Datum mgr_stop_agent_all(PG_FUNCTION_ARGS);
extern Datum mgr_stop_agent_hostnamelist(PG_FUNCTION_ARGS);
extern Datum mgr_runmode_cndn(nodenames_supplier supplier,
							  nodenames_consumer consumer,
							  char nodetype,
							  char cmdtype, 
							  char *shutdown_mode, 
							  PG_FUNCTION_ARGS);

extern Datum mgr_boottime_all(PG_FUNCTION_ARGS);
extern Datum mgr_boottime_gtmcoord_all(PG_FUNCTION_ARGS);
extern Datum mgr_boottime_datanode_all(PG_FUNCTION_ARGS);
extern Datum mgr_boottime_coordinator_all(PG_FUNCTION_ARGS);

extern Datum mgr_monitor_all(PG_FUNCTION_ARGS);
extern Datum mgr_monitor_datanode_all(PG_FUNCTION_ARGS);
extern Datum mgr_monitor_gtmcoord_all(PG_FUNCTION_ARGS);

extern Datum mgr_monitor_nodetype_all(PG_FUNCTION_ARGS);
extern Datum mgr_boottime_nodetype_all(PG_FUNCTION_ARGS);
extern Datum mgr_boottime_nodetype_namelist(PG_FUNCTION_ARGS);

extern Datum mgr_monitor_nodetype_namelist(PG_FUNCTION_ARGS);
extern Datum mgr_monitor_agent_all(PG_FUNCTION_ARGS);
extern Datum mgr_monitor_agent_hostlist(PG_FUNCTION_ARGS);

extern Datum mgr_append_dnmaster(PG_FUNCTION_ARGS);
extern Datum mgr_append_dnslave(PG_FUNCTION_ARGS);
extern bool mgr_append_dn_slave_func(char *dnName);
extern Datum mgr_append_coordmaster(PG_FUNCTION_ARGS);
extern Datum mgr_append_agtmslave(PG_FUNCTION_ARGS);

extern bool mgr_append_agtm_slave_func(char *gtmname);

extern Datum mgr_list_acl_all(PG_FUNCTION_ARGS);
extern Datum mgr_priv_manage(PG_FUNCTION_ARGS);
extern Datum mgr_priv_all_to_username(PG_FUNCTION_ARGS);
extern Datum mgr_priv_list_to_all(PG_FUNCTION_ARGS);

extern Datum mgr_add_host_func(PG_FUNCTION_ARGS);
extern Datum mgr_add_node_func(PG_FUNCTION_ARGS);
extern Datum mgr_list_nodesize_all(PG_FUNCTION_ARGS);
extern Datum mgr_alter_node_func(PG_FUNCTION_ARGS);

extern Datum mgr_configure_nodes_all(PG_FUNCTION_ARGS);

extern bool mgr_has_priv_add(void);
extern bool mgr_has_priv_drop(void);
extern bool mgr_has_priv_alter(void);
extern bool mgr_has_priv_set(void);
extern bool mgr_has_priv_reset(void);

extern void mgr_start_cndn_get_result(const char cmdtype, GetAgentCmdRst *getAgentCmdRst, Relation noderel, HeapTuple aimtuple);
extern List *get_fcinfo_namelist(const char *sepstr, int argidx, FunctionCallInfo fcinfo);
void check_dn_slave(char nodetype, List *nodenamelist, Relation rel_node, StringInfo infosendmsg);
extern bool mgr_refresh_pgxc_node_tbl(char *cndnname, int32 cndnport, char *cndnaddress, bool isprimary, Oid cndnmasternameoid, GetAgentCmdRst *getAgentCmdRst);
extern void mgr_send_conf_parameters(char filetype, char *datapath, StringInfo infosendmsg, Oid hostoid, GetAgentCmdRst *getAgentCmdRst);
extern void mgr_append_pgconf_paras_str_str(char *key, char *value, StringInfo infosendmsg);
extern void mgr_append_pgconf_paras_str_int(char *key, int value, StringInfo infosendmsg);
extern void mgr_append_infostr_infostr(StringInfo infostr, StringInfo sourceinfostr);
extern void mgr_add_parameters_pgsqlconf(Oid tupleOid, char nodetype, int cndnport, StringInfo infosendparamsg);
extern void mgr_append_pgconf_paras_str_quotastr(char *key, char *value, StringInfo infosendmsg);
extern void mgr_add_parameters_recoveryconf(char nodetype, char *slavename, Oid tupleoid, StringInfo infosendparamsg);
extern void mgr_add_parameters_hbaconf(Oid mastertupleoid, char nodetype, StringInfo infosendhbamsg);
extern void mgr_add_oneline_info_pghbaconf(int type, char *database, char *user, char *addr, int addr_mark, char *auth_method, StringInfo infosendhbamsg);
extern Datum mgr_start_one_dn_master(PG_FUNCTION_ARGS);
extern Datum mgr_stop_one_dn_master(PG_FUNCTION_ARGS);
extern char *mgr_get_slavename(Oid tupleOid, char nodetype);
extern void mgr_rename_recovery_to_conf(char cmdtype, Oid hostOid, char* cndnpath, GetAgentCmdRst *getAgentCmdRst);
extern HeapTuple mgr_get_tuple_node_from_name_type(Relation rel, char *nodename);
extern char *mgr_nodetype_str(char nodetype);
extern Datum mgr_clean_all(PG_FUNCTION_ARGS);
extern Datum mgr_clean_node(PG_FUNCTION_ARGS);
extern bool mgr_check_node_exist_incluster(Name nodename, bool bincluster);
extern List* mgr_get_nodetype_namelist(char nodetype);
extern Datum mgr_remove_node_func(PG_FUNCTION_ARGS);
extern void mgr_remove_node(MgrRemoveNode *node, ParamListInfo params, DestReceiver *dest);
extern Datum mgr_monitor_ha(PG_FUNCTION_ARGS);
extern void get_nodeinfo_byname(char *node_name, char node_type, bool *is_exist, bool *is_running, AppendNodeInfo *nodeinfo);
extern void pfree_AppendNodeInfo(AppendNodeInfo nodeinfo);
extern bool mgr_lock_cluster_deprecated(PGconn **pg_conn, Oid *cnoid);
extern bool mgr_lock_cluster_involve_gtm_coord(PGconn **pg_conn, Oid *cnoid);
extern void mgr_unlock_cluster_deprecated(PGconn **pg_conn);
extern void mgr_unlock_cluster_involve_gtm_coord(PGconn **pg_conn);
extern void mgr_get_gtmcoord_conn(char *zone, char *dbname, PGconn **pg_conn, Oid *cnoid);
extern int mgr_get_master_sync_string(Oid mastertupleoid, bool bincluster, Oid excludeoid, StringInfo infostrparam);
extern bool mgr_pqexec_refresh_pgxc_node(pgxc_node_operator cmd, char nodetype, char *dnname
		, GetAgentCmdRst *getAgentCmdRst, PGconn **pg_conn, Oid cnoid, char *newSyncSlaveName);
/* mgr_common.c */
extern TupleDesc get_common_command_tuple_desc(void);
extern HeapTuple build_common_command_tuple(const Name name, bool success, const char *message);
extern int pingNode_user(char *host, char *port, char *user);
extern bool is_valid_ip(char *ip);
extern bool	mgr_check_host_in_use(Oid hostoid, bool check_inited);
extern void mgr_mark_node_in_cluster(Relation rel);
extern TupleDesc get_showparam_command_tuple_desc(void);
HeapTuple build_list_acl_command_tuple(const Name name, const char *message);
TupleDesc get_list_acl_command_tuple_desc(void);
List * DecodeTextArrayToValueList(Datum textarray);
void check_nodename_isvalid(char *nodename);
bool mgr_has_function_privilege_name(char *funcname, char *priv_type);
bool mgr_has_table_privilege_name(char *tablename, char *priv_type);
extern void mgr_recv_sql_stringvalues_msg(ManagerAgent	*ma, StringInfo resultstrdata);
/* get the host address */
char *get_hostaddress_from_hostoid(Oid hostOid);
char *get_hostname_from_hostoid(Oid hostOid);
char *get_hostuser_from_hostoid(Oid hostOid);
void get_hostinfo_from_hostoid(Oid hostOid, MgrHostWrapper *host);

/* get msg from agent */
bool mgr_recv_msg(ManagerAgent	*ma, GetAgentCmdRst *getAgentCmdRst);
bool mgr_recv_msg_for_nodesize(ManagerAgent	*ma, GetAgentCmdRst *getAgentCmdRst);
bool mgr_recv_msg_for_monitor(ManagerAgent	*ma, bool *ret, StringInfo agentRstStr);
extern List *monitor_get_dbname_list(char *user, char *address, int port);
extern void monitor_get_one_node_user_address_port(Relation rel_node, int *agentport, char **user, char **address, int *coordport, char nodetype);
extern HeapTuple build_ha_replication_tuple(const Name type, const Name nodename, const Name app, const Name client_addr, const Name state, const Name sent_location, const Name replay_location, const Name sync_state, const Name master_location, const Name sent_delay, const Name replay_delay);
extern TupleDesc get_ha_replication_tuple_desc(void);
extern bool mgr_promote_node(char cmdtype, Oid hostOid, char *path, StringInfo strinfo);
extern bool mgr_check_node_connect(char nodetype, Oid hostOid, int nodeport);
extern bool mgr_rewind_node(char nodetype, char *nodename, StringInfo strinfo);
extern bool mgr_ma_send_cmd(char cmdtype, char *cmdstr, Oid hostOid, StringInfo strinfo);
extern bool mgr_ma_send_cmd_get_original_result(char cmdtype, char *cmdstr, Oid hostOid, StringInfo strinfo, AGENT_RESULT_MsgTYPE resultType);
extern bool mgr_recv_msg_original_result(ManagerAgent	*ma, GetAgentCmdRst *getAgentCmdRst, AGENT_RESULT_MsgTYPE resultType);
extern void mgr_get_cmd_head_word(char cmdtype, char *str);
extern bool mgr_check_node_recovery_finish(char nodetype, Oid hostoid, int nodeport, char *address);
extern char mgr_get_master_type(char nodetype);
extern Datum mgr_typenode_cmd_run_backend_result(nodenames_supplier supplier,
												 nodenames_consumer consumer,
												 const char nodetype,
												 const char cmdtype,
												 const char *shutdown_mode,
												 PG_FUNCTION_ARGS);
extern List *nodenames_supplier_of_db(PG_FUNCTION_ARGS, char nodetype);
extern List *nodenames_supplier_of_argidx_0(PG_FUNCTION_ARGS, char nodetype);
extern List *nodenames_supplier_of_argidx_1(PG_FUNCTION_ARGS, char nodetype);
extern List *nodenames_supplier_of_clean_node(PG_FUNCTION_ARGS, char nodetype);
extern void enable_doctor_consulting(List *nodenames, char nodetype);
extern void disable_doctor_consulting(List *nodenames, char nodetype);
extern char mgr_change_cmdtype_unbackend(char cmdtype);
extern HeapTuple build_common_command_tuple_four_col(const Name name, char type, bool status, const char *description);
extern bool mgr_check_param_reload_postgresqlconf(char nodetype, Oid hostoid, int nodeport, char *address, char *check_param, char *expect_result);
extern char mgr_get_nodetype(Name nodename);
extern int mgr_get_monitor_node_result(char nodetype, Oid hostOid, int nodeport , StringInfo strinfo, StringInfo starttime, Name recoveryStrInfo);

/* monitor_hostpage.c */
extern Datum monitor_get_hostinfo(PG_FUNCTION_ARGS);
bool get_cpu_info(StringInfo hostinfostring);
extern void insert_into_monitor_alarm(Monitor_Alarm *monitor_alarm);
extern void get_threshold(int16 type, Monitor_Threshold *monitor_threshold);

/*monitor_databaseitem.c*/
extern int64 monitor_get_onesqlvalue_one_node(int agentport, char *sqlstr, char *user, char *address, int nodeport, char * dbname);
extern int64 monitor_get_result_one_node(Relation rel_node, char *sqlstr, char *dbname, char nodetype);
extern int64 monitor_get_sqlres_all_typenode_usedbname(Relation rel_node, char *sqlstr, char *dbname, char nodetype, int gettype);
extern Datum monitor_databaseitem_insert_data(PG_FUNCTION_ARGS);
extern HeapTuple monitor_build_database_item_tuple(Relation rel, const TimestampTz time, char *dbname
			, int64 dbsize, bool archive, bool autovacuum, float heaphitrate,  float commitrate, int64 dbage, int64 connectnum
			, int64 standbydelay, int64 locksnum, int64 longquerynum, int64 idlequerynum, int64 preparenum, int64 unusedindexnum, int64 indexsize);
extern Datum monitor_databasetps_insert_data(PG_FUNCTION_ARGS);
extern HeapTuple monitor_build_databasetps_qps_tuple(Relation rel, const TimestampTz time, const char *dbname, const int64 tps, const int64 qps, int64 pgdbruntime);
extern void monitor_get_stringvalues(char cmdtype, int agentport, char *sqlstr, char *user, char *address, int nodeport, char * dbname, StringInfo resultstrdata);
extern void monitor_delete_data(MonitorDeleteData *node, ParamListInfo params, DestReceiver *dest);
extern Datum monitor_delete_data_interval_days(PG_FUNCTION_ARGS);
extern void mgr_set_init(MGRSetClusterInit *node, ParamListInfo params, DestReceiver *dest);
extern Datum mgr_set_init_cluster(PG_FUNCTION_ARGS);

/*monitor_slowlog.c*/
extern char *monitor_get_onestrvalue_one_node(int agentport, char *sqlstr, char *user, char *address, int port, char * dbname);
extern void monitor_get_onedb_slowdata_insert(Relation rel, int agentport, char *user, char *address, int port, char *dbname);
extern HeapTuple monitor_build_slowlog_tuple(Relation rel, TimestampTz time, char *dbname, char *username, float singletime, int totalnum, char *query, char *queryplan);
extern Datum monitor_slowlog_insert_data(PG_FUNCTION_ARGS);
extern HeapTuple check_record_yestoday_today(Relation rel, int *callstoday, int *callsyestd, bool *gettoday, bool *getyesdt, char *query, char *user, char *dbname, pg_time_t ptimenow);
extern void monitor_insert_record(Relation rel, int agentport, TimestampTz time, char *dbname, char *dbuser, float singletime, int calls, char *querystr, char *user, char *address, int port);

/*monitor_dbthreshold.c*/
extern char *monitor_get_timestamptz_onenode(int agentport, char *user, char *address, int port);
extern bool monitor_get_sqlvalues_one_node(int agentport, char *sqlstr, char *user, char *address, int port, char * dbname, int64 iarray[], int len);
extern void mthreshold_levelvalue_positiveseq(DbthresholdObject objectype, char *address, const char *time
		, int value, char *descp);
extern void mthreshold_levelvalue_impositiveseq(DbthresholdObject objectype, char *address, const char * time
		, int value, char *descp);

/*mgr_updateparm*/
extern void mgr_add_updateparm(MGRUpdateparm *node, ParamListInfo params, DestReceiver *dest);
extern void mgr_add_parm(char *nodename, char nodetype, StringInfo infosendparamsg);
extern void mgr_reset_updateparm(MGRUpdateparmReset *node, ParamListInfo params, DestReceiver *dest);
extern void mgr_parmr_update_tuple_nodename_nodetype(Relation noderel, Name nodename, char oldnodetype, char newnodetype);
extern void mgr_update_parm_after_dn_failover(Name oldmastername, char oldmastertype, Name oldslavename, char oldslavetype);
extern void mgr_parm_after_gtm_failover_handle(Name mastername, char mastertype, Name slavename, char slavetype);
extern void mgr_parmr_delete_tuple_nodename_nodetype(Relation noderel, Name nodename, char nodetype);
extern void mgr_flushhost(MGRFlushHost *node, ParamListInfo params, DestReceiver *dest);
extern  Datum mgr_flush_host(PG_FUNCTION_ARGS);
extern Datum mgr_show_var_param(PG_FUNCTION_ARGS);
Datum mgr_update_param_gtm_failover(PG_FUNCTION_ARGS);
Datum mgr_update_param_datanode_failover(PG_FUNCTION_ARGS);

/*mgr_hba    mgr_hba.c*/

extern void mgr_clean_hba_table(char *coord_name, char *values);
extern void add_hba_table_to_file(char *coord_name);
extern void add_one_to_hba_file(const char *coord_name, const char *hba_value, GetAgentCmdRst *err_msg);
extern Datum mgr_list_hba_by_name(PG_FUNCTION_ARGS);
extern Datum mgr_show_hba_all(PG_FUNCTION_ARGS);
extern Datum mgr_drop_hba(PG_FUNCTION_ARGS);
extern Datum mgr_add_hba(PG_FUNCTION_ARGS);

/*monitor_jobitem.c*/
extern void monitor_jobitem_add(MonitorJobitemAdd *node, ParamListInfo params, DestReceiver *dest);
extern void monitor_jobitem_alter(MonitorJobitemAlter *node, ParamListInfo params, DestReceiver *dest);
extern void monitor_jobitem_drop(MonitorJobitemDrop *node, ParamListInfo params, DestReceiver *dest);
extern Datum monitor_jobitem_add_func(PG_FUNCTION_ARGS);
extern Datum monitor_jobitem_alter_func(PG_FUNCTION_ARGS);
extern Datum monitor_jobitem_drop_func(PG_FUNCTION_ARGS);

extern void monitor_job_add(MonitorJobAdd *node, ParamListInfo params, DestReceiver *dest);
extern void monitor_job_alter(MonitorJobAlter *node, ParamListInfo params, DestReceiver *dest);
extern void monitor_job_drop(MonitorJobDrop *node, ParamListInfo params, DestReceiver *dest);
extern Datum monitor_job_add_func(PG_FUNCTION_ARGS);
extern Datum monitor_job_alter_func(PG_FUNCTION_ARGS);
extern Datum monitor_job_drop_func(PG_FUNCTION_ARGS);
extern Datum adbmonitor_job(PG_FUNCTION_ARGS);

/*create/drop extension*/
Datum mgr_extension_handle(PG_FUNCTION_ARGS);
void mgr_extension(MgrExtensionAdd *node, ParamListInfo params, DestReceiver *dest);

/*mgr_manual.c*/
extern Datum mgr_failover_manual_adbmgr_func(PG_FUNCTION_ARGS);
extern Datum mgr_failover_manual_promote_func(PG_FUNCTION_ARGS);
extern Datum mgr_failover_manual_pgxcnode_func(PG_FUNCTION_ARGS);
extern Datum mgr_failover_manual_rewind_func(PG_FUNCTION_ARGS);
extern Datum mgr_append_coord_to_coord(PG_FUNCTION_ARGS);
extern bool mgr_append_coord_slave_func(char *m_coordname, char *s_coordname, StringInfoData *strerr);
extern Datum mgr_append_activate_coord(PG_FUNCTION_ARGS);
extern Datum mgr_switchover_func(PG_FUNCTION_ARGS);
extern Datum mgr_switchover_func_deprecated(PG_FUNCTION_ARGS);
extern bool mgr_update_agtm_port_host(PGconn **pg_conn, char *hostaddress, int cndnport, Oid cnoid, StringInfo recorderr);

/*expansion calls*/
extern void	mgr_make_sure_all_running(char node_type);
extern bool is_node_running(char *hostaddr, int32 hostport, char *user, char nodetype);
extern bool mgr_try_max_pingnode(char *host, char *port, char *user, const int max_times);
extern char mgr_get_master_type(char nodetype);
extern void mgr_get_nodeinfo_byname_type(char *node_name, char node_type, bool bincluster, bool *is_exist, bool *is_running, AppendNodeInfo *nodeinfo);
extern void get_nodeinfo_byname(char *node_name, char node_type, bool *is_exist, bool *is_running, AppendNodeInfo *nodeinfo);
extern void get_nodeinfo(char *nodename, char node_type, bool *is_exist, bool *is_running, AppendNodeInfo *nodeinfo);
extern void mgr_add_hbaconf(char nodetype, char *dnusername, char *dnaddr);
extern void mgr_check_dir_exist_and_priv(Oid hostoid, char *dir);
extern void mgr_pgbasebackup(char nodetype, AppendNodeInfo *appendnodeinfo, AppendNodeInfo *parentnodeinfo);
extern void mgr_start_node(char nodetype, const char *nodepath, Oid hostoid);
extern HeapTuple build_common_command_tuple_for_boottime(const Name name, char type, bool status, const char *description,
						const char *starttime ,const Name hostaddr);
extern HeapTuple build_common_command_tuple_for_monitor(const Name name, char type, bool status, const char *description
                                                        ,const char *starttime, const Name hostaddr, const int port, const Name recoveryStatus, const Name zone);
extern bool mgr_get_self_address(char *server_address, int server_port, Name self_address);

extern Datum monitor_handle_coordinator(PG_FUNCTION_ARGS);
extern int get_agentPort_from_hostoid(Oid hostOid);
extern bool mgr_check_job_in_updateparam(const char *subjobstr);
extern char *get_nodepath_from_tupleoid(Oid tupleOid);
extern int mgr_get_normal_slave_node(Relation relNode, Oid masterTupleOid, int sync_state_sync, Oid excludeOid, Name slaveNodeName);
extern bool mgr_get_slave_node(Relation relNode, Oid masterTupleOid, int syncType, Oid excludeOid, Name slaveNodeName);
extern char *mgr_get_mastername_by_nodename_type(char* nodename, char nodetype);
extern void mgr_add_hbaconf_by_masteroid(Oid mastertupleoid, char *dbname, char *user, char *address);
extern char *mgr_get_agtm_name(void);
extern bool mgr_check_slave_replicate_status(const Oid masterTupleOid, const char nodetype, const char *slaveName);
extern bool mgr_set_all_nodetype_param(const char nodetype, char *paramName, char *paramValue);
extern Datum monitor_handle_datanode(PG_FUNCTION_ARGS);
extern Datum monitor_handle_gtm(PG_FUNCTION_ARGS);
extern HeapTuple mgr_get_sync_slavenode_tuple(Oid mastertupleoid, bool bincluster, Oid includeoid, Oid excludeoid, int seqNum);
extern bool mgr_get_createnodeCmd_on_readonly_cn(char *nodeName, bool bincluster, StringInfo cmdstring);

extern int mgr_pqexec_boolsql_try_maxnum(PGconn **pg_conn, char *sqlstr, const int maxnum, int sqltype);
extern void mgr_get_prefer_nodename_for_cn(char *cnName, List *dnNamelist, Name preferredDnName);
extern Oid mgr_get_tupoid_from_nodename(Relation relNode, char *nodename);
extern bool mgr_check_list_in(List *list, char *checkName);
extern bool mgr_try_max_times_get_stringvalues(char cmdtype, int agentPort, char *sqlStr, char *userName, char *nodeAddress
	, int nodePort, char *dbname, StringInfo restmsg, int max, char *checkResultStr);
extern bool mgr_get_dnlist(Name oldPreferredNode, char *separateStr, StringInfo restmsg, List **dnList);
extern void mgr_set_preferred_node(char *oldPreferredNode, char *preferredDnName
		,char *coordname, char *userName, char *nodeAddress, int agentPort, int nodePort);

extern List *mgr_append_coord_update_pgxcnode(StringInfo sqlstrmsg, List *dnList, Name oldPreferredNode, int nodeSeqNum, char *execNodeName);
extern Oid mgr_get_nodeMaster_tupleOid(char *nodeName);
extern int mgr_get_nodetype_num(const char nodeType, const bool inCluster, const bool readOnly);
extern bool mgr_modify_readonly_coord_pgxc_node(Relation rel_node, StringInfo infostrdata, char *nodename, int newport);
extern void mgr_flushparam(MGRFlushParam *node, ParamListInfo params, DestReceiver *dest);
extern void mgr_check_all_agent(void);
extern void check_node_incluster(void);

/*online expand external function*/
extern Datum mgr_expand_dnmaster(PG_FUNCTION_ARGS);
extern Datum mgr_expand_activate_dnmaster(PG_FUNCTION_ARGS);
extern Datum mgr_checkout_dnslave_status(PG_FUNCTION_ARGS);
extern Datum mgr_expand_recover_backup_fail(PG_FUNCTION_ARGS);
extern Datum mgr_expand_recover_backup_suc(PG_FUNCTION_ARGS);
extern Datum mgr_expand_activate_recover_promote_suc(PG_FUNCTION_ARGS);
extern Datum mgr_expand_check_status(PG_FUNCTION_ARGS);
extern Datum mgr_expand_show_status(PG_FUNCTION_ARGS);
extern Datum mgr_expand_clean(PG_FUNCTION_ARGS);

/* online doctor functions */
extern Datum mgr_doctor_start(PG_FUNCTION_ARGS);
extern Datum mgr_doctor_stop(PG_FUNCTION_ARGS);
extern Datum mgr_doctor_param(PG_FUNCTION_ARGS);
extern Datum mgr_doctor_list(PG_FUNCTION_ARGS);
extern Datum mgr_doctor_list_param(PG_FUNCTION_ARGS);
extern Datum mgr_doctor_list_node(PG_FUNCTION_ARGS);
extern Datum mgr_doctor_list_host(PG_FUNCTION_ARGS);
extern void mgr_doctor_set_param(MGRDoctorSet *node,
								 ParamListInfo params,
								 DestReceiver *dest);

/*online expand internal function*/
//void mgr_cluster_slot_init(ClusterSlotInitStmt *node, ParamListInfo params, DestReceiver *dest, const char *query);
bool get_agent_info_from_hostoid(const Oid hostOid, char *agent_addr, int *agent_port);
Datum mgr_failover_one_dn_inner_func(char *nodename, char cmdtype, char nodetype, bool nodetypechange, bool bforce);
bool mgr_get_active_node(Name nodename, char nodetype, char *zone, Oid lowPriorityOid);

extern bool AddHbaIsValid(const AppendNodeInfo *nodeinfo, StringInfo infosendmsg);
extern bool RemoveHba(const AppendNodeInfo *nodeinfo, const StringInfo infosendmsg);
extern bool mgr_execute_direct_on_all_coord(PGconn **pg_conn, const char *sql, const int iloop, const int res_type, StringInfo strinfo);
extern bool mgr_manipulate_pgxc_node_on_all_coord(PGconn **pg_conn, Oid cnoidOfConn, const int iloop, AppendNodeInfo *nodeinfo, PGXC_NODE_MANIPULATE_TYPE manipulateType, StringInfo strinfo);
extern bool mgr_manipulate_pgxc_node_on_node(PGconn **pg_conn, 
											 const int iloop, 
									  		 AppendNodeInfo *nodeinfo, 
									  		 Form_mgr_node executeOnNode, 
									  	 	 bool localExecute, 
									  		 PGXC_NODE_MANIPULATE_TYPE manipulateType,
									  		 StringInfo strinfo);
extern void hexp_pqexec_direct_execute_utility(PGconn *pg_conn, char *sqlstr, int ret_type);

/* zone */
extern bool mgr_check_nodename_repeate(Relation rel, char *nodename);
extern bool mgr_checknode_in_currentzone(const char *zone, const Oid TupleOid);
extern Datum mgr_zone_failover(PG_FUNCTION_ARGS);
extern Datum mgr_zone_config_all(PG_FUNCTION_ARGS);
extern HeapTuple mgr_get_nodetuple_by_name_zone(Relation rel, char *nodename, char *nodezone);
extern Datum mgr_zone_clear(PG_FUNCTION_ARGS);
extern Datum mgr_zone_init(PG_FUNCTION_ARGS);
extern bool mgr_node_has_slave_inzone(Relation rel, char *zone, Oid mastertupleoid);
extern bool mgr_update_cn_pgxcnode_readonlysql_slave(char *updateKey, bool isSlaveSync, Node *node);
extern void mgr_clean_cn_pgxcnode_readonlysql_slave(void);
extern bool mgr_check_nodetype_exist(char nodeType, char nodeTypeList[8]);

extern char *getMgrNodeSyncStateValue(sync_state state);
extern uint64 updateDoctorStatusOfMgrNodes(List *nodenames, char nodetype, bool allowcure, char *curestatus);
extern uint64 updateDoctorStatusOfMgrNode(char *nodename, char nodetype, bool allowcure, char *curestatus);
extern uint64 updateAllowcureOfMgrHosts(List *hostnames, bool allowcure);
extern uint64 updateAllowcureOfMgrHost(char *hostname, bool allowcure);
extern void MgrSendAlterNodeDataToGtm(PGconn *pg_conn, char *nodes_slq);
extern void MgrSendFinishActiveBackendToGtm(PGconn *pg_conn);
extern void MgrSendDataCleanToGtm(PGconn *pg_conn);
extern int MgrSendSelectMsg(PGconn *pg_conn, StringInfoData* psql);
extern void MgrSendAlterMsg(PGconn *pg_conn, StringInfoData *psql);
extern void mgr_clean_node_folder(char cmdtype, Oid hostoid, char *nodepath, GetAgentCmdRst *getAgentCmdRst);
extern int MgrGetAdbcleanNum(PGconn *pg_conn);
extern char* mgr_get_cmdname(int cmdtype);
extern char *MgrGetDefDbName(void);
extern char mgr_zone_get_restart_cmd(char nodetype);
extern void hexp_restart_node(AppendNodeInfo *node);
extern void hexp_update_conf_pgxc_node_name(AppendNodeInfo *node, char* newname);
#endif /* MGR_CMDS_H */
