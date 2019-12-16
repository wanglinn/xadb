/*
 * commands of node
 */
#include <netdb.h>
#include <arpa/inet.h>
#include <unistd.h>
#include<pwd.h>

#include "postgres.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/catalog.h"
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

/*
hot_expansion changes below functions:
1.mgr_pgbasebackup:add dnmaster type.
2.use MGRDatabaseName to login.
3.get_nodeinfo_byname:nodeinfo->nodename isn't set null when item isn't found.
4.mgr_runmode_cndn_get_result:add
		case AGT_CMD_DN_MASTER_PROMOTE:
			cmdmode = "promote";
			zmode = "datanode";
			break;
5.mgr_runmode_cndn_get_result
	else if (AGT_CMD_DN_MASTER_PROMOTE == cmdtype)
	{
		appendStringInfo(&infosendmsg, " %s -w -D %s", cmdmode, cndnPath);
	}
6.mgr_get_cmd_head_word
		case AGT_CMD_DN_MASTER_PROMOTE:
*/

extern char	*MGRDatabaseName;

NameData GTM_COORD_PGXC_NODE_NAME = {{0}};

static PGconn *
ExpPQsetdbLogin(const char *pghost, const char *pgport, const char *pgoptions,
			 const char *pgtty, const char *login, const char *pwd);

static PGconn *
ExpPQsetdbLogin(const char *pghost, const char *pgport, const char *pgoptions,
			 const char *pgtty, const char *login, const char *pwd)

{
	char* database ;
	if(0!=strcmp(MGRDatabaseName,""))
		database = MGRDatabaseName;
	else
		database = DEFAULT_DB;
	return PQsetdbLogin(pghost, pgport, pgoptions, pgtty, database, login, pwd);
}

typedef struct NodeSizeInfo
{
	Relation rel_host;
	HeapScanDesc rel_scan;
	ListCell  **lcp;
}NodeSizeInfo;

typedef struct MgrDatanodeInfo
{
	Oid				masterOid;
	Form_mgr_node	masterNode;
	Form_mgr_node	slaveNode;
}MgrDatanodeInfo;

typedef struct ReadonlyUpdateparm
{
	NameData	updateparmnodename;
	char		updateparmnodetype;
	NameData	updateparmkey;
	NameData	updateparmvalue;
}ReadonlyUpdateparm;

#define MAX_PREPARED_TRANSACTIONS_DEFAULT	120
#define PG_DUMPALL_TEMP_FILE "/tmp/pg_dumpall_temp"
#define MAX_WAL_SENDERS_NUM	5
#define WAL_KEEP_SEGMENTS_NUM	32
#define WAL_LEVEL_MODE	"hot_standby"
#define APPEND_DNMASTER  1
#define APPEND_CNMASTER  2
#define SYNC            't'
#define ASYNC           'f'
#define SPACE           ' '

bool with_data_checksums = false;
Oid specHostOid = 0;
Oid clusterLockCoordNodeOid = 0;
NameData paramV;

/* Need to refresh the slave node information about the read-only sql in the pgxc_node table. */
bool readonlySqlSlaveInfoRefreshFlag;
/* Mark the read-only sql slave node information successfully refreshed to coordinate */
bool readonlySqlSlaveInfoRefreshComplete = false;

static struct enum_sync_state sync_state_tab[] =
{
	{SYNC_STATE_SYNC, "sync"},
	{SYNC_STATE_ASYNC, "async"},
	{SYNC_STATE_POTENTIAL, "potential"},
	{-1, NULL}
};

static struct enum_recovery_status enum_recovery_status_tab[] =
{
	{RECOVERY_IN, "true"},
	{RECOVERY_NOT_IN, "false"},
	{RECOVERY_UNKNOWN, "unknown"},
	{-1, NULL}
};

#define DEFAULT_WAIT	60

void release_append_node_info(AppendNodeInfo *node_info, bool is_release);

static TupleDesc common_command_tuple_desc = NULL;
static TupleDesc common_boottime_tuple_desc = NULL;

static TupleDesc get_common_command_tuple_desc_for_monitor(void);
static TupleDesc get_common_command_tuple_desc_for_boottime(void);
static void mgr_get_appendnodeinfo(char node_type, char *nodename, AppendNodeInfo *appendnodeinfo);
static void mgr_append_init_cndnmaster(AppendNodeInfo *appendnodeinfo);
static void mgr_get_other_parm(char node_type, StringInfo infosendmsg);
static bool mgr_get_active_hostoid_and_port(char node_type, Oid *hostoid, int32 *hostport, AppendNodeInfo *appendnodeinfo, bool set_ip);
static void mgr_pg_dumpall(Oid hostoid, int32 hostport, Oid dnmasteroid, char *temp_file);
static void mgr_stop_node_with_restoremode(const char *nodepath, Oid hostoid);
static void mgr_pg_dumpall_input_node(const Oid dn_master_oid, const int32 dn_master_port, char *temp_file);
static void mgr_rm_dumpall_temp_file(Oid dnhostoid,char *temp_file);
static void mgr_start_node_with_restoremode(const char *nodepath, Oid hostoid, char nodetype);
static void mgr_create_node_on_all_coord(PG_FUNCTION_ARGS, char nodetype, char *dnname, Oid dnhostoid, int32 dnport);
static bool mgr_drop_node_on_all_coord(char nodetype, char *nodename);
static void mgr_set_inited_incluster(char *nodename, char nodetype, bool checkvalue, bool setvalue);
static void mgr_add_hbaconf_all(char *dnusername, char *dnaddr, bool check_incluster);
static void mgr_after_gtm_failover_handle(char *hostaddress, int cndnport, Relation noderel, GetAgentCmdRst *getAgentCmdRst, HeapTuple aimtuple, char *cndnPath, PGconn **pg_conn, Oid cnoid);
static void mgr_after_datanode_failover_handle(Oid nodemasternameoid, Name cndnname, int cndnport, char *hostaddress, Relation noderel, GetAgentCmdRst *getAgentCmdRst, HeapTuple aimtuple, char *cndnPath, char aimtuplenodetype, PGconn **pg_conn, Oid cnoid);
static void mgr_get_parent_appendnodeinfo(Oid nodemasternameoid, AppendNodeInfo *parentnodeinfo);
static char *get_temp_file_name(void);
static Datum mgr_prepare_clean_all(PG_FUNCTION_ARGS);
static bool mgr_node_has_slave(Relation rel, Oid mastertupleoid);
static void mgr_set_master_sync(void);
static void mgr_check_appendnodeinfo(char node_type, char *append_node_name);
static struct tuple_cndn *get_new_pgxc_node(pgxc_node_operator cmd, char *node_name, char node_type);
static bool mgr_refresh_pgxc_node(pgxc_node_operator cmd, char nodetype, char *dnname, GetAgentCmdRst *getAgentCmdRst);
static void mgr_modify_port_after_initd(Relation rel_node, HeapTuple nodetuple, char *nodename, char nodetype, int32 newport);
static bool mgr_modify_node_parameter_after_initd(Relation rel_node, HeapTuple nodetuple, StringInfo infosendmsg, bool brestart);
static void mgr_modify_port_recoveryconf(Relation rel_node, HeapTuple aimtuple, int32 master_newport);
static bool mgr_modify_coord_pgxc_node(Relation rel_node, StringInfo infostrdata, char *nodename, int newport);
static bool mgr_add_extension_sqlcmd(char *sqlstr);
static char *get_username_list_str(List *user_list);
static void mgr_manage_flush(char command_type, char *user_list_str);
static void mgr_manage_stop_func(StringInfo commandsql);
static void mgr_manage_stop_view(StringInfo commandsql);
static void mgr_manage_stop(char command_type, char *user_list_str);
static void mgr_manage_deploy(char command_type, char *user_list_str);
static void mgr_manage_reset(char command_type, char *user_list_str);
static void mgr_manage_set(char command_type, char *user_list_str);
static void mgr_manage_alter(char command_type, char *user_list_str);
static void mgr_manage_drop(char command_type, char *user_list_str);
static void mgr_manage_add(char command_type, char *user_list_str);
static void mgr_manage_start(char command_type, char *user_list_str);
static void mgr_manage_show(char command_type, char *user_list_str);
static void mgr_manage_monitor(char command_type, char *user_list_str);
static void mgr_manage_init(char command_type, char *user_list_str);
static void mgr_manage_append(char command_type, char *user_list_str);
static void mgr_manage_failover(char command_type, char *user_list_str);
static void mgr_manage_clean(char command_type, char *user_list_str);
static void mgr_manage_list(char command_type, char *user_list_str);
static void mgr_check_username_valid(List *username_list);
static void mgr_check_command_valid(List *command_list);
static List *get_username_list(void);
static void mgr_get_acl_by_username(char *username, StringInfo acl);
static bool mgr_acl_flush(char *username);
static bool mgr_acl_stop(char *username);
static bool mgr_acl_deploy(char *username);
static bool mgr_acl_reset(char *username);
static bool mgr_acl_set(char *username);
static bool mgr_acl_alter(char *username);
static bool mgr_acl_drop(char *username);
static bool mgr_acl_add(char *username);
static bool mgr_acl_start(char *username);
static bool mgr_acl_show(char *username);
static bool mgr_acl_monitor(char *username);
static bool mgr_acl_list(char *username);
static bool mgr_acl_append(char *username);
static bool mgr_acl_failover(char *username);
static bool mgr_acl_clean(char *username);
static bool mgr_acl_init(char *username);
static bool mgr_has_table_priv(char *rolename, char *tablename, char *priv_type);
static bool mgr_has_func_priv(char *rolename, char *funcname, char *priv_type);
static List *get_username_list(void);
static Oid mgr_get_role_oid_or_public(const char *rolname);
static void mgr_priv_all(char command_type, char *username_list_str);
static bool mgr_extension_pg_stat_statements(char cmdtype, char *extension_name);
static bool mgr_check_syncstate_node_exist(Relation rel, Oid masterTupleOid, int sync_state_type, Oid excludeoid, bool needCheckIncluster);
static bool mgr_check_node_path(Relation rel, Oid hostoid, char *path);
static bool mgr_check_node_port(Relation rel, Oid hostoid, int port);
static void mgr_update_one_potential_to_sync(Relation rel, Oid mastertupleoid, bool bincluster, bool excludeoid);
static bool exec_remove_coordinator(char *nodename);
static bool mgr_get_async_slave_readonly_state(List **parms);
static bool mgr_get_sync_slave_readonly_state(void);
static bool check_all_cn_sync_slave_is_active(void);
static bool mgr_exec_update_cn_pgxcnode_readonlysql_slave(Form_mgr_node	cn_master_node, List *datanode_list, List *sync_parms);
static bool mgr_exec_update_dn_pgxcnode_slave(Form_mgr_node	dn_master_node, Oid master_oid);
static void mgr_update_da_master_pgxcnode_slave_info(const char* dn_master_name);
static void check_readsql_slave_param_state(Form_mgr_node cn_master_node, List *sync_parms, char *key, bool *state);

static bool get_local_ip(Name local_ip);
extern HeapTuple build_list_nodesize_tuple(const Name nodename, char nodetype, int32 nodeport, const char *nodepath, int64 nodesize);
static void mgr_get_gtm_host_snapsender_gxidsender_port(StringInfo infosendmsg);

#if (Natts_mgr_node != 12)
#error "need change code"
#endif


void mgr_add_node(MGRAddNode *node, ParamListInfo params, DestReceiver *dest)
{
	if (mgr_has_priv_add())
	{
		DirectFunctionCall4(mgr_add_node_func,
									CharGetDatum(node->nodetype),
									CStringGetDatum(node->mastername),
									CStringGetDatum(node->name),
									PointerGetDatum(node->options));
		return;
	}
	else
	{
		ereport(ERROR, (errmsg("permission denied")));
		return ;
	}
}

Datum mgr_add_node_func(PG_FUNCTION_ARGS)
{
	Relation rel;
	HeapTuple tuple;
	HeapTuple newtuple;
	HeapTuple checktuple;
	ListCell *lc;
	DefElem *def;
	List *options;
	NameData name;
	NameData mastername;
	NameData sync_state_name;
	NameData hostname;
	NameData zoneData;
	NameData curestatus;
	Datum datum[Natts_mgr_node];
	ObjectAddress myself;
	ObjectAddress host;
	bool isnull[Natts_mgr_node];
	bool got[Natts_mgr_node];
	bool hasSyncNode = false;
	bool otherZone = false;
	Oid cndn_oid = InvalidOid;
	Oid hostoid = InvalidOid;
	Oid masterTupleOid = InvalidOid;
	int32 port = -1;
	char nodetype;   /*coordinator or datanode master/slave*/
	char mastertype;
	char *nodename;
	char *str;
	char pathstr[MAXPGPATH];
	Form_mgr_node mgr_node;

	nodetype = PG_GETARG_CHAR(0);
	namestrcpy(&mastername, PG_GETARG_CSTRING(1));
	nodename = PG_GETARG_CSTRING(2);
	options = (List *)PG_GETARG_POINTER(3);
	Assert(nodename);
	namestrcpy(&name, nodename);

	memset(datum, 0, sizeof(datum));
	memset(isnull, 0, sizeof(isnull));
	memset(got, 0, sizeof(got));
	/* get node zone */
	foreach(lc, options)
	{
		def = lfirst(lc);
		Assert(def && IsA(def, DefElem));
		if(strcmp(def->defname, "zone") == 0)
		{
			if(got[Anum_mgr_node_nodezone-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			str = defGetString(def);
			namestrcpy(&zoneData, str);
			datum[Anum_mgr_node_nodezone-1] = NameGetDatum(&zoneData);
			got[Anum_mgr_node_nodezone-1] = true;
		}
	}
	if (!got[Anum_mgr_node_nodezone-1])
	{
		namestrcpy(&zoneData, mgr_zone);
		datum[Anum_mgr_node_nodezone-1] = NameGetDatum(&zoneData);
		got[Anum_mgr_node_nodezone-1] = true;
	}

	mastertype = mgr_get_master_type(nodetype);
	if (mastertype == nodetype)
	{
		/* the node is master type */
		if (namestrcmp(&zoneData, mgr_zone) != 0)
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					, errmsg("the node \"%s\" is master type, should be in the current zone \"%s\", not \"%s\" zone"
					, name.data, mgr_zone, zoneData.data)));
	}

	rel = heap_open(NodeRelationId, RowExclusiveLock);

	PG_TRY();
	{
		/* check the node exist */
		if (CNDN_TYPE_COORDINATOR_MASTER == nodetype || CNDN_TYPE_COORDINATOR_SLAVE == nodetype)
		{
			checktuple = mgr_get_nodetuple_by_name_zone(rel, NameStr(name), zoneData.data);
			if (HeapTupleIsValid(checktuple))
			{
				heap_freetuple(checktuple);
				ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT)
						, errmsg("the name \"%s\" is already exist in zone \"%s\"", NameStr(name), zoneData.data)));
			}

		}
		else
		{
			if (mgr_check_nodename_repeate(rel, NameStr(name)))
				ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT)
						, errmsg("the name \"%s\" is already exist in node table", NameStr(name))));
		}
		/* check the master exist */
		if (mastertype != nodetype)
		{
			if (CNDN_TYPE_COORDINATOR_SLAVE == nodetype)
			{
				if (strcmp(mastername.data, name.data) == 0)
					otherZone = true;
				checktuple = mgr_get_nodetuple_by_name_zone(rel, mastername.data, mgr_zone);
			}
			else
				checktuple = mgr_get_nodetuple_by_name_zone(rel, mastername.data, zoneData.data);
				if (!HeapTupleIsValid(checktuple))
					checktuple = mgr_get_tuple_node_from_name_type(rel, mastername.data);
			if (!HeapTupleIsValid(checktuple))
			{
				ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					, errmsg("%s \"%s\" does not exist in zone \"%s\"", mgr_nodetype_str(mastertype)
					, NameStr(mastername), otherZone ? mgr_zone:zoneData.data)));
			}
			masterTupleOid = HeapTupleGetOid(checktuple);
			mgr_node = (Form_mgr_node)GETSTRUCT(checktuple);
			if (CNDN_TYPE_COORDINATOR_SLAVE == mgr_node->nodetype)
			{
				heap_freetuple(checktuple);
				ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT)
					,errmsg("not support add the node of coordinator slave for coordinator slave")));
			}
			if ((nodetype == CNDN_TYPE_DATANODE_SLAVE && CNDN_TYPE_DATANODE_MASTER != mgr_node->nodetype)
					|| (nodetype == CNDN_TYPE_GTM_COOR_SLAVE && CNDN_TYPE_GTM_COOR_MASTER != mgr_node->nodetype))
			{
				ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					, errmsg("the type of node \"%s\" is not %s"
						, NameStr(mastername), mgr_nodetype_str(mastertype))));
			}
			if ((strcmp(zoneData.data, NameStr(mgr_node->nodezone))!=0)
				&& mgr_node_has_slave_inzone(rel, zoneData.data, masterTupleOid))
				ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT)
					,errmsg("%s \"%s\" already has slave node in zone \"%s\"", mgr_nodetype_str(mastertype)
					, NameStr(mgr_node->nodename), zoneData.data)));
			heap_freetuple(checktuple);

			hasSyncNode = mgr_check_syncstate_node_exist(rel, masterTupleOid, SYNC_STATE_SYNC, InvalidOid, false);
		}

		/* name */
		datum[Anum_mgr_node_nodename-1] = NameGetDatum(&name);
		foreach(lc, options)
		{
			def = lfirst(lc);
			Assert(def && IsA(def, DefElem));

			if(strcmp(def->defname, "host") == 0)
			{
				if(got[Anum_mgr_node_nodehost-1])
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
						,errmsg("conflicting or redundant options")));
				/* find host oid */
				namestrcpy(&hostname, defGetString(def));
				tuple = SearchSysCache1(HOSTHOSTNAME, NameGetDatum(&hostname));
				if(!HeapTupleIsValid(tuple))
				{
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
						, errmsg("host \"%s\" does not exist", defGetString(def))));
				}
				hostoid = HeapTupleGetOid(tuple);
				datum[Anum_mgr_node_nodehost-1] = ObjectIdGetDatum(hostoid);
				got[Anum_mgr_node_nodehost-1] = true;
				ReleaseSysCache(tuple);
			}else if(strcmp(def->defname, "port") == 0)
			{
				if(got[Anum_mgr_node_nodeport-1])
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
						,errmsg("conflicting or redundant options")));
				port = defGetInt32(def);
				if(port <= 0 || port > UINT16_MAX)
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
						,errmsg("%d is outside the valid range for parameter \"%s\" (%d .. %d)"
						, port, "port", 1, UINT16_MAX)));
				datum[Anum_mgr_node_nodeport-1] = Int32GetDatum(port);
				got[Anum_mgr_node_nodeport-1] = true;
			}else if(strcmp(def->defname, "path") == 0)
			{
				if(got[Anum_mgr_node_nodepath-1])
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
						,errmsg("conflicting or redundant options")));
				str = defGetString(def);
				if(str[0] != '/' || str[0] == '\0')
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
						,errmsg("invalid absoulte path: \"%s\"", str)));
				datum[Anum_mgr_node_nodepath-1] = PointerGetDatum(cstring_to_text(str));
				got[Anum_mgr_node_nodepath-1] = true;
				strncpy(pathstr, str, strlen(str)>MAXPGPATH ? MAXPGPATH:strlen(str));
			}else if(strcmp(def->defname, "sync_state") == 0)
			{
				if(got[Anum_mgr_node_nodesync-1])
				{
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
				}
				str = defGetString(def);
				if (strcmp(str, sync_state_tab[SYNC_STATE_SYNC].name) != 0 && strcmp(str, sync_state_tab[SYNC_STATE_ASYNC].name) != 0
						&& strcmp(str, sync_state_tab[SYNC_STATE_POTENTIAL].name) != 0 )
				{
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
						,errmsg("the sync_state of node can be set as \"sync\", \"potential\" or \"async\"")));
				}
				do
				{
					if (nodetype == mastertype)
					{
						namestrcpy(&sync_state_name, "");
						break;
					}
					if (strcmp(zoneData.data, mgr_zone) !=0)
					{
						namestrcpy(&sync_state_name, sync_state_tab[SYNC_STATE_ASYNC].name);
						break;
					}

					/*sync state*/
					if(strcmp(str, sync_state_tab[SYNC_STATE_SYNC].name) == 0)
					{
						namestrcpy(&sync_state_name, sync_state_tab[SYNC_STATE_SYNC].name);
					}
					else if(strcmp(str, sync_state_tab[SYNC_STATE_ASYNC].name) == 0)
					{
						namestrcpy(&sync_state_name, sync_state_tab[SYNC_STATE_ASYNC].name);
					}else if(strcmp(str, sync_state_tab[SYNC_STATE_POTENTIAL].name) == 0)
					{
						/*check the master of node has sync, if it has not ,set this as sync node*/
						if (!hasSyncNode)
						{
							ereport(NOTICE, (errmsg("the master of this node has no synchronous slave node, make this node as synchronous node")));
							namestrcpy(&sync_state_name, sync_state_tab[SYNC_STATE_SYNC].name);
						}
						else
							namestrcpy(&sync_state_name, sync_state_tab[SYNC_STATE_POTENTIAL].name);
					}
					else
					{
						ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
							,errmsg("the sync_state of node can be set as \"sync\", \"potential\" or \"async\"")));
					}
				}while(0);
				datum[Anum_mgr_node_nodesync-1] = NameGetDatum(&sync_state_name);
				got[Anum_mgr_node_nodesync-1] = true;
			}else if(strcmp(def->defname, "zone") == 0)
			{
				/* do nothing here*/
			}else
			{
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("option \"%s\" is not recognized", def->defname)
					, errhint("option is host, port, sync_state and path")));
			}
		}

		/* if not give, set to default */
		if(got[Anum_mgr_node_nodetype-1] == false)
		{
			datum[Anum_mgr_node_nodetype-1] = CharGetDatum(nodetype);
		}
		if(got[Anum_mgr_node_nodepath-1] == false)
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("option \"path\" must be given")));
		}
		if(got[Anum_mgr_node_nodehost-1] == false)
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("option \"host\" must be given")));
		}
		if(got[Anum_mgr_node_nodeport-1] == false)
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("option \"port\" must be given")));
		}
		if(got[Anum_mgr_node_nodezone-1] == false)
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("option \"zone\" must be given")));
		}

		/*check path not used*/
		if (mgr_check_node_path(rel, hostoid, pathstr))
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
					,errmsg("on host \"%s\" the path \"%s\" has already been used in node table", hostname.data, pathstr)
					,errhint("try \"list node;\" for more information")));
		}

		if (mgr_check_node_port(rel, hostoid, port))
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
					,errmsg("on host \"%s\" the port \"%d\" has already been used in node table", hostname.data, port)
					,errhint("try \"list node;\" for more information")));
		}

		/* default values for user do not set sync in add slave */
		if(got[Anum_mgr_node_nodesync-1] == false)
		{
			if(nodetype != mastertype)
			{
				if (strcmp(zoneData.data, mgr_zone) !=0)
						namestrcpy(&sync_state_name, sync_state_tab[SYNC_STATE_ASYNC].name);
				else
				{
					if (!hasSyncNode)
						namestrcpy(&sync_state_name, sync_state_tab[SYNC_STATE_SYNC].name);
					else
						namestrcpy(&sync_state_name, sync_state_tab[SYNC_STATE_POTENTIAL].name);
				}
			}
			else
				namestrcpy(&sync_state_name, "");
			datum[Anum_mgr_node_nodesync-1] = NameGetDatum(&sync_state_name);
		}
		if(got[Anum_mgr_node_nodemasternameoid-1] == false)
		{
			if (CNDN_TYPE_DATANODE_MASTER == nodetype || CNDN_TYPE_COORDINATOR_MASTER == nodetype || CNDN_TYPE_GTM_COOR_MASTER == nodetype)
				datum[Anum_mgr_node_nodemasternameoid-1] = UInt32GetDatum(0);
			else
			{
				datum[Anum_mgr_node_nodemasternameoid-1] = ObjectIdGetDatum(masterTupleOid);
			}
		}
	}PG_CATCH();
	{
		heap_close(rel, RowExclusiveLock);
		PG_RE_THROW();
	}PG_END_TRY();

	/*the node is not in cluster until config all*/
	datum[Anum_mgr_node_nodeincluster-1] = BoolGetDatum(false);
	/* now, node is not initialized*/
	datum[Anum_mgr_node_nodeinited-1] = BoolGetDatum(false);
	/* by default adb doctor extension would not work on this node until it has been initiated and it is in cluster. */
	datum[Anum_mgr_node_allowcure-1] = BoolGetDatum(false);
	namestrcpy(&curestatus, CURE_STATUS_NORMAL);
	datum[Anum_mgr_node_curestatus-1] = NameGetDatum(&curestatus);

	/* now, we can insert record */
	newtuple = heap_form_tuple(RelationGetDescr(rel), datum, isnull);
	cndn_oid = CatalogTupleInsert(rel, newtuple);
	heap_freetuple(newtuple);

	/*close relation */
	heap_close(rel, RowExclusiveLock);

	/* Record dependencies on host */
	myself.classId = NodeRelationId;
	myself.objectId = cndn_oid;
	myself.objectSubId = 0;

	host.classId = HostRelationId;
	host.objectId = DatumGetObjectId(datum[Anum_mgr_node_nodehost-1]);
	host.objectSubId = 0;
	recordDependencyOn(&myself, &host, DEPENDENCY_NORMAL);
	PG_RETURN_BOOL(true);
}

void mgr_alter_node(MGRAlterNode *node, ParamListInfo params, DestReceiver *dest)
{
	if (mgr_has_priv_alter())
	{
		DirectFunctionCall3(mgr_alter_node_func,
									CharGetDatum(node->nodetype),
									CStringGetDatum(node->name),
									PointerGetDatum(node->options));
		return;
	}
	else
	{
		ereport(ERROR, (errmsg("permission denied")));
		return ;
	}

}

Datum mgr_alter_node_func(PG_FUNCTION_ARGS)
{
	Relation rel;
	HeapTuple oldtuple = NULL;
	HeapTuple new_tuple;
	HeapTuple masterTuple;
	ListCell *lc;
	DefElem *def;
	Datum datum[Natts_mgr_node];
	bool isnull[Natts_mgr_node];
	bool got[Natts_mgr_node];
	bool bnodeInCluster = false;
	bool hasSyncNode = false;
	bool hasSyncNodeInCluster = false;
	bool hasPotenNode = false;
	bool hasPotenNodeInCluster = false;
	int32 newport = -1;
	int  syncNum = 0;
	HeapTuple hostTuple;
	TupleDesc cndn_dsc;
	Form_mgr_node mgr_node;
	Form_mgr_node mgr_nodeM;
	List *options;
	NameData hostname;
	NameData sync_state_name;
	NameData mastername;
	NameData name;
	NameData zoneData;
	char new_sync = SYNC_STATE_SYNC;
	char nodetype;
	char mastertype;
	char *str;
	char *name_str;
	char *masterPath;
	Oid hostoid = InvalidOid;
	Oid selftupleoid = InvalidOid;
	Oid masterTupleOid = InvalidOid;
	Oid masterHostOid = InvalidOid;
	StringInfoData infoSyncStr;
	StringInfoData infosendmsg;
	StringInfoData infoSyncStrTmp;
	GetAgentCmdRst getAgentCmdRst;

	nodetype = PG_GETARG_CHAR(0);
	name_str = PG_GETARG_CSTRING(1);
	options = (List *)PG_GETARG_POINTER(2);
	Assert(name_str);

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot assign TransactionIds during recovery")));

	rel = heap_open(NodeRelationId, RowExclusiveLock);

	PG_TRY();
	{
		cndn_dsc = RelationGetDescr(rel);
		namestrcpy(&name, name_str);

		/* check node exist */
		oldtuple = mgr_get_tuple_node_from_name_type(rel, NameStr(name));
		if(!(HeapTupleIsValid(oldtuple)))
		{
			 ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					 ,errmsg("%s \"%s\" does not exist", mgr_nodetype_str(nodetype), NameStr(name))));
		}
		selftupleoid = HeapTupleGetOid(oldtuple);
		mgr_node = (Form_mgr_node)GETSTRUCT(oldtuple);
		Assert(mgr_node);
		/* check the nodetype from the tuple */
		if (mgr_node->nodetype != nodetype)
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				,errmsg("%s \"%s\" does not exist, check the node table", mgr_nodetype_str(nodetype), NameStr(name))));

		hostoid = mgr_node->nodehost;
		if (CNDN_TYPE_GTM_COOR_MASTER == mgr_node->nodetype || CNDN_TYPE_COORDINATOR_MASTER == mgr_node->nodetype
			|| CNDN_TYPE_DATANODE_MASTER == mgr_node->nodetype)
			masterTupleOid = selftupleoid;
		else
			masterTupleOid = mgr_node->nodemasternameoid;
		bnodeInCluster = mgr_node->nodeincluster;
		namestrcpy(&zoneData, NameStr(mgr_node->nodezone));
		memset(datum, 0, sizeof(datum));
		memset(isnull, 0, sizeof(isnull));
		memset(got, 0, sizeof(got));

		hasSyncNode = mgr_check_syncstate_node_exist(rel, masterTupleOid, SYNC_STATE_SYNC, selftupleoid, false);
		hasSyncNodeInCluster = mgr_check_syncstate_node_exist(rel, masterTupleOid, SYNC_STATE_SYNC, selftupleoid, true);
		hasPotenNode = mgr_check_syncstate_node_exist(rel, masterTupleOid, SYNC_STATE_POTENTIAL, selftupleoid, false);
		hasPotenNodeInCluster = mgr_check_syncstate_node_exist(rel, masterTupleOid, SYNC_STATE_POTENTIAL, selftupleoid, true);
		/* check master node */
		mastertype = mgr_get_master_type(nodetype);
		if (mastertype != nodetype)
		{
			masterTuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(masterTupleOid));
			if(!HeapTupleIsValid(masterTuple))
			{
				ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					, errmsg("cache lookup failed for the master of \"%s\"", NameStr(name))));
			}
			mgr_nodeM = (Form_mgr_node)GETSTRUCT(masterTuple);
			Assert(mgr_nodeM);
			namestrcpy(&mastername, NameStr(mgr_nodeM->nodename));
			masterHostOid = mgr_nodeM->nodehost;
			ReleaseSysCache(masterTuple);
		}

		/* name */
		datum[Anum_mgr_node_nodename-1] = NameGetDatum(&name);
		foreach(lc, options)
		{
			def = lfirst(lc);
			Assert(def && IsA(def, DefElem));
			if(strcmp(def->defname, "host") == 0)
			{
				if(got[Anum_mgr_node_nodehost-1])
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
						,errmsg("conflicting or redundant options")));
				if (bnodeInCluster)
					ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
					 ,errmsg("%s \"%s\" has been initialized in the cluster, cannot be changed"
					 , mgr_nodetype_str(nodetype), NameStr(name))));
				/* find host oid */
				namestrcpy(&hostname, defGetString(def));
				hostTuple = SearchSysCache1(HOSTHOSTNAME, NameGetDatum(&hostname));
				if(!HeapTupleIsValid(hostTuple))
				{
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
						, errmsg("host \"%s\" does not exist", defGetString(def))));
				}
				datum[Anum_mgr_node_nodehost-1] = ObjectIdGetDatum(HeapTupleGetOid(hostTuple));
				got[Anum_mgr_node_nodehost-1] = true;
				ReleaseSysCache(hostTuple);
			}else if(strcmp(def->defname, "port") == 0)
			{
				int32 port;
				if(got[Anum_mgr_node_nodeport-1])
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
						,errmsg("conflicting or redundant options")));
				port = defGetInt32(def);
				if(port <= 0 || port > UINT16_MAX)
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
						,errmsg("%d is outside the valid range for parameter \"%s\" (%d .. %d)", port, "port", 1, UINT16_MAX)));
				datum[Anum_mgr_node_nodeport-1] = Int32GetDatum(port);
				got[Anum_mgr_node_nodeport-1] = true;
				newport = port;
			}else if(strcmp(def->defname, "path") == 0)
			{
				if(got[Anum_mgr_node_nodepath-1])
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
						,errmsg("conflicting or redundant options")));
				if (bnodeInCluster)
					ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
					 ,errmsg("%s \"%s\" has been initialized in the cluster, cannot be changed"
					 , mgr_nodetype_str(nodetype), NameStr(name))));
				str = defGetString(def);
				if(str[0] != '/' || str[0] == '\0')
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
						,errmsg("invalid absoulte path: \"%s\"", str)));
				datum[Anum_mgr_node_nodepath-1] = PointerGetDatum(cstring_to_text(str));
				got[Anum_mgr_node_nodepath-1] = true;
			}else if(strcmp(def->defname, "sync_state") == 0)
			{
				if(got[Anum_mgr_node_nodesync-1])
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
						,errmsg("conflicting or redundant options")));
				if (nodetype == mastertype)
					ereport(ERROR, (errmsg("synchronous relationship must set on the slave node")));

				str = defGetString(def);
				if (strcmp(str, sync_state_tab[SYNC_STATE_SYNC].name) != 0
						&& strcmp(str, sync_state_tab[SYNC_STATE_ASYNC].name) != 0
						&& strcmp(str, sync_state_tab[SYNC_STATE_POTENTIAL].name) != 0)
				{
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
						,errmsg("the sync_state of node can be set as \"sync\", \"potential\" or \"async\"")));
				}
				do
				{
					if (strcmp(zoneData.data, mgr_zone) !=0)
					{
						ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
						,errmsg("not support alter sync_state if the node not in current zone \"%s\"", mgr_zone)));
					}
					/*sync state*/
					if(strcmp(str, sync_state_tab[SYNC_STATE_SYNC].name) == 0)
					{
						namestrcpy(&sync_state_name, sync_state_tab[SYNC_STATE_SYNC].name);
						new_sync = SYNC_STATE_SYNC;
					}
					else if(strcmp(str, sync_state_tab[SYNC_STATE_ASYNC].name) == 0)
					{
						namestrcpy(&sync_state_name, sync_state_tab[SYNC_STATE_ASYNC].name);
						new_sync = SYNC_STATE_ASYNC;

						if (bnodeInCluster)
						{
							if ((!hasSyncNodeInCluster) && hasPotenNodeInCluster)
							{
								/* put one potential to sync in cluster */
								mgr_update_one_potential_to_sync(rel, masterTupleOid, true, selftupleoid);
							}
							else if ((!hasSyncNode) && hasPotenNode)
							{
								/* put one potential which is not in cluster to sync */
								mgr_update_one_potential_to_sync(rel, masterTupleOid, false, selftupleoid);
							}
						}
					}else if(strcmp(str, sync_state_tab[SYNC_STATE_POTENTIAL].name) == 0)
					{

						if(hasSyncNodeInCluster)
						{
							namestrcpy(&sync_state_name, sync_state_tab[SYNC_STATE_POTENTIAL].name);
							new_sync = SYNC_STATE_POTENTIAL;
						}
						else
						{
							ereport(NOTICE, (errmsg("the master of this node has no synchronous slave node, "
							"make this node as synchronous node")));
							namestrcpy(&sync_state_name, sync_state_tab[SYNC_STATE_SYNC].name);
							new_sync = SYNC_STATE_SYNC;
						}
					}
					else
					{
						ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
							,errmsg("the sync_state of node can be set as \"sync\", \"potential\" or \"async\"")));
					}
				}while(0);
				datum[Anum_mgr_node_nodesync-1] = NameGetDatum(&sync_state_name);
				got[Anum_mgr_node_nodesync-1] = true;
			}else if(strcmp(def->defname, "readonly") == 0)
			{
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("not support to modify the column \"readonly\"")));
			}else
			{
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("option \"%s\" is not recognized", def->defname)
					, errhint("option is host, port, sync_state and path")));
			}
			datum[Anum_mgr_node_nodetype-1] = CharGetDatum(nodetype);
		}
		/*check port*/
		if (mgr_check_node_port(rel, hostoid, newport))
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
					,errmsg("on host \"%s\" the port \"%d\" has already been used in node table"
					, get_hostname_from_hostoid(hostoid), newport)
					,errhint("try \"list node;\" for more information")));
		}
		/*check if this tuple is initiated. If it has been initiated and it is in cluster, then we check whether it can be altered*/
		if(bnodeInCluster)
		{
			if(got[Anum_mgr_node_nodesync-1] == true)
			{
				/* get one sync slave node for refresh pgxc_node on read only coordinator */
				NameData newSyncSlaveName;
				newSyncSlaveName.data[0] = '\0';
				/* check the slave node streaming replicate normal */
				if (SYNC_STATE_SYNC == new_sync)
				{
					if (!mgr_check_slave_replicate_status(masterTupleOid, nodetype, NameStr(name)))
						ereport(ERROR, (errmsg("the streaming replication of %s \"%s\" is not normal"
						, mgr_nodetype_str(nodetype), NameStr(name))));
				}

				initStringInfo(&infoSyncStr);
				initStringInfo(&infosendmsg);
				initStringInfo(&(getAgentCmdRst.description));
				initStringInfo(&infoSyncStrTmp);

				syncNum = mgr_get_master_sync_string(masterTupleOid, true, selftupleoid, &infoSyncStr);
				if(infoSyncStr.len != 0 && syncNum > 0)
				{
					int i = 0;
					while(i<infoSyncStr.len && infoSyncStr.data[i] != ',' && i<NAMEDATALEN)
					{
							newSyncSlaveName.data[i] = infoSyncStr.data[i];
							i++;
					}
					if (i<NAMEDATALEN)
						newSyncSlaveName.data[i] = '\0';
				}

				if (0 == infoSyncStr.len)
				{
					if (SYNC_STATE_SYNC == new_sync)
					{
						syncNum++;
						namestrcpy(&newSyncSlaveName, NameStr(name));
						mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", NameStr(name)
							, &infosendmsg);
					}
					else
					{
						mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
					}
				}
				else
				{
					if (SYNC_STATE_SYNC == new_sync)
					{
						syncNum++;
						if (syncNum == 1)
							namestrcpy(&newSyncSlaveName, NameStr(name));
						appendStringInfo(&infoSyncStrTmp, "%d (%s,%s)", syncNum, NameStr(name), infoSyncStr.data);
					}
					else if (SYNC_STATE_POTENTIAL == new_sync)
					{
						appendStringInfo(&infoSyncStrTmp, "%d (%s,%s)", syncNum, infoSyncStr.data, NameStr(name));
					}
					else
						appendStringInfo(&infoSyncStrTmp, "%d (%s)", syncNum, infoSyncStr.data);
					mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", infoSyncStrTmp.data, &infosendmsg);
				}

				masterPath = get_nodepath_from_tupleoid(masterTupleOid);
				mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, masterPath, &infosendmsg, masterHostOid, &getAgentCmdRst);
				pfree(masterPath);
				pfree(infoSyncStr.data);
				pfree(infosendmsg.data);
				if (!getAgentCmdRst.ret)
					ereport(ERROR, (errmsg("refresh synchronous_standby_names='%s' on %s \"%s\" fail %s"
						, infoSyncStrTmp.data, mgr_nodetype_str(mastertype), mastername.data, getAgentCmdRst.description.data)));
				pfree(infoSyncStrTmp.data);
				pfree(getAgentCmdRst.description.data);

			}
			if (got[Anum_mgr_node_nodeport-1] == true)
			{
				if (CNDN_TYPE_GTM_COOR_MASTER == nodetype)
					mgr_check_job_in_updateparam("monitor_handle_gtm");
				else if (CNDN_TYPE_COORDINATOR_MASTER == nodetype)
					mgr_check_job_in_updateparam("monitor_handle_coordinator");
				else if (CNDN_TYPE_DATANODE_MASTER == nodetype)
					mgr_check_job_in_updateparam("monitor_handle_datanode");
				mgr_modify_port_after_initd(rel, oldtuple, name.data, nodetype, newport);
			}
		}
	}PG_CATCH();
	{
		if (HeapTupleIsValid(oldtuple))
			heap_freetuple(oldtuple);
		heap_close(rel, RowExclusiveLock);
		PG_RE_THROW();
	}PG_END_TRY();

	new_tuple = heap_modify_tuple(oldtuple, cndn_dsc, datum,isnull, got);
	CatalogTupleUpdate(rel, &oldtuple->t_self, new_tuple);

	heap_freetuple(oldtuple);
	heap_close(rel, RowExclusiveLock);

	PG_RETURN_BOOL(true);
}

void mgr_drop_node(MGRDropNode *node, ParamListInfo params, DestReceiver *dest)
{
	if (mgr_has_priv_drop())
	{
		DirectFunctionCall2(mgr_drop_node_func,
									CharGetDatum(node->nodetype),
									PointerGetDatum(node->name));
		return;
	}
	else
	{
		ereport(ERROR, (errmsg("permission denied")));
		return ;
	}
}

Datum mgr_drop_node_func(PG_FUNCTION_ARGS)
{
	Relation rel;
	Relation rel_updateparm;
	HeapTuple tuple;
	MemoryContext context, old_context;
	NameData nodenameData;
	NameData nameall;
	NameData syncData;
	HeapScanDesc rel_scan;
	ScanKeyData key[1];
	Form_mgr_node mgr_node;
	char nodetype;
	char mastertype;
	char *nodename;
	int getnum = 0;
	Oid selftupleoid;
	Oid mastertupleoid;

	nodetype = PG_GETARG_CHAR(0);
	nodename = PG_GETARG_CSTRING(1);
	namestrcpy(&nodenameData, nodename);
	context = AllocSetContextCreate(CurrentMemoryContext,
									"DROP NODE",
									ALLOCSET_DEFAULT_SIZES);
	rel = heap_open(NodeRelationId, RowExclusiveLock);
	old_context = MemoryContextSwitchTo(context);

	/* first we need check is it all exists and used by other */
	tuple = mgr_get_tuple_node_from_name_type(rel, nodename);
	if(!HeapTupleIsValid(tuple))
	{
		heap_close(rel, RowExclusiveLock);
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					,errmsg("%s \"%s\" does not exist", mgr_nodetype_str(nodetype), nodename)));
	}
	/*check this tuple initd or not, if it has inited and in cluster, cannot be dropped*/
	mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
	Assert(mgr_node);
	if(mgr_node->nodeincluster)
	{
		heap_freetuple(tuple);
		heap_close(rel, RowExclusiveLock);
		ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
				 ,errmsg("%s \"%s\" has been initialized in the cluster, cannot be dropped"
				 , mgr_nodetype_str(nodetype), nodename)));
	}
	/*check the node has been used by its slave*/
	if (CNDN_TYPE_DATANODE_MASTER == mgr_node->nodetype|| CNDN_TYPE_GTM_COOR_MASTER == mgr_node->nodetype
		|| CNDN_TYPE_COORDINATOR_MASTER == mgr_node->nodetype)
	{
		if (mgr_node_has_slave(rel, HeapTupleGetOid(tuple)))
		{
			heap_freetuple(tuple);
			heap_close(rel, RowExclusiveLock);
			ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
					 ,errmsg("%s \"%s\" has been used by slave, cannot be dropped"
						, mgr_nodetype_str(nodetype), nodename)));
		}
	}
	namestrcpy(&syncData, NameStr(mgr_node->nodesync));
	mastertupleoid = mgr_node->nodemasternameoid;
	selftupleoid = HeapTupleGetOid(tuple);
	CatalogTupleDelete(rel, &(tuple->t_self));
	heap_freetuple(tuple);

	/* now we can delete node(s) */
	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(nodetype));
	rel_scan = heap_beginscan_catalog(rel, 1, key);
	getnum = 0;
	while ((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		if(HeapTupleIsValid(tuple))
		{
			getnum++;
		}
	}
	heap_endscan(rel_scan);

	/*delete the parm in mgr_updateparm for this type of node*/
	rel_updateparm = heap_open(UpdateparmRelationId, RowExclusiveLock);
	mgr_parmr_delete_tuple_nodename_nodetype(rel_updateparm, &nodenameData, nodetype);

	mastertype = mgr_get_master_type(nodetype);
	if (mastertype != nodetype && (strcmp(syncData.data, sync_state_tab[SYNC_STATE_SYNC].name) == 0))
	{
		/*if the node is sync node, and its master has potential node, 
		* we need update one potential node to sync node
		*/
		if (!mgr_check_syncstate_node_exist(rel, mastertupleoid, SYNC_STATE_SYNC, selftupleoid, false))
		{
			mgr_update_one_potential_to_sync(rel, mastertupleoid, false, selftupleoid);
		}
	}

	/*if the node is coordinator, so it's need to update the hba table*/
	if( CNDN_TYPE_COORDINATOR_MASTER == nodetype)
	{
		mgr_clean_hba_table(nodename, NULL);
	}

	/*delete the parm in mgr_updateparm for this type and nodename in mgr_updateparm is MACRO_STAND_FOR_ALL_NODENAME*/
	if (getnum == 1)
	{
		namestrcpy(&nameall, MACRO_STAND_FOR_ALL_NODENAME);
		mgr_parmr_delete_tuple_nodename_nodetype(rel_updateparm, &nameall, nodetype);
	}
	heap_close(rel_updateparm, RowExclusiveLock);
	heap_close(rel, RowExclusiveLock);
	(void)MemoryContextSwitchTo(old_context);
	MemoryContextDelete(context);
	PG_RETURN_BOOL(true);
}

/*
* execute init gtm master, send information to agent to init gtm master
*/
Datum
mgr_init_gtmcoor_master(PG_FUNCTION_ARGS)
{
	List *nodenamelist = NIL;

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot assign TransactionIds during recovery")));

	nodenamelist = mgr_get_nodetype_namelist(CNDN_TYPE_GTM_COOR_MASTER);
	return mgr_runmode_cndn(CNDN_TYPE_GTM_COOR_MASTER, AGT_CMD_GTMCOOR_INIT, nodenamelist, TAKEPLAPARM_N, fcinfo);
}

/*
* execute init gtm slave, send information to agent to init gtm slave
*/
Datum
mgr_init_gtmcoor_slave(PG_FUNCTION_ARGS)
{
	List *nodenamelist = NIL;

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot assign TransactionIds during recovery")));

	nodenamelist = mgr_get_nodetype_namelist(CNDN_TYPE_GTM_COOR_SLAVE);
	return mgr_runmode_cndn(CNDN_TYPE_GTM_COOR_SLAVE, AGT_CMD_GTMCOOR_SLAVE_INIT, nodenamelist, TAKEPLAPARM_N, fcinfo);
}

/*
* init coordinator master dn1,dn2...
* init coordinator master all
*/
Datum
mgr_init_cn_master(PG_FUNCTION_ARGS)
{
	List *nodenamelist = NIL;

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot assign TransactionIds during recovery")));

	if (PG_ARGISNULL(0))
		nodenamelist = mgr_get_nodetype_namelist(CNDN_TYPE_COORDINATOR_MASTER);
	else
		nodenamelist = get_fcinfo_namelist("", 0, fcinfo);

	return mgr_runmode_cndn(CNDN_TYPE_COORDINATOR_MASTER, AGT_CMD_CNDN_CNDN_INIT
		, nodenamelist, TAKEPLAPARM_N, fcinfo);
}

/*
* init datanode master dn1,dn2...
* init datanode master all
*/
Datum
mgr_init_dn_master(PG_FUNCTION_ARGS)
{
	List *nodenamelist = NIL;

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot assign TransactionIds during recovery")));

	if (PG_ARGISNULL(0))
		nodenamelist = mgr_get_nodetype_namelist(CNDN_TYPE_DATANODE_MASTER);
	else
		nodenamelist = get_fcinfo_namelist("", 0, fcinfo);

	return mgr_runmode_cndn(CNDN_TYPE_DATANODE_MASTER, AGT_CMD_CNDN_CNDN_INIT, nodenamelist, TAKEPLAPARM_N, fcinfo);
}

/*
*	execute init datanode slave all, send information to agent to init
*/
Datum
mgr_init_dn_slave_all(PG_FUNCTION_ARGS)
{
	InitNodeInfo *info;
	GetAgentCmdRst getAgentCmdRst;
	Form_mgr_node mgr_node;
	FuncCallContext *funcctx;
	HeapTuple tuple
			,tup_result,
			mastertuple;
	ScanKeyData key[1];
	uint32 masterport;
	Oid masterhostOid;
	char *masterhostaddress;
	char *mastername;

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot assign TransactionIds during recovery")));

	/*output the exec result: col1 hostname,col2 SUCCESS(t/f),col3 description*/
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		ScanKeyInit(&key[0],
		Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(CNDN_TYPE_DATANODE_SLAVE));
		info = palloc(sizeof(*info));
		info->rel_node = heap_open(NodeRelationId, RowExclusiveLock);
		info->rel_scan = heap_beginscan_catalog(info->rel_node, 1, key);
		/* save info */
		funcctx->user_fctx = info;
		MemoryContextSwitchTo(oldcontext);
	}
	funcctx = SRF_PERCALL_SETUP();
	info = funcctx->user_fctx;
	Assert(info);
	tuple = heap_getnext(info->rel_scan, ForwardScanDirection);
	if(tuple == NULL)
	{
		/* end of row */
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, RowExclusiveLock);
		pfree(info);
		SRF_RETURN_DONE(funcctx);
	}
	/*get nodename*/
	mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
	Assert(mgr_node);
	/*get the master port, master host address*/
	mastertuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(mgr_node->nodemasternameoid));
	if(!HeapTupleIsValid(mastertuple))
	{
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, RowExclusiveLock);
		pfree(info);
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
			, errmsg("datanode master \"%s\" does not exist", NameStr(mgr_node->nodename))));
	}
	mgr_node = (Form_mgr_node)GETSTRUCT(mastertuple);
	Assert(mastertuple);
	masterport = mgr_node->nodeport;
	masterhostOid = mgr_node->nodehost;
	mastername = NameStr(mgr_node->nodename);
	masterhostaddress = get_hostaddress_from_hostoid(masterhostOid);
	ReleaseSysCache(mastertuple);
	initStringInfo(&(getAgentCmdRst.description));
	mgr_init_dn_slave_get_result(AGT_CMD_CNDN_SLAVE_INIT, &getAgentCmdRst, info->rel_node, tuple, masterhostaddress, masterport, mastername);
	pfree(masterhostaddress);
	tup_result = build_common_command_tuple(
		&(getAgentCmdRst.nodename)
		, getAgentCmdRst.ret
		, getAgentCmdRst.description.data);
	pfree(getAgentCmdRst.description.data);
	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}

void mgr_init_dn_slave_get_result(const char cmdtype, GetAgentCmdRst *getAgentCmdRst, Relation noderel, HeapTuple aimtuple, char *masterhostaddress, uint32 masterport, char *mastername)
{
	/*get datanode slave path from adbmgr.node*/
	Datum datumPath;
	char *cndnPath;
	char *cndnnametmp;
	char *nodetypestr;
	char *user;
	char nodetype;
	Oid hostOid;
	Oid	masteroid;
	Oid	tupleOid;
	StringInfoData buf;
	StringInfoData infosendmsg;
	StringInfoData strinfocoordport;
	ManagerAgent *ma;
	bool initdone = false;
	bool isNull = false;
	bool ismasterrunning = false;
	Form_mgr_node mgr_node;
	int cndnport;
	Datum DatumStartDnMaster,
	DatumStopDnMaster;

	getAgentCmdRst->ret = false;
	initStringInfo(&infosendmsg);
	/*get column values from aimtuple*/
	mgr_node = (Form_mgr_node)GETSTRUCT(aimtuple);
	Assert(mgr_node);
	cndnnametmp = NameStr(mgr_node->nodename);
	hostOid = mgr_node->nodehost;
	/*get the port*/
	cndnport = mgr_node->nodeport;
	/*get master oid*/
	masteroid = mgr_node->nodemasternameoid;
	/*get nodetype*/
	nodetype = mgr_node->nodetype;
	/*get tuple oid*/
	tupleOid = HeapTupleGetOid(aimtuple);
	/*get the host address for return result*/
	namestrcpy(&(getAgentCmdRst->nodename), cndnnametmp);
	/*check node init or not*/
	if (mgr_node->nodeinited)
	{
		nodetypestr = mgr_nodetype_str(nodetype);
		appendStringInfo(&(getAgentCmdRst->description), "%s \"%s\" has been initialized", nodetypestr, cndnnametmp);
		getAgentCmdRst->ret = false;
		pfree(nodetypestr);
		return;
	}
	/*get cndnPath from aimtuple*/
	datumPath = heap_getattr(aimtuple, Anum_mgr_node_nodepath, RelationGetDescr(noderel), &isNull);
	if(isNull)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
			, errmsg("column cndnpath is null")));
	}
	/*if datanode master doesnot running, first make it running*/
	initStringInfo(&strinfocoordport);
	appendStringInfo(&strinfocoordport, "%d", masterport);
	user = get_hostuser_from_hostoid(mgr_node->nodehost);
	ismasterrunning = pingNode_user(masterhostaddress, strinfocoordport.data, user);
	pfree(user);
	pfree(strinfocoordport.data);
	if(ismasterrunning != PQPING_OK && ismasterrunning != PQPING_REJECT)
	{
		/*it need start datanode master*/
		DatumStartDnMaster = DirectFunctionCall1(mgr_start_one_dn_master, CStringGetDatum(mastername));
		if(DatumGetObjectId(DatumStartDnMaster) == InvalidOid)
			ereport(ERROR,
				(errmsg("start datanode master \"%s\" fail", mastername)));
	}
	cndnPath = TextDatumGetCString(datumPath);
	appendStringInfo(&infosendmsg, " -p %u", masterport);
	appendStringInfo(&infosendmsg, " -h %s", masterhostaddress);
	appendStringInfo(&infosendmsg, " -D %s", cndnPath);
	appendStringInfo(&infosendmsg, " --nodename %s", cndnnametmp);
	/* connection agent */
	ma = ma_connect_hostoid(hostOid);
	if(!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst->ret = false;
		appendStringInfoString(&(getAgentCmdRst->description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}

	/*send path*/
	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, cmdtype);
	ma_sendstring(&buf,infosendmsg.data);
	ma_endmessage(&buf, ma);
	if (! ma_flush(ma, true))
	{
		getAgentCmdRst->ret = false;
		appendStringInfoString(&(getAgentCmdRst->description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}
	/*check the receive msg*/
	initdone = mgr_recv_msg(ma, getAgentCmdRst);
	ma_close(ma);
	/*stop datanode master if we start it*/
	if(ismasterrunning != PQPING_OK && ismasterrunning != PQPING_REJECT)
	{
		/*it need start datanode master*/
		DatumStopDnMaster = DirectFunctionCall1(mgr_stop_one_dn_master, CStringGetDatum(mastername));
		if(DatumGetObjectId(DatumStopDnMaster) == InvalidOid)
			ereport(ERROR,
				(errmsg("stop datanode master \"%s\" fail", mastername)));
	}
	/*update node system table's column to set initial is true*/
	if (initdone)
	{
		mgr_node->nodeinited = true;
		heap_inplace_update(noderel, aimtuple);
		/*refresh postgresql.conf of this node*/
		resetStringInfo(&(getAgentCmdRst->description));
		resetStringInfo(&infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("pgxc_node_name", cndnnametmp, &infosendmsg);
		mgr_add_parameters_pgsqlconf(tupleOid, nodetype, cndnport, &infosendmsg);
		mgr_add_parm(cndnnametmp, nodetype, &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("pgxc_node_name", cndnnametmp, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, cndnPath, &infosendmsg, hostOid, getAgentCmdRst);
		/*refresh recovry.conf*/
		resetStringInfo(&(getAgentCmdRst->description));
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_recoveryconf(nodetype, NameStr(mgr_node->nodename), masteroid, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_RECOVERCONF, cndnPath, &infosendmsg, hostOid, getAgentCmdRst);
	}
	pfree(infosendmsg.data);
}

/*
* get the datanode/coordinator name list
*/
List *
get_fcinfo_namelist(const char *sepstr, int argidx, FunctionCallInfo fcinfo)
{
	int i;
	char *nodename;
	List *nodenamelist = NIL;

	for (i = argidx; i < PG_NARGS(); i++)
	{
		if (!PG_ARGISNULL(i))
		{
			nodename = PG_GETARG_CSTRING(i);
			nodenamelist = lappend(nodenamelist, nodename);
		}
	}

	return nodenamelist;
}

/*
* start gtm master
*/
Datum mgr_start_gtmcoor_master(PG_FUNCTION_ARGS)
{
	List *nodenamelist = NIL;
	char *nodename;

	mgr_check_job_in_updateparam("monitor_handle_gtm");
	if (PG_ARGISNULL(0))
	{
		nodenamelist = mgr_get_nodetype_namelist(CNDN_TYPE_GTM_COOR_MASTER);
		updateDoctorStatusOfMgrNodes(nodenamelist, CNDN_TYPE_GTM_COOR_MASTER, true, CURE_STATUS_NORMAL);
		return mgr_typenode_cmd_run_backend_result(CNDN_TYPE_GTM_COOR_MASTER, AGT_CMD_GTMCOOR_START_MASTER_BACKEND, nodenamelist, TAKEPLAPARM_N,fcinfo);
	}
	else
	{
		nodename = PG_GETARG_CSTRING(0);
		nodenamelist = lappend(nodenamelist, nodename);
		updateDoctorStatusOfMgrNodes(nodenamelist, CNDN_TYPE_GTM_COOR_MASTER, true, CURE_STATUS_NORMAL);
		return mgr_runmode_cndn(CNDN_TYPE_GTM_COOR_MASTER, AGT_CMD_GTMCOOR_START_MASTER, nodenamelist, TAKEPLAPARM_N, fcinfo);
	}
}

/*
* start gtm slave
*/
Datum mgr_start_gtmcoor_slave(PG_FUNCTION_ARGS)
{
	List *nodenamelist = NIL;
	char *nodename;

	if (PG_ARGISNULL(0))
	{
		nodenamelist = mgr_get_nodetype_namelist(CNDN_TYPE_GTM_COOR_SLAVE);
		updateDoctorStatusOfMgrNodes(nodenamelist, CNDN_TYPE_GTM_COOR_SLAVE, true, CURE_STATUS_NORMAL);
		return mgr_typenode_cmd_run_backend_result(CNDN_TYPE_GTM_COOR_SLAVE, AGT_CMD_GTMCOOR_START_SLAVE_BACKEND, nodenamelist, TAKEPLAPARM_N, fcinfo);
	}
	else
	{
		nodename = PG_GETARG_CSTRING(0);
		nodenamelist = lappend(nodenamelist, nodename);
		updateDoctorStatusOfMgrNodes(nodenamelist, CNDN_TYPE_GTM_COOR_SLAVE, true, CURE_STATUS_NORMAL);
		return mgr_runmode_cndn(CNDN_TYPE_GTM_COOR_SLAVE, AGT_CMD_GTMCOOR_START_SLAVE, nodenamelist, TAKEPLAPARM_N, fcinfo);
	}
}

/*
* start coordinator master dn1,dn2...
* start coordinator master all
*/
Datum mgr_start_cn_master(PG_FUNCTION_ARGS)
{
	List *nodenamelist = NIL;

	mgr_check_job_in_updateparam("monitor_handle_coordinator");

	if (PG_ARGISNULL(0))
	{
		nodenamelist = mgr_get_nodetype_namelist(CNDN_TYPE_COORDINATOR_MASTER);
		updateDoctorStatusOfMgrNodes(nodenamelist, CNDN_TYPE_COORDINATOR_MASTER, true, CURE_STATUS_NORMAL);
		return mgr_typenode_cmd_run_backend_result(CNDN_TYPE_COORDINATOR_MASTER, AGT_CMD_CN_START_BACKEND, nodenamelist, TAKEPLAPARM_N, fcinfo);
	}
	else
	{
		nodenamelist = get_fcinfo_namelist("", 0, fcinfo);
		updateDoctorStatusOfMgrNodes(nodenamelist, CNDN_TYPE_COORDINATOR_MASTER, true, CURE_STATUS_NORMAL);
		return mgr_runmode_cndn(CNDN_TYPE_COORDINATOR_MASTER, AGT_CMD_CN_START, nodenamelist, TAKEPLAPARM_N, fcinfo);
	}
}

/*
* start coordinator slave dn1,dn2...
* start coordinator slave all
*/
Datum mgr_start_cn_slave(PG_FUNCTION_ARGS)
{
	List *nodenamelist = NIL;

	mgr_check_job_in_updateparam("monitor_handle_coordinator");

	if (PG_ARGISNULL(0))
	{
		nodenamelist = mgr_get_nodetype_namelist(CNDN_TYPE_COORDINATOR_SLAVE);
		updateDoctorStatusOfMgrNodes(nodenamelist, CNDN_TYPE_COORDINATOR_SLAVE, true, CURE_STATUS_NORMAL);
		return mgr_typenode_cmd_run_backend_result(CNDN_TYPE_COORDINATOR_SLAVE, AGT_CMD_CN_START_BACKEND, nodenamelist, TAKEPLAPARM_N, fcinfo);
	}
	else
	{
		nodenamelist = get_fcinfo_namelist("", 0, fcinfo);
		updateDoctorStatusOfMgrNodes(nodenamelist, CNDN_TYPE_COORDINATOR_SLAVE, true, CURE_STATUS_NORMAL);
		return mgr_runmode_cndn(CNDN_TYPE_COORDINATOR_SLAVE, AGT_CMD_CN_START, nodenamelist, TAKEPLAPARM_N, fcinfo);
	}
}

/*
* start datanode master dn1,dn2...
* start datanode master all
*/
Datum mgr_start_dn_master(PG_FUNCTION_ARGS)
{
	List *nodenamelist = NIL;

	mgr_check_job_in_updateparam("monitor_handle_datanode");
	if (PG_ARGISNULL(0))
	{
		nodenamelist = mgr_get_nodetype_namelist(CNDN_TYPE_DATANODE_MASTER);
		updateDoctorStatusOfMgrNodes(nodenamelist, CNDN_TYPE_DATANODE_MASTER, true, CURE_STATUS_NORMAL);
		return mgr_typenode_cmd_run_backend_result(CNDN_TYPE_DATANODE_MASTER, AGT_CMD_DN_START_BACKEND, nodenamelist, TAKEPLAPARM_N, fcinfo);
	}
	else
	{
		nodenamelist = get_fcinfo_namelist("", 0, fcinfo);
		updateDoctorStatusOfMgrNodes(nodenamelist, CNDN_TYPE_DATANODE_MASTER, true, CURE_STATUS_NORMAL);
		return mgr_runmode_cndn(CNDN_TYPE_DATANODE_MASTER, AGT_CMD_DN_START, nodenamelist, TAKEPLAPARM_N, fcinfo);
	}
}

/*
* start datanode master dn1
*/
Datum mgr_start_one_dn_master(PG_FUNCTION_ARGS)
{
	GetAgentCmdRst getAgentCmdRst;
	HeapTuple tup_result
			,aimtuple;
	char *nodename;
	InitNodeInfo *info;

	nodename = PG_GETARG_CSTRING(0);
	info = palloc(sizeof(*info));
	info->rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	aimtuple = mgr_get_tuple_node_from_name_type(info->rel_node, nodename);
	if (!HeapTupleIsValid(aimtuple))
	{
		heap_close(info->rel_node, RowExclusiveLock);
		pfree(info);
		ereport(ERROR,
			(errmsg("datanode master \"%s\" does not exist", nodename)));
	}
	/*get execute cmd result from agent*/
	initStringInfo(&(getAgentCmdRst.description));
	mgr_runmode_cndn_get_result(AGT_CMD_DN_START, &getAgentCmdRst, info->rel_node, aimtuple, TAKEPLAPARM_N);
	tup_result = build_common_command_tuple(
		&(getAgentCmdRst.nodename)
		, getAgentCmdRst.ret
		, getAgentCmdRst.description.data);
	heap_freetuple(aimtuple);
	heap_close(info->rel_node, RowExclusiveLock);
	pfree(getAgentCmdRst.description.data);
	pfree(info);
	return HeapTupleGetDatum(tup_result);
}

/*
* start datanode slave dn1,dn2...
* start datanode slave all
*/
Datum mgr_start_dn_slave(PG_FUNCTION_ARGS)
{
	List *nodenamelist = NIL;

	if (PG_ARGISNULL(0))
	{
		nodenamelist = mgr_get_nodetype_namelist(CNDN_TYPE_DATANODE_SLAVE);
		updateDoctorStatusOfMgrNodes(nodenamelist, CNDN_TYPE_DATANODE_SLAVE, true, CURE_STATUS_NORMAL);
		return mgr_typenode_cmd_run_backend_result(CNDN_TYPE_DATANODE_SLAVE, AGT_CMD_DN_START_BACKEND, nodenamelist, TAKEPLAPARM_N, fcinfo);
	}
	else
	{
		nodenamelist = get_fcinfo_namelist("", 0, fcinfo);
		updateDoctorStatusOfMgrNodes(nodenamelist, CNDN_TYPE_DATANODE_SLAVE, true, CURE_STATUS_NORMAL);
		return mgr_runmode_cndn(CNDN_TYPE_DATANODE_SLAVE, AGT_CMD_DN_START, nodenamelist, TAKEPLAPARM_N, fcinfo);
	}
}

void mgr_runmode_cndn_get_result(const char cmdtype, GetAgentCmdRst *getAgentCmdRst, Relation noderel, HeapTuple aimtuple, const char *shutdown_mode)
{
	Form_mgr_node mgr_node;
	Form_mgr_node mgr_nodetmp;
	Form_mgr_node mgr_node_gtm;
	Datum datumPath;
	Datum DatumStopDnMaster;
	StringInfoData infosendmsg;
	StringInfoData strinfoport;
	bool isNull = false;
	bool execRes = false;
	char *hostaddress;
	char *cndnPath;
	char *cmdmode;
	char *zmode = NULL;
	char *user = NULL;
	char *cndnname;
	char *masterhostaddress;
	char *mastername;
	char *nodetypestr;
	char nodetype;
	char cmdtype_s = -1;
	int32 cndnport;
	int masterport;
	Oid hostOid;
	Oid nodemasternameoid;
	Oid	tupleOid;
	Oid	masterhostOid;
	Oid cnoid;
	bool ismasterrunning = 0;
	HeapTuple gtmmastertuple;
	NameData cndnnamedata;
	HeapTuple mastertuple;
	PGconn *pg_conn = NULL;

	getAgentCmdRst->ret = false;
	initStringInfo(&infosendmsg);
	initStringInfo(&strinfoport);
	/*get column values from aimtuple*/
	mgr_node = (Form_mgr_node)GETSTRUCT(aimtuple);
	Assert(mgr_node);
	hostOid = mgr_node->nodehost;
	/*get host address*/
	hostaddress = get_hostaddress_from_hostoid(hostOid);
	Assert(hostaddress);
	/*get nodename*/
	cndnname = NameStr(mgr_node->nodename);
	/*get the host address for return result*/
	namestrcpy(&(getAgentCmdRst->nodename), cndnname);
	/*get node type*/
	nodetype = mgr_node->nodetype;
	nodetypestr = mgr_nodetype_str(nodetype);
	/* Clear the slave node information about the read-only query in the pgxc_node table,
	 * avoid repeating node names and causing subsequent work to fail */
	if (AGT_CMD_GTMCOOR_SLAVE_FAILOVER == cmdtype || AGT_CMD_DN_FAILOVER == cmdtype)
		mgr_clean_cn_pgxcnode_readonlysql_slave();
	/*check node init or not*/
	if ((AGT_CMD_CNDN_CNDN_INIT == cmdtype || AGT_CMD_GTMCOOR_INIT == cmdtype || AGT_CMD_GTMCOOR_SLAVE_INIT == cmdtype ) && mgr_node->nodeinited)
	{
		appendStringInfo(&(getAgentCmdRst->description), "%s \"%s\" has been initialized", nodetypestr, cndnname);
		getAgentCmdRst->ret = false;
		goto end;
	}
	if(AGT_CMD_CNDN_CNDN_INIT != cmdtype && AGT_CMD_GTMCOOR_INIT != cmdtype && AGT_CMD_GTMCOOR_SLAVE_INIT != cmdtype
		&& AGT_CMD_CLEAN_NODE != cmdtype && AGT_CMD_GTMCOOR_STOP_MASTER != cmdtype && AGT_CMD_GTMCOOR_STOP_SLAVE != cmdtype
		&& AGT_CMD_CN_STOP != cmdtype && AGT_CMD_DN_STOP != cmdtype && !mgr_node->nodeinited
		&& AGT_CMD_DN_RESTART != cmdtype && AGT_CMD_CN_RESTART != cmdtype && AGT_CMD_AGTM_RESTART != cmdtype
		&& AGT_CMD_GTMCOOR_STOP_MASTER_BACKEND != cmdtype && AGT_CMD_GTMCOOR_STOP_SLAVE_BACKEND != cmdtype
		&& AGT_CMD_CN_STOP_BACKEND != cmdtype && AGT_CMD_DN_STOP_BACKEND != cmdtype)
	{
		appendStringInfo(&(getAgentCmdRst->description), "%s \"%s\" has not been initialized", nodetypestr, cndnname);
		getAgentCmdRst->ret = false;
		goto end;
	}

	/*get the port*/
	cndnport = mgr_node->nodeport;
	/*get node master oid*/
	nodemasternameoid = mgr_node->nodemasternameoid;
	/*get tuple oid*/
	tupleOid = HeapTupleGetOid(aimtuple);
	datumPath = heap_getattr(aimtuple, Anum_mgr_node_nodepath, RelationGetDescr(noderel), &isNull);
	if(isNull)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
			, errmsg("column cndnpath is null")));
	}
	/*get cndnPath from aimtuple*/
	cndnPath = TextDatumGetCString(datumPath);
	switch(cmdtype)
	{
		case AGT_CMD_GTMCOOR_START_MASTER:
		case AGT_CMD_GTMCOOR_START_MASTER_BACKEND:
		case AGT_CMD_GTMCOOR_START_SLAVE:
		case AGT_CMD_GTMCOOR_START_SLAVE_BACKEND:
			cmdmode = "start";
			zmode = "gtm_coord";
			break;
		case AGT_CMD_GTMCOOR_STOP_MASTER:
		case AGT_CMD_GTMCOOR_STOP_MASTER_BACKEND:
		case AGT_CMD_GTMCOOR_STOP_SLAVE:
		case AGT_CMD_GTMCOOR_STOP_SLAVE_BACKEND:
			cmdmode = "stop";
			zmode = "gtm_coord";
			break;
		case AGT_CMD_CN_START:
		case AGT_CMD_CN_START_BACKEND:
				cmdmode = "start";
				zmode = "coordinator";
			break;
		case AGT_CMD_CN_STOP:
		case AGT_CMD_CN_STOP_BACKEND:
			cmdmode = "stop";
			zmode = "coordinator";
			break;
		case AGT_CMD_DN_START:
		case AGT_CMD_DN_START_BACKEND:
			cmdmode = "start";
			zmode = "datanode";
			break;
		case AGT_CMD_DN_RESTART:
			cmdmode = "restart";
			zmode = "datanode";
			break;
		case AGT_CMD_CN_RESTART:
			cmdmode = "restart";
			zmode = "coordinator";
			break;
		case AGT_CMD_DN_STOP:
		case AGT_CMD_DN_STOP_BACKEND:
			cmdmode = "stop";
			zmode = "datanode";
			break;
		case AGT_CMD_DN_FAILOVER:
			cmdmode = "promote";
			zmode = "datanode";
			break;
		case AGT_CMD_GTMCOOR_SLAVE_FAILOVER:
			cmdmode = "promote";
			zmode = "node";
			break;
		case AGT_CMD_DN_MASTER_PROMOTE:
			cmdmode = "promote";
			zmode = "datanode";
			break;
		case AGT_CMD_AGTM_RESTART:
			cmdmode = "restart";
			zmode = "gtm_coord";
			break;
		case AGT_CMD_CLEAN_NODE:
			cmdmode = "rm -rf";
			break;
		default:
			/*never come here*/
			cmdmode = "node";
			zmode = "node";
			break;
	}

	/*init coordinator/datanode/gtmcoord*/
	if (AGT_CMD_CNDN_CNDN_INIT == cmdtype || AGT_CMD_GTMCOOR_INIT == cmdtype)
	{
		appendStringInfo(&infosendmsg, " -D %s", cndnPath);
		if (with_data_checksums)
			appendStringInfo(&infosendmsg, " --nodename %s -E UTF8 --locale=C -k", cndnname);
		else
			appendStringInfo(&infosendmsg, " --nodename %s -E UTF8 --locale=C", cndnname);
	}  /*init gtmcoord slave*/
	else if (AGT_CMD_GTMCOOR_SLAVE_INIT == cmdtype)
	{
		user = get_hostuser_from_hostoid(hostOid);
		/*get gtmcoord masterport, masterhostaddress*/
		gtmmastertuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(nodemasternameoid));
		if(!HeapTupleIsValid(gtmmastertuple))
		{
			appendStringInfo(&(getAgentCmdRst->description), "gtmcoord master dosen't exist");
			getAgentCmdRst->ret = false;
			ereport(LOG, (errcode(ERRCODE_UNDEFINED_OBJECT)
				, errmsg("gtmcoord master does not exist")));
			goto end;
		}
		mgr_node_gtm = (Form_mgr_node)GETSTRUCT(gtmmastertuple);
		Assert(gtmmastertuple);
		masterport = mgr_node_gtm->nodeport;
		masterhostOid = mgr_node_gtm->nodehost;
		mastername = NameStr(mgr_node_gtm->nodename);
		masterhostaddress = get_hostaddress_from_hostoid(masterhostOid);
		appendStringInfo(&infosendmsg, " -p %u", masterport);
		appendStringInfo(&infosendmsg, " -h %s", masterhostaddress);
		appendStringInfo(&infosendmsg, " -D %s", cndnPath);
		appendStringInfo(&infosendmsg, " -U %s", user);
		appendStringInfo(&infosendmsg, " --nodename %s", cndnname);
		ReleaseSysCache(gtmmastertuple);
		/*check it need start gtm master*/
		appendStringInfo(&strinfoport, "%d", masterport);
		ismasterrunning = pingNode_user(masterhostaddress, strinfoport.data, user);
		pfree(masterhostaddress);
		pfree(user);
		if(ismasterrunning != 0)
		{
			appendStringInfo(&(getAgentCmdRst->description), "gtmcoord master \"%s\" is not running normal", mastername);
			getAgentCmdRst->ret = false;
			ereport(WARNING, (errmsg("gtmcoord master \"%s\" is not running normal", mastername)));
			goto end;
		}
	}
	/*stop coordinator/datanode*/
	else if(AGT_CMD_CN_STOP == cmdtype || AGT_CMD_DN_STOP == cmdtype ||
			AGT_CMD_GTMCOOR_STOP_MASTER == cmdtype || AGT_CMD_GTMCOOR_STOP_SLAVE == cmdtype)
	{
		appendStringInfo(&infosendmsg, " %s -D %s", cmdmode, cndnPath);
		appendStringInfo(&infosendmsg, " -Z %s -m %s -o -i -w -c", zmode, shutdown_mode);
	}
	else if (AGT_CMD_GTMCOOR_SLAVE_FAILOVER == cmdtype)
	{
		/*pause cluster*/
		mgr_lock_cluster_involve_gtm_coord(&pg_conn, &cnoid);
		/*stop gtm master*/
		mastertuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(nodemasternameoid));
		if(!HeapTupleIsValid(mastertuple))
		{
			mgr_unlock_cluster_involve_gtm_coord(&pg_conn);
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				,errmsg("gtm master \"%s\" does not exist", cndnname)));
		}
		DatumStopDnMaster = DirectFunctionCall1(mgr_stop_one_gtm_master, (Datum)0);
		if(DatumGetObjectId(DatumStopDnMaster) == InvalidOid)
			ereport(WARNING, (errmsg("stop gtm master \"%s\" fail", cndnname)));
		ReleaseSysCache(mastertuple);

		appendStringInfo(&infosendmsg, " %s -w -D %s", cmdmode, cndnPath);
	}
	else if (AGT_CMD_DN_MASTER_PROMOTE == cmdtype)
	{
		appendStringInfo(&infosendmsg, " %s -w -D %s", cmdmode, cndnPath);
	}
	else if (AGT_CMD_DN_FAILOVER == cmdtype)
	{
		/*pause cluster*/
		mgr_lock_cluster_involve_gtm_coord(&pg_conn, &cnoid);
		/*stop datanode master*/
		 mastertuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(nodemasternameoid));
		 if(!HeapTupleIsValid(mastertuple))
		 {
			mgr_unlock_cluster_involve_gtm_coord(&pg_conn);
			ereport(WARNING, (errcode(ERRCODE_UNDEFINED_OBJECT)
				,errmsg("datanode master \"%s\" dosen't exist", cndnname)));
		 }
		 else
		 {
				mgr_nodetmp = (Form_mgr_node)GETSTRUCT(mastertuple);
				Assert(mgr_nodetmp);
				DatumStopDnMaster = DirectFunctionCall1(mgr_stop_one_dn_master, CStringGetDatum(NameStr(mgr_nodetmp->nodename)));
				if(DatumGetObjectId(DatumStopDnMaster) == InvalidOid)
						 ereport(WARNING, (errmsg("stop datanode master \"%s\" fail", cndnname)));
				ReleaseSysCache(mastertuple);
		 }

		appendStringInfo(&infosendmsg, " %s -w -D %s", cmdmode, cndnPath);
	}
	else if (AGT_CMD_DN_RESTART == cmdtype || AGT_CMD_CN_RESTART == cmdtype || AGT_CMD_AGTM_RESTART == cmdtype)
	{
		appendStringInfo(&infosendmsg, " %s -D %s", cmdmode, cndnPath);
		appendStringInfo(&infosendmsg, " -Z %s -m %s -o -i -w -c -l %s/logfile", zmode, shutdown_mode, cndnPath);
	}
	else if (AGT_CMD_CLEAN_NODE == cmdtype)
	{
		appendStringInfo(&infosendmsg, "rm -rf %s; mkdir -p %s; chmod 0700 %s", cndnPath, cndnPath, cndnPath);
	}
	else if(AGT_CMD_CN_STOP_BACKEND == cmdtype || AGT_CMD_DN_STOP_BACKEND == cmdtype ||
			AGT_CMD_GTMCOOR_STOP_MASTER_BACKEND == cmdtype || AGT_CMD_GTMCOOR_STOP_SLAVE_BACKEND == cmdtype)
	{
		cmdtype_s = mgr_change_cmdtype_unbackend(cmdtype);
		appendStringInfo(&infosendmsg, " %s -D %s", cmdmode, cndnPath);
		appendStringInfo(&infosendmsg, " -Z %s -m %s -o -i -w -c -W", zmode, shutdown_mode);
	}
	else if (AGT_CMD_CN_START_BACKEND == cmdtype || AGT_CMD_DN_START_BACKEND == cmdtype ||
		AGT_CMD_GTMCOOR_START_MASTER_BACKEND == cmdtype || AGT_CMD_GTMCOOR_START_SLAVE_BACKEND == cmdtype)
	{
		cmdtype_s = mgr_change_cmdtype_unbackend(cmdtype);
		appendStringInfo(&infosendmsg, " %s -D %s", cmdmode, cndnPath);
		appendStringInfo(&infosendmsg, " -Z %s -o -i -w -c -W -l %s/logfile", zmode, cndnPath);
	}
	else /*dn,cn start*/
	{
		appendStringInfo(&infosendmsg, " %s -D %s", cmdmode, cndnPath);
		appendStringInfo(&infosendmsg, " -Z %s -o -i -w -c -l %s/logfile", zmode, cndnPath);
	}

	PG_TRY();
	{
	if (-1 != cmdtype_s)
		execRes= mgr_ma_send_cmd(cmdtype_s, infosendmsg.data, hostOid, &(getAgentCmdRst->description));
	else
	{
		if (AGT_CMD_CLEAN_NODE == cmdtype)
		{
			StringInfoData	cleanSlinksendmsg;
			initStringInfo(&cleanSlinksendmsg);
			/* parameters are used to delete the tablespace folder */
			appendStringInfo(&cleanSlinksendmsg, "%s/pg_tblspc|%s_%s", cndnPath, TABLESPACE_VERSION_DIRECTORY, cndnname);
			/* clean tablespace dir*/
			execRes= mgr_ma_send_cmd_get_original_result(cmdtype, cleanSlinksendmsg.data, hostOid, &(getAgentCmdRst->description), AGENT_RESULT_MESSAGE);
		}
		execRes= mgr_ma_send_cmd(cmdtype, infosendmsg.data, hostOid, &(getAgentCmdRst->description));
	}
	}PG_CATCH();
	{
		if (AGT_CMD_DN_FAILOVER == cmdtype || AGT_CMD_GTMCOOR_SLAVE_FAILOVER == cmdtype)
		{
			mgr_unlock_cluster_involve_gtm_coord(&pg_conn);
			pg_conn = NULL;
		}

		PG_RE_THROW();
	}PG_END_TRY();

	getAgentCmdRst->ret = execRes;
	if (!execRes && pg_conn)
	{
		/* check the slave node running status, if it had promote to master, skip this error */
		if (AGT_CMD_DN_FAILOVER == cmdtype || AGT_CMD_GTMCOOR_SLAVE_FAILOVER == cmdtype)
		{
			if (mgr_check_node_recovery_finish(nodetype, hostOid, cndnport, hostaddress))
				execRes = true;
			else
			{
				mgr_unlock_cluster_involve_gtm_coord(&pg_conn);
				goto end;
			}
		}
	}

	/*when init, 1. update gtm system table's column to set initial is true 2. refresh postgresql.conf*/
	if (execRes && AGT_CMD_GTMCOOR_SLAVE_INIT == cmdtype)
	{
		/*update node system table's column to set initial is true when cmd is init*/
		mgr_node->nodeinited = true;
		heap_inplace_update(noderel, aimtuple);
		/*refresh postgresql.conf of this node*/
		resetStringInfo(&(getAgentCmdRst->description));
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_pgsqlconf(tupleOid, nodetype, cndnport, &infosendmsg);
		mgr_add_parm(cndnname, nodetype, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, cndnPath, &infosendmsg, hostOid, getAgentCmdRst);
		/*refresh pg_hba.conf*/
		resetStringInfo(&(getAgentCmdRst->description));
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_hbaconf((mgr_node->nodemasternameoid == 0)? HeapTupleGetOid(aimtuple):mgr_node->nodemasternameoid
			, CNDN_TYPE_GTM_COOR_MASTER, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF, cndnPath, &infosendmsg, hostOid, getAgentCmdRst);
		/*refresh recovry.conf*/
		resetStringInfo(&(getAgentCmdRst->description));
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_recoveryconf(nodetype, cndnname, nodemasternameoid, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_RECOVERCONF, cndnPath, &infosendmsg, hostOid, getAgentCmdRst);
	}

	/*update node system table's column to set initial is true when cmd is init*/
	if ((AGT_CMD_CNDN_CNDN_INIT == cmdtype ||  AGT_CMD_GTMCOOR_INIT == cmdtype) && execRes)
	{
		mgr_node->nodeinited = true;
		heap_inplace_update(noderel, aimtuple);
		/*refresh postgresql.conf of this node*/
		resetStringInfo(&(getAgentCmdRst->description));
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_pgsqlconf(tupleOid, nodetype, cndnport, &infosendmsg);
		mgr_add_parm(cndnname, nodetype, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, cndnPath, &infosendmsg, hostOid, getAgentCmdRst);
		/*refresh pg_hba.conf*/
		resetStringInfo(&(getAgentCmdRst->description));
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_hbaconf((mgr_node->nodemasternameoid == 0)? HeapTupleGetOid(aimtuple):mgr_node->nodemasternameoid
			, nodetype, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF, cndnPath, &infosendmsg, hostOid, getAgentCmdRst);
	}

	/*failover execute success*/
	PG_TRY();
	{
		if(AGT_CMD_DN_FAILOVER == cmdtype && execRes)
		{
			namestrcpy(&cndnnamedata, cndnname);
			mgr_after_datanode_failover_handle(nodemasternameoid, &cndnnamedata, cndnport, hostaddress
				, noderel, getAgentCmdRst, aimtuple, cndnPath, nodetype, &pg_conn, cnoid);
		}

		/*gtm failover*/
		if (AGT_CMD_GTMCOOR_SLAVE_FAILOVER == cmdtype && execRes)
		{
			mgr_after_gtm_failover_handle(hostaddress, cndnport, noderel, getAgentCmdRst, aimtuple
				, cndnPath, &pg_conn, cnoid);
		}
	}PG_CATCH();
	{
		if (pg_conn)
			mgr_unlock_cluster_involve_gtm_coord(&pg_conn);
		PG_RE_THROW();
	}PG_END_TRY();

end:
	pfree(infosendmsg.data);
	pfree(strinfoport.data);
	pfree(hostaddress);

	/* Refresh slave node information about read-only query in pgxc_node table */ 
	if (!readonlySqlSlaveInfoRefreshComplete
		&& (AGT_CMD_CN_START == cmdtype || AGT_CMD_CN_START_BACKEND == cmdtype 
		|| AGT_CMD_CN_RESTART == cmdtype || AGT_CMD_GTMCOOR_SLAVE_FAILOVER == cmdtype 
		|| AGT_CMD_DN_FAILOVER == cmdtype || AGT_CMD_GTMCOOR_START_MASTER_BACKEND == cmdtype
		|| AGT_CMD_GTMCOOR_START_MASTER == cmdtype || AGT_CMD_GTMCOOR_START_SLAVE == cmdtype))
		mgr_update_cn_pgxcnode_readonlysql_slave(NULL, NULL, NULL);

	if (mgr_node->nodetype == CNDN_TYPE_DATANODE_MASTER && (AGT_CMD_DN_START == cmdtype || AGT_CMD_DN_START_BACKEND == cmdtype || AGT_CMD_DN_RESTART == cmdtype))
		mgr_update_da_master_pgxcnode_slave_info(cndnname);
}

/*
* stop gtm master
*/
Datum mgr_stop_gtmcoor_master(PG_FUNCTION_ARGS)
{
	List *nodenamelist = NIL;
	char *stop_mode;
	char *nodename;

	mgr_check_job_in_updateparam("monitor_handle_gtm");
	stop_mode = PG_GETARG_CSTRING(0);
	if (PG_ARGISNULL(1))
	{
		nodenamelist = mgr_get_nodetype_namelist(CNDN_TYPE_GTM_COOR_MASTER);
		updateDoctorStatusOfMgrNodes(nodenamelist, CNDN_TYPE_GTM_COOR_MASTER, false, CURE_STATUS_NORMAL);
		return mgr_typenode_cmd_run_backend_result(CNDN_TYPE_GTM_COOR_MASTER, AGT_CMD_CN_STOP_BACKEND, nodenamelist, stop_mode, fcinfo);
	}
	else
	{
		nodename = PG_GETARG_CSTRING(1);
		nodenamelist = lappend(nodenamelist, nodename);
		updateDoctorStatusOfMgrNodes(nodenamelist, CNDN_TYPE_GTM_COOR_MASTER, false, CURE_STATUS_NORMAL);
		return mgr_runmode_cndn(CNDN_TYPE_GTM_COOR_MASTER, AGT_CMD_CN_STOP_BACKEND, nodenamelist, stop_mode, fcinfo);
	}
}

/*
* stop gtm master ,used for DirectFunctionCall1
*/
Datum mgr_stop_one_gtm_master(PG_FUNCTION_ARGS)
{
	GetAgentCmdRst getAgentCmdRst;
	HeapTuple tup_result;
	HeapTuple aimtuple = NULL;
	ScanKeyData key[0];
	Relation rel_node;
	HeapScanDesc rel_scan;

	ScanKeyInit(&key[0],
		Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(CNDN_TYPE_GTM_COOR_MASTER));
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	rel_scan = heap_beginscan_catalog(rel_node, 1, key);
	while((aimtuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		break;
	}
	if (!HeapTupleIsValid(aimtuple))
	{
		ereport(ERROR, (errmsg("gtm master does not exist")));
	}
	/*get execute cmd result from agent*/
	initStringInfo(&(getAgentCmdRst.description));
	mgr_runmode_cndn_get_result(AGT_CMD_GTMCOOR_STOP_MASTER, &getAgentCmdRst, rel_node, aimtuple, SHUTDOWN_I);
	tup_result = build_common_command_tuple(
		&(getAgentCmdRst.nodename)
		, getAgentCmdRst.ret
		, getAgentCmdRst.description.data);
	heap_endscan(rel_scan);
	heap_close(rel_node, RowExclusiveLock);
	pfree(getAgentCmdRst.description.data);

	return HeapTupleGetDatum(tup_result);
}

/*
* stop gtm slave
*/
Datum mgr_stop_gtmcoor_slave(PG_FUNCTION_ARGS)
{
	List *nodenamelist = NIL;
	char *stop_mode;
	char *nodename;

	stop_mode = PG_GETARG_CSTRING(0);
	if (PG_ARGISNULL(1))
	{
		nodenamelist = mgr_get_nodetype_namelist(CNDN_TYPE_GTM_COOR_SLAVE);
		updateDoctorStatusOfMgrNodes(nodenamelist, CNDN_TYPE_GTM_COOR_SLAVE, false, CURE_STATUS_NORMAL);
		return mgr_typenode_cmd_run_backend_result(CNDN_TYPE_GTM_COOR_SLAVE, AGT_CMD_GTMCOOR_STOP_SLAVE_BACKEND, nodenamelist, stop_mode, fcinfo);
	}
	else
	{
		nodename = PG_GETARG_CSTRING(1);
		nodenamelist = lappend(nodenamelist, nodename);
		updateDoctorStatusOfMgrNodes(nodenamelist, CNDN_TYPE_GTM_COOR_SLAVE, false, CURE_STATUS_NORMAL);
		return mgr_runmode_cndn(CNDN_TYPE_GTM_COOR_SLAVE, AGT_CMD_GTMCOOR_STOP_SLAVE, nodenamelist, stop_mode, fcinfo);
	}
}

/*
* stop coordinator master cn1,cn2...
* stop coordinator master all
*/
Datum mgr_stop_cn_master(PG_FUNCTION_ARGS)
{
	List *nodenamelist = NIL;
	char *stop_mode;

	mgr_check_job_in_updateparam("monitor_handle_coordinator");

	stop_mode = PG_GETARG_CSTRING(0);
	if (PG_ARGISNULL(1))
	{
		nodenamelist = mgr_get_nodetype_namelist(CNDN_TYPE_COORDINATOR_MASTER);
		updateDoctorStatusOfMgrNodes(nodenamelist, CNDN_TYPE_COORDINATOR_MASTER, false, CURE_STATUS_NORMAL);
		return mgr_typenode_cmd_run_backend_result(CNDN_TYPE_COORDINATOR_MASTER, AGT_CMD_CN_STOP_BACKEND, nodenamelist, stop_mode, fcinfo);
	}
	else
	{
		nodenamelist = get_fcinfo_namelist("", 1, fcinfo);
		updateDoctorStatusOfMgrNodes(nodenamelist, CNDN_TYPE_COORDINATOR_MASTER, false, CURE_STATUS_NORMAL);
		return mgr_runmode_cndn(CNDN_TYPE_COORDINATOR_MASTER, AGT_CMD_CN_STOP, nodenamelist, stop_mode, fcinfo);
	}
}

/*
* stop coordinator slave cn1,cn2...
* stop coordinator slave all
*/
Datum mgr_stop_cn_slave(PG_FUNCTION_ARGS)
{
	List *nodenamelist = NIL;
	char *stop_mode;

	mgr_check_job_in_updateparam("monitor_handle_coordinator");

	stop_mode = PG_GETARG_CSTRING(0);
	if (PG_ARGISNULL(1))
	{
		nodenamelist = mgr_get_nodetype_namelist(CNDN_TYPE_COORDINATOR_SLAVE);
		updateDoctorStatusOfMgrNodes(nodenamelist, CNDN_TYPE_COORDINATOR_SLAVE, false, CURE_STATUS_NORMAL);
		return mgr_typenode_cmd_run_backend_result(CNDN_TYPE_COORDINATOR_SLAVE, AGT_CMD_CN_STOP_BACKEND, nodenamelist, stop_mode, fcinfo);
	}
	else
	{
		nodenamelist = get_fcinfo_namelist("", 1, fcinfo);
		updateDoctorStatusOfMgrNodes(nodenamelist, CNDN_TYPE_COORDINATOR_SLAVE, false, CURE_STATUS_NORMAL);
		return mgr_runmode_cndn(CNDN_TYPE_COORDINATOR_SLAVE, AGT_CMD_CN_STOP, nodenamelist, stop_mode, fcinfo);
	}
}

/*
* stop datanode master cn1,cn2...
* stop datanode master all
*/
Datum mgr_stop_dn_master(PG_FUNCTION_ARGS)
{
	List *nodenamelist = NIL;
	char *stop_mode;

	mgr_check_job_in_updateparam("monitor_handle_datanode");
	stop_mode = PG_GETARG_CSTRING(0);
	if (PG_ARGISNULL(1))
	{
		nodenamelist = mgr_get_nodetype_namelist(CNDN_TYPE_DATANODE_MASTER);
		updateDoctorStatusOfMgrNodes(nodenamelist, CNDN_TYPE_DATANODE_MASTER, false, CURE_STATUS_NORMAL);
		return mgr_typenode_cmd_run_backend_result(CNDN_TYPE_DATANODE_MASTER, AGT_CMD_DN_STOP_BACKEND, nodenamelist, stop_mode, fcinfo);
	}
	else
	{
		nodenamelist = get_fcinfo_namelist("", 1, fcinfo);
		updateDoctorStatusOfMgrNodes(nodenamelist, CNDN_TYPE_DATANODE_MASTER, false, CURE_STATUS_NORMAL);
		return mgr_runmode_cndn(CNDN_TYPE_DATANODE_MASTER, AGT_CMD_DN_STOP, nodenamelist, stop_mode, fcinfo);
	}
}

/*
* stop datanode master dn1
*/
Datum mgr_stop_one_dn_master(PG_FUNCTION_ARGS)
{
	GetAgentCmdRst getAgentCmdRst;
	HeapTuple tup_result
			,aimtuple;
	char *nodename;
	InitNodeInfo *info;

	info = palloc(sizeof(*info));
	nodename = PG_GETARG_CSTRING(0);
	info->rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	aimtuple = mgr_get_tuple_node_from_name_type(info->rel_node, nodename);
	if (!HeapTupleIsValid(aimtuple))
	{
		heap_close(info->rel_node, RowExclusiveLock);
		pfree(info);
		ereport(ERROR, (errmsg("datanode master \"%s\" does not exist", nodename)));
	}
	/*get execute cmd result from agent*/
	initStringInfo(&(getAgentCmdRst.description));
	mgr_runmode_cndn_get_result(AGT_CMD_DN_STOP, &getAgentCmdRst, info->rel_node, aimtuple, SHUTDOWN_I);
	tup_result = build_common_command_tuple(
		&(getAgentCmdRst.nodename)
		, getAgentCmdRst.ret
		, getAgentCmdRst.description.data);
	heap_freetuple(aimtuple);
	heap_close(info->rel_node, RowExclusiveLock);
	pfree(getAgentCmdRst.description.data);
	pfree(info);
	return HeapTupleGetDatum(tup_result);
}

/*
* stop datanode slave dn1,dn2...
* stop datanode slave all
*/
Datum mgr_stop_dn_slave(PG_FUNCTION_ARGS)
{
	List *nodenamelist = NIL;
	char *stop_mode;

	stop_mode = PG_GETARG_CSTRING(0);
	if (PG_ARGISNULL(1))
	{
		nodenamelist = mgr_get_nodetype_namelist(CNDN_TYPE_DATANODE_SLAVE);
		updateDoctorStatusOfMgrNodes(nodenamelist, CNDN_TYPE_DATANODE_SLAVE, false, CURE_STATUS_NORMAL);
		return mgr_typenode_cmd_run_backend_result(CNDN_TYPE_DATANODE_SLAVE, AGT_CMD_DN_STOP_BACKEND, nodenamelist, stop_mode, fcinfo);
	}
	else
	{
		nodenamelist = get_fcinfo_namelist("", 1, fcinfo);
		updateDoctorStatusOfMgrNodes(nodenamelist, CNDN_TYPE_DATANODE_SLAVE, false, CURE_STATUS_NORMAL);
		return mgr_runmode_cndn(CNDN_TYPE_DATANODE_SLAVE, AGT_CMD_DN_STOP, nodenamelist, stop_mode, fcinfo);
	}
}

/*
* get the result of start/stop/init gtm master/slave, coordinator master/slave, datanode master/slave
*/
Datum mgr_runmode_cndn(char nodetype, char cmdtype, List* nodenamelist , char *shutdown_mode, PG_FUNCTION_ARGS)
{
	GetAgentCmdRst getAgentCmdRst;
	HeapTuple tup_result;
	HeapTuple aimtuple =NULL;
	FuncCallContext *funcctx;
	ListCell **lcp;
	List *new_list;
	InitNodeInfo *info;
	char *nodestrname;
	NameData nodenamedata;
	Form_mgr_node mgr_node;

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		/* allocate memory for user context */
		info = palloc(sizeof(*info));
		info->lcp = (ListCell **) palloc(sizeof(ListCell *));
		new_list = list_copy(nodenamelist);
		*(info->lcp) = list_head(new_list);
		list_free(nodenamelist);

		info->rel_node = heap_open(NodeRelationId, RowExclusiveLock);
		/* save info */
		funcctx->user_fctx = info;
		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();
	info = funcctx->user_fctx;
	Assert(info);
	lcp = info->lcp;
	if (*lcp == NULL)
	{
		heap_close(info->rel_node, RowExclusiveLock);
		pfree(info->lcp);
		pfree(info);
		SRF_RETURN_DONE(funcctx);
	}
	nodestrname = (char *) lfirst(*lcp);
	*lcp = lnext(*lcp);
	if(namestrcpy(&nodenamedata, nodestrname) != 0)
	{
		heap_close(info->rel_node, RowExclusiveLock);
		pfree(info->lcp);
		pfree(info);
		ereport(ERROR, (errmsg("namestrcpy %s fail", nodestrname)));
	}
	aimtuple = mgr_get_tuple_node_from_name_type(info->rel_node, NameStr(nodenamedata));
	if (!HeapTupleIsValid(aimtuple))
	{
		heap_close(info->rel_node, RowExclusiveLock);
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
			errmsg("%s \"%s\" does not exist", mgr_nodetype_str(nodetype), nodestrname)));
	}
	/*check the type is given type*/
	mgr_node = (Form_mgr_node)GETSTRUCT(aimtuple);
	Assert(mgr_node);
	if(nodetype != mgr_node->nodetype)
	{
		heap_freetuple(aimtuple);
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
			errmsg("the type of  %s is not right, use \"list node\" to check", nodestrname)));
	}
	/*get execute cmd result from agent*/
	initStringInfo(&(getAgentCmdRst.description));
	mgr_runmode_cndn_get_result(cmdtype, &getAgentCmdRst, info->rel_node, aimtuple, shutdown_mode);
	tup_result = build_common_command_tuple(
		&(getAgentCmdRst.nodename)
		, getAgentCmdRst.ret
		, getAgentCmdRst.description.data);
	heap_freetuple(aimtuple);
	pfree(getAgentCmdRst.description.data);
	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}

Datum mgr_boottime_all(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	InitNodeInfo *info;
	HeapTuple tup;
	HeapTuple tup_result;
	Form_mgr_node mgr_node;
	StringInfoData resultstrdata;
	StringInfoData starttime;
	NameData host;
	NameData recoveryStatus;
	char nodetype;
	char *host_addr = NULL;
	char *nodetypeStr = NULL;
	int ret = PQPING_REJECT;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		info = palloc(sizeof(*info));
		info->rel_node = heap_open(NodeRelationId, AccessShareLock);
		info->rel_scan = heap_beginscan_catalog(info->rel_node, 0, NULL);
		info->lcp =NULL;
		/* save info */
		funcctx->user_fctx = info;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	Assert(funcctx);
	info = funcctx->user_fctx;
	Assert(info);

	tup = heap_getnext(info->rel_scan, ForwardScanDirection);
	if(tup == NULL)
	{
		/* end of row */
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, AccessShareLock);
		pfree(info);
		SRF_RETURN_DONE(funcctx);
	}

	mgr_node = (Form_mgr_node)GETSTRUCT(tup);
	Assert(mgr_node);

	nodetype = mgr_node->nodetype;
	host_addr = get_hostaddress_from_hostoid(mgr_node->nodehost);
	initStringInfo(&resultstrdata);
	initStringInfo(&starttime);
	ret = mgr_get_monitor_node_result(nodetype, mgr_node->nodehost, mgr_node->nodeport
	, &resultstrdata, &starttime, &recoveryStatus);

	/* check the node recovery status */
	nodetypeStr = mgr_nodetype_str(nodetype);
	if (nodetype == CNDN_TYPE_COORDINATOR_MASTER || nodetype == CNDN_TYPE_DATANODE_MASTER
		|| nodetype == CNDN_TYPE_GTM_COOR_MASTER)
	{
		if (strcmp(recoveryStatus.data, enum_recovery_status_tab[RECOVERY_NOT_IN].name) != 0)
			ereport(WARNING, (errmsg("%s %s recovery status is %s", nodetypeStr
				, NameStr(mgr_node->nodename), recoveryStatus.data)));
	}
	else
	{
		if (strcmp(recoveryStatus.data, enum_recovery_status_tab[RECOVERY_IN].name) != 0)
			ereport(WARNING, (errmsg("%s %s recovery status is %s", nodetypeStr
				, NameStr(mgr_node->nodename), recoveryStatus.data)));
	}

	pfree(nodetypeStr);
	namestrcpy(&host, host_addr);
	tup_result = build_common_command_tuple_for_boottime(
				&(mgr_node->nodename)
				,nodetype
				,ret == PQPING_OK ? true:false
				,resultstrdata.data
				,starttime.data
				,&host);
	pfree(resultstrdata.data);
	pfree(starttime.data);
	pfree(host_addr);
	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}


/*
* MONITOR ALL
*/
Datum mgr_monitor_all(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	InitNodeInfo *info;
	HeapTuple tup;
	HeapTuple tup_result;
	Form_mgr_node mgr_node;
	StringInfoData resultstrdata;
	StringInfoData starttime;
	NameData host;
	NameData recoveryStatus;
	char nodetype;
	char *host_addr = NULL;
	char *nodetypeStr = NULL;
	int ret = PQPING_REJECT;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		info = palloc(sizeof(*info));
		info->rel_node = heap_open(NodeRelationId, AccessShareLock);
		info->rel_scan = heap_beginscan_catalog(info->rel_node, 0, NULL);
		info->lcp =NULL;
		/* save info */
		funcctx->user_fctx = info;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	Assert(funcctx);
	info = funcctx->user_fctx;
	Assert(info);

	tup = heap_getnext(info->rel_scan, ForwardScanDirection);
	if(tup == NULL)
	{
		/* end of row */
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, AccessShareLock);
		pfree(info);
		SRF_RETURN_DONE(funcctx);
	}

	mgr_node = (Form_mgr_node)GETSTRUCT(tup);
	Assert(mgr_node);

	nodetype = mgr_node->nodetype;
	host_addr = get_hostaddress_from_hostoid(mgr_node->nodehost);
	initStringInfo(&resultstrdata);
	initStringInfo(&starttime);
	ret = mgr_get_monitor_node_result(nodetype, mgr_node->nodehost, mgr_node->nodeport
	, &resultstrdata, &starttime, &recoveryStatus);

	/* check the node recovery status */
	nodetypeStr = mgr_nodetype_str(nodetype);
	if (nodetype == CNDN_TYPE_COORDINATOR_MASTER || nodetype == CNDN_TYPE_DATANODE_MASTER
		|| nodetype == CNDN_TYPE_GTM_COOR_MASTER)
	{
		if (strcmp(recoveryStatus.data, enum_recovery_status_tab[RECOVERY_NOT_IN].name) != 0)
			ereport(WARNING, (errmsg("%s %s recovery status is %s", nodetypeStr
				, NameStr(mgr_node->nodename), recoveryStatus.data)));
	}
	else
	{
		if (strcmp(recoveryStatus.data, enum_recovery_status_tab[RECOVERY_IN].name) != 0)
			ereport(WARNING, (errmsg("%s %s recovery status is %s", nodetypeStr
				, NameStr(mgr_node->nodename), recoveryStatus.data)));
	}

	pfree(nodetypeStr);
	namestrcpy(&host, host_addr);
	tup_result = build_common_command_tuple_for_monitor(
				&(mgr_node->nodename)
				,nodetype
				,ret == PQPING_OK ? true:false
				,resultstrdata.data
				,starttime.data
				,&host
				,mgr_node->nodeport
				,&recoveryStatus);
	pfree(resultstrdata.data);
	pfree(starttime.data);
	pfree(host_addr);
	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}

/*
 * MONITOR DATANODE ALL;
 */
Datum mgr_monitor_datanode_all(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	InitNodeInfo *info;
	HeapTuple tup;
	HeapTuple tup_result;
	Form_mgr_node mgr_node;
	StringInfoData resultstrdata;
	StringInfoData starttime;
	NameData host;
	NameData recoveryStatus;
	char nodetype;
	char *host_addr = NULL;
	char *nodetypeStr = NULL;
	int ret = PQPING_REJECT;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		info = palloc(sizeof(*info));
		info->rel_node = heap_open(NodeRelationId, AccessShareLock);
		info->rel_scan = heap_beginscan_catalog(info->rel_node, 0, NULL);
		info->lcp =NULL;

		/* save info */
		funcctx->user_fctx = info;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	Assert(funcctx);
	info = funcctx->user_fctx;
	Assert(info);

	while ((tup = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tup);
		Assert(mgr_node);

		/* if node type is datanode master ,datanode slave. */
		if (mgr_node->nodetype == CNDN_TYPE_DATANODE_MASTER || mgr_node->nodetype == CNDN_TYPE_DATANODE_SLAVE)
		{
			initStringInfo(&resultstrdata);
			initStringInfo(&starttime);
			nodetype = mgr_node->nodetype;
			host_addr = get_hostaddress_from_hostoid(mgr_node->nodehost);
			ret = mgr_get_monitor_node_result(nodetype, mgr_node->nodehost, mgr_node->nodeport
			, &resultstrdata, &starttime, &recoveryStatus);

			/* check the node recovery status */
			nodetypeStr = mgr_nodetype_str(nodetype);
			if (nodetype == CNDN_TYPE_DATANODE_MASTER)
			{
				if (strcmp(recoveryStatus.data, enum_recovery_status_tab[RECOVERY_NOT_IN].name) != 0)
					ereport(WARNING, (errmsg("%s %s recovery status is %s", nodetypeStr
						, NameStr(mgr_node->nodename), recoveryStatus.data)));
			}
			else
			{
				if (strcmp(recoveryStatus.data, enum_recovery_status_tab[RECOVERY_IN].name) != 0)
					ereport(WARNING, (errmsg("%s %s recovery status is %s", nodetypeStr
						, NameStr(mgr_node->nodename), recoveryStatus.data)));
			}

			pfree(nodetypeStr);
			namestrcpy(&host, host_addr);
			tup_result = build_common_command_tuple_for_monitor(
						&(mgr_node->nodename)
						,nodetype
						,ret == 0 ? true:false
						,resultstrdata.data
						,starttime.data
						,&host
						,mgr_node->nodeport
						,&recoveryStatus);

			pfree(host_addr);
			pfree(resultstrdata.data);
			pfree(starttime.data);
			SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
		}
		else
			continue;
	}

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
	SRF_RETURN_DONE(funcctx);
}

/*
 * MONITOR GTMCOORD ALL;
 */
Datum mgr_monitor_gtmcoor_all(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	InitNodeInfo *info;
	HeapTuple tup;
	HeapTuple tup_result;
	Form_mgr_node mgr_node;
	StringInfoData resultstrdata;
	StringInfoData starttime;
	StringInfoData strdata;
	NameData host;
	NameData recoveryStatus;
	char nodetype;
	char *host_addr = NULL;
	char *nodetypeStr = NULL;
	int ret = PQPING_REJECT;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		info = palloc(sizeof(*info));
		info->rel_node = heap_open(NodeRelationId, AccessShareLock);
		info->rel_scan = heap_beginscan_catalog(info->rel_node, 0, NULL);
		info->lcp =NULL;

		/* save info */
		funcctx->user_fctx = info;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	Assert(funcctx);
	info = funcctx->user_fctx;
	Assert(info);

	while ((tup = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tup);
		Assert(mgr_node);

		/* if node type is gtmcoord master ,gtmcoord slave. */
		if (mgr_node->nodetype == CNDN_TYPE_GTM_COOR_MASTER || mgr_node->nodetype == CNDN_TYPE_GTM_COOR_SLAVE)
		{
			initStringInfo(&resultstrdata);
			initStringInfo(&starttime);
			initStringInfo(&strdata);
			nodetype = mgr_node->nodetype;
			host_addr = get_hostaddress_from_hostoid(mgr_node->nodehost);
			ret = mgr_get_monitor_node_result(nodetype, mgr_node->nodehost, mgr_node->nodeport
			, &resultstrdata, &starttime, &recoveryStatus);

			/* check the node recovery status */
			nodetypeStr = mgr_nodetype_str(nodetype);
			if (nodetype == CNDN_TYPE_GTM_COOR_MASTER)
			{
				if (strcmp(recoveryStatus.data, enum_recovery_status_tab[RECOVERY_NOT_IN].name) != 0)
				{
					ereport(WARNING, (errmsg("%s %s recovery status is %s", nodetypeStr
						, NameStr(mgr_node->nodename), recoveryStatus.data)));
				}
			}
			else
			{
				if (strcmp(recoveryStatus.data, enum_recovery_status_tab[RECOVERY_IN].name) != 0)
					ereport(WARNING, (errmsg("%s %s recovery status is %s", nodetypeStr
						, NameStr(mgr_node->nodename), recoveryStatus.data)));
			}

			pfree(nodetypeStr);
			namestrcpy(&host, host_addr);
			tup_result = build_common_command_tuple_for_monitor(
						&(mgr_node->nodename)
						,nodetype
						,ret == 0 ? true:false
						,resultstrdata.data
						,starttime.data
						,&host
						,mgr_node->nodeport
						,&recoveryStatus);

			pfree(host_addr);
			pfree(resultstrdata.data);
			pfree(starttime.data);
			SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
		}
		else
			continue;
	}

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
	SRF_RETURN_DONE(funcctx);
}

/*
 * BOOTTIME GTMCOORD ALL;
 */
Datum mgr_boottime_gtmcoor_all(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	InitNodeInfo *info;
	HeapTuple tup;
	HeapTuple tup_result;
	Form_mgr_node mgr_node;
	StringInfoData resultstrdata;
	StringInfoData starttime;
	StringInfoData strdata;
	NameData host;
	NameData recoveryStatus;
	char nodetype;
	char *host_addr = NULL;
	char *nodetypeStr = NULL;
	int ret = PQPING_REJECT;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		info = palloc(sizeof(*info));
		info->rel_node = heap_open(NodeRelationId, AccessShareLock);
		info->rel_scan = heap_beginscan_catalog(info->rel_node, 0, NULL);
		info->lcp =NULL;

		/* save info */
		funcctx->user_fctx = info;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	Assert(funcctx);
	info = funcctx->user_fctx;
	Assert(info);

	while ((tup = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tup);
		Assert(mgr_node);

		/* if node type is gtmcoord master ,gtmcoord slave. */
		if (mgr_node->nodetype == CNDN_TYPE_GTM_COOR_MASTER || mgr_node->nodetype == CNDN_TYPE_GTM_COOR_SLAVE)
		{
			initStringInfo(&resultstrdata);
			initStringInfo(&starttime);
			initStringInfo(&strdata);
			nodetype = mgr_node->nodetype;
			host_addr = get_hostaddress_from_hostoid(mgr_node->nodehost);
			ret = mgr_get_monitor_node_result(nodetype, mgr_node->nodehost, mgr_node->nodeport
			, &resultstrdata, &starttime, &recoveryStatus);

			/* check the node recovery status */
			nodetypeStr = mgr_nodetype_str(nodetype);
			if (nodetype == CNDN_TYPE_GTM_COOR_MASTER)
			{
				if (strcmp(recoveryStatus.data, enum_recovery_status_tab[RECOVERY_NOT_IN].name) != 0)
				{
					ereport(WARNING, (errmsg("%s %s recovery status is %s", nodetypeStr
						, NameStr(mgr_node->nodename), recoveryStatus.data)));
				}
			}
			else
			{
				if (strcmp(recoveryStatus.data, enum_recovery_status_tab[RECOVERY_IN].name) != 0)
					ereport(WARNING, (errmsg("%s %s recovery status is %s", nodetypeStr
						, NameStr(mgr_node->nodename), recoveryStatus.data)));
			}

			pfree(nodetypeStr);
			namestrcpy(&host, host_addr);
			tup_result = build_common_command_tuple_for_boottime(
						&(mgr_node->nodename)
						,nodetype
						,ret == 0 ? true:false
						,resultstrdata.data
						,starttime.data
						,&host);

			pfree(host_addr);
			pfree(resultstrdata.data);
			pfree(starttime.data);
			SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
		}
		else
			continue;
	}

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
	SRF_RETURN_DONE(funcctx);
}

/*
 * BOOTTIME DATANODE ALL;
 */
Datum mgr_boottime_datanode_all(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	InitNodeInfo *info;
	HeapTuple tup;
	HeapTuple tup_result;
	Form_mgr_node mgr_node;
	StringInfoData resultstrdata;
	StringInfoData starttime;
	NameData host;
	NameData recoveryStatus;
	char nodetype;
	char *host_addr = NULL;
	char *nodetypeStr = NULL;
	int ret = PQPING_REJECT;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		info = palloc(sizeof(*info));
		info->rel_node = heap_open(NodeRelationId, AccessShareLock);
		info->rel_scan = heap_beginscan_catalog(info->rel_node, 0, NULL);
		info->lcp =NULL;

		/* save info */
		funcctx->user_fctx = info;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	Assert(funcctx);
	info = funcctx->user_fctx;
	Assert(info);

	while ((tup = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tup);
		Assert(mgr_node);

		/* if node type is datanode master ,datanode slave. */
		if (mgr_node->nodetype == CNDN_TYPE_DATANODE_MASTER || mgr_node->nodetype == CNDN_TYPE_DATANODE_SLAVE)
		{
			initStringInfo(&resultstrdata);
			initStringInfo(&starttime);
			nodetype = mgr_node->nodetype;
			host_addr = get_hostaddress_from_hostoid(mgr_node->nodehost);
			ret = mgr_get_monitor_node_result(nodetype, mgr_node->nodehost, mgr_node->nodeport
			, &resultstrdata, &starttime, &recoveryStatus);

			/* check the node recovery status */
			nodetypeStr = mgr_nodetype_str(nodetype);
			if (nodetype == CNDN_TYPE_DATANODE_MASTER)
			{
				if (strcmp(recoveryStatus.data, enum_recovery_status_tab[RECOVERY_NOT_IN].name) != 0)
					ereport(WARNING, (errmsg("%s %s recovery status is %s", nodetypeStr
						, NameStr(mgr_node->nodename), recoveryStatus.data)));
			}
			else
			{
				if (strcmp(recoveryStatus.data, enum_recovery_status_tab[RECOVERY_IN].name) != 0)
					ereport(WARNING, (errmsg("%s %s recovery status is %s", nodetypeStr
						, NameStr(mgr_node->nodename), recoveryStatus.data)));
			}

			pfree(nodetypeStr);
			namestrcpy(&host, host_addr);
			tup_result = build_common_command_tuple_for_boottime(
						&(mgr_node->nodename)
						,nodetype
						,ret == 0 ? true:false
						,resultstrdata.data
						,starttime.data
						,&host);

			pfree(host_addr);
			pfree(resultstrdata.data);
			pfree(starttime.data);
			SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
		}
		else
			continue;
	}

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
	SRF_RETURN_DONE(funcctx);
}

/*
 * BOOTTIME COORDINATOR ALL;
 */
Datum mgr_boottime_coordinator_all(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	InitNodeInfo *info;
	HeapTuple tup;
	HeapTuple tup_result;
	Form_mgr_node mgr_node;
	StringInfoData resultstrdata;
	StringInfoData starttime;
	NameData host;
	NameData recoveryStatus;
	char nodetype;
	char *host_addr = NULL;
	char *nodetypeStr = NULL;
	int ret = PQPING_REJECT;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		info = palloc(sizeof(*info));
		info->rel_node = heap_open(NodeRelationId, AccessShareLock);
		info->rel_scan = heap_beginscan_catalog(info->rel_node, 0, NULL);
		info->lcp =NULL;

		/* save info */
		funcctx->user_fctx = info;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	Assert(funcctx);
	info = funcctx->user_fctx;
	Assert(info);

	while ((tup = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tup);
		Assert(mgr_node);

		/* if node type is coordinator master ,coordinator slave. */
		if (mgr_node->nodetype == CNDN_TYPE_COORDINATOR_MASTER || mgr_node->nodetype == CNDN_TYPE_COORDINATOR_SLAVE)
		{
			initStringInfo(&resultstrdata);
			initStringInfo(&starttime);
			nodetype = mgr_node->nodetype;
			host_addr = get_hostaddress_from_hostoid(mgr_node->nodehost);
			ret = mgr_get_monitor_node_result(nodetype, mgr_node->nodehost, mgr_node->nodeport
			, &resultstrdata, &starttime, &recoveryStatus);

			/* check the node recovery status */
			nodetypeStr = mgr_nodetype_str(nodetype);
			if (nodetype == CNDN_TYPE_DATANODE_MASTER)
			{
				if (strcmp(recoveryStatus.data, enum_recovery_status_tab[RECOVERY_NOT_IN].name) != 0)
					ereport(WARNING, (errmsg("%s %s recovery status is %s", nodetypeStr
						, NameStr(mgr_node->nodename), recoveryStatus.data)));
			}
			else
			{
				if (strcmp(recoveryStatus.data, enum_recovery_status_tab[RECOVERY_IN].name) != 0)
					ereport(WARNING, (errmsg("%s %s recovery status is %s", nodetypeStr
						, NameStr(mgr_node->nodename), recoveryStatus.data)));
			}

			pfree(nodetypeStr);
			namestrcpy(&host, host_addr);
			tup_result = build_common_command_tuple_for_boottime(
						&(mgr_node->nodename)
						,nodetype
						,ret == 0 ? true:false
						,resultstrdata.data
						,starttime.data
						,&host);

			pfree(host_addr);
			pfree(resultstrdata.data);
			pfree(starttime.data);
			SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
		}
		else
			continue;
	}

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
	SRF_RETURN_DONE(funcctx);
}

/*
 * boottime nodetype(datanode master/slave|coordinator|gtm master/slave) namelist ...
 */
Datum mgr_boottime_nodetype_namelist(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	InitNodeInfo *info;
	ListCell **lcp;
	List *nodenamelist=NIL;
	HeapTuple tup, tup_result;
	Form_mgr_node mgr_node;
	StringInfoData resultstrdata;
	StringInfoData starttime;
	NameData host;
	NameData recoveryStatus;
	char *host_addr = NULL;
	char *nodename = NULL;
	char *nodetypeStr = NULL;
	int ret = PQPING_REJECT;
	char nodetype;

	nodetype = PG_GETARG_CHAR(0);

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		nodenamelist = get_fcinfo_namelist("", 1, fcinfo);

		info = palloc(sizeof(*info));
		info->lcp = (ListCell **) palloc(sizeof(ListCell *));
		*(info->lcp) = list_head(nodenamelist);
		info->rel_node = heap_open(NodeRelationId, RowExclusiveLock);

		/* save info */
		funcctx->user_fctx = info;

		MemoryContextSwitchTo(oldcontext);
	}


	funcctx = SRF_PERCALL_SETUP();
	Assert(funcctx);
	info = funcctx->user_fctx;
	Assert(info);

	lcp = info->lcp;
	if (*lcp == NULL)
	{
		heap_close(info->rel_node, RowExclusiveLock);
		pfree(info);
		SRF_RETURN_DONE(funcctx);
	}

	nodename = (char *)lfirst(*lcp);
	*lcp = lnext(*lcp);
	tup = mgr_get_tuple_node_from_name_type(info->rel_node, nodename);
	if (!HeapTupleIsValid(tup))
	{
		switch (nodetype)
		{
			case CNDN_TYPE_COORDINATOR_MASTER:
				ereport(ERROR, (errmsg("coordinator master \"%s\" does not exist", nodename)));
				break;
			case CNDN_TYPE_COORDINATOR_SLAVE:
				ereport(ERROR, (errmsg("coordinator slave \"%s\" does not exist", nodename)));
				break;
			case CNDN_TYPE_DATANODE_MASTER:
				ereport(ERROR, (errmsg("datanode master \"%s\" does not exist", nodename)));
				break;
			case CNDN_TYPE_DATANODE_SLAVE:
				ereport(ERROR, (errmsg("datanode slave \"%s\" does not exist", nodename)));
				break;
			case CNDN_TYPE_GTM_COOR_MASTER:
				ereport(ERROR, (errmsg("gtm master \"%s\" does not exist", nodename)));
				break;
			case CNDN_TYPE_GTM_COOR_SLAVE:
				ereport(ERROR, (errmsg("gtm slave \"%s\" does not exist", nodename)));
				break;
			default:
				ereport(ERROR, (errmsg("node type \"%c\" does not exist", nodetype)));
				break;
		}
	}

	mgr_node = (Form_mgr_node)GETSTRUCT(tup);
	Assert(mgr_node);

	if (nodetype != mgr_node->nodetype)
		ereport(ERROR, (errmsg("node type is not right: %s", nodename)));

	host_addr = get_hostaddress_from_hostoid(mgr_node->nodehost);
	initStringInfo(&resultstrdata);
	initStringInfo(&starttime);
	ret = mgr_get_monitor_node_result(mgr_node->nodetype, mgr_node->nodehost, mgr_node->nodeport
			, &resultstrdata, &starttime, &recoveryStatus);

	/* check the node recovery status */
	nodetypeStr = mgr_nodetype_str(nodetype);
	if (nodetype == CNDN_TYPE_COORDINATOR_MASTER || nodetype == CNDN_TYPE_DATANODE_MASTER
		|| nodetype == CNDN_TYPE_GTM_COOR_MASTER)
	{
		if (strcmp(recoveryStatus.data, enum_recovery_status_tab[RECOVERY_NOT_IN].name) != 0)
			ereport(WARNING, (errmsg("%s %s recovery status is %s", nodetypeStr
				, NameStr(mgr_node->nodename), recoveryStatus.data)));
	}
	else
	{
		if (strcmp(recoveryStatus.data, enum_recovery_status_tab[RECOVERY_IN].name) != 0)
			ereport(WARNING, (errmsg("%s %s recovery status is %s", nodetypeStr
				, NameStr(mgr_node->nodename), recoveryStatus.data)));
	}

	pfree(nodetypeStr);
	namestrcpy(&host, host_addr);
	tup_result = build_common_command_tuple_for_boottime(
				&(mgr_node->nodename)
				,nodetype
				,ret == 0 ? true:false
				,resultstrdata.data
				,starttime.data
				,&host);

	pfree(host_addr);
	pfree(resultstrdata.data);
	pfree(starttime.data);
	heap_freetuple(tup);
	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}


/*
 * monitor nodetype(datanode master/slave|coordinator|gtm master/slave) namelist ...
 */
Datum mgr_monitor_nodetype_namelist(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	InitNodeInfo *info;
	ListCell **lcp;
	List *nodenamelist=NIL;
	HeapTuple tup, tup_result;
	Form_mgr_node mgr_node;
	StringInfoData resultstrdata;
	StringInfoData starttime;
	NameData host;
	NameData recoveryStatus;
	char *host_addr = NULL;
	char *nodename = NULL;
	char *nodetypeStr = NULL;
	int ret = PQPING_REJECT;
	char nodetype;

	nodetype = PG_GETARG_CHAR(0);

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		nodenamelist = get_fcinfo_namelist("", 1, fcinfo);

		info = palloc(sizeof(*info));
		info->lcp = (ListCell **) palloc(sizeof(ListCell *));
		*(info->lcp) = list_head(nodenamelist);
		info->rel_node = heap_open(NodeRelationId, RowExclusiveLock);

		/* save info */
		funcctx->user_fctx = info;

		MemoryContextSwitchTo(oldcontext);
	}


	funcctx = SRF_PERCALL_SETUP();
	Assert(funcctx);
	info = funcctx->user_fctx;
	Assert(info);

	lcp = info->lcp;
	if (*lcp == NULL)
	{
		heap_close(info->rel_node, RowExclusiveLock);
		pfree(info);
		SRF_RETURN_DONE(funcctx);
	}

	nodename = (char *)lfirst(*lcp);
	*lcp = lnext(*lcp);
	tup = mgr_get_tuple_node_from_name_type(info->rel_node, nodename);
	if (!HeapTupleIsValid(tup))
	{
		switch (nodetype)
		{
			case CNDN_TYPE_COORDINATOR_MASTER:
				ereport(ERROR, (errmsg("coordinator master \"%s\" does not exist", nodename)));
				break;
			case CNDN_TYPE_COORDINATOR_SLAVE:
				ereport(ERROR, (errmsg("coordinator slave \"%s\" does not exist", nodename)));
				break;
			case CNDN_TYPE_DATANODE_MASTER:
				ereport(ERROR, (errmsg("datanode master \"%s\" does not exist", nodename)));
				break;
			case CNDN_TYPE_DATANODE_SLAVE:
				ereport(ERROR, (errmsg("datanode slave \"%s\" does not exist", nodename)));
				break;
			case CNDN_TYPE_GTM_COOR_MASTER:
				ereport(ERROR, (errmsg("gtm master \"%s\" does not exist", nodename)));
				break;
			case CNDN_TYPE_GTM_COOR_SLAVE:
				ereport(ERROR, (errmsg("gtm slave \"%s\" does not exist", nodename)));
				break;
			default:
				ereport(ERROR, (errmsg("node type \"%c\" does not exist", nodetype)));
				break;
		}
	}

	mgr_node = (Form_mgr_node)GETSTRUCT(tup);
	Assert(mgr_node);

	if (nodetype != mgr_node->nodetype)
		ereport(ERROR, (errmsg("node type is not right: %s", nodename)));

	host_addr = get_hostaddress_from_hostoid(mgr_node->nodehost);
	initStringInfo(&resultstrdata);
	initStringInfo(&starttime);
	ret = mgr_get_monitor_node_result(mgr_node->nodetype, mgr_node->nodehost, mgr_node->nodeport
			, &resultstrdata, &starttime, &recoveryStatus);

	/* check the node recovery status */
	nodetypeStr = mgr_nodetype_str(nodetype);
	if (nodetype == CNDN_TYPE_COORDINATOR_MASTER || nodetype == CNDN_TYPE_DATANODE_MASTER
		|| nodetype == CNDN_TYPE_GTM_COOR_MASTER)
	{
		if (strcmp(recoveryStatus.data, enum_recovery_status_tab[RECOVERY_NOT_IN].name) != 0)
			ereport(WARNING, (errmsg("%s %s recovery status is %s", nodetypeStr
				, NameStr(mgr_node->nodename), recoveryStatus.data)));
	}
	else
	{
		if (strcmp(recoveryStatus.data, enum_recovery_status_tab[RECOVERY_IN].name) != 0)
			ereport(WARNING, (errmsg("%s %s recovery status is %s", nodetypeStr
				, NameStr(mgr_node->nodename), recoveryStatus.data)));
	}

	pfree(nodetypeStr);
	namestrcpy(&host, host_addr);
	tup_result = build_common_command_tuple_for_monitor(
				&(mgr_node->nodename)
				,nodetype
				,ret == 0 ? true:false
				,resultstrdata.data
				,starttime.data
				,&host
				,mgr_node->nodeport
				,&recoveryStatus);

	pfree(host_addr);
	pfree(resultstrdata.data);
	pfree(starttime.data);
	heap_freetuple(tup);
	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}

/*
 * boottime nodetype(DATANODE MASTER/SLAVE |COORDINATOR |GTMCOORD MASTER|SLAVE) ALL
 */
Datum mgr_boottime_nodetype_all(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	InitNodeInfo *info;
	HeapTuple tup;
	HeapTuple tup_result;
	Form_mgr_node mgr_node;
	ScanKeyData  key[1];
	StringInfoData resultstrdata;
	StringInfoData starttime;
	NameData host;
	NameData recoveryStatus;
	char *host_addr = NULL;
	char *nodetypeStr = NULL;
	int ret = PQPING_REJECT;
	char nodetype;

	nodetype = PG_GETARG_CHAR(0);

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		info = palloc(sizeof(*info));
		info->rel_node = heap_open(NodeRelationId, AccessShareLock);

		ScanKeyInit(&key[0]
					,Anum_mgr_node_nodetype
					,BTEqualStrategyNumber
					,F_CHAREQ
					,CharGetDatum(nodetype));
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

	tup = heap_getnext(info->rel_scan, ForwardScanDirection);
	if(tup == NULL)
	{
		/* end of row */
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, AccessShareLock);
		pfree(info);
		SRF_RETURN_DONE(funcctx);
	}

	mgr_node = (Form_mgr_node)GETSTRUCT(tup);
	Assert(mgr_node);

	initStringInfo(&resultstrdata);
	initStringInfo(&starttime);
	host_addr = get_hostaddress_from_hostoid(mgr_node->nodehost);
	ret = mgr_get_monitor_node_result(mgr_node->nodetype, mgr_node->nodehost, mgr_node->nodeport
			, &resultstrdata, &starttime, &recoveryStatus);

	/* check the node recovery status */
	if (nodetype == CNDN_TYPE_COORDINATOR_MASTER || nodetype == CNDN_TYPE_DATANODE_MASTER
		|| nodetype == CNDN_TYPE_GTM_COOR_MASTER)
	{
		if (strcmp(recoveryStatus.data, enum_recovery_status_tab[RECOVERY_NOT_IN].name) != 0)
		{
			nodetypeStr = mgr_nodetype_str(nodetype);
			ereport(WARNING, (errmsg("%s %s is in recovery status", nodetypeStr, NameStr(mgr_node->nodename))));
			pfree(nodetypeStr);
		}
	}
	else
	{
		if (strcmp(recoveryStatus.data, enum_recovery_status_tab[RECOVERY_IN].name) != 0)
		{
			nodetypeStr = mgr_nodetype_str(nodetype);
			ereport(WARNING, (errmsg("%s %s is not in recovery status", nodetypeStr, NameStr(mgr_node->nodename))));
			pfree(nodetypeStr);
		}
	}

	namestrcpy(&host, host_addr);
	tup_result = build_common_command_tuple_for_boottime(
				&(mgr_node->nodename)
				,nodetype
				,ret == 0 ? true:false
				,resultstrdata.data
				,starttime.data
				,&host);

	pfree(host_addr);
	pfree(resultstrdata.data);
	pfree(starttime.data);
	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}


/*
 * MONITOR nodetype(DATANODE MASTER/SLAVE |COORDINATOR |GTMCOORD MASTER|SLAVE) ALL
 */
Datum mgr_monitor_nodetype_all(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	InitNodeInfo *info;
	HeapTuple tup;
	HeapTuple tup_result;
	Form_mgr_node mgr_node;
	ScanKeyData  key[1];
	StringInfoData resultstrdata;
	StringInfoData starttime;
	NameData host;
	NameData recoveryStatus;
	char *host_addr = NULL;
	char *nodetypeStr = NULL;
	int ret = PQPING_REJECT;
	char nodetype;

	nodetype = PG_GETARG_CHAR(0);

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		info = palloc(sizeof(*info));
		info->rel_node = heap_open(NodeRelationId, AccessShareLock);

		ScanKeyInit(&key[0]
					,Anum_mgr_node_nodetype
					,BTEqualStrategyNumber
					,F_CHAREQ
					,CharGetDatum(nodetype));
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

	tup = heap_getnext(info->rel_scan, ForwardScanDirection);
	if(tup == NULL)
	{
		/* end of row */
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, AccessShareLock);
		pfree(info);
		SRF_RETURN_DONE(funcctx);
	}

	mgr_node = (Form_mgr_node)GETSTRUCT(tup);
	Assert(mgr_node);

	initStringInfo(&resultstrdata);
	initStringInfo(&starttime);
	host_addr = get_hostaddress_from_hostoid(mgr_node->nodehost);
	ret = mgr_get_monitor_node_result(mgr_node->nodetype, mgr_node->nodehost, mgr_node->nodeport
			, &resultstrdata, &starttime, &recoveryStatus);

	/* check the node recovery status */
	if (nodetype == CNDN_TYPE_COORDINATOR_MASTER || nodetype == CNDN_TYPE_DATANODE_MASTER
		|| nodetype == CNDN_TYPE_GTM_COOR_MASTER)
	{
		if (strcmp(recoveryStatus.data, enum_recovery_status_tab[RECOVERY_NOT_IN].name) != 0)
		{
			nodetypeStr = mgr_nodetype_str(nodetype);
			ereport(WARNING, (errmsg("%s %s is in recovery status", nodetypeStr, NameStr(mgr_node->nodename))));
			pfree(nodetypeStr);
		}
	}
	else
	{
		if (strcmp(recoveryStatus.data, enum_recovery_status_tab[RECOVERY_IN].name) != 0)
		{
			nodetypeStr = mgr_nodetype_str(nodetype);
			ereport(WARNING, (errmsg("%s %s is not in recovery status", nodetypeStr, NameStr(mgr_node->nodename))));
			pfree(nodetypeStr);
		}
	}

	namestrcpy(&host, host_addr);
	tup_result = build_common_command_tuple_for_monitor(
				&(mgr_node->nodename)
				,nodetype
				,ret == 0 ? true:false
				,resultstrdata.data
				,starttime.data
				,&host
				,mgr_node->nodeport
				,&recoveryStatus);

	pfree(host_addr);
	pfree(resultstrdata.data);
	pfree(starttime.data);
	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}

HeapTuple build_common_command_tuple_for_boottime(const Name name, char type, bool status, const char *description,
						const char *starttime ,const Name hostaddr)
{
	Datum datums[6];
	bool nulls[6];
	TupleDesc desc;
	NameData typestr;
	AssertArg(name && description);
	desc = get_common_command_tuple_desc_for_boottime();

	AssertArg(desc && desc->natts == 6
		&& TupleDescAttr(desc, 0)->atttypid == NAMEOID
		&& TupleDescAttr(desc, 1)->atttypid == NAMEOID
		&& TupleDescAttr(desc, 2)->atttypid == BOOLOID
		&& TupleDescAttr(desc, 3)->atttypid == TEXTOID
		&& TupleDescAttr(desc, 4)->atttypid == NAMEOID
		&& TupleDescAttr(desc, 5)->atttypid == TEXTOID);

	switch(type)
	{
		case CNDN_TYPE_GTM_COOR_MASTER:
			namestrcpy(&typestr, "gtmcoord master");
			break;
		case CNDN_TYPE_GTM_COOR_SLAVE:
			namestrcpy(&typestr, "gtmcoord slave");
			break;
		case CNDN_TYPE_COORDINATOR_MASTER:
			namestrcpy(&typestr, "coordinator master");
			break;
		case CNDN_TYPE_COORDINATOR_SLAVE:
			namestrcpy(&typestr, "coordinator slave");
			break;
		case CNDN_TYPE_DATANODE_MASTER:
			namestrcpy(&typestr, "datanode master");
			break;
		case CNDN_TYPE_DATANODE_SLAVE:
			namestrcpy(&typestr, "datanode slave");
			break;
		default:
			namestrcpy(&typestr, "unknown type");
			break;
	}

	datums[0] = NameGetDatum(name);
	datums[1] = NameGetDatum(&typestr);
	datums[2] = BoolGetDatum(status);
	datums[3] = CStringGetTextDatum(description);
	datums[4] = NameGetDatum(hostaddr);
	datums[5] = CStringGetTextDatum(starttime);
	nulls[0] = nulls[1] = nulls[2] = nulls[3] = nulls[4] = nulls[5] = false;
	return heap_form_tuple(desc, datums, nulls);
}

HeapTuple build_common_command_tuple_for_monitor(const Name name, char type, bool status, const char *description,
						const char *starttime ,const Name hostaddr, const int port, const Name recoveryStatus)
{
	Datum datums[8];
	bool nulls[8];
	TupleDesc desc;
	NameData typestr;
	AssertArg(name && description);
	desc = get_common_command_tuple_desc_for_monitor();

	AssertArg(desc && desc->natts == 8
		&& TupleDescAttr(desc, 0)->atttypid == NAMEOID
		&& TupleDescAttr(desc, 1)->atttypid == NAMEOID
		&& TupleDescAttr(desc, 2)->atttypid == BOOLOID
		&& TupleDescAttr(desc, 3)->atttypid == TEXTOID
		&& TupleDescAttr(desc, 4)->atttypid == NAMEOID
		&& TupleDescAttr(desc, 5)->atttypid == INT4OID
		&& TupleDescAttr(desc, 6)->atttypid == NAMEOID
		&& TupleDescAttr(desc, 7)->atttypid == TEXTOID);

	switch(type)
	{
		case CNDN_TYPE_GTM_COOR_MASTER:
			namestrcpy(&typestr, "gtmcoord master");
			break;
		case CNDN_TYPE_GTM_COOR_SLAVE:
			namestrcpy(&typestr, "gtmcoord slave");
			break;
		case CNDN_TYPE_COORDINATOR_MASTER:
			namestrcpy(&typestr, "coordinator master");
			break;
		case CNDN_TYPE_COORDINATOR_SLAVE:
			namestrcpy(&typestr, "coordinator slave");
			break;
		case CNDN_TYPE_DATANODE_MASTER:
			namestrcpy(&typestr, "datanode master");
			break;
		case CNDN_TYPE_DATANODE_SLAVE:
			namestrcpy(&typestr, "datanode slave");
			break;
		default:
			namestrcpy(&typestr, "unknown type");
			break;
	}


	datums[0] = NameGetDatum(name);
	datums[1] = NameGetDatum(&typestr);
	datums[2] = BoolGetDatum(status);
	datums[3] = CStringGetTextDatum(description);
	datums[4] = NameGetDatum(hostaddr);
	datums[5] = Int32GetDatum(port);
	datums[6] = NameGetDatum(recoveryStatus);
	datums[7] = CStringGetTextDatum(starttime);
	nulls[0] = nulls[1] = nulls[2] = nulls[3] = nulls[4] = nulls[5] = nulls[6] = nulls[7] = false;
	return heap_form_tuple(desc, datums, nulls);
}

static TupleDesc get_common_command_tuple_desc_for_boottime(void)
{
    if(common_boottime_tuple_desc == NULL)
    {
        MemoryContext volatile old_context = MemoryContextSwitchTo(TopMemoryContext);
        TupleDesc volatile desc = NULL;
        PG_TRY();
        {
            desc = CreateTemplateTupleDesc(6, false);
            TupleDescInitEntry(desc, (AttrNumber) 1, "nodename",
                        NAMEOID, -1, 0);
            TupleDescInitEntry(desc, (AttrNumber) 2, "nodetype",
                        NAMEOID, -1, 0);
            TupleDescInitEntry(desc, (AttrNumber) 3, "status",
                        BOOLOID, -1, 0);
            TupleDescInitEntry(desc, (AttrNumber) 4, "description",
                        TEXTOID, -1, 0);
            TupleDescInitEntry(desc, (AttrNumber) 5, "host",
                        NAMEOID, -1, 0);
            TupleDescInitEntry(desc, (AttrNumber) 6, "boot time",
                        TEXTOID, -1, 0);
            common_boottime_tuple_desc = BlessTupleDesc(desc);
        }PG_CATCH();
        {
            if(desc)
                FreeTupleDesc(desc);
            PG_RE_THROW();
        }PG_END_TRY();
        (void)MemoryContextSwitchTo(old_context);
    }
    Assert(common_boottime_tuple_desc);
    return common_boottime_tuple_desc;
}

static TupleDesc get_common_command_tuple_desc_for_monitor(void)
{
	if(common_command_tuple_desc == NULL)
	{
		MemoryContext volatile old_context = MemoryContextSwitchTo(TopMemoryContext);
		TupleDesc volatile desc = NULL;
		PG_TRY();
		{
			desc = CreateTemplateTupleDesc(8, false);
			TupleDescInitEntry(desc, (AttrNumber) 1, "nodename",
						NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 2, "nodetype",
						NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 3, "status",
						BOOLOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 4, "description",
						TEXTOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 5, "host",
						NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 6, "port",
						INT4OID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 7, "recovery",
						NAMEOID, -1, 0);
			TupleDescInitEntry(desc, (AttrNumber) 8, "boot time",
						TEXTOID, -1, 0);
			common_command_tuple_desc = BlessTupleDesc(desc);
		}PG_CATCH();
		{
			if(desc)
				FreeTupleDesc(desc);
			PG_RE_THROW();
		}PG_END_TRY();
		(void)MemoryContextSwitchTo(old_context);
	}
	Assert(common_command_tuple_desc);
	return common_command_tuple_desc;
}

/*
 * APPEND DATANODE MASTER nodename
 */
Datum mgr_append_dnmaster(PG_FUNCTION_ARGS)
{
	AppendNodeInfo appendnodeinfo;
	AppendNodeInfo agtm_m_nodeinfo;
	bool agtm_m_is_exist = false;
	bool agtm_m_is_running = false;  /* agtm master status */
	bool is_add_hba;
	bool result = true;
	StringInfoData send_hba_msg;
	StringInfoData infosendmsg;
	NameData nodename;
	NameData gtmMasterNameData;
	int max_locktry = 600;
	const int max_pingtry = 60;
	int ret = 0;
	char *temp_file;
	char *gtmMasterName;
	char nodeport_buf[10];
	char coordport_buf[10];
	Oid dnhostoid = InvalidOid;
	int32 dnport = 0;
	PGconn * pg_conn = NULL;
	HeapTuple tup_result;
	GetAgentCmdRst getAgentCmdRst;

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot assign TransactionIds during recovery")));

	memset(&appendnodeinfo, 0, sizeof(AppendNodeInfo));
	memset(&agtm_m_nodeinfo, 0, sizeof(AppendNodeInfo)); 

	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsg);
	appendnodeinfo.nodename = PG_GETARG_CSTRING(0);
	Assert(appendnodeinfo.nodename);

	namestrcpy(&nodename, appendnodeinfo.nodename);

	PG_TRY();
	{
		/* get node info for append datanode master */
		mgr_check_appendnodeinfo(CNDN_TYPE_DATANODE_MASTER, appendnodeinfo.nodename);
		mgr_get_appendnodeinfo(CNDN_TYPE_DATANODE_MASTER, nodename.data, &appendnodeinfo);
		gtmMasterName = mgr_get_agtm_name();
		namestrcpy(&gtmMasterNameData, gtmMasterName);
		pfree(gtmMasterName);
		get_nodeinfo(gtmMasterNameData.data, CNDN_TYPE_GTM_COOR_MASTER, &agtm_m_is_exist, &agtm_m_is_running, &agtm_m_nodeinfo);

		mgr_make_sure_all_running(CNDN_TYPE_COORDINATOR_MASTER);

		if (agtm_m_is_exist)
		{
			if (agtm_m_is_running)
			{
				/* append "host all postgres  ip/32" for agtm master pg_hba.conf and reload it. */
				 mgr_add_hbaconf(CNDN_TYPE_GTM_COOR_MASTER, appendnodeinfo.nodeusername, appendnodeinfo.nodeaddr);
			}
			else
			{ ereport(ERROR, (errmsg("gtmcoord is not running")));}
		}
		else
		{ ereport(ERROR, (errmsg("gtmcoord is not initialized")));}

		/* for gtm slave */
		mgr_add_hbaconf(CNDN_TYPE_GTM_COOR_SLAVE, appendnodeinfo.nodeusername, appendnodeinfo.nodeaddr);

		/* step 1: init workdir */
		mgr_check_dir_exist_and_priv(appendnodeinfo.nodehost, appendnodeinfo.nodepath);
		mgr_append_init_cndnmaster(&appendnodeinfo);

		/* step 2: update datanode master's postgresql.conf. */
		resetStringInfo(&infosendmsg);
		mgr_get_other_parm(CNDN_TYPE_DATANODE_MASTER, &infosendmsg);
		mgr_add_parm(appendnodeinfo.nodename, CNDN_TYPE_DATANODE_MASTER, &infosendmsg);
		mgr_append_pgconf_paras_str_int("port", appendnodeinfo.nodeport, &infosendmsg);
		mgr_get_gtm_host_snapsender_gxidsender_port(&infosendmsg);

		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF,
								appendnodeinfo.nodepath,
								&infosendmsg,
								appendnodeinfo.nodehost,
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/* step 3: update datanode master's pg_hba.conf */
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_hbaconf(appendnodeinfo.nodemasteroid, CNDN_TYPE_DATANODE_MASTER, &infosendmsg);
		mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", "all", appendnodeinfo.nodeaddr, 32, "trust", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
								appendnodeinfo.nodepath,
								&infosendmsg,
								appendnodeinfo.nodehost,
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));
		/*param table*/
		resetStringInfo(&(getAgentCmdRst.description));
		resetStringInfo(&infosendmsg);
		mgr_add_parm(appendnodeinfo.nodename, CNDN_TYPE_DATANODE_MASTER, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, appendnodeinfo.nodepath, &infosendmsg, appendnodeinfo.nodehost, &getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/* step 4: block all the DDL lock */
		initStringInfo(&send_hba_msg);
		is_add_hba = AddHbaIsValid(&agtm_m_nodeinfo, &send_hba_msg);
		sprintf(coordport_buf, "%d", agtm_m_nodeinfo.nodeport);

		pg_conn = ExpPQsetdbLogin(agtm_m_nodeinfo.nodeaddr
								,coordport_buf
								,NULL, NULL
								,appendnodeinfo.nodeusername
								,NULL);

		if (pg_conn == NULL || PQstatus((PGconn*)pg_conn) != CONNECTION_OK)
		{
			ereport(ERROR,
				(errmsg("Fail to connect to gtmcoord %s", PQerrorMessage((PGconn*)pg_conn)),
				errhint("coordinator info(host=%s port=%d dbname=%s user=%s)",
					agtm_m_nodeinfo.nodeaddr, agtm_m_nodeinfo.nodeport, DEFAULT_DB, appendnodeinfo.nodeusername)));
		}

		ret = mgr_pqexec_boolsql_try_maxnum(&pg_conn, "set FORCE_PARALLEL_MODE = off; select pgxc_lock_for_backup();"
			, max_locktry, CMD_SELECT);
		if (ret < 0)
		{
			ereport(ERROR,
				(errmsg("sql error:  %s\n", PQerrorMessage((PGconn*)pg_conn)),
				errhint("try %d times execute command failed: set FORCE_PARALLEL_MODE = off; select pgxc_lock_for_backup()."
					, max_locktry)));
		}

		/* step 5: dumpall catalog message */
		mgr_get_active_hostoid_and_port(CNDN_TYPE_DATANODE_MASTER, &dnhostoid, &dnport, &appendnodeinfo, true);

		temp_file = get_temp_file_name();
		mgr_pg_dumpall(dnhostoid, dnport, appendnodeinfo.nodehost, temp_file);

		/* step 6: start the datanode master with restoremode mode, and input all catalog message */
		mgr_start_node_with_restoremode(appendnodeinfo.nodepath, appendnodeinfo.nodehost, CNDN_TYPE_DATANODE_MASTER);
		mgr_pg_dumpall_input_node(appendnodeinfo.nodehost, appendnodeinfo.nodeport, temp_file);
		mgr_rm_dumpall_temp_file(appendnodeinfo.nodehost, temp_file);

		/* step 7: stop the datanode master with restoremode, and then start it with "datanode" mode */
		mgr_stop_node_with_restoremode(appendnodeinfo.nodepath, appendnodeinfo.nodehost);
		mgr_start_node(CNDN_TYPE_DATANODE_MASTER, appendnodeinfo.nodepath, appendnodeinfo.nodehost);

		/* step 8: create node on all the coordinator */
		mgr_create_node_on_all_coord(fcinfo, CNDN_TYPE_DATANODE_MASTER, appendnodeinfo.nodename, appendnodeinfo.nodehost, appendnodeinfo.nodeport);
		resetStringInfo(&(getAgentCmdRst.description));
		result = mgr_refresh_pgxc_node(PGXC_APPEND, CNDN_TYPE_DATANODE_MASTER, appendnodeinfo.nodename, &getAgentCmdRst);

		/* step 9: release the DDL lock */
		PQfinish(pg_conn);
		pg_conn = NULL;
		if (is_add_hba)
			RemoveHba(&agtm_m_nodeinfo, &send_hba_msg);
		pfree(send_hba_msg.data);

		/* step10: update node system table's column to set initial is true */
		mgr_set_inited_incluster(appendnodeinfo.nodename, CNDN_TYPE_DATANODE_MASTER, false, true);
	}PG_CATCH();
	{
		if(pg_conn)
		{
			PQfinish(pg_conn);
			pg_conn = NULL;
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
	pfree_AppendNodeInfo(agtm_m_nodeinfo);

	return HeapTupleGetDatum(tup_result);
}

/*
 * APPEND DATANODE SLAVE nodename
 */
Datum mgr_append_dnslave(PG_FUNCTION_ARGS)
{
	AppendNodeInfo appendnodeinfo;
	AppendNodeInfo parentnodeinfo;
	AppendNodeInfo agtm_m_nodeinfo;
	bool agtm_m_is_exist, agtm_m_is_running; /* agtm master status */
	bool dnmaster_is_running; /* datanode master status */
	bool result = true;
	bool bsyncnode = false;
	StringInfoData  infosendmsg;
	StringInfoData primary_conninfo_value;
	StringInfoData recorderr;
	StringInfoData infostrparam;
	StringInfoData infostrparamtmp;
	NameData nodename;
	NameData gtmMasterNameData;
	HeapTuple tup_result;
	GetAgentCmdRst getAgentCmdRst;
	const int max_pingtry = 60;
	char nodeport_buf[10];
	char *gtmMasterName;
	Oid mastertupleoid;
	Relation rel;
	int syncNum = 0;

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot assign TransactionIds during recovery")));

	memset(&appendnodeinfo, 0, sizeof(AppendNodeInfo));
	memset(&parentnodeinfo, 0, sizeof(AppendNodeInfo));
	memset(&agtm_m_nodeinfo, 0, sizeof(AppendNodeInfo));

	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsg);
	appendnodeinfo.nodename = PG_GETARG_CSTRING(0);
	Assert(appendnodeinfo.nodename);

	namestrcpy(&nodename, appendnodeinfo.nodename);

	PG_TRY();
	{
		/* get node info both slave and master node. */
		mgr_check_appendnodeinfo(CNDN_TYPE_DATANODE_SLAVE, appendnodeinfo.nodename);
		mgr_get_appendnodeinfo(CNDN_TYPE_DATANODE_SLAVE, nodename.data, &appendnodeinfo);
		rel = heap_open(NodeRelationId, AccessShareLock);
		if (strcmp(NameStr(appendnodeinfo.sync_state), sync_state_tab[SYNC_STATE_POTENTIAL].name) == 0
			&& (!mgr_check_syncstate_node_exist(rel, appendnodeinfo.nodemasteroid, SYNC_STATE_SYNC, appendnodeinfo.tupleoid, true)))
		{
			pfree(getAgentCmdRst.description.data);
			pfree(infosendmsg.data);
			heap_close(rel, AccessShareLock);
			ereport(ERROR, (errmsg("datanode master \"%s\" has no sync slave node, can not append this node as potential node", NameStr(nodename))));
		}
		heap_close(rel, AccessShareLock);
		mgr_get_parent_appendnodeinfo(appendnodeinfo.nodemasteroid, &parentnodeinfo);
		/* gtm master */
		gtmMasterName =  mgr_get_agtm_name();
		namestrcpy(&gtmMasterNameData, gtmMasterName);
		pfree(gtmMasterName);
		get_nodeinfo(gtmMasterNameData.data, CNDN_TYPE_GTM_COOR_MASTER, &agtm_m_is_exist, &agtm_m_is_running, &agtm_m_nodeinfo);
		mastertupleoid = appendnodeinfo.nodemasteroid;
		/* step 1: make sure datanode master, agtm master or agtm slave is running. */
		dnmaster_is_running = is_node_running(parentnodeinfo.nodeaddr, parentnodeinfo.nodeport, parentnodeinfo.nodeusername, parentnodeinfo.nodetype);
		if (!dnmaster_is_running)
			ereport(ERROR, (errmsg("datanode master \"%s\" is not running", parentnodeinfo.nodename)));

		if (agtm_m_is_exist)
		{
			if (agtm_m_is_running)
			{
				/* append "host all postgres  ip/32" for agtm master pg_hba.conf and reload it. */
				mgr_add_hbaconf(CNDN_TYPE_GTM_COOR_MASTER, appendnodeinfo.nodeusername, appendnodeinfo.nodeaddr);
			}
			else
				{	ereport(ERROR, (errmsg("gtm master is not running")));}
		}
		else
		{	ereport(ERROR, (errmsg("gtm master is not initialized")));}

		/* append "host all postgres ip/32" for agtm slave pg_hba.conf and reload it. */
		mgr_add_hbaconf_by_masteroid(agtm_m_nodeinfo.tupleoid, "all", appendnodeinfo.nodeusername, appendnodeinfo.nodeaddr);

		/* for datanode slave , which has the same datanode master */
		mgr_add_hbaconf_by_masteroid(mastertupleoid, "replication", appendnodeinfo.nodeusername, appendnodeinfo.nodeaddr);

		/* step 2: update datanode master's postgresql.conf. */
		// to do nothing now

		/* step 3: update datanode master's pg_hba.conf. */
		resetStringInfo(&infosendmsg);
		mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "replication", appendnodeinfo.nodeusername, appendnodeinfo.nodeaddr, 32, "trust", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
								parentnodeinfo.nodepath,
								&infosendmsg,
								parentnodeinfo.nodehost,
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/* step 4: reload datanode master. */
		mgr_reload_conf(parentnodeinfo.nodehost, parentnodeinfo.nodepath);

		/* step 5: basebackup for datanode master using pg_basebackup command. */
		mgr_check_dir_exist_and_priv(appendnodeinfo.nodehost, appendnodeinfo.nodepath);
		mgr_pgbasebackup(CNDN_TYPE_DATANODE_SLAVE, &appendnodeinfo, &parentnodeinfo);

		/* step 6: update datanode slave's postgresql.conf. */
		resetStringInfo(&infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("pgxc_node_name", appendnodeinfo.nodename, &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("archive_command", "", &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("log_directory", "pg_log", &infosendmsg);
		mgr_add_parm(appendnodeinfo.nodename, CNDN_TYPE_DATANODE_SLAVE, &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
		mgr_append_pgconf_paras_str_str("hot_standby", "on", &infosendmsg);
		mgr_append_pgconf_paras_str_int("port", appendnodeinfo.nodeport, &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("pgxc_node_name", appendnodeinfo.nodename, &infosendmsg);
		mgr_get_gtm_host_snapsender_gxidsender_port(&infosendmsg);

		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF,
								appendnodeinfo.nodepath,
								&infosendmsg,
								appendnodeinfo.nodehost,
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));
		/*param table*/
		resetStringInfo(&infosendmsg);
		resetStringInfo(&(getAgentCmdRst.description));
		mgr_add_parm(appendnodeinfo.nodename, CNDN_TYPE_DATANODE_SLAVE, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, appendnodeinfo.nodepath, &infosendmsg, appendnodeinfo.nodehost, &getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/* step 7: update datanode slave's recovery.conf. */
		resetStringInfo(&infosendmsg);
		initStringInfo(&primary_conninfo_value);
		appendStringInfo(&primary_conninfo_value, "host=%s port=%d user=%s application_name=%s",
						get_hostaddress_from_hostoid(parentnodeinfo.nodehost),
						parentnodeinfo.nodeport,
						get_hostuser_from_hostoid(parentnodeinfo.nodehost),
						nodename.data);

		mgr_append_pgconf_paras_str_quotastr("standby_mode", "on", &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("primary_conninfo", primary_conninfo_value.data, &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("recovery_target_timeline", "latest", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_RECOVERCONF,
								appendnodeinfo.nodepath,
								&infosendmsg,
								appendnodeinfo.nodehost,
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/* step 8: start datanode slave. */
		mgr_start_node(CNDN_TYPE_DATANODE_SLAVE, appendnodeinfo.nodepath, appendnodeinfo.nodehost);

		/* step 9: update datanode master's postgresql.conf.*/
		resetStringInfo(&infosendmsg);
		initStringInfo(&infostrparam);
		initStringInfo(&infostrparamtmp);
		if (strcmp(NameStr(appendnodeinfo.sync_state), sync_state_tab[SYNC_STATE_SYNC].name) == 0)
		{
			bsyncnode = true;
			appendStringInfo(&infostrparam, "%s", nodename.data);
		}
		syncNum = mgr_get_master_sync_string(mastertupleoid, true, InvalidOid, &infostrparam);

		if (bsyncnode)
			syncNum++;
		if (strcmp(NameStr(appendnodeinfo.sync_state), sync_state_tab[SYNC_STATE_POTENTIAL].name) == 0)
		{
			Assert(infostrparam.len != 0);
			appendStringInfo(&infostrparam, ",%s", nodename.data);
		}
		if (infostrparam.len == 0)
			mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
		else
		{
			appendStringInfo(&infostrparamtmp, "%d (%s)", syncNum, infostrparam.data);
			mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", infostrparamtmp.data, &infosendmsg);
		}
		pfree(infostrparam.data);
		pfree(infostrparamtmp.data);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF,
								parentnodeinfo.nodepath,
								&infosendmsg,
								parentnodeinfo.nodehost,
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/* step 10: reload datanode master's postgresql.conf. */
		mgr_reload_conf(parentnodeinfo.nodehost, parentnodeinfo.nodepath);

		/* step 11: update node system table's column to set initial is true */
		mgr_set_inited_incluster(appendnodeinfo.nodename, CNDN_TYPE_DATANODE_SLAVE, false, true);

	}PG_CATCH();
	{
		PG_RE_THROW();
	}PG_END_TRY();

	/*wait the node can accept connections*/
	sprintf(nodeport_buf, "%d", appendnodeinfo.nodeport);
	initStringInfo(&recorderr);
	if (!mgr_try_max_pingnode(appendnodeinfo.nodeaddr, nodeport_buf, appendnodeinfo.nodeusername, max_pingtry))
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
	pfree_AppendNodeInfo(appendnodeinfo);
	pfree_AppendNodeInfo(parentnodeinfo);
	pfree_AppendNodeInfo(agtm_m_nodeinfo);

	return HeapTupleGetDatum(tup_result);
}

/*
 * APPEND COORDINATOR MASTER nodename
 */
Datum mgr_append_coordmaster(PG_FUNCTION_ARGS)
{
	AppendNodeInfo appendnodeinfo;
	AppendNodeInfo agtm_m_nodeinfo;
	bool agtm_m_is_exist, agtm_m_is_running; /* agtm master status */
	bool is_add_hba; /*whether to add manager hba to node*/
	StringInfoData send_hba_msg;
	StringInfoData infosendmsg;
	GetAgentCmdRst getAgentCmdRst;
	char *temp_file;
	PGconn *pg_conn = NULL;
	HeapTuple tup_result;
	char coordport_buf[10];
	char nodeport_buf[10];
	char *gtmMasterName;
	NameData nodename;
	NameData gtmMasterNameData;
	bool result = true;
	int max_locktry = 600;
	const int max_pingtry = 60;
	int ret = 0;

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot assign TransactionIds during recovery")));

	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsg);
	memset(&appendnodeinfo, 0, sizeof(AppendNodeInfo));
	memset(&agtm_m_nodeinfo, 0, sizeof(AppendNodeInfo));

	/* get node info for append coordinator master */
	appendnodeinfo.nodename = PG_GETARG_CSTRING(0);
	Assert(appendnodeinfo.nodename);

	namestrcpy(&nodename, appendnodeinfo.nodename);
	PG_TRY();
	{
		/* get node info for append coordinator master */
		mgr_check_appendnodeinfo(CNDN_TYPE_COORDINATOR_MASTER, appendnodeinfo.nodename);
		mgr_get_appendnodeinfo(CNDN_TYPE_COORDINATOR_MASTER, nodename.data, &appendnodeinfo);
		/* gtm master */
		gtmMasterName = mgr_get_agtm_name();
		namestrcpy(&gtmMasterNameData, gtmMasterName);
		pfree(gtmMasterName);
		get_nodeinfo(gtmMasterNameData.data, CNDN_TYPE_GTM_COOR_MASTER, &agtm_m_is_exist, &agtm_m_is_running, &agtm_m_nodeinfo);

		mgr_make_sure_all_running(CNDN_TYPE_GTM_COOR_MASTER);
		mgr_make_sure_all_running(CNDN_TYPE_COORDINATOR_MASTER);
		
		if (agtm_m_is_exist)
		{
			if (agtm_m_is_running)
			{
				/* append "host all postgres  ip/32" for agtm master pg_hba.conf and reload it. */
				mgr_add_hbaconf(CNDN_TYPE_GTM_COOR_MASTER, "all", appendnodeinfo.nodeaddr);
			}
			else
				{	ereport(ERROR, (errmsg("gtmcoord master is not running")));}
		}
		else
		{	ereport(ERROR, (errmsg("gtm master is not initialized")));}

		/* append "host all postgres ip/32" for agtm slave pg_hba.conf and reload it. */
		mgr_add_hbaconf_by_masteroid(agtm_m_nodeinfo.tupleoid, "all", "all", appendnodeinfo.nodeaddr);

		/* step 1: init workdir */
		mgr_check_dir_exist_and_priv(appendnodeinfo.nodehost, appendnodeinfo.nodepath);
		mgr_append_init_cndnmaster(&appendnodeinfo);

		/* step 2: update coordinator master's postgresql.conf. */
		resetStringInfo(&infosendmsg);
		mgr_get_other_parm(CNDN_TYPE_COORDINATOR_MASTER, &infosendmsg);
		mgr_add_parm(appendnodeinfo.nodename, CNDN_TYPE_COORDINATOR_MASTER, &infosendmsg);
		mgr_append_pgconf_paras_str_int("port", appendnodeinfo.nodeport, &infosendmsg);
		mgr_get_gtm_host_snapsender_gxidsender_port(&infosendmsg);
		
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF,
								appendnodeinfo.nodepath,
								&infosendmsg,
								appendnodeinfo.nodehost,
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));
		/*param table*/
		resetStringInfo(&infosendmsg);
		resetStringInfo(&(getAgentCmdRst.description));
		mgr_add_parm(appendnodeinfo.nodename, CNDN_TYPE_COORDINATOR_MASTER, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, appendnodeinfo.nodepath, &infosendmsg, appendnodeinfo.nodehost, &getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/* step 3: update coordinator master's pg_hba.conf */
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_hbaconf(appendnodeinfo.nodemasteroid, CNDN_TYPE_COORDINATOR_MASTER, &infosendmsg);
		mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", "all", appendnodeinfo.nodeaddr, 32, "trust", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
								appendnodeinfo.nodepath,
								&infosendmsg,
								appendnodeinfo.nodehost,
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/* add host line for exist already */
		mgr_add_hbaconf_all(appendnodeinfo.nodeusername, appendnodeinfo.nodeaddr, true);

		/* step 4: block all the DDL lock */
		initStringInfo(&send_hba_msg);
		is_add_hba = AddHbaIsValid(&agtm_m_nodeinfo, &send_hba_msg);
		sprintf(coordport_buf, "%d", agtm_m_nodeinfo.nodeport);

		pg_conn = ExpPQsetdbLogin(agtm_m_nodeinfo.nodeaddr
								,coordport_buf
								,NULL, NULL
								,appendnodeinfo.nodeusername
								,NULL);

		if (pg_conn == NULL || PQstatus((PGconn*)pg_conn) != CONNECTION_OK)
		{
			ereport(ERROR,
				(errmsg("Fail to connect to gtmcoord %s", PQerrorMessage((PGconn*)pg_conn)),
				errhint("gtmcoord info(host=%s port=%d dbname=%s user=%s)",
					agtm_m_nodeinfo.nodeaddr, agtm_m_nodeinfo.nodeport, DEFAULT_DB, appendnodeinfo.nodeusername)));
		}

		ret = mgr_pqexec_boolsql_try_maxnum(&pg_conn, "set FORCE_PARALLEL_MODE = off; \
								select pgxc_lock_for_backup();", max_locktry, CMD_SELECT);
		if (ret < 0)
		{
		ereport(ERROR,
			(errmsg("sql error:  %s\n", PQerrorMessage((PGconn*)pg_conn)),
			errhint("try %d times execute command failed: set FORCE_PARALLEL_MODE = off; \
						select pgxc_lock_for_backup().", max_locktry)));
		}

		/* step 5: dumpall catalog message */
		temp_file = get_temp_file_name();
		mgr_pg_dumpall(agtm_m_nodeinfo.nodehost, agtm_m_nodeinfo.nodeport, appendnodeinfo.nodehost, temp_file);

		/* step 6: start the append coordiantor with restoremode mode, and input all catalog message */
		mgr_start_node_with_restoremode(appendnodeinfo.nodepath, appendnodeinfo.nodehost, CNDN_TYPE_COORDINATOR_MASTER);
		mgr_pg_dumpall_input_node(appendnodeinfo.nodehost, appendnodeinfo.nodeport, temp_file);
		mgr_rm_dumpall_temp_file(appendnodeinfo.nodehost, temp_file);

		/* step 7: stop the append coordiantor with restoremode, and then start it with "coordinator" mode */
		mgr_stop_node_with_restoremode(appendnodeinfo.nodepath, appendnodeinfo.nodehost);
		mgr_start_node(CNDN_TYPE_COORDINATOR_MASTER, appendnodeinfo.nodepath, appendnodeinfo.nodehost);

		/* step 8: create node on all the coordinator */
		mgr_create_node_on_all_coord(fcinfo, CNDN_TYPE_COORDINATOR_MASTER, appendnodeinfo.nodename, appendnodeinfo.nodehost, appendnodeinfo.nodeport);

		/* step 9: alter pgxc_node in append coordinator */
		resetStringInfo(&(getAgentCmdRst.description));
		result = mgr_refresh_pgxc_node(PGXC_APPEND, CNDN_TYPE_COORDINATOR_MASTER, appendnodeinfo.nodename, &getAgentCmdRst);
		/* step 10: release the DDL lock */
		PQfinish(pg_conn);
		pg_conn = NULL;
		if (is_add_hba)
			RemoveHba(&agtm_m_nodeinfo, &send_hba_msg);
		pfree(send_hba_msg.data);

		/* step 11: update node system table's column to set initial is true */
		mgr_set_inited_incluster(appendnodeinfo.nodename, CNDN_TYPE_COORDINATOR_MASTER, false, true);

		/*step 12: to update the data in the hba table to the specified pg_hba.conf file*/
		add_hba_table_to_file(appendnodeinfo.nodename);
	}PG_CATCH();
	{
		if(pg_conn)
		{
			PQfinish(pg_conn);
			pg_conn = NULL;
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
	pfree_AppendNodeInfo(agtm_m_nodeinfo);

	return HeapTupleGetDatum(tup_result);
}

Datum mgr_append_agtmslave(PG_FUNCTION_ARGS)
{
	AppendNodeInfo appendnodeinfo;
	AppendNodeInfo agtm_m_nodeinfo;
	bool agtm_m_is_exist;
	bool agtm_m_is_running; /* agtm master status */
	bool result = true;
	bool bsyncnode = false;
	StringInfoData infosendmsg;
	StringInfoData primary_conninfo_value;
	StringInfoData recorderr;
	StringInfoData infostrparam;
	StringInfoData infostrparamtmp;
	NameData nodename;
	NameData gtmMasterNameData;
	HeapTuple tup_result;
	HeapTuple gtmMasterTuple;
	GetAgentCmdRst getAgentCmdRst;
	char nodeport_buf[10];
	const int max_pingtry = 60;
	Oid mastertupleoid;
	int syncNum = 0;
	Relation rel;
	Form_mgr_node mgr_node;
	char mastertype;

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot assign TransactionIds during recovery")));

	memset(&appendnodeinfo, 0, sizeof(AppendNodeInfo));
	memset(&agtm_m_nodeinfo, 0, sizeof(AppendNodeInfo));

	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsg);
	appendnodeinfo.nodename = PG_GETARG_CSTRING(0);
	Assert(appendnodeinfo.nodename);

	namestrcpy(&nodename, appendnodeinfo.nodename);

	PG_TRY();
	{
		/* get agtm slave and agtm master node info. */
		mgr_check_appendnodeinfo(CNDN_TYPE_GTM_COOR_SLAVE, appendnodeinfo.nodename);
		mgr_get_appendnodeinfo(CNDN_TYPE_GTM_COOR_SLAVE, nodename.data, &appendnodeinfo);
		rel = heap_open(NodeRelationId, AccessShareLock);
		if (strcmp(NameStr(appendnodeinfo.sync_state), sync_state_tab[SYNC_STATE_POTENTIAL].name) == 0
			&& (!mgr_check_syncstate_node_exist(rel, appendnodeinfo.nodemasteroid, SYNC_STATE_SYNC, appendnodeinfo.tupleoid, true)))
		{
			pfree(getAgentCmdRst.description.data);
			pfree(infosendmsg.data);
			heap_close(rel, AccessShareLock);
			ereport(ERROR, (errmsg("gtm master \"%s\" has no sync slave node, can not append this node as potential node", NameStr(nodename))));
		}
		heap_close(rel, AccessShareLock);
		/* gtm master */
		gtmMasterTuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(appendnodeinfo.nodemasteroid));
		if(!HeapTupleIsValid(gtmMasterTuple))
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				, errmsg("cache lookup failed for gtm master oid %d", appendnodeinfo.nodemasteroid)));
		}
		mgr_node = (Form_mgr_node)GETSTRUCT(gtmMasterTuple);
		mastertype = mgr_node->nodetype;
		namestrcpy(&gtmMasterNameData, NameStr(mgr_node->nodename));
		ReleaseSysCache(gtmMasterTuple);
		get_nodeinfo(gtmMasterNameData.data, mastertype, &agtm_m_is_exist, &agtm_m_is_running, &agtm_m_nodeinfo);
		mastertupleoid = appendnodeinfo.nodemasteroid;
		if (!agtm_m_is_exist)
			ereport(ERROR, (errmsg("the master of node is not initialized")));

		if (!agtm_m_is_running)
			ereport(ERROR, (errmsg("the master of node is not running")));

		/* flush agtm slave's pg_hba.conf "host replication postgres slave_ip/32 trust" if agtm slave exist */
		mgr_add_hbaconf_by_masteroid(mastertupleoid, "replication", appendnodeinfo.nodeusername, appendnodeinfo.nodeaddr);

		/* step 1: update agtm master's pg_hba.conf. */
		resetStringInfo(&infosendmsg);
		mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "replication", appendnodeinfo.nodeusername, appendnodeinfo.nodeaddr, 32, "trust", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
								agtm_m_nodeinfo.nodepath,
								&infosendmsg,
								agtm_m_nodeinfo.nodehost,
								&getAgentCmdRst);

		/* step 2: reload agtm master. */
		mgr_reload_conf(agtm_m_nodeinfo.nodehost, agtm_m_nodeinfo.nodepath);

		/* step 3: basebackup for datanode master using pg_basebackup command. */
		mgr_check_dir_exist_and_priv(appendnodeinfo.nodehost, appendnodeinfo.nodepath);
		mgr_pgbasebackup(CNDN_TYPE_GTM_COOR_SLAVE, &appendnodeinfo, &agtm_m_nodeinfo);

		/* step 4: update agtm slave's postgresql.conf. */
		resetStringInfo(&infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("archive_command", "", &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("log_directory", "pg_log", &infosendmsg);
		mgr_add_parm(appendnodeinfo.nodename, CNDN_TYPE_GTM_COOR_SLAVE, &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
		mgr_append_pgconf_paras_str_str("hot_standby", "on", &infosendmsg);
		mgr_append_pgconf_paras_str_int("port", appendnodeinfo.nodeport, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF,
								appendnodeinfo.nodepath,
								&infosendmsg,
								appendnodeinfo.nodehost,
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));
		/*param table*/
		resetStringInfo(&infosendmsg);
		resetStringInfo(&(getAgentCmdRst.description));
		mgr_add_parm(appendnodeinfo.nodename, CNDN_TYPE_GTM_COOR_SLAVE, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, appendnodeinfo.nodepath, &infosendmsg, appendnodeinfo.nodehost, &getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/* step 5: update agtm slave's recovery.conf. */
		resetStringInfo(&infosendmsg);
		initStringInfo(&primary_conninfo_value);
		appendStringInfo(&primary_conninfo_value, "host=%s port=%d user=%s application_name=%s",
						get_hostaddress_from_hostoid(agtm_m_nodeinfo.nodehost),
						agtm_m_nodeinfo.nodeport,
						appendnodeinfo.nodeusername,
						nodename.data);

		mgr_append_pgconf_paras_str_quotastr("standby_mode", "on", &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("primary_conninfo", primary_conninfo_value.data, &infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("recovery_target_timeline", "latest", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_RECOVERCONF,
								appendnodeinfo.nodepath,
								&infosendmsg,
								appendnodeinfo.nodehost,
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/* step 6: start agtm slave. */
		mgr_start_node(CNDN_TYPE_GTM_COOR_SLAVE, appendnodeinfo.nodepath, appendnodeinfo.nodehost);

		/* step 7: update agtm master's postgresql.conf.*/
		resetStringInfo(&infosendmsg);
		initStringInfo(&infostrparam);
		initStringInfo(&infostrparamtmp);
		if (strcmp(NameStr(appendnodeinfo.sync_state), sync_state_tab[SYNC_STATE_SYNC].name) == 0)
		{
			appendStringInfo(&infostrparam, "%s", nodename.data);
			bsyncnode = true;
		}
		syncNum = mgr_get_master_sync_string(mastertupleoid, true, InvalidOid, &infostrparam);
		if (bsyncnode)
			syncNum++;
		if (strcmp(NameStr(appendnodeinfo.sync_state), sync_state_tab[SYNC_STATE_POTENTIAL].name) == 0)
		{
			Assert(infostrparam.len != 0);
			appendStringInfo(&infostrparam, ",%s", nodename.data);
		}
		if (infostrparam.len == 0)
			mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
		else
		{
			appendStringInfo(&infostrparamtmp, "%d (%s)", syncNum, infostrparam.data);
			mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", infostrparamtmp.data, &infosendmsg);
		}
		pfree(infostrparam.data);
		pfree(infostrparamtmp.data);

		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF,
								agtm_m_nodeinfo.nodepath,
								&infosendmsg,
								agtm_m_nodeinfo.nodehost,
								&getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

		/* step 8: reload agtm master's postgresql.conf. */
		mgr_reload_conf(agtm_m_nodeinfo.nodehost, agtm_m_nodeinfo.nodepath);

		/* step 9: update node system table's column to set initial is true */
		mgr_set_inited_incluster(appendnodeinfo.nodename, CNDN_TYPE_GTM_COOR_SLAVE, false, true);

	}PG_CATCH();
	{
		PG_RE_THROW();
	}PG_END_TRY();
	/*wait the node can accept connections*/
	sprintf(nodeport_buf, "%d", appendnodeinfo.nodeport);
	initStringInfo(&recorderr);
	if (!mgr_try_max_pingnode(appendnodeinfo.nodeaddr, nodeport_buf, appendnodeinfo.nodeusername, max_pingtry))
	{
		result = false;
		appendStringInfo(&recorderr, "waiting %d seconds for the new node can accept connections failed", max_pingtry);
	}
	if (result)
		tup_result = build_common_command_tuple(&nodename, true, "success");
	else
	{
		tup_result = build_common_command_tuple(&nodename, false, recorderr.data);
	}

	pfree(recorderr.data);
	pfree_AppendNodeInfo(appendnodeinfo);
	pfree_AppendNodeInfo(agtm_m_nodeinfo);

	return HeapTupleGetDatum(tup_result);
}

void pfree_AppendNodeInfo(AppendNodeInfo nodeinfo)
{
	if (nodeinfo.nodename != NULL)
	{
		pfree(nodeinfo.nodename);
		nodeinfo.nodename = NULL;
	}

	if (nodeinfo.nodepath != NULL)
	{
		pfree(nodeinfo.nodepath);
		nodeinfo.nodepath = NULL;
	}

	if (nodeinfo.nodeaddr != NULL)
	{
		pfree(nodeinfo.nodeaddr);
		nodeinfo.nodeaddr = NULL;
	}

	if (nodeinfo.nodeusername != NULL)
	{
		pfree(nodeinfo.nodeusername);
		nodeinfo.nodeusername = NULL;
	}
}

static char *get_temp_file_name()
{
	StringInfoData file_name_str;
	initStringInfo(&file_name_str);

	appendStringInfo(&file_name_str, "%s_%d.txt", PG_DUMPALL_TEMP_FILE, rand());

	return file_name_str.data;
}

/*
* get the node info. if bincluster is true, we will get infomation of the node which is inited
* and in the cluster; if bincluster is false, we will get information of the node no matter it
* inited or not, in cluster or not.
*
*/
void mgr_get_nodeinfo_byname_type(char *node_name, char node_type, bool bincluster, bool *is_exist, bool *is_running, AppendNodeInfo *nodeinfo)
{
	InitNodeInfo *info;
	ScanKeyData key[4];
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	Datum datumPath;
	NameData nodename;
	bool isNull = false;

	*is_exist = true;
	*is_running = true;

	namestrcpy(&nodename, node_name);
	if (bincluster)
	{
		ScanKeyInit(&key[0]
				,Anum_mgr_node_nodetype
				,BTEqualStrategyNumber
				,F_CHAREQ
				,CharGetDatum(node_type));

		ScanKeyInit(&key[1]
				,Anum_mgr_node_nodename
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,NameGetDatum(&nodename));
		ScanKeyInit(&key[2]
					,Anum_mgr_node_nodeinited
					,BTEqualStrategyNumber
					,F_BOOLEQ
					,BoolGetDatum(true));

		ScanKeyInit(&key[3]
					,Anum_mgr_node_nodeincluster
					,BTEqualStrategyNumber
					,F_BOOLEQ
					,BoolGetDatum(true));
	}
	else
	{
		ScanKeyInit(&key[0]
				,Anum_mgr_node_nodetype
				,BTEqualStrategyNumber
				,F_CHAREQ
				,CharGetDatum(node_type));

		ScanKeyInit(&key[1]
				,Anum_mgr_node_nodename
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,NameGetDatum(&nodename));
	}

	info = (InitNodeInfo *)palloc0(sizeof(InitNodeInfo));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);
	if (bincluster)
		info->rel_scan = heap_beginscan_catalog(info->rel_node, 4, key);
	else
		info->rel_scan = heap_beginscan_catalog(info->rel_node, 2, key);
	info->lcp =NULL;

	if ((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) == NULL)
	{
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, AccessShareLock);
		pfree(info);

		*is_exist = false;
		//for example, mgr_expand_dnmaster uses this variable.
		//nodeinfo->nodename = NULL;
		nodeinfo->nodeaddr = NULL;
		nodeinfo->nodeusername = NULL;
		nodeinfo->nodepath = NULL;
		namestrcpy(&(nodeinfo->sync_state), "");
		return;
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
	nodeinfo->tupleoid = HeapTupleGetOid(tuple);
	namestrcpy(&(nodeinfo->sync_state), NameStr(mgr_node->nodesync));
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

	if ( !is_node_running(nodeinfo->nodeaddr, nodeinfo->nodeport, nodeinfo->nodeusername, nodeinfo->nodetype))
		*is_running = false;

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
}

void get_nodeinfo_byname(char *node_name, char node_type, bool *is_exist, bool *is_running, AppendNodeInfo *nodeinfo)
{
	bool bincluster = true;

	mgr_get_nodeinfo_byname_type(node_name, node_type, bincluster, is_exist, is_running, nodeinfo);
}

void get_nodeinfo(char *nodename, char node_type, bool *is_exist, bool *is_running, AppendNodeInfo *nodeinfo)
{
	InitNodeInfo *info = NULL;
	ScanKeyData key[4];
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	Datum datumPath;
	NameData nodenameData;
	bool isNull = false;

	Assert(nodename);
	Assert(nodeinfo);

	*is_exist = true;
	*is_running = true;
	namestrcpy(&nodenameData, nodename);

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
	ScanKeyInit(&key[2]
				,Anum_mgr_node_nodetype
				,BTEqualStrategyNumber
				,F_CHAREQ
				,CharGetDatum(node_type));
	ScanKeyInit(&key[3]
				,Anum_mgr_node_nodename
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,NameGetDatum(&nodenameData));

	info = (InitNodeInfo *)palloc0(sizeof(InitNodeInfo));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);
	info->rel_scan = heap_beginscan_catalog(info->rel_node, 4, key);
	info->lcp =NULL;

	if ((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) == NULL)
	{
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, AccessShareLock);
		pfree(info);

		*is_exist = false;
		return;
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
	nodeinfo->tupleoid = HeapTupleGetOid(tuple);
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

	if ( !is_node_running(nodeinfo->nodeaddr, nodeinfo->nodeport, nodeinfo->nodeusername, nodeinfo->nodetype))
		*is_running = false;

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
}

void mgr_pgbasebackup(char nodetype, AppendNodeInfo *appendnodeinfo, AppendNodeInfo *parentnodeinfo)
{

	ManagerAgent *ma;
	StringInfoData sendstrmsg, buf;
	GetAgentCmdRst getAgentCmdRst;

	initStringInfo(&sendstrmsg);
	initStringInfo(&(getAgentCmdRst.description));

	if (nodetype == CNDN_TYPE_GTM_COOR_SLAVE || nodetype == CNDN_TYPE_DATANODE_MASTER
			 || nodetype == CNDN_TYPE_DATANODE_SLAVE)
	{
		appendStringInfo(&sendstrmsg, " -h %s -p %d -U %s -D %s -Xs -Fp -R --nodename %s",
									get_hostaddress_from_hostoid(parentnodeinfo->nodehost)
									,parentnodeinfo->nodeport
									,get_hostuser_from_hostoid(parentnodeinfo->nodehost)
									,appendnodeinfo->nodepath
									,appendnodeinfo->nodename);
	}

	ma = ma_connect_hostoid(appendnodeinfo->nodehost);
	if(!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		ereport(ERROR, (errmsg("could not connect socket for agent \"%s\".",
						get_hostname_from_hostoid(appendnodeinfo->nodehost))));
		return;
	}
	getAgentCmdRst.ret = false;
	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, AGT_CMD_CNDN_SLAVE_INIT);
	mgr_append_infostr_infostr(&buf, &sendstrmsg);
	pfree(sendstrmsg.data);
	ma_endmessage(&buf, ma);
	if (! ma_flush(ma, true))
	{
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}
	/*check the receive msg*/
	mgr_recv_msg(ma, &getAgentCmdRst);
	ma_close(ma);
	if (!getAgentCmdRst.ret)
		ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

}
void mgr_make_sure_all_running(char node_type)
{
	InitNodeInfo *info;
	ScanKeyData key[4];
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	char * hostaddr = NULL;
	char *nodetype_str = NULL;
	char *user;
	NameData nodetypestr_data;
	NameData nodename;

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

	ScanKeyInit(&key[2]
				,Anum_mgr_node_nodetype
				,BTEqualStrategyNumber
				,F_CHAREQ
				,CharGetDatum(node_type));

	ScanKeyInit(&key[3]
			,Anum_mgr_node_nodezone
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,CStringGetDatum(mgr_zone));

	info = (InitNodeInfo *)palloc0(sizeof(InitNodeInfo));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);
	info->rel_scan = heap_beginscan_catalog(info->rel_node, 4, key);
	info->lcp = NULL;

	while ((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);

		hostaddr = get_hostaddress_from_hostoid(mgr_node->nodehost);
		user = get_hostuser_from_hostoid(mgr_node->nodehost);
		if (!is_node_running(hostaddr, mgr_node->nodeport, user, mgr_node->nodetype))
		{
			nodetype_str = mgr_nodetype_str(mgr_node->nodetype);
			namestrcpy(&nodename, NameStr(mgr_node->nodename));
			heap_endscan(info->rel_scan);
			heap_close(info->rel_node, AccessShareLock);
			pfree(info);
			pfree(hostaddr);
			pfree(user);
			namestrcpy(&nodetypestr_data, nodetype_str);
			pfree(nodetype_str);
			ereport(ERROR, (errmsg("%s \"%s\" is not running", nodetypestr_data.data,nodename.data)));
		} 
		pfree(user);
	}

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);

	if (hostaddr != NULL)
		pfree(hostaddr);

	return;
}

bool is_node_running(char *hostaddr, int32 hostport, char *user, char nodetype)
{
	char bufPort[10];
	int ret;
	PGconn *pgconn = NULL;
	PGresult *res = NULL;

	memset(bufPort, 0, 10);
	sprintf(bufPort, "%d", hostport);

	ret = pingNode_user(hostaddr, bufPort, user);
	if (ret != PQPING_OK)
	{
		return false;
	}
	else
	{
		/* all gtm coord node have the same pgxc_node_name value */
		if(nodetype == CNDN_TYPE_GTM_COOR_MASTER ||
		   nodetype == CNDN_TYPE_GTM_COOR_SLAVE)
		{
			pgconn = ExpPQsetdbLogin(hostaddr
									,bufPort
									,NULL, NULL
									,user
									,NULL);
			if (PQstatus(pgconn) != CONNECTION_OK)
			{
				if(pgconn)
				{
					PQfinish(pgconn);
					pgconn = NULL;
				}
				return false;
			}
			else 
			{
				/* all gtm coord node have the same pgxc_node_name value */
				res = PQexec(pgconn, "show pgxc_node_name");
				if (PQresultStatus(res) == PGRES_TUPLES_OK)
				{
					namestrcpy(&GTM_COORD_PGXC_NODE_NAME, PQgetvalue(res, 0, 0));
				}
				PQclear(res);
				res = NULL;
			}
		}
		else 
		{
			/* ignore */
		}
	}
	if(pgconn)
		PQfinish(pgconn);
	if(res)
		PQclear(res);	

	return true;
}

static void mgr_get_parent_appendnodeinfo(Oid nodemasternameoid, AppendNodeInfo *parentnodeinfo)
{
	Relation noderelation;
	HeapTuple mastertuple;
	Form_mgr_node mgr_node;
	Datum datumPath;
	bool isNull = false;

	noderelation = heap_open(NodeRelationId, AccessShareLock);

	mastertuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(nodemasternameoid));
	if(!HeapTupleIsValid(mastertuple))
	{
		ReleaseSysCache(mastertuple);
		heap_close(noderelation, AccessShareLock);

		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
			,errmsg("could not find datanode master")));
	}

	mgr_node = (Form_mgr_node)GETSTRUCT(mastertuple);
	Assert(mgr_node);

	parentnodeinfo->nodename = pstrdup(NameStr(mgr_node->nodename));
	parentnodeinfo->nodetype = mgr_node->nodetype;
	parentnodeinfo->nodeaddr = get_hostaddress_from_hostoid(mgr_node->nodehost);
	parentnodeinfo->nodeusername = get_hostuser_from_hostoid(mgr_node->nodehost);
	parentnodeinfo->nodeport = mgr_node->nodeport;
	parentnodeinfo->nodehost = mgr_node->nodehost;

	if (mgr_node->nodeinited == false)
	{
		ReleaseSysCache(mastertuple);
		heap_close(noderelation, AccessShareLock);
		ereport(ERROR, (errmsg("datanode master \"%s\" does not initialized", parentnodeinfo->nodename)));
	}

	/*get nodepath from tuple*/
	datumPath = heap_getattr(mastertuple, Anum_mgr_node_nodepath, RelationGetDescr(noderelation), &isNull);
	if (isNull)
	{
		ReleaseSysCache(mastertuple);
		heap_close(noderelation, AccessShareLock);
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
			, errmsg("column nodepath is null")));
	}

	parentnodeinfo->nodepath = pstrdup(TextDatumGetCString(datumPath));

	ReleaseSysCache(mastertuple);
	heap_close(noderelation, AccessShareLock);
}

static void mgr_add_hbaconf_all(char *dnusername, char *dnaddr, bool check_incluster)
{
	InitNodeInfo *info;
	ScanKeyData key[3];
	GetAgentCmdRst getAgentCmdRst;
	StringInfoData  infosendmsg;
	HeapTuple tuple;
	Datum datumPath;
	bool isNull;
	Form_mgr_node mgr_node;

	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsg);

	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodezone
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,CStringGetDatum(mgr_zone));
	ScanKeyInit(&key[1]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,CharGetDatum(true));
	ScanKeyInit(&key[2]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	info = palloc(sizeof(*info));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);
	if (check_incluster)
		info->rel_scan = heap_beginscan_catalog(info->rel_node, 3, key);
	else
		info->rel_scan = heap_beginscan_catalog(info->rel_node, 0, NULL);
	info->lcp =NULL;

	while ((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);

		/*get nodepath from tuple*/
		datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(info->rel_node), &isNull);
		if (isNull)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
				, errmsg("column nodepath is null")));
		}
		mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", "all", dnaddr, 32, "trust", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
							TextDatumGetCString(datumPath),
							&infosendmsg,
							mgr_node->nodehost,
							&getAgentCmdRst);
		resetStringInfo(&infosendmsg);

		mgr_reload_conf(mgr_node->nodehost, TextDatumGetCString(datumPath));
	}
	pfree(infosendmsg.data);
	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
}


void mgr_add_hbaconf(char nodetype, char *dnusername, char *dnaddr)
{

	InitNodeInfo *info;
	ScanKeyData key[2];
	GetAgentCmdRst getAgentCmdRst;
	StringInfoData  infosendmsg;
	HeapTuple tuple;
	Datum datumPath;
	bool isNull;
	Oid hostoid;
	char *nodepath;
	Form_mgr_node mgr_node;
	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsg);

	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodetype
				,BTEqualStrategyNumber
				,F_CHAREQ
				,CharGetDatum(nodetype));
	ScanKeyInit(&key[1]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));

	info = palloc(sizeof(*info));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);
	info->rel_scan = heap_beginscan_catalog(info->rel_node, 2, key);
	info->lcp =NULL;

	while((tuple = heap_getnext(info->rel_scan, ForwardScanDirection))!= NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);

		/*get nodepath from tuple*/
		datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(info->rel_node), &isNull);
		if (isNull)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
				, errmsg("column nodepath is null")));
		}

		mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", dnusername, dnaddr, 32, "trust", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
								TextDatumGetCString(datumPath),
								&infosendmsg,
								mgr_node->nodehost,
								&getAgentCmdRst);

		hostoid = mgr_node->nodehost;
		nodepath = TextDatumGetCString(datumPath);
		/* reload it at last */
		mgr_reload_conf(hostoid, nodepath);
	}

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
}

void mgr_reload_conf(Oid hostoid, char *nodepath)
{
	ManagerAgent *ma;
	StringInfoData sendstrmsg, buf;
	GetAgentCmdRst getAgentCmdRst;
	bool execRes = false;
	char *addr;
	NameData hostaddr;

	addr = get_hostname_from_hostoid(hostoid);
	namestrcpy(&hostaddr, addr);
	pfree(addr);
	initStringInfo(&sendstrmsg);
	initStringInfo(&(getAgentCmdRst.description));
	appendStringInfo(&sendstrmsg, " reload -D %s", nodepath); /* pg_ctl reload -D pathdir */
	ma = ma_connect_hostoid(hostoid);
	if(!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		ereport(ERROR, (errmsg("could not connect socket for agent \"%s\".",
						hostaddr.data)));
		return;
	}
	getAgentCmdRst.ret = false;
	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, AGT_CMD_NODE_RELOAD);
	mgr_append_infostr_infostr(&buf, &sendstrmsg);
	pfree(sendstrmsg.data);
	ma_endmessage(&buf, ma);
	if (! ma_flush(ma, true))
	{
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}
	/*check the receive msg*/
	execRes = mgr_recv_msg(ma, &getAgentCmdRst);
	ma_close(ma);
	if (!execRes)
	{
		ereport(WARNING, (errmsg("%s reload -D %s fail %s",
			hostaddr.data, nodepath, getAgentCmdRst.description.data)));
	}
	pfree(getAgentCmdRst.description.data);
}

static void mgr_set_inited_incluster(char *nodename, char nodetype, bool checkvalue, bool setvalue)
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
				,CharGetDatum(nodetype));

	ScanKeyInit(&key[2]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(checkvalue));

	ScanKeyInit(&key[3]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(checkvalue));

	info = palloc(sizeof(*info));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);
	info->rel_scan = heap_beginscan_catalog(info->rel_node, 4, key);
	info->lcp =NULL;

	tuple = heap_getnext(info->rel_scan, ForwardScanDirection);
	if(tuple == NULL)
	{
		/* end of row */
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, AccessShareLock);
		pfree(info);
		return ;
	}

	mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
	Assert(mgr_node);

	mgr_node->nodeinited = setvalue;
	mgr_node->nodeincluster = setvalue;
	mgr_node->allowcure = setvalue;
	namestrcpy(&mgr_node->curestatus, CURE_STATUS_NORMAL);
	heap_inplace_update(info->rel_node, tuple);

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
}

static void mgr_rm_dumpall_temp_file(Oid dnhostoid,char *temp_file)
{
	StringInfoData cmd_str;
	StringInfoData buf;
	GetAgentCmdRst getAgentCmdRst;
	ManagerAgent *ma;
	bool execRes = false;
	char *addr;
	NameData hostaddr;

	initStringInfo(&cmd_str);
	initStringInfo(&buf);
	initStringInfo(&(getAgentCmdRst.description));

	appendStringInfo(&cmd_str, "%s", temp_file);

	addr = get_hostname_from_hostoid(dnhostoid);
	namestrcpy(&hostaddr, addr);
	pfree(addr);
	/* connection agent */
	ma = ma_connect_hostoid(dnhostoid);
	if (!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		ereport(ERROR, (errmsg("could not connect socket for agent \"%s\".",
						hostaddr.data)));
		return;
	}

	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, AGT_CMD_RM);
	ma_sendstring(&buf, cmd_str.data);
	pfree(cmd_str.data);
	ma_endmessage(&buf, ma);

	if (! ma_flush(ma, true))
	{
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}

	/*check the receive msg*/
	execRes = mgr_recv_msg(ma, &getAgentCmdRst);
	if(!execRes)
		ereport(WARNING, (errmsg("%s rm -f %s fail %s",
			hostaddr.data, temp_file, getAgentCmdRst.description.data)));
	ma_close(ma);
	pfree(getAgentCmdRst.description.data);
}

static void mgr_create_node_on_all_coord(PG_FUNCTION_ARGS, char nodetype, char *dnname, Oid dnhostoid, int32 dnport)
{
	InitNodeInfo *info;
	ScanKeyData key[2];
	HeapTuple tuple;
	ManagerAgent *ma;
	Form_mgr_node mgr_node;
	StringInfoData psql_cmd;
	bool execRes = false;
	StringInfoData buf;
	char *addressconnect = NULL;
	char *addressnode = NULL;
	char *user = NULL;

	GetAgentCmdRst getAgentCmdRst;

	initStringInfo(&(getAgentCmdRst.description));

	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	ScanKeyInit(&key[1]
				,Anum_mgr_node_nodezone
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,CStringGetDatum(mgr_zone));

	info = palloc(sizeof(*info));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);
	info->rel_scan = heap_beginscan_catalog(info->rel_node, 2, key);
	info->lcp = NULL;

	while ((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);

		if (mgr_node->nodetype != CNDN_TYPE_COORDINATOR_MASTER && mgr_node->nodetype != CNDN_TYPE_GTM_COOR_MASTER)
			continue;

		/* connection agent */
		ma = ma_connect_hostoid(mgr_node->nodehost);
		if (!ma_isconnected(ma))
		{
			/* report error message */
			getAgentCmdRst.ret = false;
			appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
			ma_close(ma);

			heap_endscan(info->rel_scan);
			heap_close(info->rel_node, AccessShareLock);
			pfree(info);

			ereport(ERROR, (errmsg("could not connect socket for agent \"%s\".",
							get_hostname_from_hostoid(mgr_node->nodehost))));
			return;
		}

		initStringInfo(&psql_cmd);
		addressconnect = get_hostaddress_from_hostoid(mgr_node->nodehost);
		user = get_hostuser_from_hostoid(mgr_node->nodehost);
		appendStringInfo(&psql_cmd, " -h %s -p %u -d %s -U %s -a -c \""
						,addressconnect
						,mgr_node->nodeport
						,DEFAULT_DB
						,user);

		addressnode = get_hostaddress_from_hostoid(dnhostoid);

		if (nodetype == CNDN_TYPE_COORDINATOR_MASTER)
			appendStringInfo(&psql_cmd, " CREATE NODE \\\"%s\\\" WITH (TYPE = 'coordinator', HOST='%s', PORT=%d);"
							,dnname
							,addressnode
							,dnport);
		if (nodetype == CNDN_TYPE_DATANODE_MASTER)
			appendStringInfo(&psql_cmd, " CREATE NODE \\\"%s\\\" WITH (TYPE = 'datanode', HOST='%s', PORT=%d);"
							,dnname
							,addressnode
							,dnport);

		appendStringInfo(&psql_cmd, " select pgxc_pool_reload();\"");

		ma_beginmessage(&buf, AGT_MSG_COMMAND);
		ma_sendbyte(&buf, AGT_CMD_PSQL_CMD);
		ma_sendstring(&buf, psql_cmd.data);
		pfree(psql_cmd.data);
		pfree(addressconnect);
		pfree(addressnode);
		pfree(user);
		ma_endmessage(&buf, ma);

		if (! ma_flush(ma, true))
		{
			getAgentCmdRst.ret = false;
			appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
			ma_close(ma);

			heap_endscan(info->rel_scan);
			heap_close(info->rel_node, AccessShareLock);
			pfree(info);

			return;
		}

		/*check the receive msg*/
		execRes = mgr_recv_msg(ma, &getAgentCmdRst);
		ma_close(ma);
		if (!execRes)
			ereport(WARNING, (errmsg("create node on all coordinators fail %s",
				getAgentCmdRst.description.data)));
	}

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
	pfree(getAgentCmdRst.description.data);
}

/*
	execute remove coordinator command, so the function is need to remove
	the coordinator from pgxc_node table.
	it just remove the tuple, but it didn't change the primary and preferred value
*/
static bool mgr_drop_node_on_all_coord(char nodetype, char *nodename)
{
	InitNodeInfo *info;
	ScanKeyData key[3];
	HeapTuple tuple;
	ManagerAgent *ma;
	Form_mgr_node mgr_node;
	StringInfoData psql_cmd, psql_cmd2;
	bool execRes = false;
	bool execRes2 = false;
	StringInfoData buf;
	char *addressconnect = NULL;
	char *user = NULL;

	GetAgentCmdRst getAgentCmdRst;

	initStringInfo(&(getAgentCmdRst.description));

	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodetype
				,BTEqualStrategyNumber
				,F_CHAREQ
				,CharGetDatum(nodetype));

	ScanKeyInit(&key[1]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	ScanKeyInit(&key[2]
				,Anum_mgr_node_nodezone
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,CStringGetDatum(mgr_zone));
	info = palloc(sizeof(*info));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);
	info->rel_scan = heap_beginscan_catalog(info->rel_node, 3, key);
	info->lcp = NULL;

	while ((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);

		if ((strcmp(NameStr(mgr_node->nodename), nodename) == 0) && mgr_node->nodetype == nodetype)
			continue;

		/* connection agent */
		ma = ma_connect_hostoid(mgr_node->nodehost);
		if (!ma_isconnected(ma))
		{
			/* report error message */
			getAgentCmdRst.ret = false;
			appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
			ma_close(ma);

			heap_endscan(info->rel_scan);
			heap_close(info->rel_node, AccessShareLock);
			pfree(info);

			ereport(ERROR, (errmsg("could not connect socket for agent \"%s\".",
							get_hostname_from_hostoid(mgr_node->nodehost))));
		}

		initStringInfo(&psql_cmd);
		initStringInfo(&psql_cmd2);
		addressconnect = get_hostaddress_from_hostoid(mgr_node->nodehost);
		user = get_hostuser_from_hostoid(mgr_node->nodehost);
		appendStringInfo(&psql_cmd, " -h %s -p %u -d %s -U %s -a -c \""
						,addressconnect
						,mgr_node->nodeport
						,DEFAULT_DB
						,user);
		appendStringInfoString(&psql_cmd2, psql_cmd.data);


		appendStringInfo(&psql_cmd, " DROP NODE \\\"%s\\\";", nodename);
		appendStringInfo(&psql_cmd, " set FORCE_PARALLEL_MODE = off;\"");

		ma_beginmessage(&buf, AGT_MSG_COMMAND);
		ma_sendbyte(&buf, AGT_CMD_PSQL_CMD);
		ma_sendstring(&buf, psql_cmd.data);
		pfree(psql_cmd.data);
		pfree(addressconnect);
		pfree(user);
		ma_endmessage(&buf, ma);

		if (! ma_flush(ma, true))
		{
			getAgentCmdRst.ret = false;
			appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
			ma_close(ma);

			heap_endscan(info->rel_scan);
			heap_close(info->rel_node, AccessShareLock);
			pfree(info);

			return execRes;
		}
		/*check the receive msg*/
		execRes = mgr_recv_msg(ma, &getAgentCmdRst);
		ma_close(ma);
		if (!execRes)
			ereport(WARNING, (errmsg("drop node \"%s\" on coordinators \"%s\" fail %s"
				,nodename, NameStr(mgr_node->nodename), getAgentCmdRst.description.data)));


		/* exec "select pgxc_pool_reload();" */
		ma = ma_connect_hostoid(mgr_node->nodehost);
		appendStringInfo(&psql_cmd2, " select pgxc_pool_reload();\"");
		ma_beginmessage(&buf, AGT_MSG_COMMAND);
		ma_sendbyte(&buf, AGT_CMD_PSQL_CMD);
		ma_sendstring(&buf, psql_cmd2.data);
		pfree(psql_cmd2.data);
		ma_endmessage(&buf, ma);

		if (! ma_flush(ma, true))
		{
			getAgentCmdRst.ret = false;
			appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
			ma_close(ma);

			heap_endscan(info->rel_scan);
			heap_close(info->rel_node, AccessShareLock);
			pfree(info);

			return execRes;
		}
		/*check the receive msg*/
		execRes2 = mgr_recv_msg(ma, &getAgentCmdRst);
		ma_close(ma);
		if (!execRes2)
			ereport(WARNING, (errmsg("exec \"select pgxc_pool_reload();\" on coordinators \"%s\" fail.", NameStr(mgr_node->nodename))));
	}

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
	pfree(getAgentCmdRst.description.data);

	return execRes;
}

void mgr_start_node(char nodetype, const char *nodepath, Oid hostoid)
{
	StringInfoData start_cmd;
	StringInfoData buf;
	GetAgentCmdRst getAgentCmdRst;
	ManagerAgent *ma;

	initStringInfo(&start_cmd);
	initStringInfo(&buf);
	initStringInfo(&(getAgentCmdRst.description));

	switch (nodetype)
	{
		case CNDN_TYPE_COORDINATOR_MASTER:
		case CNDN_TYPE_COORDINATOR_SLAVE:
			appendStringInfo(&start_cmd, " start -Z coordinator -D %s -o -i -w -c -l %s/logfile", nodepath, nodepath);
			break;
		case CNDN_TYPE_DATANODE_MASTER:
		case CNDN_TYPE_DATANODE_SLAVE:
			appendStringInfo(&start_cmd, " start -Z datanode -D %s -o -i -w -c -l %s/logfile", nodepath, nodepath);
			break;
		case CNDN_TYPE_GTM_COOR_MASTER:
		case CNDN_TYPE_GTM_COOR_SLAVE:
			appendStringInfo(&start_cmd, " start -Z gtm_coord -D %s -o -i -w -c -l %s/logfile", nodepath, nodepath);
			break;
		default:
			ereport(ERROR, (errmsg("node type \"%c\" does not exist", nodetype)));
			break;
	}

	/* connection agent */
	ma = ma_connect_hostoid(hostoid);
	if (!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		ereport(ERROR, (errmsg("could not connect socket for agent \"%s\".",
						get_hostname_from_hostoid(hostoid))));
		return;
	}

	ma_beginmessage(&buf, AGT_MSG_COMMAND);

	if (nodetype == CNDN_TYPE_GTM_COOR_MASTER || nodetype == CNDN_TYPE_GTM_COOR_SLAVE)
		ma_sendbyte(&buf, AGT_CMD_GTMCOOR_START_SLAVE); /* agtm_ctl */
	else if (nodetype == CNDN_TYPE_COORDINATOR_MASTER || nodetype == CNDN_TYPE_COORDINATOR_SLAVE)
		ma_sendbyte(&buf, AGT_CMD_CN_START);  /* pg_ctl  */
	else
		ma_sendbyte(&buf, AGT_CMD_DN_START);  /* pg_ctl  */

	ma_sendstring(&buf, start_cmd.data);
	pfree(start_cmd.data);
	ma_endmessage(&buf, ma);

	if (! ma_flush(ma, true))
	{
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
	}

	/*check the receive msg*/
	mgr_recv_msg(ma, &getAgentCmdRst);
	ma_close(ma);
	if (!getAgentCmdRst.ret)
		ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));
}

/**
* GetAgentCmdRst.ret == 1, means success,
* otherwise failure, see error message in GetAgentCmdRst.description.
* The return result is palloced, so pfree it if it useless.
*/
GetAgentCmdRst *mgr_start_node_execute(char nodetype, const char *nodepath, char *hostaddr, int32 hostagentport)
{
	StringInfoData start_cmd;
	StringInfoData buf;
	GetAgentCmdRst *cmdRst;
	ManagerAgent *ma;

	initStringInfo(&start_cmd);
	initStringInfo(&buf);
	cmdRst = palloc0(sizeof(GetAgentCmdRst));
	initStringInfo(&cmdRst->description);

	switch (nodetype)
	{
		case CNDN_TYPE_COORDINATOR_MASTER:
		case CNDN_TYPE_COORDINATOR_SLAVE:
			appendStringInfo(&start_cmd, " start -Z coordinator -D %s -o -i -w -c -l %s/logfile", nodepath, nodepath);
			break;
		case CNDN_TYPE_DATANODE_MASTER:
		case CNDN_TYPE_DATANODE_SLAVE:
			appendStringInfo(&start_cmd, " start -Z datanode -D %s -o -i -w -c -l %s/logfile", nodepath, nodepath);
			break;
		case CNDN_TYPE_GTM_COOR_MASTER:
		case CNDN_TYPE_GTM_COOR_SLAVE:
			appendStringInfo(&start_cmd, " start -D %s -o -i -w -c -l %s/logfile", nodepath, nodepath);
			break;
		default:
			appendStringInfo(&(cmdRst->description), "node type \"%c\" does not exist", nodetype);
			cmdRst->ret = 0;
			pfree(start_cmd.data);
			pfree(buf.data);
			return cmdRst;
	}

	/* connection agent */
	ma = ma_connect(hostaddr, hostagentport);
	if (!ma_isconnected(ma))
	{
		/* report error message */
		cmdRst->ret = 0;
		appendStringInfoString(&(cmdRst->description), ma_last_error_msg(ma));
		ma_close(ma);
		pfree(start_cmd.data);
		pfree(buf.data);
		return cmdRst;
	}

	ma_beginmessage(&buf, AGT_MSG_COMMAND);

	if (nodetype == CNDN_TYPE_GTM_COOR_MASTER || nodetype == CNDN_TYPE_GTM_COOR_SLAVE)
		ma_sendbyte(&buf, AGT_CMD_GTMCOOR_START_SLAVE); /* agtm_ctl */
	else if (nodetype == CNDN_TYPE_COORDINATOR_MASTER || nodetype == CNDN_TYPE_COORDINATOR_SLAVE)
		ma_sendbyte(&buf, AGT_CMD_CN_START);  /* pg_ctl  */
	else
		ma_sendbyte(&buf, AGT_CMD_DN_START);  /* pg_ctl  */

	ma_sendstring(&buf, start_cmd.data);
	ma_endmessage(&buf, ma);
	if (!ma_flush(ma, true))
	{
		cmdRst->ret = false;
		appendStringInfoString(&(cmdRst->description), ma_last_error_msg(ma));
		ma_close(ma);
	}
	else
	{
		/*check the receive msg*/
		mgr_recv_msg(ma, cmdRst);
		ma_close(ma);
	}
	pfree(start_cmd.data);
	return cmdRst;
}

/**
* GetAgentCmdRst.ret == 1, means success,
* otherwise failure, see error message in GetAgentCmdRst.description.
* The return result is palloced, so pfree it if it useless.
*/
GetAgentCmdRst *mgr_stop_node_execute(char nodetype, const char *nodepath, char *hostaddr, int32 hostagentport, char *shutdown_mode)
{
	StringInfoData cmdStr;
	StringInfoData buf;
	GetAgentCmdRst *cmdRst;
	ManagerAgent *ma;

	initStringInfo(&cmdStr);
	initStringInfo(&buf);
	cmdRst = palloc0(sizeof(GetAgentCmdRst));
	initStringInfo(&cmdRst->description);

	switch (nodetype)
	{
		case CNDN_TYPE_COORDINATOR_MASTER:
		case CNDN_TYPE_COORDINATOR_SLAVE:
			appendStringInfo(&cmdStr, " stop -D %s -Z coordinator -m %s -o -i -w -c",nodepath, shutdown_mode);
			break;
		case CNDN_TYPE_DATANODE_MASTER:
		case CNDN_TYPE_DATANODE_SLAVE:
			appendStringInfo(&cmdStr, " stop -D %s -Z datanode -m %s -o -i -w -c", nodepath, shutdown_mode);
			break;
		case CNDN_TYPE_GTM_COOR_MASTER:
		case CNDN_TYPE_GTM_COOR_SLAVE:
			appendStringInfo(&cmdStr, " stop -D %s -m %s -o -i -w -c", nodepath, shutdown_mode);
			break;
		default:
			appendStringInfo(&(cmdRst->description), "node type \"%c\" does not exist", nodetype);
			cmdRst->ret = 0;
			pfree(cmdStr.data);
			pfree(buf.data);
			return cmdRst;
	}

	/* connection agent */
	ma = ma_connect(hostaddr, hostagentport);
	if (!ma_isconnected(ma))
	{
		/* report error message */
		cmdRst->ret = 0;
		appendStringInfoString(&(cmdRst->description), ma_last_error_msg(ma));
		ma_close(ma);
		pfree(cmdStr.data);
		pfree(buf.data);
		return cmdRst;
	}

	ma_beginmessage(&buf, AGT_MSG_COMMAND);

	if (nodetype == CNDN_TYPE_GTM_COOR_MASTER)
		ma_sendbyte(&buf, AGT_CMD_GTMCOOR_STOP_MASTER);
	else if (nodetype == CNDN_TYPE_GTM_COOR_SLAVE)
		ma_sendbyte(&buf, AGT_CMD_GTMCOOR_STOP_SLAVE);
	else if (nodetype == CNDN_TYPE_COORDINATOR_MASTER || nodetype == CNDN_TYPE_COORDINATOR_SLAVE)
		ma_sendbyte(&buf, AGT_CMD_CN_STOP);  /* pg_ctl  */
	else
		ma_sendbyte(&buf, AGT_CMD_DN_STOP);  /* pg_ctl  */

	ma_sendstring(&buf, cmdStr.data);
	ma_endmessage(&buf, ma);
	if (!ma_flush(ma, true))
	{
		cmdRst->ret = false;
		appendStringInfoString(&(cmdRst->description), ma_last_error_msg(ma));
		ma_close(ma);
	}
	else
	{
		/*check the receive msg*/
		mgr_recv_msg(ma, cmdRst);
		ma_close(ma);
	}
	pfree(cmdStr.data);
	return cmdRst;
}

static void mgr_stop_node_with_restoremode(const char *nodepath, Oid hostoid)
{
	StringInfoData stop_cmd;
	StringInfoData buf;
	GetAgentCmdRst getAgentCmdRst;
	ManagerAgent *ma;

	initStringInfo(&stop_cmd);
	initStringInfo(&buf);
	initStringInfo(&(getAgentCmdRst.description));

	appendStringInfo(&stop_cmd, " stop -Z restoremode -D %s", nodepath);

	/* connection agent */
	ma = ma_connect_hostoid(hostoid);
	if (!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		ereport(ERROR, (errmsg("could not connect socket for agent \"%s\".",
						get_hostname_from_hostoid(hostoid))));
		return;
	}

	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, AGT_CMD_DN_STOP);
	ma_sendstring(&buf, stop_cmd.data);
	pfree(stop_cmd.data);
	ma_endmessage(&buf, ma);

	if (! ma_flush(ma, true))
	{
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}

	/*check the receive msg*/
	mgr_recv_msg(ma, &getAgentCmdRst);
	ma_close(ma);
	if (!getAgentCmdRst.ret)
		ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));
}

static void mgr_pg_dumpall_input_node(const Oid dn_master_oid, const int32 dn_master_port, char *temp_file)
{
	StringInfoData pgsql_cmd;
	StringInfoData buf;
	GetAgentCmdRst getAgentCmdRst;
	ManagerAgent *ma;
	char *dn_master_addr;
	bool execRes = false;

	initStringInfo(&pgsql_cmd);
	initStringInfo(&buf);
	initStringInfo(&(getAgentCmdRst.description));

	dn_master_addr = get_hostaddress_from_hostoid(dn_master_oid);
	appendStringInfo(&pgsql_cmd, " -h %s -p %d -d %s -f %s", dn_master_addr, dn_master_port, DEFAULT_DB, temp_file);

	/* connection agent */
	ma = ma_connect_hostoid(dn_master_oid);
	if (!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		ereport(ERROR, (errmsg("could not connect socket for agent \"%s\".",
						get_hostname_from_hostoid(dn_master_oid))));
		return;
	}

	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, AGT_CMD_PSQL_CMD);
	ma_sendstring(&buf, pgsql_cmd.data);
	pfree(pgsql_cmd.data);
	ma_endmessage(&buf, ma);

	if (! ma_flush(ma, true))
	{
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}

	/*check the receive msg*/
	execRes = mgr_recv_msg(ma, &getAgentCmdRst);
	if (!execRes)
		ereport(WARNING, (errmsg("dump input node info fail %s", getAgentCmdRst.description.data)));
	ma_close(ma);
	pfree(dn_master_addr);
	pfree(getAgentCmdRst.description.data);
}

static void mgr_start_node_with_restoremode(const char *nodepath, Oid hostoid, char nodetype)
{
	StringInfoData start_cmd;
	StringInfoData buf;
	GetAgentCmdRst getAgentCmdRst;
	ManagerAgent *ma;
	char *nodetypestring;

	Assert(nodetype == CNDN_TYPE_COORDINATOR_MASTER || nodetype == CNDN_TYPE_DATANODE_MASTER);

	initStringInfo(&start_cmd);
	initStringInfo(&buf);
	initStringInfo(&(getAgentCmdRst.description));
	nodetypestring = (nodetype == CNDN_TYPE_COORDINATOR_MASTER) ? "coordinator" : "datanode";

	appendStringInfo(&start_cmd, " start -Z restoremode -D %s -R %s -o -i -w -c -l %s/logfile"
		, nodepath, nodetypestring, nodepath);

	/* connection agent */
	ma = ma_connect_hostoid(hostoid);
	if (!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		ereport(ERROR, (errmsg("could not connect socket for agent \"%s\".",
						get_hostname_from_hostoid(hostoid))));
		return;
	}

	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, AGT_CMD_DN_START);
	ma_sendstring(&buf, start_cmd.data);
	pfree(start_cmd.data);
	ma_endmessage(&buf, ma);

	if (! ma_flush(ma, true))
	{
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}

	/*check the receive msg*/
	mgr_recv_msg(ma, &getAgentCmdRst);
	ma_close(ma);

	if (!getAgentCmdRst.ret)
		ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));
}

static void mgr_pg_dumpall(Oid hostoid, int32 hostport, Oid dnmasteroid, char *temp_file)
{
	StringInfoData pg_dumpall_cmd;
	StringInfoData buf;
	GetAgentCmdRst getAgentCmdRst;
	ManagerAgent *ma;
	char * hostaddr;

	initStringInfo(&pg_dumpall_cmd);
	initStringInfo(&buf);
	initStringInfo(&(getAgentCmdRst.description));

	hostaddr = get_hostaddress_from_hostoid(hostoid);
	appendStringInfo(&pg_dumpall_cmd, " -h %s -p %d -s --include-nodes --dump-nodes --dump-adb_slot -f %s", hostaddr, hostport, temp_file);

	/* connection agent */
	ma = ma_connect_hostoid(dnmasteroid);
	if (!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		ereport(ERROR, (errmsg("could not connect socket for agent \"%s\".",
						get_hostname_from_hostoid(dnmasteroid))));
		return;
	}

	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, AGT_CMD_PGDUMPALL);
	ma_sendstring(&buf, pg_dumpall_cmd.data);
	pfree(pg_dumpall_cmd.data);
	ma_endmessage(&buf, ma);

	if (! ma_flush(ma, true))
	{
		getAgentCmdRst.ret = false;
		appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}

	/*check the receive msg*/
	mgr_recv_msg(ma, &getAgentCmdRst);
	ma_close(ma);
	pfree(hostaddr);

	if (!getAgentCmdRst.ret)
		ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));
}

static bool mgr_get_active_hostoid_and_port(char node_type, Oid *hostoid, int32 *hostport, AppendNodeInfo *appendnodeinfo, bool set_ip)
{
	InitNodeInfo *info;
	ScanKeyData key[3];
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	char * host;
	char *user;
	char coordportstr[19];
	bool isNull;
	bool bget = false;
	Datum datumPath;
	GetAgentCmdRst getAgentCmdRst;
	StringInfoData  infosendmsg;

	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodetype
				,BTEqualStrategyNumber
				,F_CHAREQ
				,CharGetDatum(node_type));
	ScanKeyInit(&key[1]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	ScanKeyInit(&key[2]
				,Anum_mgr_node_nodezone
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,CStringGetDatum(mgr_zone));
	info = palloc(sizeof(*info));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);
	info->rel_scan = heap_beginscan_catalog(info->rel_node, 3, key);
	info->lcp =NULL;

	while((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		/* check the coordinator active */
		sprintf(coordportstr, "%d", mgr_node->nodeport);
		host = get_hostaddress_from_hostoid(mgr_node->nodehost);
		user = get_hostuser_from_hostoid(mgr_node->nodehost);
		if(PQPING_OK != pingNode_user(host, coordportstr, user))
		{
			if (host)
				pfree(host);
			pfree(user);
			continue;
		}
		pfree(user);
		pfree(host);
		bget = true;
		break;
	}
	if (!bget)
	{
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, AccessShareLock);
		pfree(info);
		return false;
	}

	if (hostoid)
		*hostoid = mgr_node->nodehost;
	if (hostport)
		*hostport = mgr_node->nodeport;

	if ((node_type == CNDN_TYPE_DATANODE_MASTER || node_type == CNDN_TYPE_COORDINATOR_MASTER) && set_ip)
	{
		/*get nodepath from tuple*/
		datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(info->rel_node), &isNull);
		if (isNull)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
				, errmsg("column nodepath is null")));
		}
		initStringInfo(&infosendmsg);
		initStringInfo(&(getAgentCmdRst.description));
		getAgentCmdRst.ret = false;
		mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", "all", appendnodeinfo->nodeaddr,
										32, "trust", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
								TextDatumGetCString(datumPath),
								&infosendmsg,
								mgr_node->nodehost,
								&getAgentCmdRst);
		pfree(infosendmsg.data);
		if (!getAgentCmdRst.ret)
		{
			heap_endscan(info->rel_scan);
			heap_close(info->rel_node, AccessShareLock);
			pfree(info);
			ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));
		}
		pfree(getAgentCmdRst.description.data);
		mgr_reload_conf(mgr_node->nodehost, TextDatumGetCString(datumPath));
	}
	else
	{
		/*do nothing*/
	}

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);

	return true;
}

static void mgr_get_other_parm(char node_type, StringInfo infosendmsg)
{
	mgr_append_pgconf_paras_str_str("synchronous_commit", "on", infosendmsg);
	mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", infosendmsg);
	mgr_append_pgconf_paras_str_int("max_wal_senders", MAX_WAL_SENDERS_NUM, infosendmsg);
	mgr_append_pgconf_paras_str_int("wal_keep_segments", WAL_KEEP_SEGMENTS_NUM, infosendmsg);
	mgr_append_pgconf_paras_str_str("wal_level", WAL_LEVEL_MODE, infosendmsg);
	mgr_append_pgconf_paras_str_quotastr("listen_addresses", "*", infosendmsg);
	mgr_append_pgconf_paras_str_int("max_prepared_transactions", MAX_PREPARED_TRANSACTIONS_DEFAULT, infosendmsg);
	mgr_append_pgconf_paras_str_quotastr("log_destination", "csvlog", infosendmsg);
	mgr_append_pgconf_paras_str_str("logging_collector", "on", infosendmsg);
	mgr_append_pgconf_paras_str_quotastr("log_directory", "pg_log", infosendmsg);
}

static void mgr_get_appendnodeinfo(char node_type, char *nodename, AppendNodeInfo *appendnodeinfo)
{
	InitNodeInfo *info;
	ScanKeyData key[4];
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	Datum datumPath;
	bool isNull = false;
	NameData nodenameData;

	Assert(nodename);
	namestrcpy(&nodenameData, nodename);
	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodename
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,NameGetDatum(&nodenameData));

	ScanKeyInit(&key[1]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(false));

	ScanKeyInit(&key[2]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(false));

	ScanKeyInit(&key[3]
				,Anum_mgr_node_nodetype
				,BTEqualStrategyNumber
				,F_CHAREQ
				,CharGetDatum(node_type));


	info = (InitNodeInfo *)palloc0(sizeof(InitNodeInfo));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);
	info->rel_scan = heap_beginscan_catalog(info->rel_node, 4, key);
	info->lcp =NULL;

	if ((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) == NULL)
	{
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, AccessShareLock);
		pfree(info);
		ereport(ERROR, (errmsg("%s \"%s\" does not exist", mgr_nodetype_str(node_type), nodename)));
	}

	mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
	Assert(mgr_node);

	appendnodeinfo->nodename = pstrdup(NameStr(mgr_node->nodename));
	appendnodeinfo->nodetype = mgr_node->nodetype;
	appendnodeinfo->nodeaddr = get_hostaddress_from_hostoid(mgr_node->nodehost);
	appendnodeinfo->nodeusername = get_hostuser_from_hostoid(mgr_node->nodehost);
	appendnodeinfo->nodeport = mgr_node->nodeport;
	appendnodeinfo->nodehost = mgr_node->nodehost;
	appendnodeinfo->nodemasteroid = mgr_node->nodemasternameoid;
	appendnodeinfo->tupleoid = HeapTupleGetOid(tuple);
	namestrcpy(&(appendnodeinfo->sync_state), NameStr(mgr_node->nodesync));
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
	appendnodeinfo->nodepath = pstrdup(TextDatumGetCString(datumPath));

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
}

void mgr_check_dir_exist_and_priv(Oid hostoid, char *dir)
{
	StringInfoData strinfo;
	char cmdtype = AGT_CMD_CHECK_DIR_EXIST;
	bool res = false;

	initStringInfo(&strinfo);
	res = mgr_ma_send_cmd(cmdtype, dir, hostoid, &strinfo);

	if (!res)
		ereport(ERROR, (errmsg("%s", strinfo.data)));
	pfree(strinfo.data);

	return;
}

static void mgr_append_init_cndnmaster(AppendNodeInfo *appendnodeinfo)
{
	StringInfoData  infosendmsg;
	StringInfoData strinfo;
	char cmdtype = AGT_CMD_CNDN_CNDN_INIT;
	bool res = false;

	initStringInfo(&infosendmsg);

	/*init datanode*/
	appendStringInfo(&infosendmsg, " -D %s", appendnodeinfo->nodepath);
	if (with_data_checksums)
		appendStringInfo(&infosendmsg, " --nodename %s -E UTF8 --locale=C -k", appendnodeinfo->nodename);
	else
		appendStringInfo(&infosendmsg, " --nodename %s -E UTF8 --locale=C", appendnodeinfo->nodename);
	initStringInfo(&strinfo);
	res = mgr_ma_send_cmd(cmdtype, infosendmsg.data, appendnodeinfo->nodehost, &strinfo);
	pfree(infosendmsg.data);

	if (!res)
		ereport(ERROR, (errmsg("%s", strinfo.data)));
	pfree(strinfo.data);
}

/*
* failover datanode
* this function is deprecated, please see function "mgr_failover_one_dn()"
*/
Datum mgr_failover_one_dn_deprecated(PG_FUNCTION_ARGS)
{
	Relation relNode;
	char *nodename;
	bool force_get;
	bool force = false;
	int pingres = PQPING_NO_RESPONSE;
	Oid masterTupleOid;
	HeapTuple masterTuple;
	HeapTuple slaveTuple;
	HeapTuple tup_result;
	Form_mgr_node mgr_node;
	NameData slaveNodeName;
	GetAgentCmdRst getAgentCmdRst;

	nodename = PG_GETARG_CSTRING(0);
	force_get = PG_GETARG_BOOL(1);

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot assign TransactionIds during recovery")));

	//mgr_make_sure_all_running(CNDN_TYPE_GTM_COOR_MASTER);
	mgr_make_sure_all_running(CNDN_TYPE_COORDINATOR_MASTER);

	if(force_get)
		force = true;
	relNode = heap_open(NodeRelationId, RowExclusiveLock);
	initStringInfo(&(getAgentCmdRst.description));

	PG_TRY();
	{
		/* check the datanode master exist */
		masterTuple = mgr_get_tuple_node_from_name_type(relNode, nodename);
		if(!HeapTupleIsValid(masterTuple))
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					,errmsg("datanode master \"%s\" does not exist", nodename)));
		}
		mgr_node = (Form_mgr_node)GETSTRUCT(masterTuple);
		Assert(mgr_node);
		if (!mgr_node->nodeincluster)
		{
			heap_freetuple(masterTuple);
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				,errmsg("datanode master \"%s\" does not exist in cluster", nodename)));
		}
		if (mgr_node->nodetype != CNDN_TYPE_DATANODE_MASTER)
		{
			heap_freetuple(masterTuple);
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				,errmsg("the type of node \"%s\" is not datanode master", nodename)));
		}
		specHostOid = mgr_node->nodehost;
		masterTupleOid = HeapTupleGetOid(masterTuple);
		heap_freetuple(masterTuple);
		/* check the datanode master has sync slave node */
		pingres = mgr_get_normal_slave_node(relNode, masterTupleOid, SYNC_STATE_SYNC, InvalidOid, &slaveNodeName);
		if (!force)
		{
			if (pingres == PQPING_OK)
			{} /*do nothing */
			else if (pingres == AGENT_DOWN)
				ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					,errmsg("some agents could not be connected and cannot find any running normal synchronous slave node for datanode master \"%s\"", nodename)
					,errhint("try \"monitor agent all;\" to check agents status")));
			else
				ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					,errmsg("datanode master \"%s\" does not have running normal synchronous slave node", nodename)
					,errhint("if the master has one normal asynchronous slave node and you want to promote it to master, execute \"FAILOVER DATANODE %s FORCE\" to force promote the slave node to master", nodename)));
		}
		else
		{
			if (pingres != PQPING_OK)
			{
				pingres = mgr_get_normal_slave_node(relNode, masterTupleOid, SYNC_STATE_POTENTIAL, InvalidOid, &slaveNodeName);
				if (pingres != PQPING_OK)
					pingres = mgr_get_normal_slave_node(relNode, masterTupleOid, SYNC_STATE_ASYNC, InvalidOid, &slaveNodeName);
				if (pingres != PQPING_OK)
				{
					if (pingres == AGENT_DOWN)
						ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
							,errmsg("some agents could not be connected and cannot find any running normal slave node for datanode master \"%s\"", nodename)
							,errhint("try \"monitor agent all;\" to check agents status")));
					else
						ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
							,errmsg("datanode master \"%s\" does not have one normal slave node", nodename)));
				}
			}
		}

		slaveTuple = mgr_get_tuple_node_from_name_type(relNode, slaveNodeName.data);
		mgr_runmode_cndn_get_result(AGT_CMD_DN_FAILOVER, &getAgentCmdRst, relNode, slaveTuple, TAKEPLAPARM_N);
		heap_freetuple(slaveTuple);

		tup_result = build_common_command_tuple(
			&(slaveNodeName)
			, getAgentCmdRst.ret
			, getAgentCmdRst.description.data);
		ereport(LOG, (errmsg("the command for failover:\nresult is: %s\ndescription is: %s\n", getAgentCmdRst.ret == true ? "true" : "false", getAgentCmdRst.description.data)));
	}PG_CATCH();
	{
		heap_close(relNode, RowExclusiveLock);
		pfree(getAgentCmdRst.description.data);
		PG_RE_THROW();
	}PG_END_TRY();

	heap_close(relNode, RowExclusiveLock);
	pfree(getAgentCmdRst.description.data);

	return HeapTupleGetDatum(tup_result);
}

static void mgr_construct_add_coornode(InitNodeInfo *info_in, const char nodetype, 
		Form_mgr_node mgr_node_out, StringInfoData *cmdstring)
{
	ScanKeyData key_in[1];
	HeapTuple tuple_in;
	char *address = NULL;
	bool is_gtm = false;
	Form_mgr_node mgr_node_in;

	info_in->rel_node = heap_open(NodeRelationId, AccessShareLock);
	ScanKeyInit(&key_in[0]
				,Anum_mgr_node_nodetype
				,BTEqualStrategyNumber
				,F_CHAREQ
				,CharGetDatum(nodetype));
	info_in->rel_scan = heap_beginscan_catalog(info_in->rel_node, 1, key_in);
	info_in->lcp =NULL;

	while ((tuple_in = heap_getnext(info_in->rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node_in = (Form_mgr_node)GETSTRUCT(tuple_in);
		Assert(mgr_node_in);

		if (mgr_node_in->nodetype == CNDN_TYPE_GTM_COOR_MASTER)
			is_gtm = true;
		else
			is_gtm = false;

		address = get_hostaddress_from_hostoid(mgr_node_in->nodehost);
		if (strcmp(NameStr(mgr_node_in->nodename), NameStr(mgr_node_out->nodename)) == 0)
		{
			appendStringInfo(cmdstring, "ALTER NODE \\\"%s\\\" WITH (HOST='%s', PORT=%d, GTM=%d);"
							,NameStr(mgr_node_in->nodename)
							,address
							,mgr_node_in->nodeport
							,is_gtm);
		}
		else
		{
			appendStringInfo(cmdstring, " CREATE NODE \\\"%s\\\" WITH (TYPE='coordinator', HOST='%s', PORT=%d, GTM=%d);"
							,NameStr(mgr_node_in->nodename)
							,address
							,mgr_node_in->nodeport
							,is_gtm);
		}
		pfree(address);
	}
	heap_endscan(info_in->rel_scan);
	heap_close(info_in->rel_node, AccessShareLock);
}

/*
 * last step for init all
 * we need cofigure all nodes information to pgxc_node table
 */
Datum mgr_configure_nodes_all(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	InitNodeInfo *info_out, *info_in;
	HeapTuple tuple_out, tuple_in, tup_result;
	Form_mgr_node mgr_node_out, mgr_node_in;
	GetAgentCmdRst getAgentCmdRst;
	StringInfoData cmdstring;
	StringInfoData buf;
	ManagerAgent *ma;
	bool execRes = false;
	char *address = NULL;
	char *cnAddress = NULL;
	char *cnUser = NULL;

	bool is_preferred = false;
	bool is_primary = false;
	bool find_preferred = false;
	struct tuple_cndn *prefer_cndn;
	ListCell *cn_lc, *dn_lc;
	HeapTuple tuple_primary = NULL;
	HeapTuple tuple_preferred = NULL;
	int coordinator_num = 0, datanode_num = 0;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		/* check the number of coordinator */
		if (mgr_get_nodetype_num(CNDN_TYPE_GTM_COOR_MASTER, false, false) < 1)
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					, errmsg("there is not any gtmcoord in the node table")));
		pg_usleep(3000000L);
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		info_out = palloc(sizeof(*info_out));
		info_out->rel_node = heap_open(NodeRelationId, AccessShareLock);
		info_out->rel_scan = heap_beginscan_catalog(info_out->rel_node, 0, NULL);
		info_out->lcp = NULL;

		/* save info */
		funcctx->user_fctx = info_out;
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	Assert(funcctx);
	info_out = funcctx->user_fctx;
	Assert(info_out);

	while((tuple_out = heap_getnext(info_out->rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node_out = (Form_mgr_node)GETSTRUCT(tuple_out);
		Assert(mgr_node_out);

		if (CNDN_TYPE_GTM_COOR_MASTER != mgr_node_out->nodetype && CNDN_TYPE_COORDINATOR_MASTER != mgr_node_out->nodetype)
			continue;

		initStringInfo(&(getAgentCmdRst.description));
		namestrcpy(&(getAgentCmdRst.nodename), NameStr(mgr_node_out->nodename));
		//getAgentCmdRst.nodename = get_hostname_from_hostoid(mgr_node_out->nodehost);

		initStringInfo(&cmdstring);
		cnAddress = get_hostaddress_from_hostoid(mgr_node_out->nodehost);
		cnUser = get_hostuser_from_hostoid(mgr_node_out->nodehost);
		appendStringInfo(&cmdstring, " -h %s -p %u -d %s -U %s -a -c \""
						,cnAddress
						,mgr_node_out->nodeport
						,DEFAULT_DB
						,cnUser);

		info_in = palloc(sizeof(*info_in));
		mgr_construct_add_coornode(info_in, CNDN_TYPE_GTM_COOR_MASTER, mgr_node_out, &cmdstring);
		mgr_construct_add_coornode(info_in, CNDN_TYPE_COORDINATOR_MASTER, mgr_node_out, &cmdstring);
		pfree(info_in);

		prefer_cndn = get_new_pgxc_node(PGXC_CONFIG, NULL, 0);

		if(PointerIsValid(prefer_cndn->coordiantor_list))
			coordinator_num = prefer_cndn->coordiantor_list->length;
		if(PointerIsValid(prefer_cndn->datanode_list))
			datanode_num = prefer_cndn->datanode_list->length;

		/*get the datanode of primary in the pgxc_node*/
		if(coordinator_num < datanode_num)
		{
			dn_lc = list_tail(prefer_cndn->datanode_list);
			tuple_primary = (HeapTuple)lfirst(dn_lc);
		}
		else if(datanode_num >0)
		{
			dn_lc = list_head(prefer_cndn->datanode_list);
			tuple_primary = (HeapTuple)lfirst(dn_lc);
		}
		/*get the datanode of preferred in the pgxc_node*/
		forboth(cn_lc, prefer_cndn->coordiantor_list, dn_lc, prefer_cndn->datanode_list)
		{
			tuple_in = (HeapTuple)lfirst(cn_lc);
			if(HeapTupleGetOid(tuple_out) == HeapTupleGetOid(tuple_in))
			{
				tuple_preferred = (HeapTuple)lfirst(dn_lc);
				find_preferred = true;
				break;
			}
		}
		/*send msg to the coordinator and set pgxc_node*/

		foreach(dn_lc, prefer_cndn->datanode_list)
		{
			tuple_in = (HeapTuple)lfirst(dn_lc);
			mgr_node_in = (Form_mgr_node)GETSTRUCT(tuple_in);
			Assert(mgr_node_in);
			address = get_hostaddress_from_hostoid(mgr_node_in->nodehost);
			if(true == find_preferred)
			{
				if(HeapTupleGetOid(tuple_preferred) == HeapTupleGetOid(tuple_in))
					is_preferred = true;
				else
					is_preferred = false;
			}
			else
			{
				is_preferred = false;
			}
			if(HeapTupleGetOid(tuple_primary) == HeapTupleGetOid(tuple_in))
			{
				is_primary = true;
			}
			else
			{
				is_primary = false;
			}
			appendStringInfo(&cmdstring, "create node \\\"%s\\\" with(type='datanode', host='%s', port=%d , primary = %s, preferred = %s);"
									,NameStr(mgr_node_in->nodename)
									,address
									,mgr_node_in->nodeport
									,true == is_primary ? "true":"false"
									,true == is_preferred ? "true":"false");
			pfree(address);
		}

		foreach(cn_lc, prefer_cndn->coordiantor_list)
		{
			heap_freetuple((HeapTuple)lfirst(cn_lc));
		}
		foreach(dn_lc, prefer_cndn->datanode_list)
		{
			heap_freetuple((HeapTuple)lfirst(dn_lc));
		}
		if(PointerIsValid(prefer_cndn->coordiantor_list))
			list_free(prefer_cndn->coordiantor_list);
		if(PointerIsValid(prefer_cndn->datanode_list))
			list_free(prefer_cndn->datanode_list);
		pfree(prefer_cndn);

		appendStringInfoString(&cmdstring, "\"");

		/* connection agent */
		getAgentCmdRst.ret = false;
		ma = ma_connect_hostoid(mgr_node_out->nodehost);
		if(!ma_isconnected(ma))
		{
			/* report error message */
			appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
			goto func_end;
		}

		ma_beginmessage(&buf, AGT_MSG_COMMAND);
		ma_sendbyte(&buf, AGT_CMD_PSQL_CMD);
		ma_sendstring(&buf,cmdstring.data);
		ma_endmessage(&buf, ma);
		if (! ma_flush(ma, true))
		{
			appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
			goto func_end;
		}

		/*check the receive msg*/
		execRes = mgr_recv_msg_original_result(ma, &getAgentCmdRst, AGENT_RESULT_DEBUG);
		if (!execRes)
		{
			ereport(WARNING, (errmsg("config all, create node on all coordinators fail %s",
					getAgentCmdRst.description.data)));
			goto func_end;
		}
		ma_close(ma);
		/* pgxc pool reload */
		resetStringInfo(&cmdstring);
		getAgentCmdRst.ret = false;
		appendStringInfo(&cmdstring, " -h %s -p %u -d %s -U %s -a -c \""
							,cnAddress
							,mgr_node_out->nodeport
							,DEFAULT_DB
							,cnUser);
		appendStringInfoString(&cmdstring, "select pgxc_pool_reload();\"");
		ma = ma_connect_hostoid(mgr_node_out->nodehost);
		if(!ma_isconnected(ma))
		{
			/* report error message */
			appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
			goto func_end;
		}
		ma_beginmessage(&buf, AGT_MSG_COMMAND);
		ma_sendbyte(&buf, AGT_CMD_PSQL_CMD);
		ma_sendstring(&buf,cmdstring.data);

		ma_endmessage(&buf, ma);
		if (! ma_flush(ma, true))
		{
			appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
			goto func_end;
		}
		/* check the receive msg */
		execRes = mgr_recv_msg_original_result(ma, &getAgentCmdRst, AGENT_RESULT_DEBUG);
		if (!execRes)
			ereport(WARNING, (errmsg("config all, select pgxc_pool_reload() fail %s",
			getAgentCmdRst.description.data)));

		func_end:
			tup_result = build_common_command_tuple( &(getAgentCmdRst.nodename)
					,getAgentCmdRst.ret
					,getAgentCmdRst.ret == true ? "success":getAgentCmdRst.description.data);

		ma_close(ma);
		pfree(cnAddress);
		pfree(cnUser);
		pfree(cmdstring.data);
		pfree(getAgentCmdRst.description.data);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));

	}
	/* end of row */

	/*mark the tuple in node systbl is in cluster*/
	mgr_mark_node_in_cluster(info_out->rel_node);
	heap_endscan(info_out->rel_scan);
	heap_close(info_out->rel_node, AccessShareLock);
	pfree(info_out);

	/*set gtm or datanode master synchronous_standby_names*/
	mgr_set_master_sync();

	/*add content of hba table to the pg_hba.conf file ,the "*" is meaning all*/
	add_hba_table_to_file("*");
	/* get the content from coordinator and gtm, then insert into mgr_parm which used
	* to check set parameters
	*/
	mgr_flushparam(NULL, NULL, NULL);
	SRF_RETURN_DONE(funcctx);
}

/*
* send paramters for postgresql.conf which need refresh to agent
* datapath: the absolute path for postgresql.conf
* infosendmsg: which include the paramters and its values, the interval is '\0', the two bytes of string are two '\0'
* hostoid: the hostoid which agent it need send
* getAgentCmdRst: the execute result in it
*/
void mgr_send_conf_parameters(char filetype, char *datapath, StringInfo infosendmsg, Oid hostoid, GetAgentCmdRst *getAgentCmdRst)
{
	ManagerAgent *ma;
	StringInfoData sendstrmsg;
	StringInfoData buf;

	initStringInfo(&sendstrmsg);
	appendStringInfoString(&sendstrmsg, datapath);
	appendStringInfoCharMacro(&sendstrmsg, '\0');
	mgr_append_infostr_infostr(&sendstrmsg, infosendmsg);
	ma = ma_connect_hostoid(hostoid);
	if(!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst->ret = false;
		appendStringInfoString(&(getAgentCmdRst->description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}
	getAgentCmdRst->ret = false;
	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, filetype);
	mgr_append_infostr_infostr(&buf, &sendstrmsg);
	pfree(sendstrmsg.data);
	ma_endmessage(&buf, ma);
	if (! ma_flush(ma, true))
	{
		getAgentCmdRst->ret = false;
		appendStringInfoString(&(getAgentCmdRst->description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}
	/*check the receive msg*/
	mgr_recv_msg(ma, getAgentCmdRst);
	ma_close(ma);
}

/*
* add key value to infosendmsg, use '\0' to interval, both the key value the type are char*
*/
void mgr_append_pgconf_paras_str_str(char *key, char *value, StringInfo infosendmsg)
{
	Assert(key != '\0' && value != '\0' && &(infosendmsg->data) != '\0');
	appendStringInfoString(infosendmsg, key);
	appendStringInfoCharMacro(infosendmsg, '\0');
	appendStringInfoString(infosendmsg, value);
	appendStringInfoCharMacro(infosendmsg, '\0');
}

/*
* add key value to infosendmsg, use '\0' to interval, the type of key is char*, the type of value is int
*/
void mgr_append_pgconf_paras_str_int(char *key, int value, StringInfo infosendmsg)
{
	Assert(key != '\0' && value != '\0' && &(infosendmsg->data) != '\0');
	appendStringInfoString(infosendmsg, key);
	appendStringInfoCharMacro(infosendmsg, '\0');
	appendStringInfo(infosendmsg, "%d", value);
	appendStringInfoCharMacro(infosendmsg, '\0');
}

/*
* add key value to infosendmsg, use '\0' to interval, both the key value the type are char* and need in quota
*/
void mgr_append_pgconf_paras_str_quotastr(char *key, char *value, StringInfo infosendmsg)
{
	Assert(key != '\0' && value != '\0' && &(infosendmsg->data) != '\0');
	appendStringInfoString(infosendmsg, key);
	appendStringInfoCharMacro(infosendmsg, '\0');
	appendStringInfo(infosendmsg, "'%s'", value);
	appendStringInfoCharMacro(infosendmsg, '\0');
}

/*
* add the content of sourceinfostr to infostr, the string in sourceinfostr use '\0' to interval
*/
void mgr_append_infostr_infostr(StringInfo infostr, StringInfo sourceinfostr)
{
	int len = 0;
	char *ptmp = sourceinfostr->data;
	while(*ptmp != '\0')
	{
		appendStringInfoString(infostr, ptmp);
		appendStringInfoCharMacro(infostr, '\0');
		len = strlen(ptmp);
		ptmp = ptmp + len + 1;
	}
}

/*
* the parameters which need refresh for postgresql.conf
*/
void mgr_add_parameters_pgsqlconf(Oid tupleOid, char nodetype, int cndnport, StringInfo infosendparamsg)
{
	if(nodetype == CNDN_TYPE_DATANODE_SLAVE || nodetype == CNDN_TYPE_GTM_COOR_SLAVE)
	{
		mgr_append_pgconf_paras_str_str("hot_standby", "on", infosendparamsg);
	}
	mgr_append_pgconf_paras_str_str("synchronous_commit", "on", infosendparamsg);
	mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", infosendparamsg);
	mgr_append_pgconf_paras_str_int("max_wal_senders", MAX_WAL_SENDERS_NUM, infosendparamsg);
	mgr_append_pgconf_paras_str_int("wal_keep_segments", WAL_KEEP_SEGMENTS_NUM, infosendparamsg);
	mgr_append_pgconf_paras_str_str("wal_level", WAL_LEVEL_MODE, infosendparamsg);
	mgr_append_pgconf_paras_str_int("port", cndnport, infosendparamsg);
	mgr_append_pgconf_paras_str_quotastr("listen_addresses", "*", infosendparamsg);
	mgr_append_pgconf_paras_str_int("max_prepared_transactions", MAX_PREPARED_TRANSACTIONS_DEFAULT, infosendparamsg);
	mgr_append_pgconf_paras_str_quotastr("log_destination", "csvlog", infosendparamsg);
	mgr_append_pgconf_paras_str_str("logging_collector", "on", infosendparamsg);
	mgr_append_pgconf_paras_str_quotastr("log_directory", "pg_log", infosendparamsg);
	if(nodetype == CNDN_TYPE_DATANODE_MASTER
		|| nodetype == CNDN_TYPE_DATANODE_SLAVE
		|| nodetype == CNDN_TYPE_COORDINATOR_MASTER
		|| nodetype == CNDN_TYPE_GTM_COOR_MASTER)
	{
		mgr_get_gtm_host_snapsender_gxidsender_port(infosendparamsg);
	}
}

/*
* the parameters which need refresh for recovery.conf
*/
void mgr_add_parameters_recoveryconf(char nodetype, char *slavename, Oid tupleoid, StringInfo infosendparamsg)
{
	Form_mgr_node mgr_node;
	Form_mgr_host mgr_host;
	HeapTuple mastertuple;
	HeapTuple tup;
	int32 masterport;
	Oid masterhostOid;
	char *masterhostaddress;
	NameData username;
	StringInfoData primary_conninfo_value;

	/*get the master port, master host address*/
	mastertuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(tupleoid));
	if(!HeapTupleIsValid(mastertuple))
	{
		ereport(ERROR, (errmsg("node oid \"%u\" not exist", tupleoid)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
			, errcode(ERRCODE_INTERNAL_ERROR)));
	}
	mgr_node = (Form_mgr_node)GETSTRUCT(mastertuple);
	Assert(mastertuple);
	masterport = mgr_node->nodeport;
	masterhostOid = mgr_node->nodehost;
	masterhostaddress = get_hostaddress_from_hostoid(masterhostOid);
	ReleaseSysCache(mastertuple);

	/*get host user from system: host*/
	tup = SearchSysCache1(HOSTHOSTOID, ObjectIdGetDatum(masterhostOid));
	if(!(HeapTupleIsValid(tup)))
	{
		ereport(ERROR, (errmsg("host oid \"%u\" not exist", masterhostOid)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
			, errcode(ERRCODE_UNDEFINED_OBJECT)));
	}
	mgr_host= (Form_mgr_host)GETSTRUCT(tup);
	Assert(mgr_host);
	namestrcpy(&username, NameStr(mgr_host->hostuser));
	
	ReleaseSysCache(tup);

	/*primary_conninfo*/
	initStringInfo(&primary_conninfo_value);
	appendStringInfo(&primary_conninfo_value, "host=%s port=%d user=%s application_name=%s", masterhostaddress, masterport, username.data, slavename);
	mgr_append_pgconf_paras_str_str("recovery_target_timeline", "latest", infosendparamsg);
	mgr_append_pgconf_paras_str_str("standby_mode", "on", infosendparamsg);
	mgr_append_pgconf_paras_str_quotastr("primary_conninfo", primary_conninfo_value.data, infosendparamsg);
	pfree(primary_conninfo_value.data);
	pfree(masterhostaddress);
}

/*
* the parameters which need refresh for pg_hba.conf
* gtm : include all gtm master/slave ip and all coordinators ip and datanode masters/slave ip
*        replication include slave ip
* coordinator: include all coordinators ip
* datanode: include all coordinators ip, replication include slave ip
*/
void mgr_add_parameters_hbaconf(Oid mastertupleoid, char nodetype, StringInfo infosendhbamsg)
{
	Relation rel_node;
	HeapScanDesc rel_scan;
	char *cnuser;
	char *cnaddress;
	Form_mgr_node mgr_node;
	HeapTuple tuple;
	ScanKeyData key[1];
	NameData self_address;
	bool bgetAddress;

	rel_node = heap_open(NodeRelationId, AccessShareLock);
	/*get all coordinator master ip*/
	if (CNDN_TYPE_COORDINATOR_MASTER == nodetype)
	{
		ScanKeyInit(&key[0]
				,Anum_mgr_node_nodezone
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,CStringGetDatum(mgr_zone));
		rel_scan = heap_beginscan_catalog(rel_node, 1, key);
		while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			/*get address*/
			cnaddress = get_hostaddress_from_hostoid(mgr_node->nodehost);
			if (CNDN_TYPE_COORDINATOR_MASTER == mgr_node->nodetype || 
				CNDN_TYPE_GTM_COOR_MASTER == mgr_node->nodetype ||
				CNDN_TYPE_GTM_COOR_SLAVE == mgr_node->nodetype)
				mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", "all", cnaddress, 32, "trust", infosendhbamsg);
			pfree(cnaddress);
		}
		heap_endscan(rel_scan);
	}
	else
	{
		/*for datanode or gtm replication*/
		rel_scan = heap_beginscan_catalog(rel_node, 0, NULL);
		while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			if (mastertupleoid != 0 && (mastertupleoid == mgr_node->nodemasternameoid))
			{
				/*database user*/
				cnuser = get_hostuser_from_hostoid(mgr_node->nodehost);
				/*get address*/
				cnaddress = get_hostaddress_from_hostoid(mgr_node->nodehost);
				mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "replication", cnuser, cnaddress, 32, "trust", infosendhbamsg);
				pfree(cnuser);
				pfree(cnaddress);
			}
		}
		heap_endscan(rel_scan);
		/*for allow connect*/
		rel_scan = heap_beginscan_catalog(rel_node, 0, NULL);
		while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			cnaddress = get_hostaddress_from_hostoid(mgr_node->nodehost);
			if (CNDN_TYPE_GTM_COOR_MASTER == nodetype || CNDN_TYPE_GTM_COOR_SLAVE == nodetype)
			{
				if (mgr_node->nodetype != CNDN_TYPE_GTM_COOR_SLAVE)
					mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", "all", cnaddress, 32, "trust", infosendhbamsg);
			}
			else
			{
				if (CNDN_TYPE_COORDINATOR_MASTER == mgr_node->nodetype ||
					CNDN_TYPE_GTM_COOR_MASTER == mgr_node->nodetype ||
					CNDN_TYPE_GTM_COOR_SLAVE == mgr_node->nodetype)
					mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", "all", cnaddress, 32, "trust", infosendhbamsg);
			}
			pfree(cnaddress);
		}
		heap_endscan(rel_scan);
	}
	/*get the adbmanager ip*/
	memset(self_address.data, 0, NAMEDATALEN);
	bgetAddress = get_local_ip(&self_address);
	if (!bgetAddress)
	{
		ereport(ERROR, (errmsg("get adb manager local ip.")));
	}
	mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", "all", NameStr(self_address), 32, "trust", infosendhbamsg);
	heap_close(rel_node, AccessShareLock);
}
/*
* add one line content to infosendhbamsg, which will send to agent to refresh pg_hba.conf, the word in this line interval by '\0',donot change the order
*/
void mgr_add_oneline_info_pghbaconf(int type, char *database, char *user, char *addr, int addr_mark, char *auth_method, StringInfo infosendhbamsg)
{
	appendStringInfo(infosendhbamsg, "%c", type);
	appendStringInfoCharMacro(infosendhbamsg, '\0');
	appendStringInfoString(infosendhbamsg, database);
	appendStringInfoCharMacro(infosendhbamsg, '\0');
	appendStringInfoString(infosendhbamsg, user);
	appendStringInfoCharMacro(infosendhbamsg, '\0');
	appendStringInfoString(infosendhbamsg, addr);
	appendStringInfoCharMacro(infosendhbamsg, '\0');
	appendStringInfo(infosendhbamsg, "%d", addr_mark);
	appendStringInfoCharMacro(infosendhbamsg, '\0');
	appendStringInfoString(infosendhbamsg, auth_method);
	appendStringInfoCharMacro(infosendhbamsg, '\0');
}

/*
* give nodename, nodetype to get tuple from node systbl,
*/
HeapTuple mgr_get_tuple_node_from_name_type(Relation rel, char *nodename)
{
	ScanKeyData key[1];
	HeapScanDesc rel_scan;
	HeapTuple tuple =NULL;
	HeapTuple tupleret = NULL;
	NameData nameattrdata;

	Assert(nodename);
	namestrcpy(&nameattrdata, nodename);
	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodename
		,BTEqualStrategyNumber, F_NAMEEQ
		,NameGetDatum(&nameattrdata));
	rel_scan = heap_beginscan_catalog(rel, 1, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		break;
	}
	tupleret = heap_copytuple(tuple);
	heap_endscan(rel_scan);
	return tupleret;
}

/*mark the node in node systbl is in cluster*/
void mgr_mark_node_in_cluster(Relation rel)
{
	HeapScanDesc rel_scan;
	Form_mgr_node mgr_node;
	HeapTuple tuple;

	rel_scan = heap_beginscan_catalog(rel, 0, NULL);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if (mgr_node->nodeinited)
		{
			mgr_node->nodeincluster = true;
			heap_inplace_update(rel, tuple);
		}
	}
	heap_endscan(rel_scan);
}

/*
* gtm failover
*/
Datum mgr_failover_gtm_deprecated(PG_FUNCTION_ARGS)
{
	Relation relNode;
	char *nodename;
	bool force_get;
	bool force = false;
	int pingres = PQPING_NO_RESPONSE;
	Oid masterTupleOid;
	HeapTuple masterTuple;
	HeapTuple slaveTuple;
	HeapTuple tup_result;
	Form_mgr_node mgr_node;
	NameData slaveNodeName;
	GetAgentCmdRst getAgentCmdRst;

	nodename = PG_GETARG_CSTRING(0);
	force_get = PG_GETARG_BOOL(1);

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot assign TransactionIds during recovery")));

	if(force_get)
		force = true;

	ereport(LOG, (errmsg("check gtm slave status in failover cmd start")));

	relNode = heap_open(NodeRelationId, RowExclusiveLock);
	PG_TRY();
	{
		/* check the gtm master exist */
		masterTuple = mgr_get_tuple_node_from_name_type(relNode, nodename);
		if(!HeapTupleIsValid(masterTuple))
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					,errmsg("gtm master \"%s\" does not exist", nodename)));
		}
		mgr_node = (Form_mgr_node)GETSTRUCT(masterTuple);
		Assert(mgr_node);
		if (!mgr_node->nodeincluster)
		{
			heap_freetuple(masterTuple);
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				,errmsg("gtm master \"%s\" does not exist in cluster", nodename)));
		}
		if (mgr_node->nodetype != CNDN_TYPE_GTM_COOR_MASTER)
		{
			heap_freetuple(masterTuple);
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				,errmsg("the type of node \"%s\" is not gtm master", nodename)));
		}

		masterTupleOid = HeapTupleGetOid(masterTuple);
		heap_freetuple(masterTuple);
		/* check the gtm master has sync slave node */
		pingres = mgr_get_normal_slave_node(relNode, masterTupleOid, SYNC_STATE_SYNC, InvalidOid, &slaveNodeName);
		if (!force)
		{
			if (pingres == PQPING_OK)
			{} /*do nothing */
			else if (pingres == AGENT_DOWN)
				ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					,errmsg("some agents could not be connected and cannot find any running normal synchronous slave node for gtmcoord master \"%s\"", nodename)
					,errhint("try \"monitor agent all;\" to check agents status")));
			else
				ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					,errmsg("gtm master \"%s\" does not have running normal synchronous slave node", nodename)
					,errhint("if the master has one normal asynchronous slave node and you want to promote it to master, execute \"FAILOVER GTMCOORD %s FORCE\" to force promote the slave node to master", nodename)));
		}
		else
		{
			if (pingres != PQPING_OK)
			{
				pingres = mgr_get_normal_slave_node(relNode, masterTupleOid, SYNC_STATE_POTENTIAL, InvalidOid, &slaveNodeName);
				if (pingres != PQPING_OK)
					pingres = mgr_get_normal_slave_node(relNode, masterTupleOid, SYNC_STATE_ASYNC, InvalidOid, &slaveNodeName);
				if (pingres == PQPING_OK)
				{} /*do nothing */
				else if (pingres == AGENT_DOWN)
					ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
						,errmsg("some agents could not be connected and cannot find any running normal slave node for gtm master \"%s\"", nodename)
						,errhint("try \"monitor agent all;\" to check agents status")));
				else
					ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
						,errmsg("gtm master \"%s\" does not have one running normal slave node", nodename)));
			}
		}

	}PG_CATCH();
	{
		heap_close(relNode, RowExclusiveLock);
		PG_RE_THROW();
	}PG_END_TRY();

	ereport(LOG, (errmsg("check gtm slave status in failover cmd end")));

	initStringInfo(&(getAgentCmdRst.description));
	slaveTuple = mgr_get_tuple_node_from_name_type(relNode, slaveNodeName.data);
	mgr_runmode_cndn_get_result(AGT_CMD_GTMCOOR_SLAVE_FAILOVER, &getAgentCmdRst, relNode, slaveTuple, TAKEPLAPARM_N);
	heap_freetuple(slaveTuple);

	tup_result = build_common_command_tuple(
		&(slaveNodeName)
		, getAgentCmdRst.ret
		, getAgentCmdRst.description.data);
	ereport(LOG, (errmsg("the command for failover:\nresult is: %s\ndescription is: %s\n", getAgentCmdRst.ret == true ? "true" : "false", getAgentCmdRst.description.data)));
	pfree(getAgentCmdRst.description.data);
	heap_close(relNode, RowExclusiveLock);

	return HeapTupleGetDatum(tup_result);
}

/*
* gtm slave promote to master, some work need to do:
* 1.stop the old gtm master (before promote)
* 2.promote gtm slave to gtm master
* 3.wait the new master accept connect
* 4.refresh all datanode postgresql.conf:agtm_port,agtm_host and check reload, sync xid and check the result
* 5.refresh all coordinator postgresql.conf:agtm_port,agtm_host and check reload, sync xid and check the result
* 6.new gtm master: refresh postgresql.conf and reload it
* 7.delete old master record in node systbl
* 8.change slave type to master type
* 9.refresh the other gtm slave nodemasternameoid in node systbl and recovery.confs and restart it
*/
static void mgr_after_gtm_failover_handle(char *hostaddress, int cndnport, Relation noderel, GetAgentCmdRst *getAgentCmdRst, HeapTuple aimtuple, char *cndnPath, PGconn **pg_conn, Oid cnoid)
{
	StringInfoData infosendmsg;
	StringInfoData infosendsyncmsg;
	StringInfoData recorderr;
	StringInfoData infosendsyncmsgtmp;
	HeapScanDesc rel_scan;
	Form_mgr_node mgr_node;
	Form_mgr_node mgr_nodecn;
	Form_mgr_node mgr_nodetmp;
	Form_mgr_node mgr_nodemaster;
	HeapTuple tuple;
	HeapTuple mastertuple;
	HeapTuple cn_tuple;
	Oid hostOidtmp;
	Oid hostOid;
	Oid nodemasternameoid;
	Oid newGtmMasterTupleOid;
	Datum datumPath;
	bool isNull;
	bool reload_host = false;
	bool reload_port = false;
	bool rest = false;
	bool hasOtherSlave = true;
	char *cndnPathtmp;
	NameData cndnname;
	NameData cnnamedata;
	NameData sync_state_name;
	NameData slaveNodeName;
	NameData masterNodeName;
	char *address;
	char *strnodetype;
	char aimtuplenodetype;
	char nodeport_buf[10];
	ScanKeyData key[2];
	PGresult * volatile res = NULL;
	int maxtry = 15;
	int try = 0;
	int nrow = 0;
	int pingres = PQPING_NO_RESPONSE;
	int syncNum = 0;
	MemoryContext volatile oldcontext = CurrentMemoryContext;

	initStringInfo(&infosendmsg);
	initStringInfo(&recorderr);
	initStringInfo(&infosendsyncmsg);

	PG_TRY();
	{
		newGtmMasterTupleOid = HeapTupleGetOid(aimtuple);
		mgr_node = (Form_mgr_node)GETSTRUCT(aimtuple);
		Assert(mgr_node);
		hostOid = mgr_node->nodehost;
		nodemasternameoid = mgr_node->nodemasternameoid;
		aimtuplenodetype = mgr_node->nodetype;

		/*get nodename*/
		namestrcpy(&cndnname,NameStr(mgr_node->nodename));
		sprintf(nodeport_buf, "%d", mgr_node->nodeport);

		/*wait the new master accept connect*/
		mgr_check_node_connect(aimtuplenodetype, mgr_node->nodehost, mgr_node->nodeport);

		/*get agtm_port,agtm_host*/
		resetStringInfo(&infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("agtm_host", hostaddress, &infosendmsg);
		mgr_append_pgconf_paras_str_int("agtm_port", cndnport, &infosendmsg);

		/*refresh datanode master/slave, coordinator slave reload agtm_port, agtm_host*/
		ScanKeyInit(&key[0],
			Anum_mgr_node_nodeincluster
			,BTEqualStrategyNumber
			,F_BOOLEQ
			,BoolGetDatum(true));
		rel_scan = heap_beginscan_catalog(noderel, 1, key);
		while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
		{
			mgr_nodetmp = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_nodetmp);
			if (mgr_nodetmp->nodetype == CNDN_TYPE_DATANODE_MASTER 
				|| mgr_nodetmp->nodetype == CNDN_TYPE_DATANODE_SLAVE || mgr_nodetmp->nodetype == CNDN_TYPE_COORDINATOR_SLAVE)
			{
				hostOidtmp = mgr_nodetmp->nodehost;
				datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(noderel), &isNull);
				if(isNull)
				{
					heap_endscan(rel_scan);
					ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
						, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_nodetmp")
						, errmsg("column cndnpath is null")));
				}
				cndnPathtmp = TextDatumGetCString(datumPath);
				try = maxtry;
				address = get_hostaddress_from_hostoid(mgr_nodetmp->nodehost);
				strnodetype = mgr_nodetype_str(mgr_nodetmp->nodetype);
				ereport(LOG, (errmsg("on %s \"%s\" reload \"agtm_host\", \"agtm_port\"", strnodetype, NameStr(mgr_nodetmp->nodename))));
				while(try-- >= 0)
				{
					resetStringInfo(&(getAgentCmdRst->description));
					mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, cndnPathtmp, &infosendmsg, hostOidtmp, getAgentCmdRst);
					/*sleep 0.1s*/
					pg_usleep(100000L);

					/*check the agtm_host, agtm_port*/
					if(mgr_check_param_reload_postgresqlconf(mgr_nodetmp->nodetype, hostOidtmp, mgr_nodetmp->nodeport, address, "agtm_host", hostaddress)
						&& mgr_check_param_reload_postgresqlconf(mgr_nodetmp->nodetype, hostOidtmp, mgr_nodetmp->nodeport, address, "agtm_port", nodeport_buf))
					{
						break;
					}
				}
				if (try < 0)
				{
					ereport(WARNING, (errmsg("on %s \"%s\" reload \"agtm_host\", \"agtm_port\" fail", strnodetype, NameStr(mgr_nodetmp->nodename))));
					appendStringInfo(&recorderr, "on %s \"%s\" reload \"agtm_host\", \"agtm_port\" fail\n", strnodetype, NameStr(mgr_nodetmp->nodename));
				}
				pfree(strnodetype);
				pfree(address);
			}
		}
		heap_endscan(rel_scan);

		/*get name of coordinator, whos oid is cnoid*/
		cn_tuple = SearchSysCache1(NODENODEOID, cnoid);
		if(!HeapTupleIsValid(cn_tuple))
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				, errmsg("oid \"%u\" of coordinator does not exist", cnoid)));
		}
		mgr_nodecn = (Form_mgr_node)GETSTRUCT(cn_tuple);
		Assert(cn_tuple);
		namestrcpy(&cnnamedata, NameStr(mgr_nodecn->nodename));
		ReleaseSysCache(cn_tuple);

		/*coordinator reload agtm_port, agtm_host*/
		ScanKeyInit(&key[0],
			Anum_mgr_node_nodeincluster
			,BTEqualStrategyNumber
			,F_BOOLEQ
			,BoolGetDatum(true));
		ScanKeyInit(&key[1]
			,Anum_mgr_node_nodetype
			,BTEqualStrategyNumber
			,F_CHAREQ
			,CharGetDatum(CNDN_TYPE_COORDINATOR_MASTER));
		rel_scan = heap_beginscan_catalog(noderel, 1, key);
		while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
		{
			mgr_nodetmp = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_nodetmp);
			if (mgr_nodetmp->nodetype != CNDN_TYPE_COORDINATOR_MASTER
				&& mgr_nodetmp->nodetype != CNDN_TYPE_COORDINATOR_SLAVE)
				continue;
			hostOidtmp = mgr_nodetmp->nodehost;
			datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(noderel), &isNull);
			if(isNull)
			{
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
					, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_nodetmp")
					, errmsg("column cndnpath is null")));
			}
			cndnPathtmp = TextDatumGetCString(datumPath);
			try = maxtry;
			ereport(LOG, (errmsg("on coordinator \"%s\" reload \"agtm_host\", \"agtm_port\"", NameStr(mgr_nodetmp->nodename))));
			while(try-- >=0)
			{
				resetStringInfo(&(getAgentCmdRst->description));
				mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, cndnPathtmp, &infosendmsg, hostOidtmp, getAgentCmdRst);

				pg_usleep(100000L);
				/*check the agtm_host, agtm_port*/
				reload_host = false;
				reload_port = false;
				resetStringInfo(&infosendsyncmsg);
				appendStringInfo(&infosendsyncmsg,"EXECUTE DIRECT ON (\"%s\") 'select setting from pg_settings where name=''agtm_host'';'", NameStr(mgr_nodetmp->nodename));
				res = PQexec(*pg_conn, infosendsyncmsg.data);
				if (PQresultStatus(res) == PGRES_TUPLES_OK)
				{
					nrow = PQntuples(res);
					if (nrow > 0)
						if (strcasecmp(hostaddress, PQgetvalue(res, 0, 0)) == 0)
							reload_host = true;
				}
				PQclear(res);
				resetStringInfo(&infosendsyncmsg);
				appendStringInfo(&infosendsyncmsg,"EXECUTE DIRECT ON (\"%s\") 'select setting from pg_settings where name=''agtm_port'';'", NameStr(mgr_nodetmp->nodename));
				res = PQexec(*pg_conn, infosendsyncmsg.data);
				if (PQresultStatus(res) == PGRES_TUPLES_OK)
				{
					nrow = PQntuples(res);
					if (nrow > 0)
						if (strcasecmp(nodeport_buf, PQgetvalue(res, 0, 0)) == 0)
							reload_port = true;
				}
				PQclear(res);
				if (reload_port && reload_host)
				{
					break;
				}
			}
			if (try < 0)
			{
				ereport(WARNING, (errmsg("on coordinator \"%s\" reload \"agtm_host\", \"agtm_port\" fail", NameStr(mgr_nodetmp->nodename))));
				appendStringInfo(&recorderr, "on coordinator \"%s\" reload \"agtm_host\", \"agtm_port\" fail\n", NameStr(mgr_nodetmp->nodename));
			}
		}
		heap_endscan(rel_scan);

		/*send sync agtm xid*/
		rel_scan = heap_beginscan_catalog(noderel, 2, key);
		while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
		{
			mgr_nodetmp = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_nodetmp);
			resetStringInfo(&infosendsyncmsg);
			appendStringInfo(&infosendsyncmsg,"set FORCE_PARALLEL_MODE = off; EXECUTE DIRECT ON (\"%s\") \
				'select pgxc_pool_reload()';", NameStr(mgr_nodetmp->nodename));
			ereport(LOG, (errmsg("on coordinator \"%s\" execute \"%s\"", cnnamedata.data, infosendsyncmsg.data)));
			try = maxtry;
			while(try-- >= 0)
			{
				res = PQexec(*pg_conn, infosendsyncmsg.data);
				if (PQresultStatus(res) == PGRES_TUPLES_OK)
				{
						if (strcasecmp("t", PQgetvalue(res, 0, 0)) == 0)
						{
							PQclear(res);
							break;
						}
				}
				PQclear(res);
			}
			if (try < 0)
			{
				ereport(WARNING, (errcode(ERRCODE_DATA_EXCEPTION)
					,errmsg("on coordinator \"%s\" execute \"%s\" fail", cnnamedata.data, infosendsyncmsg.data)));
				appendStringInfo(&recorderr, "on coordinator \"%s\" execute \"%s\" fail\n", cnnamedata.data, infosendsyncmsg.data);
			}
		}
		heap_endscan(rel_scan);
	}PG_CATCH();
	{
		//ErrorData  *edata;

		getAgentCmdRst->ret = false;
		resetStringInfo(&(getAgentCmdRst->description));
		appendStringInfo(&(getAgentCmdRst->description), "do the steps after gtm promote fail");
		mgr_unlock_cluster_involve_gtm_coord(pg_conn);
		pfree(infosendsyncmsg.data);
		pfree(infosendmsg.data);
		pfree(recorderr.data);

		MemoryContextSwitchTo(oldcontext);
		//edata = CopyErrorData();
		FlushErrorState();

		return;
	}PG_END_TRY();

	/*unlock cluster*/
	mgr_unlock_cluster_involve_gtm_coord(pg_conn);

	/*refresh new master synchronous_standby_names*/
	resetStringInfo(&infosendmsg);
	resetStringInfo(&infosendsyncmsg);
	syncNum = mgr_get_master_sync_string(nodemasternameoid, true, newGtmMasterTupleOid, &infosendsyncmsg);
	if(infosendsyncmsg.len != 0)
	{
		int i = 0;
		while(i<infosendsyncmsg.len && infosendsyncmsg.data[i] != ',' && i < NAMEDATALEN)
		{
			slaveNodeName.data[i] = infosendsyncmsg.data[i];
			i++;
		}
		if (i < NAMEDATALEN)
			slaveNodeName.data[i] = '\0';
		hasOtherSlave = true;
		if (syncNum == 0)
			syncNum = 1;
	}
	else
	{
		pingres = mgr_get_normal_slave_node(noderel, nodemasternameoid, SYNC_STATE_ASYNC, newGtmMasterTupleOid, &slaveNodeName);
		rest = false;
		if (pingres != PQPING_OK)
			rest = mgr_get_slave_node(noderel, nodemasternameoid, SYNC_STATE_ASYNC, newGtmMasterTupleOid, &slaveNodeName);
		if (pingres == PQPING_OK || rest)
		{
			appendStringInfo(&infosendsyncmsg, "%s", slaveNodeName.data);
			syncNum++;
		}
		else
			hasOtherSlave = false;
	}

	if (infosendsyncmsg.len !=0)
	{
		initStringInfo(&infosendsyncmsgtmp);
		appendStringInfo(&infosendsyncmsgtmp, "%d (%s)", syncNum, infosendsyncmsg.data);
		resetStringInfo(&infosendsyncmsg);
		appendStringInfo(&infosendsyncmsg, "%s", infosendsyncmsgtmp.data);
		pfree(infosendsyncmsgtmp.data);
	}
	mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", infosendsyncmsg.len !=0 ? infosendsyncmsg.data:"", &infosendmsg);

	/*refresh new master postgresql.conf*/
	address = get_hostaddress_from_hostoid(mgr_node->nodehost);
	ereport(LOG, (errmsg("on gtm master \"%s\" reload \"synchronous_standby_names = '%s'\"", NameStr(mgr_node->nodename), infosendsyncmsg.len != 0 ? infosendsyncmsg.data : "")));
	ereport(NOTICE, (errmsg("on gtm master \"%s\" reload \"synchronous_standby_names = '%s'\"", NameStr(mgr_node->nodename), infosendsyncmsg.len != 0 ? infosendsyncmsg.data : "")));
	try = maxtry;
	while (try-- >= 0)
	{
		resetStringInfo(&(getAgentCmdRst->description));
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, cndnPath, &infosendmsg, hostOid, getAgentCmdRst);
		/*check*/
		if (mgr_check_param_reload_postgresqlconf(mgr_node->nodetype, mgr_node->nodehost, mgr_node->nodeport, address, "synchronous_standby_names", infosendsyncmsg.len != 0 ? infosendsyncmsg.data : ""))
			break;
	}
	pfree(infosendsyncmsg.data);
	pfree(address);
	if (try < 0)
	{
		ereport(WARNING, (errmsg("on gtm master \"%s\" reload \"synchronous_standby_names\" fail", NameStr(mgr_node->nodename))));
		appendStringInfo(&recorderr, "on gtm master \"%s\" reload \"synchronous_standby_names\" fail\n", NameStr(mgr_node->nodename));
	}

	ereport(LOG, (errmsg("refresh \"node\" table in ADB Manager for node \"%s\"", NameStr(mgr_node->nodename))));
	mastertuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(nodemasternameoid));
	if(!HeapTupleIsValid(mastertuple))
	{
		ereport(WARNING, (errcode(ERRCODE_UNDEFINED_OBJECT)
			,errmsg("gtm master \"%s\" does not exist", cndnname.data)));
	}
	else
	{
		mgr_nodemaster = (Form_mgr_node)GETSTRUCT(mastertuple);
		namestrcpy(&masterNodeName, NameStr(mgr_nodemaster->nodename));
		/*delete old master record in node systbl*/
		CatalogTupleDelete(noderel, &mastertuple->t_self);
		ReleaseSysCache(mastertuple);
	}
	/*change slave type to master type*/
	mgr_node = (Form_mgr_node)GETSTRUCT(aimtuple);
	Assert(mgr_node);
	mgr_node->nodetype = CNDN_TYPE_GTM_COOR_MASTER;
	mgr_node->nodemasternameoid = 0;
	namestrcpy(&(mgr_node->nodesync), "");
	heap_inplace_update(noderel, aimtuple);
	/*for mgr_updateparm systbl, drop the old master param, update slave parm info in the mgr_updateparm systbl*/
	ereport(LOG, (errmsg("refresh \"param\" table in ADB Manager for node \"%s\"", NameStr(mgr_node->nodename))));
	mgr_parm_after_gtm_failover_handle(&masterNodeName, CNDN_TYPE_GTM_COOR_MASTER, &cndnname, aimtuplenodetype);

	if (!hasOtherSlave)
		ereport(WARNING, (errmsg("the new gtm master \"%s\" has no slave, it is better to append a new gtm slave node", cndnname.data)));

	/*update the other gtm slave nodemasternameoid, refresh gtm slave recovery.conf*/
	ScanKeyInit(&key[0],
		Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(CNDN_TYPE_GTM_COOR_SLAVE));
	rel_scan = heap_beginscan_catalog(noderel, 1, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		if (newGtmMasterTupleOid == HeapTupleGetOid(tuple))
			continue;
		mgr_nodetmp = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_nodetmp);
		mgr_nodetmp->nodemasternameoid = newGtmMasterTupleOid;
		if (strcmp(NameStr(mgr_nodetmp->nodename), slaveNodeName.data) == 0)
		{
			namestrcpy(&sync_state_name, sync_state_tab[SYNC_STATE_SYNC].name);
			namestrcpy(&(mgr_nodetmp->nodesync), sync_state_name.data);
		}
		heap_inplace_update(noderel, tuple);
		/*check the node is initialized or not*/
		if (!mgr_nodetmp->nodeincluster)
			continue;

		/* update gtm master's pg_hba.conf */
		resetStringInfo(&infosendmsg);
		resetStringInfo(&(getAgentCmdRst->description));
		ereport(LOG, (errmsg("update new gtm master \"%s\" pg_hba.conf for gtm slave \"%s\" sreaming replication", NameStr(mgr_nodetmp->nodename), cndnname.data)));
		address = get_hostaddress_from_hostoid(mgr_nodetmp->nodehost);
		mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "replication", "all", address, 32, "trust", &infosendmsg);
		pfree(address);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
								cndnPath,
								&infosendmsg,
								mgr_node->nodehost,
								getAgentCmdRst);
		if (!getAgentCmdRst->ret)
		{
			ereport(WARNING, (errmsg("refresh pg_hba.conf of new gtm master for agtm slave %s fail", NameStr(mgr_nodetmp->nodename))));
			appendStringInfo(&recorderr, "refresh pg_hba.conf of new gtm master for agtm slave %s fail\n", NameStr(mgr_nodetmp->nodename));
		}
		mgr_reload_conf(mgr_node->nodehost, cndnPath);

		/*refresh gtm slave recovery.conf*/
		resetStringInfo(&(getAgentCmdRst->description));
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_recoveryconf(mgr_nodetmp->nodetype, NameStr(mgr_nodetmp->nodename), HeapTupleGetOid(aimtuple), &infosendmsg);
		datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(noderel), &isNull);
		if(isNull)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
				, errmsg("column cndnpath is null")));
		}
		/*get cndnPathtmp from tuple*/
		ereport(LOG, (errmsg("refresh recovery.conf of gtmcoord slave \"%s\"", NameStr(mgr_nodetmp->nodename))));
		cndnPathtmp = TextDatumGetCString(datumPath);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_RECOVERCONF, cndnPathtmp, &infosendmsg, mgr_nodetmp->nodehost, getAgentCmdRst);
		if(!getAgentCmdRst->ret)
		{
			ereport(WARNING, (errmsg("refresh recovery.conf of agtm slave \"%s\" fail", NameStr(mgr_nodetmp->nodename))));
			appendStringInfo(&recorderr, "refresh recovery.conf of agtm slave \"%s\" fail\n", NameStr(mgr_nodetmp->nodename));
		}
		/*restart gtmcoord slave*/
		ereport(LOG, (errmsg("pg_ctl restart gtmcoord slave \"%s\"", NameStr(mgr_nodetmp->nodename))));
		resetStringInfo(&(getAgentCmdRst->description));
		mgr_runmode_cndn_get_result(AGT_CMD_AGTM_RESTART, getAgentCmdRst, noderel, tuple, SHUTDOWN_F);
		if(!getAgentCmdRst->ret)
		{
			ereport(WARNING, (errmsg("pg_ctl restart gtmcoord slave \"%s\" fail", NameStr(mgr_nodetmp->nodename))));
			appendStringInfo(&recorderr, "pg_ctl restart gtmcoord slave \"%s\" fail\n", NameStr(mgr_nodetmp->nodename));
		}
	}
	heap_endscan(rel_scan);

	pfree(infosendmsg.data);
	if (recorderr.len > 0)
	{
		resetStringInfo(&(getAgentCmdRst->description));
		appendStringInfo(&(getAgentCmdRst->description), "%s", recorderr.data);
		getAgentCmdRst->ret = false;
	}
	pfree(recorderr.data);
}

/*
* datanode slave failover, some work need to do.
* cmd: failover datanode slave dn1
* 1.stop immediate old datanode master
* 2.promote datanode slave to datanode master
* 3.wait the new master accept connect
* 4.refresh pgxc_node on all coordinators
* 5. refresh synchronous_standby_names for new master
* 6.refresh node systbl: delete old master tuple and change slave type to master type
* 7.update param systbl
* 8.change the datanode  slave dn1's recovery.conf and restart it
*
*/
static void mgr_after_datanode_failover_handle(Oid nodemasternameoid, Name cndnname, int cndnport,char *hostaddress, Relation noderel, GetAgentCmdRst *getAgentCmdRst, HeapTuple aimtuple, char *cndnPath, char aimtuplenodetype, PGconn **pg_conn, Oid cnoid)
{
	StringInfoData infosendmsg;
	StringInfoData infosendsyncmsg;
	StringInfoData recorderr;
	StringInfoData infosendsyncmsgtmp;
	HeapScanDesc rel_scan;
	Form_mgr_node mgr_node;
	Form_mgr_node mgr_nodetmp;
	HeapTuple tuple;
	Oid newmastertupleoid;
	Oid oldMasterTupleOid;
	Datum datumPath;
	bool isNull;
	bool getrefresh = false;
	bool rest = false;
	bool hasOtherSlave = true;
	char *cndnPathtmp;
	char *address;
	char *node_user;
	char coordport_buf[10];
	int maxtry = 15;
	int try;
	int pingres = PQPING_NO_RESPONSE;
	int syncNum = 0;
	ScanKeyData key[2];
	NameData sync_state_name;
	NameData slaveNodeName;
	NameData newMasterNodeName;
	NameData masterNameData;

	initStringInfo(&recorderr);
	initStringInfo(&infosendmsg);
	initStringInfo(&infosendsyncmsg);
	PG_TRY();
	{
		resetStringInfo(&(getAgentCmdRst->description));
		mgr_node = (Form_mgr_node)GETSTRUCT(aimtuple);
		Assert(mgr_node);
		namestrcpy(&newMasterNodeName, NameStr(mgr_node->nodename));
		oldMasterTupleOid = mgr_node->nodemasternameoid;
		newmastertupleoid = HeapTupleGetOid(aimtuple);
		address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		sprintf(coordport_buf, "%d", mgr_node->nodeport);

		/*check recovery finish*/
		mgr_check_node_connect(mgr_node->nodetype, mgr_node->nodehost, mgr_node->nodeport);

		/* get the sync slave node for new datanode master */
		namestrcpy(&slaveNodeName, "");
		syncNum = mgr_get_master_sync_string(oldMasterTupleOid, true, newmastertupleoid, &infosendsyncmsg);
		/*refresh master's postgresql.conf*/
		if(infosendsyncmsg.len != 0)
		{
			int i = 0;
			if (syncNum == 0)
				syncNum++;
			while(i<infosendsyncmsg.len && infosendsyncmsg.data[i] != ',' && i<NAMEDATALEN)
			{
				slaveNodeName.data[i] = infosendsyncmsg.data[i];
				i++;
			}
			if (i<NAMEDATALEN)
				slaveNodeName.data[i] = '\0';
			hasOtherSlave = true;
		}
		else
		{
			pingres = mgr_get_normal_slave_node(noderel, nodemasternameoid, SYNC_STATE_ASYNC
												, newmastertupleoid, &slaveNodeName);
			rest = false;
			if (pingres != PQPING_OK)
				rest = mgr_get_slave_node(noderel, nodemasternameoid, SYNC_STATE_ASYNC, newmastertupleoid
											, slaveNodeName.data[0] == '\0' ? NULL:&slaveNodeName);
			if (pingres == PQPING_OK || rest)
			{
				appendStringInfo(&infosendsyncmsg, "%s", slaveNodeName.data);
				syncNum++;
			}
			else
				hasOtherSlave = false;
		}
		if (infosendsyncmsg.len != 0)
		{
			initStringInfo(&infosendsyncmsgtmp);
			appendStringInfo(&infosendsyncmsgtmp, "%d (%s)", syncNum, infosendsyncmsg.data);
			resetStringInfo(&infosendsyncmsg);
			appendStringInfo(&infosendsyncmsg, "%s", infosendsyncmsgtmp.data);
			pfree(infosendsyncmsgtmp.data);
		}

		/* set new datanode master synchronous_standby_names = '' */
		mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names"
						, "", &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, cndnPath, &infosendmsg
						, mgr_node->nodehost, getAgentCmdRst);
		resetStringInfo(&infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names"
						, infosendsyncmsg.len !=0 ? infosendsyncmsg.data:"", &infosendmsg);
		/*refresh pgxc_node on all coordiantors and datanode masters */
		getrefresh = mgr_pqexec_refresh_pgxc_node(PGXC_FAILOVER, mgr_node->nodetype
						, NameStr(mgr_node->nodename), getAgentCmdRst, pg_conn, cnoid, slaveNodeName.data);
		if(!getrefresh)
		{
			getAgentCmdRst->ret = getrefresh;
			appendStringInfo(&recorderr, "%s\n", (getAgentCmdRst->description).data);
		}
	}PG_CATCH();
	{
		mgr_unlock_cluster_involve_gtm_coord(pg_conn);
		PG_RE_THROW();
	}PG_END_TRY();

	/*unlock cluster*/
	mgr_unlock_cluster_involve_gtm_coord(pg_conn);

	/* refresh new master synchronous_standby_names */
	ereport(LOG, (errmsg("on datanode master \"%s\" reload \"synchronous_standby_names = '%s'\""
			, NameStr(mgr_node->nodename), infosendsyncmsg.len != 0 ? infosendsyncmsg.data : "")));
	ereport(NOTICE, (errmsg("on datanode master \"%s\" reload \"synchronous_standby_names = '%s'\""
			, NameStr(mgr_node->nodename), infosendsyncmsg.len != 0 ? infosendsyncmsg.data : "")));

	try = maxtry;
	while (try-- >= 0)
	{
		resetStringInfo(&(getAgentCmdRst->description));
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, cndnPath, &infosendmsg
									, mgr_node->nodehost, getAgentCmdRst);
		/*check*/
		if (mgr_check_param_reload_postgresqlconf(aimtuplenodetype, mgr_node->nodehost, mgr_node->nodeport
					, address, "synchronous_standby_names", infosendsyncmsg.len ? infosendsyncmsg.data : ""))
				break;
	}
	if(try < 0)
	{
		ereport(WARNING, (errmsg("reload \"synchronous_standby_names\" in postgresql.conf of datanode master \"%s\" fail"
					, NameStr(mgr_node->nodename))));
		appendStringInfo(&recorderr, "reload \"synchronous_standby_names\" in postgresql.conf of datanode master \"%s\" fail"
					, NameStr(mgr_node->nodename));
	}
	pfree(address);
	pfree(infosendsyncmsg.data);

	/*delete old master record in node systbl*/
	rel_scan = heap_beginscan_catalog(noderel, 0, NULL);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_nodetmp = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_nodetmp);
		if (HeapTupleGetOid(tuple) != oldMasterTupleOid)
			continue;
		mgr_nodetmp->nodeinited = false;
		mgr_nodetmp->nodeincluster = false;
		heap_inplace_update(noderel, tuple);
		namestrcpy(&masterNameData, NameStr(mgr_nodetmp->nodename));
		CatalogTupleDelete(noderel, &tuple->t_self);
		break;
	}
	heap_endscan(rel_scan);

	/*change slave type to master type*/
	mgr_node = (Form_mgr_node)GETSTRUCT(aimtuple);
	Assert(mgr_node);
	mgr_node->nodeinited = true;
	mgr_node->nodetype = CNDN_TYPE_DATANODE_MASTER;
	mgr_node->nodemasternameoid = 0;
	namestrcpy(&(mgr_node->nodesync), "");
	heap_inplace_update(noderel, aimtuple);
	/*refresh parm systbl*/
	mgr_update_parm_after_dn_failover(&masterNameData, CNDN_TYPE_DATANODE_MASTER, cndnname, aimtuplenodetype);

	if (!hasOtherSlave)
		ereport(WARNING, (errmsg("the datanode master \"%s\" has no slave node, it is better to append a new datanode slave node", cndnname->data)));

	/*update the others datanode slave nodemasternameoid, refresh recovery.conf, restart the node*/
	ScanKeyInit(&key[0],
		Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(CNDN_TYPE_DATANODE_SLAVE));
	ScanKeyInit(&key[1]
		,Anum_mgr_node_nodemasternameoid
		,BTEqualStrategyNumber
		,F_OIDEQ
		,ObjectIdGetDatum(oldMasterTupleOid));
	rel_scan = heap_beginscan_catalog(noderel, 2, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		if (newmastertupleoid == HeapTupleGetOid(tuple))
			continue;
		mgr_nodetmp = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_nodetmp);
		/*update datanode slave nodemasternameoid*/
		mgr_nodetmp->nodemasternameoid = newmastertupleoid;
		if (strcmp(NameStr(mgr_nodetmp->nodename), slaveNodeName.data) == 0)
		{
			namestrcpy(&sync_state_name, sync_state_tab[SYNC_STATE_SYNC].name);
			namestrcpy(&(mgr_nodetmp->nodesync), sync_state_name.data);
		}
		heap_inplace_update(noderel, tuple);
		/*check the node is initialized or not*/
		if (!mgr_nodetmp->nodeincluster)
			continue;

		/* update datanode master's pg_hba.conf */
		resetStringInfo(&infosendmsg);
		resetStringInfo(&(getAgentCmdRst->description));
		ereport(LOG, (errmsg("update new datanode master \"%s\" pg_hba.conf for datanode slave %s", newMasterNodeName.data, NameStr(mgr_node->nodename))));
		address = get_hostaddress_from_hostoid(mgr_nodetmp->nodehost);
		node_user = get_hostuser_from_hostoid(mgr_nodetmp->nodehost);
		mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "replication", node_user, address, 32, "trust", &infosendmsg);
		pfree(address);
		pfree(node_user);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF,
								cndnPath,
								&infosendmsg,
								mgr_node->nodehost,
								getAgentCmdRst);
		if (!getAgentCmdRst->ret)
		{
			ereport(WARNING, (errmsg("refresh pg_hba.conf of new datanode master \"%s\" for datanode slave %s fail", newMasterNodeName.data, NameStr(mgr_node->nodename))));
			appendStringInfo(&recorderr, "refresh pg_hba.conf of new datanode master \"%s\" for datanode %s fail\n", newMasterNodeName.data, NameStr(mgr_node->nodename));
		}
		mgr_reload_conf(mgr_node->nodehost, cndnPath);

		/*refresh datanode slave recovery.conf*/
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_recoveryconf(mgr_nodetmp->nodetype, NameStr(mgr_nodetmp->nodename), HeapTupleGetOid(aimtuple), &infosendmsg);
		datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(noderel), &isNull);
		if(isNull)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
				, errmsg("datanode slave %s column cndnpath is null", NameStr(mgr_nodetmp->nodename))));
		}
		/*get cndnPathtmp from tuple*/
		ereport(LOG, (errmsg("refresh recovery.conf of datanode slave \"%s\"", NameStr(mgr_node->nodename))));
		cndnPathtmp = TextDatumGetCString(datumPath);
		resetStringInfo(&(getAgentCmdRst->description));
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_RECOVERCONF, cndnPathtmp, &infosendmsg, mgr_nodetmp->nodehost, getAgentCmdRst);
		if(!getAgentCmdRst->ret)
		{
			ereport(WARNING, (errmsg("refresh recovery.conf of datanode slave %s fail", NameStr(mgr_nodetmp->nodename))));
			appendStringInfo(&recorderr, "refresh recovery.conf of datanode slave %s fail\n", NameStr(mgr_nodetmp->nodename));
		}
		/*restart datanode slave*/
		ereport(LOG, (errmsg("pg_ctl restart datanode slave %s", NameStr(mgr_nodetmp->nodename))));
		resetStringInfo(&(getAgentCmdRst->description));
		mgr_runmode_cndn_get_result(AGT_CMD_DN_RESTART, getAgentCmdRst, noderel, tuple, SHUTDOWN_F);
		if(!getAgentCmdRst->ret)
		{
			ereport(WARNING, (errmsg("pg_ctl restart datanode slave %s fail", NameStr(mgr_nodetmp->nodename))));
			appendStringInfo(&recorderr, "pg_ctl restart datanode slave %s fail\n", NameStr(mgr_nodetmp->nodename));
		}
	}
	heap_endscan(rel_scan);
	pfree(infosendmsg.data);

	if (recorderr.len > 0)
	{
		resetStringInfo(&(getAgentCmdRst->description));
		appendStringInfo(&(getAgentCmdRst->description), "%s", recorderr.data);
		getAgentCmdRst->ret = false;
	}
	pfree(recorderr.data);
}

char *mgr_nodetype_str(char nodetype)
{
	char *nodestring;
	char *retstr;
		switch(nodetype)
	{
		case CNDN_TYPE_GTM_COOR_MASTER:
			nodestring = "gtmcoord master";
			break;
		case CNDN_TYPE_GTM_COOR_SLAVE:
			nodestring = "gtmcoord slave";
			break;
		case CNDN_TYPE_COORDINATOR_MASTER:
			nodestring = "coordinator master";
			break;
		case CNDN_TYPE_COORDINATOR_SLAVE:
			nodestring = "coordinator slave";
			break;
		case CNDN_TYPE_DATANODE_MASTER:
			nodestring = "datanode master";
			break;
		case CNDN_TYPE_DATANODE_SLAVE:
			nodestring = "datanode slave";
			break;
		default:
			nodestring = "none node type";
			/*never come here*/
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
				, errmsg("node is not recognized")
				, errhint("option type is gtm or coordinator or datanode master/slave")));
			break;
	}
	retstr = pstrdup(nodestring);
	return retstr;
}

/*
* clean all: 1. check the database cluster running, if it running(check gtm master), give the tip: stop cluster first; if not
* running, clean node. clean gtm, clean coordinator, clean datanode master, clean datanode slave
*/
Datum mgr_clean_all(PG_FUNCTION_ARGS)
{
	NameData resnamedata;
	NameData restypedata;

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot assign TransactionIds during recovery")));

	/*check all node stop*/
	if (!mgr_check_cluster_stop(&resnamedata, &restypedata))
		ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
			,errmsg("%s \"%s\" still running, please stop it before clean all", restypedata.data, resnamedata.data)
			,errhint("try \"monitor all;\" for more information")));

	/*clean gtm master/slave, clean coordinator, clean datanode master/slave*/
	return mgr_prepare_clean_all(fcinfo);
}
/*
* clean the given node: the command format: clean nodetype nodename
* clean gtm master/slave gtm_name
* clean coordinator nodename, ...
* clean datanode master/slave nodename, ...
*/

Datum mgr_clean_node(PG_FUNCTION_ARGS)
{
	char nodetype;
	char *nodename;
	char *user;
	char *address;
	NameData namedata;
	List *nodenamelist = NIL;
	Relation rel_node;
	HeapScanDesc rel_scan;
	HeapTuple tuple;
	ListCell   *cell;
	Form_mgr_node mgr_node;
	ScanKeyData key[2];
	char port_buf[10];
	int ret;
	/*ndoe type*/
	nodetype = PG_GETARG_CHAR(0);
	nodenamelist = get_fcinfo_namelist("", 1, fcinfo);

	/*check the node not in the cluster*/
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	foreach(cell, nodenamelist)
	{
		nodename = (char *) lfirst(cell);
		namestrcpy(&namedata, nodename);
		ScanKeyInit(&key[0],
			Anum_mgr_node_nodetype
			,BTEqualStrategyNumber
			,F_CHAREQ
			,CharGetDatum(nodetype));
		ScanKeyInit(&key[1],
			Anum_mgr_node_nodename
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,NameGetDatum(&namedata));

		rel_scan = heap_beginscan_catalog(rel_node, 2, key);
		if ((tuple = heap_getnext(rel_scan, ForwardScanDirection)) == NULL)
		{
			heap_endscan(rel_scan);
			heap_close(rel_node, RowExclusiveLock);
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				 ,errmsg("%s \"%s\" does not exist", mgr_nodetype_str(nodetype), nodename)));
		}
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		if (mgr_node->nodeincluster)
		{
			heap_endscan(rel_scan);
			heap_close(rel_node, RowExclusiveLock);
			ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
				 ,errmsg("%s \"%s\" already exists in cluster, cannot be cleaned", mgr_nodetype_str(nodetype), nodename)));
		}
		/*check node stoped*/
		address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		sprintf(port_buf, "%d", mgr_node->nodeport);
		user = get_hostuser_from_hostoid(mgr_node->nodehost);
		ret = pingNode_user(address, port_buf, user);
		pfree(address);
		pfree(user);
		if (ret == 0)
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
				 ,errmsg("%s \"%s\" is running, cannot be cleaned, stop it first", mgr_nodetype_str(nodetype), nodename)));
		}
		mgr_node->allowcure = false;
		namestrcpy(&mgr_node->curestatus, CURE_STATUS_NORMAL);
		heap_inplace_update(rel_node, tuple);
		heap_endscan(rel_scan);
	}
	heap_close(rel_node, RowExclusiveLock);

	return mgr_runmode_cndn(nodetype, AGT_CMD_CLEAN_NODE, nodenamelist, TAKEPLAPARM_N, fcinfo);

}
/*clean the node folder*/
void mgr_clean_node_folder(char cmdtype, Oid hostoid, char *nodepath, GetAgentCmdRst *getAgentCmdRst)
{
	StringInfoData infosendmsg;
	StringInfoData clean_tablespace_sendmsg;
	bool res = false;

	Assert(strcasecmp(nodepath, "/") != 0);

	getAgentCmdRst->ret = false;
	initStringInfo(&(getAgentCmdRst->description));
	/* clean tablespace dir*/
	initStringInfo(&clean_tablespace_sendmsg);
	appendStringInfo(&clean_tablespace_sendmsg, "%s/pg_tblspc|%s", nodepath, "*");
	res = mgr_ma_send_cmd_get_original_result(cmdtype, clean_tablespace_sendmsg.data, hostoid, &(getAgentCmdRst->description), AGENT_RESULT_MESSAGE);

	/* clean nodepath dir*/
	initStringInfo(&infosendmsg);
	appendStringInfo(&infosendmsg, "rm -rf %s; mkdir -p %s; chmod 0700 %s", nodepath, nodepath, nodepath);
	res = mgr_ma_send_cmd(cmdtype, infosendmsg.data, hostoid, &(getAgentCmdRst->description));

	getAgentCmdRst->ret = res;
	if (!getAgentCmdRst->ret)
		ereport(WARNING, (errmsg("clean folder \"%s\" fail %s", nodepath, (getAgentCmdRst->description).data)));
	pfree(infosendmsg.data);

}

/*clean all node: gtm/datanode/coordinator which in cluster*/
static Datum mgr_prepare_clean_all(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	InitNodeInfo *info;
	HeapTuple tuple;
	HeapTuple tup_result;
	Form_mgr_node mgr_node;
	Datum datumpath;
	GetAgentCmdRst getAgentCmdRst;
	ScanKeyData key[1];
	char *nodepath;
	bool isNull;
	char cmdtype = AGT_CMD_CLEAN_NODE;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		info = palloc(sizeof(*info));
		info->rel_node = heap_open(NodeRelationId, RowExclusiveLock);
		ScanKeyInit(&key[0]
				,Anum_mgr_node_nodezone
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,CStringGetDatum(mgr_zone));
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

	tuple = heap_getnext(info->rel_scan, ForwardScanDirection);
	if(tuple == NULL)
	{
		/* end of row */
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, RowExclusiveLock);
		pfree(info);
		SRF_RETURN_DONE(funcctx);
	}

	mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
	Assert(mgr_node);
	/*clean one node folder*/
	datumpath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(info->rel_node), &isNull);
	if(isNull)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
			, errmsg("%s %s column cndnpath is null", mgr_nodetype_str(mgr_node->nodetype),  NameStr(mgr_node->nodename))));
	}
	/*get nodepath from tuple*/
	nodepath = TextDatumGetCString(datumpath);
	mgr_clean_node_folder(cmdtype, mgr_node->nodehost, nodepath, &getAgentCmdRst);
	/*update node systbl, set inited and incluster to false*/
	if ( true == getAgentCmdRst.ret)
	{
		mgr_node->nodeinited = false;
		mgr_node->nodeincluster = false;
		mgr_node->allowcure = false;
		namestrcpy(&mgr_node->curestatus, CURE_STATUS_NORMAL);
		heap_inplace_update(info->rel_node, tuple);
	}
	tup_result = build_common_command_tuple_four_col(
		&(mgr_node->nodename)
		,mgr_node->nodetype
		,getAgentCmdRst.ret
		,getAgentCmdRst.description.data
		);
	pfree(getAgentCmdRst.description.data);
	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}

/*check the oid has been used by slave*/
static bool mgr_node_has_slave(Relation rel, Oid mastertupleoid)
{
	ScanKeyData key[1];
	HeapTuple tuple;
	HeapScanDesc scan;

	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodemasternameoid
		,BTEqualStrategyNumber
		,F_OIDEQ
		,ObjectIdGetDatum(mastertupleoid));
	scan = heap_beginscan_catalog(rel, 1, key);
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		heap_endscan(scan);
		return true;
	}
	heap_endscan(scan);
	return false;
}

/*check given type of node exist*/
bool mgr_check_node_exist_incluster(Name nodename, bool bincluster)
{
	Relation rel_node;
	HeapScanDesc rel_scan;
	ScanKeyData key[4];
	HeapTuple tuple;
	bool getnode = false;

	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodename
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,CStringGetDatum(nodename));
	ScanKeyInit(&key[1]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(bincluster));
	ScanKeyInit(&key[2]
				,Anum_mgr_node_nodezone
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,CStringGetDatum(mgr_zone));
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	rel_scan = heap_beginscan_catalog(rel_node, 3, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		getnode = true;
	}

	heap_endscan(rel_scan);
	heap_close(rel_node, RowExclusiveLock);

	return getnode;
}


/*acoording to the value of nodesync in node systable, refresh synchronous_standby_names in postgresql.conf of gtm
* or datanode master.
*/
static void mgr_set_master_sync(void)
{
	Relation rel_node;
	HeapScanDesc rel_scan;
	HeapTuple tuple;
	Datum datumpath;
	bool isNull = false;
	char *path;
	char *address;
	char *value;
	StringInfoData infosendmsg;
	StringInfoData infostrparam;
	StringInfoData infostrparamtmp;
	Form_mgr_node mgr_node;
	GetAgentCmdRst getAgentCmdRst;
	int syncNum = 0;
	ScanKeyData key[1];

	initStringInfo(&infosendmsg);
	initStringInfo(&infostrparam);
	initStringInfo(&infostrparamtmp);
	initStringInfo(&(getAgentCmdRst.description));
	getAgentCmdRst.ret = false;
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodezone
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,CStringGetDatum(mgr_zone));
	rel_scan = heap_beginscan_catalog(rel_node, 1, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if (CNDN_TYPE_GTM_COOR_MASTER != mgr_node->nodetype && CNDN_TYPE_DATANODE_MASTER != mgr_node->nodetype)
			continue;
		/*get master path*/
		datumpath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(rel_node), &isNull);
		if(isNull)
		{
			heap_endscan(rel_scan);
			heap_close(rel_node, RowExclusiveLock);
			pfree(infosendmsg.data);
			pfree(infostrparam.data);
			pfree(getAgentCmdRst.description.data);
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
				, errmsg("column cndnpath is null")));
		}
		path = TextDatumGetCString(datumpath);
		syncNum = mgr_get_master_sync_string(HeapTupleGetOid(tuple), true, InvalidOid, &infostrparam);
		if (infostrparam.len == 0)
			mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", "", &infosendmsg);
		else
		{
			appendStringInfo(&infostrparamtmp, "%d (%s)", syncNum, infostrparam.data);
			mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", infostrparamtmp.data, &infosendmsg);
		}

		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD,
								path,
								&infosendmsg,
								mgr_node->nodehost,
								&getAgentCmdRst);

		value = &infosendmsg.data[strlen("synchronous_standby_names")+1];
		address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		ereport(LOG, (errmsg("%s, set %s synchronous_standby_names=%s.", address, path, value)));
		if (!getAgentCmdRst.ret)
		{
			ereport(WARNING, (errmsg("%s, set %s synchronous_standby_names=%s failed.", address, path
					,value)));
		}
		pfree(address);
		resetStringInfo(&infosendmsg);
		resetStringInfo(&infostrparam);
		resetStringInfo(&infostrparamtmp);
		resetStringInfo(&(getAgentCmdRst.description));
	}
	heap_endscan(rel_scan);
	heap_close(rel_node, RowExclusiveLock);
	pfree(infosendmsg.data);
	pfree(infostrparam.data);
	pfree(infostrparamtmp.data);
	pfree(getAgentCmdRst.description.data);

}

/*
* get the command head word
*/
void mgr_get_cmd_head_word(char cmdtype, char *str)
{
	Assert(str != NULL);

	switch(cmdtype)
	{
		case AGT_CMD_GTMCOOR_START_MASTER:
		case AGT_CMD_GTMCOOR_START_SLAVE:
		case AGT_CMD_GTMCOOR_STOP_MASTER:
		case AGT_CMD_GTMCOOR_STOP_SLAVE:
		case AGT_CMD_GTMCOOR_SLAVE_FAILOVER:
		case AGT_CMD_AGTM_RESTART:
		case AGT_CMD_CN_RESTART:
		case AGT_CMD_CN_START:
		case AGT_CMD_CN_STOP:
		case AGT_CMD_DN_START:
		case AGT_CMD_DN_RESTART:
		case AGT_CMD_DN_STOP:
		case AGT_CMD_DN_FAILOVER:
		case AGT_CMD_NODE_RELOAD:
		case AGT_CMD_DN_MASTER_PROMOTE:
			strcpy(str, "pg_ctl");
			break;
		case AGT_CMD_GTMCOOR_CLEAN:
		case AGT_CMD_CLEAN_NODE:
			strcpy(str, "");
			break;
		case AGT_CMD_GTMCOOR_INIT:
		case AGT_CMD_CNDN_CNDN_INIT:
			strcpy(str, "initdb");
			break;
		case AGT_CMD_CNDN_SLAVE_INIT:
		case AGT_CMD_GTMCOOR_SLAVE_INIT:
			strcpy(str, "pg_basebackup");
			break;
		case AGT_CMD_PSQL_CMD:
			strcpy(str, "psql");
			break;
		case AGT_CMD_CNDN_REFRESH_PGSQLCONF:
		case AGT_CMD_CNDN_REFRESH_RECOVERCONF:
		case AGT_CMD_CNDN_REFRESH_PGHBACONF:
		case AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD:
		case AGT_CMD_CNDN_DELPARAM_PGSQLCONF_FORCE:
		case AGT_CMD_CNDN_RENAME_RECOVERCONF:
			strcpy(str, "update");
			break;
		case AGT_CMD_MONITOR_GETS_HOST_INFO:
			strcpy(str, "monitor");
			break;
		case AGT_CMD_PGDUMPALL:
			strcpy(str, "pg_dumpall");
			break;
		case AGT_CMD_STOP_AGENT:
			strcpy(str, "stop agent");
			break;
		case AGT_CMD_SHOW_AGTM_PARAM:
		case AGT_CMD_SHOW_CNDN_PARAM:
			strcpy(str, "show parameter");
			break;
		case AGT_CMD_NODE_REWIND:
			strcpy(str, "adb_rewind");
			break;
		case AGT_CMD_AGTM_REWIND:
			strcpy(str, "pg_rewind");
			break;
		case AGT_CMD_CHECK_DIR_EXIST:
			strcpy(str, "check directory");
			break;
		case AGT_CMD_RM:
			strcpy(str, "rm ");
			break;
		case AGT_CMD_GET_BATCH_JOB:
			strcpy(str, "");
			break;
		default:
			strcpy(str, "unknown cmd");
			break;
		str[strlen(str)-1]='\0';
	}
}

static struct tuple_cndn *get_new_pgxc_node(pgxc_node_operator cmd, char *node_name, char node_type)
{
	struct host
	{
		char *address;
		List *coordiantor_list;
		List *datanode_list;
	};
	StringInfoData file_name_str;
	Form_mgr_node mgr_dn_node, mgr_cn_node;

	Relation rel;
	HeapScanDesc scan;
	HeapTuple tup, temp_tuple;
	Form_mgr_node mgr_node;
	ListCell *lc_out, *lc_in, *cn_lc, *dn_lc;
	Datum host_addr;
	char *host_address;
	char *user;
	struct host *host_info = NULL;
	List *host_list = NIL;/*store cn and dn base on host*/
	struct tuple_cndn *leave_cndn = NULL;/*store the left cn and dn which */
	struct tuple_cndn *prefer_cndn = NULL;/*store the prefer datanode to the coordiantor one by one */
	bool isNull = false;
	StringInfoData str_port;
	char cn_dn_type;
	leave_cndn = palloc(sizeof(struct tuple_cndn));
	memset(leave_cndn,0,sizeof(struct tuple_cndn));
	prefer_cndn = palloc(sizeof(struct tuple_cndn));
	memset(prefer_cndn,0,sizeof(struct tuple_cndn));
	initStringInfo(&str_port);
	/*get dn and cn from mgr_host and mgr_node*/
	rel = heap_open(HostRelationId, AccessShareLock);
	scan = heap_beginscan_catalog(rel, 0, NULL);
	while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		host_addr = heap_getattr(tup, Anum_mgr_host_hostaddr, RelationGetDescr(rel), &isNull);
		host_address = pstrdup(TextDatumGetCString(host_addr));
		if(isNull)
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
				, errmsg("column hostaddr is null")));
		host_info = palloc(sizeof(struct host));
		memset(host_info,0,sizeof(struct host));
		host_info->address = host_address;
		host_list = lappend(host_list, host_info);
	}
	heap_endscan(scan);
	heap_close(rel, AccessShareLock);
	/*link the datanode and coordiantor to the list of host */
	rel= heap_open(NodeRelationId, AccessShareLock);
	scan = heap_beginscan_catalog(rel, 0, NULL);
	while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tup);
		Assert(mgr_node);
		cn_dn_type = mgr_node->nodetype;
		if((CNDN_TYPE_DATANODE_MASTER != cn_dn_type)&&(CNDN_TYPE_COORDINATOR_MASTER != cn_dn_type)
				&&(CNDN_TYPE_GTM_COOR_MASTER != cn_dn_type))
			continue;
		host_address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		if(true == mgr_node->nodeinited)
		{
			if(PGXC_FAILOVER != cmd)
				temp_tuple = heap_copytuple(tup);
			else
			{
				if(strcmp( node_name, NameStr(mgr_node->nodename)) == 0)
				{
					temp_tuple = mgr_get_tuple_node_from_name_type(rel, node_name);
					pfree(host_address);
					mgr_node = (Form_mgr_node)GETSTRUCT(temp_tuple);
					host_address = get_hostaddress_from_hostoid(mgr_node->nodehost);
				}
				else
					temp_tuple = heap_copytuple(tup);
			}
		}
		else
		{
			if (PGXC_CONFIG == cmd)
			{
				resetStringInfo(&str_port);
				appendStringInfo(&str_port, "%d", mgr_node->nodeport);
				/*iust init ,but haven't alter the mgr_node table*/
				user = get_hostuser_from_hostoid(mgr_node->nodehost);
				if(PQPING_OK == pingNode_user(host_address, str_port.data, user))
				{
					pfree(user);
					temp_tuple = heap_copytuple(tup);
				}
				else
				{
					pfree(user);
					pfree(host_address);
					continue;
				}
			}
			else if(PGXC_APPEND == cmd)
			{
				if(strcmp(node_name, NameStr(mgr_node->nodename)) == 0)
					temp_tuple = heap_copytuple(tup);
				else
				{
					pfree(host_address);
					continue;
				}
			}/*may be operator FAILOVER ,and node table has member not init*/
			else
			{
				pfree(host_address);
				continue;
			}
		}
		foreach(lc_out, host_list)
		{
			host_info = (struct host *)lfirst(lc_out);
			if(strcmp(host_info->address, host_address) == 0)
				break;
		}
		/*not find host is correspind to node*/
		if(NULL == lc_out)
			continue;
		if(CNDN_TYPE_DATANODE_MASTER == cn_dn_type)
		{
			host_info->datanode_list = lappend(host_info->datanode_list, temp_tuple);
		}
		else if(CNDN_TYPE_COORDINATOR_MASTER == cn_dn_type || CNDN_TYPE_GTM_COOR_MASTER == cn_dn_type)
		{
			host_info->coordiantor_list = lappend(host_info->coordiantor_list, temp_tuple);
		}
		pfree(host_address);
	}
	pfree(str_port.data);
	heap_endscan(scan);
	heap_close(rel, AccessShareLock);
	/*calculate the prefer of pgxc_node */
	foreach(lc_out, host_list)
	{
		host_info = (struct host *)lfirst(lc_out);
		forboth(cn_lc, host_info->coordiantor_list, dn_lc, host_info->datanode_list)
		{
			temp_tuple = (HeapTuple)lfirst(cn_lc);
			prefer_cndn->coordiantor_list = lappend(prefer_cndn->coordiantor_list, temp_tuple);
			temp_tuple = (HeapTuple)lfirst(dn_lc);
			prefer_cndn->datanode_list = lappend(prefer_cndn->datanode_list, temp_tuple);
		}
		if(NULL == cn_lc )
		{
			for_each_cell(lc_in, dn_lc)
			{
				leave_cndn->datanode_list = lappend(leave_cndn->datanode_list, lfirst(lc_in));
			}
		}
		else
		{
			for_each_cell(lc_in, cn_lc)
			{
				leave_cndn->coordiantor_list = lappend(leave_cndn->coordiantor_list, lfirst(lc_in));
			}
		}
		list_free(host_info->datanode_list);
		list_free(host_info->coordiantor_list);
	}
	list_free(host_list);
	foreach(cn_lc, leave_cndn->coordiantor_list)
	{
		prefer_cndn->coordiantor_list = lappend(prefer_cndn->coordiantor_list, lfirst(cn_lc));
	}
	foreach(dn_lc, leave_cndn->datanode_list)
	{
		prefer_cndn->datanode_list = lappend(prefer_cndn->datanode_list, lfirst(dn_lc));
	}
	list_free(leave_cndn->coordiantor_list);
	list_free(leave_cndn->datanode_list);
	pfree(leave_cndn);
	/*now the cn and prefer dn have store in list prefer_cndn
	but may be list leave_cndn still have member
	*/
	initStringInfo(&file_name_str);
	forboth(cn_lc, prefer_cndn->coordiantor_list, dn_lc, prefer_cndn->datanode_list)
	{
		temp_tuple =(HeapTuple)lfirst(cn_lc);
		mgr_cn_node = (Form_mgr_node)GETSTRUCT(temp_tuple);
		Assert(mgr_cn_node);
		temp_tuple =(HeapTuple)lfirst(dn_lc);
		mgr_dn_node = (Form_mgr_node)GETSTRUCT(temp_tuple);
		Assert(mgr_dn_node);
		appendStringInfo(&file_name_str, "%s\t%s",NameStr(mgr_cn_node->nodename),NameStr(mgr_dn_node->nodename));
	}
	pfree(file_name_str.data);

	return prefer_cndn;
}

static void mgr_check_appendnodeinfo(char node_type, char *append_node_name)
{
	InitNodeInfo *info;
	ScanKeyData key[5];
	HeapTuple tuple;

	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodename
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,CStringGetDatum(append_node_name)); /* CString compatible Name */

	ScanKeyInit(&key[1]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));

	ScanKeyInit(&key[2]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));

	ScanKeyInit(&key[3]
				,Anum_mgr_node_nodetype
				,BTEqualStrategyNumber
				,F_CHAREQ
				,CharGetDatum(node_type));

	ScanKeyInit(&key[4]
			,Anum_mgr_node_nodezone
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,CStringGetDatum(mgr_zone));
	info = palloc(sizeof(*info));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);
	info->rel_scan = heap_beginscan_catalog(info->rel_node, 5, key);
	info->lcp =NULL;

	if ((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
	{
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, AccessShareLock);
		pfree(info);
		ereport(ERROR, (errmsg("%s \"%s\" already exists in cluster", mgr_nodetype_str(node_type), append_node_name)));
	}

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
}

static bool mgr_refresh_pgxc_node(pgxc_node_operator cmd, char nodetype, char *dnname, GetAgentCmdRst *getAgentCmdRst)
{
	struct tuple_cndn *prefer_cndn;
	ListCell *lc_out, *cn_lc, *dn_lc;
	int coordinator_num = 0, datanode_num = 0;
	HeapTuple tuple_in, tuple_out;
	StringInfoData cmdstring;
	StringInfoData buf;
	Form_mgr_node mgr_node_out, mgr_node_in;
	ManagerAgent *ma;
	char *host_address;
	bool is_preferred = false;
	bool execRes = false;
	bool result = true;

	prefer_cndn = get_new_pgxc_node(cmd, dnname, nodetype);
	if(!PointerIsValid(prefer_cndn->coordiantor_list))
	{
		appendStringInfoString(&(getAgentCmdRst->description),"not exist coordinator in the cluster");
		return false;
	}

	initStringInfo(&cmdstring);
	coordinator_num = 0;
	foreach(lc_out, prefer_cndn->coordiantor_list)
	{
		coordinator_num = coordinator_num + 1;
		tuple_out = (HeapTuple)lfirst(lc_out);
		mgr_node_out = (Form_mgr_node)GETSTRUCT(tuple_out);
		Assert(mgr_node_out);

		resetStringInfo(&(getAgentCmdRst->description));
		namestrcpy(&(getAgentCmdRst->nodename), NameStr(mgr_node_out->nodename));
		resetStringInfo(&cmdstring);
		host_address = get_hostaddress_from_hostoid(mgr_node_out->nodehost);
		appendStringInfo(&cmdstring, " -h %s -p %u -d %s -U %s -a -c \""
					,host_address
					,mgr_node_out->nodeport
					,DEFAULT_DB
					,get_hostuser_from_hostoid(mgr_node_out->nodehost));
		if(PGXC_APPEND == cmd)
		{
			appendStringInfo(&cmdstring, "ALTER NODE \\\"%s\\\" WITH (HOST='%s', PORT=%d);"
								,NameStr(mgr_node_out->nodename)
								,host_address
								,mgr_node_out->nodeport);
		}
		pfree(host_address);
		datanode_num = 0;
		foreach(dn_lc, prefer_cndn->datanode_list)
		{
			datanode_num = datanode_num +1;
			tuple_in = (HeapTuple)lfirst(dn_lc);
			mgr_node_in = (Form_mgr_node)GETSTRUCT(tuple_in);
			Assert(mgr_node_in);
			host_address = get_hostaddress_from_hostoid(mgr_node_in->nodehost);
			if(coordinator_num == datanode_num)
			{
				is_preferred = true;
			}
			else
			{
				is_preferred = false;
			}
			appendStringInfo(&cmdstring, "alter node \"%s\" with(host='%s', port=%d, preferred = %s) on (\"%s\");"
								,NameStr(mgr_node_in->nodename)
								,host_address
								,mgr_node_in->nodeport
								,true == is_preferred ? "true":"false"
								,NameStr(mgr_node_out->nodename));
			pfree(host_address);
		}
		appendStringInfoString(&cmdstring, "set FORCE_PARALLEL_MODE = off; select pgxc_pool_reload();\"");

		/* connection agent */
		ma = ma_connect_hostoid(mgr_node_out->nodehost);
		if (!ma_isconnected(ma))
		{
			/* report error message */
			getAgentCmdRst->ret = false;
			appendStringInfoString(&(getAgentCmdRst->description), ma_last_error_msg(ma));
			result = false;
			break;
		}
		ma_beginmessage(&buf, AGT_MSG_COMMAND);
		ma_sendbyte(&buf, AGT_CMD_PSQL_CMD);
		ma_sendstring(&buf,cmdstring.data);
		ma_endmessage(&buf, ma);
		if (! ma_flush(ma, true))
		{
			getAgentCmdRst->ret = false;
			appendStringInfoString(&(getAgentCmdRst->description), ma_last_error_msg(ma));
			result = false;
			break;
		}
		/*check the receive msg*/
		execRes = mgr_recv_msg(ma, getAgentCmdRst);
		if(execRes != true)
		{
			result = false;
			ereport(WARNING, (errmsg("execute \"%s\" fail:%s ", cmdstring.data
					, (getAgentCmdRst->description).data)));
		}
		ma_close(ma);
	}
	pfree(cmdstring.data);
	foreach(cn_lc, prefer_cndn->coordiantor_list)
	{
		heap_freetuple((HeapTuple)lfirst(cn_lc));
	}
	foreach(dn_lc, prefer_cndn->datanode_list)
	{
		heap_freetuple((HeapTuple)lfirst(dn_lc));
	}

	if(PointerIsValid(prefer_cndn->coordiantor_list))
		list_free(prefer_cndn->coordiantor_list);
	if(PointerIsValid(prefer_cndn->datanode_list))
		list_free(prefer_cndn->datanode_list);
	pfree(prefer_cndn);
	return result;
}

/*
* modifty node port after initd cluster
*/

static void mgr_modify_port_after_initd(Relation rel_node, HeapTuple nodetuple, char *nodename, char nodetype, int32 newport)
{
	Form_mgr_node mgr_node;
	StringInfoData infosendmsg;
	ScanKeyData key[2];
	HeapScanDesc rel_scan;
	HeapTuple tuple =NULL;
	Oid nodetupleoid;

	Assert(HeapTupleIsValid(nodetuple));
	nodetupleoid = HeapTupleGetOid(nodetuple);
	initStringInfo(&infosendmsg);
	/*if nodetype is slave, need modfify its postgresql.conf for port*/
	if (CNDN_TYPE_GTM_COOR_SLAVE == nodetype || CNDN_TYPE_DATANODE_SLAVE == nodetype)
	{
		resetStringInfo(&infosendmsg);
		mgr_append_pgconf_paras_str_int("port", newport, &infosendmsg);
		mgr_modify_node_parameter_after_initd(rel_node, nodetuple, &infosendmsg, true);
		if (CNDN_TYPE_DATANODE_SLAVE == nodetype)
		{
			resetStringInfo(&infosendmsg);
			appendStringInfo(&infosendmsg, "ALTER NODE \\\"%s\\\" WITH (%s=%d);"
								,nodename,"port", newport);
			mgr_modify_readonly_coord_pgxc_node(rel_node, &infosendmsg, nodename, newport);
		}
	}
	/*if nodetype is gtm master, need modify its postgresql.conf and all datanodes、coordinators postgresql.conf for  agtm_port, agtm_host*/
	else if (CNDN_TYPE_GTM_COOR_MASTER == nodetype || CNDN_TYPE_DATANODE_MASTER == nodetype)
	{
		/*gtm master*/
		if (CNDN_TYPE_DATANODE_MASTER == nodetype)
			mgr_make_sure_all_running(CNDN_TYPE_COORDINATOR_MASTER);
		resetStringInfo(&infosendmsg);
		mgr_append_pgconf_paras_str_int("port", newport, &infosendmsg);
		mgr_modify_node_parameter_after_initd(rel_node, nodetuple, &infosendmsg, true);
		/*modify its slave recovery.conf and datanodes coordinators postgresql.conf*/
		ScanKeyInit(&key[0]
					,Anum_mgr_node_nodeincluster
					,BTEqualStrategyNumber
					,F_BOOLEQ
					,BoolGetDatum(true));
		rel_scan = heap_beginscan_catalog(rel_node, 1, key);
		while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			if (CNDN_TYPE_GTM_COOR_MASTER == nodetype)
			{
				if (CNDN_TYPE_GTM_COOR_SLAVE == mgr_node->nodetype)
				{
					mgr_modify_port_recoveryconf(rel_node, tuple, newport);
				}
				else if (!(CNDN_TYPE_GTM_COOR_MASTER == mgr_node->nodetype))
				{
					resetStringInfo(&infosendmsg);
					mgr_append_pgconf_paras_str_int("agtm_port", newport, &infosendmsg);
					mgr_modify_node_parameter_after_initd(rel_node, tuple, &infosendmsg, false);
				}
				else
				{
					/*do nothing*/
				}
			}
			else
			{
				if (CNDN_TYPE_DATANODE_SLAVE == mgr_node->nodetype)
				{
					if (nodetupleoid == mgr_node->nodemasternameoid)
						mgr_modify_port_recoveryconf(rel_node, tuple, newport);
				}
			}
		}
		heap_endscan(rel_scan);
		if (CNDN_TYPE_DATANODE_MASTER == nodetype)
		{
			resetStringInfo(&infosendmsg);
			appendStringInfo(&infosendmsg, "ALTER NODE \\\"%s\\\" WITH (%s=%d);"
								,nodename
								,"port"
								,newport);
			mgr_modify_coord_pgxc_node(rel_node, &infosendmsg, NULL, 0);
			mgr_modify_readonly_coord_pgxc_node(rel_node, &infosendmsg, nodename, newport);
		}
	}
	else if (CNDN_TYPE_COORDINATOR_MASTER == nodetype)
	{
		/*refresh all pgxc_node all coordinators*/
		mgr_make_sure_all_running(CNDN_TYPE_COORDINATOR_MASTER);

		/*modify port*/
		resetStringInfo(&infosendmsg);
		mgr_append_pgconf_paras_str_int("port", newport, &infosendmsg);
		mgr_modify_node_parameter_after_initd(rel_node, nodetuple, &infosendmsg, true);

		resetStringInfo(&infosendmsg);
		appendStringInfo(&infosendmsg, "ALTER NODE \\\"%s\\\" WITH (%s=%d);"
							,nodename
							,"port"
							,newport);
		mgr_modify_coord_pgxc_node(rel_node, &infosendmsg, nodename, newport);
		mgr_modify_readonly_coord_pgxc_node(rel_node, &infosendmsg, nodename, newport);
	}
	else
	{
		/*do nothing*/
	}

	pfree(infosendmsg.data);

}

/*
* modify the given node port after it initd
*/
static bool mgr_modify_node_parameter_after_initd(Relation rel_node, HeapTuple nodetuple, StringInfo infosendmsg, bool brestart)
{
	Form_mgr_node mgr_node;
	Datum datumpath;
	char *address;
	char *nodepath;
	char nodetype;
	bool isNull = false;
	bool bnormal = true;
	Oid hostoid;
	GetAgentCmdRst getAgentCmdRst;

	mgr_node = (Form_mgr_node)GETSTRUCT(nodetuple);
	Assert(mgr_node);
	/*get hostoid*/
	hostoid = mgr_node->nodehost;
	nodetype = mgr_node->nodetype;
	/*get path*/
	datumpath = heap_getattr(nodetuple, Anum_mgr_node_nodepath, RelationGetDescr(rel_node), &isNull);
	if(isNull)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
			, errmsg("column nodepath is null")));
	}
	nodepath = TextDatumGetCString(datumpath);
	initStringInfo(&(getAgentCmdRst.description));
	mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, nodepath, infosendmsg, hostoid, &getAgentCmdRst);
	if (!getAgentCmdRst.ret)
	{
		address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		ereport(WARNING, (errmsg("modify %s %s/postgresql.conf %s fail: %s", address, nodepath, infosendmsg->data, getAgentCmdRst.description.data)));
		pfree(address);
		bnormal = false;
	}
	if (brestart)
	{
		resetStringInfo(&(getAgentCmdRst.description));
		getAgentCmdRst.ret = false;
		switch(nodetype)
		{
			case CNDN_TYPE_GTM_COOR_MASTER:
			case CNDN_TYPE_GTM_COOR_SLAVE:
				mgr_runmode_cndn_get_result(AGT_CMD_AGTM_RESTART, &getAgentCmdRst, rel_node, nodetuple, SHUTDOWN_F);
				break;
			case CNDN_TYPE_COORDINATOR_MASTER:
			case CNDN_TYPE_COORDINATOR_SLAVE:
				mgr_runmode_cndn_get_result(AGT_CMD_CN_RESTART, &getAgentCmdRst, rel_node, nodetuple, SHUTDOWN_F);
				break;
			case CNDN_TYPE_DATANODE_MASTER:
			case CNDN_TYPE_DATANODE_SLAVE:
				mgr_runmode_cndn_get_result(AGT_CMD_DN_RESTART, &getAgentCmdRst, rel_node, nodetuple, SHUTDOWN_F);
				break;
			default:
				break;
		}
		if (!getAgentCmdRst.ret)
			bnormal = false;

	}
	pfree(getAgentCmdRst.description.data);
	return bnormal;
}

/*
* modify gtm or datanode slave port in recovery.conf
*/
static void mgr_modify_port_recoveryconf(Relation rel_node, HeapTuple aimtuple, int32 master_newport)
{
	Form_mgr_node mgr_node;
	Form_mgr_node mgr_nodemaster;
	Form_mgr_host mgr_host;
	HeapTuple mastertuple;
	HeapTuple tup;
	Datum datumpath;
	Oid masterhostoid;
	Oid mastertupleoid;
	Oid hostoid;
	char nodetype;
	char *masterhostaddress;
	char *nodepath;
	char *address;
	bool isNull = false;
	NameData username;
	NameData nodenameData;
	StringInfoData primary_conninfo_value;
	StringInfoData infosendparamsg;
	GetAgentCmdRst getAgentCmdRst;

	mgr_node = (Form_mgr_node)GETSTRUCT(aimtuple);
	Assert(mgr_node);
	nodetype = mgr_node->nodetype;
	namestrcpy(&nodenameData, NameStr(mgr_node->nodename));
	if (!(CNDN_TYPE_GTM_COOR_SLAVE ==nodetype || CNDN_TYPE_DATANODE_SLAVE == nodetype))
		return;
	mastertupleoid = mgr_node->nodemasternameoid;
	hostoid = mgr_node->nodehost;
	/*get path*/
	datumpath = heap_getattr(aimtuple, Anum_mgr_node_nodepath, RelationGetDescr(rel_node), &isNull);
	if(isNull)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
			, errmsg("column nodepath is null")));
	}
	nodepath = TextDatumGetCString(datumpath);

	/*get the master port, master host address*/
	mastertuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(mastertupleoid));
	if(!HeapTupleIsValid(mastertuple))
	{
		ereport(ERROR, (errmsg("node oid \"%u\" not exist", mastertupleoid)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
			, errcode(ERRCODE_INTERNAL_ERROR)));
	}
	mgr_nodemaster = (Form_mgr_node)GETSTRUCT(mastertuple);
	Assert(mastertuple);
	masterhostoid = mgr_nodemaster->nodehost;
	ReleaseSysCache(mastertuple);

	/*get host user from system: host*/
	tup = SearchSysCache1(HOSTHOSTOID, ObjectIdGetDatum(masterhostoid));
	if(!(HeapTupleIsValid(tup)))
	{
		ereport(ERROR, (errmsg("host oid \"%u\" not exist", masterhostoid)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
			, errcode(ERRCODE_UNDEFINED_OBJECT)));
	}
	mgr_host= (Form_mgr_host)GETSTRUCT(tup);
	Assert(mgr_host);
	namestrcpy(&username, NameStr(mgr_host->hostuser));

	ReleaseSysCache(tup);

	/*primary_conninfo*/
	initStringInfo(&primary_conninfo_value);
	masterhostaddress = get_hostaddress_from_hostoid(masterhostoid);
	appendStringInfo(&primary_conninfo_value, "host=%s port=%d user=%s application_name=%s", masterhostaddress, master_newport, username.data, nodenameData.data);
	initStringInfo(&infosendparamsg);
	mgr_append_pgconf_paras_str_quotastr("primary_conninfo", primary_conninfo_value.data, &infosendparamsg);
	pfree(primary_conninfo_value.data);
	pfree(masterhostaddress);

	initStringInfo(&(getAgentCmdRst.description));
	mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_RECOVERCONF, nodepath, &infosendparamsg, hostoid, &getAgentCmdRst);
	pfree(infosendparamsg.data);
	if (!getAgentCmdRst.ret)
	{
		address = get_hostaddress_from_hostoid(hostoid);
		ereport(WARNING, (errmsg("modify %s %s/recovery.conf fail: %s", address, nodepath, getAgentCmdRst.description.data)));
		pfree(address);
	}
	switch(nodetype)
	{
		case CNDN_TYPE_GTM_COOR_SLAVE:
			mgr_runmode_cndn_get_result(AGT_CMD_AGTM_RESTART, &getAgentCmdRst, rel_node, aimtuple, SHUTDOWN_F);
			break;
		case CNDN_TYPE_DATANODE_SLAVE:
			mgr_runmode_cndn_get_result(AGT_CMD_DN_RESTART, &getAgentCmdRst, rel_node, aimtuple, SHUTDOWN_F);
			break;
		default:
			break;
	}
	pfree(getAgentCmdRst.description.data);
}

/*
* modify coordinators port of pgxc_node
*/
static bool mgr_modify_coord_pgxc_node(Relation rel_node, StringInfo infostrdata, char *nodename, int newport)
{
	StringInfoData infosendmsg;
	StringInfoData buf;
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	ScanKeyData key[3];
	char *host_address = "127.0.0.1";
	char *user;
	char *address;
	bool execRes = false;
	bool bnormal= true;
	HeapScanDesc rel_scan;
	ManagerAgent *ma;
	GetAgentCmdRst getAgentCmdRst;

	initStringInfo(&infosendmsg);
	initStringInfo(&(getAgentCmdRst.description));

	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(CNDN_TYPE_COORDINATOR_MASTER));
	ScanKeyInit(&key[1]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	rel_scan = heap_beginscan_catalog(rel_node, 2, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);

		user = get_hostuser_from_hostoid(mgr_node->nodehost);
		resetStringInfo(&infosendmsg);
		appendStringInfo(&infosendmsg, " -h %s -p %u -d %s -U %s -a -c \""
			,host_address
			,(nodename == NULL ? mgr_node->nodeport : (strcmp(nodename, NameStr(mgr_node->nodename)) == 0 ? newport : mgr_node->nodeport))
			,DEFAULT_DB
			,user);
		appendStringInfo(&infosendmsg, "%s", infostrdata->data);
		appendStringInfo(&infosendmsg, " set FORCE_PARALLEL_MODE = off; select pgxc_pool_reload();\"");
		pfree(user);
		/* connection agent */
		ma = ma_connect_hostoid(mgr_node->nodehost);
		if (!ma_isconnected(ma))
		{
			/* report error message */
			ereport(WARNING, (errmsg("%s", ma_last_error_msg(ma))));
			ma_close(ma);
			break;
		}
		ma_beginmessage(&buf, AGT_MSG_COMMAND);
		ma_sendbyte(&buf, AGT_CMD_PSQL_CMD);
		ma_sendstring(&buf,infosendmsg.data);
		ma_endmessage(&buf, ma);
		if (! ma_flush(ma, true))
		{
			ereport(WARNING, (errmsg("%s", ma_last_error_msg(ma))));
			ma_close(ma);
			break;
		}
		resetStringInfo(&getAgentCmdRst.description);
		execRes = mgr_recv_msg(ma, &getAgentCmdRst);
		ma_close(ma);
		if (!execRes)
		{
			address = get_hostaddress_from_hostoid(mgr_node->nodehost);
			ereport(WARNING, (errmsg("refresh pgxc_node in %s fail: %s", address, getAgentCmdRst.description.data)));
			pfree(address);
			bnormal = false;
		}
	}
	heap_endscan(rel_scan);
	pfree(infosendmsg.data);
	pfree(getAgentCmdRst.description.data);

	return bnormal;
}

/*
* modify address in host table after initd
* 1. alter all need address in host table (do this before this function)
* 2. check all node running normal, agent also running normal
* 3. add new address in pg_hba.conf of all nodes and reload it
* 4. refresh agtm_host of postgresql.conf in all coordinators and datanodes
* 5. refresh all pgxc_node of all coordinators
* 6. refresh recovery.conf of all slave, then restart
*/
void mgr_flushhost(MGRFlushHost *node, ParamListInfo params, DestReceiver *dest)
{
	if (mgr_has_priv_add())
	{
		DirectFunctionCall1(mgr_flush_host, (Datum)0);
		return;
	}
	else
	{
		ereport(ERROR, (errmsg("permission denied")));
		return ;
	}
}

Datum mgr_flush_host(PG_FUNCTION_ARGS)
{
	ScanKeyData key[2];
	Form_mgr_node mgr_node;
	HeapScanDesc rel_scan;
	StringInfoData infosendmsg;
	StringInfoData infosqlsendmsg;
	HeapTuple tuple;
	Relation rel_node;
	GetAgentCmdRst getAgentCmdRst;
	Datum datumpath;
	char nodetype;
	char *cndnpath;
	char *address = NULL;
	char *gtmmaster_address = NULL;
	bool isNull = false;
	bool bgetwarning = false;
	Oid hostoid;

	mgr_check_job_in_updateparam("monitor_handle_gtm");
	mgr_check_job_in_updateparam("monitor_handle_coordinator");
	mgr_check_job_in_updateparam("monitor_handle_datanode");

	initStringInfo(&infosendmsg);
	initStringInfo(&(getAgentCmdRst.description));
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	/*check agent running normal*/
	mgr_check_all_agent();
	/*check all master nodes running normal*/
	mgr_make_sure_all_running(CNDN_TYPE_GTM_COOR_MASTER);
	mgr_make_sure_all_running(CNDN_TYPE_COORDINATOR_MASTER);
	mgr_make_sure_all_running(CNDN_TYPE_DATANODE_MASTER);
	/*refresh pg_hba.conf*/
	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	rel_scan = heap_beginscan_catalog(rel_node, 1, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if (CNDN_TYPE_GTM_COOR_MASTER == mgr_node->nodetype)
		{
			gtmmaster_address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		}
		/*get master path*/
		datumpath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(rel_node), &isNull);
		if(isNull)
		{
			heap_endscan(rel_scan);
			heap_close(rel_node, RowExclusiveLock);
			pfree(infosendmsg.data);
			pfree(getAgentCmdRst.description.data);
			if (gtmmaster_address)
				pfree(gtmmaster_address);
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
				, errmsg("column nodepath is null")));
		}
		hostoid = mgr_node->nodehost;
		cndnpath = TextDatumGetCString(datumpath);
		resetStringInfo(&(getAgentCmdRst.description));
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_hbaconf((mgr_node->nodemasternameoid == 0)? HeapTupleGetOid(tuple):mgr_node->nodemasternameoid
			, mgr_node->nodetype, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF, cndnpath, &infosendmsg, hostoid, &getAgentCmdRst);
		if (!getAgentCmdRst.ret)
		{
			address = get_hostaddress_from_hostoid(mgr_node->nodehost);
			ereport(WARNING, (errmsg("%s  add address in %s/pg_hba.conf fail: %s", address, cndnpath, getAgentCmdRst.description.data)));
			pfree(address);
			bgetwarning = true;
		}
		mgr_reload_conf(hostoid, cndnpath);
	}
	heap_endscan(rel_scan);

	/*refresh recovery.conf of all slave, then restart*/
	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	rel_scan = heap_beginscan_catalog(rel_node, 1, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		nodetype = mgr_node->nodetype;
		if (nodetype == CNDN_TYPE_GTM_COOR_MASTER || nodetype == CNDN_TYPE_COORDINATOR_MASTER || nodetype == CNDN_TYPE_DATANODE_MASTER)
			continue;
		/*get node path*/
		isNull = false;
		datumpath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(rel_node), &isNull);
		if(isNull)
		{
			heap_endscan(rel_scan);
			heap_close(rel_node, RowExclusiveLock);
			pfree(infosendmsg.data);
			pfree(getAgentCmdRst.description.data);
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
				, errmsg("column nodepath is null")));
		}
		hostoid = mgr_node->nodehost;
		cndnpath = TextDatumGetCString(datumpath);
		/*refresh recovry.conf*/
		resetStringInfo(&(getAgentCmdRst.description));
		resetStringInfo(&infosendmsg);
		mgr_add_parameters_recoveryconf(nodetype, NameStr(mgr_node->nodename), mgr_node->nodemasternameoid, &infosendmsg);
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_RECOVERCONF, cndnpath, &infosendmsg, hostoid, &getAgentCmdRst);
		if (!getAgentCmdRst.ret)
		{
			bgetwarning = true;
		}
		if (!getAgentCmdRst.ret)
		{
			address = get_hostaddress_from_hostoid(hostoid);
			ereport(WARNING, (errmsg("%s  add address in %s/recovery.conf fail: %s", address, cndnpath, getAgentCmdRst.description.data)));
			pfree(address);
			bgetwarning = true;
		}
		/*restart*/
		switch(nodetype)
		{
			case CNDN_TYPE_GTM_COOR_SLAVE:
				mgr_runmode_cndn_get_result(AGT_CMD_AGTM_RESTART, &getAgentCmdRst, rel_node, tuple, SHUTDOWN_F);
				break;
			case CNDN_TYPE_DATANODE_SLAVE:
				mgr_runmode_cndn_get_result(AGT_CMD_DN_RESTART, &getAgentCmdRst, rel_node, tuple, SHUTDOWN_F);
				break;
			default:
				break;
		}
		if (!getAgentCmdRst.ret)
			bgetwarning = true;
	}
	heap_endscan(rel_scan);

	initStringInfo(&infosqlsendmsg);
	/*refresh agtm_host of postgresql.conf in all coordinators and datanodes*/
	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	rel_scan = heap_beginscan_catalog(rel_node, 1, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		nodetype = mgr_node->nodetype;
		if (nodetype == CNDN_TYPE_GTM_COOR_MASTER || nodetype == CNDN_TYPE_GTM_COOR_SLAVE)
			continue;
		address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		if (nodetype == CNDN_TYPE_COORDINATOR_MASTER || nodetype == CNDN_TYPE_DATANODE_MASTER)
			appendStringInfo(&infosqlsendmsg, "ALTER NODE \\\"%s\\\" WITH (%s='%s');"
							,NameStr(mgr_node->nodename)
							,"HOST"
							,address);
		pfree(address);
		/*get master path*/
		datumpath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(rel_node), &isNull);
		if(isNull)
		{
			heap_endscan(rel_scan);
			heap_close(rel_node, RowExclusiveLock);
			pfree(infosendmsg.data);
			pfree(getAgentCmdRst.description.data);
			if (gtmmaster_address)
				pfree(gtmmaster_address);
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
				, errmsg("column nodepath is null")));
		}
		resetStringInfo(&infosendmsg);
		mgr_append_pgconf_paras_str_quotastr("agtm_host", gtmmaster_address, &infosendmsg);
		if (!mgr_modify_node_parameter_after_initd(rel_node, tuple, &infosendmsg, false))
			bgetwarning = true;
	}
	if (gtmmaster_address)
		pfree(gtmmaster_address);
	heap_endscan(rel_scan);

	/*refresh all pgxc_node of all coordinators*/
	if(!mgr_modify_coord_pgxc_node(rel_node, &infosqlsendmsg, NULL, 0))
		bgetwarning = true;

	heap_close(rel_node, RowExclusiveLock);
	pfree(infosendmsg.data);
	pfree(getAgentCmdRst.description.data);
	if (bgetwarning)
		PG_RETURN_BOOL(false);
	else
		PG_RETURN_BOOL(true);
}

void mgr_check_all_agent(void)
{
	Form_mgr_host mgr_host;
	HeapScanDesc rel_scan;
	HeapTuple tuple;
	Datum host_datumaddr;
	char *address;
	bool isNull = false;
	ManagerAgent *ma;
	Relation		rel_host;

	rel_host = heap_open(HostRelationId, AccessShareLock);
	rel_scan = heap_beginscan_catalog(rel_host, 0, NULL);
	while ((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_host = (Form_mgr_host)GETSTRUCT(tuple);
		Assert(mgr_host);
		/*get agent address and port*/
		host_datumaddr = heap_getattr(tuple, Anum_mgr_host_hostaddr, RelationGetDescr(rel_host), &isNull);
		if(isNull)
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
				, errmsg("column hostaddr is null")));
		address = TextDatumGetCString(host_datumaddr);
		ma = ma_connect(address, mgr_host->hostagentport);
		if(!ma_isconnected(ma))
		{
			heap_endscan(rel_scan);
			heap_close(rel_host, AccessShareLock);
			ma_close(ma);
			ereport(ERROR, (errmsg("hostname \"%s\" : agent is not running", NameStr(mgr_host->hostname))));
		}
		ma_close(ma);
	}

	heap_endscan(rel_scan);
	heap_close(rel_host, AccessShareLock);
}

/*
* sql command for create extension
*/

static bool mgr_add_extension_sqlcmd(char *sqlstr)
{
	ScanKeyData key[2];
	HeapScanDesc rel_scan;
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	ManagerAgent *ma;
	char *user;
	char *address;
	bool execRes = false;
	StringInfoData infosendmsg;
	StringInfoData buf;
	GetAgentCmdRst getAgentCmdRst;
	Relation rel_node;

	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(CNDN_TYPE_COORDINATOR_MASTER));
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	rel_scan = heap_beginscan_catalog(rel_node, 1, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		break;
	}
	if (NULL == tuple)
	{
		heap_endscan(rel_scan);
		heap_close(rel_node, RowExclusiveLock);
		ereport(ERROR, (errmsg("can not get right coordinator to execute \"%s\"", sqlstr)));
		return false;
	}
	user = get_hostuser_from_hostoid(mgr_node->nodehost);
	initStringInfo(&infosendmsg);
	appendStringInfo(&infosendmsg, " -h %s -p %u -d %s -U %s -a -c \""
		,"127.0.0.1"
		,mgr_node->nodeport
		,DEFAULT_DB
		,user);
	appendStringInfo(&infosendmsg, " %s\"", sqlstr);
	pfree(user);
	/* connection agent */
	ma = ma_connect_hostoid(mgr_node->nodehost);
	if (!ma_isconnected(ma))
	{
		/* report error message */
		heap_endscan(rel_scan);
		heap_close(rel_node, RowExclusiveLock);
		ereport(ERROR, (errmsg("%s, %s", sqlstr, ma_last_error_msg(ma))));
		ma_close(ma);
		return false;
	}
	initStringInfo(&buf);
	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, AGT_CMD_PSQL_CMD);
	ma_sendstring(&buf,infosendmsg.data);
	ma_endmessage(&buf, ma);
	pfree(infosendmsg.data);
	if (! ma_flush(ma, true))
	{
		heap_endscan(rel_scan);
		heap_close(rel_node, RowExclusiveLock);
		ereport(ERROR, (errmsg("%s, %s", sqlstr, ma_last_error_msg(ma))));
		ma_close(ma);
		return false;
	}
	getAgentCmdRst.ret = false;
	initStringInfo(&getAgentCmdRst.description);
	execRes = mgr_recv_msg(ma, &getAgentCmdRst);
	ma_close(ma);
	if (!execRes)
	{
		address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		heap_endscan(rel_scan);
		heap_close(rel_node, RowExclusiveLock);
		ereport(ERROR, (errmsg(" %s %s:  %s fail, %s", address, NameStr(mgr_node->nodename), sqlstr, getAgentCmdRst.description.data)));
		pfree(address);
	}

	pfree(getAgentCmdRst.description.data);
	heap_endscan(rel_scan);
	heap_close(rel_node, RowExclusiveLock);

	return execRes;
}
Datum mgr_priv_list_to_all(PG_FUNCTION_ARGS)
{
	List *command_list = NIL;
	List *username_list = NIL;
	ListCell *lc = NULL;
	Value *command = NULL;
	char *username_list_str = NULL;
	Datum datum_command_list;

	char command_type = PG_GETARG_CHAR(0);
	Assert(command_type == PRIV_GRANT || command_type == PRIV_REVOKE);
	datum_command_list = PG_GETARG_DATUM(1);

	/* get command list and username list  */
	command_list = DecodeTextArrayToValueList(datum_command_list);
	username_list = get_username_list();

	/* check command is valid */
	mgr_check_command_valid(command_list);

	username_list_str = get_username_list_str(username_list);

	foreach(lc, command_list)
	{
		command = lfirst(lc);
		Assert(command && IsA(command, String));

		if (strcmp(strVal(command), "add") == 0)
			mgr_manage_add(command_type, username_list_str);
		else if (strcmp(strVal(command), "alter") == 0)
			mgr_manage_alter(command_type, username_list_str);
		else if (strcmp(strVal(command), "append") == 0)
			mgr_manage_append(command_type, username_list_str);
		else if (strcmp(strVal(command), "clean") == 0)
			mgr_manage_clean(command_type, username_list_str);
		else if (strcmp(strVal(command), "deploy") == 0)
			mgr_manage_deploy(command_type, username_list_str);
		else if (strcmp(strVal(command), "drop") == 0)
			mgr_manage_drop(command_type, username_list_str);
		else if (strcmp(strVal(command), "failover") == 0)
			mgr_manage_failover(command_type, username_list_str);
		else if (strcmp(strVal(command), "flush") == 0)
			mgr_manage_flush(command_type, username_list_str);
		else if (strcmp(strVal(command), "init") == 0)
			mgr_manage_init(command_type, username_list_str);
		else if (strcmp(strVal(command), "list") == 0)
			mgr_manage_list(command_type, username_list_str);
		else if (strcmp(strVal(command), "monitor") == 0)
			mgr_manage_monitor(command_type, username_list_str);
		else if (strcmp(strVal(command), "reset") == 0)
			mgr_manage_reset(command_type, username_list_str);
		else if (strcmp(strVal(command), "set") == 0)
			mgr_manage_set(command_type, username_list_str);
		else if (strcmp(strVal(command), "show") == 0)
			mgr_manage_show(command_type, username_list_str);
		else if (strcmp(strVal(command), "start") == 0)
			mgr_manage_start(command_type, username_list_str);
		else if (strcmp(strVal(command), "stop") == 0)
			mgr_manage_stop(command_type, username_list_str);
		else
			ereport(ERROR, (errmsg("unrecognized command type \"%s\"", strVal(command))));
	}

	if (command_type == PRIV_GRANT)
		PG_RETURN_TEXT_P(cstring_to_text("GRANT"));
	else
		PG_RETURN_TEXT_P(cstring_to_text("REVOKE"));
}

Datum mgr_priv_all_to_username(PG_FUNCTION_ARGS)
{
	List *username_list = NIL;
	Datum datum_username_list;
	char *username_list_str = NULL;

	char command_type = PG_GETARG_CHAR(0);
	Assert(command_type == PRIV_GRANT || command_type == PRIV_REVOKE);

	datum_username_list = PG_GETARG_DATUM(1);
	username_list = DecodeTextArrayToValueList(datum_username_list);

	mgr_check_username_valid(username_list);

	username_list_str = get_username_list_str(username_list);
	mgr_priv_all(command_type, username_list_str);

	if (command_type == PRIV_GRANT)
		PG_RETURN_TEXT_P(cstring_to_text("GRANT"));
	else
		PG_RETURN_TEXT_P(cstring_to_text("REVOKE"));
}

static void mgr_priv_all(char command_type, char *username_list_str)
{
	mgr_manage_add(command_type, username_list_str);
	mgr_manage_alter(command_type, username_list_str);
	mgr_manage_append(command_type, username_list_str);
	mgr_manage_clean(command_type, username_list_str);
	mgr_manage_deploy(command_type, username_list_str);
	mgr_manage_drop(command_type, username_list_str);
	mgr_manage_failover(command_type, username_list_str);
	mgr_manage_flush(command_type, username_list_str);
	mgr_manage_init(command_type, username_list_str);
	mgr_manage_list(command_type, username_list_str);
	mgr_manage_monitor(command_type, username_list_str);
	mgr_manage_reset(command_type, username_list_str);
	mgr_manage_set(command_type, username_list_str);
	mgr_manage_show(command_type, username_list_str);
	mgr_manage_start(command_type, username_list_str);
	mgr_manage_stop(command_type, username_list_str);

	return;
}

Datum mgr_priv_manage(PG_FUNCTION_ARGS)
{
	List *command_list = NIL;
	List *username_list = NIL;
	ListCell *lc = NULL;
	Value *command = NULL;
	char *username_list_str = NULL;
	Datum datum_command_list;
	Datum datum_username_list;

	char command_type = PG_GETARG_CHAR(0);
	Assert(command_type == PRIV_GRANT || command_type == PRIV_REVOKE);

	datum_command_list = PG_GETARG_DATUM(1);
	datum_username_list = PG_GETARG_DATUM(2);

	/* get command list and username list  */
	command_list = DecodeTextArrayToValueList(datum_command_list);
	username_list = DecodeTextArrayToValueList(datum_username_list);

	/* check command and username is valid */
	mgr_check_command_valid(command_list);
	mgr_check_username_valid(username_list);

	username_list_str = get_username_list_str(username_list);

	foreach(lc, command_list)
	{
		command = lfirst(lc);
		Assert(command && IsA(command, String));

		if (strcmp(strVal(command), "add") == 0)
			mgr_manage_add(command_type, username_list_str);
		else if (strcmp(strVal(command), "alter") == 0)
			mgr_manage_alter(command_type, username_list_str);
		else if (strcmp(strVal(command), "append") == 0)
			mgr_manage_append(command_type, username_list_str);
		else if (strcmp(strVal(command), "clean") == 0)
			mgr_manage_clean(command_type, username_list_str);
		else if (strcmp(strVal(command), "deploy") == 0)
			mgr_manage_deploy(command_type, username_list_str);
		else if (strcmp(strVal(command), "drop") == 0)
			mgr_manage_drop(command_type, username_list_str);
		else if (strcmp(strVal(command), "failover") == 0)
			mgr_manage_failover(command_type, username_list_str);
		else if (strcmp(strVal(command), "flush") == 0)
			mgr_manage_flush(command_type, username_list_str);
		else if (strcmp(strVal(command), "init") == 0)
			mgr_manage_init(command_type, username_list_str);
		else if (strcmp(strVal(command), "list") == 0)
			mgr_manage_list(command_type, username_list_str);
		else if (strcmp(strVal(command), "monitor") == 0)
			mgr_manage_monitor(command_type, username_list_str);
		else if (strcmp(strVal(command), "reset") == 0)
			mgr_manage_reset(command_type, username_list_str);
		else if (strcmp(strVal(command), "set") == 0)
			mgr_manage_set(command_type, username_list_str);
		else if (strcmp(strVal(command), "show") == 0)
			mgr_manage_show(command_type, username_list_str);
		else if (strcmp(strVal(command), "start") == 0)
			mgr_manage_start(command_type, username_list_str);
		else if (strcmp(strVal(command), "stop") == 0)
			mgr_manage_stop(command_type, username_list_str);
		else
			ereport(ERROR, (errmsg("unrecognized command type \"%s\"", strVal(command))));
	}

	if (command_type == PRIV_GRANT)
		PG_RETURN_TEXT_P(cstring_to_text("GRANT"));
	else
		PG_RETURN_TEXT_P(cstring_to_text("REVOKE"));
}

static void mgr_manage_flush(char command_type, char *user_list_str)
{
	StringInfoData commandsql;
	int exec_ret;
	int ret;
	initStringInfo(&commandsql);

	if (command_type == PRIV_GRANT)
	{
		/*grant execute on function func_name [, ...] to user_name [, ...] */
		appendStringInfoString(&commandsql, "GRANT EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_flush_host() ");
		appendStringInfoString(&commandsql, "TO ");
	}else if (command_type == PRIV_REVOKE)
	{
		/*revoke execute on function func_name [, ...] from user_name [, ...] */
		appendStringInfoString(&commandsql, "REVOKE EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_flush_host() ");
		appendStringInfoString(&commandsql, "FROM ");
	}
	else
		ereport(ERROR, (errmsg("command type is wrong: %c", command_type)));

	appendStringInfoString(&commandsql, user_list_str);

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("grant/revoke: SPI_connect failed: error code %d", ret)));

	exec_ret = SPI_execute(commandsql.data, false, 0);
	if (exec_ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("grant/revoke: SPI_execute failed: error code %d", exec_ret)));

	SPI_finish();
	return;
}

static void mgr_manage_stop_func(StringInfo commandsql)
{
	appendStringInfoString(commandsql, "mgr_stop_agent_all(), ");
	appendStringInfoString(commandsql, "mgr_stop_agent_hostnamelist(text[]), ");
	appendStringInfoString(commandsql, "mgr_stop_gtmcoor_master(\"any\"), ");
	appendStringInfoString(commandsql, "mgr_stop_gtmcoor_slave(\"any\"), ");
	appendStringInfoString(commandsql, "mgr_stop_cn_master(\"any\"), ");
	appendStringInfoString(commandsql, "mgr_stop_dn_master(\"any\"), ");
	appendStringInfoString(commandsql, "mgr_stop_dn_slave(\"any\")");

	return;
}

static void mgr_manage_stop_view(StringInfo commandsql)
{
	appendStringInfoString(commandsql, "adbmgr.stop_gtm_all, ");
	appendStringInfoString(commandsql, "adbmgr.stop_gtm_all_f, ");
	appendStringInfoString(commandsql, "adbmgr.stop_gtm_all_i, ");
	appendStringInfoString(commandsql, "adbmgr.stop_datanode_all, ");
	appendStringInfoString(commandsql, "adbmgr.stop_datanode_all_f, ");
	appendStringInfoString(commandsql, "adbmgr.stop_datanode_all_i, ");
	appendStringInfoString(commandsql, "adbmgr.stopall, ");
	appendStringInfoString(commandsql, "adbmgr.stopall_f, ");
	appendStringInfoString(commandsql, "adbmgr.stopall_i ");

	return;
}

static void mgr_manage_stop(char command_type, char *user_list_str)
{
	StringInfoData commandsql;
	int exec_ret;
	int ret;
	initStringInfo(&commandsql);

	if (command_type == PRIV_GRANT)
	{
		// grant execute on function func_name [, ...] to user_name [, ...];
		// grant select on schema.view [, ...] to user [, ...]
		appendStringInfoString(&commandsql, "GRANT EXECUTE ON FUNCTION ");
		mgr_manage_stop_func(&commandsql);
		appendStringInfoString(&commandsql, "TO ");
		appendStringInfoString(&commandsql, user_list_str);
		appendStringInfoString(&commandsql, ";");
		appendStringInfoString(&commandsql, "GRANT select ON ");
		mgr_manage_stop_view(&commandsql);
		appendStringInfoString(&commandsql, "TO ");
	}else if (command_type == PRIV_REVOKE)
	{
		// revoke execute on function func_name [, ...] from user_name [, ...];
		// revoke select on schema.view [, ...] from user [, ...]
		appendStringInfoString(&commandsql, "REVOKE EXECUTE ON FUNCTION ");
		mgr_manage_stop_func(&commandsql);
		appendStringInfoString(&commandsql, "FROM ");
		appendStringInfoString(&commandsql, user_list_str);
		appendStringInfoString(&commandsql, ";");
		appendStringInfoString(&commandsql, "REVOKE select ON ");
		mgr_manage_stop_view(&commandsql);
		appendStringInfoString(&commandsql, "FROM ");
	}
	else
		ereport(ERROR, (errmsg("command type is wrong: %c", command_type)));

	appendStringInfoString(&commandsql, user_list_str);

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("grant/revoke: SPI_connect failed: error code %d", ret)));

	exec_ret = SPI_execute(commandsql.data, false, 0);
	if (exec_ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("grant/revoke: SPI_execute failed: error code %d", exec_ret)));

	SPI_finish();
	return;
}

static void mgr_manage_deploy(char command_type, char *user_list_str)
{
	StringInfoData commandsql;
	int exec_ret;
	int ret;
	initStringInfo(&commandsql);

	if (command_type == PRIV_GRANT)
	{
		/*grant execute on function func_name [, ...] to user_name [, ...] */
		appendStringInfoString(&commandsql, "GRANT EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_deploy_all(cstring), ");
		appendStringInfoString(&commandsql, "mgr_deploy_hostnamelist(cstring, text[]) ");
		appendStringInfoString(&commandsql, "TO ");
	}else if (command_type == PRIV_REVOKE)
	{
		/*revoke execute on function func_name [, ...] from user_name [, ...] */
		appendStringInfoString(&commandsql, "REVOKE EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_deploy_all(cstring), ");
		appendStringInfoString(&commandsql, "mgr_deploy_hostnamelist(cstring, text[]) ");
		appendStringInfoString(&commandsql, "FROM ");
	}
	else
		ereport(ERROR, (errmsg("command type is wrong: %c", command_type)));

	appendStringInfoString(&commandsql, user_list_str);

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("grant/revoke: SPI_connect failed: error code %d", ret)));

	exec_ret = SPI_execute(commandsql.data, false, 0);
	if (exec_ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("grant/revoke: SPI_execute failed: error code %d", exec_ret)));

	SPI_finish();
	return;
}

static void mgr_manage_reset(char command_type, char *user_list_str)
{
	StringInfoData commandsql;
	int exec_ret;
	int ret;
	initStringInfo(&commandsql);

	if (command_type == PRIV_GRANT)
	{
		/*grant execute on function func_name [, ...] to user_name [, ...] */
		appendStringInfoString(&commandsql, "GRANT EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_reset_updateparm_func(\"char\", cstring, \"char\", boolean, \"any\") ");
		appendStringInfoString(&commandsql, "TO ");
	}else if (command_type == PRIV_REVOKE)
	{
		/*revoke execute on function func_name [, ...] from user_name [, ...] */
		appendStringInfoString(&commandsql, "REVOKE EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_reset_updateparm_func(\"char\", cstring, \"char\", boolean, \"any\") ");
		appendStringInfoString(&commandsql, "FROM ");
	}
	else
		ereport(ERROR, (errmsg("command type is wrong: %c", command_type)));

	appendStringInfoString(&commandsql, user_list_str);

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("grant/revoke: SPI_connect failed: error code %d", ret)));

	exec_ret = SPI_execute(commandsql.data, false, 0);
	if (exec_ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("grant/revoke: SPI_execute failed: error code %d", exec_ret)));

	SPI_finish();
	return;
}

static void mgr_manage_set(char command_type, char *user_list_str)
{
	StringInfoData commandsql;
	int exec_ret;
	int ret;
	initStringInfo(&commandsql);

	if (command_type == PRIV_GRANT)
	{
		/*grant execute on function func_name [, ...] to user_name [, ...] */
		appendStringInfoString(&commandsql, "GRANT EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_add_updateparm_func(\"char\", cstring, \"char\", boolean, \"any\"), ");
		appendStringInfoString(&commandsql, "mgr_set_init_cluster()");
		appendStringInfoString(&commandsql, "TO ");
	}else if (command_type == PRIV_REVOKE)
	{
		/*revoke execute on function func_name [, ...] from user_name [, ...] */
		appendStringInfoString(&commandsql, "REVOKE EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_add_updateparm_func(\"char\", cstring, \"char\", boolean, \"any\"), ");
		appendStringInfoString(&commandsql, "mgr_set_init_cluster()");
		appendStringInfoString(&commandsql, "FROM ");
	}
	else
		ereport(ERROR, (errmsg("command type is wrong: %c", command_type)));

	appendStringInfoString(&commandsql, user_list_str);

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("grant/revoke: SPI_connect failed: error code %d", ret)));

	exec_ret = SPI_execute(commandsql.data, false, 0);
	if (exec_ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("grant/revoke: SPI_execute failed: error code %d", exec_ret)));

	SPI_finish();
	return;
}

static void mgr_manage_alter(char command_type, char *user_list_str)
{
	StringInfoData commandsql;
	int exec_ret;
	int ret;
	initStringInfo(&commandsql);

	if (command_type == PRIV_GRANT)
	{
		/*grant execute on function func_name [, ...] to user_name [, ...] */
		appendStringInfoString(&commandsql, "GRANT EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_alter_host_func(boolean, cstring, \"any\"), ");
		appendStringInfoString(&commandsql, "mgr_alter_node_func(\"char\", cstring, \"any\") ");
		appendStringInfoString(&commandsql, "TO ");
	}else if (command_type == PRIV_REVOKE)
	{
		/*revoke execute on function func_name [, ...] from user_name [, ...] */
		appendStringInfoString(&commandsql, "REVOKE EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_alter_host_func(boolean, cstring, \"any\"), ");
		appendStringInfoString(&commandsql, "mgr_alter_node_func(\"char\", cstring, \"any\") ");
		appendStringInfoString(&commandsql, "FROM ");
	}
	else
		ereport(ERROR, (errmsg("command type is wrong: %c", command_type)));

	appendStringInfoString(&commandsql, user_list_str);

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("grant/revoke: SPI_connect failed: error code %d", ret)));

	exec_ret = SPI_execute(commandsql.data, false, 0);
	if (exec_ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("grant/revoke: SPI_execute failed: error code %d", exec_ret)));

	SPI_finish();
	return;
}

static void mgr_manage_drop(char command_type, char *user_list_str)
{
	StringInfoData commandsql;
	int exec_ret;
	int ret;
	initStringInfo(&commandsql);

	if (command_type == PRIV_GRANT)
	{
		/*grant execute on function func_name [, ...] to user_name [, ...] */
		appendStringInfoString(&commandsql, "GRANT EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_drop_host_func(boolean, \"any\"), ");
		appendStringInfoString(&commandsql, "mgr_drop_node_func(\"char\", \"any\"), ");
		appendStringInfoString(&commandsql, "mgr_drop_hba(\"any\") ");
		appendStringInfoString(&commandsql, "TO ");
	}else if (command_type == PRIV_REVOKE)
	{
		/*revoke execute on function func_name [, ...] from user_name [, ...] */
		appendStringInfoString(&commandsql, "REVOKE EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_drop_host_func(boolean, \"any\"), ");
		appendStringInfoString(&commandsql, "mgr_drop_node_func(\"char\", \"any\"), ");
		appendStringInfoString(&commandsql, "mgr_drop_hba(\"any\") ");
		appendStringInfoString(&commandsql, "FROM ");
	}
	else
		ereport(ERROR, (errmsg("command type is wrong: %c", command_type)));

	appendStringInfoString(&commandsql, user_list_str);

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("grant/revoke: SPI_connect failed: error code %d", ret)));

	exec_ret = SPI_execute(commandsql.data, false, 0);
	if (exec_ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("grant/revoke: SPI_execute failed: error code %d", exec_ret)));

	SPI_finish();
	return;
}

static void mgr_manage_add(char command_type, char *user_list_str)
{
	StringInfoData commandsql;
	int exec_ret;
	int ret;
	initStringInfo(&commandsql);

	if (command_type == PRIV_GRANT)
	{
		/*grant execute on function func_name [, ...] to user_name [, ...] */
		appendStringInfoString(&commandsql, "GRANT EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_add_host_func(boolean,cstring,\"any\"), ");
		appendStringInfoString(&commandsql, "mgr_add_node_func(boolean,\"char\",cstring,cstring,\"any\"), ");
		appendStringInfoString(&commandsql, "mgr_add_hba(\"any\") ");
		appendStringInfoString(&commandsql, "TO ");
	}else if (command_type == PRIV_REVOKE)
	{
		/*revoke execute on function func_name [, ...] from user_name [, ...] */
		appendStringInfoString(&commandsql, "REVOKE EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_add_host_func(boolean,cstring,\"any\"), ");
		appendStringInfoString(&commandsql, "mgr_add_node_func(boolean,\"char\",cstring,cstring, \"any\"), ");
		appendStringInfoString(&commandsql, "mgr_add_hba(\"any\") ");
		appendStringInfoString(&commandsql, "FROM ");
	}
	else
		ereport(ERROR, (errmsg("command type is wrong: %c", command_type)));

	appendStringInfoString(&commandsql, user_list_str);

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("grant/revoke: SPI_connect failed: error code %d", ret)));

	exec_ret = SPI_execute(commandsql.data, false, 0);
	if (exec_ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("grant/revoke: SPI_execute failed: error code %d", exec_ret)));

	SPI_finish();
	return;
}

static void mgr_manage_start(char command_type, char *user_list_str)
{
	StringInfoData commandsql;
	int exec_ret;
	int ret;
	initStringInfo(&commandsql);

	if (command_type == PRIV_GRANT)
	{
		// grant execute on function func_name [, ...] to user_name [, ...];
		// grant select on schema.view [, ...] to user [, ...]
		appendStringInfoString(&commandsql, "GRANT EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_start_agent_all(cstring), ");
		appendStringInfoString(&commandsql, "mgr_start_agent_hostnamelist(cstring,text[]), ");
		appendStringInfoString(&commandsql, "mgr_start_gtmcoor_master(\"any\"), ");
		appendStringInfoString(&commandsql, "mgr_start_gtmcoor_slave(\"any\"), ");
		appendStringInfoString(&commandsql, "mgr_start_cn_master(\"any\"), ");
		appendStringInfoString(&commandsql, "mgr_start_dn_master(\"any\"), ");
		appendStringInfoString(&commandsql, "mgr_start_dn_slave(\"any\") ");
		appendStringInfoString(&commandsql, "TO ");
		appendStringInfoString(&commandsql, user_list_str);
		appendStringInfoString(&commandsql, ";");
		appendStringInfoString(&commandsql, "GRANT select ON ");
		appendStringInfoString(&commandsql, "adbmgr.start_gtmcoor_all, ");
		appendStringInfoString(&commandsql, "adbmgr.start_datanode_all, ");
		appendStringInfoString(&commandsql, "adbmgr.startall ");
		appendStringInfoString(&commandsql, "TO ");
	}else if (command_type == PRIV_REVOKE)
	{
		// revoke execute on function func_name [, ...] from user_name [, ...];
		// revoke select on schema.view [, ...] from user [, ...]
		appendStringInfoString(&commandsql, "REVOKE EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_start_agent_all(cstring), ");
		appendStringInfoString(&commandsql, "mgr_start_agent_hostnamelist(cstring,text[]), ");
		appendStringInfoString(&commandsql, "mgr_start_gtmcoor_master(\"any\"), ");
		appendStringInfoString(&commandsql, "mgr_start_gtmcoor_slave(\"any\"), ");
		appendStringInfoString(&commandsql, "mgr_start_cn_master(\"any\"), ");
		appendStringInfoString(&commandsql, "mgr_start_dn_master(\"any\"), ");
		appendStringInfoString(&commandsql, "mgr_start_dn_slave(\"any\") ");
		appendStringInfoString(&commandsql, "FROM ");
		appendStringInfoString(&commandsql, user_list_str);
		appendStringInfoString(&commandsql, ";");
		appendStringInfoString(&commandsql, "REVOKE select ON ");
		appendStringInfoString(&commandsql, "adbmgr.start_gtmcoor_all, ");
		appendStringInfoString(&commandsql, "adbmgr.start_datanode_all, ");
		appendStringInfoString(&commandsql, "adbmgr.startall ");
		appendStringInfoString(&commandsql, "FROM ");
	}
	else
		ereport(ERROR, (errmsg("command type is wrong: %c", command_type)));

	appendStringInfoString(&commandsql, user_list_str);

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("grant/revoke: SPI_connect failed: error code %d", ret)));

	exec_ret = SPI_execute(commandsql.data, false, 0);
	if (exec_ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("grant/revoke: SPI_execute failed: error code %d", exec_ret)));

	SPI_finish();
	return;
}

static void mgr_manage_show(char command_type, char *user_list_str)
{
	StringInfoData commandsql;
	int exec_ret;
	int ret;
	initStringInfo(&commandsql);

	if (command_type == PRIV_GRANT)
	{
		/*grant execute on function func_name [, ...] to user_name [, ...] */
		appendStringInfoString(&commandsql, "GRANT EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_show_var_param(\"any\"), ");
		appendStringInfoString(&commandsql, "mgr_show_hba_all(\"any\") ");
		appendStringInfoString(&commandsql, "mgr_boottime_gtmcoor_all(), ");
		appendStringInfoString(&commandsql, "mgr_boottime_datanode_all(), ");
		appendStringInfoString(&commandsql, "mgr_boottime_coordinator_all(), ");
		appendStringInfoString(&commandsql, "mgr_boottime_nodetype_namelist(bigint, \"any\"), ");
		appendStringInfoString(&commandsql, "mgr_boottime_nodetype_all(bigint) ");
		appendStringInfoString(&commandsql, "TO ");
		appendStringInfoString(&commandsql, user_list_str);
		appendStringInfoString(&commandsql, ";");
		appendStringInfoString(&commandsql, "GRANT select ON ");
		appendStringInfoString(&commandsql, "adbmgr.boottime_all ");
		appendStringInfoString(&commandsql, "TO ");
	}else if (command_type == PRIV_REVOKE)
	{
		/*revoke execute on function func_name [, ...] from user_name [, ...] */
		appendStringInfoString(&commandsql, "REVOKE EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_show_var_param(\"any\"), ");
		appendStringInfoString(&commandsql, "mgr_show_hba_all(\"any\") ");
		appendStringInfoString(&commandsql, "mgr_boottime_gtmcoor_all(), ");
		appendStringInfoString(&commandsql, "mgr_boottime_datanode_all(), ");
		appendStringInfoString(&commandsql, "mgr_boottime_coordinator_all(), ");
		appendStringInfoString(&commandsql, "mgr_boottime_nodetype_namelist(bigint, \"any\"), ");
		appendStringInfoString(&commandsql, "mgr_boottime_nodetype_all(bigint) ");
		appendStringInfoString(&commandsql, "FROM ");
		appendStringInfoString(&commandsql, user_list_str);
		appendStringInfoString(&commandsql, ";");
		appendStringInfoString(&commandsql, "REVOKE select ON ");
		appendStringInfoString(&commandsql, "adbmgr.boottime_all ");
		appendStringInfoString(&commandsql, "FROM ");
	}
	else
		ereport(ERROR, (errmsg("command type is wrong: %c", command_type)));

	appendStringInfoString(&commandsql, user_list_str);

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("grant/revoke: SPI_connect failed: error code %d", ret)));

	exec_ret = SPI_execute(commandsql.data, false, 0);
	if (exec_ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("grant/revoke: SPI_execute failed: error code %d", exec_ret)));

	SPI_finish();
	return;
}

static void mgr_manage_monitor(char command_type, char *user_list_str)
{
	StringInfoData commandsql;
	int exec_ret;
	int ret;
	initStringInfo(&commandsql);

	if (command_type == PRIV_GRANT)
	{
		// grant execute on function func_name [, ...] to user_name [, ...];
		// grant select on schema.view [, ...] to user [, ...]
		appendStringInfoString(&commandsql, "GRANT EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_monitor_agent_all(), ");
		appendStringInfoString(&commandsql, "mgr_monitor_agent_hostlist(text[]), ");
		appendStringInfoString(&commandsql, "mgr_monitor_gtmcoor_all(), ");
		appendStringInfoString(&commandsql, "mgr_monitor_datanode_all(), ");
		appendStringInfoString(&commandsql, "mgr_monitor_nodetype_namelist(bigint, \"any\"), ");
		appendStringInfoString(&commandsql, "mgr_monitor_nodetype_all(bigint), ");
		appendStringInfoString(&commandsql, "mgr_monitor_ha() ");
		appendStringInfoString(&commandsql, "TO ");
		appendStringInfoString(&commandsql, user_list_str);
		appendStringInfoString(&commandsql, ";");
		appendStringInfoString(&commandsql, "GRANT select ON ");
		appendStringInfoString(&commandsql, "adbmgr.monitor_all ");
		appendStringInfoString(&commandsql, "TO ");
	}else if (command_type == PRIV_REVOKE)
	{
		// revoke execute on function func_name [, ...] from user_name [, ...];
		// revoke select on schema.view [, ...] from user [, ...]
		appendStringInfoString(&commandsql, "REVOKE EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_monitor_agent_all(), ");
		appendStringInfoString(&commandsql, "mgr_monitor_agent_hostlist(text[]), ");
		appendStringInfoString(&commandsql, "mgr_monitor_gtmcoor_all(), ");
		appendStringInfoString(&commandsql, "mgr_monitor_datanode_all(), ");
		appendStringInfoString(&commandsql, "mgr_monitor_nodetype_namelist(bigint, \"any\"), ");
		appendStringInfoString(&commandsql, "mgr_monitor_nodetype_all(bigint), ");
		appendStringInfoString(&commandsql, "mgr_monitor_ha() ");
		appendStringInfoString(&commandsql, "FROM ");
		appendStringInfoString(&commandsql, user_list_str);
		appendStringInfoString(&commandsql, ";");
		appendStringInfoString(&commandsql, "REVOKE select ON ");
		appendStringInfoString(&commandsql, "adbmgr.monitor_all ");
		appendStringInfoString(&commandsql, "FROM ");
	}
	else
		ereport(ERROR, (errmsg("command type is wrong: %c", command_type)));

	appendStringInfoString(&commandsql, user_list_str);

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("grant/revoke: SPI_connect failed: error code %d", ret)));

	exec_ret = SPI_execute(commandsql.data, false, 0);
	if (exec_ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("grant/revoke: SPI_execute failed: error code %d", exec_ret)));

	SPI_finish();
	return;
}

static void mgr_manage_list(char command_type, char *user_list_str)
{
	StringInfoData commandsql;
	int exec_ret;
	int ret;
	initStringInfo(&commandsql);

	if (command_type == PRIV_GRANT)
	{
		// grant execute on function func_name [, ...] to user_name [, ...];
		// grant select on schema.view [, ...] to user [, ...]
		appendStringInfoString(&commandsql, "GRANT EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_list_hba_by_name(\"any\") ");
		appendStringInfoString(&commandsql, "TO ");
		appendStringInfoString(&commandsql, user_list_str);
		appendStringInfoString(&commandsql, ";");
		appendStringInfoString(&commandsql, "GRANT select ON ");
		appendStringInfoString(&commandsql, "adbmgr.host, ");
		appendStringInfoString(&commandsql, "adbmgr.node, ");
		appendStringInfoString(&commandsql, "adbmgr.updateparm, ");
		appendStringInfoString(&commandsql, "adbmgr.hba ");
		appendStringInfoString(&commandsql, "TO ");
	}else if (command_type == PRIV_REVOKE)
	{
		// revoke execute on function func_name [, ...] from user_name [, ...];
		// revoke select on schema.view [, ...] from user [, ...]
		appendStringInfoString(&commandsql, "REVOKE EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_list_hba_by_name(\"any\") ");
		appendStringInfoString(&commandsql, "FROM ");
		appendStringInfoString(&commandsql, user_list_str);
		appendStringInfoString(&commandsql, ";");
		appendStringInfoString(&commandsql, "REVOKE select ON ");
		appendStringInfoString(&commandsql, "adbmgr.host, ");
		appendStringInfoString(&commandsql, "adbmgr.node, ");
		appendStringInfoString(&commandsql, "adbmgr.updateparm, ");
		appendStringInfoString(&commandsql, "adbmgr.hba ");
		appendStringInfoString(&commandsql, "FROM ");
	}
	else
		ereport(ERROR, (errmsg("command type is wrong: %c", command_type)));

	appendStringInfoString(&commandsql, user_list_str);

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("grant/revoke: SPI_connect failed: error code %d", ret)));

	exec_ret = SPI_execute(commandsql.data, false, 0);
	if (exec_ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("grant/revoke: SPI_execute failed: error code %d", exec_ret)));

	SPI_finish();
	return;
}

static void mgr_manage_clean(char command_type, char *user_list_str)
{
	StringInfoData commandsql;
	int exec_ret;
	int ret;
	initStringInfo(&commandsql);

	if (command_type == PRIV_GRANT)
	{
		/*grant execute on function func_name [, ...] to user_name [, ...] */
		appendStringInfoString(&commandsql, "GRANT EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_clean_all(), ");
		appendStringInfoString(&commandsql, "mgr_clean_node(\"any\") ");
		appendStringInfoString(&commandsql, "TO ");
	}else if (command_type == PRIV_REVOKE)
	{
		/*revoke execute on function func_name [, ...] from user_name [, ...] */
		appendStringInfoString(&commandsql, "REVOKE EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_clean_all(), ");
		appendStringInfoString(&commandsql, "mgr_clean_node(\"any\") ");
		appendStringInfoString(&commandsql, "FROM ");
	}
	else
		ereport(ERROR, (errmsg("command type is wrong: %c", command_type)));

	appendStringInfoString(&commandsql, user_list_str);

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("grant/revoke: SPI_connect failed: error code %d", ret)));

	exec_ret = SPI_execute(commandsql.data, false, 0);
	if (exec_ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("grant/revoke: SPI_execute failed: error code %d", exec_ret)));

	SPI_finish();
	return;
}

static void mgr_manage_failover(char command_type, char *user_list_str)
{
	StringInfoData commandsql;
	int exec_ret;
	int ret;
	initStringInfo(&commandsql);

	if (command_type == PRIV_GRANT)
	{
		/*grant execute on function func_name [, ...] to user_name [, ...] */
		appendStringInfoString(&commandsql, "GRANT EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_failover_one_dn(cstring, boolean), ");
		appendStringInfoString(&commandsql, "mgr_failover_gtm(cstring,boolean) ");
		appendStringInfoString(&commandsql, "TO ");
	}else if (command_type == PRIV_REVOKE)
	{
		/*revoke execute on function func_name [, ...] from user_name [, ...] */
		appendStringInfoString(&commandsql, "REVOKE EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_failover_one_dn(cstring, boolean), ");
		appendStringInfoString(&commandsql, "mgr_failover_gtm(cstring,boolean) ");
		appendStringInfoString(&commandsql, "FROM ");
	}
	else
		ereport(ERROR, (errmsg("command type is wrong: %c", command_type)));

	appendStringInfoString(&commandsql, user_list_str);

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("grant/revoke: SPI_connect failed: error code %d", ret)));

	exec_ret = SPI_execute(commandsql.data, false, 0);
	if (exec_ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("grant/revoke: SPI_execute failed: error code %d", exec_ret)));

	SPI_finish();
	return;
}

static void mgr_manage_append(char command_type, char *user_list_str)
{
	StringInfoData commandsql;
	int exec_ret;
	int ret;
	initStringInfo(&commandsql);

	if (command_type == PRIV_GRANT)
	{
		/*grant execute on function func_name [, ...] to user_name [, ...] */
		appendStringInfoString(&commandsql, "GRANT EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_append_dnmaster(cstring), ");
		appendStringInfoString(&commandsql, "mgr_append_dnslave(cstring) ");
		appendStringInfoString(&commandsql, "mgr_append_coordmaster(cstring), ");
		appendStringInfoString(&commandsql, "mgr_append_agtmslave(cstring) ");
		appendStringInfoString(&commandsql, "TO ");
	}else if (command_type == PRIV_REVOKE)
	{
		/*revoke execute on function func_name [, ...] from user_name [, ...] */
		appendStringInfoString(&commandsql, "REVOKE EXECUTE ON FUNCTION ");
		appendStringInfoString(&commandsql, "mgr_append_dnmaster(cstring), ");
		appendStringInfoString(&commandsql, "mgr_append_dnslave(cstring), ");
		appendStringInfoString(&commandsql, "mgr_append_coordmaster(cstring), ");
		appendStringInfoString(&commandsql, "mgr_append_agtmslave(cstring) ");
		appendStringInfoString(&commandsql, "FROM ");
	}
	else
		ereport(ERROR, (errmsg("command type is wrong: %c", command_type)));

	appendStringInfoString(&commandsql, user_list_str);

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("grant/revoke: SPI_connect failed: error code %d", ret)));

	exec_ret = SPI_execute(commandsql.data, false, 0);
	if (exec_ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("grant/revoke: SPI_execute failed: error code %d", exec_ret)));

	SPI_finish();
	return;
}

static void mgr_manage_init(char command_type, char *user_list_str)
{
	StringInfoData commandsql;
	int exec_ret;
	int ret;
	initStringInfo(&commandsql);

	if (command_type == PRIV_GRANT)
	{
		/*grant select on schema.view [, ...] to user [, ...] */
		appendStringInfoString(&commandsql, "GRANT select ON adbmgr.initall TO ");
	}else if (command_type == PRIV_REVOKE)
	{
		/*revoke select on schema.view [, ...] from user [, ...] */
		appendStringInfoString(&commandsql, "REVOKE select ON adbmgr.initall FROM ");
	}
	else
		ereport(ERROR, (errmsg("command type is wrong: %c", command_type)));

	appendStringInfoString(&commandsql, user_list_str);

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("grant/revoke: SPI_connect failed: error code %d", ret)));

	exec_ret = SPI_execute(commandsql.data, false, 0);
	if (exec_ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("grant/revoke: SPI_execute failed: error code %d", exec_ret)));

	SPI_finish();
	return;
}

static char *get_username_list_str(List *username_list)
{
	StringInfoData username_list_str;
	ListCell *lc = NULL;
	Value *username = NULL;

	initStringInfo(&username_list_str);

	foreach(lc, username_list)
	{
		username = lfirst(lc);
		Assert(username && IsA(username, String));

		/* add double quotes for the user name */
		/* in order to make the user name in digital or pure digital effective */
		appendStringInfoChar(&username_list_str, '"');
		appendStringInfoString(&username_list_str, strVal(username));
		appendStringInfoChar(&username_list_str, '"');

		appendStringInfoChar(&username_list_str, ',');
	}

	username_list_str.data[username_list_str.len - 1] = '\0';
	return username_list_str.data;
}

static void mgr_check_command_valid(List *command_list)
{
	ListCell *lc = NULL;
	Value *command = NULL;
	char *command_str = NULL;

	foreach(lc, command_list)
	{
		command = lfirst(lc);
		Assert(command && IsA(command, String));

		command_str = strVal(command);

		if (strcmp(command_str, "add") == 0      ||
			strcmp(command_str, "alter") == 0    ||
			strcmp(command_str, "append") == 0   ||
			strcmp(command_str, "clean") == 0    ||
			strcmp(command_str, "deploy") == 0   ||
			strcmp(command_str, "drop") == 0     ||
			strcmp(command_str, "failover") == 0 ||
			strcmp(command_str, "flush") == 0    ||
			strcmp(command_str, "init") == 0     ||
			strcmp(command_str, "list") == 0     ||
			strcmp(command_str, "monitor") == 0  ||
			strcmp(command_str, "reset") == 0    ||
			strcmp(command_str, "set") == 0      ||
			strcmp(command_str, "show") == 0     ||
			strcmp(command_str, "start") == 0    ||
			strcmp(command_str, "stop") == 0 )
			continue;
		else
			ereport(ERROR, (errmsg("unrecognized command type \"%s\"", command_str)));
	}

	return ;
}

static void mgr_check_username_valid(List *username_list)
{
	ListCell *lc = NULL;
	Value *username = NULL;
	Oid oid;

	foreach(lc, username_list)
	{
		username = lfirst(lc);
		Assert(username && IsA(username, String));

		oid = GetSysCacheOid1(AUTHNAME, CStringGetDatum(strVal(username)));
		if (!OidIsValid(oid))
			ereport(ERROR, (errmsg("role \"%s\" does not exist", strVal(username))));
		else
			continue;
	}

	return ;
}

Datum mgr_list_acl_all(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	InitAclInfo *info;
	HeapTuple tup;
	HeapTuple tup_result;
	char *username;
	Form_pg_authid pg_authid;
	StringInfoData acl;

	initStringInfo(&acl);

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		info = palloc(sizeof(*info));
		info->rel_authid = heap_open(AuthIdRelationId, AccessShareLock);
		info->rel_scan = heap_beginscan_catalog(info->rel_authid, 0, NULL);
		info->lcp =NULL;
		/* save info */
		funcctx->user_fctx = info;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	Assert(funcctx);
	info = funcctx->user_fctx;
	Assert(info);

	tup = heap_getnext(info->rel_scan, ForwardScanDirection);
	if(tup == NULL)
	{
		/* end of row */
		heap_endscan(info->rel_scan);
		heap_close(info->rel_authid, AccessShareLock);
		pfree(info);
		SRF_RETURN_DONE(funcctx);
	}

	pg_authid = (Form_pg_authid)GETSTRUCT(tup);
	Assert(pg_authid);

	resetStringInfo(&acl);
	username = NameStr(pg_authid->rolname);
	mgr_get_acl_by_username(username, &acl);
	tup_result = build_list_acl_command_tuple(&(pg_authid->rolname), acl.data);

	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}

/**
 * @brief
 * @note
 * @retval
 */
Datum mgr_list_nodesize_all(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	NodeSizeInfo *info;
	HeapTuple tup;
	HeapTuple tup_result;
	Form_mgr_node mgr_node;
	ManagerAgent *ma;
	GetAgentCmdRst getAgentCmdRst;
	StringInfoData message;
	char * nodepath;
	bool isNull;
	int checkNodeName;
	Datum datumPath;
	bool checkSoftLink = PG_GETARG_BOOL(PG_NARGS()-1);
	int i;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		info = palloc(sizeof(*info));
		info->rel_host = heap_open(NodeRelationId, AccessShareLock);
		info->rel_scan = heap_beginscan_catalog(info->rel_host, 0, NULL);
        info->lcp = NULL;

		/* save info */
		funcctx->user_fctx = info;
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	Assert(funcctx);
	info = funcctx->user_fctx;
	Assert(info);

	do
	{
		tup = heap_getnext(info->rel_scan, ForwardScanDirection);
		if(tup == NULL)
		{
			/* end of row */
			heap_endscan(info->rel_scan);
			heap_close(info->rel_host, AccessShareLock);
			pfree(info);
			SRF_RETURN_DONE(funcctx);
		}
		mgr_node = (Form_mgr_node)GETSTRUCT(tup);
		Assert(mgr_node);

		checkNodeName = 1;	// check column node name;
		for(i=0; i<(PG_NARGS()-1); i++)
		{
			if((checkNodeName = strcmp(PG_GETARG_CSTRING(i), (mgr_node->nodename).data)) == 0)
				break;
		}
	} while(checkNodeName != 0 && PG_NARGS() > 1);	//checke nodeName && check argument count

	// *get column nodepath
	datumPath = heap_getattr(tup, Anum_mgr_node_nodepath, RelationGetDescr(info->rel_host), &isNull);
	if (isNull)
	{
		ereport(NOTICE,  (errcode(ERRCODE_DUPLICATE_OBJECT),
				errmsg("node \"%s\" nodepath is null", NameStr(mgr_node->nodename))));
		PG_RETURN_BOOL(false);
	}

	nodepath = text_to_cstring((DatumGetTextP(datumPath)));

	/* test is running ? */
	ma = ma_connect_hostoid(mgr_node->nodehost);
	if(!ma_isconnected(ma))
	{
		tup_result = build_list_nodesize_tuple(&(mgr_node->nodename),
												   mgr_node->nodetype,
												   mgr_node->nodeport,
												   nodepath,
												   0 );
		ma_close(ma);
		ereport(INFO, (errmsg("ndoename \"%s\" : agent is not running", NameStr(mgr_node->nodename))));
	}else
	{
		initStringInfo(&message);
		initStringInfo(&(getAgentCmdRst.description));

		/*send cmd*/
		ma_beginmessage(&message, AGT_MSG_COMMAND);
		if(!checkSoftLink)	//true or false check softlink
			ma_sendbyte(&message, AGT_CMD_LIST_NODESIZE);
		else
			ma_sendbyte(&message, AGT_CMD_LIST_NODESIZE_CHECK_SOFTLINK);
		ma_sendstring(&message, nodepath);	//ndoepath
		ma_endmessage(&message, ma);
		if (!ma_flush(ma, true))
		{
			getAgentCmdRst.ret = false;
			appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
			ma_close(ma);
			tup_result = build_common_command_tuple(&(mgr_node->nodename)
			, getAgentCmdRst.ret, getAgentCmdRst.description.data);
		}
		else
		{
			mgr_recv_msg_for_nodesize(ma, &getAgentCmdRst);
			ma_close(ma);
			//check result data
			if (strlen(getAgentCmdRst.description.data) > 20)
				ereport(INFO, (errmsg("ndoename \"%s\" %s", NameStr(mgr_node->nodename), getAgentCmdRst.description.data)));
			tup_result = build_list_nodesize_tuple(&(mgr_node->nodename),
												   mgr_node->nodetype,
												   mgr_node->nodeport,
												   nodepath,
												   pg_strtouint64(getAgentCmdRst.description.data, NULL, 10));
			//free getAgentCmdRst.description.data
			if(getAgentCmdRst.description.data)
				pfree(getAgentCmdRst.description.data);
			//free nodepath
			pfree(nodepath);
			nodepath = NULL;
		}
	}

	//return tuple_result
	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}

static void mgr_get_acl_by_username(char *username, StringInfo acl)
{
	Oid roleid;

	roleid = mgr_get_role_oid_or_public(username);
	if (superuser_arg(roleid))
	{
		appendStringInfo(acl, "superuser");
		return ;
	}

	if (mgr_acl_add(username))
		appendStringInfo(acl, "add ");

	if (mgr_acl_alter(username))
		appendStringInfo(acl, "alter ");

	if (mgr_acl_append(username))
		appendStringInfo(acl, "append ");

	if (mgr_acl_clean(username))
		appendStringInfo(acl, "clean ");

	if (mgr_acl_deploy(username))
		appendStringInfo(acl, "deploy ");

	if (mgr_acl_drop(username))
		appendStringInfo(acl, "drop ");

	if (mgr_acl_failover(username))
		appendStringInfo(acl, "failover ");

	if (mgr_acl_flush(username))
		appendStringInfo(acl, "flush ");

	if (mgr_acl_init(username))
		appendStringInfo(acl, "init ");

	if (mgr_acl_list(username))
		appendStringInfo(acl, "list ");

	if (mgr_acl_monitor(username))
		appendStringInfo(acl, "monitor ");

	if (mgr_acl_reset(username))
		appendStringInfo(acl, "reset ");

	if (mgr_acl_set(username))
		appendStringInfo(acl, "set ");

	if (mgr_acl_show(username))
		appendStringInfo(acl, "show ");

	if (mgr_acl_start(username))
		appendStringInfo(acl, "start ");

	if (mgr_acl_stop(username))
		appendStringInfo(acl, "stop ");

	return;
}

static bool mgr_acl_flush(char *username)
{
	return mgr_has_func_priv(username, "mgr_flush_host()", "execute");
}

static bool mgr_acl_stop(char *username)
{
	bool f1, f2, f3, f4, f5, f6, f7;
	bool t1, t2, t3, t4, t5, t6, t7, t8, t9;

	f1 = mgr_has_func_priv(username, "mgr_stop_agent_all()", "execute");
	f2 = mgr_has_func_priv(username, "mgr_stop_agent_hostnamelist(text[])", "execute");
	f3 = mgr_has_func_priv(username, "mgr_stop_gtmcoor_master(\"any\")", "execute");
	f4 = mgr_has_func_priv(username, "mgr_stop_gtmcoor_slave(\"any\")", "execute");
	f5 = mgr_has_func_priv(username, "mgr_stop_cn_master(\"any\")", "execute");
	f6 = mgr_has_func_priv(username, "mgr_stop_dn_master(\"any\")", "execute");
	f7 = mgr_has_func_priv(username, "mgr_stop_dn_slave(\"any\")", "execute");

	t1 = mgr_has_table_priv(username, "adbmgr.stop_gtm_all", "select");
	t2 = mgr_has_table_priv(username, "adbmgr.stop_gtm_all_f", "select");
	t3 = mgr_has_table_priv(username, "adbmgr.stop_gtm_all_i", "select");
	t4 = mgr_has_table_priv(username, "adbmgr.stop_datanode_all", "select");
	t5 = mgr_has_table_priv(username, "adbmgr.stop_datanode_all_f", "select");
	t6 = mgr_has_table_priv(username, "adbmgr.stop_datanode_all_i", "select");
	t7 = mgr_has_table_priv(username, "adbmgr.stopall", "select");
	t8 = mgr_has_table_priv(username, "adbmgr.stopall_f", "select");
	t9 = mgr_has_table_priv(username, "adbmgr.stopall_i", "select");

	return (f1 && f2 && f3 && f4 && f5 && f6 && f7 &&
			t1 && t2 && t3 && t4 && t5 && t6 && t7 && t8 && t9);
}

static bool mgr_acl_deploy(char *username)
{
	bool f1, f2;

	f1 = mgr_has_func_priv(username, "mgr_deploy_all(cstring)", "execute");
	f2 = mgr_has_func_priv(username, "mgr_deploy_hostnamelist(cstring, text[])", "execute");

	return (f1 && f2);
}

bool mgr_has_priv_reset(void)
{
	bool f1;

	f1 = mgr_has_function_privilege_name("mgr_reset_updateparm_func(\"char\", cstring, \"char\", boolean, \"any\")",
										"execute");
	return (f1);
}

static bool mgr_acl_reset(char *username)
{
	bool f1;

	f1 = mgr_has_func_priv(username,
							"mgr_reset_updateparm_func(\"char\", cstring, \"char\", boolean, \"any\")",
							"execute");

	return f1;
}

bool mgr_has_priv_set(void)
{
	bool f1;

	f1 = mgr_has_function_privilege_name("mgr_add_updateparm_func(\"char\", cstring, \"char\", boolean, \"any\")",
										"execute");
	return (f1);
}

static bool mgr_acl_set(char *username)
{
	bool f1;

	f1 = mgr_has_func_priv(username,
							"mgr_add_updateparm_func(\"char\", cstring, \"char\", boiolean, \"any\")",
							"execute");

	return f1;
}

bool mgr_has_priv_alter(void)
{
	bool f1, f2;

	f1 = mgr_has_function_privilege_name("mgr_alter_host_func(boolean, cstring, \"any\")", "execute");
	f2 = mgr_has_function_privilege_name("mgr_alter_node_func(\"char\", cstring, \"any\")", "execute");

	return (f1 && f2);
}

static bool mgr_acl_alter(char *username)
{
	bool f1, f2;

	f1 = mgr_has_func_priv(username, "mgr_alter_host_func(boolean, cstring, \"any\")", "execute");
	f2 = mgr_has_func_priv(username, "mgr_alter_node_func(\"char\", cstring, \"any\")", "execute");

	return (f1 && f2);
}

bool mgr_has_priv_drop(void)
{
	bool f1, f2, f3;

	f1 = mgr_has_function_privilege_name("mgr_drop_host_func(boolean, \"any\")", "execute");
	f2 = mgr_has_function_privilege_name("mgr_drop_node_func(\"char\", \"any\")", "execute");
	f3 = mgr_has_function_privilege_name("mgr_drop_hba(\"any\")", "execute");

	return (f1 && f2 && f3);
}

static bool mgr_acl_drop(char *username)
{
	bool f1, f2, f3;

	f1 = mgr_has_func_priv(username, "mgr_drop_host_func(boolean,\"any\")", "execute");
	f2 = mgr_has_func_priv(username, "mgr_drop_node_func(\"char\",\"any\")", "execute");
	f3 = mgr_has_func_priv(username, "mgr_drop_hba(\"any\")", "execute");

	return (f1 && f2 && f3);
}

bool mgr_has_priv_add(void)
{
	bool f1, f2, f3;

	f1 = mgr_has_function_privilege_name("mgr_add_host_func(boolean,cstring,\"any\")", "execute");
	f2 = mgr_has_function_privilege_name("mgr_add_node_func(boolean,\"char\",cstring,cstring,\"any\")", "execute");
	f3 = mgr_has_function_privilege_name("mgr_add_hba(\"any\")", "execute");

	return (f1 && f2 && f3);
}

static bool mgr_acl_add(char *username)
{
	bool f1, f2, f3;

	f1 = mgr_has_func_priv(username, "mgr_add_host_func(boolean,cstring,\"any\")", "execute");
	f2 = mgr_has_func_priv(username, "mgr_add_node_func(boolean,\"char\",cstring,cstring,\"any\")", "execute");
	f3 = mgr_has_func_priv(username, "mgr_add_hba(\"any\")", "execute");

	return (f1 && f2 && f3);
}

static bool mgr_acl_start(char *username)
{
	bool f1, f2, f3, f4, f5, f6, f7;
	bool t1, t2, t3;

	f1 = mgr_has_func_priv(username, "mgr_start_agent_all(cstring)", "execute");
	f2 = mgr_has_func_priv(username, "mgr_stop_agent_hostnamelist(text[])", "execute");
	f3 = mgr_has_func_priv(username, "mgr_start_gtmcoor_master(\"any\")", "execute");
	f4 = mgr_has_func_priv(username, "mgr_start_gtmcoor_slave(\"any\")", "execute");
	f5 = mgr_has_func_priv(username, "mgr_start_cn_master(\"any\")", "execute");
	f6 = mgr_has_func_priv(username, "mgr_start_dn_master(\"any\")", "execute");
	f7 = mgr_has_func_priv(username, "mgr_start_dn_slave(\"any\")", "execute");

	t1 = mgr_has_table_priv(username, "adbmgr.start_gtmcoor_all", "select");
	t2 = mgr_has_table_priv(username, "adbmgr.start_datanode_all", "select");
	t3 = mgr_has_table_priv(username, "adbmgr.startall", "select");

	return (f1 && f2 && f3 && f4 && f5 && f6 && f7 && t1 && t2 && t3);
}

static bool mgr_acl_show(char *username)
{
	bool f1, f2, f3, f4, f5, f6, f7;
	bool t1;

	f1 = mgr_has_func_priv(username, "mgr_show_var_param(\"any\")", "execute");
	f2 = mgr_has_func_priv(username, "mgr_show_hba_all(\"any\")", "execute");

	f3 = mgr_has_func_priv(username, "mgr_boottime_nodetype_all(bigint)", "execute");
	f4 = mgr_has_func_priv(username, "mgr_boottime_nodetype_namelist(bigint, \"any\")", "execute");
	f5 = mgr_has_func_priv(username, "mgr_boottime_gtmcoor_all()", "execute");
	f6 = mgr_has_func_priv(username, "mgr_boottime_datanode_all()", "execute");
	f7 = mgr_has_func_priv(username, "mgr_boottime_coordinator_all()", "execute");
	
	t1 = mgr_has_table_priv(username, "adbmgr.boottime_all", "select");
	return (f1 && f2 && f3 && f4 && f5 && f6 && f7 && t1);
}

static bool mgr_acl_monitor(char *username)
{
	bool f1, f2, f3, f4, f5, f6, f7;
	bool t1;

	f1 = mgr_has_func_priv(username, "mgr_monitor_agent_all()", "execute");
	f2 = mgr_has_func_priv(username, "mgr_monitor_agent_hostlist(text[])", "execute");
	f3 = mgr_has_func_priv(username, "mgr_monitor_gtmcoor_all()", "execute");
	f4 = mgr_has_func_priv(username, "mgr_monitor_datanode_all()", "execute");
	f5 = mgr_has_func_priv(username, "mgr_monitor_nodetype_namelist(bigint, \"any\")", "execute");
	f6 = mgr_has_func_priv(username, "mgr_monitor_nodetype_all(bigint)", "execute");
	f7 = mgr_has_func_priv(username, "mgr_monitor_ha()", "execute");

	t1 = mgr_has_table_priv(username, "adbmgr.monitor_all", "select");

	return (f1 && f2 && f3 && f4 && f5 && f6 && f7 && t1);
}

static bool mgr_acl_list(char *username)
{
	bool func;
	bool table_host;
	bool table_node;
	bool table_parm;
	bool table_hba;

	func       = mgr_has_func_priv(username, "mgr_list_hba_by_name(\"any\")", "execute");
	table_host = mgr_has_table_priv(username, "adbmgr.host", "select");
	table_node = mgr_has_table_priv(username, "adbmgr.node", "select");
	table_parm = mgr_has_table_priv(username, "adbmgr.updateparm", "select");
	table_hba  = mgr_has_table_priv(username, "adbmgr.hba", "select");

	return (func && table_host &&
			table_node && table_parm &&
			table_hba);
}

static bool mgr_acl_append(char *username)
{
	bool func_dnmaster;
	bool func_dnslave;
	bool func_cdmaster;
	bool func_gtmslave;

	func_dnmaster = mgr_has_func_priv(username, "mgr_append_dnmaster(cstring)", "execute");
	func_dnslave  = mgr_has_func_priv(username, "mgr_append_dnslave(cstring)", "execute");
	func_cdmaster = mgr_has_func_priv(username, "mgr_append_coordmaster(cstring)", "execute");
	func_gtmslave = mgr_has_func_priv(username, "mgr_append_agtmslave(cstring)", "execute");

	return (func_dnmaster && func_dnslave && func_cdmaster &&
			func_gtmslave);
}

static bool mgr_acl_failover(char *username)
{
	bool func_gtm;
	bool func_dn;

	func_dn  = mgr_has_func_priv(username, "mgr_failover_one_dn(cstring,boolean)", "execute");
	func_gtm = mgr_has_func_priv(username, "mgr_failover_gtm(cstring,boolean)", "execute");

	return (func_gtm && func_dn);
}

static bool mgr_acl_clean(char *username)
{
	bool f1, f2;

	f1 = mgr_has_func_priv(username, "mgr_clean_all()", "execute");
	f2 = mgr_has_func_priv(username, "mgr_clean_node (\"any\")", "execute");
	return (f1 && f2);
}

static bool mgr_acl_init(char *username)
{
	return mgr_has_table_priv(username, "adbmgr.initall", "select");
}

static bool mgr_has_table_priv(char *rolename, char *tablename, char *priv_type)
{
	Datum aclresult;
	NameData name;
	namestrcpy(&name, rolename);

	aclresult = DirectFunctionCall3(has_table_privilege_name_name,
									NameGetDatum(&name),
									CStringGetTextDatum(tablename),
									CStringGetTextDatum(priv_type));

	return DatumGetBool(aclresult);
}

static bool mgr_has_func_priv(char *rolename, char *funcname, char *priv_type)
{
	Datum aclresult;
	NameData name;
	namestrcpy(&name, rolename);

	aclresult = DirectFunctionCall3(has_function_privilege_name_name,
									NameGetDatum(&name),
									CStringGetTextDatum(funcname),
									CStringGetTextDatum(priv_type));

	return DatumGetBool(aclresult);
}

static List *get_username_list(void)
{
	Relation pg_authid_rel;
	HeapScanDesc rel_scan;
	HeapTuple tuple;

	Form_pg_authid pg_authid;
	List *username_list = NULL;

	pg_authid_rel = heap_open(AuthIdRelationId, AccessShareLock);
	rel_scan =  heap_beginscan_catalog(pg_authid_rel, 0, NULL);

	while ((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		pg_authid = (Form_pg_authid)GETSTRUCT(tuple);
		Assert(pg_authid);

		username_list = lappend(username_list, makeString(NameStr(pg_authid->rolname)));
	}

	heap_endscan(rel_scan);
	heap_close(pg_authid_rel, AccessShareLock);

	return username_list;
}

static Oid mgr_get_role_oid_or_public(const char *rolname)
{
	if (strcmp(rolname, "public") == 0)
		return ACL_ID_PUBLIC;

	return get_role_oid(rolname, false);
}

List* mgr_get_nodetype_namelist(char nodetype)
{
	ScanKeyData key[1];
	HeapScanDesc rel_scan;
	HeapTuple tuple =NULL;
	Relation rel_node;
	List *nodenamelist =NIL;
	Form_mgr_node mgr_node;

	rel_node = heap_open(NodeRelationId, AccessShareLock);
	ScanKeyInit(&key[0],
					Anum_mgr_node_nodetype
					,BTEqualStrategyNumber
					,F_CHAREQ
					,CharGetDatum(nodetype));
	rel_scan = heap_beginscan_catalog(rel_node, 1, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		nodenamelist = lappend(nodenamelist, NameStr(mgr_node->nodename));
		if (CNDN_TYPE_GTM_COOR_MASTER == nodetype)
						break;
	}
	heap_endscan(rel_scan);
	heap_close(rel_node, AccessShareLock);
	return nodenamelist;
}

/*
* mgr_lock_cluster
*  lock the cluster: find the active coodinator and execute 'SELECT PGXC_PAUSE_CLUSER()'
*/
bool mgr_lock_cluster_deprecated(PGconn **pg_conn, Oid *cnoid)
{
	Oid coordhostoid = InvalidOid;
	int32 coordport = -1;
	int iloop = 0;
	int max = 3;
	char *coordhost = NULL;
	char coordport_buf[10];
	char *connect_user = NULL;
	char cnpath[1024];
	int try = 0;
	const int maxnum = 3;
	NameData self_address;
	NameData nodename;
	GetAgentCmdRst getAgentCmdRst;
	StringInfoData infosendmsg;
	Datum datumPath;
	Relation rel_node;
	HeapTuple tuple = NULL;
	Form_mgr_node mgr_node;
	bool isNull;
	bool breload = false;
	bool bgetAddress = true;
	bool ret = true;
	PGresult *res;

	rel_node = heap_open(NodeRelationId, AccessShareLock);

	ereport(LOG, (errmsg("get active coordinator to connect start")));

	for (iloop = 0; iloop < max; iloop++)
	{
		/*get active coordinator to connect*/
		if (!mgr_get_active_node(&nodename, CNDN_TYPE_COORDINATOR_MASTER, specHostOid))
		{
			if (iloop == max-1)
			{
				heap_close(rel_node, AccessShareLock);
				ereport(ERROR, (errmsg("can not get active coordinator in cluster %d", iloop)));
			}
			else
			{
				ereport(WARNING, (errmsg("can not get active coordinator in cluster %d", iloop)));
			}
		}
		else
		{
			tuple = mgr_get_tuple_node_from_name_type(rel_node, nodename.data);
			if(!(HeapTupleIsValid(tuple)))
			{
				heap_close(rel_node, AccessShareLock);
				ereport(ERROR, (errmsg("coordinator \"%s\" does not exist", nodename.data)
					, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
					, errcode(ERRCODE_UNDEFINED_OBJECT)));
			}
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			coordhostoid = mgr_node->nodehost;
			coordport = mgr_node->nodeport;
			coordhost = get_hostaddress_from_hostoid(coordhostoid);
			connect_user = get_hostuser_from_hostoid(coordhostoid);
			*cnoid = HeapTupleGetOid(tuple);
			clusterLockCoordNodeOid = *cnoid;
			namestrcpy(&clusterLockCoordNodeName, NameStr(mgr_node->nodename));
			/*get the adbmanager ip*/
			memset(self_address.data, 0, NAMEDATALEN);
			bgetAddress = mgr_get_self_address(coordhost, coordport, &self_address);
			if (bgetAddress)
				break;
			else
			{
				heap_freetuple(tuple);
				pfree(coordhost);
				pfree(connect_user);
			}
		}
	}

	if (!bgetAddress)
	{
		heap_close(rel_node, AccessShareLock);
		ereport(ERROR, (errmsg("on ADB Manager get local address fail, so cannot do \"FAILOVER\" command")));
	}

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
		*pg_conn = ExpPQsetdbLogin(coordhost
								,coordport_buf
								,NULL, NULL
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
				ereport(ERROR, (errmsg("set ADB Manager ip \"%s\" to %s coordinator %s/pg_hba,conf fail %s"
					, self_address.data, coordhost, cnpath, getAgentCmdRst.description.data)));
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

	ereport(LOG, (errmsg("get active coordinator to connect end")));

	/* get the value of pool_release_to_idle_timeout to record */
	try = 0;
	namestrcpy(&paramV, "-1");
	while(try++ < maxnum)
	{
		res = PQexec(*pg_conn, "show pool_release_to_idle_timeout");
		if (PQresultStatus(res) == PGRES_TUPLES_OK)
		{
			namestrcpy(&paramV, PQgetvalue(res, 0, 0));
			PQclear(res);
			break;
		}
		PQclear(res);
	}
	if (strcmp(paramV.data, "-1") != 0)
	{
		ereport(LOG, (errmsg("set pool_release_to_idle_timeout = -1 start")));
		ereport(NOTICE, (errmsg("set all coordinators pool_release_to_idle_timeout = -1, original value is '%s'", paramV.data)));
		ereport(LOG, (errmsg("set all coordinators pool_release_to_idle_timeout = -1, original value is '%s'", paramV.data)));
		/* set all coordinators pool_release_to_idle_timeout = -1 */
		mgr_set_all_nodetype_param(CNDN_TYPE_COORDINATOR_MASTER, "pool_release_to_idle_timeout", "-1");
		pg_usleep(300000L);
		ereport(LOG, (errmsg("set pool_release_to_idle_timeout = -1 end")));
	}

	/*lock cluster*/
	ereport(NOTICE, (errmsg("lock cluster on coordinator %s : set FORCE_PARALLEL_MODE = off; SELECT PG_PAUSE_CLUSTER();"
	  , clusterLockCoordNodeName.data)));
	ereport(LOG, (errmsg("lock cluster on coordinator %s : set FORCE_PARALLEL_MODE = off; SELECT PG_PAUSE_CLUSTER();"
	  , clusterLockCoordNodeName.data)));
	try = mgr_pqexec_boolsql_try_maxnum(pg_conn, "set FORCE_PARALLEL_MODE = off; \
				SELECT PG_PAUSE_CLUSTER();", maxnum, CMD_SELECT);
	if (try < 0)
	{
		ret = false;
		ereport(WARNING,
			(errmsg("sql error:  %s\n", PQerrorMessage((PGconn*)*pg_conn)),
			errhint("execute command failed: \"set FORCE_PARALLEL_MODE = off; SELECT PG_PAUSE_CLUSTER()\".")));
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

	return ret;
}

bool mgr_lock_cluster_involve_gtm_coord(PGconn **pg_conn, Oid *cnoid)
{
	Oid coordhostoid = InvalidOid;
	int32 coordport = -1;
	int iloop = 0;
	int max = 3;
	char *coordhost = NULL;
	char coordport_buf[10];
	char *connect_user = NULL;
	char cnpath[1024];
	int try = 0;
	const int maxnum = 3;
	NameData self_address;
	NameData nodename;
	GetAgentCmdRst getAgentCmdRst;
	StringInfoData infosendmsg;
	Datum datumPath;
	Relation rel_node;
	HeapTuple tuple = NULL;
	Form_mgr_node mgr_node;
	bool isNull;
	bool breload = false;
	bool bgetAddress = true;
	bool ret = true;
	PGresult *res;
	char holdLockCoordNodetype = 0;

	rel_node = heap_open(NodeRelationId, AccessShareLock);

	ereport(LOG, (errmsg("get active coordinator to connect start")));

	for (iloop = 0; iloop < max; iloop++)
	{
		/*get active coordinator to connect*/
		if (!mgr_get_active_node(&nodename, CNDN_TYPE_GTM_COOR_MASTER, specHostOid))
		{
			if (iloop == max-1)
			{
				heap_close(rel_node, AccessShareLock);
				ereport(ERROR, (errmsg("can not get active coordinator in cluster %d", iloop)));
			}
			else
			{
				ereport(WARNING, (errmsg("can not get active coordinator in cluster %d", iloop)));
			}
		}
		else
		{
			tuple = mgr_get_tuple_node_from_name_type(rel_node, nodename.data);
			if(!(HeapTupleIsValid(tuple)))
			{
				heap_close(rel_node, AccessShareLock);
				ereport(ERROR, (errmsg("coordinator \"%s\" does not exist", nodename.data)
					, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
					, errcode(ERRCODE_UNDEFINED_OBJECT)));
			}
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			coordhostoid = mgr_node->nodehost;
			coordport = mgr_node->nodeport;
			coordhost = get_hostaddress_from_hostoid(coordhostoid);
			connect_user = get_hostuser_from_hostoid(coordhostoid);
			*cnoid = HeapTupleGetOid(tuple);
			clusterLockCoordNodeOid = *cnoid;
			namestrcpy(&clusterLockCoordNodeName, NameStr(mgr_node->nodename));
			holdLockCoordNodetype = mgr_node->nodetype;
			/*get the adbmanager ip*/
			memset(self_address.data, 0, NAMEDATALEN);
			bgetAddress = mgr_get_self_address(coordhost, coordport, &self_address);
			if (bgetAddress)
				break;
			else
			{
				heap_freetuple(tuple);
				pfree(coordhost);
				pfree(connect_user);
			}
		}
	}

	if (!bgetAddress)
	{
		heap_close(rel_node, AccessShareLock);
		ereport(ERROR, (errmsg("on ADB Manager get local address fail, so cannot do \"FAILOVER\" command")));
	}

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
		*pg_conn = ExpPQsetdbLogin(coordhost
								,coordport_buf
								,NULL, NULL
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
				ereport(ERROR, (errmsg("set ADB Manager ip \"%s\" to %s coordinator %s/pg_hba,conf fail %s"
					, self_address.data, coordhost, cnpath, getAgentCmdRst.description.data)));
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

	
	ereport(LOG, (errmsg("get active coordinator to connect end")));

	/* all gtm coord node have the same pgxc_node_name value */
	if(holdLockCoordNodetype == CNDN_TYPE_GTM_COOR_MASTER)
	{
		res = PQexec(*pg_conn, "show pgxc_node_name");
		if (PQresultStatus(res) == PGRES_TUPLES_OK)
		{
			namestrcpy(&GTM_COORD_PGXC_NODE_NAME, PQgetvalue(res, 0, 0));
		}
		PQclear(res);
		res = NULL;
	}

	/* get the value of pool_release_to_idle_timeout to record */
	try = 0;
	namestrcpy(&paramV, "-1");
	while(try++ < maxnum)
	{
		res = PQexec(*pg_conn, "show pool_release_to_idle_timeout");
		if (PQresultStatus(res) == PGRES_TUPLES_OK)
		{
			namestrcpy(&paramV, PQgetvalue(res, 0, 0));
			PQclear(res);
			break;
		}
		PQclear(res);
	}
	if (strcmp(paramV.data, "-1") != 0)
	{
		ereport(LOG, (errmsg("set pool_release_to_idle_timeout = -1 start")));
		ereport(NOTICE, (errmsg("set all coordinators pool_release_to_idle_timeout = -1, original value is '%s'", paramV.data)));
		ereport(LOG, (errmsg("set all coordinators pool_release_to_idle_timeout = -1, original value is '%s'", paramV.data)));
		/* set all coordinators pool_release_to_idle_timeout = -1 */
		mgr_set_all_nodetype_param(CNDN_TYPE_COORDINATOR_MASTER, "pool_release_to_idle_timeout", "-1");
		pg_usleep(300000L);
		ereport(LOG, (errmsg("set pool_release_to_idle_timeout = -1 end")));
	}

	/*lock cluster*/
	ereport(NOTICE, (errmsg("lock cluster on coordinator %s : set FORCE_PARALLEL_MODE = off; SELECT PG_PAUSE_CLUSTER();"
	  , clusterLockCoordNodeName.data)));
	ereport(LOG, (errmsg("lock cluster on coordinator %s : set FORCE_PARALLEL_MODE = off; SELECT PG_PAUSE_CLUSTER();"
	  , clusterLockCoordNodeName.data)));
	try = mgr_pqexec_boolsql_try_maxnum(pg_conn, "set FORCE_PARALLEL_MODE = off; \
				SELECT PG_PAUSE_CLUSTER();", maxnum, CMD_SELECT);
	if (try < 0)
	{
		ret = false;
		ereport(WARNING,
			(errmsg("sql error:  %s\n", PQerrorMessage((PGconn*)*pg_conn)),
			errhint("execute command failed: \"set FORCE_PARALLEL_MODE = off; SELECT PG_PAUSE_CLUSTER()\".")));
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

	return ret;
}

void mgr_unlock_cluster_deprecated(PGconn **pg_conn)
{
	int try = 0;
	const int maxnum = 3;
	Relation relNode = NULL;
	HeapScanDesc rel_scan = NULL;
	HeapTuple tuple = NULL;
	Form_mgr_node mgr_node;
	StringInfoData cmdstring;
	ScanKeyData key[3];
	char *sqlstr = "set FORCE_PARALLEL_MODE = off; SELECT PG_UNPAUSE_CLUSTER();";

	if (!*pg_conn)
		return;
	ereport(NOTICE, (errmsg("on coordinator \"%s\" : unlock cluster: %s", clusterLockCoordNodeName.data, sqlstr)));
	ereport(LOG, (errmsg("on coordinator \"%s\" : unlock cluster: %s", clusterLockCoordNodeName.data, sqlstr)));
	try = mgr_pqexec_boolsql_try_maxnum(pg_conn, sqlstr, maxnum, CMD_SELECT);
	if (try<0)
	{
		ereport(WARNING, (errcode(ERRCODE_DATA_EXCEPTION)
			,errmsg("execute \"%s\" fail %s", sqlstr, PQerrorMessage((PGconn*)*pg_conn))));
	}

	if (strcmp(paramV.data, "-1") != 0)
	{
		/* set all coordinators pool_release_to_idle_timeout to record value */
		ereport(NOTICE, (errmsg("set all coordinators pool_release_to_idle_timeout = '%s'", paramV.data)));
		ereport(LOG, (errmsg("set all coordinators pool_release_to_idle_timeout = '%s'", paramV.data)));
		mgr_set_all_nodetype_param(CNDN_TYPE_COORDINATOR_MASTER, "pool_release_to_idle_timeout", paramV.data);
	}

	initStringInfo(&cmdstring);
	/* close idle process */
	relNode = heap_open(NodeRelationId, AccessShareLock);
	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(CNDN_TYPE_COORDINATOR_MASTER));
	ScanKeyInit(&key[1]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	ScanKeyInit(&key[2]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	rel_scan = heap_beginscan_catalog(relNode, 3, key);
	while ((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		if (clusterLockCoordNodeOid == HeapTupleGetOid(tuple))
			continue;
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		resetStringInfo(&cmdstring);
		appendStringInfo(&cmdstring, "set FORCE_PARALLEL_MODE = off; EXECUTE DIRECT ON (\"%s\") 'select pool_close_idle_conn();'"
			, NameStr(mgr_node->nodename));
		ereport(NOTICE, (errmsg("on coordinator \"%s\" : %s", clusterLockCoordNodeName.data, cmdstring.data)));
		ereport(LOG, (errmsg("on coordinator \"%s\" : %s", clusterLockCoordNodeName.data, cmdstring.data)));
		try = mgr_pqexec_boolsql_try_maxnum(pg_conn, cmdstring.data, maxnum, CMD_SELECT);
		if (try<0)
		{
			ereport(WARNING, (errcode(ERRCODE_DATA_EXCEPTION)
				,errmsg("execute \"%s\" fail %s", cmdstring.data, PQerrorMessage((PGconn*)*pg_conn))));
		}

	}

	resetStringInfo(&cmdstring);
	appendStringInfo(&cmdstring, "set FORCE_PARALLEL_MODE = off; select pool_close_idle_conn();");
	ereport(NOTICE, (errmsg("on coordinator \"%s\" : %s", clusterLockCoordNodeName.data, cmdstring.data)));
	ereport(LOG, (errmsg("on coordinator \"%s\" : %s", clusterLockCoordNodeName.data, cmdstring.data)));
	try = mgr_pqexec_boolsql_try_maxnum(pg_conn, cmdstring.data, maxnum, CMD_SELECT);
	if (try<0)
	{
		ereport(WARNING, (errcode(ERRCODE_DATA_EXCEPTION)
			,errmsg("execute \"%s\" fail %s", cmdstring.data, PQerrorMessage((PGconn*)*pg_conn))));
	}

	heap_endscan(rel_scan);
	heap_close(relNode, AccessShareLock);

	PQfinish(*pg_conn);
	*pg_conn = NULL;
}

void mgr_unlock_cluster_involve_gtm_coord(PGconn **pg_conn)
{
	int try = 0;
	const int maxnum = 3;
	Relation relNode = NULL;
	HeapScanDesc rel_scan = NULL;
	HeapTuple tuple = NULL;
	Form_mgr_node mgr_node;
	StringInfoData cmdstring;
	ScanKeyData key[3];
	char *sqlstr = "set FORCE_PARALLEL_MODE = off; SELECT PG_UNPAUSE_CLUSTER();";

	if (!*pg_conn)
		return;
	ereport(NOTICE, (errmsg("on coordinator \"%s\" : unlock cluster: %s", clusterLockCoordNodeName.data, sqlstr)));
	ereport(LOG, (errmsg("on coordinator \"%s\" : unlock cluster: %s", clusterLockCoordNodeName.data, sqlstr)));
	try = mgr_pqexec_boolsql_try_maxnum(pg_conn, sqlstr, maxnum, CMD_SELECT);
	if (try<0)
	{
		ereport(WARNING, (errcode(ERRCODE_DATA_EXCEPTION)
			,errmsg("execute \"%s\" fail %s", sqlstr, PQerrorMessage((PGconn*)*pg_conn))));
	}

	if (strcmp(paramV.data, "-1") != 0)
	{
		/* set all coordinators pool_release_to_idle_timeout to record value */
		ereport(NOTICE, (errmsg("set all coordinators pool_release_to_idle_timeout = '%s'", paramV.data)));
		ereport(LOG, (errmsg("set all coordinators pool_release_to_idle_timeout = '%s'", paramV.data)));
		mgr_set_all_nodetype_param(CNDN_TYPE_COORDINATOR_MASTER, "pool_release_to_idle_timeout", paramV.data);
	}

	initStringInfo(&cmdstring);
	/* close idle process */
	relNode = heap_open(NodeRelationId, AccessShareLock);
	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodetype
				,BTEqualStrategyNumber
				,F_CHAREQ
				,CharGetDatum(CNDN_TYPE_COORDINATOR_MASTER));
	ScanKeyInit(&key[1]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	ScanKeyInit(&key[2]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	rel_scan = heap_beginscan_catalog(relNode, 3, key);
	while ((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		if (clusterLockCoordNodeOid == HeapTupleGetOid(tuple))
			continue;
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		resetStringInfo(&cmdstring);
		appendStringInfo(&cmdstring, "set FORCE_PARALLEL_MODE = off; EXECUTE DIRECT ON (\"%s\") 'select pool_close_idle_conn();'"
			, NameStr(mgr_node->nodename));
		ereport(NOTICE, (errmsg("on coordinator \"%s\" : %s", clusterLockCoordNodeName.data, cmdstring.data)));
		ereport(LOG, (errmsg("on coordinator \"%s\" : %s", clusterLockCoordNodeName.data, cmdstring.data)));
		try = mgr_pqexec_boolsql_try_maxnum(pg_conn, cmdstring.data, maxnum, CMD_SELECT);
		if (try<0)
		{
			ereport(WARNING, (errcode(ERRCODE_DATA_EXCEPTION)
				,errmsg("execute \"%s\" fail %s", cmdstring.data, PQerrorMessage((PGconn*)*pg_conn))));
		}
	}
	heap_endscan(rel_scan);

	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(CNDN_TYPE_GTM_COOR_MASTER));
	rel_scan = heap_beginscan_catalog(relNode, 3, key);
	while ((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		if (clusterLockCoordNodeOid == HeapTupleGetOid(tuple))
			continue;
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		resetStringInfo(&cmdstring);
		appendStringInfo(&cmdstring, "set FORCE_PARALLEL_MODE = off; EXECUTE DIRECT ON (\"%s\") 'select pool_close_idle_conn();'"
			, strlen(NameStr(GTM_COORD_PGXC_NODE_NAME)) ==0 ? NameStr(mgr_node->nodename) : NameStr(GTM_COORD_PGXC_NODE_NAME));
		ereport(NOTICE, (errmsg("on coordinator \"%s\" : %s", clusterLockCoordNodeName.data, cmdstring.data)));
		ereport(LOG, (errmsg("on coordinator \"%s\" : %s", clusterLockCoordNodeName.data, cmdstring.data)));
		try = mgr_pqexec_boolsql_try_maxnum(pg_conn, cmdstring.data, maxnum, CMD_SELECT);
		if (try<0)
		{
			ereport(WARNING, (errcode(ERRCODE_DATA_EXCEPTION)
				,errmsg("execute \"%s\" fail %s", cmdstring.data, PQerrorMessage((PGconn*)*pg_conn))));
		}

	}

	resetStringInfo(&cmdstring);
	appendStringInfo(&cmdstring, "set FORCE_PARALLEL_MODE = off; select pool_close_idle_conn();");
	ereport(NOTICE, (errmsg("on coordinator \"%s\" : %s", clusterLockCoordNodeName.data, cmdstring.data)));
	ereport(LOG, (errmsg("on coordinator \"%s\" : %s", clusterLockCoordNodeName.data, cmdstring.data)));
	try = mgr_pqexec_boolsql_try_maxnum(pg_conn, cmdstring.data, maxnum, CMD_SELECT);
	if (try<0)
	{
		ereport(WARNING, (errcode(ERRCODE_DATA_EXCEPTION)
			,errmsg("execute \"%s\" fail %s", cmdstring.data, PQerrorMessage((PGconn*)*pg_conn))));
	}

	heap_endscan(rel_scan);
	heap_close(relNode, AccessShareLock);

	PQfinish(*pg_conn);
	*pg_conn = NULL;	
}

bool mgr_pqexec_refresh_pgxc_node(pgxc_node_operator cmd, char nodetype, char *dnname
		, GetAgentCmdRst *getAgentCmdRst, PGconn **pg_conn, Oid cnoid, char *newSyncSlaveName)
{
	struct tuple_cndn *prefer_cndn;
	ListCell *lc_out, *dn_lc;
	int coordinator_num = 0, datanode_num = 0;
	HeapTuple tuple_in;
	HeapTuple tuple_out;
	HeapTuple newMasterTuple;
	StringInfoData cmdstring;
	StringInfoData recorderr;
	Form_mgr_node mgr_node_out, mgr_node_in;
	Form_mgr_node mgr_node;
	Form_mgr_node mgr_nodeNewm;
	char *host_address;
	char *masterName;
	char *newMasterAddress;
	bool is_preferred = false;
	bool result = true;
	bool bExecDirect = false;
	bool slotIsNotEmpty = true;
	const int maxnum = 3;
	int try = 0;
	int newMasterPort;
	HeapTuple cn_tuple;
	NameData cnnamedata;
	NameData masternameData;
	Relation relNode;

	Assert(dnname);
	relNode = heap_open(NodeRelationId, AccessShareLock);
	newMasterTuple = mgr_get_tuple_node_from_name_type(relNode, dnname);
	if(!HeapTupleIsValid(newMasterTuple))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
			, errmsg("cache lookup failed for the node \"%s\" in node table", dnname)));
	}

	mgr_nodeNewm = (Form_mgr_node)GETSTRUCT(newMasterTuple);
	Assert(mgr_nodeNewm);
	newMasterAddress = get_hostaddress_from_hostoid(mgr_nodeNewm->nodehost);
	newMasterPort = mgr_nodeNewm->nodeport;
	heap_freetuple(newMasterTuple);
	heap_close(relNode, AccessShareLock);

	initStringInfo(&recorderr);
	resetStringInfo(&(getAgentCmdRst->description));
	prefer_cndn = get_new_pgxc_node(cmd, dnname, nodetype);
	if(!PointerIsValid(prefer_cndn->coordiantor_list))
	{
		appendStringInfoString(&(getAgentCmdRst->description),"not exist coordinator in the cluster");
		appendStringInfoString(&recorderr, "not exist coordinator in the cluster\n");
		return false;
	}

	masterName = mgr_get_mastername_by_nodename_type(dnname, nodetype);
	namestrcpy(&masternameData, masterName);
	pfree(masterName);
	/*get name of coordinator, whos oid is cnoid*/
	cn_tuple = SearchSysCache1(NODENODEOID, cnoid);
	if(!HeapTupleIsValid(cn_tuple))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
			, errmsg("oid \"%u\" of coordinator does not exist", cnoid)));
	}
	mgr_node = (Form_mgr_node)GETSTRUCT(cn_tuple);
	Assert(mgr_node);
	namestrcpy(&cnnamedata, NameStr(mgr_node->nodename));
	ReleaseSysCache(cn_tuple);

	initStringInfo(&cmdstring);
	coordinator_num = 0;
	ereport(LOG, (errmsg("refresh the new datanode master \"%s\" information in pgxc_node"
			" on all coordinators", dnname)));
	ereport(NOTICE, (errmsg("refresh the new datanode master \"%s\" information in pgxc_node"
			" on all coordinators", dnname)));

	/* hexp_pqexec_direct_execute_utility((PGconn*)*pg_conn,SQL_BEGIN_TRANSACTION
				, MGR_PGEXEC_DIRECT_EXE_UTI_RET_COMMAND_OK); */
	foreach(lc_out, prefer_cndn->coordiantor_list)
	{
		coordinator_num = coordinator_num + 1;
		tuple_out = (HeapTuple)lfirst(lc_out);
		mgr_node_out = (Form_mgr_node)GETSTRUCT(tuple_out);
		Assert(mgr_node_out);
		datanode_num = 0;
		bExecDirect = (cnoid != HeapTupleGetOid(tuple_out));

		foreach(dn_lc, prefer_cndn->datanode_list)
		{
			datanode_num = datanode_num +1;
			tuple_in = (HeapTuple)lfirst(dn_lc);
			mgr_node_in = (Form_mgr_node)GETSTRUCT(tuple_in);
			Assert(mgr_node_in);
			host_address = get_hostaddress_from_hostoid(mgr_node_in->nodehost);
			if(coordinator_num == datanode_num)
			{
				is_preferred = true;
			}
			else
			{
				is_preferred = false;
			}
			resetStringInfo(&cmdstring);
			if (!bExecDirect)
				appendStringInfo(&cmdstring, "alter node \"%s\" with(name='%s', host='%s', port=%d, preferred=%s) on (\"%s\");"
								,strcmp(NameStr(mgr_node_in->nodename), masternameData.data) == 0 ? masternameData.data:NameStr(mgr_node_in->nodename)
								,strcmp(NameStr(mgr_node_in->nodename), masternameData.data) == 0 ? dnname:NameStr(mgr_node_in->nodename)
								,strcmp(NameStr(mgr_node_in->nodename), masternameData.data) == 0 ? newMasterAddress : host_address
								,strcmp(NameStr(mgr_node_in->nodename), masternameData.data) == 0 ? newMasterPort : mgr_node_in->nodeport
								,true == is_preferred ? "true":"false"
								,NameStr(cnnamedata));
			else
				appendStringInfo(&cmdstring, "alter node \"%s\" with(name='%s', host='%s', port=%d, preferred=%s) on (\"%s\");"
								,strcmp(NameStr(mgr_node_in->nodename), masternameData.data) == 0 ? masternameData.data:NameStr(mgr_node_in->nodename)
								,strcmp(NameStr(mgr_node_in->nodename), masternameData.data) == 0 ? dnname:NameStr(mgr_node_in->nodename)
								,strcmp(NameStr(mgr_node_in->nodename), masternameData.data) == 0 ? newMasterAddress : host_address
								,strcmp(NameStr(mgr_node_in->nodename), masternameData.data) == 0 ? newMasterPort : mgr_node_in->nodeport
								,true == is_preferred ? "true":"false"
								,NameStr(mgr_node_out->nodename));
			pfree(host_address);
			ereport(LOG, (errmsg("on coordinator \"%s\" execute \"%s\"", cnnamedata.data, cmdstring.data)));
			try = mgr_pqexec_boolsql_try_maxnum(pg_conn, cmdstring.data, maxnum, CMD_UTILITY);
			if (try<0)
			{
				result = false;
				appendStringInfo(&recorderr, "on coordinator \"%s\" execute \"%s\" fail %s\n"
					, cnnamedata.data, cmdstring.data, PQerrorMessage((PGconn*)*pg_conn));
				ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION)
					,errmsg("on coordinator \"%s\" execute \"%s\" fail %s", cnnamedata.data, cmdstring.data, PQerrorMessage((PGconn*)*pg_conn))));
			}
		}
	}

	/* update the pgxc_node information on datanode masters */
	slotIsNotEmpty = hexp_check_select_result_count(*pg_conn, SELECT_ADB_SLOT_TABLE_COUNT);
	if (slotIsNotEmpty)
	{
		ereport(LOG, (errmsg("refresh the new datanode master \"%s\" information in pgxc_node"
			" on all datanode masters", dnname)));
		ereport(NOTICE, (errmsg("refresh the new datanode master \"%s\" information in pgxc_node"
			" on all datanode masters", dnname)));
	}
	else
	{
		ereport(LOG, (errmsg("refresh the new datanode master \"%s\" information in its pgxc_node"
		, dnname)));
		ereport(NOTICE, (errmsg("refresh the new datanode master \"%s\" information in its pgxc_node"
		, dnname)));
	}
	foreach(dn_lc, prefer_cndn->datanode_list)
	{
		resetStringInfo(&cmdstring);
		tuple_in = (HeapTuple)lfirst(dn_lc);
		mgr_node_in = (Form_mgr_node)GETSTRUCT(tuple_in);
		Assert(mgr_node_in);
		if (slotIsNotEmpty ||
			(!slotIsNotEmpty && strcmp(NameStr(mgr_node_in->nodename), NameStr(masternameData)) == 0))
		{
			appendStringInfo(&cmdstring, "alter node \"%s\" with(name='%s', host='%s', port=%d, preferred=%s) on (\"%s\");"
					,NameStr(masternameData)
					,dnname
					,newMasterAddress
					,newMasterPort
					,"false"
					,strcmp(NameStr(mgr_node_in->nodename), NameStr(masternameData)) == 0 ?
						dnname : NameStr(mgr_node_in->nodename));
			try = mgr_pqexec_boolsql_try_maxnum(pg_conn, cmdstring.data, maxnum, CMD_UTILITY);
			if (try<0)
			{
				result = false;
				appendStringInfo(&recorderr, "on coordinator \"%s\" execute \"%s\" fail %s\n"
					, cnnamedata.data, cmdstring.data, PQerrorMessage((PGconn*)*pg_conn));
				ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION)
					,errmsg("on coordinator \"%s\" execute \"%s\" fail %s", cnnamedata.data, cmdstring.data, PQerrorMessage((PGconn*)*pg_conn))));
			}
		}
	}

	/* update adb_slot */
	if (slotIsNotEmpty)
	{
		ereport(LOG, (errmsg("refresh the new datanode master \"%s\" information in adb_slot"
			, dnname)));
		ereport(NOTICE, (errmsg("refresh the new datanode master \"%s\" information in adb_slot"
			, dnname)));
		hexp_alter_slotinfo_nodename_noflush((PGconn*)*pg_conn, NameStr(masternameData), dnname, false, true);
		hexp_pqexec_direct_execute_utility((PGconn*)*pg_conn, "flush slot;"
				, MGR_PGEXEC_DIRECT_EXE_UTI_RET_COMMAND_OK);
	}

	/* hexp_pqexec_direct_execute_utility((PGconn*)*pg_conn,SQL_COMMIT_TRANSACTION
			, MGR_PGEXEC_DIRECT_EXE_UTI_RET_COMMAND_OK); */

	/* on coordinator , select pgxc_pool_reload() */
	ereport(LOG, (errmsg("select pgxc_pool_reload() on all coordinators")));
	ereport(NOTICE, (errmsg("select pgxc_pool_reload() on all coordinators")));

	foreach(lc_out, prefer_cndn->coordiantor_list)
	{
		tuple_out = (HeapTuple)lfirst(lc_out);
		mgr_node_out = (Form_mgr_node)GETSTRUCT(tuple_out);
		Assert(mgr_node_out);
		datanode_num = 0;
		bExecDirect = (cnoid != HeapTupleGetOid(tuple_out));
		resetStringInfo(&cmdstring);

		if (!bExecDirect)
			appendStringInfo(&cmdstring, "%s", "set FORCE_PARALLEL_MODE = off; select pgxc_pool_reload();");
		else
			appendStringInfo(&cmdstring, "set FORCE_PARALLEL_MODE = off; EXECUTE DIRECT ON (\"%s\") \
				'select pgxc_pool_reload();'", NameStr(mgr_node_out->nodename));
			pg_usleep(100000L);
			ereport(LOG, (errmsg("on coordinator \"%s\" execute \"%s\"", cnnamedata.data, cmdstring.data)));
			try = mgr_pqexec_boolsql_try_maxnum(pg_conn, cmdstring.data, maxnum, CMD_SELECT);
			if (try < 0)
			{
				result = false;
				ereport(WARNING, (errcode(ERRCODE_DATA_EXCEPTION)
					,errmsg("on coordinator \"%s\" execute \"%s\" fail %s", cnnamedata.data
						, cmdstring.data, PQerrorMessage((PGconn*)*pg_conn))));
				appendStringInfo(&recorderr, "on coordinator \"%s\" execute \"%s\" fail %s\n"
					, cnnamedata.data, cmdstring.data, PQerrorMessage((PGconn*)*pg_conn));
			}
	}

	if (recorderr.len > 0)
	{
		appendStringInfo(&(getAgentCmdRst->description), "%s", recorderr.data);
	}
	pfree(newMasterAddress);
	pfree(recorderr.data);
	pfree(cmdstring.data);

	return result;
}

/*
* try maxnum to execute the sql, the result of sql if bool type
*/
int mgr_pqexec_boolsql_try_maxnum(PGconn **pg_conn, char *sqlstr, const int maxnum, int sqltype)
{
	int result = maxnum;
	PGresult *res;

	while(result-- >= 0)
	{
		res = PQexec(*pg_conn, sqlstr);
		if (CMD_SELECT == sqltype)
		{
			if (PQresultStatus(res) == PGRES_TUPLES_OK)
			{
				if (strcasecmp("t", PQgetvalue(res, 0, 0)) == 0)
				{
					PQclear(res);
					res = NULL;
					break;
				}
			}
			else
			{
				ereport(WARNING, (errcode(ERRCODE_DATA_EXCEPTION)
				,errmsg("on coordinator   execute \"%s\" fail %s", sqlstr, PQerrorMessage((PGconn*)*pg_conn))));
			}
		}
		else if (CMD_UPDATE == sqltype || CMD_DELETE == sqltype 
			|| CMD_INSERT == sqltype || CMD_UTILITY == sqltype)
		{
			if (PQresultStatus(res) == PGRES_COMMAND_OK)
			{
				PQclear(res);
				res = NULL;
				break;
			}
			else
				ereport(WARNING, (errcode(ERRCODE_DATA_EXCEPTION)
					,errmsg("on coordinator   execute \"%s\" fail %s", sqlstr, PQerrorMessage((PGconn*)*pg_conn))));
		}
		else
		{
			/* do nothing now */
		}

		if (res)
		{
			PQclear(res);
			res = NULL;
		}
		pg_usleep(100000L);
	}

	return result;
}

/*
* ADD EXTENSION extension_name
*/
void mgr_extension(MgrExtensionAdd *node, ParamListInfo params, DestReceiver *dest)
{
	if (mgr_has_priv_add())
	{
		DirectFunctionCall2(mgr_extension_handle,
									CharGetDatum(node->cmdtype),
									CStringGetDatum(node->name));
		return;
	}
	else
	{
		ereport(ERROR, (errmsg("permission denied")));
		return ;
	}
}

/*
* create extension
*/
Datum mgr_extension_handle(PG_FUNCTION_ARGS)
{
	char *extension_name;
	char cmdtype;
	StringInfoData cmdstring;
	bool ret;

	cmdtype = PG_GETARG_CHAR(0);
	extension_name = PG_GETARG_CSTRING(1);
	if (strcmp (extension_name, "pg_stat_statements") == 0)
	{
		ret = mgr_extension_pg_stat_statements(cmdtype, extension_name);
	}
	else
	{
		initStringInfo(&cmdstring);
		if (cmdtype == EXTENSION_CREATE)
			appendStringInfo(&cmdstring, "CREATE EXTENSION IF NOT EXISTS %s;", extension_name);
		else if (cmdtype == EXTENSION_DROP)
			appendStringInfo(&cmdstring, "DROP EXTENSION IF NOT EXISTS %s;", extension_name);
		else
		{
			pfree(cmdstring.data);
			ereport(ERROR, (errmsg("no such cmdtype '%c'", cmdtype)));
		}
		ret = mgr_add_extension_sqlcmd(cmdstring.data);
		pfree(cmdstring.data);

	}
	if (ret)
		ereport(NOTICE, (errmsg("need set the parameters for the extension \"%s\" and put its dynamic library file on the library path", extension_name)));

	PG_RETURN_BOOL(ret);
}

/*
* create or drop extension pg_stat_statements
*/

static bool mgr_extension_pg_stat_statements(char cmdtype, char *extension_name)
{
	MGRUpdateparm *nodestmt;
	MGRUpdateparmReset *resetnodestmt;
	StringInfoData cmdstring;

	initStringInfo(&cmdstring);
	/*create extension*/
	if (cmdtype == EXTENSION_CREATE)
	{
		/*create extension*/
		appendStringInfo(&cmdstring, "CREATE EXTENSION IF NOT EXISTS %s;", extension_name);
		if (!mgr_add_extension_sqlcmd(cmdstring.data))
			return false;

		nodestmt = makeNode(MGRUpdateparm);
		nodestmt->parmtype = PARM_TYPE_COORDINATOR;
		nodestmt->nodetype = CNDN_TYPE_COORDINATOR_MASTER;
		nodestmt->nodename = MACRO_STAND_FOR_ALL_NODENAME;
		nodestmt->is_force = false;
		nodestmt->options = lappend(nodestmt->options, makeDefElem("shared_preload_libraries", (Node *)makeString(extension_name), -1));
		mgr_add_updateparm(nodestmt, NULL, NULL);
		/*for datanode*/
		nodestmt->parmtype = PARM_TYPE_DATANODE;
		nodestmt->nodetype = CNDN_TYPE_DATANODE;
		nodestmt->nodename = MACRO_STAND_FOR_ALL_NODENAME;
		nodestmt->is_force = false;
		mgr_add_updateparm(nodestmt, NULL, NULL);
	}
	else if (cmdtype == EXTENSION_DROP)
	{
		/*drop extension*/
		appendStringInfo(&cmdstring, "DROP EXTENSION IF EXISTS %s;", extension_name);
		if (!mgr_add_extension_sqlcmd(cmdstring.data))
			return false;

		resetnodestmt = makeNode(MGRUpdateparmReset);
		resetnodestmt->parmtype = PARM_TYPE_COORDINATOR;
		resetnodestmt->nodetype = CNDN_TYPE_COORDINATOR_MASTER;
		resetnodestmt->nodename = MACRO_STAND_FOR_ALL_NODENAME;
		resetnodestmt->is_force = false;
		resetnodestmt->options = lappend(resetnodestmt->options, makeDefElem("shared_preload_libraries", (Node *)makeString("''"), -1));
		mgr_reset_updateparm(resetnodestmt, NULL, NULL);
	}
	else
	{
		pfree(cmdstring.data);
		ereport(ERROR, (errmsg("no such cmdtype '%c'", cmdtype)));
	}

	pfree(cmdstring.data);

	return true;
}

bool mgr_get_self_address(char *server_address, int server_port, Name self_address)
{
		pgsocket sock;
		int nRet;
		struct sockaddr_in serv_addr;
		struct sockaddr_in addr;
		socklen_t addr_len;

		Assert(server_address);
		memset(&serv_addr, 0, sizeof(serv_addr));

		sock = socket(PF_INET, SOCK_STREAM, 0);
		if (sock == -1)
		{
			ereport(WARNING, (errmsg("on ADB Manager create sock fail")));
			return false;
		}

		serv_addr.sin_family = AF_INET;
		serv_addr.sin_addr.s_addr = inet_addr(server_address);
		serv_addr.sin_port = htons(server_port);

		if (connect(sock,(struct sockaddr*)&serv_addr,sizeof(serv_addr)) == -1)
		{
			ereport(WARNING, (errmsg("on ADB Manager sock connect \"%s\" \"%d\" fail", server_address, server_port)));
			closesocket(sock);
			return false;
		}

		addr_len = sizeof(struct sockaddr_in);
		nRet = getsockname(sock,(struct sockaddr*)&addr,&addr_len);
		if(nRet == -1)
		{
			ereport(WARNING, (errmsg("on ADB Manager sock connect \"%s\" \"%d\" to getsockname fail", server_address, server_port)));
			closesocket(sock);
			return false;
		}
		namestrcpy(self_address, inet_ntoa(addr.sin_addr));
		closesocket(sock);

		return true;
}

/*
* check the node is recovery or not
*/
bool mgr_check_node_recovery_finish(char nodetype, Oid hostoid, int nodeport, char *address)
{
	StringInfoData resultstrdata;
	HeapTuple tuple;
	Form_mgr_host mgr_host;
	char *pstr;
	char *sqlstr = "select * from pg_is_in_recovery()";

	tuple = SearchSysCache1(HOSTHOSTOID, hostoid);
	if(!(HeapTupleIsValid(tuple)))
	{
		ereport(ERROR, (errmsg("host oid \"%u\" not exist", hostoid)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
			, errcode(ERRCODE_UNDEFINED_OBJECT)));
	}
	mgr_host= (Form_mgr_host)GETSTRUCT(tuple);
	Assert(mgr_host);
	initStringInfo(&resultstrdata);
	monitor_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES, mgr_host->hostagentport, sqlstr, NameStr(mgr_host->hostuser), address, nodeport, DEFAULT_DB, &resultstrdata);
	ReleaseSysCache(tuple);
	if (resultstrdata.len == 0)
	{
		return false;
	}
	pstr = resultstrdata.data;
	if (strcmp(pstr, "f") !=0)
	{
		pfree(resultstrdata.data);
		return false;
	}
	pfree(resultstrdata.data);

	return true;
}

/*
* check the param reload in postgresql.conf
*/
bool mgr_check_param_reload_postgresqlconf(char nodetype, Oid hostoid, int nodeport, char *address, char *check_param, char *expect_result)
{
	StringInfoData resultstrdata;
	StringInfoData sqlstrdata;
	HeapTuple tuple;
	Form_mgr_host mgr_host;
	char *pstr;

	Assert(expect_result);
	tuple = SearchSysCache1(HOSTHOSTOID, hostoid);
	if(!(HeapTupleIsValid(tuple)))
	{
		ereport(ERROR, (errmsg("host oid \"%u\" not exist", hostoid)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
			, errcode(ERRCODE_UNDEFINED_OBJECT)));
	}
	mgr_host= (Form_mgr_host)GETSTRUCT(tuple);
	Assert(mgr_host);
	initStringInfo(&resultstrdata);
	initStringInfo(&sqlstrdata);
	appendStringInfo(&sqlstrdata, "show %s", check_param);
	monitor_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES, mgr_host->hostagentport, sqlstrdata.data, NameStr(mgr_host->hostuser), address, nodeport, DEFAULT_DB, &resultstrdata);
	ReleaseSysCache(tuple);
	pfree(sqlstrdata.data);
	if (resultstrdata.len == 0)
	{
		return false;
	}
	pstr = resultstrdata.data;
	if (strcmp(pstr, expect_result) !=0)
	{
		pfree(resultstrdata.data);
		return false;
	}
	pfree(resultstrdata.data);

	return true;
}

/*
* mgr_check_syncstate_node_exist
*  check the master node has the sync node exclude the oid that given. if needCheckIncluster is true, we
*  need seek the node which in cluster, otherwise no need care whether the node in cluster or not.
*/

static bool mgr_check_syncstate_node_exist(Relation rel, Oid masterTupleOid, int sync_state_type, Oid excludeoid, bool needCheckIncluster)
{
	ScanKeyData key[4];
	HeapScanDesc rel_scan;
	HeapTuple mastertuple;
	HeapTuple tuple;
	NameData sync_state_name;
	bool bget = false;

	/* check master node exist */
	mastertuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(masterTupleOid));
	if(!HeapTupleIsValid(mastertuple))
	{
		ereport(WARNING, (errcode(ERRCODE_UNDEFINED_OBJECT)
			, errmsg("tuple oid=%d does not exist in mgr_node table", masterTupleOid)));
	}
	else
		ReleaseSysCache(mastertuple);

	namestrcpy(&sync_state_name, sync_state_tab[sync_state_type].name);
	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodemasternameoid
		,BTEqualStrategyNumber
		,F_OIDEQ
		,ObjectIdGetDatum(masterTupleOid));
	ScanKeyInit(&key[1]
		,Anum_mgr_node_nodesync
		,BTEqualStrategyNumber, F_NAMEEQ
		,NameGetDatum(&sync_state_name));
	if (needCheckIncluster)
		ScanKeyInit(&key[2]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,CharGetDatum(true));
	if (needCheckIncluster)
		rel_scan = heap_beginscan_catalog(rel, 3, key);
	else
		rel_scan = heap_beginscan_catalog(rel, 2, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		if (HeapTupleGetOid(tuple) == excludeoid)
			continue;
		bget = true;
		break;
	}
	heap_endscan(rel_scan);

	return bget;
}

/*
* check the node hostname and path, not allow repeated with others
*/
static bool mgr_check_node_path(Relation rel, Oid hostoid, char *path)
{
	ScanKeyData key[2];
	HeapScanDesc rel_scan;
	HeapTuple tuple;
	bool bget = false;

	ScanKeyInit(&key[0],
		Anum_mgr_node_nodehost
		,BTEqualStrategyNumber
		,F_OIDEQ
		,ObjectIdGetDatum(hostoid));
	ScanKeyInit(&key[1],
		Anum_mgr_node_nodepath
		,BTEqualStrategyNumber
		,F_TEXTEQ
		,CStringGetTextDatum(path));

	rel_scan = heap_beginscan_catalog(rel, 2, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		bget = true;
		break;
	}
	heap_endscan(rel_scan);

	return bget;
}

/*
* check the node hostname and path, not allow repeated with others
*/
static bool mgr_check_node_port(Relation rel, Oid hostoid, int port)
{
	ScanKeyData key[2];
	HeapScanDesc rel_scan;
	HeapTuple tuple;
	bool bget = false;

	ScanKeyInit(&key[0],
		Anum_mgr_node_nodehost
		,BTEqualStrategyNumber
		,F_OIDEQ
		,ObjectIdGetDatum(hostoid));
	ScanKeyInit(&key[1],
		Anum_mgr_node_nodeport
		,BTEqualStrategyNumber
		,F_INT4EQ
		,Int32GetDatum(port));

	rel_scan = heap_beginscan_catalog(rel, 2, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		bget = true;
		break;
	}
	heap_endscan(rel_scan);

	return bget;
}

/*remove node from cluster*/
void mgr_remove_node(MgrRemoveNode *node, ParamListInfo params, DestReceiver *dest)
{
	if (mgr_has_priv_drop())
	{
		DirectFunctionCall2(mgr_remove_node_func,
									CharGetDatum(node->nodetype),
									PointerGetDatum(node->names));
		return;
	}
	else
	{
		ereport(ERROR, (errmsg("permission denied")));
		return ;
	}
}

/*remove node from cluster*/
Datum mgr_remove_node_func(PG_FUNCTION_ARGS)
{
	char nodetype;
	char *address;
	char *nodestring;
	char *masterpath;
	char port_buf[10];
	NameData namedata;
	NameData mastername;
	List *nodenamelist = NIL;
	Relation rel;
	HeapScanDesc rel_scan;
	HeapTuple tuple;
	HeapTuple mastertuple;
	ListCell   *cell;
	Form_mgr_node mgr_node;
	Form_mgr_node mgr_masternode;
	ScanKeyData key[3];
	int iloop = 0;
	int num = 0;
	int syncNum = 0;
	int removeNode = 0;
	bool bsync_exist;
	bool isNull;
	bool res;
	bool hasFailOnce = false;
	Oid selftupleoid;
	Datum datumPath;
	GetAgentCmdRst getAgentCmdRst;
	StringInfoData  infosendmsg;
	StringInfoData infostrparam;
	StringInfoData infostrparamtmp;
	Value *val;
	char *user;

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot assign TransactionIds during recovery")));

	/*ndoe type*/
	nodetype = PG_GETARG_CHAR(0);
	if (CNDN_TYPE_DATANODE_MASTER == nodetype || CNDN_TYPE_GTM_COOR_MASTER == nodetype)
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
			, errmsg("it does not support remove master node now execpt for coordinator")));
	nodenamelist = (List *)PG_GETARG_POINTER(1);

	/*check the node in the cluster*/
	rel = heap_open(NodeRelationId, RowExclusiveLock);

	/*check the num of type node*/
	if (CNDN_TYPE_COORDINATOR_MASTER == nodetype)
	{
		ScanKeyInit(&key[0],
			Anum_mgr_node_nodetype
			,BTEqualStrategyNumber
			,F_CHAREQ
			,CharGetDatum(nodetype));
		ScanKeyInit(&key[1]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,CharGetDatum(true));
		rel_scan = heap_beginscan_catalog(rel, 2, key);
		while ((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			num++;
		}
		if (1 == num)
		{
			heap_endscan(rel_scan);
			heap_close(rel, RowExclusiveLock);
			ereport(ERROR, (errmsg("the cluster only has one coordinator, cannot be removed")));
		}
		heap_endscan(rel_scan);
	}

	foreach(cell, nodenamelist)
	{
		val = lfirst(cell);
		Assert(val && IsA(val,String));
		namestrcpy(&namedata, strVal(val));
		ScanKeyInit(&key[0],
			Anum_mgr_node_nodetype
			,BTEqualStrategyNumber
			,F_CHAREQ
			,CharGetDatum(nodetype));
		ScanKeyInit(&key[1],
			Anum_mgr_node_nodename
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,NameGetDatum(&namedata));
		ScanKeyInit(&key[2]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,CharGetDatum(true));

		rel_scan = heap_beginscan_catalog(rel, 3, key);
		if ((tuple = heap_getnext(rel_scan, ForwardScanDirection)) == NULL)
		{
			heap_endscan(rel_scan);
			heap_close(rel, RowExclusiveLock);
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				 ,errmsg("%s \"%s\" does not exist in cluster", mgr_nodetype_str(nodetype), namedata.data)));
		}
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		if (CNDN_TYPE_COORDINATOR_MASTER == nodetype)
			removeNode++;
		address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		sprintf(port_buf, "%d", mgr_node->nodeport);
		iloop = 0;
		while (iloop++ < 2)
		{
			user = get_hostuser_from_hostoid(mgr_node->nodehost);
			if (pingNode_user(address, port_buf, user) == 0 || pingNode_user(address, port_buf, user) == -2)
			{
				pfree(user);
				pfree(address);
				heap_endscan(rel_scan);
				heap_close(rel, RowExclusiveLock);
				ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
					,errmsg("\"%s\" is running, stop it first", NameStr(mgr_node->nodename))));
			}
			pfree(user);
		}
		heap_endscan(rel_scan);
		pfree(address);
	}

	/* the cluster must has at least one read-write coordinator */
	if (CNDN_TYPE_COORDINATOR_MASTER == nodetype && (num <= removeNode))
		ereport(ERROR, (errmsg("the cluster must has at least one read-write coordinator, cannot be removed")));

	/*if coordinator is remove, just to remove it directly*/
	if (CNDN_TYPE_COORDINATOR_MASTER == nodetype)
	{
		foreach(cell, nodenamelist)
		{
			val = lfirst(cell);
			Assert(val && IsA(val, String));
			res = exec_remove_coordinator(strVal(val));
			if (!hasFailOnce && !res)
				hasFailOnce = true;
		}
		heap_close(rel, RowExclusiveLock);
		PG_RETURN_BOOL(!hasFailOnce);
	}
	initStringInfo(&(getAgentCmdRst.description));
	initStringInfo(&infosendmsg);
	initStringInfo(&infostrparam);
	initStringInfo(&infostrparamtmp);

	foreach(cell, nodenamelist)
	{
		val = lfirst(cell);
		Assert(val && IsA(val,String));
		namestrcpy(&namedata, strVal(val));
		ScanKeyInit(&key[0],
			Anum_mgr_node_nodetype
			,BTEqualStrategyNumber
			,F_CHAREQ
			,CharGetDatum(nodetype));
		ScanKeyInit(&key[1],
			Anum_mgr_node_nodename
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,NameGetDatum(&namedata));
		ScanKeyInit(&key[2]
			,Anum_mgr_node_nodeincluster
			,BTEqualStrategyNumber
			,F_BOOLEQ
			,CharGetDatum(true));
		rel_scan = heap_beginscan_catalog(rel, 3, key);
		tuple = heap_getnext(rel_scan, ForwardScanDirection);
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		/*if mgr_node->nodesync = SYNC, set its master as async*/
		selftupleoid = HeapTupleGetOid(tuple);
		bsync_exist = mgr_check_syncstate_node_exist(rel, mgr_node->nodemasternameoid, SYNC_STATE_SYNC, selftupleoid, true);
		if (!bsync_exist)
		{
			mgr_update_one_potential_to_sync(rel, mgr_node->nodemasternameoid, true, selftupleoid);
		}

		if (strcmp(NameStr(mgr_node->nodesync),sync_state_tab[SYNC_STATE_SYNC].name) == 0
				|| strcmp(NameStr(mgr_node->nodesync), sync_state_tab[SYNC_STATE_POTENTIAL].name) == 0)
		{
				syncNum = mgr_get_master_sync_string(mgr_node->nodemasternameoid, true, selftupleoid, &infostrparam);
		}

		if (infostrparam.len == 0)
			appendStringInfoString(&infostrparam, "");
		else
		{
			if (syncNum)
			{
				resetStringInfo(&infostrparamtmp);
				appendStringInfo(&infostrparamtmp, "%d(%s)", syncNum, infostrparam.data);
				resetStringInfo(&infostrparam);
				appendStringInfo(&infostrparam, "%s", infostrparamtmp.data);
			}
		}
		mastertuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(mgr_node->nodemasternameoid));
		if(!HeapTupleIsValid(mastertuple))
		{
			heap_endscan(rel_scan);
			heap_close(rel, RowExclusiveLock);
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				, errmsg("the master \"%s\" does not exist", NameStr(mgr_node->nodename))));
		}
		mgr_masternode = (Form_mgr_node)GETSTRUCT(mastertuple);
		namestrcpy(&mastername, NameStr(mgr_masternode->nodename));
		datumPath = heap_getattr(mastertuple, Anum_mgr_node_nodepath, RelationGetDescr(rel), &isNull);
		if (isNull)
		{
			ReleaseSysCache(mastertuple);
			heap_close(rel, RowExclusiveLock);

			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
				, errmsg("column nodepath is null")));
		}
		resetStringInfo(&(getAgentCmdRst.description));
		resetStringInfo(&infosendmsg);
		masterpath = TextDatumGetCString(datumPath);
		mgr_append_pgconf_paras_str_quotastr("synchronous_standby_names", infostrparam.data, &infosendmsg);
		nodestring = mgr_nodetype_str(mgr_masternode->nodetype);
		ereport(LOG, (errmsg("set \"synchronous_standby_names = '%s' in postgresql.conf of the %s \"%s\""
			, infostrparam.data, nodestring, NameStr(mgr_masternode->nodename))));
		mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, masterpath, &infosendmsg
			, mgr_masternode->nodehost, &getAgentCmdRst);
		if (!getAgentCmdRst.ret)
			ereport(WARNING, (errmsg("set synchronous_standby_names = '%s' in postgresql.conf of %s \"%s\"fail"
				, infostrparam.data, nodestring, NameStr(mgr_masternode->nodename))));
		ReleaseSysCache(mastertuple);
		pfree(nodestring);
		/*check its master has sync node*/
		if (strcmp(infostrparam.data, "") == 0)
		{
			if (CNDN_TYPE_DATANODE_SLAVE == nodetype)
				ereport(WARNING, (errmsg("the datanode master \"%s\" has no synchronous slave node", mastername.data)));
			else
				ereport(WARNING, (errmsg("the gtm master \"%s\" has no synchronous slave node", mastername.data)));
		}
		/*update the tuple*/
		mgr_node->nodeinited = false;
		mgr_node->nodeincluster = false;
		namestrcpy(&(mgr_node->nodesync), sync_state_tab[SYNC_STATE_ASYNC].name);
		heap_inplace_update(rel, tuple);
		heap_endscan(rel_scan);
		resetStringInfo(&infostrparam);
	}
	pfree(infosendmsg.data);
	pfree(infostrparam.data);
	pfree(infostrparamtmp.data);
	pfree(getAgentCmdRst.description.data);
	heap_close(rel, RowExclusiveLock);

	PG_RETURN_BOOL(true);
}

/*
* exec_remove_coordinator
* 	remove coordinator
*/
static bool exec_remove_coordinator(char *nodename)
{
	HeapTuple tuple;
	Relation relNode;
	Form_mgr_node mgr_node;
	char *userName;
	char *nodeAddr;
	bool isRunning;
	bool res1 = true;
	bool res2 = true;

	relNode = heap_open(NodeRelationId, AccessShareLock);
	tuple = mgr_get_tuple_node_from_name_type(relNode, nodename);
	if(!HeapTupleIsValid(tuple))
	{
		heap_close(relNode, AccessShareLock);
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
			, errmsg("coordinator master \"%s\" does not exist", nodename)));
	}
	mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
	Assert(mgr_node);
	if (!mgr_node->nodeinited)
	{
		heap_freetuple(tuple);
		heap_close(relNode, AccessShareLock);
		ereport(ERROR, (errmsg("coordinator master \"%s\" dose not inited", nodename)));
	}

	/* check the remove coordinator is running */
	nodeAddr = get_hostaddress_from_hostoid(mgr_node->nodehost);
	userName = get_hostuser_from_hostoid(mgr_node->nodehost);
	isRunning = is_node_running(nodeAddr, mgr_node->nodeport, userName, mgr_node->nodetype);
	pfree(nodeAddr);
	pfree(userName);
	heap_freetuple(tuple);
	heap_close(relNode, AccessShareLock);

	if (isRunning)
	{
		ereport(ERROR, (errmsg("coordinator master \"%s\" , stop it first", nodename)));
	}
	/* modify the pgxc_node table, because the coordinator has stoppend so it's not need to add ddl lock */
	res1 = mgr_drop_node_on_all_coord(CNDN_TYPE_COORDINATOR_MASTER, nodename);
	res2 = mgr_drop_node_on_all_coord(CNDN_TYPE_GTM_COOR_MASTER, nodename);

	/* modify the mgr_node table */
	mgr_set_inited_incluster(nodename, CNDN_TYPE_COORDINATOR_MASTER, true, false);

	return res1 && res2;
}
/*
* check the node pingNode ok max_try times
*/
bool mgr_try_max_pingnode(char *host, char *port, char *user, const int max_times)
{
	int ret = 0;

	/*wait the node can accept connections*/
	fputs(_("waiting for the new node can accept connections..."), stdout);
	fflush(stdout);
	while(1)
	{
		ret++;
		if (pingNode_user(host, port, user) != 0)
		{
			fputs(_("."), stdout);
			fflush(stdout);
			pg_usleep(1 * 1000000L);
		}
		else
			break;
		if (ret > max_times)
			break;
	}
	if (ret > max_times)
	{
		fputs(_(" failed\n"), stdout);
	}
	else
		fputs(_(" done\n"), stdout);
	fflush(stdout);

	return ret < max_times;
}

/*
* get the master type
*/
char mgr_get_master_type(char nodetype)
{
	char mastertype;

	switch(nodetype)
	{
		case CNDN_TYPE_GTM_COOR_SLAVE:
			mastertype = CNDN_TYPE_GTM_COOR_MASTER;
			break;
		case CNDN_TYPE_DATANODE_SLAVE:
			mastertype = CNDN_TYPE_DATANODE_MASTER;
			break;
		case CNDN_TYPE_COORDINATOR_SLAVE:
			mastertype = CNDN_TYPE_COORDINATOR_MASTER;
			break;
		case CNDN_TYPE_GTM_COOR_MASTER:
		case CNDN_TYPE_COORDINATOR_MASTER:
		case CNDN_TYPE_DATANODE_MASTER:
			mastertype = nodetype;
			break;
		default:
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("no such type '%c' of node", nodetype)));
			break;
	}

	return mastertype;
}

/*
* if we drop one sync node and the master has no other sync node, we need update one potential node to sync node
*/

static void mgr_update_one_potential_to_sync(Relation rel, Oid mastertupleoid, bool bincluster, bool excludeoid)
{
	NameData sync_state_name;
	NameData sync_state_name_sync;
	Form_mgr_node mgr_node;
	HeapTuple tuple;
	HeapScanDesc rel_scan;
	ScanKeyData key[4];
	char *nodetypestr;

	namestrcpy(&sync_state_name, sync_state_tab[SYNC_STATE_POTENTIAL].name);
	namestrcpy(&sync_state_name_sync, sync_state_tab[SYNC_STATE_SYNC].name);
	ScanKeyInit(&key[0]
		,Anum_mgr_node_nodemasternameoid
		,BTEqualStrategyNumber
		,F_OIDEQ
		,ObjectIdGetDatum(mastertupleoid));
	ScanKeyInit(&key[1]
		,Anum_mgr_node_nodesync
		,BTEqualStrategyNumber, F_NAMEEQ
		,NameGetDatum(&sync_state_name));
	ScanKeyInit(&key[2]
		,Anum_mgr_node_nodeincluster
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(bincluster));
	ScanKeyInit(&key[3]
		,Anum_mgr_node_nodezone
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,CStringGetDatum(mgr_zone));
	rel_scan = heap_beginscan_catalog(rel, 4, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		if (excludeoid == HeapTupleGetOid(tuple))
			continue;
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		namestrcpy(&(mgr_node->nodesync), sync_state_name_sync.data);
		heap_inplace_update(rel, tuple);
		nodetypestr = mgr_nodetype_str(mgr_node->nodetype);
		ereport(NOTICE, (errmsg("the master of this node has no synchronous slave node, make potential node %s \"%s\" as synchronous node", nodetypestr, NameStr(mgr_node->nodename))));
		pfree(nodetypestr);
		break;
	}
	heap_endscan(rel_scan);
}

/*
* get the string "synchronous_standby_names" of master, but not include the tuple which oid is excludeoid
* the get string record in infostrparam
*/
int mgr_get_master_sync_string(Oid mastertupleoid, bool bincluster, Oid excludeoid, StringInfo infostrparam)
{
	NameData sync_state_name;
	Form_mgr_node mgr_node;
	HeapTuple tuple;
	HeapScanDesc rel_scan;
	ScanKeyData key[4];
	Relation rel;
	int i = 0;
	int no_async_num = 0;

	rel = heap_open(NodeRelationId, AccessShareLock);
	for(i=0; i<2; i++)
	{
		if (i == 0)
			namestrcpy(&sync_state_name, sync_state_tab[SYNC_STATE_SYNC].name);
		else
			namestrcpy(&sync_state_name, sync_state_tab[SYNC_STATE_POTENTIAL].name);
		ScanKeyInit(&key[0]
			,Anum_mgr_node_nodemasternameoid
			,BTEqualStrategyNumber
			,F_OIDEQ
			,ObjectIdGetDatum(mastertupleoid));
		ScanKeyInit(&key[1]
			,Anum_mgr_node_nodesync
			,BTEqualStrategyNumber, F_NAMEEQ
			,NameGetDatum(&sync_state_name));
		ScanKeyInit(&key[2]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(bincluster));
		ScanKeyInit(&key[3]
				,Anum_mgr_node_nodezone
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,CStringGetDatum(mgr_zone));
		rel_scan = heap_beginscan_catalog(rel, 4, key);
		while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			if (HeapTupleGetOid(tuple) == excludeoid)
				continue;
			if (i == 0)
				no_async_num++;
			if (infostrparam->len == 0)
				appendStringInfo(infostrparam, "%s", NameStr(mgr_node->nodename));
			else
				appendStringInfo(infostrparam, ",%s", NameStr(mgr_node->nodename));
		}
		heap_endscan(rel_scan);
	}

	heap_close(rel, AccessShareLock);

	return no_async_num;
}

/*monitor ha, get the diff between master and slave*/
Datum mgr_monitor_ha(PG_FUNCTION_ARGS)
{
	InitNodeInfo *info;
	StringInfoData sqlstrdata;
	StringInfoData resultstrdata;
	HeapTuple mastertuple;
	HeapTuple out;
	HeapTuple tuple;
	HeapTuple hosttuple;
	Form_mgr_node mgr_node;
	Form_mgr_node mgr_node_m;
	Form_mgr_host mgr_host;
	NameData name[11];
	FuncCallContext *funcctx;
	ScanKeyData key[1];
	int i = 0;
	char *ptr;
	char *address;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		info = palloc(sizeof(*info));
		info->rel_node = heap_open(NodeRelationId, AccessShareLock);
		ScanKeyInit(&key[0]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
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

	initStringInfo(&resultstrdata);
	initStringInfo(&sqlstrdata);
	while((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if (CNDN_TYPE_GTM_COOR_SLAVE != mgr_node->nodetype && CNDN_TYPE_DATANODE_SLAVE != mgr_node->nodetype 
			&& CNDN_TYPE_COORDINATOR_SLAVE != mgr_node->nodetype)
			continue;
		/*get master port, ip, and agent_port*/
		mastertuple = SearchSysCache1(NODENODEOID, ObjectIdGetDatum(mgr_node->nodemasternameoid));
		if (!HeapTupleIsValid(mastertuple))
			continue;
		mgr_node_m = (Form_mgr_node)GETSTRUCT(mastertuple);
		Assert(mgr_node_m);
		hosttuple = SearchSysCache1(HOSTHOSTOID, ObjectIdGetDatum(mgr_node_m->nodehost));
		mgr_host = (Form_mgr_host)GETSTRUCT(hosttuple);
		address = get_hostaddress_from_hostoid(mgr_node_m->nodehost);
		resetStringInfo(&resultstrdata);
		resetStringInfo(&sqlstrdata);
		appendStringInfo(&sqlstrdata, "select application_name, client_addr, state, \
		pg_walfile_name_offset(sent_lsn) as sent_lsn , pg_walfile_name_offset(replay_lsn) as \
		replay_lsn, sync_state, pg_walfile_name_offset(pg_current_wal_insert_lsn()) as \
		master_lsn, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_insert_lsn(),sent_lsn)) \
		sent_delay,pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_insert_lsn(),replay_lsn)) \
		replay_delay  from pg_stat_replication where application_name='%s';"
			, NameStr(mgr_node->nodename));

		monitor_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES, mgr_host->hostagentport, sqlstrdata.data, NameStr(mgr_host->hostuser), address, mgr_node_m->nodeport, DEFAULT_DB, &resultstrdata);
		pfree(address);
		ReleaseSysCache(mastertuple);
		ReleaseSysCache(hosttuple);
		if (mgr_node->nodetype == CNDN_TYPE_GTM_COOR_SLAVE)
			namestrcpy(&name[0], "gtmcoord slave");
		else if (mgr_node->nodetype == CNDN_TYPE_DATANODE_SLAVE)
			namestrcpy(&name[0], "datanode slave");
		else if (mgr_node->nodetype == CNDN_TYPE_COORDINATOR_SLAVE)
			namestrcpy(&name[0], "coordinator slave");
		else
			namestrcpy(&name[0], "unknown nodetype");

		namestrcpy(&name[1], NameStr(mgr_node->nodename));
		ptr = resultstrdata.data;
		for(i=0; i<9; i++)
		{
			if (*ptr)
				namestrcpy(&name[i+2], ptr);
			else
				namestrcpy(&name[i+2], "");
			if (*ptr)
				ptr = ptr+strlen(name[i+2].data)+1;
		}
		if (strcmp(NameStr(name[4]), "") == 0)
			namestrcpy(&name[4], "down");
		out = build_ha_replication_tuple(&name[0], &name[1],&name[2],&name[3],&name[4],&name[5],&name[6],&name[7],&name[8],&name[9],&name[10]);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(out));
	}

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
	pfree(resultstrdata.data);
	pfree(sqlstrdata.data);
	SRF_RETURN_DONE(funcctx);
}

void release_append_node_info(AppendNodeInfo *node_info, bool is_release)
{
	if (!PointerIsValid(node_info))
		return;
	if (PointerIsValid(node_info->nodename))
		pfree(node_info->nodename);
	if (PointerIsValid(node_info->nodepath))
		pfree(node_info->nodepath);
	if (PointerIsValid(node_info->nodeaddr))
		pfree(node_info->nodeaddr);
	if (PointerIsValid(node_info->nodeusername))
		pfree(node_info->nodeusername);
	/*checking whether release the struct of AppendNodeInfo*/
	if (is_release)
		pfree(node_info);
}

/*
	the parameter nodeinfo as the test object,
	the manager use libpq to connect the node directly,
	if connect success then return false; present it's not need to add hba
	if the function return true; show that we add manager hba to node
	so we need remove the hba when we close the pg_conn.
*/
bool AddHbaIsValid(const AppendNodeInfo *nodeinfo, StringInfo infosendmsg)
{
	const int MAX_TRY = 3;
	int try = MAX_TRY;
	NameData local_ip;
	NameData node_port;
	GetAgentCmdRst getAgentCmdRst;
	PGconn *pg_conn = NULL;

	initStringInfo(&(getAgentCmdRst.description));

	if (!get_local_ip(&local_ip))
	{
		ereport(ERROR, (errmsg("get adb manager local ip.")));
	}
	sprintf(NameStr(node_port), "%d", nodeinfo->nodeport);
	try = MAX_TRY;
	do
	{
		pg_conn = ExpPQsetdbLogin(nodeinfo->nodeaddr
									,NameStr(node_port)
									,NULL, NULL
									,nodeinfo->nodeusername
									,NULL);
		if ((try--) <= 0)
			break;
	}while(PQstatus((PGconn*)pg_conn) != CONNECTION_OK);
	/*release the pg_conn */
	PQfinish(pg_conn);
	/*not need to add manager hba to node*/
	if (try > 0)
	{
		return false;
	}
	/*send adb manager ip to coordinator pg_hba.conf file*/
	mgr_add_oneline_info_pghbaconf(CONNECT_HOST, "all", "all", NameStr(local_ip), 32, "trust", infosendmsg);
	mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF
							,nodeinfo->nodepath
							,infosendmsg
							,nodeinfo->nodehost
							,&getAgentCmdRst);
	if (!getAgentCmdRst.ret)
		ereport(ERROR, (errmsg("%s", getAgentCmdRst.description.data)));

	/*execute pgxc_ctl reload to take effect for the new value in the pg_hba.conf  */
	mgr_reload_conf(nodeinfo->nodehost, nodeinfo->nodepath);

	/*try to connect coodinator */
	try = MAX_TRY;
	do
	{
		pg_conn = ExpPQsetdbLogin(nodeinfo->nodeaddr
									,NameStr(node_port)
									,NULL, NULL
									,nodeinfo->nodeusername
									,NULL);
		if ((try--) <= 0)
			break;
	}while(PQstatus((PGconn*)pg_conn) != CONNECTION_OK);
	if (try < 0)
	{
		ereport(ERROR,
			(errmsg("Fail to connect to coordinator %s", PQerrorMessage((PGconn*)pg_conn)),
			errhint("coordinator info(host=%s port=%d dbname=%s user=%s)",
				nodeinfo->nodeaddr, nodeinfo->nodeport, DEFAULT_DB, nodeinfo->nodeusername)));
	}
	/*release the pg_conn */
	PQfinish(pg_conn);
	return true;
}

/*
remove the add line from coordinator pg_hba.conf
*/
bool RemoveHba(const AppendNodeInfo *nodeinfo, const StringInfo infosendmsg)
{
	GetAgentCmdRst getAgentCmdRst;
	initStringInfo(&(getAgentCmdRst.description));

	mgr_send_conf_parameters(AGT_CMD_CNDN_DELETE_PGHBACONF
							,nodeinfo->nodepath
							,infosendmsg
							,nodeinfo->nodehost
							,&getAgentCmdRst);
	if (!getAgentCmdRst.ret)
	{
		ereport(WARNING, (errmsg("remove \"%s\" from \"%s\" pg_hba.conf fail.\n %s"
							, infosendmsg->data
							, nodeinfo->nodename
							, getAgentCmdRst.description.data)));
	}
	mgr_reload_conf(nodeinfo->nodehost, nodeinfo->nodepath);
	return true;
}

/*
Get the local IP address by checking the server
if success return true;
else return false;
*/
static bool get_local_ip(Name local_ip)
{
	Datum agent_host_ip;
	int32 port;
	bool isNull;
	bool rest = true;
	ManagerAgent *ma;
	Relation rel;
	HeapScanDesc rel_scan;
	HeapTuple tuple =NULL;
	Form_mgr_host mgr_host;

	Assert(local_ip->data != NULL);

	/*Query the the first agent information in the cluster but must make sure it's running*/
	rel = heap_open(HostRelationId, AccessShareLock);
	rel_scan = heap_beginscan_catalog(rel, 0, NULL);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		ma = ma_connect_hostoid(HeapTupleGetOid(tuple));
		/*to get the local host ip ,you must make sure the server is running*/
		if(ma_isconnected(ma))
		{
			ma_close(ma);
			break;
		}
	}
	if(!(HeapTupleIsValid(tuple)))
	{
		return false;
	}
	mgr_host = (Form_mgr_host)GETSTRUCT(tuple);
	Assert(mgr_host);

	/*	get the local ip  */
	agent_host_ip = SysCacheGetAttr(HOSTHOSTOID, tuple, Anum_mgr_host_hostaddr, &isNull);
	if(isNull)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
			, errmsg("column hostaddr is null")));
	}
	port = mgr_host->hostagentport;
	rest = mgr_get_self_address(TextDatumGetCString(agent_host_ip), port, local_ip);

	heap_endscan(rel_scan);
	heap_close(rel, AccessShareLock);

	return rest;
}
/*
	if node_name is NULL
	find the first node which respond to the node_type
	success return true;
	failed return false;
*/
bool get_active_node_info(const char node_type, const char *node_name, AppendNodeInfo *nodeinfo)
{
	InitNodeInfo *info = NULL;
	ScanKeyData key[5];
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	Datum datumPath;
	bool isNull = false;
	bool is_running = false;
	char *nodeusername = NULL;
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

	ScanKeyInit(&key[2]
				,Anum_mgr_node_nodetype
				,BTEqualStrategyNumber
				,F_CHAREQ
				,CharGetDatum(node_type));
	ScanKeyInit(&key[3]
				,Anum_mgr_node_nodezone
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,CStringGetDatum(mgr_zone));
	if (node_name)
		ScanKeyInit(&key[4]
				,Anum_mgr_node_nodename
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,CStringGetDatum(node_name));
	info = (InitNodeInfo *)palloc0(sizeof(InitNodeInfo));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);
	if (PointerIsValid(node_name))
		info->rel_scan = heap_beginscan_catalog(info->rel_node, 5, key);
	else
		info->rel_scan = heap_beginscan_catalog(info->rel_node, 4, key);
	info->lcp =NULL;
	while((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		nodeinfo->nodeaddr = get_hostaddress_from_hostoid(mgr_node->nodehost);
		nodeinfo->nodeport = mgr_node->nodeport;
		nodeusername = get_hostuser_from_hostoid(mgr_node->nodehost);
		is_running = is_node_running(nodeinfo->nodeaddr, nodeinfo->nodeport, nodeusername, mgr_node->nodetype);
		pfree(nodeusername);
		if (is_running)
			break;
	}
	if (!is_running)
	{
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, AccessShareLock);
		pfree(info);
		return false;
	}
	nodeinfo->nodeusername = get_hostuser_from_hostoid(mgr_node->nodehost);
	nodeinfo->nodename = pstrdup(NameStr(mgr_node->nodename));
	nodeinfo->nodetype = mgr_node->nodetype;
	nodeinfo->nodehost = mgr_node->nodehost;
	nodeinfo->nodemasteroid = mgr_node->nodemasternameoid;
	nodeinfo->tupleoid = HeapTupleGetOid(tuple);
	namestrcpy(&(nodeinfo->sync_state), NameStr(mgr_node->nodesync));
	datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(info->rel_node), &isNull);
	if (isNull)
	{
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, AccessShareLock);
		pfree(info);
		return false;
	}
	nodeinfo->nodepath = pstrdup(TextDatumGetCString(datumPath));

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(info);
	return true;
}

/*
* read gtm_port gtm_host from system table:gtm, add agtm_host, agtm_port to infosendmsg
* ,use '\0' to interval
*/
static void
mgr_get_gtm_host_snapsender_gxidsender_port(StringInfo infosendmsg)
{
	char *gtm_host = NULL;
	Relation rel_node;
	HeapScanDesc rel_scan;
	Form_mgr_node mgr_node;
	ScanKeyData key[4];
	HeapTuple tuple;
	Oid gtm_port;

	/*get the gtm_port, gtm_host*/
	ScanKeyInit(&key[0]
				, Anum_mgr_node_nodetype
				, BTEqualStrategyNumber
				, F_CHAREQ
				, CharGetDatum(CNDN_TYPE_GTM_COOR_MASTER));
	ScanKeyInit(&key[1]
				, Anum_mgr_node_nodezone
				, BTEqualStrategyNumber
				, F_NAMEEQ
				, CStringGetDatum(mgr_zone));
	ScanKeyInit(&key[2]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	ScanKeyInit(&key[3]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	rel_node = heap_open(NodeRelationId, AccessShareLock);
	rel_scan = heap_beginscan_catalog(rel_node, 4, key);
	while ((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		gtm_host = get_hostaddress_from_hostoid(mgr_node->nodehost);
		gtm_port = mgr_node->nodeport;
		break;
	}
	heap_endscan(rel_scan);

	if(!gtm_host)
	{
		rel_scan = heap_beginscan_catalog(rel_node, 2, key);
		while ((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			gtm_host = get_hostaddress_from_hostoid(mgr_node->nodehost);
			gtm_port = mgr_node->nodeport;
			break;
		}
		heap_endscan(rel_scan);
	}
	heap_close(rel_node, AccessShareLock);

	if (!gtm_host)
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
		, errmsg("the type of gmt_coord for coordinator does not exist")));

	mgr_append_pgconf_paras_str_quotastr("agtm_host", gtm_host, infosendmsg);
	mgr_append_pgconf_paras_str_int("agtm_port", gtm_port, infosendmsg);
	pfree(gtm_host);
}

static void mgr_update_da_master_pgxcnode_slave_info(const char* dn_master_name)
{
	InitNodeInfo	*info;
	ScanKeyData		ndkey[4];
	HeapTuple		tuple;
	Form_mgr_node	dn_master_node;

	info = palloc(sizeof(*info));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);	/* open table */

	/* get datanode master info */
	ScanKeyInit(&ndkey[0]
			,Anum_mgr_node_nodeinited
			,BTEqualStrategyNumber
			,F_BOOLEQ
			,BoolGetDatum(true));
	ScanKeyInit(&ndkey[1]
			,Anum_mgr_node_nodetype
			,BTEqualStrategyNumber
			,F_CHAREQ
			,CharGetDatum(CNDN_TYPE_DATANODE_MASTER));
	ScanKeyInit(&ndkey[2]
			,Anum_mgr_node_nodetype
			,BTEqualStrategyNumber
			,F_CHAREQ
			,CharGetDatum(CNDN_TYPE_DATANODE_MASTER));
	ScanKeyInit(&ndkey[3]
			,Anum_mgr_node_nodename
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,CStringGetDatum(dn_master_name));
	info->rel_scan = heap_beginscan_catalog(info->rel_node, 4, ndkey);
	while ((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
	{
		dn_master_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(dn_master_node);
		mgr_exec_update_dn_pgxcnode_slave(dn_master_node, HeapTupleGetOid(tuple));
		
	}
	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);	/* close table */
	pfree(info);
}

/* Automatically add read-only standby node information to coordinate */
bool mgr_update_cn_pgxcnode_readonlysql_slave(char *updateKey, bool isSlaveSync, Node *node)
{
	InitNodeInfo	*info;
	ScanKeyData		cnkey[1], ndkey[3], ndskey[4];
	HeapTuple		tuple;
	Form_mgr_node	cn_master_node, dn_master_node;
	MgrDatanodeInfo	*mgr_datanode_info;
	List			*datanode_list = NIL;
	ListCell		*cell, *prev;
	List			*sync_parms = NIL;
	NameData		nodeSync;
	bool			updateAll = true;
	ReadonlyUpdateparm *rdUpdateparm;

	/* Check for the need for updates based on read-write separation parameters */
	if (!mgr_get_sync_slave_readonly_state() && 
		!(updateKey && (strcmp(updateKey, "enable_readsql_on_slave") == 0 || strcmp(updateKey, "enable_readsql_on_slave_async") == 0)))
		return true;

	/* Check whether there is an unstarted coord synchronous standby */
	if (!check_all_cn_sync_slave_is_active())
		return true;

	/* get read-write separation parameters */
	if (!mgr_get_async_slave_readonly_state(&sync_parms) && updateKey == NULL)
		return true; 

	info = palloc(sizeof(*info));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);	/* open table */
	info->lcp = NULL;

	readonlySqlSlaveInfoRefreshComplete = false;

	/* update or add parameter settings */
	if (updateKey != NULL && node != NULL)
	{	
		ReadonlyUpdateparm		*newUpdateparm;
		MGRUpdateparm			*parm_node;
		MGRUpdateparmReset		*parm_node_reset;

		Assert(nodeTag(node) == T_MGRUpdateparm || nodeTag(node) == T_MGRUpdateparmReset);

		newUpdateparm = (ReadonlyUpdateparm *) palloc(sizeof(ReadonlyUpdateparm));
		if (nodeTag(node) == T_MGRUpdateparm)
		{
			parm_node = (MGRUpdateparm *)node;
			strcpy(NameStr(newUpdateparm->updateparmnodename), parm_node->nodename);
			newUpdateparm->updateparmnodetype = parm_node->nodetype;
			strcpy(NameStr(newUpdateparm->updateparmkey), updateKey);
			strcpy(NameStr(newUpdateparm->updateparmvalue), isSlaveSync ? "on":"off");
		}
		else
		{
			parm_node_reset = (MGRUpdateparmReset *)node;
			strcpy(NameStr(newUpdateparm->updateparmnodename), parm_node_reset->nodename);
			newUpdateparm->updateparmnodetype = parm_node_reset->nodetype;
			strcpy(NameStr(newUpdateparm->updateparmkey), updateKey);
			strcpy(NameStr(newUpdateparm->updateparmvalue), isSlaveSync ? "on":"off");
		}
		if (list_length(sync_parms) > 0)
		{
			prev = NULL;
			foreach (cell, sync_parms)
			{
				rdUpdateparm = (ReadonlyUpdateparm *) lfirst(cell);
				if (strcmp(NameStr(rdUpdateparm->updateparmnodename), NameStr(newUpdateparm->updateparmnodename)) == 0 
					&& rdUpdateparm->updateparmnodetype == newUpdateparm->updateparmnodetype
					&& strcmp(NameStr(rdUpdateparm->updateparmkey), NameStr(newUpdateparm->updateparmkey)) == 0)
				{
					sync_parms = list_delete_cell(sync_parms, cell, prev);
					pfree(rdUpdateparm);
					break;
				}
				prev = cell;
			}
			sync_parms = lappend(sync_parms, newUpdateparm);
		}
		else
		{
			sync_parms = lappend(sync_parms, newUpdateparm);
		}
	}

	/* get datanode master info */
	ScanKeyInit(&ndkey[0]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	ScanKeyInit(&ndkey[1]
			,Anum_mgr_node_nodetype
			,BTEqualStrategyNumber
			,F_CHAREQ
			,CharGetDatum(CNDN_TYPE_DATANODE_MASTER));
	ScanKeyInit(&ndkey[2]
			,Anum_mgr_node_nodezone
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,CStringGetDatum("local"));
	info->rel_scan = heap_beginscan_catalog(info->rel_node, 3, ndkey);
	while ((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
	{
		dn_master_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(dn_master_node);
		mgr_datanode_info = palloc0(sizeof(MgrDatanodeInfo));
		mgr_datanode_info->masterNode = dn_master_node;
		mgr_datanode_info->masterOid = HeapTupleGetOid(tuple);
		datanode_list = lappend(datanode_list, mgr_datanode_info);
	}
	/* not find the datanode master node. */
	if (list_length(datanode_list) == 0)
	{
		list_free(datanode_list);
		heap_endscan(info->rel_scan);
		heap_close(info->rel_node, AccessShareLock);	/* close table */
		return false;
	}
	heap_endscan(info->rel_scan);
	
	/* get DN slave info by DN master */
	namestrcpy(&nodeSync, "sync");
	ScanKeyInit(&ndskey[0]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	ScanKeyInit(&ndskey[1]
			,Anum_mgr_node_nodezone
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,CStringGetDatum("local"));

	/* get slave info */
	foreach (cell, datanode_list)
	{
		mgr_datanode_info = (MgrDatanodeInfo *) lfirst(cell);
		mgr_datanode_info->slaveNode = NULL;

		ScanKeyInit(&ndskey[2]
				,Anum_mgr_node_nodemasternameoid
				,BTEqualStrategyNumber
				,F_OIDEQ
				,ObjectIdGetDatum(mgr_datanode_info->masterOid));
		ScanKeyInit(&ndskey[3]
				,Anum_mgr_node_nodesync
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,NameGetDatum(&nodeSync));

		/* In read-write separation mode, synchronous slave node is used by default, 
		 * and asynchronous slave node can be used if no synchronous slave node exists. */
		info->rel_scan = heap_beginscan_catalog(info->rel_node, 4, ndskey);
		if ((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
		{
			mgr_datanode_info->slaveNode = (Form_mgr_node)GETSTRUCT(tuple);
		}
		/* Allow reading of asynchronous node slave */
		else
		{
			heap_endscan(info->rel_scan);
			ScanKeyInit(&ndskey[3]
					,Anum_mgr_node_nodesync
					,BTEqualStrategyNumber
					,F_NAMENE
					,NameGetDatum(&nodeSync));
			
			info->rel_scan = heap_beginscan_catalog(info->rel_node, 4, ndskey);
			if ((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
			{
				mgr_datanode_info->slaveNode = (Form_mgr_node)GETSTRUCT(tuple);
			}
		}
		heap_endscan(info->rel_scan);
	}

	/* get CN master info */
	ScanKeyInit(&cnkey[0]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	info->rel_scan = heap_beginscan_catalog(info->rel_node, 1, cnkey);
	while ((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
	{
		cn_master_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(cn_master_node);

		if (CNDN_TYPE_GTM_COOR_MASTER != cn_master_node->nodetype && CNDN_TYPE_COORDINATOR_MASTER != cn_master_node->nodetype)
			continue;

		/* Perform the actual update work */
		if (!mgr_exec_update_cn_pgxcnode_readonlysql_slave(cn_master_node, datanode_list, sync_parms))
			updateAll = false;
	}
	list_free(datanode_list);
	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);	/* close table */
	pfree(info);
	if (updateAll)
	{
		readonlySqlSlaveInfoRefreshComplete = true;
		ereport(NOTICE, 
				(errmsg("Updating pgxc_node successfully at all datanode master.")));
	}
	else
	{
		readonlySqlSlaveInfoRefreshComplete = false;
		ereport(WARNING, 
				(errmsg("Pgxc_node update not completed on all datanode masters.")));
	}
	/* Add read-write separation mode data to force consistency setting reminder */
	if (updateKey && isSlaveSync)
	{
		ereport(WARNING, 
				(errcode(ERRCODE_WARNING),
				 errmsg("Data nodes are synchronized by means of stream replication between primary and secondary nodes, which may cause extremely short synchronization delay. \
				 		 If strong data consistency is required for the read-write separation function, configure the commit mode for 'DATANODE MASTER'."),
				 errhint("SYNCHRONOUS_COMMIT = REMOTE_APPLY")));

	}
	return true;
}

/* Execute the update of the pgxc_node table */
static bool
mgr_exec_update_dn_pgxcnode_slave(Form_mgr_node	dn_master_node, Oid master_oid)
{
	bool			ret_flag;
	InitNodeInfo	*info;
	ScanKeyData		ndskey[2];
	Form_mgr_node	slavenode;
	HeapTuple		tuple;

	StringInfoData	connStr, execSql, checkSql;
	PGconn			*conn = NULL;
	PGresult		*res = NULL;
	char			*warningMassage = "Failed to write slave node information to pgxc_node table for datanode master";

	ret_flag = false;
	/* init datanode master connect string */
	initStringInfo(&connStr);
	appendStringInfo(&connStr, 
					"postgresql://%s@%s:%d/%s", 
					get_hostuser_from_hostoid(dn_master_node->nodehost), 
					get_hostaddress_from_hostoid(dn_master_node->nodehost), 
					dn_master_node->nodeport, 
					DEFAULT_DB);
	appendStringInfoCharMacro(&connStr, '\0');
	
	/* get coordinate connect */
	conn = PQconnectdb(connStr.data);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		pg_usleep(1 * 1000000L);
		conn = PQconnectdb(connStr.data);
		if (PQstatus(conn) != CONNECTION_OK)
		{
			PQfinish(conn);
			pfree(connStr.data);
			ereport(WARNING, 
					(errmsg("%s, attempt to link to the node '%s' failed, please confirm that the cluster is running. %s", 
							warningMassage,
							dn_master_node->nodename.data, 
							PQerrorMessage(conn))));
			return ret_flag;
		}
	}
	pfree(connStr.data);
	
	info = palloc(sizeof(*info));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);	/* open table */
	info->lcp = NULL;
	ScanKeyInit(&ndskey[0]
			,Anum_mgr_node_nodemasternameoid
			,BTEqualStrategyNumber
			,F_OIDEQ
			,ObjectIdGetDatum(master_oid));
	ScanKeyInit(&ndskey[1]
			,Anum_mgr_node_nodetype
			,BTEqualStrategyNumber
			,F_CHAREQ
			,CharGetDatum(CNDN_TYPE_DATANODE_SLAVE));

	/* In read-write separation mode, synchronous slave node is used by default, 
		* and asynchronous slave node can be used if no synchronous slave node exists. */
	info->rel_scan = heap_beginscan_catalog(info->rel_node, 2, ndskey);

	initStringInfo(&execSql);
	initStringInfo(&checkSql);
	while ((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
	{
		slavenode = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(slavenode);

		resetStringInfo(&checkSql);
		appendStringInfo(&checkSql, "select * from pgxc_node where node_master_oid = (select oid from pgxc_node where node_name = '%s');",
					dn_master_node->nodename.data);
		res = PQexec(conn, checkSql.data);
		/* query failed */
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			PQclear(res);
			ereport(WARNING, 
					(errmsg("%s, failed to query pgxc_node in '%s'.", 
							warningMassage,
							dn_master_node->nodename.data)));
			goto exit;
		}
	
		/* Add nonexistent, or update existing */
		if(PQntuples(res) == 0)
		{
			appendStringInfo(&execSql, 
					"create node %s for %s with(type='datanode slave', host='%s', port=%d);", 
					slavenode->nodename.data,
					dn_master_node->nodename.data,
					get_hostaddress_from_hostoid(slavenode->nodehost),
					slavenode->nodeport);
		}
		else
		{
			appendStringInfo(&execSql, 
					"update pgxc_node set node_name = '%s', node_host = '%s', node_port = %d where node_master_oid = (select oid from pgxc_node where node_name = '%s');",
					slavenode->nodename.data,
					get_hostaddress_from_hostoid(slavenode->nodehost),
					slavenode->nodeport,
					dn_master_node->nodename.data);
		}
		PQclear(res);
	}

	if (execSql.len > 0)
	{
		res = PQexec(conn, execSql.data);
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			PQclear(res);
			ereport(WARNING, 
					(errmsg("%s, Failed to update pgxc_node in '%s'.", 
							warningMassage,
							dn_master_node->nodename.data)));
			goto exit;
		}
		PQclear(res);
		ereport(NOTICE, 
				(errmsg("Update pgxc_node successfully in '%s'.", 
						dn_master_node->nodename.data)));
	}
	ret_flag = true;

exit:
	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);	/* close table */
	pfree(info);

	PQfinish(conn);
	pfree(checkSql.data);
	pfree(execSql.data);
	return ret_flag;
}

/* Execute the update of the pgxc_node table */
static bool
mgr_exec_update_cn_pgxcnode_readonlysql_slave(Form_mgr_node	cn_master_node, List *datanode_list, List *sync_parms)
{
	MgrDatanodeInfo	*mgr_datanode_info;
	StringInfoData	connStr, execSql, checkSql;
	PGconn			*conn = NULL;
	PGresult		*res = NULL;
	ListCell		*cell;
	char			*warningMassage = "Failed to write slave node information to pgxc_node table";
	bool			enable_slave = false;
	bool			enable_slave_async = false;


	/* check Read-Write separation switch */
	check_readsql_slave_param_state(cn_master_node, sync_parms, "enable_readsql_on_slave", &enable_slave);

	/* check whether asynchronous standby is allowed */
	check_readsql_slave_param_state(cn_master_node, sync_parms, "enable_readsql_on_slave_async", &enable_slave_async);

	/* init coordinate connect string */
	initStringInfo(&connStr);
	appendStringInfo(&connStr, 
						"postgresql://%s@%s:%d/%s", 
						get_hostuser_from_hostoid(cn_master_node->nodehost), 
						get_hostaddress_from_hostoid(cn_master_node->nodehost), 
						cn_master_node->nodeport, 
						DEFAULT_DB);
	appendStringInfoCharMacro(&connStr, '\0');
	
	/* get coordinate connect */
	conn = PQconnectdb(connStr.data);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		pg_usleep(1 * 1000000L);
		conn = PQconnectdb(connStr.data);
		if (PQstatus(conn) != CONNECTION_OK)
		{
			PQfinish(conn);
			pfree(connStr.data);
			ereport(WARNING, 
					(errmsg("%s, attempt to link to the node '%s' failed, please confirm that the cluster is running. %s", 
							warningMassage,
							cn_master_node->nodename.data, 
							PQerrorMessage(conn))));
			return false;
		}
	}
	
	initStringInfo(&execSql);
	foreach (cell, datanode_list)
	{
		initStringInfo(&checkSql);

		/* Delete slave node information when closing read-write separation */
		if (enable_slave == false)
		{
			appendStringInfoString(&execSql, "delete from pgxc_node where node_type = 'E';");
			break;
		}

		mgr_datanode_info = (MgrDatanodeInfo *) lfirst(cell);
		if (mgr_datanode_info->slaveNode == NULL)
		{	
			/* delete unused slave node info */
			appendStringInfo(&execSql, 
						"delete from pgxc_node where node_master_oid = (select oid from pgxc_node where node_name = '%s');", 
						mgr_datanode_info->masterNode->nodename.data);
			continue;
		}
		else
		{
			appendStringInfo(&checkSql, "select * from pgxc_node where node_master_oid = (select oid from pgxc_node where node_name = '%s');", mgr_datanode_info->masterNode->nodename.data);
			res = PQexec(conn, checkSql.data);
			/* query failed */
			if (PQresultStatus(res) != PGRES_TUPLES_OK)
			{
				PQclear(res);
				PQfinish(conn);
				pfree(connStr.data);
				pfree(checkSql.data);
				pfree(execSql.data);
				ereport(WARNING, 
						(errmsg("%s, failed to query pgxc_node in '%s'.", 
								warningMassage,
								cn_master_node->nodename.data)));
				return false;
			}
			/* Add nonexistent, or update existing */
			if(PQntuples(res) == 0)
			{
				if (strcmp(NameStr(mgr_datanode_info->slaveNode->nodesync), "sync") == 0 
					|| (strcmp(NameStr(mgr_datanode_info->slaveNode->nodesync), "async") == 0 && enable_slave_async))
					appendStringInfo(&execSql, 
							"create node %s for %s with(type='datanode slave', host='%s', port=%d);", 
							mgr_datanode_info->slaveNode->nodename.data,
							mgr_datanode_info->masterNode->nodename.data,
							get_hostaddress_from_hostoid(mgr_datanode_info->slaveNode->nodehost),
							mgr_datanode_info->slaveNode->nodeport);
			}
			else
			{
				if (strcmp(NameStr(mgr_datanode_info->slaveNode->nodesync), "sync") == 0 
					|| (strcmp(NameStr(mgr_datanode_info->slaveNode->nodesync), "async") == 0 && enable_slave_async))
				{
					appendStringInfo(&execSql, 
							"update pgxc_node set node_name = '%s', node_host = '%s', node_port = %d where node_master_oid = (select oid from pgxc_node where node_name = '%s');",
							mgr_datanode_info->slaveNode->nodename.data,
							get_hostaddress_from_hostoid(mgr_datanode_info->slaveNode->nodehost),
							mgr_datanode_info->slaveNode->nodeport,
							mgr_datanode_info->masterNode->nodename.data);
				}
				/* When asynchronous readonly is turned off, the relevant datanode slave information is deleted */
				else if (strcmp(NameStr(mgr_datanode_info->slaveNode->nodesync), "async") == 0 && !enable_slave_async)
				{
					appendStringInfo(&execSql, 
						"delete from pgxc_node where node_master_oid = (select oid from pgxc_node where node_name = '%s');", 
						mgr_datanode_info->masterNode->nodename.data);
				}

			}
			PQclear(res);
		}
	}
	if (execSql.len > 0)
	{
		res = PQexec(conn, execSql.data);
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			PQclear(res);
			PQfinish(conn);
			pfree(connStr.data);
			pfree(checkSql.data);
			pfree(execSql.data);
			ereport(WARNING, 
					(errmsg("%s, Failed to update pgxc_node in '%s'.", 
							warningMassage,
							cn_master_node->nodename.data)));
			return false;
		}
		PQclear(res);
		ereport(NOTICE, 
				(errmsg("Update pgxc_node successfully in '%s'.", 
						cn_master_node->nodename.data)));
	}
	else
		ereport(WARNING, 
			(errmsg("Node '%s' has no update task. HINT: Please check the read-write separation parameters or the synchronization status of datanode slave.", 
					cn_master_node->nodename.data)));
	
	PQfinish(conn);
	pfree(connStr.data);
	pfree(checkSql.data);
	pfree(execSql.data);
	return true;
}

/* Gets whether the asynchronous slave node is available in read-write separation mode */
static bool
mgr_get_async_slave_readonly_state(List **sync_parms)
{
	Relation rel_updateparm;
	Form_mgr_updateparm mgr_updateparm;
	HeapScanDesc rel_scan;
	HeapTuple tuple;
	ReadonlyUpdateparm *rdUpdateparm;

	Assert(*sync_parms == NIL);

	/* get synchronization parameters */
	rel_updateparm = heap_open(UpdateparmRelationId, AccessShareLock);
	rel_scan = heap_beginscan_catalog(rel_updateparm, 0, NULL);
	while ((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_updateparm = (Form_mgr_updateparm)GETSTRUCT(tuple);
		Assert(mgr_updateparm);
		if (strcmp(NameStr(mgr_updateparm->updateparmkey), "enable_readsql_on_slave") == 0
			|| strcmp(NameStr(mgr_updateparm->updateparmkey), "enable_readsql_on_slave_async") == 0)
		{
			rdUpdateparm = (ReadonlyUpdateparm *) palloc(sizeof(ReadonlyUpdateparm));
			strcpy(NameStr(rdUpdateparm->updateparmnodename), NameStr(mgr_updateparm->updateparmnodename));
			rdUpdateparm->updateparmnodetype = mgr_updateparm->updateparmnodetype;
			strcpy(NameStr(rdUpdateparm->updateparmkey), NameStr(mgr_updateparm->updateparmkey));
			strcpy(NameStr(rdUpdateparm->updateparmvalue), text_to_cstring(&mgr_updateparm->updateparmvalue));

			*sync_parms = lappend(*sync_parms, rdUpdateparm);
		}
	}
	heap_endscan(rel_scan);
	heap_close(rel_updateparm, AccessShareLock);	/* close table */

	if(*sync_parms == NIL)
		return false;
	else
		return true;
}

/* Gets whether the synchronous slave node is available in read-write separation mode */
static bool
mgr_get_sync_slave_readonly_state(void)
{
	Relation rel_updateparm;
	Form_mgr_updateparm mgr_updateparm;
	ScanKeyData key[1];
	HeapScanDesc rel_scan;
	HeapTuple tuple;
	NameData updateparmkey;
	char *updateParmValue = NULL;
	bool isSync;

	namestrcpy(&updateparmkey, "enable_readsql_on_slave");
	ScanKeyInit(&key[0]
		,Anum_mgr_updateparm_updateparmkey
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,NameGetDatum(&updateparmkey));
	rel_updateparm = heap_open(UpdateparmRelationId, AccessShareLock);
	rel_scan = heap_beginscan_catalog(rel_updateparm, 1, key);
	tuple = heap_getnext(rel_scan, ForwardScanDirection);
	if (tuple != NULL)
	{
		bool isNull = false;
		Datum datumValue;

		mgr_updateparm = (Form_mgr_updateparm)GETSTRUCT(tuple);
		Assert(mgr_updateparm);
		/*get key, value*/
		datumValue = heap_getattr(tuple, Anum_mgr_updateparm_updateparmvalue, RelationGetDescr(rel_updateparm), &isNull);
		if(isNull)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_updateparm")
				, errmsg("column value is null")));
		}
		updateParmValue = pstrdup(TextDatumGetCString(datumValue));

		if (strcmp(updateParmValue, "on") == 0)
			isSync = true;
		else
			isSync = false;
	}
	else
	{
		isSync = false;
	}
	heap_endscan(rel_scan);
	heap_close(rel_updateparm, AccessShareLock);	/* close table */

	return isSync;
}

/* 
 * Check whether the coord or gtmcoord synchronous standby machine is active,
 * avoid cluster startup failure due to synchronous standby not starting after updating pgxc_node.
 */
static bool
check_all_cn_sync_slave_is_active(void)
{
	Relation		rel_mgr_node;
	Form_mgr_node	mgr_node;
	ScanKeyData		key[1];
	HeapScanDesc	rel_scan;
	HeapTuple		tuple;
	NameData		nodesync;
	PGconn			*conn = NULL;
	StringInfoData	connStr;
	bool			is_exist_sync = false;
	bool			is_all_active = true;

	namestrcpy(&nodesync, "sync");
	/* get datanode master info */
	ScanKeyInit(&key[0]
				,Anum_mgr_node_nodesync
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,NameGetDatum(&nodesync));
	rel_mgr_node = heap_open(NodeRelationId, AccessShareLock);
	rel_scan = heap_beginscan_catalog(rel_mgr_node, 1, key);
	while ((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL && is_all_active)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		/* Check whether the standby coord or gtmcoord is synchronized */
		if (mgr_node->nodetype == CNDN_TYPE_GTM_COOR_SLAVE || mgr_node->nodetype == CNDN_TYPE_COORDINATOR_SLAVE)
		{
			is_exist_sync = true;
			/* init coordinate connect string */
			initStringInfo(&connStr);
			appendStringInfo(&connStr, 
								"postgresql://%s@%s:%d/%s", 
								get_hostuser_from_hostoid(mgr_node->nodehost), 
								get_hostaddress_from_hostoid(mgr_node->nodehost), 
								mgr_node->nodeport, 
								DEFAULT_DB);
			appendStringInfoCharMacro(&connStr, '\0');
			
			/* get coordinate connect */
			conn = PQconnectdb(connStr.data);
			if (PQstatus(conn) != CONNECTION_OK)
			{
				pg_usleep(1 * 1000000L);
				conn = PQconnectdb(connStr.data);
				if (PQstatus(conn) != CONNECTION_OK)
				{
					is_all_active = false;
					ereport(WARNING, 
						(errmsg("There is an unstarted coordinator synchronous slave %s in the cluster, updating pgxc_node read-only standby information failed.", mgr_node->nodename.data)));
		
				}
			}
			pfree(connStr.data);
			PQfinish(conn);
		}
		
	}
	heap_endscan(rel_scan);
	heap_close(rel_mgr_node, AccessShareLock);	/* close table */

	if ((is_exist_sync && is_all_active) || !is_exist_sync)
		return true;
	else
		return false;
	
}

/* Clear the slave node information about the read-only query in the pgxc_node table,
 * avoid repeating node names and causing subsequent work to fail */
void mgr_clean_cn_pgxcnode_readonlysql_slave(void)
{
	InitNodeInfo	*info;
	ScanKeyData		cnkey[2];
	HeapTuple		tuple;
	Form_mgr_node	mgr_node;
	PGconn			*conn = NULL;
	PGresult		*res = NULL;
	StringInfoData	connStr, cleanSql;

	/* get CN master info */
	ScanKeyInit(&cnkey[0]
				,Anum_mgr_node_nodeinited
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
	ScanKeyInit(&cnkey[1]
			,Anum_mgr_node_nodetype
			,BTEqualStrategyNumber
			,F_CHAREQ
			,CharGetDatum(CNDN_TYPE_COORDINATOR_MASTER));
			
	info = palloc(sizeof(*info));
	info->rel_node = heap_open(NodeRelationId, AccessShareLock);	/* open table */
	info->lcp = NULL;
	info->rel_scan = heap_beginscan_catalog(info->rel_node, 2, cnkey);
	while ((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);

		/* init coordinate connect string */
		initStringInfo(&connStr);
		appendStringInfo(&connStr, 
					"postgresql://%s@%s:%d/%s", 
					get_hostuser_from_hostoid(mgr_node->nodehost), 
					get_hostaddress_from_hostoid(mgr_node->nodehost), 
					mgr_node->nodeport, 
					DEFAULT_DB);
		appendStringInfoCharMacro(&connStr, '\0');
		/* get coordinate connect */
		conn = PQconnectdb(connStr.data);
		if (PQstatus(conn) != CONNECTION_OK)
		{
			pg_usleep(1 * 1000000L);
			conn = PQconnectdb(connStr.data);
			if (PQstatus(conn) != CONNECTION_OK)
			{
				PQfinish(conn);
				pfree(connStr.data);
				ereport(WARNING, 
						(errmsg("Attempt to link to the node '%s' failed, please confirm that the cluster is running. %s", 
								mgr_node->nodename.data, 
								PQerrorMessage(conn))));
				continue;
			}
		}
		initStringInfo(&cleanSql);
		/* Generate clear information about the slave node in the pgxc_node table */
		appendStringInfoString(&cleanSql, "delete from pgxc_node where node_type = 'E';");
		res = PQexec(conn, cleanSql.data);
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			PQclear(res);
			PQfinish(conn);
			pfree(connStr.data);
			pfree(cleanSql.data);
			ereport(WARNING, 
					(errmsg("Failed to clean pgxc_node in '%s'.", 
							mgr_node->nodename.data)));
			continue;
		}
		else
		{
			ereport(NOTICE, 
					(errmsg("Clearing pgxc_node in '%s' successfully.", 
							mgr_node->nodename.data)));
		}
		PQclear(res);
		PQfinish(conn);
		pfree(connStr.data);
		pfree(cleanSql.data);
	}	
	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);	/* close table */
	pfree(info);
}

/* 
 * Obtaining read-write separation status information 
 * of specified nodes based on parameter information.
 */
static void
check_readsql_slave_param_state(Form_mgr_node cn_master_node, List *sync_parms, char *key, bool *state)
{
	bool		isUpdate = false;
	ListCell	*cell;
	
	foreach (cell, sync_parms)
	{
		ReadonlyUpdateparm *updateparm = (ReadonlyUpdateparm *) lfirst(cell);

		/* If the coordinator node name is specified explicitly, the relevant settings of the nodeName wildcard'*'will be ignored. */
		if (strcmp(NameStr(cn_master_node->nodename), NameStr(updateparm->updateparmnodename)) == 0 
			|| (!isUpdate && strcmp(NameStr(updateparm->updateparmnodename), "*") == 0))
		{
			if (strcmp(NameStr(updateparm->updateparmkey), key) == 0
				&& (updateparm->updateparmnodetype == cn_master_node->nodetype 
					|| (updateparm->updateparmnodetype == CNDN_TYPE_GTMCOOR && (cn_master_node->nodetype == CNDN_TYPE_GTM_COOR_MASTER || cn_master_node->nodetype == CNDN_TYPE_GTM_COOR_SLAVE))
					|| (updateparm->updateparmnodetype == CNDN_TYPE_COORDINATOR && (cn_master_node->nodetype == CNDN_TYPE_COORDINATOR_MASTER || cn_master_node->nodetype == CNDN_TYPE_COORDINATOR_SLAVE))
				))
			{
				if (strcmp(NameStr(updateparm->updateparmvalue), "on") == 0)
					*state = true;
				else
					*state = false;
				isUpdate = true;
			}
		}
	}
}

char *getMgrNodeSyncStateValue(sync_state state)
{
	return sync_state_tab[state].name;
}

uint64 updateDoctorStatusOfMgrNodes(List *nodenames, char nodetype, bool allowcure, char *curestatus)
{
	int ret;
	int i;
	StringInfoData buf;
	uint64 rows;
	StringInfoData dynamic;
	ListCell *lc;
	char *nodename;

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, 
				(errmsg("ADB Manager SPI_connect failed: error code %d", 
						ret)));
	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "update pg_catalog.mgr_node  "
					 "set allowcure = %d::boolean ",
					 allowcure);
	if(curestatus !=NULL && strlen(curestatus) > 0)
	{
		appendStringInfo(&buf, 
						", curestatus = '%s' ",
						curestatus);
	}
	if(nodenames != NIL || nodetype != 0)
	{
		initStringInfo(&dynamic);
		if(nodenames != NIL)
		{
			appendStringInfo(&dynamic," AND nodename in ( ");
			i = 0;
			foreach(lc, nodenames)
			{
				i++;
				nodename = lfirst(lc);
				appendStringInfo(&dynamic," '%s' ", nodename);
				if(i < nodenames->length)
				{
					appendStringInfo(&dynamic,",");
				}
			}
			appendStringInfo(&dynamic," ) ");
		}
		if(nodetype != 0)
		{
			appendStringInfo(&dynamic,
					 		 " AND nodetype = '%c' ",
					 		 nodetype);
		}
		appendStringInfo(&buf, 
						 " WHERE %s", 
						 strcasestr(dynamic.data, "AND") + 3);
		pfree(dynamic.data);
	}
	ret = SPI_execute(buf.data, false, 0);
	pfree(buf.data);
	if (ret != SPI_OK_UPDATE)
		ereport(ERROR, 
				(errmsg("ADB Manager SPI_execute \"%s\"failed: error code %d", 
						buf.data, 
						ret)));
	rows = SPI_processed;
	SPI_finish();
	return rows;
}

/**
 * If nodename is NULL, ignore condition "and nodename=?".
 * If nodetype is 0, ignore condition "and nodetype=?".
 */
uint64 updateDoctorStatusOfMgrNode(char *nodename, char nodetype, bool allowcure, char *curestatus)
{
	uint64 rows;
	List *nodenames = NIL;

	if(nodename)
		nodenames = lappend(nodenames, nodename);
	rows = updateDoctorStatusOfMgrNodes(nodenames, nodetype, allowcure, curestatus);
	list_free(nodenames);
	return rows;
}

uint64 updateAllowcureOfMgrHosts(List *hostnames, bool allowcure)
{
	int ret;
	int i;
	StringInfoData buf;
	uint64 rows;
	ListCell *lc;
	char *hostname;

	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, 
				(errmsg("ADB Manager SPI_connect failed: error code %d", 
						ret)));
	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "update pg_catalog.mgr_host  "
					 "set allowcure = %d::boolean ",
					 allowcure);
	if(hostnames != NIL)
	{
		appendStringInfo(&buf," WHERE hostname in ( ");
		i = 0;
		foreach(lc, hostnames)
		{
			i++;
			hostname = lfirst(lc);
			appendStringInfo(&buf," '%s' ", hostname);
			if(i < hostnames->length)
				appendStringInfo(&buf,",");
		}
		appendStringInfo(&buf," ) ");
	}
	ret = SPI_execute(buf.data, false, 0);
	pfree(buf.data);
	if (ret != SPI_OK_UPDATE)
		ereport(ERROR, 
				(errmsg("ADB Manager SPI_execute \"%s\"failed: error code %d", 
						buf.data, 
						ret)));
	rows = SPI_processed;
	SPI_finish();
	return rows;
}

/**
 * If hostname is NULL, ignore condition "and hostname=?". 
 */
uint64 updateAllowcureOfMgrHost(char *hostname, bool allowcure)
{
	uint64 rows;
	List *hostnames = NIL;

	if(hostname)
		hostnames = lappend(hostnames, hostname);
	rows = updateAllowcureOfMgrHosts(hostnames, allowcure);
	list_free(hostnames);
	return rows;
}