
#ifndef MGR_CNDNNODE_H
#define MGR_CNDNNODE_H

#include "catalog/genbki.h"
#include "catalog/mgr_node_d.h"
#include "nodes/pg_list.h"

CATALOG(mgr_node,9782,NodeRelationId)
{
	Oid			oid;			/* oid */

	/* node name */
	NameData	nodename;

	/* node hostoid from host*/
	Oid			nodehost;

	/* node type */
	char		nodetype;

	/* node sync for slave */
	NameData		nodesync;

	/* node port */
	int32		nodeport;

	/* is initialized */
	bool		nodeinited;

	/* 0 stands for the node is not slave*/
	Oid			nodemasternameoid;

	/*check the node in cluster*/
	bool		nodeincluster;

	NameData	nodezone;

	/* 
	 * a flag that indication "adb doctor extension" whether work or not.
	 * doctor process will not launched until this node by has been initiated and it is in cluster.
	 * or else, doctor process auto exit.
	 */
	bool		allowcure;

	/* see the macro definition which prefixed by CURE_STATUS below */
	NameData	curestatus;

#ifdef CATALOG_VARLEN

	text		nodepath;		/* node data path */
#endif						/* CATALOG_VARLEN */
} FormData_mgr_node;

/* ----------------
 *		Form_mgr_node corresponds to a pointer to a tuple with
 *		the format of mgr_nodenode relation.
 * ----------------
 */
typedef FormData_mgr_node *Form_mgr_node;

DECLARE_UNIQUE_INDEX(mgr_node_oid_index, 9766, on mgr_node using btree(oid oid_ops));
#define NodeOidIndexId 9766

#ifdef EXPOSE_TO_CLIENT_CODE

#define CNDN_TYPE_COORDINATOR_MASTER		'c'
#define CNDN_TYPE_COORDINATOR_SLAVE			's'
#define CNDN_TYPE_DATANODE_MASTER			'd'
#define CNDN_TYPE_DATANODE_SLAVE			'b'

#define CNDN_TYPE_GTM_COOR_MASTER		'g'
#define CNDN_TYPE_GTM_COOR_SLAVE		'p'

/*CNDN_TYPE_DATANODE include : datanode master,slave*/
#define CNDN_TYPE_COORDINATOR		'C'
#define CNDN_TYPE_DATANODE		'D'
#define CNDN_TYPE_GTMCOOR		'G'

/* not exist node type */
#define CNDN_TYPE_NONE			'0'

#define SHUTDOWN_S  "smart"
#define SHUTDOWN_F  "fast"
#define SHUTDOWN_I  "immediate"
#define TAKEPLAPARM_N  "none"

#define	PARAM_ARCHIVE  			"archive"
#define	PARAM_ARCHIVE_MODE  	"archive_mode"
#define	PARAM_ON  				"on"
#define	PARAM_OFF  				"off"

#endif							/* EXPOSE_TO_CLIENT_CODE */

typedef enum AGENT_STATUS
{
	AGENT_DOWN = 4, /*the number is enum PGPing max_value + 1*/
	AGENT_RUNNING
}agent_status;

struct enum_sync_state
{
	int type;
	char *name;
};

typedef enum SYNC_STATE
{
	SYNC_STATE_SYNC,
	SYNC_STATE_ASYNC,
	SYNC_STATE_POTENTIAL,
}sync_state;

typedef enum{
	PGXC_CONFIG,
	PGXC_APPEND,
	PGXC_FAILOVER,
	PGXC_REMOVE
}pgxc_node_operator;

/*the values see agt_cmd.c, used for pg_hba.conf add content*/
typedef enum ConnectType
{
	CONNECT_LOCAL=1,
	CONNECT_HOST,
	CONNECT_HOSTSSL,
	CONNECT_HOSTNOSSL
}ConnectType;

typedef struct nodeInfo
{
	Oid tupleOid;
	Oid hostOid;
	int port;
	NameData name;
	bool isPreferred;
}nodeInfo;


struct enum_recovery_status
{
	int type;
	char *name;
};

typedef enum RECOVERY_STATUS
{
	RECOVERY_IN,
	RECOVERY_NOT_IN,
	RECOVERY_UNKNOWN
}recovery_status;

extern bool with_data_checksums;
extern List *g_initall_options;

#define DEFAULT_DB "postgres"

/* the value of curestatus is not case sensitive */
#define CURE_STATUS_NORMAL "normal"
#define CURE_STATUS_CURING "curing"
#define CURE_STATUS_WAIT_SWITCH "waitswitch"
#define CURE_STATUS_SWITCHING "switching"
#define CURE_STATUS_SWITCHED "switched"
#define CURE_STATUS_FOLLOW_FAIL "followfail"
#define CURE_STATUS_OLD_MASTER "oldmaster"
#define CURE_STATUS_ISOLATED "isolated"


#define GTMCOORD_MASTER_NAME		"gtmcoord master"
#define GTMCOORD_SLAVE_NAME			"gtmcoord slave"
#define COORD_MASTER_NAME			"coordinator master"
#define COORD_SLAVE_NAME			"coordinator slave"
#define DATANODE_MASTER_NAME		"datanode master"
#define DATANODE_SLAVE_NAME			"datanode slave"

#define SET_FORCE_PARALLEL_MODE_OFF		"set FORCE_PARALLEL_MODE = off;" 
#define SELECT_PGXC_POOL_RELOAD       	"select pgxc_pool_reload();"

#endif /* MGR_CNDNNODE_H */
