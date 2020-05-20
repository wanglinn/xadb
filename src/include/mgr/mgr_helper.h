/*--------------------------------------------------------------------------
 *
 * Copyright (c) 2018-2019, Asiainfo Database Innovation Lab
 *
 * -------------------------------------------------------------------------
 */

#ifndef MGR_HELPER_H
#define MGR_HELPER_H

#include "postgres.h"
#include "lib/ilist.h"
#include "catalog/mgr_node.h"
#include "catalog/mgr_host.h"
#include "utils/palloc.h"
#include "mgr/mgr_agent.h"
#include "mgr/mgr_msg_type.h"
#include "access/xlogdefs.h"
#include "nodes/pg_list.h"
#include "../../interfaces/libpq/libpq-fe.h"

#define CHECK_SYNC_STANDBY_NAMES_SECONDS 60
#define CHECK_GTM_INFO_SECONDS 60
#define SHUTDOWN_NODE_FAST_SECONDS 5
#define SHUTDOWN_NODE_IMMEDIATE_SECONDS 90
#define STARTUP_NODE_SECONDS 90

#define EXTRACT_GTM_INFOMATION(gtmMaster, agtm_host,  \
							   agtm_port)             \
	do                                                \
	{                                                 \
		Assert(gtmMaster != NULL);                    \
		agtm_host = gtmMaster->host->hostaddr;        \
		pg_ltoa(gtmMaster->form.nodeport, agtm_port); \
	} while (0)

typedef enum NodeConnectionStatus
{
	NODE_CONNECTION_STATUS_FAIL,
	NODE_CONNECTION_STATUS_SUCCESS,
	NODE_CONNECTION_STATUS_BUSY,
	NODE_CONNECTION_STATUS_STARTING_UP
} NodeConnectionStatus;

typedef enum NodeRunningMode
{
	NODE_RUNNING_MODE_UNKNOW,
	NODE_RUNNING_MODE_MASTER,
	NODE_RUNNING_MODE_SLAVE
} NodeRunningMode;

typedef struct NodeRecoveryStatus
{
	bool queryOk;
	bool isInRecovery;
} NodeRecoveryStatus;

/* pfree the element 'message' that in 
 * this struct if you don't need it anymore. */
typedef struct CallAgentResult
{
	bool agentRes;
	StringInfoData message;
} CallAgentResult;

typedef struct PingNodeResult
{
	bool agentRes;
	PGPing pgPing;
} PingNodeResult;

typedef struct PGHbaItem
{
	ConnectType type;
	char *database;
	char *user;
	char *address;
	int netmask;
	char *method;
	struct PGHbaItem *next;
} PGHbaItem;

typedef struct PGConfParameterItem
{
	char *name;
	char *value;
	bool quoteValue;
	struct PGConfParameterItem *next;
} PGConfParameterItem;

typedef struct MgrHostWrapper
{
	FormData_mgr_host form;
	Oid oid;
	char *hostaddr;
	char *hostadbhome;
	dlist_node link;
	dlist_node cmpLink;
} MgrHostWrapper;

typedef struct MgrNodeWrapper
{
	FormData_mgr_node form;
	Oid oid;
	char *nodepath;
	MgrHostWrapper *host;
	dlist_node link;
	dlist_node cmpLink;
} MgrNodeWrapper;

typedef struct SynchronousStandbyNamesConfig
{
	int num_sync;
	uint8 syncrep_method;
	List *syncStandbyNames;
	List *potentialStandbyNames;
} SynchronousStandbyNamesConfig;

static inline PGHbaItem *newPGHbaItem(ConnectType type,
									  char *database,
									  char *user,
									  char *address,
									  int netmask,
									  char *method)
{
	PGHbaItem *item = palloc(sizeof(PGHbaItem));
	item->type = type;
	item->database = psprintf("%s", database);
	item->user = psprintf("%s", user);
	item->address = psprintf("%s", address);
	item->netmask = netmask;
	item->method = psprintf("%s", method);
	item->next = NULL;
	return item;
}

static inline void pfreePGHbaItem(PGHbaItem *obj)
{
	PGHbaItem *curr;
	PGHbaItem *next;
	if (obj)
	{
		curr = obj;
		while (curr)
		{
			pfree(curr->database);
			pfree(curr->user);
			pfree(curr->address);
			pfree(curr->method);
			next = curr->next;
			pfree(curr);
			curr = next;
		}
		obj = NULL;
	}
}

/**
 * pfree the returned result if you don't need it anymore.
 */
static inline char *toStringPGHbaItem(PGHbaItem *obj)
{
	PGHbaItem *curr;
	StringInfoData str;
	if (obj)
	{
		initStringInfo(&str);
		curr = obj;
		while (curr)
		{
			appendStringInfo(&str, "%d,%s,%s,%s,%d,%s\n",
							 curr->type,
							 curr->database,
							 curr->user,
							 curr->address,
							 curr->netmask,
							 curr->method);
			curr = curr->next;
		}
		/* delete the last \n */
		str.len = str.len - 1;
		str.data[str.len] = '\0';
		return str.data;
	}
	else
	{
		return (char *)palloc0(1);
	}
}

static inline PGConfParameterItem *newPGConfParameterItem(char *name,
														  char *value,
														  bool quoteValue)
{
	PGConfParameterItem *item;
	Assert(name != NULL);
	item = palloc(sizeof(PGConfParameterItem));
	item->name = psprintf("%s", name);
	item->value = psprintf("%s", (value == NULL) ? "" : value);
	item->quoteValue = quoteValue;
	item->next = NULL;
	return item;
}

static inline void pfreePGConfParameterItem(PGConfParameterItem *obj)
{
	PGConfParameterItem *curr;
	PGConfParameterItem *next;
	if (obj)
	{
		curr = obj;
		while (curr)
		{
			pfree(curr->name);
			pfree(curr->value);
			next = curr->next;
			pfree(curr);
			curr = next;
		}
		obj = NULL;
	}
}

/**
 * pfree the returned result if you don't need it anymore.
 */
static inline char *toStringPGConfParameterItem(PGConfParameterItem *obj)
{
	PGConfParameterItem *curr;
	StringInfoData str;
	if (obj)
	{
		initStringInfo(&str);
		curr = obj;
		while (curr)
		{
			appendStringInfo(&str, "%s=", curr->name);
			if (curr->quoteValue)
			{
				appendStringInfo(&str, "'%s'", curr->value);
			}
			else
			{
				appendStringInfo(&str, "%s", curr->value);
			}
			appendStringInfoString(&str, "\t");
			curr = curr->next;
		}
		/* delete the last \n */
		str.len = str.len - 1;
		str.data[str.len] = '\0';
		return str.data;
	}
	else
	{
		return (char *)palloc0(1);
	}
}

static inline void pfreeMgrHostWrapper(MgrHostWrapper *obj)
{
	if (obj)
	{
		if (obj->hostaddr)
		{
			pfree(obj->hostaddr);
			obj->hostaddr = NULL;
		}
		if (obj->hostadbhome)
		{
			pfree(obj->hostadbhome);
			obj->hostadbhome = NULL;
		}
		pfree(obj);
		obj = NULL;
	}
}

static inline void pfreeMgrNodeWrapper(MgrNodeWrapper *obj)
{
	if (obj)
	{
		if (obj->nodepath)
		{
			pfree(obj->nodepath);
			obj->nodepath = NULL;
		}
		if (obj->host)
		{
			pfreeMgrHostWrapper(obj->host);
			obj->host = NULL;
		}
		pfree(obj);
		obj = NULL;
	}
}

static inline void pfreeMgrHostWrapperList(dlist_head *list,
										   MgrHostWrapper *exclude)
{
	dlist_mutable_iter miter;
	MgrHostWrapper *data;

	dlist_foreach_modify(miter, list)
	{
		data = dlist_container(MgrHostWrapper, link, miter.cur);
		dlist_delete(miter.cur);
		if (data != exclude)
		{
			pfreeMgrHostWrapper(data);
		}
	}
}

static inline void pfreeMgrNodeWrapperList(dlist_head *list,
										   MgrNodeWrapper *exclude)
{
	dlist_mutable_iter miter;
	MgrNodeWrapper *data;

	dlist_foreach_modify(miter, list)
	{
		data = dlist_container(MgrNodeWrapper, link, miter.cur);
		dlist_delete(miter.cur);
		if (data != exclude)
		{
			pfreeMgrNodeWrapper(data);
		}
	}
}

extern void logMgrNodeWrapper(MgrNodeWrapper *src, char *title, int elevel);
extern void logMgrHostWrapper(MgrHostWrapper *src, char *title, int elevel);

extern char getMgrMasterNodetype(char slaveNodetype);
extern char getMgrSlaveNodetype(char masterNodetype);
extern bool isMasterNode(char nodetype, bool complain);
extern bool isSlaveNode(char nodetype, bool complain);

/* spi functions */
extern void selectMgrNodes(char *sql,
						   MemoryContext spiContext,
						   dlist_head *resultList);
extern MgrNodeWrapper *selectMgrNodeByOid(Oid oid, MemoryContext spiContext);
extern MgrNodeWrapper *selectMgrNodeByNodenameType(char *nodename,
												   char nodetype,
												   MemoryContext spiContext);
extern void selectActiveMasterCoordinators(MemoryContext spiContext,
										   dlist_head *resultList);
extern void selectMgrNodeByNodetype(MemoryContext spiContext,
									char nodetype,
									dlist_head *resultList);
extern void selectActiveMgrNodeByNodetype(MemoryContext spiContext,
										  char nodetype,
										  dlist_head *resultList);
extern void selectMgrAllDataNodes(MemoryContext spiContext,
								  dlist_head *resultList);
extern void selectActiveMgrSlaveNodes(Oid masterOid,
									  char nodetype,
									  MemoryContext spiContext,
									  dlist_head *resultList);
extern void selectActiveMgrSlaveNodesInZone(Oid masterOid,
							   char nodetype,
							   char *zone,
							   MemoryContext spiContext,
							   dlist_head *resultList);																			  
extern void selectSiblingActiveNodes(MgrNodeWrapper *faultNode,
									 dlist_head *resultList,
									 MemoryContext spiContext);
extern void selectIsolatedMgrSlaveNodes(Oid masterOid,
										char nodetype,
										MemoryContext spiContext,
										dlist_head *resultList);
extern void selectIsolatedMgrSlaveNodesByNodeType(char nodetype,
										MemoryContext spiContext,
										dlist_head *resultList);										
extern void selectAllMgrSlaveNodes(Oid masterOid,
								   char nodetype,
								   MemoryContext spiContext,
								   dlist_head *resultList);
extern void selectMgrNodesForNodeDoctors(MemoryContext spiContext,
										 dlist_head *resultList);
extern MgrNodeWrapper *selectMgrNodeForNodeDoctor(Oid oid,
												  MemoryContext spiContext);
extern void selectMgrNodesForSwitcherDoctor(MemoryContext spiContext,
											dlist_head *resultList);
extern void selectMgrNodesForRepairerDoctor(MemoryContext spiContext,
											char nodetype,
											dlist_head *resultList);
extern void selectIsolatedMgrNodes(MemoryContext spiContext,
								   dlist_head *resultList);
extern void selectNodeNotZone(MemoryContext spiContext, 
									char *zone, 
									char nodetype,
									dlist_head *resultList);
extern void selectNodeZoneOid(MemoryContext spiContext, 
							char nodetype,
							char *nodezone,
							Oid  oid, 
							dlist_head *resultList);							
extern MgrNodeWrapper *selectMgrGtmCoordNode(MemoryContext spiContext);
extern int updateMgrNodeCurestatus(MgrNodeWrapper *mgrNode,
								   char *newCurestatus,
								   MemoryContext spiContext);
extern int updateMgrNodeNodesync(MgrNodeWrapper *mgrNode,
								 char *newNodesync,
								 MemoryContext spiContext);
extern int updateMgrNodeAfterFollowMaster(MgrNodeWrapper *mgrNode,
										  char *newCurestatus,
										  MemoryContext spiContext);
extern int updateMgrNodeToIsolate(MgrNodeWrapper *mgrNode,
								  MemoryContext spiContext);
extern int updateMgrNodeToUnIsolate(MgrNodeWrapper *mgrNode,
									MemoryContext spiContext);
extern int executeUpdateSql(char *sql, MemoryContext spiContext);
extern void selectMgrHosts(char *sql,
						   MemoryContext spiContext,
						   dlist_head *resultList);
extern MgrHostWrapper *selectMgrHostByOid(Oid oid, MemoryContext spiContext);
extern void selectMgrHostsForHostDoctor(MemoryContext spiContext,
										dlist_head *resultList);

/* libpq functions */
extern NodeConnectionStatus connectNodeDefaultDB(MgrNodeWrapper *node,
												 int connectTimeout,
												 PGconn **pgConn);
extern PGconn *getNodeDefaultDBConnection(MgrNodeWrapper *mgrNode,
										  int connectTimeout);
extern PGPing pingNodeDefaultDB(MgrNodeWrapper *node,
								int connectTimeout);
extern XLogRecPtr getNodeLastWalReceiveLsn(PGconn *pgConn);
extern XLogRecPtr getNodeCurrentWalLsn(PGconn *pgConn);
extern XLogRecPtr getNodeWalLsn(PGconn *pgConn, NodeRunningMode runningMode);
extern NodeRunningMode getNodeRunningMode(PGconn *pgConn);
extern NodeRunningMode getExpectedNodeRunningMode(bool isMaster);
extern bool checkNodeRunningMode(PGconn *pgConn, bool isMaster);
extern bool setNodeParameter(PGconn *pgConn, char *name, char *value);
extern char *showNodeParameter(PGconn *pgConn, char *name, bool complain);
extern bool PQexecCommandSql(PGconn *pgConn, char *sql, bool complain);
extern bool PQexecBoolQuery(PGconn *pgConn, char *sql, bool expectedValue, bool complain);
extern int PQexecCountSql(PGconn *pgConn, char *sql, bool complain);
extern bool equalsNodeParameter(PGconn *pgConn, char *parameterName,
								char *expectValue);

extern bool exec_pgxc_pool_reload(PGconn *coordCoon,
								  bool localExecute,
								  char *executeOnNodeName,
								  bool complain);
extern bool exec_pg_pause_cluster(PGconn *pgConn, bool complain);
extern bool exec_pg_unpause_cluster(PGconn *pgConn, bool complain);
extern bool exec_pool_close_idle_conn(PGconn *pgConn, bool complain);

/* agent functions */
extern CallAgentResult callAgentSendCmd(AgentCommand cmd,
										StringInfo cmdMessage,
										char *hostaddr,
										int32 hostagentport);
extern void PGHbaItemsToCmdMessage(char *nodepath,
								   PGHbaItem *items,
								   StringInfo cmdMessage);
extern void PGConfParameterItemsToCmdMessage(char *nodepath,
											 PGConfParameterItem *items,
											 StringInfo cmdMessage);
extern bool callAgentDeletePGHbaConf(MgrNodeWrapper *node,
									 PGHbaItem *items, bool complain);
extern bool callAgentRefreshPGHbaConf(MgrNodeWrapper *node,
									  PGHbaItem *items, bool complain);
extern bool callAgentReloadNode(MgrNodeWrapper *node, bool complain);

extern bool callAgentRefreshPGSqlConfReload(MgrNodeWrapper *node,
											PGConfParameterItem *items,
											bool complain);
extern bool callAgentRefreshPGSqlConf(MgrNodeWrapper *node,
									  PGConfParameterItem *items,
									  bool complain);
extern bool callAgentRefreshRecoveryConf(MgrNodeWrapper *node,
										 PGConfParameterItem *items,
										 bool complain);
extern bool callAgentPromoteNode(MgrNodeWrapper *node, bool complain);
extern bool callAgentStopNode(MgrNodeWrapper *node, char *shutdownMode,
							  bool wait, bool complain);
extern bool callAgentStartNode(MgrNodeWrapper *node,
							   bool wait, bool complain);
extern bool callAgentRestartNode(MgrNodeWrapper *node,
								 char *shutdownMode, bool complain);
extern bool callAgentRewindNode(MgrNodeWrapper *masterNode,
								MgrNodeWrapper *slaveNode, bool complain);
extern void getCallAgentSqlString(MgrNodeWrapper *node,
								  char *sql, StringInfo cmdMessage);
extern PingNodeResult callAgentPingNode(MgrNodeWrapper *node);
extern CallAgentResult callAgentExecuteSql(MgrNodeWrapper *node, char *sql);
extern CallAgentResult callAgentExecuteSqlCommand(MgrNodeWrapper *node, char *sql);
extern NodeRecoveryStatus callAgentGet_pg_is_in_recovery(MgrNodeWrapper *node);
extern XLogRecPtr parseLsnToXLogRecPtr(const char *str);
extern XLogRecPtr callAgentGet_pg_last_wal_receive_lsn(MgrNodeWrapper *node);

extern bool setPGHbaTrustAddress(MgrNodeWrapper *mgrNode, char *address);
extern void setPGHbaTrustSlaveReplication(MgrNodeWrapper *masterNode,
										  MgrNodeWrapper *slaveNode);
extern void setSynchronousStandbyNames(MgrNodeWrapper *mgrNode,
									   char *value);
extern void setCheckSynchronousStandbyNames(MgrNodeWrapper *mgrNode,
											PGconn *pgConn,
											char *value,
											int checkSeconds);
extern bool setGtmInfoInPGSqlConf(MgrNodeWrapper *mgrNode,
								  MgrNodeWrapper *gtmMaster,
								  bool complain);
extern bool checkGtmInfoInPGresult(PGresult *pgResult,
								   char *agtm_host,
								   char *agtm_port);
extern bool checkGtmInfoInPGSqlConf(PGconn *pgConn,
									char *nodename,
									bool localSqlCheck,
									MgrNodeWrapper *gtmMaster);
extern void setCheckGtmInfoInPGSqlConf(MgrNodeWrapper *gtmMaster,
									   MgrNodeWrapper *mgrNode,
									   PGconn *pgConn,
									   bool localSqlCheck,
									   int checkSeconds,
									   bool complain);
extern void setSlaveNodeRecoveryConf(MgrNodeWrapper *masterNode,
									 MgrNodeWrapper *slaveNode);

extern char *trimString(char *str);
extern bool equalsAfterTrim(char *str1, char *str2);

extern bool pingNodeWaitinSeconds(MgrNodeWrapper *node,
								  PGPing expectedPGPing,
								  int waitSeconds);
extern bool shutdownNodeWithinSeconds(MgrNodeWrapper *mgrNode,
									  int fastModeSeconds,
									  int immediateModeSeconds,
									  bool complain);
extern bool startupNodeWithinSeconds(MgrNodeWrapper *mgrNode,
									 int waitSeconds,
									 bool complain);
extern bool batchPingNodesWaitinSeconds(dlist_head *nodes,
										dlist_head *failedModes,
										PGPing expectedPGPing,
										int waitSeconds);
extern bool batchShutdownNodesWithinSeconds(dlist_head *nodes,
											int fastModeSeconds,
											int immediateModeSeconds,
											bool complain);
extern bool batchStartupNodesWithinSeconds(dlist_head *nodes,
										   int waitSeconds,
										   bool complain);
extern bool dropNodeFromPgxcNode(PGconn *activeConn,
								 char *executeOnNodeName,
								 bool localExecute,
								 char *nodeName,
								 bool complain);
extern bool createNodeOnPgxcNode(PGconn *activeConn,
								 char *executeOnNodeName,
								 bool localExecute,
								 MgrNodeWrapper *mgrNode,
								 char *masterNodeName,
								 bool complain);
extern bool alterNodeOnPgxcNode(PGconn *activeConn,
								char *executeOnNodeName,
								bool localExecute,
								char *oldNodeName,
								MgrNodeWrapper *newNode,
								bool complain);
extern bool nodenameExistsInPgxcNode(PGconn *activeConn,
									 char *executeOnNodeName,
									 bool localExecute,
									 char *nodeName,
									 char pgxcNodeType,
									 bool complain);
bool isMgrModeExistsInCoordinator(MgrNodeWrapper *coordinator,
								  PGconn *coordConn,
								  bool localExecute,
								  MgrNodeWrapper *mgrNode,
								  bool complain);
extern char getMappedPgxcNodetype(char mgrNodetype);
extern bool isNodeInSyncStandbyNames(MgrNodeWrapper *masterNode,
									 MgrNodeWrapper *slaveNode,
									 PGconn *masterConn);
extern void cleanMgrNodesOnCoordinator(dlist_head *mgrNodes,
									   MgrNodeWrapper *coordinator,
									   PGconn *coordConn,
									   bool complain);
extern void compareAndDropMgrNodeOnCoordinator(MgrNodeWrapper *mgrNode,
											   MgrNodeWrapper *coordinator,
											   PGconn *coordConn,
											   bool localExecute,
											   bool complain);
extern bool isGtmCoordMgrNode(char nodetype);
extern bool isDataNodeMgrNode(char nodetype);
extern bool isCoordinatorMgrNode(char nodetype);
extern bool is_equal_string(char *a, char *b);
extern bool list_contain_string(const List *list, char *str);
extern List *list_delete_string(List *list, char *str, bool deep);
extern bool isSameNodeZone(MgrNodeWrapper *mgrNode1, MgrNodeWrapper *mgrNode2);
extern bool isSameNodeName(MgrNodeWrapper *mgrNode1, MgrNodeWrapper *mgrNode2);
extern SynchronousStandbyNamesConfig *parseSynchronousStandbyNamesConfig(char *synchronous_standby_names,
																		 bool complain);
extern char *transformSynchronousStandbyNamesConfig(List *syncStandbyNames,
													List *potentialStandbyNames);
/**
 * This function may modify MgrNodeWrapper field form.nodesync.
 */
extern void appendToSyncStandbyNames(MgrNodeWrapper *masterNode,
									 MgrNodeWrapper *slaveNode,
									 dlist_head *siblingSlaveNodes,
									 PGconn *masterPGconn,
									 MemoryContext spiContext);
/**
 * This function may modify MgrNodeWrapper's field form.nodesync.
 */
extern void removeFromSyncStandbyNames(MgrNodeWrapper *masterNode,
									   MgrNodeWrapper *slaveNode,
									   dlist_head *siblingSlaveNodes,
									   PGconn *masterPGconn,
									   MemoryContext spiContext);
extern bool setPGHbaTrustMyself(MgrNodeWrapper *mgrNode);
extern void dn_master_replication_slot(char *nodename, char *slot_name, char operate);
extern void MgrGetOldDnMasterNotZone(MemoryContext spiContext, char *currentZone, char nodeType, dlist_head *masterList);

#endif /* MGR_HELPER_H */
