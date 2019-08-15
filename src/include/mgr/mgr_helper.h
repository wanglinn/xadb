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
#include "../../interfaces/libpq/libpq-fe.h"

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
	Oid hostOid;
	char *hostaddr;
	char *hostadbhome;
	dlist_node link;
} MgrHostWrapper;

typedef struct MgrNodeWrapper
{
	FormData_mgr_node form;
	Oid nodeOid;
	char *nodepath;
	MgrHostWrapper *host;
	dlist_node link;
} MgrNodeWrapper;

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
	PGConfParameterItem *item = palloc(sizeof(PGConfParameterItem));
	item->name = psprintf("%s", name);
	item->value = psprintf("%s", value);
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
			appendStringInfo(&str, "%s,", curr->name);
			if (curr->quoteValue)
			{
				appendStringInfo(&str, "'%s'", curr->value);
			}
			else
			{
				appendStringInfo(&str, "%s", curr->value);
			}
			appendStringInfoString(&str, "\n");
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

extern char getMgrMasterNodetype(char slaveNodetype);
extern char getMgrSlaveNodetype(char masterNodetype);
extern bool isMasterNode(char nodetype, bool complain);
extern bool isSlaveNode(char nodetype, bool complain);

/* spi functions */
extern MgrHostWrapper *selectMgrHostByOid(Oid oid, MemoryContext spiContext);
extern MgrNodeWrapper *selectMgrNodeByOid(Oid oid, MemoryContext spiContext);
extern MgrNodeWrapper *selectMgrNodeByNodename(char *nodename,
											   MemoryContext spiContext);
extern void selectMgrNodes(StringInfo sql,
						   MemoryContext spiContext,
						   dlist_head *resultList);
extern void selectMgrMasterCoordinators(MemoryContext spiContext,
										dlist_head *resultList);
extern void selectMgrSlaveNodes(Oid masterOid,
								char nodetype,
								MemoryContext spiContext,
								dlist_head *resultList);

/* libpq functions */
extern char *getNodePGUser(char nodetype, char *hostuser);
extern NodeConnectionStatus connectNodeDefaultDB(MgrNodeWrapper *node,
												 int connectTimeout,
												 PGconn **pgConn);
extern XLogRecPtr getNodeLastWalReceiveLsn(PGconn *pgConn);
extern XLogRecPtr getNodeCurrentWalLsn(PGconn *pgConn);
extern XLogRecPtr getNodeWalLsn(PGconn *pgConn, NodeRunningMode runningMode);
extern NodeRunningMode getNodeRunningMode(PGconn *pgConn);
extern NodeRunningMode getExpectedNodeRunningMode(bool isMaster);
extern bool checkNodeRunningMode(PGconn *pgConn, bool isMaster);
extern bool setNodeParameter(PGconn *pgConn, char *name, char *value);
extern char *showNodeParameter(PGconn *pgConn, char *name);
extern bool PQexecCommandSql(PGconn *pgConn, char *sql, bool complain);
extern bool PQexecBoolQuery(PGconn *pgConn, char *sql, bool expectedValue, bool complain);
extern int PQexecCountSql(PGconn *pgConn, char *sql, bool complain);
extern bool equalsNodeParameter(PGconn *pgConn, char *parameterName,
								char *expectValue);

extern bool exec_pgxc_pool_reload(PGconn *pgConn, bool complain);
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
extern bool callAgentRefreshRecoveryConf(MgrNodeWrapper *node,
										 PGConfParameterItem *items,
										 bool complain);
extern bool callAgentPromoteNode(MgrNodeWrapper *node, bool complain);
extern bool callAgentStopNode(MgrNodeWrapper *node,
							  char *shutdownMode, bool complain);
extern bool callAgentStartNode(MgrNodeWrapper *node, bool complain);
extern bool callAgentRestartNode(MgrNodeWrapper *node,
								 char *shutdownMode, bool complain);
extern void getCallAgentSqlString(MgrNodeWrapper *node,
								  char *sql, StringInfo cmdMessage);
extern PingNodeResult callAgentPingNode(MgrNodeWrapper *node);
extern CallAgentResult callAgentExecuteSql(MgrNodeWrapper *node, char *sql);
extern NodeRecoveryStatus callAgentGet_pg_is_in_recovery(MgrNodeWrapper *node);
extern XLogRecPtr parseLsnToXLogRecPtr(const char *str);
extern XLogRecPtr callAgentGet_pg_last_wal_receive_lsn(MgrNodeWrapper *node);

#endif /* MGR_HELPER_H */
