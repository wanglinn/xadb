/*--------------------------------------------------------------------------
 *
 * Copyright (c) 2018-2019, Asiainfo Database Innovation Lab
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "parser/mgr_node.h"
#include "mgr/mgr_helper.h"
#include "mgr/mgr_msg_type.h"
#include "mgr/mgr_cmds.h"
#include "access/htup_details.h"
#include "executor/spi.h"
#include "utils/memutils.h"
#include "utils/builtins.h"
#include "../../src/interfaces/libpq/libpq-fe.h"
#include "../../src/interfaces/libpq/libpq-int.h"
#include "catalog/pgxc_node.h"

static MgrHostWrapper *popHeadMgrHostPfreeOthers(dlist_head *mgrHosts);
static MgrNodeWrapper *popHeadMgrNodePfreeOthers(dlist_head *mgrNodes);

void logMgrNodeWrapper(MgrNodeWrapper *src, char *title, int elevel)
{
	char *rTitle = "";
	if (title != NULL && strlen(title) > 0)
		rTitle = title;
	ereport(elevel,
			(errmsg("%s oid:%u,nodename:%s,nodehost:%u,nodetype:%c,nodesync:%s,nodeport:%d,nodemasternameoid:%u,curestatus:%s",
					rTitle,
					src->oid,
					NameStr(src->form.nodename),
					src->form.nodehost,
					src->form.nodetype,
					NameStr(src->form.nodesync),
					src->form.nodeport,
					src->form.nodemasternameoid,
					NameStr(src->form.curestatus))));
}

void logMgrHostWrapper(MgrHostWrapper *src, char *title, int elevel)
{
	char *rTitle = "";
	if (title != NULL && strlen(title) > 0)
		rTitle = title;
	ereport(elevel,
			(errmsg("%s oid:%u,hostname:%s,hostuser:%s,hostport:%d,hostaddr:%s,hostagentport:%d,hostadbhome:%s",
					rTitle,
					src->oid,
					NameStr(src->form.hostname),
					NameStr(src->form.hostuser),
					src->form.hostport,
					src->hostaddr,
					src->form.hostagentport,
					src->hostadbhome)));
}

char getMgrMasterNodetype(char nodetype)
{
	switch (nodetype)
	{
	case CNDN_TYPE_COORDINATOR_MASTER:
	case CNDN_TYPE_COORDINATOR_SLAVE:
		return CNDN_TYPE_COORDINATOR_MASTER;
	case CNDN_TYPE_DATANODE_MASTER:
	case CNDN_TYPE_DATANODE_SLAVE:
		return CNDN_TYPE_DATANODE_MASTER;
	case CNDN_TYPE_GTM_COOR_MASTER:
	case CNDN_TYPE_GTM_COOR_SLAVE:
		return CNDN_TYPE_GTM_COOR_MASTER;
	default:
		ereport(ERROR,
				(errmsg("Unexpected nodetype:%c",
						nodetype)));
		break;
	}
}

char getMgrSlaveNodetype(char nodetype)
{
	switch (nodetype)
	{
	case CNDN_TYPE_COORDINATOR_MASTER:
	case CNDN_TYPE_COORDINATOR_SLAVE:
		return CNDN_TYPE_COORDINATOR_SLAVE;
	case CNDN_TYPE_DATANODE_MASTER:
	case CNDN_TYPE_DATANODE_SLAVE:
		return CNDN_TYPE_DATANODE_SLAVE;
	case CNDN_TYPE_GTM_COOR_MASTER:
	case CNDN_TYPE_GTM_COOR_SLAVE:
		return CNDN_TYPE_GTM_COOR_SLAVE;
	default:
		ereport(ERROR,
				(errmsg("Unexpected nodetype:%c",
						nodetype)));
		break;
	}
}

/**
 * If is master node return true,
 * If is slave node return false,
 * otherwise complain.
 */
bool isMasterNode(char nodetype, bool complain)
{
	switch (nodetype)
	{
	case CNDN_TYPE_COORDINATOR_MASTER:
		return true;
	case CNDN_TYPE_COORDINATOR_SLAVE:
		return false;
	case CNDN_TYPE_DATANODE_MASTER:
		return true;
	case CNDN_TYPE_DATANODE_SLAVE:
		return false;
	case CNDN_TYPE_GTM_COOR_MASTER:
		return true;
	case CNDN_TYPE_GTM_COOR_SLAVE:
		return false;
	default:
		ereport(complain ? ERROR : LOG,
				(errmsg("Unexpected nodetype:%c",
						nodetype)));
		return false;
	}
}

bool isSlaveNode(char nodetype, bool complain)
{
	switch (nodetype)
	{
	case CNDN_TYPE_COORDINATOR_MASTER:
		return false;
	case CNDN_TYPE_COORDINATOR_SLAVE:
		return true;
	case CNDN_TYPE_DATANODE_MASTER:
		return false;
	case CNDN_TYPE_DATANODE_SLAVE:
		return true;
	case CNDN_TYPE_GTM_COOR_MASTER:
		return false;
	case CNDN_TYPE_GTM_COOR_SLAVE:
		return true;
	default:
		ereport(complain ? ERROR : LOG,
				(errmsg("Unexpected nodetype:%c",
						nodetype)));
		return false;
	}
}

static MgrHostWrapper *popHeadMgrHostPfreeOthers(dlist_head *mgrHosts)
{
	dlist_node *ptr;
	MgrHostWrapper *host = NULL;

	if (dlist_is_empty(mgrHosts))
	{
		return NULL;
	}
	else
	{
		ptr = dlist_pop_head_node(mgrHosts);
		host = dlist_container(MgrHostWrapper, link, ptr);
		pfreeMgrHostWrapperList(mgrHosts, NULL);
		return host;
	}
}

static MgrNodeWrapper *popHeadMgrNodePfreeOthers(dlist_head *mgrNodes)
{
	dlist_node *ptr;
	MgrNodeWrapper *node = NULL;

	if (dlist_is_empty(mgrNodes))
	{
		return NULL;
	}
	else
	{
		ptr = dlist_pop_head_node(mgrNodes);
		node = dlist_container(MgrNodeWrapper, link, ptr);
		pfreeMgrNodeWrapperList(mgrNodes, NULL);
		return node;
	}
}

/**
 * the list link data type is MgrNodeWrapper
 */
void selectMgrNodes(char *sql,
					MemoryContext spiContext,
					dlist_head *resultList)
{
	int spiRes, rows, i;
	HeapTuple nodeTuple;
	TupleDesc nodeTupdesc;
	MgrNodeWrapper *node;
	Datum datum;
	bool isNull;
	MemoryContext oldCtx;
	SPITupleTable *tupTable;

	oldCtx = MemoryContextSwitchTo(spiContext);
	spiRes = SPI_execute(sql, false, 0);
	MemoryContextSwitchTo(oldCtx);

	if (spiRes != SPI_OK_SELECT)
		ereport(ERROR,
				(errmsg("SPI_execute failed: error code %d",
						spiRes)));

	rows = SPI_processed;
	tupTable = SPI_tuptable;
	if (rows > 0 && tupTable != NULL)
	{
		nodeTupdesc = tupTable->tupdesc;
		for (i = 0; i < rows; i++)
		{
			/* initialize to zero for convenience */
			node = palloc0(sizeof(MgrNodeWrapper));
			dlist_push_tail(resultList, &node->link);

			nodeTuple = tupTable->vals[i];
			node->oid = HeapTupleGetOid(nodeTuple);
			node->form = *((Form_mgr_node)GETSTRUCT(nodeTuple));
			datum = heap_getattr(nodeTuple, Anum_mgr_node_nodepath,
								 nodeTupdesc, &isNull);
			if (!isNull)
				node->nodepath = TextDatumGetCString(datum);
			else
				ereport(ERROR,
						(errmsg("mgr_node column nodepath is null")));
			node->host = selectMgrHostByOid(node->form.nodehost, spiContext);
			if (!node->host)
				ereport(ERROR,
						(errmsg("The host of %s was lost",
								NameStr(node->form.nodename))));
		}
	}
}

MgrNodeWrapper *selectMgrNodeByOid(Oid oid, MemoryContext spiContext)
{
	StringInfoData sql;
	dlist_head nodes = DLIST_STATIC_INIT(nodes);

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * \n"
					 "FROM pg_catalog.mgr_node \n"
					 "WHERE oid = %u \n",
					 oid);
	selectMgrNodes(sql.data, spiContext, &nodes);
	pfree(sql.data);
	return popHeadMgrNodePfreeOthers(&nodes);
}

MgrNodeWrapper *selectMgrNodeByNodenameType(char *nodename,
											char nodetype,
											MemoryContext spiContext)
{
	StringInfoData sql;
	dlist_head nodes = DLIST_STATIC_INIT(nodes);

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * \n"
					 "FROM pg_catalog.mgr_node \n"
					 "WHERE nodename = '%s' \n"
					 "AND nodetype = '%c' \n",
					 nodename,
					 nodetype);
	selectMgrNodes(sql.data, spiContext, &nodes);
	pfree(sql.data);
	return popHeadMgrNodePfreeOthers(&nodes);
}

/**
 * the list link data type is MgrNodeWrapper
 * result include gtm coordinator and ordinary coordinator
 */
void selectMgrAllMasterCoordinators(MemoryContext spiContext,
									dlist_head *resultList)
{
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * \n"
					 "FROM pg_catalog.mgr_node \n"
					 "WHERE nodetype in ('%c', '%c') \n"
					 "AND nodeinited = %d::boolean \n"
					 "AND nodeincluster = %d::boolean \n",
					 CNDN_TYPE_COORDINATOR_MASTER,
					 CNDN_TYPE_GTM_COOR_MASTER,
					 true,
					 true);
	selectMgrNodes(sql.data, spiContext, resultList);
	pfree(sql.data);
}

void selectMgrNodeByNodetype(MemoryContext spiContext,
							 char nodetype,
							 dlist_head *resultList)
{
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * \n"
					 "FROM pg_catalog.mgr_node \n"
					 "WHERE nodetype = '%c' \n"
					 "AND nodeinited = %d::boolean \n"
					 "AND nodeincluster = %d::boolean \n",
					 nodetype,
					 true,
					 true);
	selectMgrNodes(sql.data, spiContext, resultList);
	pfree(sql.data);
}

void selectMgrAllDataNodes(MemoryContext spiContext,
						   dlist_head *resultList)
{
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * \n"
					 "FROM pg_catalog.mgr_node \n"
					 "WHERE nodetype in ('%c','%c') \n"
					 "AND nodeinited = %d::boolean \n"
					 "AND nodeincluster = %d::boolean \n",
					 CNDN_TYPE_DATANODE_MASTER,
					 CNDN_TYPE_DATANODE_SLAVE,
					 true,
					 true);
	selectMgrNodes(sql.data, spiContext, resultList);
	pfree(sql.data);
}

/**
 * the list link data type is MgrNodeWrapper
 */
void selectMgrSlaveNodes(Oid masterOid, char nodetype,
						 MemoryContext spiContext,
						 dlist_head *resultList)
{
	StringInfoData sql;

	initStringInfo(&sql);

	appendStringInfo(&sql,
					 "SELECT * \n"
					 "FROM pg_catalog.mgr_node \n"
					 "WHERE nodetype = '%c' \n"
					 "AND nodeinited = %d::boolean \n"
					 "AND nodemasternameoid = %u \n"
					 "AND nodeincluster = %d::boolean \n",
					 nodetype,
					 true,
					 masterOid,
					 true);
	selectMgrNodes(sql.data, spiContext, resultList);
	pfree(sql.data);
}

void selectMgrNodesForNodeDoctors(MemoryContext spiContext,
								  dlist_head *resultList)
{
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * \n"
					 "FROM pg_catalog.mgr_node \n"
					 "WHERE nodeinited = %d::boolean \n"
					 "AND nodeincluster = %d::boolean \n"
					 "AND allowcure = %d::boolean \n"
					 "AND curestatus in ('%s', '%s', '%s', '%s', '%s') \n",
					 true,
					 true,
					 true,
					 CURE_STATUS_NORMAL,
					 CURE_STATUS_CURING,
					 CURE_STATUS_SWITCHED,
					 CURE_STATUS_FOLLOW_FAIL,
					 CURE_STATUS_OLD_MASTER);
	selectMgrNodes(sql.data, spiContext, resultList);
	pfree(sql.data);
}

MgrNodeWrapper *selectMgrNodeForNodeDoctor(Oid oid, MemoryContext spiContext)
{
	StringInfoData sql;
	dlist_head nodes = DLIST_STATIC_INIT(nodes);

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * \n"
					 "FROM pg_catalog.mgr_node \n"
					 "WHERE nodeinited = %d::boolean \n"
					 "AND nodeincluster = %d::boolean \n"
					 "AND allowcure = %d::boolean \n"
					 "AND curestatus in ('%s', '%s', '%s', '%s', '%s') \n"
					 "AND oid = %u \n",
					 true,
					 true,
					 true,
					 CURE_STATUS_NORMAL,
					 CURE_STATUS_CURING,
					 CURE_STATUS_SWITCHED,
					 CURE_STATUS_FOLLOW_FAIL,
					 CURE_STATUS_OLD_MASTER,
					 oid);
	selectMgrNodes(sql.data, spiContext, &nodes);
	pfree(sql.data);
	return popHeadMgrNodePfreeOthers(&nodes);
}

void selectMgrNodesForSwitcherDoctor(MemoryContext spiContext,
									 dlist_head *resultList)
{
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * \n"
					 "FROM pg_catalog.mgr_node \n"
					 "WHERE nodeinited = %d::boolean \n"
					 "AND nodeincluster = %d::boolean \n"
					 "AND allowcure = %d::boolean \n"
					 "AND curestatus in ('%s', '%s') \n"
					 "AND nodetype in ('%c', '%c') \n",
					 true,
					 true,
					 true,
					 CURE_STATUS_WAIT_SWITCH,
					 CURE_STATUS_SWITCHING,
					 CNDN_TYPE_DATANODE_MASTER,
					 CNDN_TYPE_GTM_COOR_MASTER);
	selectMgrNodes(sql.data, spiContext, resultList);
	pfree(sql.data);
}

void selectMgrNodesForRepairerDoctor(MemoryContext spiContext,
									 dlist_head *resultList)
{
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * \n"
					 "FROM pg_catalog.mgr_node \n"
					 "WHERE nodeinited = %d::boolean \n"
					 "AND nodeincluster = %d::boolean \n"
					 "AND allowcure = %d::boolean \n"
					 "AND curestatus in ('%s') \n"
					 "AND nodetype in ('%c') \n",
					 false,
					 false,
					 true,
					 CURE_STATUS_ISOLATED,
					 CNDN_TYPE_COORDINATOR_MASTER);
	selectMgrNodes(sql.data, spiContext, resultList);
	pfree(sql.data);
}

MgrNodeWrapper *selectMgrGtmCoordNode(MemoryContext spiContext)
{
	dlist_head nodes = DLIST_STATIC_INIT(nodes);
	selectMgrNodeByNodetype(spiContext, CNDN_TYPE_GTM_COOR_MASTER, &nodes);
	return popHeadMgrNodePfreeOthers(&nodes);
}

int updateMgrNodeCurestatus(MgrNodeWrapper *mgrNode,
							char *newCurestatus,
							MemoryContext spiContext)
{
	StringInfoData buf;
	int spiRes;
	uint64 rows;
	MemoryContext oldCtx;

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "update pg_catalog.mgr_node  \n"
					 "set curestatus = '%s' \n"
					 "WHERE oid = %u \n"
					 "and curestatus = '%s' \n"
					 "and nodetype = '%c' \n",
					 newCurestatus,
					 mgrNode->oid,
					 NameStr(mgrNode->form.curestatus),
					 mgrNode->form.nodetype);
	oldCtx = MemoryContextSwitchTo(spiContext);
	spiRes = SPI_execute(buf.data, false, 0);
	MemoryContextSwitchTo(oldCtx);
	pfree(buf.data);
	if (spiRes != SPI_OK_UPDATE)
		ereport(ERROR,
				(errmsg("SPI_execute failed: error code %d",
						spiRes)));
	rows = SPI_processed;
	return rows;
}

int updateMgrNodeAfterFollowMaster(MgrNodeWrapper *mgrNode,
								   char *newCurestatus,
								   MemoryContext spiContext)
{
	StringInfoData buf;
	int spiRes;
	uint64 rows;
	MemoryContext oldCtx;

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "update pg_catalog.mgr_node  \n"
					 "set curestatus = '%s', \n"
					 "nodesync = '%s' \n"
					 "WHERE oid = %u \n"
					 "and curestatus = '%s' \n"
					 "and nodetype = '%c' \n",
					 newCurestatus,
					 NameStr(mgrNode->form.nodesync),
					 mgrNode->oid,
					 NameStr(mgrNode->form.curestatus),
					 mgrNode->form.nodetype);
	oldCtx = MemoryContextSwitchTo(spiContext);
	spiRes = SPI_execute(buf.data, false, 0);
	MemoryContextSwitchTo(oldCtx);
	pfree(buf.data);
	if (spiRes != SPI_OK_UPDATE)
		ereport(ERROR,
				(errmsg("SPI_execute failed: error code %d",
						spiRes)));
	rows = SPI_processed;
	return rows;
}

int updateMgrNodeToIsolate(MgrNodeWrapper *mgrNode,
						   MemoryContext spiContext)
{
	StringInfoData buf;
	int spiRes;
	uint64 rows;
	MemoryContext oldCtx;

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "update pg_catalog.mgr_node  \n"
					 "set curestatus = '%s', \n"
					 "nodeinited = %d::boolean, \n"
					 "nodeincluster = %d::boolean \n"
					 "WHERE oid = %u \n"
					 "and curestatus = '%s' \n"
					 "and nodetype = '%c' \n",
					 CURE_STATUS_ISOLATED,
					 false,
					 false,
					 mgrNode->oid,
					 NameStr(mgrNode->form.curestatus),
					 mgrNode->form.nodetype);
	oldCtx = MemoryContextSwitchTo(spiContext);
	spiRes = SPI_execute(buf.data, false, 0);
	MemoryContextSwitchTo(oldCtx);
	pfree(buf.data);
	if (spiRes != SPI_OK_UPDATE)
		ereport(ERROR,
				(errmsg("SPI_execute failed: error code %d",
						spiRes)));
	rows = SPI_processed;
	return rows;
}

int updateMgrNodeToUnIsolate(MgrNodeWrapper *mgrNode,
							 MemoryContext spiContext)
{
	StringInfoData buf;
	int spiRes;
	uint64 rows;
	MemoryContext oldCtx;

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "update pg_catalog.mgr_node  \n"
					 "set curestatus = '%s', \n"
					 "nodeinited = %d::boolean, \n"
					 "nodeincluster = %d::boolean \n"
					 "WHERE oid = %u \n"
					 "and curestatus = '%s' \n"
					 "and nodetype = '%c' \n",
					 CURE_STATUS_NORMAL,
					 true,
					 true,
					 mgrNode->oid,
					 NameStr(mgrNode->form.curestatus),
					 mgrNode->form.nodetype);
	oldCtx = MemoryContextSwitchTo(spiContext);
	spiRes = SPI_execute(buf.data, false, 0);
	MemoryContextSwitchTo(oldCtx);
	pfree(buf.data);
	if (spiRes != SPI_OK_UPDATE)
		ereport(ERROR,
				(errmsg("SPI_execute failed: error code %d",
						spiRes)));
	rows = SPI_processed;
	return rows;
}

void selectMgrHosts(char *sql,
					MemoryContext spiContext,
					dlist_head *resultList)
{
	int spiRes, rows, i;
	HeapTuple tuple;
	TupleDesc tupdesc;
	MgrHostWrapper *host;
	Datum datum;
	bool isNull;
	MemoryContext oldCtx;
	SPITupleTable *tupTable;

	oldCtx = MemoryContextSwitchTo(spiContext);
	spiRes = SPI_execute(sql, false, 0);
	MemoryContextSwitchTo(oldCtx);

	if (spiRes != SPI_OK_SELECT)
		ereport(ERROR,
				(errmsg("SPI_execute failed: error code %d",
						spiRes)));

	rows = SPI_processed;
	tupTable = SPI_tuptable;
	if (rows > 0 && tupTable != NULL)
	{
		tupdesc = tupTable->tupdesc;
		for (i = 0; i < rows; i++)
		{
			/* initialize to zero for convenience */
			host = palloc0(sizeof(MgrHostWrapper));
			dlist_push_tail(resultList, &host->link);
			tuple = tupTable->vals[i];

			host->oid = HeapTupleGetOid(tuple);
			/* copy struct */
			host->form = *((Form_mgr_host)GETSTRUCT(tuple));

			datum = heap_getattr(tuple, Anum_mgr_host_hostaddr,
								 tupdesc, &isNull);
			if (!isNull)
				host->hostaddr = TextDatumGetCString(datum);
			else
				ereport(ERROR,
						(errmsg("mgr_host column hostaddr is null")));
			datum = heap_getattr(tuple, Anum_mgr_host_hostadbhome,
								 tupdesc, &isNull);
			if (!isNull)
				host->hostadbhome = TextDatumGetCString(datum);
			else
				ereport(ERROR,
						(errmsg("mgr_host column hostadbhome is null")));
		}
	}
}

MgrHostWrapper *selectMgrHostByOid(Oid oid, MemoryContext spiContext)
{
	StringInfoData sql;
	dlist_head resultList = DLIST_STATIC_INIT(resultList);

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * \n"
					 "FROM pg_catalog.mgr_host \n"
					 "WHERE oid = %u",
					 oid);
	selectMgrHosts(sql.data, spiContext, &resultList);
	pfree(sql.data);
	return popHeadMgrHostPfreeOthers(&resultList);
}

void selectMgrHostsForHostDoctor(MemoryContext spiContext,
								 dlist_head *resultList)
{
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * \n"
					 "FROM pg_catalog.mgr_host \n"
					 "WHERE allowcure = %d::boolean \n",
					 true);
	selectMgrHosts(sql.data, spiContext, resultList);
	pfree(sql.data);
}

NodeConnectionStatus connectNodeDefaultDB(MgrNodeWrapper *node,
										  int connectTimeout,
										  PGconn **pgConn)
{
	StringInfoData conninfo;
	PGconn *conn;
	NodeConnectionStatus connStatus;

	initStringInfo(&conninfo);
	appendStringInfo(&conninfo,
					 "postgresql://%s@%s:%d/%s?connect_timeout=%d",
					 NameStr(node->host->form.hostuser),
					 node->host->hostaddr,
					 node->form.nodeport,
					 DEFAULT_DB,
					 connectTimeout);
	conn = PQconnectdb(conninfo.data);
	pfree(conninfo.data);
	if (PQstatus(conn) == CONNECTION_OK)
	{
		ereport(DEBUG1,
				(errmsg("connect node %s successfully",
						NameStr(node->form.nodename))));
		connStatus = NODE_CONNECTION_STATUS_SUCCESS;
	}
	else
	{
		ereport(LOG,
				(errmsg("connect node %s error, %s",
						NameStr(node->form.nodename),
						PQerrorMessage(conn))));
		if (strcmp(conn->last_sqlstate, "57P03") == 0)
		{
			connStatus = NODE_CONNECTION_STATUS_STARTING_UP;
		}
		else if (strcmp(conn->last_sqlstate, "53300") == 0)
		{
			connStatus = NODE_CONNECTION_STATUS_BUSY;
		}
		else
		{
			connStatus = NODE_CONNECTION_STATUS_FAIL;
		}
		PQfinish(conn);
		conn = NULL;
	}
	*pgConn = conn;
	return connStatus;
}

/**
 * If get connection successfully, the PGconn is saved in *pgConnP.
 * If not, will try to set target db's pg_hba.conf trust me, 
 * and then try to get connection again.
 */
PGconn *getNodeDefaultDBConnection(MgrNodeWrapper *mgrNode,
								   int connectTimeout)
{
	PGconn *pgConn = NULL;
	bool gotConn;
	NodeConnectionStatus connStatus;
	NameData myAddress;
	int nTrys;

	connStatus = connectNodeDefaultDB(mgrNode, connectTimeout, &pgConn);
	if (connStatus == NODE_CONNECTION_STATUS_SUCCESS)
	{
		gotConn = true;
	}
	else if (connStatus == NODE_CONNECTION_STATUS_BUSY ||
			 connStatus == NODE_CONNECTION_STATUS_STARTING_UP)
	{
		gotConn = false;
	}
	else
	{
		gotConn = false;
		/* may be is the reason of hba, try to get myself 
		 * addreess and refresh node's pg_hba.conf */
		memset(myAddress.data, 0, NAMEDATALEN);

		if (!mgr_get_self_address(mgrNode->host->hostaddr,
								  mgrNode->form.nodeport,
								  &myAddress))
		{
			ereport(LOG,
					(errmsg("on ADB Manager get local address fail, "
							"this may be caused by cannot get the "
							"connection to node %s",
							NameStr(mgrNode->form.nodename))));
			goto end;
		}
		if (!setPGHbaTrustAddress(mgrNode, NameStr(myAddress)))
		{
			ereport(LOG,
					(errmsg("set node %s trust me failed, "
							"this may be caused by network error",
							NameStr(mgrNode->form.nodename))));
			goto end;
		}
		for (nTrys = 0; nTrys < 10; nTrys++)
		{
			if (pgConn)
				PQfinish(pgConn);
			pgConn = NULL;
			/*sleep 0.1s*/
			pg_usleep(100000L);
			connStatus = connectNodeDefaultDB(mgrNode, connectTimeout, &pgConn);
			if (connStatus == NODE_CONNECTION_STATUS_SUCCESS)
			{
				gotConn = true;
				break;
			}
			else
			{
				gotConn = false;
				continue;
			}
		}
	}
end:
	if (gotConn)
	{
		return pgConn;
	}
	else
	{
		if (pgConn)
			PQfinish(pgConn);
		pgConn = NULL;
		return pgConn;
	}
}

XLogRecPtr getNodeLastWalReceiveLsn(PGconn *pgConn)
{
	XLogRecPtr ptr;
	PGresult *pgResult;
	char *sql;
	char *value;

	sql = "select * from pg_catalog.pg_last_wal_receive_lsn();";
	pgResult = PQexec(pgConn, sql);
	if (PQresultStatus(pgResult) == PGRES_TUPLES_OK)
	{
		value = PQgetvalue(pgResult, 0, 0);
		ptr = parseLsnToXLogRecPtr(value);
	}
	else
	{
		ptr = InvalidXLogRecPtr;
	}
	if (pgResult)
		PQclear(pgResult);
	return ptr;
}

XLogRecPtr getNodeCurrentWalLsn(PGconn *pgConn)
{
	XLogRecPtr ptr;
	PGresult *pgResult;
	char *sql;
	char *value;

	sql = "select * from pg_catalog.pg_current_wal_lsn();";
	pgResult = PQexec(pgConn, sql);
	if (PQresultStatus(pgResult) == PGRES_TUPLES_OK)
	{
		value = PQgetvalue(pgResult, 0, 0);
		ptr = parseLsnToXLogRecPtr(value);
	}
	else
	{
		ptr = InvalidXLogRecPtr;
	}
	if (pgResult)
		PQclear(pgResult);
	return ptr;
}

/**
 * An important health indicator of streaming replication is the amount of 
 * WAL records generated in the primary, but not yet applied in the standby. 
 * You can calculate this lag by comparing the current WAL write location 
 * on the primary with the last WAL location received by the standby. 
 * These locations can be retrieved using pg_current_wal_lsn on the primary 
 * and pg_last_wal_receive_lsn on the standby respectively.
 */
XLogRecPtr getNodeWalLsn(PGconn *pgConn, NodeRunningMode runningMode)
{
	if (runningMode == NODE_RUNNING_MODE_MASTER)
	{
		return getNodeCurrentWalLsn(pgConn);
	}
	else if (runningMode == NODE_RUNNING_MODE_SLAVE)
	{
		return getNodeLastWalReceiveLsn(pgConn);
	}
	else
	{
		return InvalidXLogRecPtr;
	}
}

NodeRunningMode getNodeRunningMode(PGconn *pgConn)
{
	NodeRunningMode res;
	PGresult *pgResult;
	char *value;
	char *sql;

	sql = "select * from pg_catalog.pg_is_in_recovery();";
	pgResult = PQexec(pgConn, sql);
	if (PQresultStatus(pgResult) == PGRES_TUPLES_OK)
	{
		value = PQgetvalue(pgResult, 0, 0);
		if (pg_strcasecmp(value, "t") == 0)
		{
			res = NODE_RUNNING_MODE_SLAVE;
		}
		else if (pg_strcasecmp(value, "f") == 0)
		{
			res = NODE_RUNNING_MODE_MASTER;
		}
		else
		{
			res = NODE_RUNNING_MODE_UNKNOW;
		}
	}
	else
	{
		ereport(LOG,
				(errmsg("execute %s failed:%s",
						sql,
						PQerrorMessage(pgConn))));
		res = NODE_RUNNING_MODE_UNKNOW;
	}
	if (pgResult)
		PQclear(pgResult);
	return res;
}

NodeRunningMode getExpectedNodeRunningMode(bool isMaster)
{
	if (isMaster)
	{
		return NODE_RUNNING_MODE_MASTER;
	}
	else
	{
		return NODE_RUNNING_MODE_SLAVE;
	}
}

bool checkNodeRunningMode(PGconn *pgConn, bool isMaster)
{
	NodeRunningMode expectedMode;

	expectedMode = getExpectedNodeRunningMode(isMaster);
	return getNodeRunningMode(pgConn) == expectedMode;
}

/*
 * Pfree the returned result when no longer needed
 */
char *showNodeParameter(PGconn *pgConn, char *name, bool complain)
{
	PGresult *pgResult;
	char *value;
	char *sql;

	sql = psprintf("show %s;", name);
	pgResult = PQexec(pgConn, sql);
	if (PQresultStatus(pgResult) == PGRES_TUPLES_OK)
	{
		value = psprintf("%s", PQgetvalue(pgResult, 0, 0));
	}
	else
	{
		ereport(complain ? ERROR : LOG,
				(errmsg("execute %s failed:%s",
						sql,
						PQerrorMessage(pgConn))));
		value = palloc0(1);
	}
	pfree(sql);
	if (pgResult)
		PQclear(pgResult);
	return value;
}

bool equalsNodeParameter(PGconn *pgConn, char *name, char *expectValue)
{
	bool equal;
	char *actualValue;
	actualValue = showNodeParameter(pgConn, name, true);
	equal = strcmp(actualValue, expectValue) == 0;
	pfree(actualValue);
	return equal;
}

bool PQexecCommandSql(PGconn *pgConn, char *sql, bool complain)
{
	PGresult *pgResult;
	bool execOk;

	pgResult = PQexec(pgConn, sql);
	if (PQresultStatus(pgResult) == PGRES_COMMAND_OK)
	{
		execOk = true;
		ereport(DEBUG1,
				(errmsg("execute %s successfully",
						sql)));
	}
	else
	{
		execOk = false;
		ereport(complain ? ERROR : LOG,
				(errmsg("execute %s failed:%s",
						sql,
						PQerrorMessage(pgConn))));
	}
	if (pgResult)
		PQclear(pgResult);
	return execOk;
}

int PQexecCountSql(PGconn *pgConn, char *sql, bool complain)
{
	PGresult *pgResult;
	int count = -1;

	pgResult = PQexec(pgConn, sql);
	if (PQresultStatus(pgResult) == PGRES_TUPLES_OK)
	{
		if (0 == PQntuples(pgResult))
		{
			ereport(complain ? ERROR : LOG,
					(errmsg("execute %s failed:%s",
							sql,
							PQerrorMessage(pgConn))));
		}
		else
		{
			count = atoi(PQgetvalue(pgResult, 0, 0));
			ereport(DEBUG1,
					(errmsg("execute %s successfully",
							sql)));
		}
	}
	else
	{
		ereport(complain ? ERROR : LOG,
				(errmsg("execute %s failed:%s",
						sql,
						PQerrorMessage(pgConn))));
	}
	if (pgResult)
		PQclear(pgResult);
	return count;
}

bool PQexecBoolQuery(PGconn *pgConn, char *sql,
					 bool expectedValue, bool complain)
{
	PGresult *pgResult;
	char *value;
	bool boolResult;

	pgResult = PQexec(pgConn, sql);
	if (PQresultStatus(pgResult) == PGRES_TUPLES_OK)
	{
		ereport(DEBUG1,
				(errmsg("execute %s successfully",
						sql)));
		value = PQgetvalue(pgResult, 0, 0);
		if (pg_strcasecmp(value, "t") == 0)
		{
			boolResult = expectedValue == true;
		}
		else if (pg_strcasecmp(value, "f") == 0)
		{
			boolResult = expectedValue == false;
		}
		else
		{
			ereport(complain ? ERROR : LOG,
					(errmsg("execute %s failed, Illegal result value:%s",
							sql,
							value)));
			boolResult = false;
		}
	}
	else
	{
		ereport(complain ? ERROR : LOG,
				(errmsg("execute %s failed:%s",
						sql,
						PQerrorMessage(pgConn))));
		boolResult = false;
	}
	if (pgResult)
	{
		PQclear(pgResult);
		pgResult = NULL;
	}
	return boolResult;
}

bool exec_pgxc_pool_reload(PGconn *pgConn, bool complain)
{
	char *sql = "set FORCE_PARALLEL_MODE = off; select pgxc_pool_reload();";
	return PQexecBoolQuery(pgConn, sql, true, complain);
}

bool exec_pg_pause_cluster(PGconn *pgConn, bool complain)
{
	char *sql = "set FORCE_PARALLEL_MODE = off; SELECT PG_PAUSE_CLUSTER();";
	return PQexecBoolQuery(pgConn, sql, true, complain);
}

bool exec_pg_unpause_cluster(PGconn *pgConn, bool complain)
{
	char *sql = "set FORCE_PARALLEL_MODE = off; SELECT PG_UNPAUSE_CLUSTER();";
	return PQexecBoolQuery(pgConn, sql, true, complain);
}

bool exec_pool_close_idle_conn(PGconn *pgConn, bool complain)
{
	char *sql = "set FORCE_PARALLEL_MODE = off; select pool_close_idle_conn();";
	return PQexecBoolQuery(pgConn, sql, true, complain);
}

/* 
 * Pointer is disgusting, just return a small struct,
 * don't forget to pfree the member 'message' that in this struct.
 */
CallAgentResult callAgentSendCmd(AgentCommand cmd,
								 StringInfo cmdMessage,
								 char *hostaddr,
								 int32 hostagentport)
{
	CallAgentResult res;
	ManagerAgent *ma;
	StringInfoData sendbuf;
	StringInfoData recvbuf;
	char msg_type;

	initStringInfo(&res.message);
	res.agentRes = false;

	ma = ma_connect(hostaddr, hostagentport);
	if (!ma_isconnected(ma))
	{
		appendStringInfo(&res.message,
						 "could not connect socket for agent %s, %s",
						 hostaddr,
						 ma_last_error_msg(ma));
		goto end;
	}

	ma_beginmessage(&sendbuf, AGT_MSG_COMMAND);
	ma_sendbyte(&sendbuf, cmd);
	mgr_append_infostr_infostr(&sendbuf, cmdMessage);
	ma_endmessage(&sendbuf, ma);

	if (!ma_flush(ma, false))
	{
		appendStringInfo(&res.message,
						 "could not flush socket for agent %s, %s",
						 hostaddr,
						 ma_last_error_msg(ma));
		goto end;
	}
	initStringInfo(&recvbuf);
	for (;;)
	{
		msg_type = ma_get_message(ma, &recvbuf);
		if (msg_type == AGT_MSG_IDLE)
		{
			break;
		}
		else if (msg_type == '\0')
		{
			/* has an error */
			break;
		}
		else if (msg_type == AGT_MSG_ERROR)
		{
			/* error message */
			appendStringInfoString(&res.message,
								   ma_get_err_info(&recvbuf,
												   AGT_MSG_RESULT));
			break;
		}
		else if (msg_type == AGT_MSG_NOTICE)
		{
			/* ignore notice message */
			ereport(DEBUG1, (errmsg("receive msg: %s",
									ma_get_err_info(&recvbuf,
													AGT_MSG_RESULT))));
		}
		else if (msg_type == AGT_MSG_RESULT)
		{
			res.agentRes = true;
			appendBinaryStringInfo(&res.message, recvbuf.data, recvbuf.len);
			break;
		}
	}
	pfree(recvbuf.data);

end:
	ma_close(ma);
	if (res.agentRes)
	{
		ereport(DEBUG1,
				(errmsg("agent %s execute cmd %d %s successfully",
						hostaddr,
						cmd,
						cmdMessage->data)));
	}
	else
	{
		ereport(LOG,
				(errmsg("agent %s execute cmd %d %s failed, %s",
						hostaddr,
						cmd,
						cmdMessage->data,
						res.message.data)));
	}
	return res;
}

void PGHbaItemsToCmdMessage(char *nodepath,
							PGHbaItem *items,
							StringInfo cmdMessage)
{
	PGHbaItem *item;

	appendStringInfoString(cmdMessage, nodepath);
	appendStringInfoCharMacro(cmdMessage, '\0');
	for (item = items; (item); item = item->next)
	{
		appendStringInfo(cmdMessage, "%c", item->type);
		appendStringInfoCharMacro(cmdMessage, '\0');
		appendStringInfoString(cmdMessage, item->database);
		appendStringInfoCharMacro(cmdMessage, '\0');
		appendStringInfoString(cmdMessage, item->user);
		appendStringInfoCharMacro(cmdMessage, '\0');
		appendStringInfoString(cmdMessage, item->address);
		appendStringInfoCharMacro(cmdMessage, '\0');
		appendStringInfo(cmdMessage, "%d", item->netmask);
		appendStringInfoCharMacro(cmdMessage, '\0');
		appendStringInfoString(cmdMessage, item->method);
		appendStringInfoCharMacro(cmdMessage, '\0');
	}
}

void PGConfParameterItemsToCmdMessage(char *nodepath,
									  PGConfParameterItem *items,
									  StringInfo cmdMessage)
{
	PGConfParameterItem *item;

	appendStringInfoString(cmdMessage, nodepath);
	appendStringInfoCharMacro(cmdMessage, '\0');
	for (item = items; (item); item = item->next)
	{
		appendStringInfoString(cmdMessage, item->name);
		appendStringInfoCharMacro(cmdMessage, '\0');
		if (item->quoteValue)
		{
			appendStringInfo(cmdMessage, "'%s'", item->value);
		}
		else
		{
			appendStringInfo(cmdMessage, "%s", item->value);
		}
		appendStringInfoCharMacro(cmdMessage, '\0');
	}
}

bool callAgentDeletePGHbaConf(MgrNodeWrapper *node,
							  PGHbaItem *items,
							  bool complain)
{
	CallAgentResult res;
	StringInfoData cmdMessage;

	initStringInfo(&cmdMessage);
	PGHbaItemsToCmdMessage(node->nodepath, items, &cmdMessage);

	res = callAgentSendCmd(AGT_CMD_CNDN_DELETE_PGHBACONF,
						   &cmdMessage,
						   node->host->hostaddr,
						   node->host->form.hostagentport);
	pfree(cmdMessage.data);
	if (res.agentRes)
	{
		ereport(DEBUG1,
				(errmsg("delete %s pg_hba.conf %s successfully",
						NameStr(node->form.nodename),
						toStringPGHbaItem(items))));
	}
	else
	{
		ereport(complain ? ERROR : LOG,
				(errmsg("delete %s pg_hba.conf %s failed:%s",
						NameStr(node->form.nodename),
						toStringPGHbaItem(items),
						res.message.data)));
	}
	pfree(res.message.data);
	return res.agentRes;
}

bool callAgentRefreshPGHbaConf(MgrNodeWrapper *node,
							   PGHbaItem *items,
							   bool complain)
{
	StringInfoData cmdMessage;
	CallAgentResult res;

	initStringInfo(&cmdMessage);
	PGHbaItemsToCmdMessage(node->nodepath, items, &cmdMessage);

	res = callAgentSendCmd(AGT_CMD_CNDN_REFRESH_PGHBACONF,
						   &cmdMessage,
						   node->host->hostaddr,
						   node->host->form.hostagentport);
	pfree(cmdMessage.data);
	if (res.agentRes)
	{
		ereport(DEBUG1,
				(errmsg("refresh %s pg_hba.conf %s successfully",
						NameStr(node->form.nodename),
						toStringPGHbaItem(items))));
	}
	else
	{
		ereport(complain ? ERROR : LOG,
				(errmsg("refresh %s pg_hba.conf %s failed:%s",
						NameStr(node->form.nodename),
						toStringPGHbaItem(items),
						res.message.data)));
	}
	pfree(res.message.data);
	return res.agentRes;
}

bool callAgentReloadNode(MgrNodeWrapper *node, bool complain)
{
	StringInfoData cmdMessage;
	CallAgentResult res;

	initStringInfo(&cmdMessage);
	/* pg_ctl reload -D pathdir */
	appendStringInfo(&cmdMessage, " reload -D %s", node->nodepath);

	res = callAgentSendCmd(AGT_CMD_NODE_RELOAD,
						   &cmdMessage,
						   node->host->hostaddr,
						   node->host->form.hostagentport);
	pfree(cmdMessage.data);
	if (res.agentRes)
	{
		ereport(DEBUG1,
				(errmsg("reload %s %s successfully",
						NameStr(node->form.nodename),
						node->nodepath)));
	}
	else
	{
		ereport(complain ? ERROR : LOG,
				(errmsg("reload %s %s failed:%s",
						NameStr(node->form.nodename),
						node->nodepath,
						res.message.data)));
	}
	pfree(res.message.data);
	return res.agentRes;
}

bool callAgentRefreshPGSqlConfReload(MgrNodeWrapper *node,
									 PGConfParameterItem *items,
									 bool complain)
{
	StringInfoData cmdMessage;
	CallAgentResult res;

	initStringInfo(&cmdMessage);

	PGConfParameterItemsToCmdMessage(node->nodepath, items, &cmdMessage);
	res = callAgentSendCmd(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD,
						   &cmdMessage,
						   node->host->hostaddr,
						   node->host->form.hostagentport);
	pfree(cmdMessage.data);
	if (res.agentRes)
	{
		ereport(DEBUG1,
				(errmsg("refresh %s postgresql.conf %s successfully",
						NameStr(node->form.nodename),
						toStringPGConfParameterItem(items))));
	}
	else
	{
		ereport(complain ? ERROR : LOG,
				(errmsg("refresh %s postgresql.conf %s failed:%s",
						NameStr(node->form.nodename),
						toStringPGConfParameterItem(items),
						res.message.data)));
	}
	pfree(res.message.data);
	return res.agentRes;
}

bool callAgentRefreshPGSqlConf(MgrNodeWrapper *node,
							   PGConfParameterItem *items,
							   bool complain)
{
	StringInfoData cmdMessage;
	CallAgentResult res;

	initStringInfo(&cmdMessage);

	PGConfParameterItemsToCmdMessage(node->nodepath, items, &cmdMessage);
	res = callAgentSendCmd(AGT_CMD_CNDN_REFRESH_PGSQLCONF,
						   &cmdMessage,
						   node->host->hostaddr,
						   node->host->form.hostagentport);
	pfree(cmdMessage.data);
	if (res.agentRes)
	{
		ereport(DEBUG1,
				(errmsg("refresh %s postgresql.conf %s successfully",
						NameStr(node->form.nodename),
						toStringPGConfParameterItem(items))));
	}
	else
	{
		ereport(complain ? ERROR : LOG,
				(errmsg("refresh %s postgresql.conf %s failed:%s",
						NameStr(node->form.nodename),
						toStringPGConfParameterItem(items),
						res.message.data)));
	}
	pfree(res.message.data);
	return res.agentRes;
}

bool callAgentRefreshRecoveryConf(MgrNodeWrapper *node,
								  PGConfParameterItem *items,
								  bool complain)
{
	StringInfoData cmdMessage;
	CallAgentResult res;

	initStringInfo(&cmdMessage);

	PGConfParameterItemsToCmdMessage(node->nodepath, items, &cmdMessage);
	res = callAgentSendCmd(AGT_CMD_CNDN_REFRESH_RECOVERCONF,
						   &cmdMessage,
						   node->host->hostaddr,
						   node->host->form.hostagentport);
	pfree(cmdMessage.data);
	if (res.agentRes)
	{
		ereport(DEBUG1,
				(errmsg("refresh %s %s recovery.conf successfully",
						NameStr(node->form.nodename),
						node->nodepath)));
	}
	else
	{
		ereport(complain ? ERROR : LOG,
				(errmsg("refresh %s %s recovery.conf failed:%s",
						NameStr(node->form.nodename),
						node->nodepath,
						res.message.data)));
	}
	pfree(res.message.data);
	return res.agentRes;
}

bool callAgentPromoteNode(MgrNodeWrapper *node, bool complain)
{
	StringInfoData cmdMessage;
	CallAgentResult res;
	AgentCommand cmd;

	if (node->form.nodetype == CNDN_TYPE_DATANODE_MASTER ||
		node->form.nodetype == CNDN_TYPE_DATANODE_SLAVE)
	{
		cmd = AGT_CMD_DN_FAILOVER;
	}
	else if (node->form.nodetype == CNDN_TYPE_GTM_COOR_MASTER ||
			 node->form.nodetype == CNDN_TYPE_GTM_COOR_SLAVE)
	{
		cmd = AGT_CMD_GTMCOOR_SLAVE_FAILOVER;
	}
	else
	{
		ereport(complain ? ERROR : LOG,
				(errmsg("unexpect nodetype \"%c\"",
						node->form.nodetype)));
		return false;
	}

	initStringInfo(&cmdMessage);

	appendStringInfo(&cmdMessage, " promote -w -D %s", node->nodepath);
	appendStringInfoCharMacro(&cmdMessage, '\0');

	res = callAgentSendCmd(cmd,
						   &cmdMessage,
						   node->host->hostaddr,
						   node->host->form.hostagentport);
	pfree(cmdMessage.data);
	if (res.agentRes)
	{
		ereport(DEBUG1,
				(errmsg("promote %s %s successfully",
						NameStr(node->form.nodename),
						node->nodepath)));
	}
	else
	{
		ereport(complain ? ERROR : LOG,
				(errmsg("promote %s %s failed:%s",
						NameStr(node->form.nodename),
						node->nodepath,
						res.message.data)));
	}
	pfree(res.message.data);
	return res.agentRes;
}

PingNodeResult callAgentPingNode(MgrNodeWrapper *node)
{
	CallAgentResult res;
	StringInfoData cmdMessage;
	char pid_file_path[MAXPGPATH] = {0};
	int ping_status;
	PingNodeResult pingRes;

	snprintf(pid_file_path, MAXPGPATH, "%s/postmaster.pid", node->nodepath);

	initStringInfo(&cmdMessage);
	appendStringInfo(&cmdMessage, "%s", node->host->hostaddr);
	appendStringInfoChar(&cmdMessage, '\0');
	appendStringInfo(&cmdMessage, "%d", node->form.nodeport);
	appendStringInfoChar(&cmdMessage, '\0');
	appendStringInfo(&cmdMessage, "%s", NameStr(node->host->form.hostuser));
	appendStringInfoChar(&cmdMessage, '\0');
	appendStringInfo(&cmdMessage, "%s", pid_file_path);

	res = callAgentSendCmd(AGT_CMD_PING_NODE,
						   &cmdMessage,
						   node->host->hostaddr,
						   node->host->form.hostagentport);
	pfree(cmdMessage.data);
	if (res.agentRes)
	{
		if (res.message.len == 1)
		{
			pingRes.agentRes = true;
			ping_status = res.message.data[0];
			switch (ping_status)
			{
			case PQPING_OK:
			case PQPING_REJECT:
			case PQPING_NO_RESPONSE:
			case PQPING_NO_ATTEMPT:
				pingRes.pgPing = (PGPing)ping_status;
				break;
			default:
				pingRes.pgPing = PQPING_NO_RESPONSE;
			}
		}
		else
		{
			ereport(LOG,
					(errmsg("call agent %s error:%s.",
							node->host->hostaddr,
							res.message.data)));
			pingRes.agentRes = false;
		}
	}
	else
	{
		ereport(LOG,
				(errmsg("call agent %s error:%s.",
						node->host->hostaddr,
						res.message.data)));
		pingRes.agentRes = false;
	}
	pfree(res.message.data);
	return pingRes;
}

bool callAgentStopNode(MgrNodeWrapper *node, char *shutdownMode,
					   bool wait, bool complain)
{
	CallAgentResult res;
	AgentCommand cmd;
	StringInfoData cmdMessage;

	initStringInfo(&cmdMessage);
	switch (node->form.nodetype)
	{
	case CNDN_TYPE_COORDINATOR_MASTER:
	case CNDN_TYPE_COORDINATOR_SLAVE:
		appendStringInfo(&cmdMessage,
						 " stop -D %s -Z coordinator -m %s -o -i -c",
						 node->nodepath,
						 shutdownMode);

		break;
	case CNDN_TYPE_DATANODE_MASTER:
	case CNDN_TYPE_DATANODE_SLAVE:
		appendStringInfo(&cmdMessage,
						 " stop -D %s -Z datanode -m %s -o -i -c",
						 node->nodepath,
						 shutdownMode);
		break;
	case CNDN_TYPE_GTM_COOR_MASTER:
	case CNDN_TYPE_GTM_COOR_SLAVE:
		appendStringInfo(&cmdMessage,
						 " stop -D %s -Z gtm_coord -m %s -o -i -c",
						 node->nodepath,
						 shutdownMode);
		break;
	default:
		pfree(cmdMessage.data);
		ereport(complain ? ERROR : LOG,
				(errmsg("unexpect nodetype \"%c\"",
						node->form.nodetype)));
		return false;
	}

	wait ? appendStringInfo(&cmdMessage, " -w")
		 : appendStringInfo(&cmdMessage, " -W");

	if (node->form.nodetype == CNDN_TYPE_GTM_COOR_MASTER)
		cmd = AGT_CMD_GTMCOOR_STOP_MASTER;
	else if (node->form.nodetype == CNDN_TYPE_GTM_COOR_SLAVE)
		cmd = AGT_CMD_GTMCOOR_STOP_SLAVE;
	else if (node->form.nodetype == CNDN_TYPE_COORDINATOR_MASTER ||
			 node->form.nodetype == CNDN_TYPE_COORDINATOR_SLAVE)
		cmd = AGT_CMD_CN_STOP;
	else
		cmd = AGT_CMD_DN_STOP;

	res = callAgentSendCmd(cmd, &cmdMessage,
						   node->host->hostaddr,
						   node->host->form.hostagentport);
	pfree(cmdMessage.data);
	if (res.agentRes)
	{
		ereport(DEBUG1,
				(errmsg("stop %s %s successfully",
						NameStr(node->form.nodename),
						node->nodepath)));
	}
	else
	{
		ereport(complain ? ERROR : LOG,
				(errmsg("stop %s %s failed:%s",
						NameStr(node->form.nodename),
						node->nodepath,
						res.message.data)));
	}
	pfree(res.message.data);
	return res.agentRes;
}

bool callAgentStartNode(MgrNodeWrapper *node, bool wait, bool complain)
{
	CallAgentResult res;
	AgentCommand cmd;
	StringInfoData cmdMessage;

	initStringInfo(&cmdMessage);

	switch (node->form.nodetype)
	{
	case CNDN_TYPE_COORDINATOR_MASTER:
	case CNDN_TYPE_COORDINATOR_SLAVE:
		appendStringInfo(&cmdMessage,
						 " start -D %s -Z coordinator -o -i -c -l %s/logfile",
						 node->nodepath,
						 node->nodepath);
		break;
	case CNDN_TYPE_DATANODE_MASTER:
	case CNDN_TYPE_DATANODE_SLAVE:
		appendStringInfo(&cmdMessage,
						 " start -D %s -Z datanode -o -i -c -l %s/logfile",
						 node->nodepath,
						 node->nodepath);
		break;
	case CNDN_TYPE_GTM_COOR_MASTER:
	case CNDN_TYPE_GTM_COOR_SLAVE:
		appendStringInfo(&cmdMessage,
						 " start -D %s -Z gtm_coord -o -i -c -l %s/logfile",
						 node->nodepath,
						 node->nodepath);
		break;
	default:
		pfree(cmdMessage.data);
		ereport(complain ? ERROR : LOG,
				(errmsg("unexpect nodetype \"%c\"",
						node->form.nodetype)));
		return false;
	}

	wait ? appendStringInfo(&cmdMessage, " -w")
		 : appendStringInfo(&cmdMessage, " -W");

	if (node->form.nodetype == CNDN_TYPE_GTM_COOR_MASTER)
		cmd = AGT_CMD_GTMCOOR_START_MASTER;
	if (node->form.nodetype == CNDN_TYPE_GTM_COOR_SLAVE)
		cmd = AGT_CMD_GTMCOOR_START_SLAVE; /* agtm_ctl */
	else if (node->form.nodetype == CNDN_TYPE_COORDINATOR_MASTER ||
			 node->form.nodetype == CNDN_TYPE_COORDINATOR_SLAVE)
		cmd = AGT_CMD_CN_START; /* pg_ctl  */
	else
		cmd = AGT_CMD_DN_START; /* pg_ctl  */

	res = callAgentSendCmd(cmd, &cmdMessage,
						   node->host->hostaddr,
						   node->host->form.hostagentport);
	pfree(cmdMessage.data);
	if (res.agentRes)
	{
		ereport(DEBUG1,
				(errmsg("start %s %s successfully",
						NameStr(node->form.nodename),
						node->nodepath)));
	}
	else
	{
		ereport(complain ? ERROR : LOG,
				(errmsg("start %s %s failed:%s",
						NameStr(node->form.nodename),
						node->nodepath,
						res.message.data)));
	}
	pfree(res.message.data);
	return res.agentRes;
}

bool callAgentRestartNode(MgrNodeWrapper *node,
						  char *shutdownMode,
						  bool complain)
{
	CallAgentResult res;
	AgentCommand cmd;
	StringInfoData cmdMessage;

	initStringInfo(&cmdMessage);
	switch (node->form.nodetype)
	{
	case CNDN_TYPE_COORDINATOR_MASTER:
	case CNDN_TYPE_COORDINATOR_SLAVE:
		cmd = AGT_CMD_CN_RESTART;
		appendStringInfo(&cmdMessage,
						 " restart -D %s -Z coordinator -m %s -o -i -w -c -l %s/logfile",
						 node->nodepath,
						 shutdownMode,
						 node->nodepath);
		break;
	case CNDN_TYPE_DATANODE_MASTER:
	case CNDN_TYPE_DATANODE_SLAVE:
		cmd = AGT_CMD_DN_RESTART;
		appendStringInfo(&cmdMessage,
						 " restart -D %s -Z datanode -m %s -o -i -w -c -l %s/logfile",
						 node->nodepath,
						 shutdownMode,
						 node->nodepath);
		break;
	case CNDN_TYPE_GTM_COOR_MASTER:
	case CNDN_TYPE_GTM_COOR_SLAVE:
		cmd = AGT_CMD_AGTM_RESTART;
		appendStringInfo(&cmdMessage,
						 " restart -D %s -Z gtm_coord -m %s -o -i -w -c -l %s/logfile",
						 node->nodepath,
						 shutdownMode,
						 node->nodepath);
		break;
	default:
		pfree(cmdMessage.data);
		ereport(complain ? ERROR : LOG,
				(errmsg("unexpect nodetype \"%c\"",
						node->form.nodetype)));
		return false;
	}

	res = callAgentSendCmd(cmd, &cmdMessage,
						   node->host->hostaddr,
						   node->host->form.hostagentport);
	pfree(cmdMessage.data);
	if (res.agentRes)
	{
		ereport(DEBUG1,
				(errmsg("restart %s %s successfully",
						NameStr(node->form.nodename),
						node->nodepath)));
	}
	else
	{
		ereport(complain ? ERROR : LOG,
				(errmsg("restart %s %s failed:%s",
						NameStr(node->form.nodename),
						node->nodepath,
						res.message.data)));
	}
	pfree(res.message.data);
	return res.agentRes;
}

bool callAgentRewindNode(MgrNodeWrapper *masterNode,
						 MgrNodeWrapper *slaveNode, bool complain)
{
	CallAgentResult res;
	StringInfoData cmdMessage;
	AgentCommand cmd;
	// bool bGtmType;

	// if (CNDN_TYPE_GTM_COOR_SLAVE == slaveNode->form.nodetype)
	// {
	// 	bGtmType = true;
	// 	cmd = AGT_CMD_AGTM_REWIND;
	// }
	// else
	// {
	// 	bGtmType = false;
	// 	cmd = AGT_CMD_NODE_REWIND;
	// }
	initStringInfo(&cmdMessage);
	appendStringInfo(&cmdMessage,
					 " --target-pgdata %s --source-server='host=%s port=%d user=%s dbname=postgres'",
					 slaveNode->nodepath,
					 masterNode->host->hostaddr,
					 masterNode->form.nodeport,
					 NameStr(slaveNode->host->form.hostuser));
	// if (!bGtmType)
	// {
	appendStringInfo(&cmdMessage, " --target-nodename %s --source-nodename %s",
					 NameStr(slaveNode->form.nodename),
					 NameStr(masterNode->form.nodename));
	// }
	cmd = AGT_CMD_NODE_REWIND;
	res = callAgentSendCmd(cmd, &cmdMessage, slaveNode->host->hostaddr,
						   slaveNode->host->form.hostagentport);
	pfree(cmdMessage.data);
	if (res.agentRes)
	{
		ereport(DEBUG1,
				(errmsg("rewind %s from %s successfully",
						NameStr(slaveNode->form.nodename),
						NameStr(masterNode->form.nodename))));
	}
	else
	{
		ereport(complain ? ERROR : LOG,
				(errmsg("rewind %s from %s failed:%s",
						NameStr(slaveNode->form.nodename),
						NameStr(masterNode->form.nodename),
						res.message.data)));
	}
	pfree(res.message.data);
	return res.agentRes;
}

void getCallAgentSqlString(MgrNodeWrapper *node,
						   char *sql,
						   StringInfo cmdMessage)
{
	/*user*/
	appendStringInfoString(cmdMessage, NameStr(node->host->form.hostuser));
	appendStringInfoCharMacro(cmdMessage, '\0');
	/*port*/
	appendStringInfo(cmdMessage, "%d", node->form.nodeport);
	appendStringInfoCharMacro(cmdMessage, '\0');
	/*dbname*/
	appendStringInfoString(cmdMessage, DEFAULT_DB);
	appendStringInfoCharMacro(cmdMessage, '\0');
	/*sqlstring*/
	appendStringInfoString(cmdMessage, sql);
	appendStringInfoCharMacro(cmdMessage, '\0');
}

CallAgentResult callAgentExecuteSql(MgrNodeWrapper *node,
									char *sql)
{
	CallAgentResult res;
	StringInfoData cmdMessage;

	initStringInfo(&cmdMessage);
	getCallAgentSqlString(node, sql, &cmdMessage);
	res = callAgentSendCmd(AGT_CMD_GET_SQL_STRINGVALUES,
						   &cmdMessage,
						   node->host->hostaddr,
						   node->host->form.hostagentport);
	pfree(cmdMessage.data);
	return res;
}

CallAgentResult callAgentExecuteSqlCommand(MgrNodeWrapper *node, char *sql)
{
	CallAgentResult res;
	StringInfoData cmdMessage;

	initStringInfo(&cmdMessage);
	getCallAgentSqlString(node, sql, &cmdMessage);
	res = callAgentSendCmd(AGT_CMD_GET_SQL_STRINGVALUES_COMMAND,
						   &cmdMessage,
						   node->host->hostaddr,
						   node->host->form.hostagentport);
	pfree(cmdMessage.data);
	return res;
}

NodeRecoveryStatus callAgentGet_pg_is_in_recovery(MgrNodeWrapper *node)
{
	CallAgentResult res;
	NodeRecoveryStatus recoveryStatus;
	char *sql;

	sql = "select * from pg_catalog.pg_is_in_recovery();";
	res = callAgentExecuteSql(node,
							  sql);
	recoveryStatus.queryOk = res.agentRes;
	if (res.agentRes)
	{
		ereport(DEBUG1,
				(errmsg("%s pg_is_in_recovery, %s",
						NameStr(node->form.nodename),
						res.message.data)));
		if (pg_strcasecmp(res.message.data, "t") == 0)
		{
			recoveryStatus.isInRecovery = true;
		}
		else if (pg_strcasecmp(res.message.data, "f"))
		{
			recoveryStatus.isInRecovery = false;
		}
		else
		{
			recoveryStatus.queryOk = false;
		}
	}
	if (!recoveryStatus.queryOk)
	{
		ereport(LOG,
				(errmsg("call agent %s error, %s",
						node->host->hostaddr,
						res.message.data)));
	}
	pfree(res.message.data);
	return recoveryStatus;
}

XLogRecPtr parseLsnToXLogRecPtr(const char *str)
{
	XLogRecPtr ptr = InvalidXLogRecPtr;
	uint32 high, low;
	if (str != NULL && strlen(str) > 0)
	{
		if (sscanf(str, "%x/%x", &high, &low) == 2)
			ptr = (((XLogRecPtr)high) << 32) + (XLogRecPtr)low;
	}
	return ptr;
}

XLogRecPtr callAgentGet_pg_last_wal_receive_lsn(MgrNodeWrapper *node)
{
	XLogRecPtr ptr;
	CallAgentResult res;
	char *sql;

	sql = "SELECT * from pg_catalog.pg_last_wal_receive_lsn()";
	res = callAgentExecuteSql(node,
							  sql);
	if (res.agentRes)
	{
		if (res.message.len != 0)
		{
			ptr = parseLsnToXLogRecPtr(res.message.data);
		}
		else
		{
			ptr = InvalidXLogRecPtr;
		}
	}
	else
	{
		ptr = InvalidXLogRecPtr;
	}
	pfree(res.message.data);
	return ptr;
}

bool setPGHbaTrustAddress(MgrNodeWrapper *mgrNode, char *address)
{
	PGHbaItem *hbaItems;
	bool execOk;

	hbaItems = newPGHbaItem(CONNECT_HOST, DEFAULT_DB,
							NameStr(mgrNode->host->form.hostuser),
							address, 32, "trust");
	execOk = callAgentRefreshPGHbaConf(mgrNode, hbaItems, false);
	pfreePGHbaItem(hbaItems);
	if (!execOk)
		return execOk;
	execOk = callAgentReloadNode(mgrNode, false);
	return execOk;
}

void setPGHbaTrustSlaveReplication(MgrNodeWrapper *masterNode,
								   MgrNodeWrapper *slaveNode)
{
	PGHbaItem *hbaItems;

	hbaItems = newPGHbaItem(CONNECT_HOST, "replication",
							NameStr(slaveNode->host->form.hostuser),
							slaveNode->host->hostaddr,
							32, "trust");
	callAgentRefreshPGHbaConf(masterNode, hbaItems, true);
	callAgentReloadNode(masterNode, true);
	pfreePGHbaItem(hbaItems);
}

void setSynchronousStandbyNames(MgrNodeWrapper *mgrNode, char *value)
{
	PGConfParameterItem *syncStandbys;

	syncStandbys = newPGConfParameterItem("synchronous_standby_names",
										  value, true);
	callAgentRefreshPGSqlConfReload(mgrNode,
									syncStandbys,
									true);
	pfreePGConfParameterItem(syncStandbys);
}

void setCheckSynchronousStandbyNames(MgrNodeWrapper *mgrNode,
									 PGconn *pgConn,
									 char *value,
									 int checkSeconds)
{
	int seconds;
	bool execOk = false;

	setSynchronousStandbyNames(mgrNode, value);

	for (seconds = 0; seconds < checkSeconds; seconds++)
	{
		/* check the param */
		if (equalsNodeParameter(pgConn,
								"synchronous_standby_names",
								value))
		{
			execOk = true;
			break;
		}
		else
		{
			if (seconds < checkSeconds - 1)
				pg_usleep(1000000L);
		}
	}
	if (!execOk)
	{
		ereport(ERROR,
				(errmsg("%s set synchronous_standby_names failed",
						NameStr(mgrNode->form.nodename))));
	}
}

bool setGtmInfoInPGSqlConf(MgrNodeWrapper *mgrNode,
						   char *agtm_host,
						   char *snapsender_port,
						   char *gxidsender_port,
						   bool complain)
{
	bool done;

	PGConfParameterItem *items = NULL;
	items = newPGConfParameterItem("agtm_host", agtm_host, true);
	items->next = newPGConfParameterItem("snapsender_port",
										 snapsender_port, false);
	items->next->next = newPGConfParameterItem("gxidsender_port",
											   gxidsender_port, false);
	done = callAgentRefreshPGSqlConfReload(mgrNode, items, complain);
	pfreePGConfParameterItem(items);
	return done;
}

bool checkGtmInfoInPGresult(PGresult *pgResult,
							char *agtm_host,
							char *snapsender_port,
							char *gxidsender_port)
{
	int i;
	bool execOk;
	char *paramName;
	char *paramValue;

	execOk = true;
	for (i = 0; i < PQntuples(pgResult); i++)
	{
		paramName = PQgetvalue(pgResult, i, 0);
		paramValue = PQgetvalue(pgResult, i, 1);
		if (strcmp(paramName, "agtm_host") == 0)
		{
			if (!strcmp(paramValue, agtm_host) == 0)
			{
				execOk = false;
			}
		}
		else if (strcmp(paramName, "snapsender_port") == 0)
		{
			if (!strcmp(paramValue, snapsender_port) == 0)
			{
				execOk = false;
			}
		}
		else if (strcmp(paramName, "gxidsender_port") == 0)
		{
			if (!strcmp(paramValue, gxidsender_port) == 0)
			{
				execOk = false;
			}
		}
		else
		{
			ereport(DEBUG1,
					(errmsg("unexpected field:%s",
							paramName)));
		}
		if (!execOk)
			break;
	}
	return execOk;
}

bool checkGtmInfoInPGSqlConf(PGconn *pgConn,
							 char *nodename,
							 bool localSqlCheck,
							 char *agtm_host,
							 char *snapsender_port,
							 char *gxidsender_port)
{
	char *sql;
	PGresult *pgResult = NULL;
	bool execOk;

	if (localSqlCheck)
		sql = psprintf("select name, setting from pg_settings "
					   "where name in "
					   "('agtm_host','snapsender_port','gxidsender_port');");
	else
		sql = psprintf("EXECUTE DIRECT ON (\"%s\") "
					   "'select name, setting from pg_settings "
					   "where name in "
					   "(''agtm_host'',''snapsender_port'',''gxidsender_port'');'",
					   nodename);
	pgResult = PQexec(pgConn, sql);
	execOk = true;
	if (PQresultStatus(pgResult) == PGRES_TUPLES_OK)
	{
		execOk = checkGtmInfoInPGresult(pgResult, agtm_host,
										snapsender_port, gxidsender_port);
	}
	else
	{
		execOk = false;
		ereport(LOG,
				(errmsg("execute %s failed:%s",
						sql,
						PQerrorMessage(pgConn))));
	}
	if (pgResult)
		PQclear(pgResult);
	pfree(sql);
	return execOk;
}

void setCheckGtmInfoInPGSqlConf(MgrNodeWrapper *gtmMaster,
								MgrNodeWrapper *mgrNode,
								PGconn *pgConn,
								bool localSqlCheck,
								int checkSeconds)
{
	int seconds;
	bool execOk = false;
	char *agtm_host;
	char snapsender_port[12] = {0};
	char gxidsender_port[12] = {0};

	agtm_host = gtmMaster->host->hostaddr;
	pg_ltoa(gtmMaster->form.nodeport + 1, snapsender_port);
	pg_ltoa(gtmMaster->form.nodeport + 2, gxidsender_port);

	if (checkGtmInfoInPGSqlConf(pgConn,
								NameStr(mgrNode->form.nodename),
								localSqlCheck,
								agtm_host,
								snapsender_port,
								gxidsender_port))
	{
		return;
	}

	setGtmInfoInPGSqlConf(mgrNode, agtm_host,
						  snapsender_port,
						  gxidsender_port, true);

	for (seconds = 0; seconds < checkSeconds; seconds++)
	{
		execOk = checkGtmInfoInPGSqlConf(pgConn,
										 NameStr(mgrNode->form.nodename),
										 localSqlCheck,
										 agtm_host,
										 snapsender_port,
										 gxidsender_port);
		if (execOk)
		{
			break;
		}
		else
		{
			if (seconds < checkSeconds - 1)
				pg_usleep(1000000L);
		}
	}
	if (execOk)
	{
		ereport(NOTICE,
				(errmsg("set GTM information on %s successfully",
						NameStr(mgrNode->form.nodename))));
		ereport(LOG,
				(errmsg("set GTM information on %s successfully",
						NameStr(mgrNode->form.nodename))));
	}
	else
	{
		ereport(ERROR,
				(errmsg("set GTM information on %s failed",
						NameStr(mgrNode->form.nodename))));
	}
}

void setSlaveNodeRecoveryConf(MgrNodeWrapper *masterNode,
							  MgrNodeWrapper *slaveNode)
{
	PGConfParameterItem *items;
	char *primary_conninfo_value;

	items = newPGConfParameterItem("recovery_target_timeline", "latest", false);
	items->next = newPGConfParameterItem("standby_mode", "on", false);

	primary_conninfo_value = psprintf("host=%s port=%d user=%s application_name=%s",
									  masterNode->host->hostaddr,
									  masterNode->form.nodeport,
									  NameStr(slaveNode->host->form.hostuser),
									  NameStr(slaveNode->form.nodename));
	items->next->next = newPGConfParameterItem("primary_conninfo",
											   primary_conninfo_value, true);
	pfree(primary_conninfo_value);

	callAgentRefreshRecoveryConf(slaveNode, items, true);
	pfreePGConfParameterItem(items);
}

char *trimString(char *str)
{
	Datum datum;
	char *trimStr;

	datum = DirectFunctionCall2(btrim,
								CStringGetTextDatum(str),
								CStringGetTextDatum(" \t"));
	trimStr = TextDatumGetCString(datum);
	return trimStr;
}

bool equalsAfterTrim(char *str1, char *str2)
{
	char *trimStr1;
	char *trimStr2;

	trimStr1 = trimString(str1);
	trimStr2 = trimString(str2);
	return strcmp(trimStr1, trimStr2) == 0;
}

bool pingNodeWaitinSeconds(MgrNodeWrapper *node,
						   PGPing expectedPGPing,
						   int waitSeconds)
{
	int seconds;
	PingNodeResult pingNodeResult;
	for (seconds = 0; seconds < waitSeconds; seconds++)
	{
		pingNodeResult = callAgentPingNode(node);
		if (pingNodeResult.agentRes &&
			pingNodeResult.pgPing == expectedPGPing)
		{
			return true;
		}
		else
		{
			if (seconds < waitSeconds - 1)
				pg_usleep(1000000L);
		}
	}
	return false;
}

bool shutdownNodeWithinSeconds(MgrNodeWrapper *mgrNode,
							   int fastModeSeconds,
							   int immediateModeSeconds,
							   bool complain)
{
	callAgentStopNode(mgrNode, SHUTDOWN_F, false, false);
	if (!pingNodeWaitinSeconds(mgrNode,
							   PQPING_NO_RESPONSE,
							   fastModeSeconds))
	{
		if (immediateModeSeconds > 0)
		{
			callAgentStopNode(mgrNode, SHUTDOWN_I, false, false);
			if (!pingNodeWaitinSeconds(mgrNode,
									   PQPING_NO_RESPONSE,
									   immediateModeSeconds))
			{
				ereport(complain ? ERROR : LOG,
						(errmsg("try shut down node %s failed",
								NameStr(mgrNode->form.nodename))));
				return false;
			}
		}
		else
		{
			ereport(complain ? ERROR : LOG,
					(errmsg("try shut down node %s failed",
							NameStr(mgrNode->form.nodename))));
			return false;
		}
	}
	return true;
}

bool startupNodeWithinSeconds(MgrNodeWrapper *mgrNode,
							  int waitSeconds,
							  bool complain)
{
	callAgentStartNode(mgrNode, false, false);
	if (!pingNodeWaitinSeconds(mgrNode, PQPING_OK, waitSeconds))
	{
		ereport(complain ? ERROR : LOG,
				(errmsg("try start up node %s failed",
						NameStr(mgrNode->form.nodename))));
		return false;
	}
	return true;
}

bool batchPingNodesWaitinSeconds(dlist_head *nodes,
								 dlist_head *failedNodes,
								 PGPing expectedPGPing,
								 int waitSeconds)
{
	MgrNodeWrapper *node;
	MgrNodeWrapper *copyOfNode;
	dlist_mutable_iter iter;
	int seconds;
	PingNodeResult pingNodeResult;

	dlist_foreach_modify(iter, nodes)
	{
		node = dlist_container(MgrNodeWrapper, link, iter.cur);
		copyOfNode = palloc(sizeof(MgrNodeWrapper));
		memcpy(copyOfNode, node, sizeof(MgrNodeWrapper));
		dlist_push_tail(failedNodes, &copyOfNode->link);
	}
	for (seconds = 0; seconds < waitSeconds; seconds++)
	{
		dlist_foreach_modify(iter, failedNodes)
		{
			node = dlist_container(MgrNodeWrapper, link, iter.cur);
			pingNodeResult = callAgentPingNode(node);
			if (pingNodeResult.agentRes &&
				pingNodeResult.pgPing == expectedPGPing)
			{
				dlist_delete(iter.cur);
				pfree(node);
			}
		}
		if (dlist_is_empty(failedNodes))
		{
			break;
		}
		else
		{
			if (seconds < waitSeconds - 1)
				pg_usleep(1000000L);
		}
	}
	if (dlist_is_empty(failedNodes))
	{
		return true;
	}
	else
	{
		dlist_foreach_modify(iter, failedNodes)
		{
			node = dlist_container(MgrNodeWrapper, link, iter.cur);
			ereport(LOG,
					(errmsg("ping %s failed within seconds %d",
							NameStr(node->form.nodename),
							waitSeconds)));
		}
		return false;
	}
}

bool batchShutdownNodesWithinSeconds(dlist_head *nodes,
									 int fastModeSeconds,
									 int immediateModeSeconds,
									 bool complain)
{
	MgrNodeWrapper *node;
	dlist_head fastModeFailedNodes = DLIST_STATIC_INIT(fastModeFailedNodes);
	dlist_head immedModeFailedNodes = DLIST_STATIC_INIT(immedModeFailedNodes);
	dlist_mutable_iter iter;
	bool res;

	dlist_foreach_modify(iter, nodes)
	{
		node = dlist_container(MgrNodeWrapper, link, iter.cur);
		/* If error occurred, do not complain */
		callAgentStopNode(node, SHUTDOWN_F, false, false);
	}
	if (batchPingNodesWaitinSeconds(nodes,
									&fastModeFailedNodes,
									PQPING_NO_RESPONSE,
									fastModeSeconds))
	{
		res = true;
		goto end;
	}
	if (immediateModeSeconds <= 0)
	{
		dlist_foreach_modify(iter, &fastModeFailedNodes)
		{
			node = dlist_container(MgrNodeWrapper, link, iter.cur);
			ereport(complain ? ERROR : LOG,
					(errmsg("try shut down node %s failed",
							NameStr(node->form.nodename))));
		}
		res = false;
		goto end;
	}
	dlist_foreach_modify(iter, &fastModeFailedNodes)
	{
		node = dlist_container(MgrNodeWrapper, link, iter.cur);
		callAgentStopNode(node, SHUTDOWN_I, false, false);
	}
	if (batchPingNodesWaitinSeconds(&fastModeFailedNodes,
									&immedModeFailedNodes,
									PQPING_NO_RESPONSE,
									immediateModeSeconds))
	{
		res = true;
		goto end;
	}
	dlist_foreach_modify(iter, &immedModeFailedNodes)
	{
		node = dlist_container(MgrNodeWrapper, link, iter.cur);
		ereport(complain ? ERROR : LOG,
				(errmsg("try shut down node %s failed",
						NameStr(node->form.nodename))));
	}
	res = false;
	goto end;

end:
	dlist_foreach_modify(iter, &fastModeFailedNodes)
	{
		node = dlist_container(MgrNodeWrapper, link, iter.cur);
		dlist_delete(iter.cur);
		pfree(node);
	}
	dlist_foreach_modify(iter, &immedModeFailedNodes)
	{
		node = dlist_container(MgrNodeWrapper, link, iter.cur);
		dlist_delete(iter.cur);
		pfree(node);
	}
	return res;
}

bool batchStartupNodesWithinSeconds(dlist_head *nodes,
									int waitSeconds,
									bool complain)
{
	MgrNodeWrapper *node;
	dlist_head failedNodes = DLIST_STATIC_INIT(failedNodes);
	dlist_mutable_iter iter;
	bool res;

	dlist_foreach_modify(iter, nodes)
	{
		node = dlist_container(MgrNodeWrapper, link, iter.cur);
		/* If error occurred, do not complain */
		callAgentStartNode(node, false, false);
	}
	if (batchPingNodesWaitinSeconds(nodes, &failedNodes,
									PQPING_OK, waitSeconds))
	{
		res = true;
		goto end;
	}
	dlist_foreach_modify(iter, &failedNodes)
	{
		node = dlist_container(MgrNodeWrapper, link, iter.cur);
		ereport(complain ? ERROR : LOG,
				(errmsg("try start up node %s failed",
						NameStr(node->form.nodename))));
	}
	res = false;
	goto end;

end:
	dlist_foreach_modify(iter, &failedNodes)
	{
		node = dlist_container(MgrNodeWrapper, link, iter.cur);
		dlist_delete(iter.cur);
		pfree(node);
	}
	return res;
}

bool dropNodeFromPgxcNode(PGconn *activeCoon,
						  char *executeOnNodeName,
						  char *nodeName, bool complain)
{
	char *sql;
	bool execOk;

	sql = psprintf("drop node \"%s\" on (\"%s\");",
				   nodeName,
				   executeOnNodeName);
	execOk = PQexecCommandSql(activeCoon, sql, false);
	pfree(sql);
	if (execOk)
	{
		ereport(LOG,
				(errmsg("%s drop %s from pgxc_node successfully",
						executeOnNodeName,
						nodeName)));
	}
	else
	{
		ereport(complain ? ERROR : WARNING,
				(errmsg("%s drop %s from pgxc_node failed",
						executeOnNodeName,
						nodeName)));
	}
	return execOk;
}

bool createNodeOnPgxcNode(PGconn *activeCoon,
						  char *executeOnNodeName,
						  MgrNodeWrapper *mgrNode, bool complain)
{
	char *sql;
	bool execOk;
	char *type;

	if (mgrNode->form.nodetype == CNDN_TYPE_COORDINATOR_MASTER)
	{
		type = "coordinator";
	}
	else if (mgrNode->form.nodetype == CNDN_TYPE_DATANODE_MASTER)
	{
		type = "datanode";
	}
	else
	{
		ereport(complain ? ERROR : WARNING,
				(errmsg("%s create %s on pgxc_node failed, unknow nodetype:%c",
						executeOnNodeName,
						NameStr(mgrNode->form.nodename),
						mgrNode->form.nodetype)));
		return false;
	}
	sql = psprintf("EXECUTE DIRECT ON (\"%s\")  "
				   "'CREATE NODE %s WITH (TYPE=''%s'',HOST=''%s'',PORT=%d);'",
				   executeOnNodeName,
				   NameStr(mgrNode->form.nodename),
				   type,
				   mgrNode->host->hostaddr,
				   mgrNode->form.nodeport);
	execOk = PQexecCommandSql(activeCoon, sql, complain);
	pfree(sql);
	return execOk;
}

bool nodeExistsInPgxcNode(PGconn *activeCoon,
						  char *executeOnNodeName,
						  bool localExecute,
						  char *nodeName,
						  char pgxcNodeType,
						  bool complain)
{
	char *sql;
	bool exists;
	if (localExecute)
	{
		if (pgxcNodeType > 0)
		{
			sql = psprintf("select count(*) "
						   "from pgxc_node "
						   "where node_name = '%s' "
						   "and node_type = '%c';",
						   nodeName,
						   pgxcNodeType);
		}
		else
		{
			sql = psprintf("select count(*) "
						   "from pgxc_node "
						   "where node_name = '%s'; ",
						   nodeName);
		}
	}
	else
	{
		if (pgxcNodeType > 0)
		{
			sql = psprintf("EXECUTE DIRECT ON (\"%s\") "
						   "'select count(*) "
						   "from pgxc_node "
						   "where node_name = ''%s'' "
						   "and node_type = ''%c'' ;'",
						   executeOnNodeName,
						   nodeName,
						   pgxcNodeType);
		}
		else
		{
			sql = psprintf("EXECUTE DIRECT ON (\"%s\") "
						   "'select count(*) "
						   "from pgxc_node "
						   "where node_name = ''%s'' ;'",
						   executeOnNodeName,
						   nodeName);
		}
	}
	exists = PQexecCountSql(activeCoon, sql, complain) > 0;
	pfree(sql);
	return exists;
}

bool dataNodeMasterExistsInPgxcNode(PGconn *activeCoon,
									char *executeOnNodeName,
									bool localExecute,
									char *nodeName,
									bool complain)
{
	return nodeExistsInPgxcNode(activeCoon,
								executeOnNodeName,
								localExecute,
								nodeName,
								PGXC_NODE_DATANODE,
								complain);
}

bool dataNodeSlaveExistsInPgxcNode(PGconn *activeCoon,
								   char *executeOnNodeName,
								   bool localExecute,
								   char *nodeName,
								   bool complain)
{
	return nodeExistsInPgxcNode(activeCoon,
								executeOnNodeName,
								localExecute,
								nodeName,
								PGXC_NODE_DATANODESLAVE,
								complain);
}

bool coordinatorMasterExistsInPgxcNode(PGconn *activeCoon,
									   char *executeOnNodeName,
									   bool localExecute,
									   char *nodeName,
									   bool complain)
{
	return nodeExistsInPgxcNode(activeCoon,
								executeOnNodeName,
								localExecute,
								nodeName,
								PGXC_NODE_COORDINATOR,
								complain);
}

char getMappedPgxcNodetype(char mgrNodetype)
{
	if (mgrNodetype == CNDN_TYPE_DATANODE_MASTER)
	{
		return PGXC_NODE_DATANODE;
	}
	else if (mgrNodetype == CNDN_TYPE_DATANODE_MASTER)
	{
		return PGXC_NODE_DATANODESLAVE;
	}
	else if (mgrNodetype == CNDN_TYPE_COORDINATOR_MASTER ||
			 mgrNodetype == CNDN_TYPE_GTM_COOR_MASTER)
	{
		return PGXC_NODE_COORDINATOR;
	}
	else
	{
		return PGXC_NODE_NONE;
	}
}
