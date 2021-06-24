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
#include "replication/syncrep.h"
#include "utils/fmgroids.h"


#define MGR_MINIMUM_VERSION_FOR_RECOVERY_GUC 120000

static MgrHostWrapper *popHeadMgrHostPfreeOthers(dlist_head *mgrHosts);
static MgrNodeWrapper *popHeadMgrNodePfreeOthers(dlist_head *mgrNodes);
static void CreateReplicationSlot(char *nodename, char *slot_name);
static void DeleteReplicationSlot(char *nodename, char *slot_name);
static void UpdateSyncStandbyNames(MgrNodeWrapper *mgrNode,
									PGconn *conn);																		
static void MgrGetSyncStandByName(MgrNodeWrapper *node,
								 StringInfoData *infosendmsg);

void logMgrNodeWrapper(MgrNodeWrapper *src, char *title, int elevel)
{
	char *rTitle = "";
	if (title != NULL && strlen(title) > 0)
		rTitle = title;
	ereport(elevel,
			(errmsg("%s oid:%u,nodename:%s,nodehost:%u,nodetype:%c,nodesync:%s,nodeport:%d,nodemasternameoid:%u,curestatus:%s",
					rTitle,
					src->form.oid,
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
					src->form.oid,
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
void selectActiveMasterCoordinators(MemoryContext spiContext,
									dlist_head *resultList)
{
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * \n"
					 "FROM pg_catalog.mgr_node \n"
					 "WHERE nodetype in ('%c', '%c') \n"
					 "AND nodeinited = %d::boolean \n"
					 "AND nodeincluster = %d::boolean \n"
					 "AND curestatus != '%s' \n",
					 CNDN_TYPE_COORDINATOR_MASTER,
					 CNDN_TYPE_GTM_COOR_MASTER,
					 true,
					 true,
					 CURE_STATUS_ISOLATED);
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

void selectMgrNodeByNodetypeEx(MemoryContext spiContext,
							   char nodetype,
							   dlist_head *resultList)
{
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * \n"
					 "FROM pg_catalog.mgr_node \n"
					 "WHERE nodetype = '%c' ",
					 nodetype);
	selectMgrNodes(sql.data, spiContext, resultList);
	pfree(sql.data);
}
void selectActiveMgrNodeByNodetype(MemoryContext spiContext,
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
					 "AND nodeincluster = %d::boolean \n"
					 "AND curestatus != '%s' \n",
					 nodetype,
					 true,
					 true,
					 CURE_STATUS_ISOLATED);
	selectMgrNodes(sql.data, spiContext, resultList);
	pfree(sql.data);
}
void selectActiveMgrNode(MemoryContext spiContext,
						dlist_head *resultList)
{
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * \n"
					 "FROM pg_catalog.mgr_node \n"
					 "WHERE nodeinited = %d::boolean \n"
					 "AND nodeincluster = %d::boolean \n"
					 "AND curestatus != '%s' \n",
					 true,
					 true,
					 CURE_STATUS_ISOLATED);
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
void selectMgrAllDataNodesInZone(MemoryContext spiContext,
								 char *zone,
								 dlist_head *resultList)
{
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * \n"
					 "FROM pg_catalog.mgr_node \n"
					 "WHERE nodetype in ('%c','%c') \n"
					 "AND nodeinited = %d::boolean \n"
					 "AND nodeincluster = %d::boolean \n"
					 "AND nodezone = '%s' \n",
					 CNDN_TYPE_DATANODE_MASTER,
					 CNDN_TYPE_DATANODE_SLAVE,
					 true,
					 true,
					 zone);
	selectMgrNodes(sql.data, spiContext, resultList);
	pfree(sql.data);
}

void selectMgrSlaveNodes(Oid masterOid,
						 char nodetype,
						 MemoryContext spiContext,
						 dlist_head *resultList)
{
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * "
					 "FROM pg_catalog.mgr_node "
					 "WHERE nodetype = '%c' "
					 "AND nodeinited = %d::boolean "
					 "AND nodemasternameoid = %u "
					 "AND curestatus != '%s' ",
					 nodetype,
					 true,
					 masterOid,
					 CURE_STATUS_ISOLATED);
	selectMgrNodes(sql.data, spiContext, resultList);
	pfree(sql.data);
}
/**
 * the list link data type is MgrNodeWrapper
 */
void selectActiveMgrSlaveNodes(Oid masterOid,
							   char nodetype,
							   MemoryContext spiContext,
							   dlist_head *resultList)
{
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * "
					 "FROM pg_catalog.mgr_node "
					 "WHERE nodetype = '%c' "
					 "AND nodeinited = %d::boolean "
					 "AND nodeincluster = %d::boolean "
					 "AND nodemasternameoid = %u "
					 "AND curestatus != '%s' ",
					 nodetype,
					 true,
					 true,
					 masterOid,
					 CURE_STATUS_ISOLATED);
	selectMgrNodes(sql.data, spiContext, resultList);
	pfree(sql.data);
}
void selectMgrSlaveNodesByOidType(Oid masterOid,
								char nodetype,
								MemoryContext spiContext,
								dlist_head *resultList)
{
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * "
					 "FROM pg_catalog.mgr_node "
					 "WHERE nodetype = '%c' "
					 "AND nodemasternameoid = %u "
					 "AND curestatus != '%s' ",
					 nodetype,
					 masterOid,
					 CURE_STATUS_ISOLATED);
	selectMgrNodes(sql.data, spiContext, resultList);
	pfree(sql.data);
}
void selectChildNodes(MemoryContext spiContext,
					  Oid oid,
					  dlist_head *resultList)
{
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * "
					 "FROM pg_catalog.mgr_node "
					 "WHERE nodemasternameoid = %u ",
					 oid);
	selectMgrNodes(sql.data, spiContext, resultList);
	pfree(sql.data);
}
void selectChildNodesInZone(MemoryContext spiContext,
							Oid masterOid,
							char *zone,
							dlist_head *resultList)
{
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * "
					 "FROM pg_catalog.mgr_node "
					 "WHERE nodemasternameoid = %u "
					 "AND nodezone = '%s' ",
					 masterOid,
					 zone);
	selectMgrNodes(sql.data, spiContext, resultList);
	pfree(sql.data);
}
void selectNotActiveChildInZone(MemoryContext spiContext,
								char *zone,
								dlist_head *resultList)
{
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * "
					 "FROM pg_catalog.mgr_node "
					 "WHERE nodeinited = %d::boolean "
					 "AND nodeincluster = %d::boolean "
					 "AND nodezone = '%s' ",
					 false,
					 false,
					 zone);
	selectMgrNodes(sql.data, spiContext, resultList);
	pfree(sql.data);
}
void selectAllNodesInZone(MemoryContext spiContext,
						  char *zone,
						  dlist_head *resultList)
{
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * "
					 "FROM pg_catalog.mgr_node "
					 "WHERE nodezone = '%s' ",
					 zone);
	selectMgrNodes(sql.data, spiContext, resultList);
	pfree(sql.data);
}
void selectActiveMgrSlaveNodesInZone(Oid masterOid,
									 char nodetype,
									 char *zone,
									 MemoryContext spiContext,
									 dlist_head *resultList)
{
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * "
					 "FROM pg_catalog.mgr_node "
					 "WHERE nodetype = '%c' "
					 "AND nodeinited = %d::boolean "
					 "AND nodeincluster = %d::boolean "
					 "AND nodemasternameoid = %u "
					 "AND curestatus != '%s' "
					 "AND nodezone = '%s' ",
					 nodetype,
					 true,
					 true,
					 masterOid,
					 CURE_STATUS_ISOLATED,
					 zone);
	selectMgrNodes(sql.data, spiContext, resultList);
	pfree(sql.data);
}
void selectMgrSlaveNodesByOidTypeInZone(Oid masterOid,
									 char nodetype,
									 char *zone,
									 MemoryContext spiContext,
									 dlist_head *resultList)
{
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * "
					 "FROM pg_catalog.mgr_node "
					 "WHERE nodetype = '%c' "
					 "AND nodemasternameoid = %u "
					 "AND curestatus != '%s' "
					 "AND nodezone = '%s' ",
					 nodetype,
					 masterOid,
					 CURE_STATUS_ISOLATED,
					 zone);
	selectMgrNodes(sql.data, spiContext, resultList);
	pfree(sql.data);
}

List *selectAllNodesInRepGroup(MgrNodeWrapper *mgrNode,
							   MemoryContext spiContext)
{
	StringInfoData sql;
	Oid nodemasternameoid = InvalidOid;
	dlist_iter iter;
	List *allNodesInRepGroup = NIL;
	MgrNodeWrapper *nodeInDB;
	dlist_head resultList = DLIST_STATIC_INIT(resultList);

	nodeInDB = selectMgrNodeByOid(mgrNode->form.oid, spiContext);
	if (isMasterNode(nodeInDB->form.nodetype, true))
		nodemasternameoid = nodeInDB->form.oid;
	else
		nodemasternameoid = nodeInDB->form.nodemasternameoid;
	pfreeMgrNodeWrapper(nodeInDB);
	if (!OidIsValid(nodemasternameoid))
		ereport(ERROR,
				(errmsg("nodename:%s, can not get a valid master node oid",
						NameStr(mgrNode->form.nodename))));

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * "
					 "FROM pg_catalog.mgr_node "
					 "WHERE nodemasternameoid = %u "
					 "OR oid = %u ",
					 nodemasternameoid,
					 nodemasternameoid);
	selectMgrNodes(sql.data, spiContext, &resultList);

	dlist_foreach(iter, &resultList)
	{
		MgrNodeWrapper *node = dlist_container(MgrNodeWrapper, link, iter.cur);
		allNodesInRepGroup = lappend(allNodesInRepGroup, node);
	}
	pfree(sql.data);
	return allNodesInRepGroup;
}

void selectIsolatedMgrSlaveNodes(Oid masterOid,
								 char nodetype,
								 MemoryContext spiContext,
								 dlist_head *resultList)
{
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * \n"
					 "FROM pg_catalog.mgr_node "
					 "WHERE nodetype = '%c' "
					 "AND nodemasternameoid = %u "
					 "AND curestatus = '%s' "
					 "AND nodezone = '%s' ",
					 nodetype,
					 masterOid,
					 CURE_STATUS_ISOLATED,
					 mgr_zone);
	selectMgrNodes(sql.data, spiContext, resultList);
	pfree(sql.data);
}
void selectIsolatedMgrSlaveNodesByNodeType(char nodetype,
										   MemoryContext spiContext,
										   dlist_head *resultList)
{
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * \n"
					 "FROM pg_catalog.mgr_node "
					 "WHERE nodeinited = %d::boolean \n"
					 "AND nodeincluster = %d::boolean \n"
					 "AND nodetype = '%c' \n"
					 "AND curestatus = '%s' \n",
					 true,
					 true,
					 nodetype,
					 CURE_STATUS_ISOLATED);
	selectMgrNodes(sql.data, spiContext, resultList);
	pfree(sql.data);
}

void selectAllMgrSlaveNodes(Oid masterOid,
							char nodetype,
							MemoryContext spiContext,
							dlist_head *resultList)
{
	selectActiveMgrSlaveNodes(masterOid, nodetype,
							  spiContext, resultList);
	selectIsolatedMgrSlaveNodes(masterOid, nodetype,
								spiContext, resultList);
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
									 char nodetype,
									 dlist_head *resultList)
{
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * "
					 "FROM pg_catalog.mgr_node "
					 "WHERE allowcure = %d::boolean "
					 "AND curestatus in ('%s') "
					 "AND nodetype in ('%c') ",
					 true,
					 CURE_STATUS_ISOLATED,
					 nodetype);
	selectMgrNodes(sql.data, spiContext, resultList);
	pfree(sql.data);
}

void selectIsolatedMgrNodes(MemoryContext spiContext,
							dlist_head *resultList)
{
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * \n"
					 "FROM pg_catalog.mgr_node \n"
					 "WHERE allowcure = %d::boolean \n"
					 "AND curestatus in ('%s') \n"
					 "AND nodetype in ('%c','%c','%c') \n",
					 true,
					 CURE_STATUS_ISOLATED,
					 CNDN_TYPE_COORDINATOR_MASTER,
					 CNDN_TYPE_DATANODE_SLAVE,
					 CNDN_TYPE_GTM_COOR_SLAVE);
	selectMgrNodes(sql.data, spiContext, resultList);
	pfree(sql.data);
}
void selectNodeNotZone(MemoryContext spiContext,
					   char *zone,
					   char nodetype,
					   dlist_head *resultList)
{
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * \n"
					 "FROM pg_catalog.mgr_node \n"
					 "WHERE nodeinited = %d::boolean \n"
					 "AND nodeincluster = %d::boolean \n"
					 "AND nodetype = '%c' \n"
					 "AND nodezone != '%s' \n",
					 true,
					 true,
					 nodetype,
					 zone);
	selectMgrNodes(sql.data, spiContext, resultList);
	pfree(sql.data);
}
void selectActiveNodeInZone(MemoryContext spiContext,
							char *zone,
							char nodetype,
							dlist_head *resultList)
{
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * \n"
					 "FROM pg_catalog.mgr_node \n"
					 "WHERE nodeinited = %d::boolean \n"
					 "AND nodeincluster = %d::boolean \n"
					 "AND nodetype = '%c' \n"
					 "AND nodezone = '%s' \n",
					 true,
					 true,
					 nodetype,
					 zone);
	selectMgrNodes(sql.data, spiContext, resultList);
	pfree(sql.data);
}
void selectNodeNotZoneForFailover(MemoryContext spiContext,
								  char *zone,
								  char nodetype,
								  dlist_head *resultList)
{
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * \n"
					 "FROM pg_catalog.mgr_node \n"
					 "WHERE nodetype = '%c' \n"
					 "AND nodezone != '%s' \n",
					 nodetype,
					 zone);
	selectMgrNodes(sql.data, spiContext, resultList);
	pfree(sql.data);
}
void selectNodeZoneOid(MemoryContext spiContext,
					   char nodetype,
					   char *nodezone,
					   Oid oid,
					   dlist_head *resultList)
{
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * \n"
					 "FROM pg_catalog.mgr_node \n"
					 "WHERE nodeinited = %d::boolean \n"
					 "AND nodeincluster = %d::boolean \n"
					 "AND nodetype = '%c' \n"
					 "AND nodezone = '%s' \n"
					 "AND nodemasternameoid = %u \n",
					 true,
					 true,
					 nodetype,
					 nodezone,
					 oid);
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
					 mgrNode->form.oid,
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

int updateMgrNodeNodesync(MgrNodeWrapper *mgrNode,
						  char *newNodesync,
						  MemoryContext spiContext)
{
	StringInfoData buf;
	int spiRes;
	uint64 rows;
	MemoryContext oldCtx;

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "update pg_catalog.mgr_node "
					 "set nodesync = '%s' "
					 "WHERE oid = %u "
					 "and nodemasternameoid = %u "
					 "and nodesync = '%s' ",
					 newNodesync,
					 mgrNode->form.oid,
					 mgrNode->form.nodemasternameoid,
					 NameStr(mgrNode->form.nodesync));
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
					 "set curestatus = '%s' \n"
					 "WHERE oid = %u \n"
					 "and curestatus = '%s' \n"
					 "and nodetype = '%c' \n",
					 newCurestatus,
					 mgrNode->form.oid,
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
	char *newCurestatus;

	newCurestatus = CURE_STATUS_ISOLATED;
	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "update pg_catalog.mgr_node "
					 "set curestatus = '%s', "
					 "nodeinited = %d::boolean, "
					 "nodeincluster = %d::boolean "
					 "WHERE oid = %u "
					 "and curestatus = '%s' "
					 "and nodetype = '%c' ",
					 newCurestatus,
					 false,
					 false,
					 mgrNode->form.oid,
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
					 "update pg_catalog.mgr_node "
					 "set curestatus = '%s', "
					 "nodeinited = %d::boolean, "
					 "nodeincluster = %d::boolean "
					 "WHERE oid = %u "
					 "and curestatus = '%s' "
					 "and nodetype = '%c' ",
					 CURE_STATUS_NORMAL,
					 true,
					 true,
					 mgrNode->form.oid,
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

int executeUpdateSql(char *sql, MemoryContext spiContext)
{
	int spiRes;
	uint64 rows;
	MemoryContext oldCtx;
	oldCtx = MemoryContextSwitchTo(spiContext);
	spiRes = SPI_execute(sql, false, 0);
	MemoryContextSwitchTo(oldCtx);
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
	const char *gram = NULL;
	char *errorMessage = NULL;

	initStringInfo(&conninfo);
	appendStringInfo(&conninfo, "host='%s' port=%u dbname='%s' user='%s' connect_timeout=%d",
					 node->host->hostaddr,
					 node->form.nodeport,
					 DEFAULT_DB,
					 NameStr(node->host->form.hostuser),
					 connectTimeout);
	conn = PQconnectdb(conninfo.data);
	pfree(conninfo.data);
	if (PQstatus(conn) == CONNECTION_OK)
	{
		gram = PQparameterStatus(conn, "grammar");
		if (gram != NULL && pg_strcasecmp(gram, GARMMAR_POSTGRES) != 0)
			PQexec(conn, SET_GRAMMAR_POSTGRES);

		ereport(DEBUG1,
				(errmsg("connect node %s successfully",
						NameStr(node->form.nodename))));
		connStatus = NODE_CONNECTION_STATUS_SUCCESS;
	}
	else
	{
		errorMessage = PQerrorMessage(conn);
		ereport(LOG,
				(errmsg("connect node %s error, %s",
						NameStr(node->form.nodename),
						errorMessage)));
		if (strcmp(conn->last_sqlstate, "57P03") == 0)
		{
			if (errorMessage != NULL && strstr(errorMessage, "shutting down") != NULL)
				connStatus = NODE_CONNECTION_STATUS_CANNOT_SHUTTING_DOWN;
			else
				connStatus = NODE_CONNECTION_STATUS_CANNOT_CONNECT_NOW;
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
			 connStatus == NODE_CONNECTION_STATUS_CANNOT_CONNECT_NOW ||
			 connStatus == NODE_CONNECTION_STATUS_CANNOT_SHUTTING_DOWN)
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

PGPing pingNodeDefaultDB(MgrNodeWrapper *node,
						 int connectTimeout)
{
	PGPing pgPing;

	StringInfoData conninfo;

	initStringInfo(&conninfo);
	appendStringInfo(&conninfo,
					 "postgresql://%s@%s:%d/%s?connect_timeout=%d",
					 NameStr(node->host->form.hostuser),
					 node->host->hostaddr,
					 node->form.nodeport,
					 DEFAULT_DB,
					 connectTimeout);
	pgPing = PQping(conninfo.data);
	pfree(conninfo.data);
	return pgPing;
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
char *showNodeParameter(char *nodeName, PGconn *pgConn, char *name, bool complain)
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
				(errmsg("on %s execute %s failed:%s",
						nodeName,
						sql,
						PQerrorMessage(pgConn))));
		value = palloc0(1);
	}
	pfree(sql);
	if (pgResult)
		PQclear(pgResult);
	return value;
}

bool equalsNodeParameter(char *nodeName, PGconn *pgConn, char *name, char *expectValue)
{
	bool equal;
	char *actualValue;
	actualValue = showNodeParameter(nodeName, pgConn, name, true);
	equal = equalsParameterValue(actualValue, expectValue);
	pfree(actualValue);
	return equal;
}

bool equalsParameterValue(char *parameterValue1, char *parameterValue2)
{
	return is_equal_string(parameterValue1, parameterValue2) ||
		   ((parameterValue1 == NULL || strlen(parameterValue1) == 0) &&
			(parameterValue2 == NULL || strlen(parameterValue2) == 0));
}

bool PQexecCommandSql(PGconn *pgConn, char *sql, bool complain)
{
	PGresult *pgResult;
	bool execOk;

	pgResult = PQexec(pgConn, sql);
	if (PQresultStatus(pgResult) == PGRES_COMMAND_OK)
	{
		execOk = true;
		ereport(LOG,
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
		if (PQntuples(pgResult) < 1)
		{
			return false;
		}
		if (PQgetisnull(pgResult, 0, 0))
		{
			return false;
		}
		value = PQgetvalue(pgResult, 0, 0);
		if (!value)
		{
			return false;
		}
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

bool exec_pgxc_pool_reload(PGconn *coordCoon,
						   bool localExecute,
						   char *executeOnNodeName,
						   bool complain)
{
	char *sql;
	bool res;
	if (localExecute)
		sql = psprintf("select pgxc_pool_reload();");
	else
		sql = psprintf("EXECUTE DIRECT ON (\"%s\") "
					   "'select pgxc_pool_reload();';",
					   executeOnNodeName);
	res = PQexecBoolQuery(coordCoon, sql, true, complain);
	pfree(sql);
	if (res)
		ereport(LOG,
				(errmsg("%s execute pgxc_pool_reload() successfully",
						executeOnNodeName)));
	return res;
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
		ereport(LOG,
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
		ereport(LOG,
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
		ereport(LOG,
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
		ereport(LOG,
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
		ereport(LOG,
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
	res = callAgentSendCmd(AGT_CMD_CNDN_REFRESH_PGSQLCONFAUTO,
						&cmdMessage,
						node->host->hostaddr,
						node->host->form.hostagentport);
	pfree(cmdMessage.data);
	if (res.agentRes)
	{
		ereport(LOG,
				(errmsg("refresh %s %s postgresql.auto.conf successfully",
						NameStr(node->form.nodename),
						node->nodepath)));
	}
	else
	{
		ereport(complain ? ERROR : LOG,
				(errmsg("refresh %s %s postgresql.auto.conf failed:%s",
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
		node->form.nodetype == CNDN_TYPE_DATANODE_SLAVE ||
		node->form.nodetype == CNDN_TYPE_COORDINATOR_MASTER ||
		node->form.nodetype == CNDN_TYPE_COORDINATOR_SLAVE)
	{
		cmd = AGT_CMD_DN_FAILOVER;
	}
	else if (node->form.nodetype == CNDN_TYPE_GTM_COOR_MASTER ||
			 node->form.nodetype == CNDN_TYPE_GTM_COOR_SLAVE)
	{
		cmd = AGT_CMD_GTMCOORD_SLAVE_FAILOVER;
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
		ereportNoticeLog(errmsg("promote %s %s successfully",
						NameStr(node->form.nodename),
						node->nodepath));
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
		cmd = AGT_CMD_GTMCOORD_STOP_MASTER;
	else if (node->form.nodetype == CNDN_TYPE_GTM_COOR_SLAVE)
		cmd = AGT_CMD_GTMCOORD_STOP_SLAVE;
	else if (node->form.nodetype == CNDN_TYPE_COORDINATOR_MASTER ||
			 node->form.nodetype == CNDN_TYPE_COORDINATOR_SLAVE)
		cmd = AGT_CMD_CN_STOP;
	else
		cmd = AGT_CMD_DN_STOP;

	ereportNoticeLog(errmsg("stop %s %s %s.", mgr_get_nodetype_desc(node->form.nodetype), NameStr(node->form.nodename), shutdownMode));
	res = callAgentSendCmd(cmd, &cmdMessage,
						   node->host->hostaddr,
						   node->host->form.hostagentport);
	pfree(cmdMessage.data);
	if (res.agentRes)
	{
		ereport(LOG,
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
		cmd = AGT_CMD_GTMCOORD_START_MASTER;
	if (node->form.nodetype == CNDN_TYPE_GTM_COOR_SLAVE)
		cmd = AGT_CMD_GTMCOORD_START_SLAVE; /* agtm_ctl */
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
		ereportNoticeLog(errmsg("call agent start %s %s %s successfully",
						mgr_get_nodetype_desc(node->form.nodetype),
						NameStr(node->form.nodename),
						node->nodepath));
	}
	else
	{
		ereport(complain ? ERROR : LOG,
				(errmsg("call agent start %s %s %s failed:%s",
						mgr_get_nodetype_desc(node->form.nodetype),
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
		ereport(LOG,
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

	initStringInfo(&cmdMessage);
	appendStringInfo(&cmdMessage,
					 " --target-pgdata %s --source-server='host=%s port=%d user=%s dbname=postgres' --target-nodename %s --source-nodename %s",
					 slaveNode->nodepath,
					 masterNode->host->hostaddr,
					 masterNode->form.nodeport,
					 NameStr(slaveNode->host->form.hostuser),
					 NameStr(slaveNode->form.nodename),
					 NameStr(masterNode->form.nodename));
	res = callAgentSendCmd(AGT_CMD_NODE_REWIND,
						   &cmdMessage,
						   slaveNode->host->hostaddr,
						   slaveNode->host->form.hostagentport);
	pfree(cmdMessage.data);
	if (res.agentRes)
	{
		ereport(LOG,
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
								   MgrNodeWrapper *slaveNode,
								   bool complain)
{
	PGHbaItem *hbaItems;

	hbaItems = newPGHbaItem(CONNECT_HOST, "replication",
							NameStr(slaveNode->host->form.hostuser),
							slaveNode->host->hostaddr,
							32, "trust");
	callAgentRefreshPGHbaConf(masterNode, hbaItems, complain);
	callAgentReloadNode(masterNode, complain);
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

	for (seconds = 0; seconds <= checkSeconds; seconds++)
	{
		/* check the param */
		if (equalsNodeParameter(NameStr(mgrNode->form.nodename),
								pgConn,
								"synchronous_standby_names",
								value))
		{
			execOk = true;
			break;
		}
		else
		{
			if (seconds < checkSeconds)
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
						   MgrNodeWrapper *gtmMaster,
						   bool complain)
{
	bool done;
	char *agtm_host;
	char agtm_port[12] = {0};
	PGConfParameterItem *items = NULL;

	EXTRACT_GTM_INFOMATION(gtmMaster,
						   agtm_host,
						   agtm_port);
	items = newPGConfParameterItem("agtm_host", agtm_host, true);
	items->next = newPGConfParameterItem("agtm_port", agtm_port, false);
	done = callAgentRefreshPGSqlConfReload(mgrNode, items, complain);
	pfreePGConfParameterItem(items);
	return done;
}

bool checkGtmInfoInPGresult(PGresult *pgResult,
							char *agtm_host,
							char *agtm_port)
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
			if (strcmp(paramValue, agtm_host) != 0)
			{
				execOk = false;
			}
		}
		else if (strcmp(paramName, "agtm_port") == 0)
		{
			if (strcmp(paramValue, agtm_port) != 0)
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
							 MgrNodeWrapper *gtmMaster)
{
	char *sql;
	PGresult *pgResult = NULL;
	bool execOk;
	char *agtm_host;
	char agtm_port[12] = {0};

	EXTRACT_GTM_INFOMATION(gtmMaster,
						   agtm_host,
						   agtm_port);

	if (localSqlCheck)
		sql = psprintf("select name, setting from pg_settings "
					   "where name in "
					   "('agtm_host','agtm_port');");
	else
		sql = psprintf("EXECUTE DIRECT ON (\"%s\") "
					   "'select name, setting from pg_settings "
					   "where name in "
					   "(''agtm_host'',''agtm_port'');'",
					   nodename);
	pgResult = PQexec(pgConn, sql);
	execOk = true;
	if (PQresultStatus(pgResult) == PGRES_TUPLES_OK)
	{
		execOk = checkGtmInfoInPGresult(pgResult, agtm_host, agtm_port);
	}
	else
	{
		execOk = false;
		ereport(LOG,
				(errmsg("on node(%s) check Gtm Info, execute %s failed:%s ",
						nodename,
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
								int checkSeconds,
								bool complain)
{
	int seconds;
	bool execOk = false;

	if (checkGtmInfoInPGSqlConf(pgConn,
								NameStr(mgrNode->form.nodename),
								localSqlCheck,
								gtmMaster))
	{
		return;
	}
	if (setGtmInfoInPGSqlConf(mgrNode,
							  gtmMaster,
							  complain))
	{
		for (seconds = 0; seconds <= checkSeconds; seconds++)
		{
			execOk = checkGtmInfoInPGSqlConf(pgConn,
											 NameStr(mgrNode->form.nodename),
											 localSqlCheck,
											 gtmMaster);
			if (execOk)
			{
				break;
			}
			else
			{
				if (seconds < checkSeconds)
					pg_usleep(1000000L);
			}
		}
	}
	else
	{
		execOk = false;
	}

	if (execOk)
	{
		ereport(LOG,
				(errmsg("set GTM information on %s successfully",
						NameStr(mgrNode->form.nodename))));
	}
	else
	{
		ereport(complain ? ERROR : LOG,
				(errmsg("set GTM information on %s failed",
						NameStr(mgrNode->form.nodename))));
	}
}

static void CreateReplicationSlot(char *nodename, char *slot_name)
{
	char *qrySql;
	char *createSql;
	bool is_exist, is_running;
	AppendNodeInfo nodeinfo;
	char *address;
	char *user;
	int agentPort;
	int max_try = 3;
	StringInfoData restmsg;

	qrySql = psprintf("SELECT slot_name FROM pg_replication_slots WHERE slot_name = '%s'", slot_name);
	createSql = psprintf("SELECT pg_create_physical_replication_slot('%s')", slot_name);

	memset(&nodeinfo, 0, sizeof(AppendNodeInfo));
	mgr_get_nodeinfo_byname_type(nodename, CNDN_TYPE_DATANODE_MASTER, false,
								 &is_exist, &is_running, &nodeinfo);
	if (nodeinfo.nodehost == 0)
	{
		mgr_get_nodeinfo_byname_type(nodename, CNDN_TYPE_DATANODE_SLAVE, false,
									 &is_exist, &is_running, &nodeinfo);
		if (nodeinfo.nodehost == 0)
		{
			ereport(ERROR, (errmsg("datanode %s is not exist.", nodename)));
		}
	}

	agentPort = get_agentPort_from_hostoid(nodeinfo.nodehost);
	user = get_hostuser_from_hostoid(nodeinfo.nodehost);
	address = get_hostaddress_from_hostoid(nodeinfo.nodehost);

	initStringInfo(&restmsg);
	while (max_try-- >= 0)
	{
		resetStringInfo(&restmsg);
		monitor_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES, agentPort, qrySql, user, address, nodeinfo.nodeport, DEFAULT_DB, &restmsg);
		if (strlen(restmsg.data) == 0)
		{
			ereport(LOG, (errmsg("CreateReplicationSlot datanode(%s) create slot_name(%s).", nodename, slot_name)));
			resetStringInfo(&restmsg);
			monitor_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES, agentPort, createSql, user, address, nodeinfo.nodeport, DEFAULT_DB, &restmsg);
			if (restmsg.data)
				break;
		}
		else
		{
			ereport(LOG, (errmsg("CreateReplicationSlot datanode(%s) has exist slot_name(%s).", nodename, slot_name)));
			break;
		}
	}
	MgrFree(restmsg.data);
}
static void DeleteReplicationSlot(char *nodename, char *slot_name)
{
	char *deleteSql;
	bool is_exist, is_running;
	AppendNodeInfo nodeinfo;
	char *address;
	char *user;
	int agentPort;
	int max_try = 3;
	StringInfoData restmsg;

	deleteSql = psprintf("select pg_drop_replication_slot('%s')", slot_name);

	memset(&nodeinfo, 0, sizeof(AppendNodeInfo));
	mgr_get_nodeinfo_byname_type(nodename, CNDN_TYPE_DATANODE_MASTER, false,
								 &is_exist, &is_running, &nodeinfo);
	if (nodeinfo.nodehost == 0)
	{
		mgr_get_nodeinfo_byname_type(nodename, CNDN_TYPE_DATANODE_SLAVE, false,
									 &is_exist, &is_running, &nodeinfo);
		if (nodeinfo.nodehost == 0)
		{
			ereport(ERROR,
					(errmsg("datanode %s is not exist.", nodename)));
		}
	}

	agentPort = get_agentPort_from_hostoid(nodeinfo.nodehost);
	user = get_hostuser_from_hostoid(nodeinfo.nodehost);
	address = get_hostaddress_from_hostoid(nodeinfo.nodehost);

	initStringInfo(&restmsg);
	while (max_try-- >= 0)
	{
		ereport(LOG, (errmsg("DeleteReplicationSlot datanode(%s) delete slot_name(%s).", nodename, slot_name)));
		monitor_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES, agentPort, deleteSql, user, address, nodeinfo.nodeport, DEFAULT_DB, &restmsg);
		if (restmsg.data)
			break;
	}
	MgrFree(restmsg.data);
}
void dn_master_replication_slot(char *nodename, char *slot_name, char operate)
{
	if (operate == 'c')
	{
		CreateReplicationSlot(nodename, slot_name);
	}
	else if (operate == 'd')
	{
		DeleteReplicationSlot(nodename, slot_name);
	}
}
void setSlaveNodeRecoveryConf(MgrNodeWrapper *masterNode,
							  MgrNodeWrapper *slaveNode)
{
	PGConfParameterItem *items;
	char *primary_conninfo_value;

	items = newPGConfParameterItem("recovery_target_timeline", "latest", false);

	primary_conninfo_value = psprintf("host=%s port=%d user=%s application_name=%s",
									masterNode->host->hostaddr,
									masterNode->form.nodeport,
									NameStr(slaveNode->host->form.hostuser),
									NameStr(slaveNode->form.nodename));
	items->next = newPGConfParameterItem("primary_conninfo",
											primary_conninfo_value, true);
	/* if node is datanode slave , then update primary_slot_name in recovery.conf*/ 
	if (slaveNode->form.nodetype == CNDN_TYPE_DATANODE_SLAVE || 
		(slaveNode->form.nodetype == CNDN_TYPE_DATANODE_MASTER && 
		strcmp(NameStr(slaveNode->form.curestatus),"switching") == 0))
	{
		dn_master_replication_slot(NameStr(masterNode->form.nodename),NameStr(slaveNode->form.nodename),'c');
		items->next->next = newPGConfParameterItem("primary_slot_name",
											NameStr(slaveNode->form.nodename), false);
	}			
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
	for (seconds = 0; seconds <= waitSeconds; seconds++)
	{
		if (pingNodeDefaultDB(node, 10) == expectedPGPing)
		{
			return true;
		}
		else
		{
			if (seconds < waitSeconds)
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
							  bool waitRecovery,
							  bool complain)
{
	callAgentStartNode(mgrNode, false, false);

	if (pingNodeWaitinSeconds(mgrNode, PQPING_OK, waitSeconds))
	{
		return true;
	}
	else
	{
		if (waitRecovery && waitForNodeMayBeInRecovery(mgrNode))
			return true;

		ereport(complain ? ERROR : LOG,
				(errmsg("try start up node %s failed, ping failed, it may be dead",
						NameStr(mgrNode->form.nodename))));
		return false;
	}
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

	dlist_foreach_modify(iter, nodes)
	{
		node = dlist_container(MgrNodeWrapper, link, iter.cur);
		copyOfNode = palloc(sizeof(MgrNodeWrapper));
		memcpy(copyOfNode, node, sizeof(MgrNodeWrapper));
		dlist_push_tail(failedNodes, &copyOfNode->link);
	}
	for (seconds = 0; seconds <= waitSeconds; seconds++)
	{
		dlist_foreach_modify(iter, failedNodes)
		{
			node = dlist_container(MgrNodeWrapper, link, iter.cur);
			if (pingNodeDefaultDB(node, 10) == expectedPGPing)
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
			if (seconds < waitSeconds)
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
				(errmsg("try start up node %s failed, it maybe in recovery.",
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

bool dropNodeFromPgxcNode(PGconn *activeConn,
						  char *executeOnNodeName,
						  bool localExecute,
						  char *nodeName,
						  bool complain)
{
	char *sql;
	bool execOk;

	if (localExecute)
		sql = psprintf("drop node \"%s\";",
					   nodeName);
	else
		sql = psprintf("drop node \"%s\" on (\"%s\");",
					   nodeName,
					   executeOnNodeName);
	execOk = PQexecCommandSql(activeConn, sql, false);
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

bool createNodeOnPgxcNode(PGconn *activeConn,
						  char *executeOnNodeName,
						  bool localExecute,
						  MgrNodeWrapper *mgrNode,
						  char *masterNodeName,
						  bool complain)
{
	StringInfoData sql;
	bool execOk;
	char *type;
	bool is_gtm = false;

	if (mgrNode->form.nodetype == CNDN_TYPE_COORDINATOR_MASTER)
	{
		type = "coordinator";
	}
	else if (mgrNode->form.nodetype == CNDN_TYPE_GTM_COOR_MASTER)
	{
		type = "coordinator";
		is_gtm = true;
	}
	else if (mgrNode->form.nodetype == CNDN_TYPE_DATANODE_MASTER)
	{
		type = "datanode";
	}
	else if (mgrNode->form.nodetype == CNDN_TYPE_DATANODE_SLAVE)
	{
		type = "datanode slave";
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
	initStringInfo(&sql);
	appendStringInfo(&sql, "CREATE NODE \"%s\" ", NameStr(mgrNode->form.nodename));
	if (masterNodeName)
		appendStringInfo(&sql, "FOR \"%s\" ", masterNodeName);
	appendStringInfo(&sql, "with (TYPE='%s', HOST='%s', PORT=%d, GTM=%d) ",
					 type,
					 mgrNode->host->hostaddr,
					 mgrNode->form.nodeport,
					 is_gtm);
	if (!localExecute)
	{
		Assert(executeOnNodeName);
		appendStringInfo(&sql, "on (\"%s\") ",
						 executeOnNodeName);
	}
	appendStringInfo(&sql, "; ");
	execOk = PQexecCommandSql(activeConn, sql.data, complain);
	pfree(sql.data);
	return execOk;
}

bool alterNodeOnPgxcNode(PGconn *activeConn,
						 char *executeOnNodeName,
						 bool localExecute,
						 char *oldNodeName,
						 MgrNodeWrapper *newNode,
						 bool complain)
{
	StringInfoData sql;
	bool execOk;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "alter node \"%s\" with(",
					 oldNodeName);
	appendStringInfo(&sql,
					 "name='%s',",
					 NameStr(newNode->form.nodename));
	appendStringInfo(&sql,
					 "host='%s', port=%d) ",
					 newNode->host->hostaddr,
					 newNode->form.nodeport);
	if (!localExecute)
	{
		Assert(executeOnNodeName);
		appendStringInfo(&sql,
						 "on (\"%s\") ",
						 executeOnNodeName);
	}
	execOk = PQexecCommandSql(activeConn, sql.data, complain);
	pfree(sql.data);
	return execOk;
}

bool nodenameExistsInPgxcNode(PGconn *activeConn,
							  char *executeOnNodeName,
							  bool localExecute,
							  char *nodeName,
							  char pgxcNodeType,
							  bool complain)
{
	StringInfoData sql;
	bool exists;

	initStringInfo(&sql);
	if (localExecute)
	{
		appendStringInfo(&sql,
						 "select count(*) from pgxc_node "
						 "where node_name = '%s' ",
						 nodeName);
		if (pgxcNodeType > 0 && pgxcNodeType != PGXC_NODE_NONE)
		{
			appendStringInfo(&sql,
							 "and node_type = '%c' ",
							 pgxcNodeType);
		}
	}
	else
	{
		appendStringInfo(&sql,
						 "EXECUTE DIRECT ON (\"%s\") "
						 "'select count(*) from pgxc_node "
						 "where node_name = ''%s'' ",
						 executeOnNodeName,
						 nodeName);
		if (pgxcNodeType > 0 && pgxcNodeType != PGXC_NODE_NONE)
		{
			appendStringInfo(&sql,
							 "and node_type = ''%c'' ",
							 pgxcNodeType);
		}
		appendStringInfo(&sql, " ;'");
	}
	exists = PQexecCountSql(activeConn, sql.data, complain) > 0;
	pfree(sql.data);
	return exists;
}

bool isMgrModeExistsInCoordinator(MgrNodeWrapper *coordinator,
								  PGconn *coordConn,
								  bool localExecute,
								  MgrNodeWrapper *mgrNode,
								  bool complain)
{
	StringInfoData sql;
	bool exists;
	char pgxcNodeType;

	initStringInfo(&sql);
	pgxcNodeType = getMappedPgxcNodetype(mgrNode->form.nodetype);
	if (localExecute)
	{
		appendStringInfo(&sql,
						 "select count(*) from pgxc_node "
						 "where node_port = %d ",
						 mgrNode->form.nodeport);
		appendStringInfo(&sql,
						 "and node_host = '%s' ",
						 mgrNode->host->hostaddr);
		/* all gtmcoord have the same name */
		if (!isGtmCoordMgrNode(mgrNode->form.nodetype))
		{
			appendStringInfo(&sql,
							 "and node_name = '%s' ",
							 NameStr(mgrNode->form.nodename));
		}
		if (pgxcNodeType > 0 && pgxcNodeType != PGXC_NODE_NONE)
		{
			appendStringInfo(&sql,
							 "and node_type = '%c' ",
							 pgxcNodeType);
		}
		appendStringInfo(&sql, ";");
	}
	else
	{
		appendStringInfo(&sql,
						 "EXECUTE DIRECT ON (\"%s\") "
						 "'select count(*) from pgxc_node "
						 "where node_port = %d ",
						 NameStr(coordinator->form.nodename),
						 mgrNode->form.nodeport);
		appendStringInfo(&sql,
						 "and node_host = ''%s''",
						 mgrNode->host->hostaddr);
		/* all gtmcoord have the same name */
		if (!isGtmCoordMgrNode(mgrNode->form.nodetype))
		{
			appendStringInfo(&sql,
							 "and node_name = ''%s'' ",
							 NameStr(mgrNode->form.nodename));
		}
		if (pgxcNodeType > 0 && pgxcNodeType != PGXC_NODE_NONE)
		{
			appendStringInfo(&sql,
							 "and node_type = ''%c'' ",
							 pgxcNodeType);
		}
		appendStringInfo(&sql, " ;'");
	}
	exists = PQexecCountSql(coordConn, sql.data, complain) > 0;
	pfree(sql.data);
	return exists;
}

char getMappedPgxcNodetype(char mgrNodetype)
{
	if (mgrNodetype == CNDN_TYPE_DATANODE_MASTER)
	{
		return PGXC_NODE_DATANODE;
	}
	else if (mgrNodetype == CNDN_TYPE_DATANODE_SLAVE)
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

bool isNodeInSyncStandbyNames(MgrNodeWrapper *masterNode,
							  MgrNodeWrapper *slaveNode,
							  PGconn *masterConn)
{
	char *oldSyncNames = NULL;
	char *syncNodes = NULL;
	char *buf = NULL;
	char *temp = NULL;

	temp = showNodeParameter(NameStr(masterNode->form.nodename), masterConn,
							 "synchronous_standby_names", true);
	ereport(DEBUG1,
			(errmsg("%s synchronous_standby_names is %s",
					NameStr(masterNode->form.nodename),
					temp)));
	oldSyncNames = trimString(temp);
	pfree(temp);
	temp = NULL;
	if (oldSyncNames == NULL || strlen(oldSyncNames) == 0)
	{
		return false;
	}
	buf = palloc0(strlen(oldSyncNames) + 1);
	/* "FIRST 1 (nodename2,nodename4)" will get result nodename2,nodename4 */
	sscanf(oldSyncNames, "%*[^(](%[^)]", buf);
	syncNodes = trimString(buf);
	pfree(buf);
	if (syncNodes == NULL || strlen(syncNodes) == 0)
	{
		return false;
	}
	temp = strtok(syncNodes, ",");
	while (temp)
	{
		if (equalsAfterTrim(temp, NameStr(slaveNode->form.nodename)))
		{
			return true;
		}
		temp = strtok(NULL, ",");
	}
	return false;
}

void cleanMgrNodesOnCoordinator(dlist_head *mgrNodes,
								MgrNodeWrapper *coordinator,
								PGconn *coordConn,
								bool complain)
{
	dlist_mutable_iter iter;
	MgrNodeWrapper *mgrNode;

	dlist_foreach_modify(iter, mgrNodes)
	{
		mgrNode = dlist_container(MgrNodeWrapper, link, iter.cur);
		compareAndDropMgrNodeOnCoordinator(mgrNode,
										   coordinator,
										   coordConn,
										   true,
										   complain);
		exec_pgxc_pool_reload(coordConn,
							  true,
							  NameStr(coordinator->form.nodename),
							  complain);
	}
}

void compareAndDropMgrNodeOnCoordinator(MgrNodeWrapper *mgrNode,
										MgrNodeWrapper *coordinator,
										PGconn *coordConn,
										bool localExecute,
										bool complain)
{
	if (mgrNode->form.nodetype == CNDN_TYPE_GTM_COOR_MASTER ||
		mgrNode->form.nodetype == CNDN_TYPE_DATANODE_MASTER)
	{
		ereport(complain ? ERROR : WARNING,
				(errmsg("%s is a datanode master, can not drop it from coordinator %s",
						NameStr(mgrNode->form.nodename),
						NameStr(coordinator->form.nodename))));
	}
	else
	{
		if (isMgrModeExistsInCoordinator(coordinator,
										 coordConn,
										 localExecute,
										 mgrNode,
										 complain))
		{
			ereport(LOG,
					(errmsg("clean node %s in table pgxc_node of %s begin",
							NameStr(mgrNode->form.nodename),
							NameStr(coordinator->form.nodename))));
			dropNodeFromPgxcNode(coordConn,
								 NameStr(coordinator->form.nodename),
								 localExecute,
								 NameStr(mgrNode->form.nodename),
								 complain);
			ereport(LOG,
					(errmsg("clean node %s in table pgxc_node of %s successed",
							NameStr(mgrNode->form.nodename),
							NameStr(coordinator->form.nodename))));
		}
		else
		{
			ereport(LOG,
					(errmsg("%s not exist in table pgxc_node of %s, skip",
							NameStr(mgrNode->form.nodename),
							NameStr(coordinator->form.nodename))));
		}
	}
}

bool isGtmCoordMgrNode(char nodetype)
{
	return nodetype == CNDN_TYPE_GTM_COOR_MASTER ||
		   nodetype == CNDN_TYPE_GTM_COOR_SLAVE;
}

bool isDataNodeMgrNode(char nodetype)
{
	return nodetype == CNDN_TYPE_DATANODE_MASTER ||
		   nodetype == CNDN_TYPE_DATANODE_SLAVE;
}

bool isCoordinatorMgrNode(char nodetype)
{
	return nodetype == CNDN_TYPE_COORDINATOR_MASTER ||
		   nodetype == CNDN_TYPE_COORDINATOR_SLAVE;
}

bool is_equal_string(char *a, char *b)
{
	return (a != NULL && b != NULL) ? (strcmp(a, b) == 0) : (a == b);
}

bool string_list_contain(const List *list, char *str)
{
	ListCell *cell;

	foreach (cell, list)
	{
		if (is_equal_string((char *)lfirst(cell), str))
			return true;
	}
	return false;
}

List *string_list_delete(List *list, char *str, bool deep)
{
	ListCell *cell;

	foreach (cell, list)
	{
		if (is_equal_string((char *)lfirst(cell), str))
		{
			if (deep)
				pfree(lfirst(cell));
			return list_delete_cell(list, cell);
		}
	}
	return list;
}

bool isSameNodeZone(MgrNodeWrapper *mgrNode1, MgrNodeWrapper *mgrNode2)
{
	return is_equal_string(NameStr(mgrNode1->form.nodezone),
						   NameStr(mgrNode2->form.nodezone));
}

bool isSameNodeName(MgrNodeWrapper *mgrNode1, MgrNodeWrapper *mgrNode2)
{
	return is_equal_string(NameStr(mgrNode1->form.nodename),
						   NameStr(mgrNode2->form.nodename));
}

SynchronousStandbyNamesConfig *parseSynchronousStandbyNamesConfig(char *synchronous_standby_names,
																  bool complain)
{
	SynchronousStandbyNamesConfig *synchronousStandbyNamesConfig = NULL;
	ErrorData *edata = NULL;
	SyncRepConfigData *syncrep_parse_result_save = syncrep_parse_result;
	char *syncrep_parse_error_msg_save = syncrep_parse_error_msg;
	int i;
	char *standby_name;
	MemoryContext oldContext;
	MemoryContext tempContext;

	oldContext = CurrentMemoryContext;
	tempContext = AllocSetContextCreate(oldContext,
										"tempContext",
										ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(tempContext);

	PG_TRY();
	{
		if (synchronous_standby_names != NULL &&
			synchronous_standby_names[0] != '\0')
		{
			int parse_rc;
			/* Reset communication variables to ensure a fresh start */
			syncrep_parse_result = NULL;
			syncrep_parse_error_msg = NULL;

			/* Parse the synchronous_standby_names string */
			syncrep_scanner_init(synchronous_standby_names);
			parse_rc = syncrep_yyparse();
			syncrep_scanner_finish();

			if (parse_rc != 0 || syncrep_parse_result == NULL)
			{
				ereport(complain ? ERROR : WARNING,
						(errmsg("%s",
								(syncrep_parse_error_msg == NULL)
									? "synchronous_standby_names parser failed"
									: syncrep_parse_error_msg)));
			}
			else
			{
				if (syncrep_parse_result->num_sync < 0)
				{
					ereport(complain ? ERROR : WARNING,
							(errmsg("number of synchronous standbys (%d) must not be negtive",
									syncrep_parse_result->num_sync)));
				}
				else
				{
					if (syncrep_parse_result->syncrep_method == SYNC_REP_PRIORITY)
					{
						(void)MemoryContextSwitchTo(oldContext);

						synchronousStandbyNamesConfig = palloc0(sizeof(SynchronousStandbyNamesConfig));
						synchronousStandbyNamesConfig->num_sync = syncrep_parse_result->num_sync;
						synchronousStandbyNamesConfig->syncrep_method = syncrep_parse_result->syncrep_method;
						standby_name = syncrep_parse_result->member_names;
						for (i = 1; i <= syncrep_parse_result->nmembers; i++)
						{
							if (i <= synchronousStandbyNamesConfig->num_sync)
								synchronousStandbyNamesConfig->syncStandbyNames =
									lappend(synchronousStandbyNamesConfig->syncStandbyNames,
											psprintf("%s", standby_name));
							else
								synchronousStandbyNamesConfig->potentialStandbyNames =
									lappend(synchronousStandbyNamesConfig->potentialStandbyNames,
											psprintf("%s", standby_name));
							standby_name += strlen(standby_name) + 1;
						}

						(void)MemoryContextSwitchTo(tempContext);
					}
					else
					{
						ereport(WARNING,
								(errmsg("Currently only supports streaming replication mechanism based on priority.")));
					}
				}
			}
		}
	}
	PG_CATCH();
	{
		/* Save error info in our stmt_mcontext */
		MemoryContextSwitchTo(oldContext);
		edata = CopyErrorData();
		FlushErrorState();
	}
	PG_END_TRY();

	syncrep_parse_result = syncrep_parse_result_save;
	syncrep_parse_error_msg = syncrep_parse_error_msg_save;

	(void)MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(tempContext);

	if (edata)
	{
		if (synchronousStandbyNamesConfig)
		{
			pfree(synchronousStandbyNamesConfig);
			synchronousStandbyNamesConfig = NULL;
		}
		ReThrowError(edata);
	}

	return synchronousStandbyNamesConfig;
}

char *transformSynchronousStandbyNamesConfig(List *syncStandbyNames,
											 List *potentialStandbyNames)
{
	int nSyncs;
	char *synchronous_standby_names;
	char *tempStr;
	ListCell *cell;

	nSyncs = 0;
	synchronous_standby_names = NULL;
	if (list_length(syncStandbyNames) > 0)
	{
		nSyncs = list_length(syncStandbyNames);
		foreach (cell, syncStandbyNames)
		{
			if (synchronous_standby_names)
			{
				tempStr = psprintf("%s,%s", synchronous_standby_names, (char *)lfirst(cell));
				pfree(synchronous_standby_names);
				synchronous_standby_names = tempStr;
			}
			else
				synchronous_standby_names = psprintf("%s", (char *)lfirst(cell));
		}
	}
	if (list_length(potentialStandbyNames) > 0)
	{
		if (nSyncs < 1)
			nSyncs = 1;
		foreach (cell, potentialStandbyNames)
		{
			if (synchronous_standby_names)
			{
				tempStr = psprintf("%s,%s", synchronous_standby_names, (char *)lfirst(cell));
				pfree(synchronous_standby_names);
				synchronous_standby_names = tempStr;
			}
			else
				synchronous_standby_names = psprintf("%s", (char *)lfirst(cell));
		}
	}

	if (synchronous_standby_names)
	{
		tempStr = psprintf("FIRST %d (%s)", nSyncs, synchronous_standby_names);
		pfree(synchronous_standby_names);
		synchronous_standby_names = tempStr;
	}
	return synchronous_standby_names;
}

static void configureStandbyNames(List **syncStandbyNamesP,
								  List **potentialStandbyNamesP,
								  MgrNodeWrapper *slaveNode,
								  List *allNodesInRepGroup,
								  List *nodeSyncConfigInRepGroup,
								  bool append)
{
	int zoneExpectedSyncNodes = 0;
	int zoneActualSyncNodes = 0;
	ListCell *cell1;
	ListCell *cell2;
	List *syncStandbyNames = *syncStandbyNamesP;
	List *potentialStandbyNames = *potentialStandbyNamesP;
	MgrNodeWrapper *tempNode;

	if (slaveNode == NULL)
		return;

	syncStandbyNames = string_list_delete(syncStandbyNames,
										  NameStr(slaveNode->form.nodename),
										  true);
	potentialStandbyNames = string_list_delete(potentialStandbyNames,
											   NameStr(slaveNode->form.nodename),
											   true);

	foreach (cell2, nodeSyncConfigInRepGroup)
	{
		MgrNodeSyncConfig *mgrNodeSyncConfig = (MgrNodeSyncConfig *)lfirst(cell2);
		if (is_equal_string(NameStr(mgrNodeSyncConfig->nodezone),
							NameStr(slaveNode->form.nodezone)))
		{
			zoneExpectedSyncNodes = mgrNodeSyncConfig->expectedSyncNodes;
			break;
		}
	}

	if (zoneExpectedSyncNodes <= 0)
		goto _the_end;

	foreach (cell2, syncStandbyNames)
	{
		char *nodename = (char *)lfirst(cell2);
		foreach (cell1, allNodesInRepGroup)
		{
			tempNode = (MgrNodeWrapper *)lfirst(cell1);
			if (!is_equal_string(nodename, NameStr(tempNode->form.nodename)))
				continue;

			if (is_equal_string(NameStr(slaveNode->form.nodezone),
								NameStr(tempNode->form.nodezone)))
				zoneActualSyncNodes++;
		}
	}
	if (append)
	{
		if (zoneExpectedSyncNodes > zoneActualSyncNodes)
			syncStandbyNames = lappend(syncStandbyNames, psprintf("%s", NameStr(slaveNode->form.nodename)));
		else
			potentialStandbyNames = lappend(potentialStandbyNames, psprintf("%s", NameStr(slaveNode->form.nodename)));
	}
	else
	{
		/* pick one from potential to sync */
		foreach (cell1, allNodesInRepGroup)
		{
			tempNode = (MgrNodeWrapper *)lfirst(cell1);
			if (is_equal_string(NameStr(slaveNode->form.nodename),
								NameStr(tempNode->form.nodename)))
				continue;
			if (!is_equal_string(NameStr(slaveNode->form.nodezone),
								 NameStr(tempNode->form.nodezone)))
				continue;
			if (!string_list_contain(potentialStandbyNames,
									 NameStr(tempNode->form.nodename)))
				continue;

			potentialStandbyNames = string_list_delete(potentialStandbyNames,
													   NameStr(tempNode->form.nodename),
													   true);
			syncStandbyNames = lappend(syncStandbyNames, psprintf("%s", NameStr(tempNode->form.nodename)));
			break;
		}
	}

_the_end:
	*syncStandbyNamesP = syncStandbyNames;
	*potentialStandbyNamesP = potentialStandbyNames;
}

static void storeNodeSyncToMgrNode(MgrNodeWrapper *masterNode,
								   List *allNodesInRepGroup,
								   List *syncStandbyNames,
								   List *potentialStandbyNames,
								   List *nodeSyncConfigInRepGroup,
								   MemoryContext spiContext)
{
	ListCell *cell1;
	ListCell *cell2;
	MgrNodeSyncConfig *mgrNodeSyncConfig;
	MgrNodeWrapper *mgrNode;
	NameData newNodesync;

	/* change the 'node sync' configuration of MGR_NODE */
	foreach (cell1, allNodesInRepGroup)
	{
		mgrNode = (MgrNodeWrapper *)lfirst(cell1);
		if (is_equal_string(NameStr(masterNode->form.nodename), NameStr(mgrNode->form.nodename)))
		{
			namestrcpy(&newNodesync, "");
		}
		else if (string_list_contain(syncStandbyNames, NameStr(mgrNode->form.nodename)))
		{
			foreach (cell2, nodeSyncConfigInRepGroup)
			{
				mgrNodeSyncConfig = (MgrNodeSyncConfig *)lfirst(cell2);
				if (is_equal_string(NameStr(mgrNodeSyncConfig->nodezone),
									NameStr(mgrNode->form.nodezone)))
				{
					mgrNodeSyncConfig->actualSyncNodes++;
					break;
				}
			}
			namestrcpy(&newNodesync, getMgrNodeSyncStateValue(SYNC_STATE_SYNC));
		}
		else if (string_list_contain(potentialStandbyNames, NameStr(mgrNode->form.nodename)))
		{
			namestrcpy(&newNodesync, getMgrNodeSyncStateValue(SYNC_STATE_POTENTIAL));
		}
		else
		{
			namestrcpy(&newNodesync, getMgrNodeSyncStateValue(SYNC_STATE_ASYNC));
		}

		/* Store 'node sync' configuration to MGR_NODE if changed */
		if (is_equal_string(NameStr(mgrNode->form.nodesync), NameStr(newNodesync)))
			continue;
		if (updateMgrNodeNodesync(mgrNode, NameStr(newNodesync), spiContext) == 1)
			mgrNode->form.nodesync = newNodesync;
		else
			ereport(ERROR,
					(errmsg("%s try to change nodesync from '%s' to '%s' failed",
							NameStr(mgrNode->form.nodename),
							NameStr(mgrNode->form.nodesync),
							NameStr(newNodesync))));
	}

	/* Check whether some zones possible loss the 'node sync' configuration */
	foreach (cell2, nodeSyncConfigInRepGroup)
	{
		int diff;
		mgrNodeSyncConfig = (MgrNodeSyncConfig *)lfirst(cell2);
		diff = mgrNodeSyncConfig->expectedSyncNodes - mgrNodeSyncConfig->actualSyncNodes;
		if (diff <= 0)
			continue;
		foreach (cell1, allNodesInRepGroup)
		{
			mgrNode = (MgrNodeWrapper *)lfirst(cell1);
			if (!is_equal_string(NameStr(mgrNodeSyncConfig->nodezone),
								 NameStr(mgrNode->form.nodezone)))
				continue;
			if (is_equal_string(NameStr(masterNode->form.nodename),
								NameStr(mgrNode->form.nodename)))
				continue;
			if (is_equal_string(getMgrNodeSyncStateValue(SYNC_STATE_SYNC),
								NameStr(mgrNode->form.nodesync)))
				continue;

			/* preserve 'node sync' configuration to sync in MGR_NODE */
			namestrcpy(&newNodesync, getMgrNodeSyncStateValue(SYNC_STATE_SYNC));
			/* Store 'node sync' configuration to MGR_NODE if changed */
			if (is_equal_string(NameStr(mgrNode->form.nodesync), NameStr(newNodesync)))
				continue;
			if (updateMgrNodeNodesync(mgrNode, NameStr(newNodesync), spiContext) == 1)
				mgrNode->form.nodesync = newNodesync;
			else
				ereport(ERROR,
						(errmsg("%s try to change nodesync from '%s' to '%s' failed",
								NameStr(mgrNode->form.nodename),
								NameStr(mgrNode->form.nodesync),
								NameStr(newNodesync))));
			--diff;
			if (diff <= 0)
				break;
		}
	}
}

void adjustStreamReplication(MgrNodeWrapper *masterNode,
							 MgrNodeWrapper *slaveNode,
							 PGconn *masterPGconn,
							 MemoryContext spiContext,
							 bool append)
{
	char *oldSyncConfigStr = NULL;
	char *newSyncConfigStr = NULL;
	SynchronousStandbyNamesConfig *synchronousStandbyNamesConfig = NULL;
	List *syncStandbyNames = NIL;
	List *potentialStandbyNames = NIL;
	List *nodeSyncConfigInRepGroup = NIL;
	List *allNodesInRepGroup = NIL;

	nodeSyncConfigInRepGroup = getNodeSyncConfigInRepGroup(masterNode);
	allNodesInRepGroup = selectAllNodesInRepGroup(masterNode, spiContext);

	oldSyncConfigStr = showNodeParameter(NameStr(masterNode->form.nodename), masterPGconn,
										 "synchronous_standby_names", true);
	synchronousStandbyNamesConfig =
		parseSynchronousStandbyNamesConfig(oldSyncConfigStr, true);
	if (synchronousStandbyNamesConfig)
	{
		syncStandbyNames = synchronousStandbyNamesConfig->syncStandbyNames;
		potentialStandbyNames = synchronousStandbyNamesConfig->potentialStandbyNames;
	}

	if (slaveNode != NULL)
		configureStandbyNames(&syncStandbyNames,
							  &potentialStandbyNames,
							  slaveNode,
							  allNodesInRepGroup,
							  nodeSyncConfigInRepGroup,
							  append);

	storeNodeSyncToMgrNode(masterNode,
						   allNodesInRepGroup,
						   syncStandbyNames,
						   potentialStandbyNames,
						   nodeSyncConfigInRepGroup,
						   spiContext);

	newSyncConfigStr = transformSynchronousStandbyNamesConfig(syncStandbyNames,
															  potentialStandbyNames);
	if (!equalsParameterValue(oldSyncConfigStr, newSyncConfigStr))
	{
		ereport(LOG,
				(errmsg("%s try to change synchronous_standby_names from '%s' to '%s'",
						NameStr(masterNode->form.nodename),
						oldSyncConfigStr,
						newSyncConfigStr)));
		setCheckSynchronousStandbyNames(masterNode,
										masterPGconn,
										newSyncConfigStr,
										CHECK_SYNC_STANDBY_NAMES_SECONDS);
	}

	if (oldSyncConfigStr)
		pfree(oldSyncConfigStr);
	if (newSyncConfigStr)
		pfree(newSyncConfigStr);
	if (syncStandbyNames)
		list_free_deep(syncStandbyNames);
	if (potentialStandbyNames)
		list_free_deep(potentialStandbyNames);
	if (synchronousStandbyNamesConfig)
		pfree(synchronousStandbyNamesConfig);
	if (nodeSyncConfigInRepGroup)
		list_free_deep(nodeSyncConfigInRepGroup);
	if (allNodesInRepGroup)
	{
		ListCell *cell;
		foreach (cell, allNodesInRepGroup)
		{
			MgrNodeWrapper *nodeToFree = (MgrNodeWrapper *)lfirst(cell);
			pfreeMgrNodeWrapper(nodeToFree);
		}
		list_free(allNodesInRepGroup);
	}
}

/**
 * This function may modify slaveNode's field form.nodesync.
 */
void appendToSyncStandbyNames(MgrNodeWrapper *masterNode,
							  MgrNodeWrapper *slaveNode,
							  PGconn *masterPGconn,
							  MemoryContext spiContext)
{
	adjustStreamReplication(masterNode,
							slaveNode,
							masterPGconn,
							spiContext,
							true);
}

/**
 * This function may modify MgrNodeWrapper's field form.nodesync.
 */
void removeFromSyncStandbyNames(MgrNodeWrapper *masterNode,
								MgrNodeWrapper *slaveNode,
								PGconn *masterPGconn,
								MemoryContext spiContext)
{
	adjustStreamReplication(masterNode,
							slaveNode,
							masterPGconn,
							spiContext,
							false);
}

bool setPGHbaTrustMyself(MgrNodeWrapper *mgrNode)
{
	NameData myAddress = {{0}};

	if (!mgr_get_self_address(mgrNode->host->hostaddr,
							  mgrNode->form.nodeport,
							  &myAddress))
	{
		ereport(LOG,
				(errmsg("on ADB Manager get local address fail, "
						"this may be caused by cannot get the "
						"connection to node %s",
						NameStr(mgrNode->form.nodename))));
		return false;
	}
	if (!setPGHbaTrustAddress(mgrNode, NameStr(myAddress)))
	{
		ereport(LOG,
				(errmsg("set node %s trust me failed, "
						"this may be caused by network error",
						NameStr(mgrNode->form.nodename))));
		return false;
	}
	return true;
}
void MgrGetOldDnMasterNotZone(MemoryContext spiContext,
							  char *currentZone,
							  char nodeType,
							  dlist_head *masterList,
							  char *overType)
{
	if (pg_strcasecmp(overType, OVERTYPE_SWITCHOVER) == 0)
	{
		selectNodeNotZone(spiContext,
						  currentZone,
						  nodeType,
						  masterList);
	}
	else
	{
		selectNodeNotZoneForFailover(spiContext,
									 currentZone,
									 nodeType,
									 masterList);
	}

	if (dlist_is_empty(masterList))
	{
		ereport(ERROR, (errmsg("no %s in other zone, current zone(%s).", mgr_get_nodetype_desc(nodeType), currentZone)));
	}
}

bool waitForNodeMayBeInRecovery(MgrNodeWrapper *mgrNode)
{
	PGconn *pgConn = NULL;
	bool printedMessage = false;
	NodeConnectionStatus connStatus;
	char msg[200];

	while (true)
	{
		connStatus = connectNodeDefaultDB(mgrNode, 10, &pgConn);

		PQfinish(pgConn);
		pgConn = NULL;

		if (connStatus == NODE_CONNECTION_STATUS_CANNOT_CONNECT_NOW)
		{
			if (!printedMessage)
			{
				snprintf(msg, 199, "%s may be in recovery, waiting......", NameStr(mgrNode->form.nodename));
				ereport(NOTICE,
						(errmsg("%s", msg)));
				ereport(LOG,
						(errmsg("%s", msg)));
				printedMessage = true;
			}
			pg_usleep(1000000L);
		}
		else if (connStatus == NODE_CONNECTION_STATUS_SUCCESS)
		{
			return true;
		}
		else
		{
			return false;
		}
	}
}

List *getNodeSyncConfigInRepGroup(MgrNodeWrapper *mgrNode)
{
	Relation rel;
	List *nodeSyncConfigInRepGroup = NIL;

	rel = table_open(NodeRelationId, AccessShareLock);
	PG_TRY();
	{
		TableScanDesc rel_scan;
		HeapTuple tuple;
		Form_mgr_node temp_mgr_node;
		Oid nodemasternameoid = InvalidOid;

		rel_scan = table_beginscan_catalog(rel, 0, NULL);
		while ((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
		{
			temp_mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			if (temp_mgr_node->oid == mgrNode->form.oid)
			{
				if (isMasterNode(temp_mgr_node->nodetype, true))
					nodemasternameoid = temp_mgr_node->oid;
				else
					nodemasternameoid = temp_mgr_node->nodemasternameoid;

				break;
			}
		}
		table_endscan(rel_scan);
		if (!OidIsValid(nodemasternameoid))
			ereport(ERROR,
					(errmsg("nodename:%s, can not get a valid master node oid",
							NameStr(mgrNode->form.nodename))));

		rel_scan = table_beginscan_catalog(rel, 0, NULL);
		while ((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
		{
			temp_mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			if ((temp_mgr_node->oid == nodemasternameoid ||
				 temp_mgr_node->nodemasternameoid == nodemasternameoid) &&
				is_equal_string(NameStr(temp_mgr_node->nodesync),
								getMgrNodeSyncStateValue(SYNC_STATE_SYNC)))
			{
				MgrNodeSyncConfig *mgrNodeSyncConfig;
				ListCell *cell;
				bool found = false;
				foreach (cell, nodeSyncConfigInRepGroup)
				{
					mgrNodeSyncConfig = (MgrNodeSyncConfig *)lfirst(cell);
					if (is_equal_string(NameStr(temp_mgr_node->nodezone),
										NameStr(mgrNodeSyncConfig->nodezone)))
					{
						found = true;
						mgrNodeSyncConfig->expectedSyncNodes++;
						break;
					}
				}
				if (!found)
				{
					mgrNodeSyncConfig = palloc(sizeof(MgrNodeSyncConfig));
					nodeSyncConfigInRepGroup = lappend(nodeSyncConfigInRepGroup, mgrNodeSyncConfig);
					mgrNodeSyncConfig->nodezone = temp_mgr_node->nodezone;
					mgrNodeSyncConfig->expectedSyncNodes = 1;
					mgrNodeSyncConfig->actualSyncNodes = 0;
				}
			}
		}
		table_endscan(rel_scan);
	}
	PG_CATCH();
	{
		table_close(rel, AccessShareLock);
		PG_RE_THROW();
	}
	PG_END_TRY();

	table_close(rel, AccessShareLock);

	return nodeSyncConfigInRepGroup;
}
void appendToSyncStandbyNamesForZone(MgrNodeWrapper *masterNode,
									MgrNodeWrapper *slaveNode,
									PGconn *masterPGconn,
									PGconn *slavePGconn,
									MemoryContext spiContext)
{
	UpdateSyncInfo(slaveNode,
					slavePGconn,
					NameStr(masterNode->form.nodesync),
					spiContext);	

	UpdateSyncInfo(masterNode,
					masterPGconn,
					"",
					spiContext);
}
void UpdateSyncInfo(MgrNodeWrapper *mgrNode, 
					PGconn *conn,
					char *newNodesync,
					MemoryContext spiContext)
{
	Assert(mgrNode);
	Assert(conn);

	if (updateMgrNodeNodesync(mgrNode, newNodesync, spiContext) == 1)
		namestrcpy(&mgrNode->form.nodesync, newNodesync);
	else
		ereport(ERROR,
				(errmsg("%s try to change nodesync to '%s' failed",
						NameStr(mgrNode->form.nodename),
						newNodesync)));
	UpdateSyncStandbyNames(mgrNode, conn);
}

static void UpdateSyncStandbyNames(MgrNodeWrapper *mgrNode,
									PGconn *conn)
{
	StringInfoData infosendmsg;

	initStringInfo(&infosendmsg);
	MgrGetSyncStandByName(mgrNode, &infosendmsg);

	setCheckSynchronousStandbyNames(mgrNode,
									conn,
									NameStr(infosendmsg),
									CHECK_SYNC_STANDBY_NAMES_SECONDS);
										
	MgrFree(infosendmsg.data);
}
static void MgrGetSyncStandByName(MgrNodeWrapper *node,
								 StringInfoData *infosendmsg)
{
	StringInfoData infosendsyncmsg;
	int  syncNum = 0;

	initStringInfo(&infosendsyncmsg);

	syncNum = mgr_get_master_sync_string(node->form.oid, true, node->form.oid, &infosendsyncmsg);
	if(infosendsyncmsg.len != 0)
	{
		if (syncNum == 0)
			syncNum = 1;
	
		appendStringInfo(infosendmsg, "%d (%s)", syncNum, infosendsyncmsg.data);
	}
}
void rewindMgrNodeOperation(RewindMgrNodeObject *rewindObject,
								   MemoryContext spiContext)
{
	MgrNodeWrapper *masterNode;
	MgrNodeWrapper *slaveNode;
	StringInfoData restmsg;
	StringInfoData infosendmsg;
	bool resA;
	bool resB;

	masterNode = rewindObject->masterNode;
	slaveNode = rewindObject->slaveNode;

	shutdownNodeWithinSeconds(slaveNode,
							  SHUTDOWN_NODE_SECONDS_ON_REWIND,
							  0, true);

	setPGHbaTrustAddress(masterNode, slaveNode->host->hostaddr);

	setPGHbaTrustSlaveReplication(masterNode, slaveNode, true);

	PQexecCommandSql(rewindObject->masterPGconn, "checkpoint;", true);

	initStringInfo(&restmsg);
	initStringInfo(&infosendmsg);
	appendStringInfo(&infosendmsg, "%s/bin/pg_controldata '%s' | grep 'Minimum recovery ending location:' |awk '{print $5}'",
					 masterNode->host->hostadbhome,
					 masterNode->nodepath);
	appendStringInfoCharMacro(&infosendmsg, '\0');
	resA = mgr_ma_send_cmd_get_original_result(AGT_CMD_GET_BATCH_JOB,
											   infosendmsg.data,
											   masterNode->form.nodehost,
											   &restmsg,
											   AGENT_RESULT_LOG);
	if (resA)
	{
		if (restmsg.len == 0)
			resA = false;
		else if (strcasecmp(restmsg.data, "{\"result\":\"0/0\"}") != 0)
			resA = false;
	}

	resetStringInfo(&restmsg);
	resetStringInfo(&infosendmsg);
	appendStringInfo(&infosendmsg, "%s/bin/pg_controldata '%s' |grep 'Min recovery ending loc' |awk '{print $6}'",
					 masterNode->host->hostadbhome,
					 masterNode->nodepath);
	appendStringInfoCharMacro(&infosendmsg, '\0');
	resB = mgr_ma_send_cmd_get_original_result(AGT_CMD_GET_BATCH_JOB,
											   infosendmsg.data,
											   masterNode->form.nodehost,
											   &restmsg,
											   AGENT_RESULT_LOG);
	if (resB)
	{
		if (restmsg.len == 0)
			resB = false;
		else if (strcasecmp(restmsg.data, "{\"result\":\"0\"}") != 0)
			resB = false;
	}
	pfree(restmsg.data);
	pfree(infosendmsg.data);

	if (!resA || !resB)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("on the master \"%s\" execute \"pg_controldata %s\" to get the expect value fail",
						NameStr(masterNode->form.nodename),
						masterNode->nodepath),
				 errhint("execute \"checkpoint\" on  master \"%s\", then execute  \"pg_controldata %s\" to check \"Minimum recovery \
					ending location\" is \"0/0\" and \"Min recovery ending loc's timeline\" is \"0\" before execute the rewind command again",
						 NameStr(masterNode->form.nodename),
						 masterNode->nodepath)));
	}

	/*node rewind*/
	callAgentRewindNode(masterNode, slaveNode, true);
	setSlaveNodeRecoveryConf(masterNode, slaveNode);
}
void checkSetMgrNodeGtmInfo(MgrNodeWrapper *mgrNode,
							PGconn *pgConn,
							MemoryContext spiContext)
{
	MgrNodeWrapper *gtmMaster = NULL;

	if (mgrNode->form.nodetype == CNDN_TYPE_GTM_COOR_SLAVE)
	{
		return;
	}
	gtmMaster = selectMgrGtmCoordNode(spiContext);
	if (!gtmMaster)
	{
		ereport(ERROR,
				(errmsg("There is no GTM master node in the cluster")));
	}
	setCheckGtmInfoInPGSqlConf(gtmMaster, mgrNode, pgConn, true, CHECK_GTM_INFO_SECONDS, true);
	if (gtmMaster)
		pfreeMgrNodeWrapper(gtmMaster);
}
PGconn *checkMasterRunningStatus(MgrNodeWrapper *masterNode)
{
	PGconn *conn;
	conn = getNodeDefaultDBConnection(masterNode, 10);
	if (!conn)
		ereport(ERROR,
				(errmsg("get node %s connection failed",
						NameStr(masterNode->form.nodename))));

	if (!checkNodeRunningMode(conn, true))
	{
		PQfinish(conn);
		ereport(ERROR,
				(errmsg("master node %s is not running on master mode"
						"suspend the operation of following master",
						NameStr(masterNode->form.nodename))));
	}
	return conn;
}
/**
 * pg_rewind requires that the target server either has the wal_log_hints 
 * option enabled in postgresql.conf or data checksums enabled when the 
 * cluster was initialized with initdb. Neither of these are currently 
 * on by default. full_page_writes must also be set to on, but is enabled 
 * by default. So we should set these values before rewind, and after rewind 
 * restore these value to the orginal values.
 */
bool checkSetRewindNodeParamter(MgrNodeWrapper *mgrNode, PGconn *conn)
{
	char *parameterName;
	char *expectValue;
	char *originalValue;
	PGConfParameterItem *expectItem = NULL;
	bool set = false;

	parameterName = "wal_log_hints";
	expectValue = "on";
	originalValue = showNodeParameter(NameStr(mgrNode->form.nodename), conn, parameterName, true);
	if (strcmp(originalValue, expectValue) == 0)
	{
		ereport(LOG, (errmsg("node %s parameter %s is %s, no need to set",
							 NameStr(mgrNode->form.nodename),
							 parameterName, expectValue)));
	}
	else
	{
		expectItem = newPGConfParameterItem(parameterName,
											expectValue, false);
		callAgentRefreshPGSqlConfReload(mgrNode, expectItem, true);
		pfreePGConfParameterItem(expectItem);
		set = true;
	}
	pfree(originalValue);

	parameterName = "full_page_writes";
	expectValue = "on";
	originalValue = showNodeParameter(NameStr(mgrNode->form.nodename), conn, parameterName, true);
	if (strcmp(originalValue, expectValue) == 0)
	{
		ereport(LOG, (errmsg("node %s parameter %s is %s, no need to set",
							 NameStr(mgrNode->form.nodename),
							 parameterName, expectValue)));
	}
	else
	{
		expectItem = newPGConfParameterItem(parameterName,
											expectValue, false);
		callAgentRefreshPGSqlConfReload(mgrNode, expectItem, true);
		pfreePGConfParameterItem(expectItem);
		set = true;
	}
	pfree(originalValue);
	return set;
}
void tryUpdateMgrNodeCurestatus(MgrNodeWrapper *mgrNode,
									   char *newCurestatus,
									   MemoryContext spiContext)
{
	int rows;

	rows = updateMgrNodeCurestatus(mgrNode, newCurestatus, spiContext);
	if (rows != 1)
		ereport(ERROR,
				(errmsg("%s, can not transit to curestatus:%s",
						NameStr(mgrNode->form.nodename),
						newCurestatus)));
	else
		namestrcpy(&mgrNode->form.curestatus, newCurestatus);
}
static void 
get_node_level_func(MemoryContext spiContext, 
					Oid		nodeOid,
					int 	*num)
{
	MgrNodeWrapper *mgrNode = NULL;
	if (nodeOid != 0)
	{
		(*num)++;
		mgrNode = selectMgrNodeByOid(nodeOid, spiContext);
		if (mgrNode != NULL)
			get_node_level_func(spiContext, mgrNode->form.nodemasternameoid, num);
	}
	return;
}
void
get_node_level(char 	*nodeName,
				char 	nodeType,
				int 	*num)
{
	int 			spiRes = 0;		
	MemoryContext 	oldContext;
	MemoryContext 	spiContext = NULL;
	MemoryContext   switchContext = NULL;
	MgrNodeWrapper *mgrNode = NULL;

	oldContext = CurrentMemoryContext;
	switchContext = AllocSetContextCreate(CurrentMemoryContext, "get_node_level", ALLOCSET_DEFAULT_SIZES);
	if ((spiRes = SPI_connect()) != SPI_OK_CONNECT){
		ereport(ERROR, (errmsg("SPI_connect failed, connect return:%d",	spiRes)));
	}
	spiContext = CurrentMemoryContext;
	MemoryContextSwitchTo(switchContext);

	PG_TRY();
	{
		mgrNode = selectMgrNodeByNodenameType(nodeName,
											nodeType,
											spiContext);
		Assert(mgrNode);
		get_node_level_func(spiContext, 
							mgrNode->form.oid,
							num);
	}PG_CATCH();
	{
		(void)MemoryContextSwitchTo(oldContext);
		MemoryContextDelete(switchContext);
		SPI_finish();
		PG_RE_THROW();
	}PG_END_TRY();

	(void)MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(switchContext);
	SPI_finish();
}
void warnning_node_by_level_syncstate(char 	*nodeName,
										char 	nodeType)
{
	int 			num = 0;
	int 			spiRes = 0;		
	MemoryContext 	oldContext;
	MemoryContext 	spiContext = NULL;
	MemoryContext   switchContext = NULL;
	MgrNodeWrapper  *mgrNode = NULL;

	if (!isSlaveNode(nodeType, true))
		return;

	oldContext = CurrentMemoryContext;
	switchContext = AllocSetContextCreate(CurrentMemoryContext, "warnning_node_by_level_syncstate", ALLOCSET_DEFAULT_SIZES);
	if ((spiRes = SPI_connect()) != SPI_OK_CONNECT){
		ereport(ERROR, (errmsg("SPI_connect failed, connect return:%d",	spiRes)));
	}
	spiContext = CurrentMemoryContext;
	MemoryContextSwitchTo(switchContext);

	PG_TRY();
	{
		mgrNode = selectMgrNodeByNodenameType(nodeName,
											nodeType,
											spiContext);
		Assert(mgrNode);
		get_node_level_func(spiContext, 
							mgrNode->form.oid,
							&num);
		if (num >=3 && 
			is_equal_string(getMgrNodeSyncStateValue(SYNC_STATE_SYNC), NameStr(mgrNode->form.nodesync)))
		{
			ereportWarningLog(errmsg("%s is on level %d, the sync_state is sync, but the actual replication state is asynchronous.",
				NameStr(mgrNode->form.nodename), num));
		}	
	}PG_CATCH();
	{
		(void)MemoryContextSwitchTo(oldContext);
		MemoryContextDelete(switchContext);
		SPI_finish();
		PG_RE_THROW();
	}PG_END_TRY();

	(void)MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(switchContext);
	SPI_finish();
}
void 
warnning_master_has_no_sync(char 	*nodeName,
							char 	nodeType)
{
	int 			spiRes = 0;		
	MemoryContext 	oldContext;
	MemoryContext 	spiContext = NULL;
	MemoryContext   switchContext = NULL;
	MgrNodeWrapper  *mgrNode = NULL;
	MgrNodeWrapper  *slaveNode = NULL;
	MgrNodeWrapper  *tmpNode = NULL;
	dlist_head 		nodes = DLIST_STATIC_INIT(nodes);
	dlist_iter 		iter;
	bool			hasSync = false;

	if (nodeType == CNDN_TYPE_COORDINATOR_MASTER || nodeType == CNDN_TYPE_COORDINATOR_SLAVE)
		return;

	oldContext = CurrentMemoryContext;
	switchContext = AllocSetContextCreate(CurrentMemoryContext, "warnning_master_has_no_sync", ALLOCSET_DEFAULT_SIZES);
	if ((spiRes = SPI_connect()) != SPI_OK_CONNECT){
		ereport(ERROR, (errmsg("SPI_connect failed, connect return:%d",	spiRes)));
	}
	spiContext = CurrentMemoryContext;
	MemoryContextSwitchTo(switchContext);

	PG_TRY();
	{
		if (nodeType == CNDN_TYPE_DATANODE_SLAVE || nodeType == CNDN_TYPE_GTM_COOR_SLAVE)
		{
			slaveNode = selectMgrNodeByNodenameType(nodeName,
												nodeType,
												spiContext);
			Assert(slaveNode);
			if (!is_equal_string(getMgrNodeSyncStateValue(SYNC_STATE_SYNC), NameStr(slaveNode->form.nodesync)))
			{			
				mgrNode = selectMgrNodeByOid(slaveNode->form.nodemasternameoid, spiContext);
				Assert(mgrNode);
			}			
		}
		else
		{
			mgrNode = selectMgrNodeByNodenameType(nodeName,
												nodeType,
												spiContext);
			Assert(mgrNode);
		}

		if (mgrNode != NULL && isMasterNode(mgrNode->form.nodetype, true))
		{
			selectMgrSlaveNodes(mgrNode->form.oid,
								getMgrSlaveNodetype(mgrNode->form.nodetype),
								spiContext,
								&nodes);
			dlist_foreach(iter, &nodes)
			{
				tmpNode = dlist_container(MgrNodeWrapper, link, iter.cur);
				if(is_equal_string(getMgrNodeSyncStateValue(SYNC_STATE_SYNC), NameStr(tmpNode->form.nodesync)))
				{
					hasSync = true;
					break;
				}				
			}
			if (!hasSync)
				ereportWarningLog(errmsg("%s is %s, it has no synchronous node.",
					NameStr(mgrNode->form.nodename),  mgr_get_nodetype_desc(mgrNode->form.nodetype)));
		}	
	}PG_CATCH();
	{
		(void)MemoryContextSwitchTo(oldContext);
		MemoryContextDelete(switchContext);
		SPI_finish();
		PG_RE_THROW();
	}PG_END_TRY();

	(void)MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(switchContext);
	SPI_finish();
}

void 
mgr_check_rewind_dir_exist(char *zone)
{
	MemoryContext 	oldContext;
	MemoryContext 	spiContext = NULL;
	MemoryContext   switchContext = NULL;
	int 			spiRes;
	dlist_head 		nodes = DLIST_STATIC_INIT(nodes);
	dlist_iter 		iter;
	MgrNodeWrapper 	*mgrNode = NULL;

	oldContext = CurrentMemoryContext;
	switchContext = AllocSetContextCreate(CurrentMemoryContext, "mgr_check_rewind_dir_exist", ALLOCSET_DEFAULT_SIZES);
	if ((spiRes = SPI_connect()) != SPI_OK_CONNECT){
		ereport(ERROR, (errmsg("SPI_connect failed, connect return:%d",	spiRes)));
	}
	spiContext = CurrentMemoryContext;
	MemoryContextSwitchTo(switchContext);
	PG_TRY();
	{
		selectNotActiveChildInZone(spiContext,
									zone,
									&nodes);
		dlist_foreach(iter, &nodes)
		{
			mgrNode = dlist_container(MgrNodeWrapper, link, iter.cur);
			Assert(mgrNode);
			mgr_check_rewind_dir_exist_and_priv(mgrNode->host->form.oid, mgrNode->nodepath);
		}
	}PG_CATCH();
	{
		(void)MemoryContextSwitchTo(oldContext);
		MemoryContextDelete(switchContext);
		SPI_finish();
		PG_RE_THROW();
	}PG_END_TRY();

	(void)MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(switchContext);
	SPI_finish();
}