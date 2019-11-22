/*-------------------------------------------------------------------------
 *
 * groupmgr.c
 *	  Routines to support manipulation of the pgxc_group catalog
 *	  This includes support for DDL on objects NODE GROUP
 *
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012, Postgres-XC Development Group
 * Portions Copyright (c) 2014-2017, ADB Development Group
 *
 * IDENTIFICATION
 * 		src/backend/pgxc/nodemgr/groupmgr.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_node.h"
#include "catalog/pgxc_group.h"
#include "nodes/plannodes.h"
#include "executor/execCluster.h"
#include "libpq/libpq-node.h"
#include "nodes/parsenodes.h"
#include "pgxc/groupmgr.h"
#include "pgxc/pgxc.h"
#include "storage/lwlock.h"
#include "storage/mem_toc.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/array.h"


#define REMOTE_KEY_CREATE_GROUP 1
#define REMOTE_KEY_REMOVE_GROUP 2

extern Oid PGXCNodeOid;
extern void *mem_toc_lookup(StringInfo buf, uint32 key, int *len);

static void PgxcGroupCreateLocal(CreateGroupStmt *stmt);
static void PgxcGroupRemoveLocal(DropGroupStmt *stmt);
static List *getCoordOid(bool *include_myself);

/*
 * PgxcGroupCreate
 *
 * Create a PGXC node group
 */
void
PgxcGroupCreate(CreateGroupStmt *stmt)
{
	List			*nodeOid_list = NIL;
	List			*remoteList = NIL;
	bool			include_myself = false;

	if (IsConnFromApp())
		nodeOid_list = getCoordOid(&include_myself);

	remoteList = NIL;
	if (nodeOid_list != NIL)
	{
		StringInfoData msg;
		initStringInfo(&msg);

		ClusterTocSetCustomFun(&msg, ClusterPgxcGroupCreate);

		begin_mem_toc_insert(&msg, REMOTE_KEY_CREATE_GROUP);
		saveNode(&msg, (Node*)stmt);
		end_mem_toc_insert(&msg, REMOTE_KEY_CREATE_GROUP);

		remoteList = ExecClusterCustomFunction(nodeOid_list, &msg, 0);
		pfree(msg.data);
	}

	/* exec local pgxc group */
	if (include_myself)
		PgxcGroupCreateLocal(stmt);

	if (remoteList)
	{
		PQNListExecFinish(remoteList, NULL, &PQNDefaultHookFunctions, true);
		list_free(remoteList);
	}
	list_free(nodeOid_list);
}

/* Create cluster nodeGroup */
void
ClusterPgxcGroupCreate(StringInfo mem_toc)
{
	CreateGroupStmt *stmt;
	StringInfoData buf;

	buf.data = mem_toc_lookup(mem_toc, REMOTE_KEY_CREATE_GROUP, &buf.maxlen);
	if (buf.data == NULL)
	{
		ereport(ERROR,
				(errmsg("Can not found CreateGroupStmt in cluster message"),
				 errcode(ERRCODE_PROTOCOL_VIOLATION)));
	}
	buf.len = buf.maxlen;
	buf.cursor = 0;

	stmt = castNode(CreateGroupStmt, loadNode(&buf));

	PgxcGroupCreateLocal(stmt);
}


/* Create local nodeGroup */
static void
PgxcGroupCreateLocal(CreateGroupStmt *stmt)
{
	const char *group_name = stmt->group_name;
	List	   *nodes = stmt->nodes;
	oidvector  *nodes_array;
	Oid		   *inTypes;
	Relation	rel;
	HeapTuple	tup;
	bool		nulls[Natts_pgxc_group];
	Datum		values[Natts_pgxc_group];
	int			member_count = list_length(stmt->nodes);
	ListCell   *lc;
	int			i = 0;

	/* Only a DB administrator can add cluster node groups */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to create cluster node groups")));

	/* Check if given group already exists */
	if (OidIsValid(get_pgxc_groupoid(group_name)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("PGXC Group %s: group already defined",
						group_name)));

	inTypes = (Oid *) palloc(member_count * sizeof(Oid));

	/* Build list of Oids for each node listed */
	foreach(lc, nodes)
	{
		char   *node_name = strVal(lfirst(lc));
		Oid	noid = get_pgxc_nodeoid(node_name);

		if (!OidIsValid(noid))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("PGXC Node %s: object not defined",
							node_name)));

		if (get_pgxc_nodetype(noid) != PGXC_NODE_DATANODE)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("PGXC node %s: only Datanodes can be group members",
							node_name)));

		/* OK to pick up Oid of this node */
		inTypes[i] = noid;
		i++;
	}

	/* Build array of Oids to be inserted */
	nodes_array = buildoidvector(inTypes, member_count);

	/* Iterate through all attributes initializing nulls and values */
	for (i = 0; i < Natts_pgxc_group; i++)
	{
		nulls[i]  = false;
		values[i] = (Datum) 0;
	}

	/* Insert Data correctly */
	values[Anum_pgxc_group_group_name - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(group_name));
	values[Anum_pgxc_group_group_members - 1] = PointerGetDatum(nodes_array);

	/* Open the relation for insertion */
	rel = heap_open(PgxcGroupRelationId, RowExclusiveLock);
	tup = heap_form_tuple(rel->rd_att, values, nulls);

	/* Do the insertion */
	(void)CatalogTupleInsert(rel, tup);

	heap_close(rel, RowExclusiveLock);
}

/*
 * PgxcNodeGroupsRemove():
 *
 * Remove a PGXC node group
 */
void
PgxcGroupRemove(DropGroupStmt *stmt)
{
	List			*nodeOid_list = NIL;
	List			*remoteList = NIL;
	bool			include_myself = false;

	if (IsConnFromApp())
		nodeOid_list = getCoordOid(&include_myself);

	if (nodeOid_list != NIL)
	{
		StringInfoData msg;
		initStringInfo(&msg);

		ClusterTocSetCustomFun(&msg, ClusterPgxcGroupRemove);

		begin_mem_toc_insert(&msg, REMOTE_KEY_REMOVE_GROUP);
		saveNode(&msg, (Node*)stmt);
		end_mem_toc_insert(&msg, REMOTE_KEY_REMOVE_GROUP);

		remoteList = ExecClusterCustomFunction(nodeOid_list, &msg, 0);
		pfree(msg.data);
	}

	/* exec local pgxc group */
	if (include_myself)
		PgxcGroupRemoveLocal(stmt);

	if (remoteList)
	{
		PQNListExecFinish(remoteList, NULL, &PQNDefaultHookFunctions, true);
		list_free(remoteList);
	}
	list_free(nodeOid_list);
}

void
ClusterPgxcGroupRemove(StringInfo mem_toc)
{
	DropGroupStmt *stmt;
	StringInfoData buf;

	buf.data = mem_toc_lookup(mem_toc, REMOTE_KEY_REMOVE_GROUP, &buf.maxlen);
	if (buf.data == NULL)
	{
		ereport(ERROR,
				(errmsg("Can not found CreateGroupStmt in cluster message"),
				 errcode(ERRCODE_PROTOCOL_VIOLATION)));
	}
	buf.len = buf.maxlen;
	buf.cursor = 0;

	stmt = castNode(DropGroupStmt, loadNode(&buf));

	PgxcGroupRemoveLocal(stmt);
}

static void
PgxcGroupRemoveLocal(DropGroupStmt *stmt)
{
	Relation	relation;
	HeapTuple	tup;
	const char *group_name = stmt->group_name;
	Oid			group_oid = get_pgxc_groupoid(group_name);
	Form_pgxc_group group_form;
	oidvector	   *group_members;
	int				i;

	/* Only a DB administrator can remove cluster node groups */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to remove cluster node groups")));

	/* Check if group exists */
	if (!OidIsValid(group_oid))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("PGXC Group %s: group not defined",
						group_name)));

	/* Delete the pgxc_group tuple */
	relation = heap_open(PgxcGroupRelationId, RowExclusiveLock);
	tup = SearchSysCache(PGXCGROUPOID, ObjectIdGetDatum(group_oid), 0, 0, 0);

	if (!HeapTupleIsValid(tup)) /* should not happen */
		elog(ERROR, "PGXC Group %s: group not defined", group_name);

	group_form = (Form_pgxc_group) GETSTRUCT(tup);
	group_members = &(group_form->group_members);
	for (i = 0; i < group_members->dim1; i++)
		PreventInterTransactionChain(group_members->values[i], "DROP GROUP");

	simple_heap_delete(relation, &tup->t_self);

	ReleaseSysCache(tup);

	heap_close(relation, RowExclusiveLock);
}

/* Get the nodeoid list without the current node
 * 'include_myself': Include this node or not
 */
static List*
getCoordOid(bool *include_myself)
{
	HeapTuple		tuple;
	ScanKeyData		skey[1];
	Relation		rel;
	HeapScanDesc	scan;
	Oid				nodeOid;
	List 			*nodeOid_list = NIL;
	bool			include_current_node = false;;

	rel = heap_open(PgxcNodeRelationId, AccessShareLock);
	ScanKeyInit(&skey[0],
				Anum_pgxc_node_node_type,
				BTEqualStrategyNumber, F_CHAREQ,
				CharGetDatum(PGXC_NODE_COORDINATOR));
	scan = heap_beginscan_catalog(rel, 1, skey);

	while ((tuple=heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		nodeOid = HeapTupleGetOid(tuple);
		if (nodeOid == PGXCNodeOid)
		{
			include_current_node = true;
			continue;
		}
		nodeOid_list = list_append_unique_oid(nodeOid_list, nodeOid);
	}
	heap_endscan(scan);
	heap_close(rel, AccessShareLock);

	*include_myself = include_current_node;
	return nodeOid_list;
}