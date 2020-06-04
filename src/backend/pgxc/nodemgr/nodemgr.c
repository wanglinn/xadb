/*-------------------------------------------------------------------------
 *
 * nodemgr.c
 *	  Routines to support manipulation of the pgxc_node catalog
 *	  Support concerns CREATE/ALTER/DROP on NODE object.
 *
 * Portions Copyright (c) 2010-2012, Postgres-XC Development Group
 * Portions Copyright (c) 2014-2017, ADB Development Group
 *
 * IDENTIFICATION
 * 		src/backend/pgxc/nodemgr/nodemgr.h
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "access/hash.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/pgxc_node.h"
#include "commands/defrem.h"
#include "executor/execCluster.h"
#include "intercomm/inter-node.h"
#include "libpq/libpq-node.h"
#include "nodes/parsenodes.h"
#include "storage/lwlock.h"
#include "storage/mem_toc.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "pgxc/slot.h"

#define REMOTE_KEY_ALTER_NODE	1
#define REMOTE_KEY_REMOVE_NODE	2
#define REMOTE_KEY_CREATE_NODE	3

typedef struct NodeOidInfo
{
	uint32	oid_count;
	uint32	max_count;
	Oid	   *oids;
}NodeOidInfo;

typedef struct XCNodeScanDesc XCNodeScanDesc;
struct XCNodeScanDesc
{
	Relation		xcnode_rel;
	Relation		index_rel;
	void		   *scan_desc;
	HeapTuple		tuple;
	bool			free_tup;
	TupleTableSlot *slot;
	HeapTuple	  (*getnext_fun)(XCNodeScanDesc*, ScanDirection dir);
};

int				MaxCoords = 16;
int				MaxDataNodes = 16;

static void appendNodeOidInfo(NodeOidInfo *info, Oid oid);
static void initNodeOidInfo(NodeOidInfo *info);
static uint32 adb_get_all_type_oid_array(Oid **pparr, char type, bool order_name);
static List* adb_get_all_type_oid_list(char type, bool order_name);

static void xcnode_beginscan(XCNodeScanDesc *desc, bool order_name);
#define xcnodescan_getnext(desc, dir) (*((desc)->getnext_fun))(desc, dir)
static void xcnode_endscan(XCNodeScanDesc *desc);

/*
 * How many times should we try to find a unique indetifier
 * in case hash of the node name comes out to be duplicate
 */

#define MAX_TRIES_FOR_NID	200

static Datum generate_node_id(const char *node_name);


#define OID_ALLOC_STEP	8
static void initNodeOidInfo(NodeOidInfo *info)
{
	info->max_count = OID_ALLOC_STEP;
	info->oid_count = 0;
	info->oids = palloc(sizeof(Oid)*OID_ALLOC_STEP);
}

static void appendNodeOidInfo(NodeOidInfo *info, Oid oid)
{
	Assert(info->oid_count <= info->max_count);
	if (info->oid_count == info->max_count)
	{
		info->max_count += OID_ALLOC_STEP;
		info->oids = repalloc(info->oids, sizeof(Oid) * info->max_count);
	}
	info->oids[info->oid_count] = oid;
	++(info->oid_count);
}

static uint32 adb_get_all_type_oid_array(Oid **pparr, char type, bool order_name)
{
	HeapTuple tuple;
	NodeOidInfo info;
	XCNodeScanDesc scan;
	Form_pgxc_node node_form;

	if (pparr)
	{
		initNodeOidInfo(&info);
	}else
	{
		info.oid_count = info.max_count = 0;
		info.oids = NULL;
	}

	xcnode_beginscan(&scan, order_name);
	while ((tuple = xcnodescan_getnext(&scan, ForwardScanDirection)) != NULL)
	{
		node_form = (Form_pgxc_node)GETSTRUCT(tuple);
		if (node_form->node_type != type)
			continue;

		if (pparr)
			appendNodeOidInfo(&info, node_form->oid);
		else
			++info.oid_count;
	}
	xcnode_endscan(&scan);

	if (pparr)
		*pparr = info.oids;
	return info.oid_count;
}

static List* adb_get_all_type_oid_list(char type, bool order_name)
{
	HeapTuple tuple;
	List *list;
	XCNodeScanDesc scan;
	Form_pgxc_node node_form;

	list = NIL;
	xcnode_beginscan(&scan, order_name);
	while ((tuple = xcnodescan_getnext(&scan, ForwardScanDirection)) != NULL)
	{
		node_form = (Form_pgxc_node)GETSTRUCT(tuple);
		if (node_form->node_type != type)
			continue;

		list = lappend_oid(list, node_form->oid);
	}
	xcnode_endscan(&scan);

	return list;
}

static HeapTuple xcnode_heap_scan(XCNodeScanDesc *desc, ScanDirection dir)
{
	return heap_getnext(desc->scan_desc, dir);
}

static HeapTuple xcnode_table_scan(XCNodeScanDesc *desc, ScanDirection dir)
{
	if (desc->free_tup)
		pfree(desc->tuple);
	if (table_scan_getnextslot(desc->scan_desc, dir, desc->slot))
		desc->tuple = ExecFetchSlotHeapTuple(desc->slot, false, &desc->free_tup);
	else
		desc->tuple = NULL;
	return desc->tuple;
}

static HeapTuple xcnode_index_scan(XCNodeScanDesc *desc, ScanDirection dir)
{
	if (desc->free_tup)
		pfree(desc->tuple);
	if (index_getnext_slot(desc->scan_desc, dir, desc->slot))
		desc->tuple = ExecFetchSlotHeapTuple(desc->slot, false, &desc->free_tup);
	else
		desc->tuple = NULL;
	return desc->tuple;
}

static void xcnode_beginscan(XCNodeScanDesc *desc, bool order_name)
{
	MemSet(desc, 0, sizeof(*desc));
	desc->xcnode_rel = relation_open(PgxcNodeRelationId, AccessShareLock);
	if (order_name)
	{
		desc->index_rel = index_open(PgxcNodeNodeNameIndexId, AccessShareLock);
		desc->scan_desc = index_beginscan(desc->xcnode_rel,
										  desc->index_rel,
										  RegisterSnapshot(GetCatalogSnapshot(PgxcNodeRelationId)),
										  0,
										  0);
		desc->getnext_fun = xcnode_index_scan;
		desc->slot = table_slot_create(desc->xcnode_rel, NULL);
	}else
	{
		desc->scan_desc = table_beginscan_catalog(desc->xcnode_rel, 0, NULL);
		if (desc->xcnode_rel->rd_tableam == GetHeapamTableAmRoutine())
		{
			desc->getnext_fun = xcnode_heap_scan;
		}else
		{
			desc->getnext_fun = xcnode_table_scan;
			desc->slot = table_slot_create(desc->xcnode_rel, NULL);
		}
	}
}

static void xcnode_endscan(XCNodeScanDesc *desc)
{
	if (desc->free_tup &&
		desc->tuple)
		pfree(desc->tuple);
	if (desc->slot)
		ExecDropSingleTupleTableSlot(desc->slot);
	if (desc->index_rel)
	{
		index_endscan(desc->scan_desc);
		index_close(desc->index_rel, AccessShareLock);
	}else
	{
		table_endscan(desc->scan_desc);
	}
	heap_close(desc->xcnode_rel, AccessShareLock);
}

uint32 adb_get_all_coord_oid_array(Oid **pparr, bool order_name)
{
	return adb_get_all_type_oid_array(pparr, PGXC_NODE_COORDINATOR, order_name);
}

List* adb_get_all_coord_oid_list(bool order_name)
{
	return adb_get_all_type_oid_list(PGXC_NODE_COORDINATOR, order_name);
}

uint32 adb_get_all_datanode_oid_array(Oid **pparr, bool order_name)
{
	return adb_get_all_type_oid_array(pparr, PGXC_NODE_DATANODE, order_name);
}

List* adb_get_all_datanode_oid_list(bool order_name)
{
	return adb_get_all_type_oid_list(PGXC_NODE_DATANODE, order_name);
}

void adb_get_all_node_oid_array(Oid **pparr, uint32 *ncoord, uint32 *ndatanode, bool order_name)
{
	HeapTuple tuple;
	NodeOidInfo cn_info;
	NodeOidInfo dn_info;
	XCNodeScanDesc scan;
	Form_pgxc_node node_form;

	AssertArg(ncoord && ndatanode);
	if (pparr)
	{
		initNodeOidInfo(&cn_info);
		initNodeOidInfo(&dn_info);
	}else
	{
		cn_info.oid_count = dn_info.oid_count = 0;
	}

	xcnode_beginscan(&scan, order_name);
	while ((tuple = xcnodescan_getnext(&scan, ForwardScanDirection)) != NULL)
	{
		node_form = (Form_pgxc_node)GETSTRUCT(tuple);

		if (node_form->node_type == PGXC_NODE_COORDINATOR)
		{
			if (pparr)
				appendNodeOidInfo(&cn_info, node_form->oid);
			else
				++(cn_info.oid_count);
		}else if (node_form->node_type == PGXC_NODE_DATANODE)
		{
			if (pparr)
				appendNodeOidInfo(&dn_info, node_form->oid);
			else
				++(dn_info.oid_count);
		}else
		{
			elog(ERROR, "unknown xcnode type %d", node_form->node_type);
		}
	}
	xcnode_endscan(&scan);

	*ncoord = cn_info.oid_count;
	*ndatanode = dn_info.oid_count;
	if (pparr)
	{
		if (cn_info.max_count < cn_info.oid_count + dn_info.oid_count)
			cn_info.oids = repalloc(cn_info.oids, sizeof(Oid)*(cn_info.oid_count + dn_info.oid_count));
		memcpy(&cn_info.oids[cn_info.oid_count], dn_info.oids, sizeof(Oid) * dn_info.oid_count);
		*pparr = cn_info.oids;
		pfree(dn_info.oids);
	}
}

void adb_get_all_node_oid_list(List **list_coord, List **list_datanode, bool order_name)
{
	HeapTuple tuple;
	XCNodeScanDesc scan;
	List *list_cn = NIL;
	List *list_dn = NIL;
	Form_pgxc_node node_form;

	xcnode_beginscan(&scan, order_name);
	while ((tuple = xcnodescan_getnext(&scan, ForwardScanDirection)) != NULL)
	{
		node_form = (Form_pgxc_node)GETSTRUCT(tuple);
		if (node_form->node_type == PGXC_NODE_COORDINATOR)
			list_cn = lappend_oid(list_cn, node_form->oid);
		else if (node_form->node_type == PGXC_NODE_DATANODE)
			list_dn = lappend_oid(list_dn, node_form->oid);
		else
			elog(ERROR, "unknown xcnode type %d", node_form->node_type);
	}
	xcnode_endscan(&scan);

	*list_coord = list_cn;
	*list_datanode = list_dn;
}

/*
 * Check list of options and return things filled.
 * This includes check on option values.
 */
static void
check_node_options(const char *node_name, List *options, char **node_host,
			int *node_port, char *node_type,
			bool *is_primary, bool *is_preferred,
			bool *is_gtm, char **new_node_name)
{
	ListCell   *option;

	if (!options)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("No options specified")));

	/* Filter options */
	foreach(option, options)
	{
		DefElem    *defel = (DefElem *) lfirst(option);

		if (strcmp(defel->defname, "port") == 0)
		{
			*node_port = defGetTypeLength(defel);

			if (*node_port < 1 || *node_port > 65535)
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("port value is out of range")));
		}
		else if (strcmp(defel->defname, "host") == 0)
		{
			*node_host = defGetString(defel);
		}
		else if (strcmp(defel->defname, "type") == 0)
		{
			char *type_loc;

			type_loc = defGetString(defel);

			if (strcmp(type_loc, "coordinator") != 0 &&
				strcmp(type_loc, "datanode") != 0 &&
				strcmp(type_loc, "datanode slave") != 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("type value is incorrect, specify"
							" 'coordinator or 'datanode' or 'datanode slave'")));

			if (strcmp(type_loc, "coordinator") == 0)
				*node_type = PGXC_NODE_COORDINATOR;
			else if (strcmp(type_loc, "datanode") == 0)
				*node_type = PGXC_NODE_DATANODE;
			else
				*node_type = PGXC_NODE_DATANODESLAVE;
		}
		else if (strcmp(defel->defname, "primary") == 0)
		{
			*is_primary = defGetBoolean(defel);
		}
		else if (strcmp(defel->defname, "preferred") == 0)
		{
			*is_preferred = defGetBoolean(defel);
		}
		else if (strcmp(defel->defname, "gtm") == 0)
		{
			*is_gtm = defGetBoolean(defel);
		}
		else if (new_node_name &&
			strcmp(defel->defname, "name") == 0)
		{
			*new_node_name = defGetString(defel);
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("incorrect option: %s", defel->defname)));
		}
	}

	/* A primary node has to be a Datanode */
	if (*is_primary && *node_type != PGXC_NODE_DATANODE)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("PGXC node %s: cannot be a primary node, it has to be a Datanode",
						node_name)));

	/* A preferred node has to be a Datanode */
	if (*is_preferred && *node_type != PGXC_NODE_DATANODE)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("PGXC node %s: cannot be a preferred node, it has to be a Datanode",
						node_name)));

	/* Node type check */
	if (*node_type == PGXC_NODE_NONE)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("PGXC node %s: Node type not specified",
						node_name)));
}

/*
 * generate_node_id
 *
 * Given a node name compute its hash to generate the identifier
 * If the hash comes out to be duplicate , try some other values
 * Give up after a few tries
 */
static Datum
generate_node_id(const char *node_name)
{
	Datum		node_id;
	uint32		n;
	bool		inc;
	int		i;

	/* Compute node identifier by computing hash of node name */
	node_id = hash_any((unsigned char *)node_name, strlen(node_name));

	/*
	 * Check if the hash is near the overflow limit, then we will
	 * decrement it , otherwise we will increment
	 */
	inc = true;
	n = DatumGetUInt32(node_id);
	if (n >= UINT_MAX - MAX_TRIES_FOR_NID)
		inc = false;

	/*
	 * Check if the identifier is clashing with an existing one,
	 * and if it is try some other
	 */
	for (i = 0; i < MAX_TRIES_FOR_NID; i++)
	{
		HeapTuple	tup;

		tup = SearchSysCache1(PGXCNODEIDENTIFIER, node_id);
		if (tup == NULL)
			break;

		ReleaseSysCache(tup);

		n = DatumGetUInt32(node_id);
		if (inc)
			n++;
		else
			n--;

		node_id = UInt32GetDatum(n);
	}

	/*
	 * This has really few chances to happen, but inform backend that node
	 * has not been registered correctly in this case.
	 */
	if (i >= MAX_TRIES_FOR_NID)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("Please choose different node name."),
				 errdetail("Name \"%s\" produces a duplicate identifier node_name",
						   node_name)));

	return node_id;
}

static void
PgxcNodeCreateLocal(CreateNodeStmt *stmt)
{
	Relation	pgxcnodesrel;
	HeapTuple	htup;
	bool		nulls[Natts_pgxc_node];
	Datum		values[Natts_pgxc_node];
	const char *node_name = stmt->node_name;
	const char *node_mastername = stmt->node_mastername;
	int		i;
	/* Options with default values */
	char	   *node_host = NULL;
	char		node_type = PGXC_NODE_NONE;
	int			node_port = 0;
	bool		is_primary = false;
	bool		is_preferred = false;
	bool		is_gtm = false;
	Datum		node_id;
	ListCell   *option;

	/* check datandoe master name */
	if(node_mastername)
	{
		foreach(option, stmt->options)
		{
			DefElem *defel = (DefElem *) lfirst(option);
			if (strcmp(defel->defname, "type") == 0)
			{
				char *str = strVal(defel->arg);
				if (strcmp(str, "datanode slave") == 0)
				{
					if(get_pgxc_nodeoid(node_mastername) == InvalidOid)
						ereport(ERROR,
								(errcode(ERRCODE_DUPLICATE_OBJECT),
								errmsg("Nonexistent master node name")));
				}
			}
		}

	}

	/* Only a DB administrator can add nodes */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to create cluster nodes")));

	/* Check that node name is node in use */
	if (OidIsValid(get_pgxc_nodeoid(node_name)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("PGXC Node %s: object already defined",
						node_name)));

	/* Check length of node name */
	if (strlen(node_name) >= NAMEDATALEN)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("Node name \"%s\" is too long",
						node_name)));

	/* Filter options */
	check_node_options(node_name, stmt->options, &node_host,
				&node_port, &node_type,
				&is_primary, &is_preferred,
				&is_gtm, NULL);

	/* Compute node identifier */
	node_id = generate_node_id(node_name);

	/*
	 * Check that this node is not created as a primary if one already
	 * exists.
	 */
	if (is_primary && GetPrimaryNodeHandle() != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("PGXC node %s: two nodes cannot be primary",
						node_name)));

	/*
	 * Then assign default values if necessary
	 * First for port.
	 */
	if (node_port == 0)
	{
		node_port = DEF_PGPORT;
		elog(NOTICE, "PGXC node %s: Applying default port value: %d",
			 node_name, node_port);
	}

	/* Then apply default value for host */
	if (!node_host)
	{
		node_host = strdup("localhost");
		elog(NOTICE, "PGXC node %s: Applying default host value: %s",
			 node_name, node_host);
	}

	/* Iterate through all attributes initializing nulls and values */
	for (i = 0; i < Natts_pgxc_node; i++)
	{
		nulls[i]  = false;
		values[i] = (Datum) 0;
	}

	/*
	 * Open the relation for insertion
	 * This is necessary to generate a unique Oid for the new node
	 * There could be a relation race here if a similar Oid
	 * being created before the heap is inserted.
	 */
	pgxcnodesrel = heap_open(PgxcNodeRelationId, RowExclusiveLock);

	/* Build entry tuple */
	values[Anum_pgxc_node_node_name - 1] = DirectFunctionCall1(namein, CStringGetDatum(node_name));
	values[Anum_pgxc_node_node_type - 1] = CharGetDatum(node_type);
	values[Anum_pgxc_node_node_port - 1] = Int32GetDatum(node_port);
	values[Anum_pgxc_node_node_host - 1] = DirectFunctionCall1(namein, CStringGetDatum(node_host));
	values[Anum_pgxc_node_nodeis_primary - 1] = BoolGetDatum(is_primary);
	values[Anum_pgxc_node_nodeis_preferred - 1] = BoolGetDatum(is_preferred);
	values[Anum_pgxc_node_nodeis_gtm - 1] = BoolGetDatum(is_gtm);
	values[Anum_pgxc_node_node_id - 1] = node_id;
	values[Anum_pgxc_node_node_master_oid - 1] = ObjectIdGetDatum(node_mastername ? get_pgxc_nodeoid(node_mastername) : 0);
	values[Anum_pgxc_node_oid - 1] = ObjectIdGetDatum(GetNewOidWithIndex(pgxcnodesrel, PgxcNodeOidIndexId, Anum_pgxc_node_oid));

	htup = heap_form_tuple(pgxcnodesrel->rd_att, values, nulls);

	/* Insert tuple in catalog */
	CatalogTupleInsert(pgxcnodesrel, htup);

	heap_close(pgxcnodesrel, RowExclusiveLock);
}

/*
 * PgxcNodeCreate
 *
 * Add a PGXC node
 */
void
PgxcNodeCreate(CreateNodeStmt *stmt)
{
	ListCell   *lc;
	Value	   *value;
	List	   *nodeOids;
	List	   *remoteList;
	Oid			oid;
	bool		include_myself = false;

	if (stmt->node_list == NIL)
		include_myself = true;

	nodeOids = NIL;
	foreach(lc, stmt->node_list)
	{
		value = lfirst(lc);
		if (strcasecmp(PGXCNodeName, strVal(value)) == 0)
		{
			include_myself = true;
			if (IsConnFromCoord())
				break;
			else
				continue;
		}
		if (IsConnFromApp())
		{
			oid = get_pgxc_nodeoid(strVal(value));
			Assert(oid != PGXCNodeOid);

			nodeOids = list_append_unique_oid(nodeOids, oid);
		}
	}

	remoteList = NIL;
	if (nodeOids != NIL)
	{
		StringInfoData msg;
		initStringInfo(&msg);

		ClusterTocSetCustomFun(&msg, ClusterNodeCreate);

		begin_mem_toc_insert(&msg, REMOTE_KEY_CREATE_NODE);
		saveNode(&msg, (Node*)stmt);
		end_mem_toc_insert(&msg, REMOTE_KEY_CREATE_NODE);

		remoteList = ExecClusterCustomFunction(nodeOids, &msg, 0);
		pfree(msg.data);
	}

	if (include_myself)
		PgxcNodeCreateLocal(stmt);

	if (remoteList)
	{
		PQNListExecFinish(remoteList, NULL, &PQNDefaultHookFunctions, true);
		list_free(remoteList);
	}
	list_free(nodeOids);
}

void ClusterNodeCreate(StringInfo mem_toc)
{
	CreateNodeStmt *stmt;
	StringInfoData buf;

	buf.data = mem_toc_lookup(mem_toc, REMOTE_KEY_CREATE_NODE, &buf.maxlen);
	if (buf.data == NULL)
	{
		ereport(ERROR,
				(errmsg("Can not found CreateNodeStmt in cluster message"),
				 errcode(ERRCODE_PROTOCOL_VIOLATION)));
	}
	buf.len = buf.maxlen;
	buf.cursor = 0;

	stmt = castNode(CreateNodeStmt, loadNode(&buf));

	PgxcNodeCreateLocal(stmt);
}

/*
 * PgxcNodeAlter
 *
 * Alter a PGXC node
 */
static void
PgxcNodeAlterLocal(AlterNodeStmt *stmt)
{
	const char *node_name = stmt->node_name;
	char	   *node_host_old, *node_host_new = NULL;
	char	   *new_node_name = NULL;
	int			node_port_old, node_port_new;
	char		node_type_old, node_type_new;
	bool		is_primary;
	bool		is_preferred = false;
	bool		is_gtm = false;
	HeapTuple	oldtup, newtup;
	Oid			node_oid;
	Relation	rel;
	Datum		new_record[Natts_pgxc_node];
	bool		new_record_nulls[Natts_pgxc_node];
	bool		new_record_repl[Natts_pgxc_node];
	uint32		node_id;
	Form_pgxc_node node_form;
	NodeHandle *node_handle;

	/* Only a DB administrator can alter cluster nodes */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to change cluster nodes")));

	/* Look at the node tuple, and take exclusive lock on it */
	rel = heap_open(PgxcNodeRelationId, RowExclusiveLock);

	/* Open new tuple, checks are performed on it and new values */
	oldtup = SearchSysCache1(PGXCNODENAME, PointerGetDatum(node_name));
	if (!HeapTupleIsValid(oldtup))
		elog(ERROR, "cache lookup failed for PGXC node \"%s\"", node_name);

	node_form = (Form_pgxc_node) GETSTRUCT(oldtup);
	node_oid = node_form->oid;
	Assert(OidIsValid(node_oid));
	node_host_old = pstrdup(NameStr(node_form->node_host));
	node_port_old = node_port_new = node_form->node_port;
	node_type_old = node_type_new = node_form->node_type;
	is_primary = node_form->nodeis_primary;
	is_preferred = node_form->nodeis_preferred;
	is_gtm = node_form->nodeis_gtm;
	node_id = node_form->node_id;

	/* Filter options */
	check_node_options(node_name, stmt->options,
					   &node_host_new,
					   &node_port_new,
					   &node_type_new,
					   &is_primary,
					   &is_preferred,
					   &is_gtm,
					   &new_node_name);

	if (node_host_new != NULL)
	{
		if (pg_strcasecmp(node_host_old, node_host_new) != 0)
			PreventInterTransactionChain(node_oid, "ALTER NODE HOST");
	} else
	{
		node_host_new = node_host_old;
	}
	if (node_port_old != node_port_new)
		PreventInterTransactionChain(node_oid, "ALTER NODE PORT");
	if(IsCnNode() && (node_type_old != node_type_new))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("PGXC node \"%s\": cannot alter from \"%s\" to \"%s\"",
				 		node_name,
				 		node_type_old == PGXC_NODE_COORDINATOR ? "Coordinator" : "Datanode",
				 		node_type_new == PGXC_NODE_COORDINATOR ? "Coordinator" : "Datanode")));

	/*
	 * Two nodes cannot be primary at the same time. If the primary
	 * node is this node itself, well there is no point in having an
	 * error.
	 */
	if (is_primary &&
		(node_handle=GetPrimaryNodeHandle()) != NULL &&
		node_oid != node_handle->node_id)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("PGXC node %s: two nodes cannot be primary",
						node_name)));

	/* Update values for catalog entry */
	MemSet(new_record, 0, sizeof(new_record));
	MemSet(new_record_nulls, false, sizeof(new_record_nulls));
	MemSet(new_record_repl, false, sizeof(new_record_repl));
	if (new_node_name &&
		strcmp(new_node_name, node_name) != 0)
	{
		PreventInterTransactionChain(node_oid, "ALTER NODE NAME");
		new_record[Anum_pgxc_node_node_name - 1] = DirectFunctionCall1(namein, CStringGetDatum(new_node_name));
		new_record_repl[Anum_pgxc_node_node_name - 1] = true;
		new_record_nulls[Anum_pgxc_node_node_name - 1] = false;
	}
	new_record[Anum_pgxc_node_node_port - 1] = Int32GetDatum(node_port_new);
	new_record_repl[Anum_pgxc_node_node_port - 1] = true;
	new_record[Anum_pgxc_node_node_host - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(node_host_new));
	new_record_repl[Anum_pgxc_node_node_host - 1] = true;
	new_record[Anum_pgxc_node_node_type - 1] = CharGetDatum(node_type_new);
	new_record_repl[Anum_pgxc_node_node_type - 1] = true;
	new_record[Anum_pgxc_node_nodeis_primary - 1] = BoolGetDatum(is_primary);
	new_record_repl[Anum_pgxc_node_nodeis_primary - 1] = true;
	new_record[Anum_pgxc_node_nodeis_preferred - 1] = BoolGetDatum(is_preferred);
	new_record_repl[Anum_pgxc_node_nodeis_preferred - 1] = true;
	new_record[Anum_pgxc_node_nodeis_gtm - 1] = BoolGetDatum(is_gtm);
	new_record_repl[Anum_pgxc_node_nodeis_gtm - 1] = true;
	new_record[Anum_pgxc_node_node_id - 1] = UInt32GetDatum(node_id);
	new_record_repl[Anum_pgxc_node_node_id - 1] = true;

	/* Update relation */
	newtup = heap_modify_tuple(oldtup, RelationGetDescr(rel),
							   new_record,
							   new_record_nulls, new_record_repl);
	CatalogTupleUpdate(rel, &oldtup->t_self, newtup);

	ReleaseSysCache(oldtup);

	/* Release lock at Commit */
	heap_close(rel, NoLock);
}

void
PgxcNodeAlter(AlterNodeStmt *stmt)
{
	ListCell   *lc;
	Value	   *value;
	List	   *nodeOids;
	List	   *remoteList;
	Oid			oid;
	bool		include_myself = false;

	if (stmt->node_list == NIL)
		include_myself = true;

	nodeOids = NIL;
	foreach(lc, stmt->node_list)
	{
		value = lfirst(lc);
		if (strcasecmp(PGXCNodeName, strVal(value)) == 0)
		{
			include_myself = true;
			if (IsConnFromCoord())
				break;
			else
				continue;
		}
		if (IsConnFromApp())
		{
			oid = get_pgxc_nodeoid(strVal(value));
			Assert(oid != PGXCNodeOid);

			nodeOids = list_append_unique_oid(nodeOids, oid);
		}
	}

	remoteList = NIL;
	if (nodeOids != NIL)
	{
		StringInfoData msg;
		initStringInfo(&msg);

		ClusterTocSetCustomFun(&msg, ClusterNodeAlter);

		begin_mem_toc_insert(&msg, REMOTE_KEY_ALTER_NODE);
		saveNode(&msg, (Node*)stmt);
		end_mem_toc_insert(&msg, REMOTE_KEY_ALTER_NODE);

		remoteList = ExecClusterCustomFunction(nodeOids, &msg, 0);
		pfree(msg.data);
	}

	if (include_myself)
		PgxcNodeAlterLocal(stmt);

	if (remoteList)
	{
		PQNListExecFinish(remoteList, NULL, &PQNDefaultHookFunctions, true);
		list_free(remoteList);
	}
	list_free(nodeOids);
}

void ClusterNodeAlter(StringInfo mem_toc)
{
	AlterNodeStmt *stmt;
	StringInfoData buf;

	buf.data = mem_toc_lookup(mem_toc, REMOTE_KEY_ALTER_NODE, &buf.maxlen);
	if (buf.data == NULL)
	{
		ereport(ERROR,
				(errmsg("Can not found AlterNodeStmt in cluster message"),
				 errcode(ERRCODE_PROTOCOL_VIOLATION)));
	}
	buf.len = buf.maxlen;
	buf.cursor = 0;

	stmt = castNode(AlterNodeStmt, loadNode(&buf));

	PgxcNodeAlterLocal(stmt);
}

/*
 * PgxcNodeRemoveLocal
 *
 * Remove a PGXC node
 */
static void
PgxcNodeRemoveLocal(DropNodeStmt *stmt)
{
	Relation	relation;
	HeapTuple	tup;
	const char	*node_name = stmt->node_name;
	Oid		noid = get_pgxc_nodeoid(node_name);

	/* Only a DB administrator can remove cluster nodes */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to remove cluster nodes")));

	/* Check if node is defined */
	if (!OidIsValid(noid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("PGXC Node %s: object not defined",
						node_name)));
	if (strcmp(node_name, PGXCNodeName) == 0 && 
		get_pgxc_nodetype(noid) != PGXC_NODE_DATANODESLAVE)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("PGXC Node %s: cannot drop local node",
						node_name)));

	PreventInterTransactionChain(noid, "DROP NODE");

	/* PGXCTODO:
	 * Is there any group which has this node as member
	 * XC Tables will also have this as a member in their array
	 * Do this search in the local data structure.
	 * If a node is removed, it is necessary to check if there is a distributed
	 * table on it. If there are only replicated table it is OK.
	 * However, we have to be sure that there are no pooler agents in the cluster pointing to it.
	 */

	/* Delete the pgxc_node tuple */
	relation = heap_open(PgxcNodeRelationId, RowExclusiveLock);
	tup = SearchSysCache1(PGXCNODEOID, ObjectIdGetDatum(noid));
	if (!HeapTupleIsValid(tup)) /* should not happen */
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("PGXC Node %s: object not defined",
						node_name)));

	simple_heap_delete(relation, &tup->t_self);

	ReleaseSysCache(tup);

	heap_close(relation, RowExclusiveLock);
}

void
PgxcNodeRemove(DropNodeStmt *stmt)
{
	ListCell   *lc;
	Value	   *value;
	List	   *nodeOids;
	List	   *remoteList;
	Oid			oid;
	bool		include_myself = false;

	if (stmt->node_list == NIL)
		include_myself = true;

	nodeOids = NIL;
	foreach(lc, stmt->node_list)
	{
		value = lfirst(lc);
		if (strcasecmp(PGXCNodeName, strVal(value)) == 0)
		{
			include_myself = true;
			if (IsConnFromCoord())
				break;
			else
				continue;
		}
		if (IsConnFromApp())
		{
			oid = get_pgxc_nodeoid(strVal(value));
			Assert(oid != PGXCNodeOid);

			nodeOids = list_append_unique_oid(nodeOids, oid);
		}
	}

	remoteList = NIL;
	if (nodeOids != NIL)
	{
		StringInfoData msg;
		initStringInfo(&msg);

		ClusterTocSetCustomFun(&msg, ClusterNodeRemove);

		begin_mem_toc_insert(&msg, REMOTE_KEY_REMOVE_NODE);
		saveNode(&msg, (Node*)stmt);
		end_mem_toc_insert(&msg, REMOTE_KEY_REMOVE_NODE);

		remoteList = ExecClusterCustomFunction(nodeOids, &msg, 0);
		pfree(msg.data);
	}

	if (include_myself)
		PgxcNodeRemoveLocal(stmt);

	if (remoteList)
	{
		PQNListExecFinish(remoteList, NULL, &PQNDefaultHookFunctions, true);
		list_free(remoteList);
	}
	list_free(nodeOids);
}

void ClusterNodeRemove(StringInfo mem_toc)
{
	DropNodeStmt *stmt;
	StringInfoData buf;

	buf.data = mem_toc_lookup(mem_toc, REMOTE_KEY_REMOVE_NODE, &buf.maxlen);
	if (buf.data == NULL)
	{
		ereport(ERROR,
				(errmsg("Can not found DropNodeStmt in cluster message"),
				 errcode(ERRCODE_PROTOCOL_VIOLATION)));
	}
	buf.len = buf.maxlen;
	buf.cursor = 0;

	stmt = castNode(DropNodeStmt, loadNode(&buf));

	PgxcNodeRemoveLocal(stmt);
}

void
InitPGXCNodeIdentifier(void)
{
	if (IsCnNode() || IsDnNode())
	{
		if (!OidIsValid(PGXCNodeOid))
			PGXCNodeOid = get_pgxc_nodeoid(PGXCNodeName);

		if (PGXCNodeIdentifier == 0)
			PGXCNodeIdentifier = get_pgxc_node_id(PGXCNodeOid);

		InitSLOTPGXCNodeOid();
	}
}
