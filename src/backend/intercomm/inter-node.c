/*-------------------------------------------------------------------------
 *
 * inter-node.c
 *	  Internode routines
 *
 *
 * Portions Copyright (c) 2016-2017, ADB Development Group
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/intercomm/inter-node.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"

#include "access/htup.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "utils/inval.h"
#include "catalog/indexing.h"
#include "catalog/pgxc_node.h"
#include "common/ip.h"
#include "intercomm/inter-comm.h"
#include "intercomm/inter-node.h"
#include "nodes/pg_list.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "libpq/libpq-node.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

/* Myself node id */
Oid SelfNodeID = InvalidOid;

/* The primary NodeHandle */
static NodeHandle *PrimaryHandle = NULL;

/* The NodeHandle cache hashtable */
static HTAB *NodeHandleCacheHash = NULL;

/* The flag will be set true when catch shared invalid message */
static bool NeedToRebuild = false;

static void BuildNodeHandleCacheHash(void);
static void CleanNodeHandleCacheHash(void);
static void RebuildNodeHandleCacheHash(void);
static void InvalidateNodeHandleCacheCallBack(Datum arg, int cacheid, uint32 hashvalue);
static NodeHandle *MakeNodeHandleEntry(Oid node_id, Name node_name,
									   NodeType node_type,
									   bool node_primary,
									   bool node_preferred,
									   bool nodeis_gtm);
static NodeHandle *MakeNodeHandleEntryByTuple(HeapTuple htup);
static void HandleAttatchPGconn(NodeHandle *handle);
static void HandleDetachPGconn(NodeHandle *handle);
static void GetPGconnAttatchToHandle(List *node_list, List *handle_list, bool is_report_error);
static List *GetNodeIDList(NodeType type, bool include_self);
static Oid *GetNodeIDArray(NodeType type, bool include_self, int *node_num);

/*
 * ResetNodeExecutor
 *
 * Detach PGconn of each NodeHandle in NodeHandleCacheHash.
 */
void
ResetNodeExecutor(void)
{
	if (NodeHandleCacheHash != NULL)
	{
		HASH_SEQ_STATUS	status;
		NodeHandle	   *handle;

		hash_seq_init(&status, NodeHandleCacheHash);
		while ((handle = (NodeHandle *) hash_seq_search(&status)) != NULL)
		{
			handle->node_owner = NULL;
			handle->node_conn = NULL;
			handle->node_context = NULL;
		}
	}
}

/*
 * InitializeNodeExecutor
 *
 * Build NodeHandle cache hashtable and register callback function of
 * syscache PGXCNODEOID.
 */
void
InitializeNodeExecutor(void)
{
	if (IsCnNode() && !IsBootstrapProcessingMode() && IsUnderPostmaster)
	{
		BuildNodeHandleCacheHash();

		/* Arrange to flush cache on pgxc_node changes */
		CacheRegisterSyscacheCallback(PGXCNODEOID,
									  InvalidateNodeHandleCacheCallBack,
									  (Datum) 0);
	}
}

/*
 * AtStart_NodeExecutor
 *
 * Rebuild NodeHandle cache hashtable if necessary. It must be called
 * after CurrentTransactionState set state to TRANS_INPROGRESS as
 * we will search pgxc_node syscache.
 */
void
AtStart_NodeExecutor(void)
{
	RebuildNodeHandleCacheHash();
}

/*
 * BuildNodeHandleCacheHash
 *
 * Scan pgxc_node catalog and build NodeHandle cache hashtable.
 */
static void
BuildNodeHandleCacheHash(void)
{
	Relation	rel;
	SysScanDesc scan;
	HeapTuple	tuple;

	Assert(IsCnNode());
	Assert(IsTransactionState());

	if (NodeHandleCacheHash == NULL)
	{
		HASHCTL		ctl;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(NodeHandle);
		ctl.hcxt = TopMemoryContext;
		NodeHandleCacheHash = hash_create("NodeHandle cache hash", 256,
									 	  &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	rel = heap_open(PgxcNodeRelationId, AccessShareLock);
	scan = systable_beginscan(rel, PgxcNodeOidIndexId, true,
							  NULL, 0, NULL);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
		(void) MakeNodeHandleEntryByTuple(tuple);
	systable_endscan(scan);
	heap_close(rel, AccessShareLock);

	if(!OidIsValid(SelfNodeID))
		elog(FATAL, "SelfNodeID(%d) is InvalidOid.", SelfNodeID);
}

/*
 * CleanNodeHandleCacheHash
 *
 * Remove invalid entry of NodeHandle cache hashtable
 * and reset PrimaryHandle if necessary.
 */
static void
CleanNodeHandleCacheHash(void)
{
	if (NodeHandleCacheHash != NULL)
	{
		HASH_SEQ_STATUS	status;
		NodeHandle	   *handle;

		hash_seq_init(&status, NodeHandleCacheHash);
		while ((handle = (NodeHandle *) hash_seq_search(&status)) != NULL)
		{
			if (!handle->isvalid)
			{
				if (hash_search(NodeHandleCacheHash,
								(void *) &(handle->node_id),
								HASH_REMOVE, NULL) == NULL)
					elog(ERROR, "hash table corrupted");

				if (handle == PrimaryHandle)
					PrimaryHandle = NULL;
			}
		}
	}
}

/*
 * RebuildNodeHandleCacheHash
 *
 * Rebuild NodeHandle cache hashtable if necessary.
 */
static void
RebuildNodeHandleCacheHash(void)
{
	if (NeedToRebuild && IsCnNode() && IsTransactionState())
	{
		BuildNodeHandleCacheHash();
		CleanNodeHandleCacheHash();
		NeedToRebuild = false;
	}
}

static void
InvalidateNodeHandleEntry(uint32 hashvalue)
{
	HASH_SEQ_STATUS	status;
	NodeHandle	   *handle;

	Assert(NodeHandleCacheHash != NULL);
	hash_seq_init(&status, NodeHandleCacheHash);
	while ((handle = (NodeHandle *) hash_seq_search(&status)) != NULL)
	{
		if (handle->hashvalue == hashvalue)
		{
			handle->isvalid = false;

			if (handle == PrimaryHandle)
				PrimaryHandle = NULL;
		}
	}
}

/*
 * Callback for pgxc_node inval events
 */
static void
InvalidateNodeHandleCacheCallBack(Datum arg, int cacheid, uint32 hashvalue)
{
	NeedToRebuild = true;
	InvalidateNodeHandleEntry(hashvalue);
}

static NodeHandle *
MakeNodeHandleEntry(Oid node_id, Name node_name, NodeType node_type,
					bool node_primary, bool node_preferred, bool nodeis_gtm)
{
	NodeHandle *handle = NULL;
	bool		found = false;

	Assert(NodeHandleCacheHash != NULL);
	handle = (NodeHandle *) hash_search(NodeHandleCacheHash,
										(void *) &node_id,
										HASH_ENTER, &found);
	/* Should I change this value to pgxc_node_name if I am a gtmcoord slave node? */
	namecpy(&(handle->node_name), node_name);
	handle->node_type = node_type;
	handle->node_primary = node_primary;
	handle->node_preferred = node_preferred;
	handle->isvalid = true;
	if (!found)
	{
		handle->hashvalue = GetSysCacheHashValue1(PGXCNODEOID,
												  ObjectIdGetDatum(node_id));
		handle->node_conn = NULL;
		handle->node_context = NULL;
		handle->node_owner = NULL;
	}

	if (node_primary)
		PrimaryHandle = handle;

	if (pg_strcasecmp(PGXCNodeName, NameStr(*node_name)) == 0)
		SelfNodeID = PGXCNodeOid = node_id;
	else
	{
		/* If I am a gtmcoord slave node, I have the same nodeid as my master node. */
		if ((nodeis_gtm && node_type == TYPE_CN_NODE) && /* It is a gtmcoord master */
			IsGTMCnNode())	/* I am a gtmcorod slave */
		{
			SelfNodeID = PGXCNodeOid = node_id;
		}
	}
	return handle;
}

static NodeHandle *
MakeNodeHandleEntryByTuple(HeapTuple htup)
{
	Form_pgxc_node	tuple;
	NodeType		node_type;
	Oid				node_id;
	NodeHandle	   *handle;

	if (!htup)
		return NULL;

	node_id = HeapTupleGetOid(htup);
	handle = (NodeHandle *) hash_search(NodeHandleCacheHash,
										(const void *) &node_id,
										HASH_FIND,
										NULL);
	/* return if handle exists and is valid */
	if (handle && handle->isvalid)
		return handle;

	tuple = (Form_pgxc_node) GETSTRUCT(htup);
	switch (tuple->node_type)
	{
		case PGXC_NODE_COORDINATOR:
			node_type = TYPE_CN_NODE;
			break;
		case PGXC_NODE_DATANODE:
			node_type = TYPE_DN_NODE;
			break;
		case PGXC_NODE_DATANODESLAVE:
			node_type = TYPE_DN_SLAVENODE;
			break;
		default:
			Assert(false);
			break;
	}

	return MakeNodeHandleEntry(node_id,
							   &(tuple->node_name),
							   node_type,
							   tuple->nodeis_primary,
							   tuple->nodeis_preferred,
							   tuple->nodeis_gtm);
}

NodeHandle *
GetNodeHandle(Oid node_id, bool attatch, void *context)
{
	NodeHandle *handle = NULL;
	HeapTuple	tuple = NULL;

	Assert(IsCnNode());
	Assert(NodeHandleCacheHash != NULL);

	RebuildNodeHandleCacheHash();

	/* Try to find out from the hash table */
	handle = (NodeHandle *) hash_search(NodeHandleCacheHash,
										(const void *) &node_id,
										HASH_FIND,
										NULL);

	/* Try to find out from system cache */
	if (handle == NULL && IsTransactionState())
	{
		tuple = SearchSysCache1(PGXCNODEOID, ObjectIdGetDatum(node_id));
		handle = MakeNodeHandleEntryByTuple(tuple);
	}

	if (handle != NULL)
	{
		if (attatch)
			HandleAttatchPGconn(handle);
		handle->node_context = context;
	}

	return handle;
}

List *
GetGtmHandleList(const Oid *nodes, int nnodes,
				  bool include_self, bool noerror,
				  bool attatch, void *context)
{
	NodeHandle	   *handle;
	List		   *handle_list = NIL;
	List		   *id_need = NIL;
	List		   *handle_need = NIL;
	int				i;

	if (nnodes <= 0)
		return NIL;

	RebuildNodeHandleCacheHash();

	Assert(OidIsValid(SelfNodeID));
	for (i = 0; i < nnodes; i++)
	{
		if (!include_self && nodes[i] == SelfNodeID)
			continue;
		handle = GetNodeHandle(nodes[i], false, context);
		if (!handle)
		{
			if (noerror)
				continue;

			elog(ERROR, "Invalid node \"%u\"", nodes[i]);
		}

		if (!handle->is_gtm)
			continue;

		handle_list = lappend(handle_list, handle);
		if (attatch && PQstatus(handle->node_conn) != CONNECTION_OK)
		{
			/* detach old PGconn if exists */
			HandleDetachPGconn(handle);
			id_need = lappend_oid(id_need, nodes[i]);
			handle_need = lappend(handle_need, handle);
		}
	}
	GetPGconnAttatchToHandle(id_need, handle_need, true);
	list_free(id_need);
	list_free(handle_need);

	return handle_list;
}

List *
GetNodeHandleList(const Oid *nodes, int nnodes,
				  bool include_self, bool noerror,
				  bool attatch, void *context,
				  bool is_include_gtm, bool is_report_error)
{
	NodeHandle	   *handle;
	List		   *handle_list = NIL;
	List		   *id_need = NIL;
	List		   *handle_need = NIL;
	int				i;

	if (nnodes <= 0)
		return NIL;

	RebuildNodeHandleCacheHash();

	Assert(OidIsValid(SelfNodeID));
	for (i = 0; i < nnodes; i++)
	{
		if (!include_self && nodes[i] == SelfNodeID)
			continue;
		handle = GetNodeHandle(nodes[i], false, context);
		if (!handle)
		{
			if (noerror)
				continue;

			elog(ERROR, "Invalid node \"%u\"", nodes[i]);
		}

		if (!is_include_gtm && handle->is_gtm)
			continue;

		handle_list = lappend(handle_list, handle);
		if (is_report_error && attatch && PQstatus(handle->node_conn) != CONNECTION_OK)
		{
			/* detach old PGconn if exists */
			HandleDetachPGconn(handle);
			id_need = lappend_oid(id_need, nodes[i]);
			handle_need = lappend(handle_need, handle);
		}
	}
	GetPGconnAttatchToHandle(id_need, handle_need, is_report_error);
	list_free(id_need);
	list_free(handle_need);

	return handle_list;
}

static void
HandleAttatchPGconn(NodeHandle *handle)
{
	if (handle &&
		PQstatus(handle->node_conn) != CONNECTION_OK)
	{
		List *oid_list = list_make1_oid(handle->node_id);
		List *handle_list = list_make1(handle);

		/* detach old PGconn if exists */
		HandleDetachPGconn(handle);
		GetPGconnAttatchToHandle(oid_list, handle_list, true);

		list_free(oid_list);
		list_free(handle_list);
	}
}

static void
HandleDetachPGconn(NodeHandle *handle)
{
	if (handle && handle->node_conn)
	{
		HandleGC(handle);
		handle->node_conn = NULL;
		handle->node_context = NULL;
	}
}

PGconn *
HandleGetPGconn(void *handle)
{
	if (handle)
		return ((NodeHandle *) handle)->node_conn;

	return NULL;
}

CustomOption *
PGconnSetCustomOption(PGconn *conn, void *custom, PGcustumFuns *custom_funcs)
{
	CustomOption   *opt = NULL;
	if (conn)
	{
		if (conn->funs)
		{
			opt = (CustomOption *) palloc0(sizeof(CustomOption));
			opt->cumstom = conn->custom;
			opt->cumstom_funcs = conn->funs;
		}

		conn->custom = custom;
		conn->funs = custom_funcs;
	}

	return opt;
}

void
PGconnResetCustomOption(PGconn *conn, CustomOption *opt)
{
	if (conn)
	{
		if (opt)
		{
			conn->custom = opt->cumstom;
			conn->funs = opt->cumstom_funcs;
			pfree(opt);
		} else
		{
			conn->custom = NULL;
			conn->funs = NULL;
		}
	}
}

static void
GetPGconnAttatchToHandle(List *node_list, List *handle_list, bool is_report_error)
{
	const char *param_str;
	if (node_list)
	{
		List	   *conn_list = NIL;
		ListCell   *lc_conn, *lc_handle;
		NodeHandle *handle;
		PGconn	   *conn;

		Assert(handle_list && list_length(node_list) == list_length(handle_list));

		conn_list = PQNGetConnUseOidList(node_list);
		Assert(list_length(conn_list) == list_length(node_list));
		forboth (lc_conn, conn_list, lc_handle, handle_list)
		{
			handle = (NodeHandle *) lfirst(lc_handle);
			conn = (PGconn *) lfirst(lc_conn);

			if (PQstatus(conn) != CONNECTION_OK)
			{
				if (is_report_error)
					ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("Fail to get connection with \"%s\"",
								NameStr(handle->node_name)),
							errhint("%s", PQerrorMessage(conn))));
				list_delete(handle_list, lfirst(lc_handle));
			}
			
			if (PQstatus(conn) == CONNECTION_OK)
			{
				handle->node_conn = conn;
				handle->node_conn->custom = handle;
				handle->node_conn->funs = InterQueryCustomFuncs;
				handle->is_gtm = false;
				param_str = PQparameterStatus(conn, "adb_node_type");
				if (param_str)
				{
					if (pg_strcasecmp(param_str, "gtm_coord") == 0
							|| pg_strcasecmp(param_str, "gtm") == 0)
						handle->is_gtm = true;
				}
			}
		}
		list_free(conn_list);
		conn_list = NIL;

#ifdef DEBUG_ADB
		DebugPrintHandleList(PG_FUNCNAME_MACRO, handle_list);
#endif
	}
}

NodeMixHandle *
GetMixedHandles(const List *node_list, void *context)
{
	ListCell	   *lc_id;
	List		   *id_need;
	List		   *handle_need;
	NodeMixHandle  *cur_handle;
	NodeHandle	   *handle;
	Oid				node_id;

	if (!node_list)
		return NULL;

	cur_handle = (NodeMixHandle *) palloc0(sizeof(NodeMixHandle));
	id_need = handle_need = NIL;
	foreach (lc_id, node_list)
	{
		node_id = lfirst_oid(lc_id);
		handle = GetNodeHandle(node_id, false, context);
		if (!handle)
			elog(ERROR, "Invalid node \"%u\"", node_id);
		cur_handle->mix_types |= handle->node_type;
		cur_handle->handles = lappend(cur_handle->handles, handle);
		if (handle->node_primary)
			cur_handle->pr_handle = handle;
		if (PQstatus(handle->node_conn) != CONNECTION_OK)
		{
			/* detach old PGconn if exists */
			HandleDetachPGconn(handle);
			id_need = lappend_oid(id_need, node_id);
			handle_need = lappend(handle_need, handle);
		}
	}
	GetPGconnAttatchToHandle(id_need, handle_need, true);
	list_free(id_need);
	list_free(handle_need);

	return cur_handle;
}

NodeMixHandle *
CopyMixhandle(NodeMixHandle *src)
{
	NodeMixHandle *dst = NULL;

	if (src)
	{
		dst = (NodeMixHandle *) palloc0(sizeof(NodeMixHandle));
		dst->mix_types = src->mix_types;
		dst->pr_handle = src->pr_handle;
		dst->handles = list_copy(src->handles);
	}

	return dst;
}

NodeMixHandle *
ConcatMixHandle(NodeMixHandle *mix1, NodeMixHandle *mix2)
{
	if (mix1 == NULL)
		return CopyMixhandle(mix2);
	if (mix2 == NULL)
		return mix1;
	if (mix1 == mix2)
		elog(ERROR, "cannot ConcatMixHandle() a NodeMixHandle to itself");

	if (mix1->pr_handle == NULL)
		mix1->pr_handle = mix2->pr_handle;
	else if (mix2->pr_handle != NULL)
		Assert(mix1->pr_handle == mix2->pr_handle);

	mix1->mix_types |= mix2->mix_types;
	mix1->handles = list_concat_unique(mix1->handles, mix2->handles);

	return mix1;
}

void
FreeMixHandle(NodeMixHandle *cur_handle)
{
	if (cur_handle)
	{
		list_free(cur_handle->handles);
		pfree(cur_handle);
	}
}

List *
GetAllCnIDL(bool include_self)
{
	return GetNodeIDList(TYPE_CN_NODE, include_self);
}

List *
GetAllDnIDL(bool include_self)
{
	return GetNodeIDList(TYPE_DN_NODE, include_self);
}

List *
GetAllNodeIDL(bool include_self)
{
	return GetNodeIDList(TYPE_CN_NODE | TYPE_DN_NODE, include_self);
}

static List *
GetNodeIDList(NodeType type, bool include_self)
{
	List		   *result = NIL;
	NodeHandle	   *handle;
	HASH_SEQ_STATUS	status;

	Assert(IsCnNode());
	Assert(NodeHandleCacheHash != NULL);

	RebuildNodeHandleCacheHash();

	Assert(OidIsValid(SelfNodeID));

	hash_seq_init(&status, NodeHandleCacheHash);
	while ((handle = (NodeHandle *) hash_seq_search(&status)) != NULL)
	{
		if (handle->node_id == SelfNodeID && !include_self)
			continue;

		if (handle->node_type & type)
			result = lappend_oid(result, handle->node_id);
	}

	return result;
}

Oid *
GetAllCnIDA(bool include_self, int *cn_num)
{
	return GetNodeIDArray(TYPE_CN_NODE, include_self, cn_num);
}

Oid *
GetAllDnIDA(bool include_self, int *dn_num)
{
	return GetNodeIDArray(TYPE_DN_NODE, include_self, dn_num);
}

Oid *
GetAllNodeIDA(bool include_self, int *node_num)
{
	return GetNodeIDArray(TYPE_CN_NODE | TYPE_DN_NODE, include_self, node_num);
}

static Oid *
GetNodeIDArray(NodeType type, bool include_self, int *node_num)
{
	List *nodeid_list = GetNodeIDList(type, include_self);
	Oid *nodes = NULL;

	nodes = OidListToArrary(NULL, nodeid_list, node_num);
	list_free(nodeid_list);

	return nodes;
}

const char *
GetNodeName(const Oid node_id)
{
	NodeHandle *handle = GetNodeHandle(node_id, false, NULL);

	if (handle)
		return (const char *) NameStr(handle->node_name);

	return NULL;
}

const char *
GetPrimaryNodeName(void)
{
	NodeHandle *handle = GetPrimaryNodeHandle();

	if (handle)
		return (const char *) NameStr(handle->node_name);

	return NULL;
}

NodeHandle *
GetPrimaryNodeHandle(void)
{
	RebuildNodeHandleCacheHash();

	return PrimaryHandle;
}

Oid
GetPrimaryNodeID(void)
{
	NodeHandle *handle = GetPrimaryNodeHandle();

	if (handle)
		return handle->node_id;

	return InvalidOid;
}

bool
IsPrimaryNode(Oid node_id)
{
	Oid pr_node_id = GetPrimaryNodeID();

	if (OidIsValid(pr_node_id))
		return node_id == pr_node_id;

	return false;
}

bool
HasPrimaryNode(const List *node_list)
{
	Oid pr_node_id = GetPrimaryNodeID();

	if (!OidIsValid(pr_node_id))
		return false;

	return list_member_oid(node_list, pr_node_id);
}

List *
GetPreferredRepNodes(const List *src_nodes)
{
	NodeHandle *handle;
	ListCell   *lc;
	Oid			node_id;

	if (src_nodes == NIL || list_length(src_nodes) <= 0)
		return NIL;

	foreach (lc, src_nodes)
	{
		node_id = lfirst_oid(lc);
		handle = GetNodeHandle(node_id, false, NULL);
		if (handle->node_preferred)
			return list_make1_oid(node_id);
	}

	return list_make1_oid(linitial_oid(src_nodes));
}

Size
EstimateNodeInfoSpace(void)
{
	return sizeof(PGXCNodeOid) +
		   sizeof(PGXCNodeIdentifier);
}

void
SerializeNodeInfo(Size maxsize, char *ptr)
{
	if (maxsize < sizeof(PGXCNodeOid) +
				  sizeof(PGXCNodeIdentifier))
		elog(ERROR, "not enough space to serialize node info");

	*(Oid *) ptr = PGXCNodeOid;				ptr += sizeof(PGXCNodeOid);
	*(uint32 *) ptr = PGXCNodeIdentifier;

}

void
RestoreNodeInfo(char *ptr)
{
	PGXCNodeOid = *(Oid *) ptr;				ptr += sizeof(PGXCNodeOid);
	PGXCNodeIdentifier = *(uint32 *) ptr;
}

#ifdef DEBUG_ADB
static void
MakeupConnInfo(StringInfo conn_info, const char *desc, Oid node, PGconn *conn)
{
	char			hostinfo[NI_MAXHOST];
	char			service[NI_MAXSERV];
	int				rc;

	if (desc)
		appendStringInfo(conn_info, "%s:", desc);

	if (OidIsValid(node))
		appendStringInfo(conn_info, " [node] %u", node);

	appendStringInfo(conn_info, " [sock] %d [connect]", conn->sock);

	rc = pg_getnameinfo_all(&conn->laddr.addr, conn->laddr.salen,
							hostinfo, sizeof(hostinfo),
							service, sizeof(service),
							NI_NUMERICHOST | NI_NUMERICSERV);
	if (rc != 0)
	{
		ereport(WARNING,
			(errmsg_internal("pg_getnameinfo_all() get laddr failed: %s",
							 gai_strerror(rc))));
	}
	appendStringInfo(conn_info, " %s:%s", hostinfo, service);

	rc = pg_getnameinfo_all(&conn->raddr.addr, conn->raddr.salen,
							hostinfo, sizeof(hostinfo),
							service, sizeof(service),
							NI_NUMERICHOST | NI_NUMERICSERV);
	if (rc != 0)
	{
		ereport(WARNING,
			(errmsg_internal("pg_getnameinfo_all() get raddr failed: %s",
							 gai_strerror(rc))));
	}
	appendStringInfo(conn_info, " -> %s:%s", hostinfo, service);
}

void
DebugPrintPGconn(const char *desc, Oid node, PGconn *conn)
{
	StringInfoData buf;

	initStringInfo(&buf);
	MakeupConnInfo(&buf, desc, node, conn);
	elog(LOG, "%s", buf.data);
	pfree(buf.data);
}

void
DebugPrintHandle(const char *desc, NodeHandle *handle)
{
	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfo(&buf, "%s: [name] %s", desc, NameStr(handle->node_name));
	MakeupConnInfo(&buf, NULL, handle->node_id, handle->node_conn);
	elog(LOG, "%s", buf.data);
	pfree(buf.data);
}

void
DebugPrintPGconnList(const char *desc, List *conn_list)
{
	StringInfoData	buf;
	ListCell	   *lc_conn;
	PGconn		   *conn;

	initStringInfo(&buf);
	appendStringInfo(&buf, "%s:", desc);
	foreach (lc_conn, conn_list)
	{
		conn = (PGconn *) lfirst(lc_conn);
		MakeupConnInfo(&buf, NULL, InvalidOid, conn);
	}
	elog(LOG, "%s", buf.data);
	pfree(buf.data);
}

void
DebugPrintHandleList(const char *desc, List *handle_list)
{
	StringInfoData	buf;
	ListCell	   *lc_handle;
	NodeHandle	   *handle;

	initStringInfo(&buf);
	appendStringInfo(&buf, "%s:", desc);
	foreach (lc_handle, handle_list)
	{
		handle = (NodeHandle *) lfirst(lc_handle);
		appendStringInfo(&buf, " [name] %s", NameStr(handle->node_name));
		MakeupConnInfo(&buf, NULL, handle->node_id, handle->node_conn);
	}
	elog(LOG, "%s", buf.data);
	pfree(buf.data);
}
#endif
