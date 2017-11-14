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

#include "intercomm/inter-comm.h"
#include "intercomm/inter-node.h"
#include "nodes/pg_list.h"
#include "libpq/libpq-fe.h"
#include "libpq/libpq-int.h"
#include "libpq/libpq-node.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

/* number of PGconn held */
static int NumDnConns = 0;
static int NumCnConns = 0;

/*
 * NodeHandle
 * it is determined by node table in the shared memory
 */
volatile int NumCnNodes = 0;
volatile int NumDnNodes = 0;
static int NumMaxNodes = 0;
static volatile int NumAllNodes = 0;
static NodeHandle *AllHandles = NULL;
static NodeHandle *CnHandles = NULL;
static NodeHandle *DnHandles = NULL;
static bool handle_init = false;

static NodeHandle *PrHandle = NULL;

#define foreach_all_handles(p)	\
	for (p = AllHandles; p - AllHandles < NumAllNodes; p = &p[1])
#define foreach_cn_handles(p)	\
	for (p = CnHandles; p - CnHandles < NumCnNodes; p = &p[1])
#define foreach_dn_handles(p)	\
	for (p = DnHandles; p - DnHandles < NumDnNodes; p = &p[1])

static void GetPGconnAttatchToHandle(List *node_list, List *handle_list);
static List *GetNodeIDList(NodeType type, bool include_self);
static Oid *GetNodeIDArray(NodeType type, bool include_self, int *node_num);

void
ResetNodeExecutor(void)
{
	NodeHandle  *handle;

	foreach_all_handles(handle)
		HandleDetachPGconn(handle);
}

void
ReleaseNodeExecutor(void)
{
	if (AllHandles)
	{
		Assert(NumMaxNodes > 0);
		ResetNodeExecutor();
	}
	NumAllNodes = NumCnNodes = NumDnNodes = 0;
	CnHandles = DnHandles = NULL;
	PrHandle = NULL;
	handle_init = false;
}

void
InitNodeExecutor(bool force)
{
	NodeDefinition *all_node_def;
	NodeDefinition *nodedef;
	NodeHandle	   *handle;
	Size			sz;
	int				numCN, numDN, numALL;
	int				i;

	if (force)
		ReleaseNodeExecutor();

	/* already initialized */
	if (handle_init)
		return ;

	/* Update node table in the shared memory */
	PgxcNodeListAndCount();

	all_node_def = PgxcNodeGetAllDefinition(&numCN, &numDN);
	numALL = numCN + numDN;
	sz = numALL * sizeof(NodeHandle);
	if (AllHandles == NULL)
	{
		AllHandles = (NodeHandle *) MemoryContextAlloc(TopMemoryContext, sz);
		NumMaxNodes = numALL;
	} else if (numALL > NumMaxNodes)
	{
		Assert(NumMaxNodes > 0);
		AllHandles = (NodeHandle *) repalloc(AllHandles, sz);
		NumMaxNodes = numALL;
	} else {
		/* keep compiler quiet */
	}
	sz = NumMaxNodes * sizeof(NodeHandle);
	MemSet(AllHandles, 0, sz);

	NumCnConns = 0;
	NumDnConns = 0;
	PGXCNodeId = 0;
	for (i = 0; i < numALL; i++)
	{
		nodedef = &(all_node_def[i]);
		handle = &(AllHandles[i]);

		handle->node_id = nodedef->nodeoid;
		handle->node_type = (i < numCN ? TYPE_CN_NODE : TYPE_DN_NODE);
		namecpy(&(handle->node_name), &(nodedef->nodename));
		handle->node_primary = nodedef->nodeisprimary;
		handle->node_conn = NULL;
		handle->node_context = NULL;
		handle->node_owner = NULL;
		if (handle->node_primary)
			PrHandle = handle;

		if (handle->node_type == TYPE_CN_NODE &&
			pg_strcasecmp(PGXCNodeName, NameStr(handle->node_name)) == 0)
		{
			PGXCNodeId = i + 1;
			PGXCNodeOid = handle->node_id;
		}
	}
	safe_pfree(all_node_def);

	NumAllNodes = numALL;
	NumCnNodes = numCN;
	NumDnNodes = numDN;
	CnHandles = (numCN > 0 ? AllHandles : NULL);
	DnHandles = (numDN > 0 ? &AllHandles[numCN] : NULL);

	/*
	 * No node-self?
	 * PGXCTODO: Change error code
	 */
	if (PGXCNodeId == 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("Coordinator cannot identify itself")));

	handle_init = true;
}

NodeHandle *
GetNodeHandle(Oid node_id, bool attatch, void *context)
{
	NodeHandle *handle;

	if (!handle_init)
		return NULL;

	foreach_all_handles(handle)
	{
		if (handle->node_id == node_id)
		{
			if (attatch)
				HandleAttatchPGconn(handle);
			handle->node_context = context;
			return handle;
		}
	}

	return NULL;
}

NodeHandle *
GetCnHandle(Oid cn_id, bool attatch, void *context)
{
	NodeHandle *handle;

	if (!handle_init)
		return NULL;

	foreach_cn_handles(handle)
	{
		if (handle->node_id == cn_id)
		{
			if (attatch)
				HandleAttatchPGconn(handle);
			handle->node_context = context;
			return handle;
		}
	}

	return NULL;
}

NodeHandle *
GetDnHandle(Oid dn_id, bool attatch, void *context)
{
	NodeHandle *handle;

	if (!handle_init)
		return NULL;

	foreach_dn_handles(handle)
	{
		if (handle->node_id == dn_id)
		{
			if (attatch)
				HandleAttatchPGconn(handle);
			handle->node_context = context;
			return handle;
		}
	}

	return NULL;
}

void
HandleAttatchPGconn(NodeHandle *handle)
{
	if (handle &&
		PQstatus(handle->node_conn) != CONNECTION_OK)
	{
		List *oid_list = list_make1_oid(handle->node_id);
		List *handle_list = list_make1(handle);

		/* detach old PGconn if exists */
		HandleDetachPGconn(handle);
		GetPGconnAttatchToHandle(oid_list, handle_list);

		list_free(oid_list);
		list_free(handle_list);
	}
}

void
HandleDetachPGconn(NodeHandle *handle)
{
	if (handle && handle->node_conn)
	{
		//PQfinish(handle->node_conn);
		HandleGC(handle);
		handle->node_conn = NULL;
		handle->node_context = NULL;
		handle->node_owner = NULL;
		if (handle->node_type == TYPE_CN_NODE)
			NumCnConns--;
		else
			NumDnConns--;
	}
}

void
HandleReAttatchPGconn(NodeHandle *handle)
{
	HandleDetachPGconn(handle);
	HandleAttatchPGconn(handle);
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
GetPGconnAttatchToHandle(List *node_list, List *handle_list)
{
	if (node_list)
	{
		List	   *conn_list = NIL;
		ListCell   *lc_conn, *lc_handle;
		NodeHandle *handle;

		Assert(handle_list && list_length(node_list) == list_length(handle_list));

		conn_list = PQNGetConnUseOidList(node_list);
		Assert(list_length(conn_list) == list_length(node_list));
		forboth (lc_conn, conn_list, lc_handle, handle_list)
		{
			handle = (NodeHandle *) lfirst(lc_handle);
			Assert(PQstatus(handle->node_conn) != CONNECTION_OK);
			handle->node_conn = (PGconn *) lfirst(lc_conn);
			handle->node_conn->custom = handle;
			handle->node_conn->funs = InterQueryCustomFuncs;
			if (handle->node_type == TYPE_CN_NODE)
				NumCnConns++;
			else
				NumDnConns++;
		}
		list_free(conn_list);
		conn_list = NIL;
	}
}

NodeMixHandle *
GetMixedHandles(const List *node_list, void *context)
{
	ListCell	   *lc_id;
	List		   *id_need;
	List		   *handle_need;
	NodeMixHandle  *mix_handle;
	NodeHandle	   *handle;
	Oid				node_id;

	if (!node_list || !handle_init)
		return NULL;

	mix_handle = (NodeMixHandle *) palloc0(sizeof(NodeMixHandle));
	id_need = handle_need = NIL;
	foreach (lc_id, node_list)
	{
		node_id = lfirst_oid(lc_id);
		handle = GetNodeHandle(node_id, false, context);
		if (!handle)
			ereport(ERROR,
				   (errmsg("Invalid node id %u", node_id)));
		mix_handle->mix_types |= handle->node_type;
		mix_handle->handles = lappend(mix_handle->handles, handle);
		if (handle->node_primary)
			mix_handle->pr_handle = handle;
		if (PQstatus(handle->node_conn) != CONNECTION_OK)
		{
			/* detach old PGconn if exists */
			HandleDetachPGconn(handle);
			id_need = lappend_oid(id_need, node_id);
			handle_need = lappend(handle_need, handle);
		}
	}
	GetPGconnAttatchToHandle(id_need, handle_need);
	list_free(id_need);
	list_free(handle_need);

	return mix_handle;
}

NodeMixHandle *
GetAllHandles(void *context)
{
	List		   *id_need;
	List		   *handle_need;
	NodeMixHandle  *mix_handle;
	NodeHandle	   *handle;

	/* do not initialized */
	if (!handle_init)
		return NULL;

	mix_handle = (NodeMixHandle *) palloc0(sizeof(NodeMixHandle));
	id_need = handle_need = NIL;
	foreach_all_handles(handle)
	{
		handle->node_context = context;
		mix_handle->mix_types |= handle->node_type;
		mix_handle->handles = lappend(mix_handle->handles, handle);
		if (handle->node_primary)
			mix_handle->pr_handle = handle;
		if (PQstatus(handle->node_conn) != CONNECTION_OK)
		{
			/* detach old PGconn if exists */
			HandleDetachPGconn(handle);
			id_need = lappend_oid(id_need, handle->node_id);
			handle_need = lappend(handle_need, handle);
		}
	}
	GetPGconnAttatchToHandle(id_need, handle_need);
	list_free(id_need);
	list_free(handle_need);

	return mix_handle;
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
FreeMixHandle(NodeMixHandle *mix_handle)
{
	if (mix_handle)
	{
		list_free(mix_handle->handles);
		pfree(mix_handle);
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
	List	   *result = NIL;
	NodeHandle *handle;

	if (type & TYPE_CN_NODE)
	{
		foreach_cn_handles(handle)
		{
			if (handle->node_id == PGXCNodeOid && !include_self)
				continue;

			result = lappend_oid(result, handle->node_id);
		}
	}

	if (type & TYPE_DN_NODE)
	{
		foreach_dn_handles(handle)
		{
			if (handle->node_id == PGXCNodeOid && !include_self)
				continue;

			result = lappend_oid(result, handle->node_id);
		}
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
	NodeHandle *handle;
	Oid		   *result;
	int			num = 0;

	if (type & TYPE_CN_NODE)
		num += NumCnNodes;
	if (type & TYPE_DN_NODE)
		num += NumDnNodes;
	if (num == 0)
	{
		if (node_num)
			*node_num = 0;
		return NULL;
	}

	result = (Oid *) palloc(num * sizeof(Oid));
	num = 0;
	if (type & TYPE_CN_NODE)
	{
		foreach_cn_handles(handle)
		{
			if (handle->node_id == PGXCNodeOid && !include_self)
				continue;

			result[num++] = handle->node_id;
		}
	}

	if (type & TYPE_DN_NODE)
	{
		foreach_dn_handles(handle)
		{
			if (handle->node_id == PGXCNodeOid && !include_self)
				continue;

			result[num++] = handle->node_id;
		}
	}

	if (node_num)
		*node_num = num;

	return result;
}

NodeHandle *
GetPrHandle(void)
{
	return PrHandle;
}

Oid
GetPrNodeID(void)
{
	if (PrHandle)
		return PrHandle->node_id;

	return InvalidOid;
}

bool
IsPrNode(Oid node_id)
{
	if (PrHandle && OidIsValid(PrHandle->node_id))
		return node_id == PrHandle->node_id;

	return false;
}
