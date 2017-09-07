#include "postgres.h"

#include "intercomm/inter-node.h"
#include "nodes/pg_list.h"
#include "libpq/libpq-fe.h"
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
static int NumMaxHandles = 0;
static volatile int NumAllHandles = 0;
static volatile int NumCnHandles = 0;
static volatile int NumDnHandles = 0;
static NodeHandle *AllHandles = NULL;
static NodeHandle *CnHandles = NULL;
static NodeHandle *DnHandles = NULL;
static bool handle_init = false;

#define foreach_all_handles(p)	\
	for(p=AllHandles;p-AllHandles<NumAllHandles;p=&p[1])
#define foreach_cn_handles(p)	\
	for(p=CnHandles;p-CnHandles<NumCnHandles;p=&p[1])
#define foreach_dn_handles(p)	\
	for(p=DnHandles;p-DnHandles<NumDnHandles;p=&p[1])

static void GetPGconnAttatchToHandle(List *oid_list, List *handle_list);
static List *GetNodeOids(NodeType type, bool include_self);

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
		Assert(NumMaxHandles > 0);
		ResetNodeExecutor();
	}
	NumAllHandles = NumCnHandles = NumDnHandles = 0;
	CnHandles = DnHandles = NULL;
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
		NumMaxHandles = numALL;
	} else if (numALL > NumMaxHandles)
	{
		Assert(NumMaxHandles > 0);
		AllHandles = (NodeHandle *) repalloc(AllHandles, sz);
		NumMaxHandles = numALL;
	} else {
		/* keep compiler quiet */
	}
	sz = NumMaxHandles * sizeof(NodeHandle);
	MemSet(AllHandles, 0, sz);

	NumCnConns = 0;
	NumDnConns = 0;
	PGXCNodeId = 0;
	for (i = 0; i < numALL; i++)
	{
		nodedef = &(all_node_def[i]);
		handle = &(AllHandles[i]);

		handle->node_oid = nodedef->nodeoid;
		handle->node_type = (i < numCN ? TYPE_CN_NODE : TYPE_DN_NODE);
		namecpy(&(handle->node_name), &(nodedef->nodename));
		handle->node_primary = nodedef->nodeisprimary;
		handle->node_conn = NULL;
		handle->node_context = NULL;

		if (handle->node_type == TYPE_CN_NODE &&
			pg_strcasecmp(PGXCNodeName, NameStr(handle->node_name)) == 0)
		{
			PGXCNodeId = i + 1;
			PGXCNodeOid = handle->node_oid;
		}
	}
	safe_pfree(all_node_def);

	NumAllHandles = numALL;
	NumCnHandles = numCN;
	NumDnHandles = numDN;
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
GetNodeHandle(Oid node_oid, bool attatch, void *context)
{
	NodeHandle *handle;

	if (!handle_init)
		return NULL;

	foreach_all_handles(handle)
	{
		if (handle->node_oid == node_oid)
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
GetCnHandle(Oid cn_oid, bool attatch, void *context)
{
	NodeHandle *handle;

	if (!handle_init)
		return NULL;

	foreach_cn_handles(handle)
	{
		if (handle->node_oid == cn_oid)
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
GetDnHandle(Oid dn_oid, bool attatch, void *context)
{
	NodeHandle *handle;

	if (!handle_init)
		return NULL;

	foreach_dn_handles(handle)
	{
		if (handle->node_oid == dn_oid)
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
		List *oid_list = list_make1_oid(handle->node_oid);
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
		handle->node_conn = NULL;
		handle->node_context = NULL;
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

static void
GetPGconnAttatchToHandle(List *oid_list, List *handle_list)
{
	if (oid_list)
	{
		List	   *conn_list = NIL;
		ListCell   *lc_conn, *lc_handle;
		NodeHandle *handle;

		Assert(handle_list && list_length(oid_list) == list_length(handle_list));

		conn_list = PQNGetConnUseOidList(oid_list);
		Assert(list_length(conn_list) == list_length(oid_list));
		forboth (lc_conn, conn_list, lc_handle, handle_list)
		{
			handle = (NodeHandle *) lfirst(lc_handle);
			Assert(PQstatus(handle->node_conn) != CONNECTION_OK);
			handle->node_conn = (PGconn *) lfirst(lc_conn);
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
GetMixedHandles(const List *oid_list, void *context)
{
	ListCell	   *lc_oid;
	List		   *oid_need;
	List		   *handle_need;
	NodeMixHandle  *mix_handle;
	NodeHandle	   *handle;
	Oid				node_oid;

	if (!oid_list || !handle_init)
		return NULL;

	mix_handle = (NodeMixHandle *) palloc0(sizeof(NodeMixHandle));
	oid_need = handle_need = NIL;
	foreach (lc_oid, oid_list)
	{
		node_oid = lfirst_oid(lc_oid);
		handle = GetNodeHandle(node_oid, false, context);
		mix_handle->handles = lappend(mix_handle->handles, handle);
		if (handle->node_primary)
			mix_handle->pr_handle = handle;
		if (PQstatus(handle->node_conn) != CONNECTION_OK)
		{
			/* detach old PGconn if exists */
			HandleDetachPGconn(handle);
			oid_need = lappend_oid(oid_need, node_oid);
			handle_need = lappend(handle_need, handle);
		}
	}
	GetPGconnAttatchToHandle(oid_need, handle_need);
	list_free(oid_need);
	list_free(handle_need);

	return mix_handle;
}

NodeMixHandle *
GetAllHandles(void)
{
	List		   *oid_need;
	List		   *handle_need;
	NodeMixHandle  *mix_handle;
	NodeHandle	   *handle;

	/* do not initialized */
	if (!handle_init)
		return NULL;

	mix_handle = (NodeMixHandle *) palloc0(sizeof(NodeMixHandle));
	oid_need = handle_need = NIL;
	foreach_all_handles(handle)
	{
		mix_handle->handles = lappend(mix_handle->handles, handle);
		if (handle->node_primary)
			mix_handle->pr_handle = handle;
		if (PQstatus(handle->node_conn) != CONNECTION_OK)
		{
			/* detach old PGconn if exists */
			HandleDetachPGconn(handle);
			oid_need = lappend_oid(oid_need, handle->node_oid);
			handle_need = lappend(handle_need, handle);
		}
	}
	GetPGconnAttatchToHandle(oid_need, handle_need);
	list_free(oid_need);
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
	else
		Assert(mix1->pr_handle == mix2->pr_handle);

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
GetAllCnOids(bool include_self)
{
	return GetNodeOids(TYPE_CN_NODE, include_self);
}

List *
GetAllDnOids(bool include_self)
{
	return GetNodeOids(TYPE_DN_NODE, include_self);
}

List *
GetAllNodeOids(bool include_self)
{
	return GetNodeOids(TYPE_CN_NODE | TYPE_DN_NODE, include_self);
}

static List *
GetNodeOids(NodeType type, bool include_self)
{
	List	   *result = NIL;
	NodeHandle *handle;

	if (type & TYPE_CN_NODE)
	{
		foreach_cn_handles(handle)
		{
			if (handle->node_oid == PGXCNodeOid && !include_self)
				continue;

			result = lappend_oid(result, handle->node_oid);
		}
	}

	if (type & TYPE_DN_NODE)
	{
		foreach_dn_handles(handle)
		{
			if (handle->node_oid == PGXCNodeOid && !include_self)
				continue;

			result = lappend_oid(result, handle->node_oid);
		}
	}

	return result;
}

#if NOT_USED
NodeMixHandle *
GetMixedHandles(List *cnlist, List *dnlist)
{
	NodeMixHandle  *mix_handle;
	NodeHandle	   *handle;
	List		   *conn_need = NIL;
	List		   *handle_need = NIL;
	ListCell	   *lc = NULL;
	Oid				node_oid;
	int				i;

	/* no node need to get handle */
	if (!cnlist && !dnlist)
		return NULL;

	if (!handle_init)
		return NULL;

	mix_handle = (NodeMixHandle *) palloc0(sizeof(NodeMixHandle));
	if ((mix_handle->cn_count = list_length(cnlist)) > 0)
		mix_handle->cn_handles = (NodeHandle **)
					palloc0(sizeof(NodeHandle *) * mix_handle->cn_count);
	if ((mix_handle->dn_count = list_length(dnlist)) > 0)
		mix_handle->dn_handles = (NodeHandle **)
					palloc0(sizeof(NodeHandle *) * mix_handle->dn_count);

	for (lc = list_head(cnlist), i = 0; lc != NULL; lc = lnext(lc), i++)
	{
		node_oid = lfirst_oid(lc);
		handle = GetCnHandle(node_oid, false);
		if (!handle)
			ereport(ERROR,
					(errmsg("is this node(%u) a coordinator?", node_oid)));
		mix_handle->cn_handles[i] = handle;
		if (PQstatus(handle->node_conn) != CONNECTION_OK)
		{
			/* detach old PGconn if exists */
			HandleDetachPGconn(handle);
			conn_need = lappend_oid(conn_need, node_oid);
			handle_need = lappend(handle_need, handle);
		}
	}

	for (lc = list_head(dnlist), i = 0; lc != NULL; lc = lnext(lc), i++)
	{
		node_oid = lfirst_oid(lc);
		handle = GetDnHandle(node_oid, false);
		if (!handle)
			ereport(ERROR,
					(errmsg("is this node(%u) a datanode?", node_oid)));
		mix_handle->dn_handles[i] = handle;
		if (handle->node_primary)
			mix_handle->pr_handle = handle;
		if (PQstatus(handle->node_conn) != CONNECTION_OK)
		{
			/* detach old PGconn if exists */
			HandleDetachPGconn(handle);
			conn_need = lappend_oid(conn_need, node_oid);
			handle_need = lappend(handle_need, handle);
		}
	}

	GetPGconnAttatchToHandle(conn_need, handle_need);
	list_free(conn_need);
	list_free(handle_need);

	return mix_handle;
}

NodeMixHandle *
GetAllHandles(void)
{
	NodeMixHandle  *mix_handle;
	NodeHandle	   *handle;
	List		   *conn_need = NIL;
	List		   *handle_need = NIL;
	int				i;

	if (!handle_init)
		return NULL;

	mix_handle = (NodeMixHandle *) palloc0(sizeof(NodeMixHandle));
	if ((mix_handle->cn_count = NumCnHandles) > 0)
		mix_handle->cn_handles = (NodeHandle **)
					palloc0(sizeof(NodeHandle *) * mix_handle->cn_count);
	if ((mix_handle->dn_count = NumDnHandles) > 0)
		mix_handle->dn_handles = (NodeHandle **)
					palloc0(sizeof(NodeHandle *) * mix_handle->dn_count);

	for (i = 0; i < NumCnHandles; i++)
	{
		handle = &(CnHandles[i]);
		mix_handle->cn_handles[i] = handle;
		if (PQstatus(handle->node_conn) != CONNECTION_OK)
		{
			/* detach old PGconn if exists */
			HandleDetachPGconn(handle);
			conn_need = lappend_oid(conn_need, handle->node_oid);
			handle_need = lappend(handle_need, handle);
		}
	}

	for (i = 0; i < NumDnHandles; i++)
	{
		handle = &(DnHandles[i]);
		mix_handle->dn_handles[i] = handle;
		if (handle->node_primary)
			mix_handle->pr_handle = handle;
		if (PQstatus(handle->node_conn) != CONNECTION_OK)
		{
			/* detach old PGconn if exists */
			HandleDetachPGconn(handle);
			conn_need = lappend_oid(conn_need, handle->node_oid);
			handle_need = lappend(handle_need, handle);
		}
	}

	GetPGconnAttatchToHandle(conn_need, handle_need);
	list_free(conn_need);
	list_free(handle_need);

	return mix_handle;
}
#endif
