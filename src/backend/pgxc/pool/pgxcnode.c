/*-------------------------------------------------------------------------
 *
 * pgxcnode.c
 *
 *	  Functions for the Coordinator communicating with the PGXC nodes:
 *	  Datanodes and Coordinators
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Portions Copyright (c) 2014-2017, ADB Development Group
 *
 * IDENTIFICATION
 *	  src/backend/pgxc/pool/pgxcnode.c
 *
 *
 *-------------------------------------------------------------------------
 */
#include <sys/select.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>

#include "postgres.h"
#include "miscadmin.h" /* fro CHECK_FOR_INTERRUPTS */

#include "access/transam.h"
#include "access/xact.h"
#include "agtm/agtm.h"
#include "catalog/pg_type.h"
#include "catalog/pg_collation.h"
#include "catalog/pgxc_node.h"
#include "commands/prepare.h"
#include "libpq/libpq-fe.h"
#include "libpq/libpq-int.h"
#include "nodes/nodes.h"
#include "pgxc/pause.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/execRemote.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "pgxc/poolmgr.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "tcop/dest.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/fmgroids.h"
#include "utils/formatting.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#ifdef ADB
#include "pgxc/pause.h"
#endif

#define CMD_ID_MSG_LEN 8
#define PGXC_CANCEL_DELAY 15
#define DEFAULT_HANDLE_INPUT_SIZE	(16*1024)
#define DEFAULT_HANDLE_OUTPUT_SIZE	(16*1024)
#define foreach_all_handle(p)	\
	for(p=all_node_handles;p-all_node_handles<NumAllNodeHandle;p=&p[1])
#define foreach_coord_handle(p)	\
	for(p=co_handles;p-co_handles<NumCoords;p=&p[1])
#define foreach_dn_handle(p)	\
	for(p=dn_handles;p-dn_handles<NumDataNodes;p=&p[1])

/* Number of connections held */
static int	datanode_count = 0;
static int	coord_count = 0;

/*
 * Datanode handles saved in Transaction memory context
 * when PostgresMain is launched.
 * Those handles are used inside a transaction by Coordinator to Datanodes.
 */
static PGXCNodeHandle *dn_handles = NULL;

/*
 * Coordinator handles saved in Transaction memory context
 * when PostgresMain is launched.
 * Those handles are used inside a transaction by Coordinator to Coordinators
 */
static PGXCNodeHandle *co_handles = NULL;

/*
 * memory of all dn_handles and co_handles
 */
static PGXCNodeHandle *all_node_handles = NULL;

/* Current size of dn_handles and co_handles */
volatile int NumDataNodes;
volatile int NumCoords;
static volatile int NumAllNodeHandle;

/* Cancel Delay Duration -> set by GUC */
int			pgxcnode_cancel_delay = 10;

bool		enable_node_tcp_log;
extern int	MyProcPid;

static void pgxc_node_free(PGXCNodeHandle *handle, bool freebuf);
static void pgxc_node_all_free(void);
static PGXCNodeHandle *pgxc_get_node_handle(int nodeid, char node_type);

/*
 * Initialize PGXCNodeHandle struct
 */
static void
init_pgxc_handle(PGXCNodeHandle *pgxc_handle)
{
	Assert(pgxc_handle);
	MemSet(pgxc_handle->name.data, 0, NAMEDATALEN);

#ifdef DEBUG_ADB
	MemSet(pgxc_handle->last_query, 0, DEBUG_BUF_SIZE);
#endif

	/*
	 * Socket descriptor is small non-negative integer,
	 * Indicate the handle is not initialized yet
	 */
	pgxc_handle->sock = PGINVALID_SOCKET;

	/* Initialise buffers */
	pgxc_handle->error = NULL;
	pgxc_handle->outSize = DEFAULT_HANDLE_OUTPUT_SIZE;
	pgxc_handle->outBuffer = (char *) MemoryContextAllocZero(TopMemoryContext, DEFAULT_HANDLE_OUTPUT_SIZE);
	pgxc_handle->inSize = DEFAULT_HANDLE_INPUT_SIZE;
	pgxc_handle->inBuffer = (char *) MemoryContextAllocZero(TopMemoryContext, DEFAULT_HANDLE_INPUT_SIZE);
	pgxc_handle->combiner = NULL;
	pgxc_handle->inStart = 0;
	pgxc_handle->inEnd = 0;
	pgxc_handle->inCursor = 0;
	pgxc_handle->outEnd = 0;
	pgxc_handle->file_data = NULL;
}

/*
 * Allocate and initialize memory to store Datanode and Coordinator handles.
 */
void
InitMultinodeExecutor(bool is_force)
{
	int				count;
	Oid				*coOids = NULL;
	Oid				*dnOids = NULL;
	char			*nodeName = NULL;
	PGXCNodeHandle  *handle;

	/* Free all the existing information first */
	if (is_force)
		pgxc_node_all_free();

	/* This function could get called multiple times because of sigjmp */
	if (dn_handles != NULL &&
		co_handles != NULL)
		return;

	/* Update node table in the shared memory */
	PgxcNodeListAndCount();

	/* Get classified list of node Oids */
	PgxcNodeGetOids(&coOids, &dnOids, (int*)&NumCoords, (int*)&NumDataNodes, true);
	NumAllNodeHandle = NumCoords + NumDataNodes;

	/* Do proper initialization of handles */
	all_node_handles = MemoryContextAllocExtended(TopMemoryContext,
			NumAllNodeHandle * sizeof(all_node_handles[0]), MCXT_ALLOC_NO_OOM|MCXT_ALLOC_ZERO);
	if(all_node_handles == NULL)
	{
		NumAllNodeHandle = NumCoords = NumDataNodes = 0;
		safe_pfree(coOids);
		safe_pfree(dnOids);
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory for node handles")));
		return;	/* keep analyze quiet */
	}

	/* initialize pointer */
	co_handles = NumCoords > 0 ? all_node_handles : NULL;
	dn_handles = NumDataNodes > 0 ? &all_node_handles[NumCoords] : NULL;

	count = 0;
	foreach_coord_handle(handle)
	{
		handle->type = PGXC_NODE_COORDINATOR;
		handle->nodeoid = coOids[count++];
	}
	safe_pfree(coOids);

	count = 0;
	foreach_dn_handle(handle)
	{
		handle->type = PGXC_NODE_DATANODE;
		handle->nodeoid = dnOids[count++];
	}
	safe_pfree(dnOids);

	/*
	 * init handles memory,
	 * first must let sock is invalid
	 */
	foreach_all_handle(handle)
		handle->sock = PGINVALID_SOCKET;
	PG_TRY();
	{
		foreach_all_handle(handle)
		{
			init_pgxc_handle(handle);
			nodeName = get_pgxc_nodename(handle->nodeoid);
			namestrcpy(&(handle->name), nodeName);
			pfree(nodeName);
		}
	}PG_CATCH();
	{
		pgxc_node_all_free();
		PG_RE_THROW();
	}PG_END_TRY();

	datanode_count = 0;
	coord_count = 0;
	PGXCNodeId = 0;

	/* Finally determine which is the node-self */
	count = 0;
	foreach_coord_handle(handle)
	{
		if (pg_strcasecmp(PGXCNodeName, NameStr(handle->name)) == 0)
		{
			PGXCNodeId = count + 1;
			PGXCNodeOid = handle->nodeoid;
			break;
		}
		++count;
	}

	/*
	 * No node-self?
	 * PGXCTODO: Change error code
	 */
	if (PGXCNodeId == 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("Coordinator cannot identify itself")));
}


/*
 * Builds up a connection string
 */
char *
PGXCNodeConnStr(char *host, int port, char *dbname,
				char *user, char *pgoptions, char *remote_type)
{
	return psprintf("host=%s port=%d dbname=%s user=%s application_name=pgxc"
				   " options='-c grammar=postgres -c remotetype=%s %s'",
				   host, port, dbname, user, remote_type, pgoptions);
}

/*
 * Send SET query to given connection.
 * Query is sent asynchronously and results are consumed
 */
int
PGXCNodeSendSetQuery(NODE_CONNECTION *conn, const char *sql_command)
{
	PGresult	*result;
	int res;

	if (!PQsendQuery((PGconn *) conn, sql_command))
		return -1;

	/* Consume results from SET commands */
	res = 0;
	while ((result = PQgetResult((PGconn *) conn)) != NULL)
	{
		if(PQresultStatus(result) == PGRES_FATAL_ERROR)
			res = 1;
		PQclear(result);
	}

	return res;
}

/* Close the socket handle (this process' copy) and free occupied memory
 *
 * Note that we do not free the handle and its members. This will be
 * taken care of when the transaction ends, when TopTransactionContext
 * is destroyed in xact.c.
 */
static void
pgxc_node_free(PGXCNodeHandle *handle, bool freebuf)
{
	if(handle->sock != PGINVALID_SOCKET)
	{
		closesocket(handle->sock);
		handle->sock = PGINVALID_SOCKET;
	}
	handle->state = DN_CONNECTION_STATE_IDLE;
	handle->combiner = NULL;
	FreeHandleError(handle);
	if (freebuf)
	{
		safe_pfree(handle->outBuffer);
		safe_pfree(handle->inBuffer);
	}
	if(handle->file_data)
	{
		char file_name[20];
		sprintf(file_name, "%06d-%06d.bin", MyProcPid, handle->sock);
		FreeFile(handle->file_data);
		handle->file_data = NULL;
		unlink(file_name);
	}
}

/*
 * Free all the node handles cached
 */
static void
pgxc_node_all_free(void)
{
	PGXCNodeHandle *handle;
	foreach_all_handle(handle)
		pgxc_node_free(handle, true);
	NumAllNodeHandle = NumCoords = NumDataNodes = 0;
	pfree(all_node_handles);
	all_node_handles = co_handles = dn_handles = NULL;
}

/*
 * PGXCNode_getNodeId
 *		Look at the data cached for handles and return node position
 */
int
PGXCNodeGetNodeId(Oid nodeoid, char node_type)
{
	PGXCNodeHandle *handles;
	int				num_nodes, i;
	int				res = -1;

	switch (node_type)
	{
		case PGXC_NODE_COORDINATOR:
			num_nodes = NumCoords;
			handles = co_handles;
			break;
		case PGXC_NODE_DATANODE:
			num_nodes = NumDataNodes;
			handles = dn_handles;
			break;
		default:
			/* Should not happen */
			Assert(0);
			return res;
	}

	/* Look into the handles and return correct position in array */
	for (i = 0; i < num_nodes; i++)
	{
		if (handles[i].nodeoid == nodeoid)
		{
			res = i;
			break;
		}
	}
	return res;
}

/*
 * PGXCNode_getNodeOid
 *		Look at the data cached for handles and return node Oid
 */
Oid
PGXCNodeGetNodeOid(int nodeid, char node_type)
{
	PGXCNodeHandle *handles = pgxc_get_node_handle(nodeid, node_type);
	return handles->nodeoid;
}

List *PGXCNodeGetNodeOidList(List *list, char node_type)
{
	List	   *oid_list;
	ListCell   *lc;
	PGXCNodeHandle *handles;
	int array_size;

	switch (node_type)
	{
	case PGXC_NODE_COORDINATOR:
		handles = co_handles;
		array_size = NumCoords;
		break;
	case PGXC_NODE_DATANODE:
		handles = dn_handles;
		array_size = NumDataNodes;
		break;
	default:
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
			errmsg("unknown node type %u", node_type)));
		return NIL;	/* keep compiler quiet */
	}

	oid_list = NIL;
	foreach(lc, list)
	{
		register int x = lfirst_int(lc);
		if(x >= array_size || x < 0)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("Invalid node ID index %d", x),
				errhint("node type '%c', number must between 0 and %d", node_type, array_size-1)));
		}
		oid_list = lappend_oid(oid_list, handles[x].nodeoid);
	}
	return oid_list;
}

static PGXCNodeHandle *pgxc_get_node_handle(int nodeid, char node_type)
{
	PGXCNodeHandle *handles;
	int array_size;

	switch (node_type)
	{
	case PGXC_NODE_COORDINATOR:
		handles = co_handles;
		array_size = NumCoords;
		break;
	case PGXC_NODE_DATANODE:
		handles = dn_handles;
		array_size = NumDataNodes;
		break;
	default:
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
			errmsg("unknown node type %u", node_type)));
		return NULL;	/* keep compiler quiet */
	}

	if(nodeid < 0 || nodeid >= array_size)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("Invalid node ID index %d", nodeid),
				errhint("node type '%c', number must between 0 and %d", node_type, array_size-1)));
		return NULL; /* keep analyze quiet */
	}

	return &(handles[nodeid]);
}

/*
 * pgxc_node_str
 *
 * get the name of the node
 */
Datum
pgxc_node_str(PG_FUNCTION_ARGS)
{
	/* CString compatible Name */
	PG_RETURN_CSTRING(PGXCNodeName);
}

/*
 * adb_node_oid
 *   get the oid of the node
 */
Datum adb_node_oid(PG_FUNCTION_ARGS)
{
	PG_RETURN_OID(PGXCNodeOid);
}
