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

#include "postgres.h"
#include "miscadmin.h" /* fro CHECK_FOR_INTERRUPTS */

#include "access/transam.h"
#include "access/xact.h"
#include "agtm/agtm.h"
#include "catalog/pg_type.h"
#include "catalog/pg_collation.h"
#include "catalog/pgxc_node.h"
#include "commands/prepare.h"
#include "libpq-fe.h"
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


/* Cancel Delay Duration -> set by GUC */
int			pgxcnode_cancel_delay = 10;

extern int	MyProcPid;

/*
 * Builds up a connection string
 */
char *
PGXCNodeConnStr(char *host, int port, char *dbname,
				char *user, char *pgoptions, char *remote_type)
{
	return psprintf("host=%s port=%d dbname=%s user=%s application_name=pgxc"
				   " options='-c remotetype=%s %s"
#ifdef ADB_MULTI_GRAM
				   " -c grammar=postgres"
#endif /* ADB_MULTI_GRAM */
				   "'",
				   host, port, dbname, user, remote_type, pgoptions);
}

#ifdef WITH_RDMA
/*
 * Builds up a rsocket connection string
 */
char *
PGXCNodeRsConnStr(char *hostaddr, int port, const char *dbname,
				const char *user, const char *pgoptions, char *remote_type)
{
	return psprintf("hostaddr=%s port=%d dbname=%s user=%s application_name=pgxc"
				   " rdma=1 options='-c remotetype=%s %s"
#ifdef ADB_MULTI_GRAM
				   " -c grammar=postgres"
#endif /* ADB_MULTI_GRAM */
				   "'",
				   hostaddr, port, dbname, user, remote_type, pgoptions);
}
#endif

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
