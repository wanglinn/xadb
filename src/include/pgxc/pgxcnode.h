/*-------------------------------------------------------------------------
 *
 * pgxcnode.h
 *
 *	  Utility functions to communicate to Datanodes and Coordinators
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012, Postgres-XC Development Group
 * Portions Copyright (c) 2014-2017, ADB Development Group
 *
 * IDENTIFICATION
 * 		src/include/pgxc/pgxcnode.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PGXCNODE_H
#define PGXCNODE_H

#include <unistd.h>

#include "postgres.h"
#include "nodes/pg_list.h"
#include "utils/snapshot.h"
#include "utils/timestamp.h"

/* Connection to Datanode maintained by Pool Manager */
typedef struct PGconn NODE_CONNECTION;
typedef struct PGcancel NODE_CANCEL;

#ifdef WITH_RDMA
extern char *PGXCNodeRsConnStr(char *hostaddr, int port, const char *dbname,
				const char *user, const char *pgoptions, char *remote_type);
#endif

/* Open/close connection routines (invoked from Pool Manager) */
extern char *PGXCNodeConnStr(char *host, int port, char *dbname, char *user,
							 char *pgoptions, char *remote_type);
extern int PGXCNodeSendSetQuery(NODE_CONNECTION *conn, const char *sql_command);
extern void PGXCNodeCleanAndRelease(int code, Datum arg);

extern Datum pgxc_execute_on_nodes(int numnodes, Oid *nodelist, const char *query);

/* New GUC to store delay value of cancel delay dulation in millisecond */
extern int pgxcnode_cancel_delay;

#endif /* PGXCNODE_H */
