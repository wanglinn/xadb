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

/* Helper structure to access Datanode from Session */
typedef enum
{
	DN_CONNECTION_STATE_IDLE,			/* idle, ready for query */
	DN_CONNECTION_STATE_QUERY,			/* query is sent, response expected */
	DN_CONNECTION_STATE_ERROR_FATAL,	/* fatal error */
	DN_CONNECTION_STATE_COPY_IN,
	DN_CONNECTION_STATE_COPY_OUT
}	DNConnectionState;

typedef enum
{
	HANDLE_IDLE,
	HANDLE_ERROR,
	HANDLE_DEFAULT
}	PGXCNode_HandleRequested;

/*
 * Enumeration for two purposes
 * 1. To indicate to the HandleCommandComplete function whether response checking is required or not
 * 2. To enable HandleCommandComplete function to indicate whether the response was a ROLLBACK or not
 * Response checking is required in case of PREPARE TRANSACTION and should not be done for the rest
 * of the cases for performance reasons, hence we have an option to ignore response checking.
 * The problem with PREPARE TRANSACTION is that it can result in a ROLLBACK response
 * yet Coordinator would think it got done on all nodes.
 * If we ignore ROLLBACK response then we would try to COMMIT a transaction that
 * never got prepared, which in an incorrect behavior.
 */
typedef enum
{
	RESP_ROLLBACK_IGNORE,			/* Ignore response checking */
	RESP_ROLLBACK_CHECK,			/* Check whether response was ROLLBACK */
	RESP_ROLLBACK_RECEIVED,			/* Response is ROLLBACK */
	RESP_ROLLBACK_NOT_RECEIVED		/* Response is NOT ROLLBACK */
}RESP_ROLLBACK;

#define DN_CONNECTION_STATE_ERROR(dnconn) \
		((dnconn)->state == DN_CONNECTION_STATE_ERROR_FATAL \
			|| (dnconn)->transaction_status == 'E')

#define HAS_MESSAGE_BUFFERED(conn) \
		((conn)->inCursor + 4 < (conn)->inEnd \
			&& (conn)->inCursor + ntohl(*((uint32_t *) ((conn)->inBuffer + (conn)->inCursor + 1))) < (conn)->inEnd)

#ifdef DEBUG_ADB
#define DEBUG_BUF_SIZE 1024
#endif

typedef struct PGXCNodeHandle
{
	Oid			nodeoid;
	NameData	name;
	char		type;
#ifdef DEBUG_ADB
	char		last_query[DEBUG_BUF_SIZE];
#endif

	/* fd of the connection */
	pgsocket	sock;
	/* Connection state */
	char		transaction_status;
	DNConnectionState state;
	struct RemoteQueryState *combiner;
#ifdef DN_CONNECTION_DEBUG
	bool		have_row_desc;
#endif
	FILE		*file_data;
	char		*error;
	/* Output buffer */
	char		*outBuffer;
	size_t		outSize;
	size_t		outEnd;
	/* Input buffer */
	char		*inBuffer;
	size_t		inSize;
	size_t		inStart;
	size_t		inEnd;
	size_t		inCursor;

	/*
	 * Have a variable to enable/disable response checking and
	 * if enable then read the result of response checking
	 *
	 * For details see comments of RESP_ROLLBACK
	 */
	RESP_ROLLBACK	ck_resp_rollback;
}PGXCNodeHandle;

#define FreeHandleError(handle)								\
	do {													\
		if (((PGXCNodeHandle *) (handle))->error)			\
			pfree(((PGXCNodeHandle *) (handle))->error);	\
		((PGXCNodeHandle *) (handle))->error = NULL;		\
	} while (0)

/* Structure used to get all the handles involved in a transaction */
typedef struct
{
	PGXCNodeHandle	   *primary_handle;	/* Primary connection to PGXC node */
	int					dn_conn_count;	/* number of Datanode Handles including primary handle */
	PGXCNodeHandle	  **datanode_handles;	/* an array of Datanode handles */
	int					co_conn_count;	/* number of Coordinator handles */
	PGXCNodeHandle	  **coord_handles;	/* an array of Coordinator handles */
} PGXCNodeAllHandles;

extern void InitMultinodeExecutor(bool is_force);

/* Open/close connection routines (invoked from Pool Manager) */
extern char *PGXCNodeConnStr(char *host, int port, char *dbname, char *user,
							 char *pgoptions, char *remote_type);
extern int PGXCNodeSendSetQuery(NODE_CONNECTION *conn, const char *sql_command);
extern void PGXCNodeCleanAndRelease(int code, Datum arg);

/* Look at information cached in node handles */
extern int PGXCNodeGetNodeId(Oid nodeoid, char node_type);
extern Oid PGXCNodeGetNodeOid(int nodeid, char node_type);
extern List *PGXCNodeGetNodeOidList(List *list, char node_type);

extern Datum pgxc_execute_on_nodes(int numnodes, Oid *nodelist, char *query);

/* New GUC to store delay value of cancel delay dulation in millisecond */
extern int pgxcnode_cancel_delay;

#endif /* PGXCNODE_H */
