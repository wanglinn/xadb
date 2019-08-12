/*-------------------------------------------------------------------------
 *
 * execRemote.h
 *
 *	  Functions to execute commands on multiple Datanodes
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012, Postgres-XC Development Group
 * Portions Copyright (c) 2014-2017, ADB Development Group
 *
 * IDENTIFICATION
 * 		src/include/pgxc/execRemote.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef EXECREMOTE_H
#define EXECREMOTE_H

#include "locator.h"
#include "pgxcnode.h"

#include "access/tupdesc.h"
#include "access/xlog.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "optimizer/pgxcplan.h"
#include "tcop/dest.h"
#include "tcop/pquery.h"
#include "utils/snapshot.h"

/* GUC parameters */
extern bool RequirePKeyForRepTab;

/*
 * Type of requests associated to a remote COPY OUT
 */
typedef enum
{
	REMOTE_COPY_NONE,		/* Not defined yet */
	REMOTE_COPY_STDOUT,		/* Send back to client */
	REMOTE_COPY_FILE,		/* Write in file */
	REMOTE_COPY_TUPLESTORE	/* Store data in tuplestore */
} RemoteCopyType;

/* Combines results of INSERT statements using multiple values */
typedef struct CombineTag
{
	CmdType cmdType;						/* DML command type */
	char	data[COMPLETION_TAG_BUFSIZE];	/* execution result combination data */
} CombineTag;

/*
 * Represents a DataRow message received from a remote node.
 * Contains originating node number and message body in DataRow format without
 * message code and length. Length is separate field
 */
typedef struct RemoteDataRowData
{
	char	*msg;					/* last data row message */
	int 	msglen;					/* length of the data row message */
	int 	msgnode;				/* node number of the data row message */
} 	RemoteDataRowData;
typedef RemoteDataRowData *RemoteDataRow;

typedef struct RemoteQueryState
{
	ScanState	ss;						/* its first field is NodeTag */
	List	   *cur_handles;			/* current participating nodes */
	List	   *all_handles;			/* all participating nodes */
	TupleTableSlot *iterSlot;			/* used to keep slot as an iterator, see HandleCopyOutData */
	TupleTableSlot *convertSlot;		/* used to convert scan slot if needed */
	struct ClusterRecvState *recvState;		/* used to convert scan slot if needed */
	struct ReduceExprState *reduce_state;
	CombineType combine_type;			/* see CombineType enum */
	int			command_complete_count; /* count of received CommandComplete messages */
	int			command_error_count;	/* count of received Error messages */
	TupleDesc	tuple_desc;				/* tuple descriptor to be referenced by emitted tuples */
	int			description_count;		/* count of received RowDescription messages */
	bool		query_Done;				/* query has been sent down to Datanodes */
	RemoteDataRowData currentRow;		/* next data ro to be wrapped into a tuple */
	/*
	 * To handle special case - if this RemoteQuery is feeding sorted data to
	 * Sort plan and if the connection fetching data from the Datanode
	 * is buffered. If EOF is reached on a connection it should be removed from
	 * the array, but we need to know node number of the connection to find
	 * messages in the buffer. So we store nodenum to that array if reach EOF
	 * when buffering
	 */
	int 	   *tapenodes;
	RemoteCopyType remoteCopyType;		/* Type of remote COPY operation */
	FILE	   *copy_file;      		/* used if remoteCopyType == REMOTE_COPY_FILE */
	uint64		processed;				/* count of data rows when running CopyOut */
	/* cursor support */
	char	   *cursor;					/* cursor name */
	char	   *update_cursor;			/* throw this cursor current tuple can be updated */
	int			cursor_count;			/* total count of participating nodes */
	/* Support for parameters */
	char	   *paramval_data;		/* parameter data, format is like in BIND */
	int			paramval_len;		/* length of parameter values data */
	Oid		   *rqs_param_types;	/* Types of the remote params */
	int			rqs_num_params;

	int			eflags;			/* capability flags to pass to tuplestore */
	bool		eof_underlying; /* reached end of underlying plan? */
	Tuplestorestate *tuplestorestate;
	CommandId	rqs_cmd_id;			/* Cmd id to use in some special cases */
	uint32		rqs_processed;			/* Number of rows processed (only for DMLs) */
}	RemoteQueryState;

typedef void (*xact_callback) (bool isCommit, void *args);

extern int ExecCountSlotsRemoteQuery(RemoteQuery *node);
extern RemoteQueryState *ExecInitRemoteQuery(RemoteQuery *node, EState *estate, int eflags);
extern TupleTableSlot* ExecRemoteQuery(PlanState *ps);
extern void ExecEndRemoteQuery(RemoteQueryState *step);

extern void HandleCmdComplete(CmdType commandType, CombineTag *combine, const char *msg_body, size_t len);

extern void ExecRemoteQueryReScan(RemoteQueryState *node, ExprContext *exprCtxt);

extern void SetDataRowForExtParams(ParamListInfo params, RemoteQueryState *rq_state);

/* Flags related to temporary objects included in query */
extern TupleTableSlot * ExecProcNodeDMLInXC(EState *estate, TupleTableSlot *sourceDataSlot, TupleTableSlot *newDataSlot);

extern void AtEOXact_DBCleanup(bool isCommit);

extern void set_dbcleanup_callback(xact_callback function, void *paraminfo, int paraminfo_size);

extern RemoteQueryState *CreateRemoteQueryState(int node_count, CombineType combine_type);
extern void CloseRemoteQueryState(RemoteQueryState *combiner);
#endif
