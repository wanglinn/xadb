/*-------------------------------------------------------------------------
 *
 * inter-comm.h
 *
 *	  Internode communication interface definition
 *
 *
 * Portions Copyright (c) 2016-2017, ADB Development Group
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/intercomm/inter-comm.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef INTER_COMM_H
#define INTER_COMM_H

#include "datatype/timestamp.h"
#include "intercomm/inter-node.h"
#include "libpq/libpq-node.h"
#include "nodes/bitmapset.h"
#include "optimizer/pgxcplan.h"
#include "pgxc/execRemote.h"
#include "pgxc/remotecopy.h"
#include "utils/snapshot.h"

#define NULL_TAG					""
#define TRANS_BEGIN_TAG				"BEGIN"
#define TRANS_START_TAG				"START TRANSACTION"
#define TRANS_COMMIT_TAG			"COMMIT"
#define TRANS_ROLLBACK_TAG			"ROLLBACK"
#define TRANS_PREPARE_TAG			"PREPARE TRANSACTION"
#define TRANS_COMMIT_PREPARED_TAG	"COMMIT PREPARED"
#define TRANS_ROLLBACK_PREPARED_TAG	"ROLLBACK PREPARED"
#define CLOSE_STMT_TAG				"CLOSE STATEMENT"
#define CLOSE_PORTAL_TAG			"CLOSE PORTAL"
#define CLUSTER_BARRIER_TAG			"CLUSTER BARRIER"

typedef struct InterXactStateData *InterXactState;

/* src/backend/intercomm/inter-comm.c */
extern List *OidArraryToList(MemoryContext context, Oid *oids, int noids);
extern Oid *OidListToArrary(MemoryContext context, List *oid_list, int *noids);
extern void ClusterSyncXid(void);
extern char *HandleGetError(NodeHandle *handle);
extern char *HandleCopyError(NodeHandle *handle);
extern void HandleGC(NodeHandle *handle);
extern void HandleListGC(List *handle_list);
extern void HandleCacheOrGC(NodeHandle *handle);
extern void HandleListCacheOrGC(List *handle_list);
extern bool HandleListFinishCommand(const List *handle_list, const char *commandTag);
extern bool HandleFinishCommand(NodeHandle *handle, const char *commandTag);
extern int HandleBegin(InterXactState state,
					   NodeHandle *handle,
					   GlobalTransactionId xid,
					   TimestampTz timestamp,
					   bool need_xact_block,
					   bool *already_begin);
extern int HandleSendCID(NodeHandle *handle, CommandId cid);
extern int HandleSendGXID(NodeHandle *handle, GlobalTransactionId xid);
extern int HandleSendTimestamp(NodeHandle *handle, TimestampTz timestamp);
extern int HandleSendSnapshot(NodeHandle *handle, Snapshot snapshot);
extern int HandleSendQueryTree(NodeHandle *handle,
							   CommandId cid,
							   Snapshot snapshot,
							   const char *query,
							   StringInfo query_tree);
extern int HandleSendQueryExtend(NodeHandle *handle,
								 CommandId cid,
								 Snapshot snapshot,
								 const char *command,
								 const char *stmtName,
								 const char *portalName,
								 bool sendDescribe,
								 int fetchSize,
								 int nParams,
								 const Oid *paramTypes,
								 const int *paramFormats,
								 const char *paramBinaryValue,
								 const int paramBinaryLength,
								 int nResultFormat,
								 const int *resultFormats);
extern int HandleSendClose(NodeHandle *handle, bool isStatement, const char *name);
extern int HandleSendClusterBarrier(NodeHandle *handle, char cmd_type, const char *barrierID);
extern int HandleClusterBarrier(NodeHandle *handle, char cmd_type, const char *barrierID);
extern int HandleClose(NodeHandle *handle, bool isStatement, const char *name);
extern void HandleListClose(List *handle_list, bool isStatement, const char *name);
extern void HandleResetOwner(NodeHandle *handle);
extern void HandleListResetOwner(List *handle_list);

/* src/backend/intercomm/inter-xact.c */
typedef struct InterXactStateData
{
	MemoryContext			context;
	char				   *gid;
	bool					missing_ok;
	bool					implicit;
	bool					need_xact_block;
	Oid					   *trans_nodes;		/* array of remote nodes already start transaction */
	int						trans_count;		/* remote nodes count */
	int						trans_max;			/* current max malloc count of nodes */
	struct NodeMixHandle   *cur_handle;			/* "cur_handle" is current NodeMixHandle depends
												 * on oid list input, just for one query in the
												 * transaction block */
	struct NodeMixHandle   *all_handle;			/* "all_handle" include all the NodeHandle within
												 * the transaction block, it is used to 2PC */
} InterXactStateData;

extern List *GetPGconnAttatchCurrentInterXact(const List *node_list);
extern List *GetPGconnFromHandleList(List *handle_list);
extern void FreeInterXactState(InterXactState state);
extern InterXactState MakeNewInterXactState(void);
extern InterXactState MakeTopInterXactState(void);
extern InterXactState MakeInterXactState(MemoryContext context, const List *node_list);
extern InterXactState MakeInterXactState2(InterXactState state, const List *node_list);
extern InterXactState ExecInterXactUtility(RemoteQuery *node, InterXactState state);
extern void InterXactSetGID(InterXactState state, const char *gid);
extern void InterXactSetXID(InterXactState state, TransactionId xid);
extern void InterXactSaveBeginNodes(InterXactState state, Oid node);
extern Oid *InterXactBeginNodes(InterXactState state, bool include_self, int *node_num);
extern void InterXactSerializeSnapshot(StringInfo buf, Snapshot snapshot);
extern void InterXactGCCurrent(InterXactState state);
extern void InterXactGCAll(InterXactState state);
extern void InterXactCacheCurrent(InterXactState state);
extern void InterXactCacheAll(InterXactState state);
extern void InterXactUtility(InterXactState state, Snapshot snapshot, const char *utility, StringInfo utility_tree);
extern void InterXactBegin(InterXactState state, const List *node_list);
extern void InterXactPrepare(const char *gid, Oid *nodes, int nnodes);
extern void InterXactCommit(const char *gid, Oid *nodes, int nnodes, bool missing_ok);
extern void InterXactPrepareGtm(const char *gid, Oid *nodes, int nnodes);
extern void InterXactCommitGtm(const char *gid, Oid *nodes, int nnodes, bool missing_ok);
extern void InterXactAbortGtm(const char *gid, Oid *nodes, int nnodes, bool missing_ok, bool normal);
extern void InterXactSendCommit(const char *gid, Oid *nodes, int nnodes, bool missing_ok, bool ignore_error);
extern void InterXactRecvCommit(const char *gid, Oid *nodes, int nnodes, bool missing_ok, bool ignore_error);
extern void InterXactAbort(const char *gid, Oid *nodes, int nnodes, bool missing_ok, bool ignore_error);

extern void RemoteXactCommit(int nnodes, Oid *nodes);
extern void RemoteXactAbort(int nnodes, Oid *nodes, bool normal);
extern void StartFinishPreparedRxact(const char *gid, int nnodes, Oid *nodes, bool isCommit);
extern void EndFinishPreparedRxact(const char *gid, int nnodes, Oid *nodes, bool isMissingOK, bool isCommit);

/* src/backend/intercomm/inter-query.c */
extern struct PGcustumFuns *InterQueryCustomFuncs;
struct pg_conn;
extern int HandleInterUnknownMsg(struct pg_conn *conn, char c, int msgLength);
extern List *GetRemoteNodeList(RemoteQueryState *planstate, ExecNodes *exec_nodes, RemoteQueryExecType exec_type);
extern TupleTableSlot *StartRemoteQuery(RemoteQueryState *node, TupleTableSlot *destslot);
extern TupleTableSlot *FetchRemoteQuery(RemoteQueryState *node, TupleTableSlot *destslot);
extern TupleTableSlot *HandleFetchRemote(NodeHandle *handle, RemoteQueryState *node, TupleTableSlot *destslot,
										 bool blocking, bool batch);
extern void CloseRemoteStatement(const char *stmt_name, Oid *nodes, int nnodes);

/* src/backend/intercomm/inter-copy.c */
extern void StartRemoteCopy(RemoteCopyState *node);
extern void EndRemoteCopy(RemoteCopyState *node);
extern void SendCopyFromHeader(RemoteCopyState *node, const StringInfo header);
extern void DoRemoteCopyFrom(RemoteCopyState *node, const StringInfo line_buf, const List *node_list);
extern uint64 DoRemoteCopyTo(RemoteCopyState *node);

#endif /* INTER_COMM_H */
