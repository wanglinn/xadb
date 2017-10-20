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

typedef struct InterXactStateData *InterXactState;

/* src/backend/intercomm/inter-comm.c */
extern List *OidArraryToList(MemoryContext context, Oid *oids, int noids);
extern Oid *OidListToArrary(MemoryContext context, List *oid_list, int *noids);
extern void ClusterSyncXid(void);
extern char *HandleGetError(NodeHandle *handle, bool copy);
extern void HandleGC(NodeHandle *handle);
extern void HandleListGC(List *handle_list);
extern void HandleCache(NodeHandle *handle);
extern void HandleListCache(List *handle_list);
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
extern int HandleClose(NodeHandle *handle, bool isStatement, const char *name);
extern void HandleListClose(List *handle_list, bool isStatement, const char *name);
extern void HandleResetOwner(NodeHandle *handle);
extern void HandleListResetOwner(List *handle_list);

#if 0
typedef enum
{
	HOOK_RET_REMOVE,
	HOOK_RET_CONTINUE,
	HOOK_RET_BREAK,
} HookReturnType;

typedef enum {
	HOOK_RES_NONE,
	HOOK_RES_RESULT,
	HOOK_RES_COPY_BUFFER,
	HOOK_RES_COPY_OUT_END,
	HOOK_RES_COPY_IN_END,
	HOOK_RES_ERROR,
} HookResultType;

typedef struct HookResult
{
	HookResultType			res_type;
	union {
		struct pg_result   *res_value;
		StringInfoData		str_value;
	} value;
} HookResult;

typedef HookReturnType (*HandleFinishHook)(void *result, void *handle, void *context);
extern void HandleFinishList(List *handle_list, HandleFinishHook hook, void *context);
extern int HandleFinishOne(NodeHandle *handle, HandleFinishHook hook, void *context);
#endif

/* src/backend/intercomm/inter-xact.c */
typedef struct InterXactStateData
{
	MemoryContext			context;
	StringInfo				error;
	IBlockState				block_state;		/* IBlockState of inter transaction */
	CombineType				combine_type;
	char				   *gid;
	bool					missing_ok;
	bool					hastmp;
	bool					implicit;
	bool					ignore_error;
	bool					need_xact_block;
	List				   *trans_nodes;		/* list of nodes already start transaction */
	struct NodeMixHandle   *mix_handle;			/* "mix_handle" is current NodeMixHandle depends
												 * on oid list input, just for one query in the
												 * transaction block */
	struct NodeMixHandle   *all_handle;			/* "all_handle" include all the NodeHandle within
												 * the transaction block, it is used to 2PC */
} InterXactStateData;

#define IsAbortBlockState(state)	(((InterXactState) (state))->block_state & IBLOCK_ABORT)

extern bool IsTwoPhaseCommitNeeded(void);
extern const char *GetTopInterXactGID(void);
extern bool IsTopInterXactHastmp(void);
extern void TopInterXactTmpSet(bool hastmp);
extern void ResetInterXactState(InterXactState state);
extern void FreeInterXactState(InterXactState state);
extern InterXactState MakeTopInterXactState(void);
extern InterXactState MakeInterXactState(MemoryContext context, const List *node_list);
extern InterXactState MakeInterXactState2(InterXactState state, const List *node_list);
extern InterXactState ExecInterXactUtility(RemoteQuery *node, InterXactState state);
extern void InterXactSetGID(InterXactState state, const char *gid);
extern void InterXactSaveBeginNodes(InterXactState state, Oid node);
extern Oid *InterXactBeginNodes(InterXactState state, bool include_self, int *node_num);
extern void InterXactSaveError(InterXactState state, const char *fmt, ...)
	pg_attribute_printf(2, 3);
extern void InterXactSaveHandleError(InterXactState state, NodeHandle *handle);
extern void InterXactSerializeSnapshot(StringInfo buf, Snapshot snapshot);
extern void InterXactGC(InterXactState state);
extern void InterXactUtility(InterXactState state, Snapshot snapshot, const char *utility, StringInfo utility_tree);
extern void InterXactPrepare(const char *gid, Oid *nodes, int nnodes);
extern void InterXactCommit(const char *gid, Oid *nodes, int nnodes, bool missing_ok);
extern void InterXactAbort(const char *gid, Oid *nodes, int nnodes, bool missing_ok, bool ignore_error);

extern void RemoteXactCommit(int nnodes, Oid *nodes);
extern void RemoteXactAbort(int nnodes, Oid *nodes, bool normal);
extern void StartFinishPreparedRxact(const char *gid, int nnodes, Oid *nodes, bool isImplicit, bool isCommit);
extern void EndFinishPreparedRxact(const char *gid, int nnodes, Oid *nodes, bool isMissingOK, bool isCommit);

/* src/backend/intercomm/inter-query.c */
extern struct PGcustumFuns *InterQueryCustomFuncs;
extern List *GetRemoteNodeList(RemoteQueryState *planstate, ExecNodes *exec_nodes, RemoteQueryExecType exec_type);
extern TupleTableSlot *StartRemoteQuery(RemoteQueryState *node, TupleTableSlot *slot);
extern TupleTableSlot *FetchRemoteQuery(RemoteQueryState *node, TupleTableSlot *slot);
extern TupleTableSlot *HandleFetchRemote(NodeHandle *handle, RemoteQueryState *node, TupleTableSlot *slot,
										 bool blocking, bool batch);
extern void CloseRemoteStatement(const char *stmt_name, Oid *nodes, int nnodes);

/* src/backend/intercomm/inter-copy.c */
extern void StartRemoteCopy(RemoteCopyState *node);
extern uint64 FinishRemoteCopyOut(RemoteCopyState *node);

#endif /* INTER_COMM_H */
