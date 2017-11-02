#ifndef INTER_COMM_H
#define INTER_COMM_H

#include "datatype/timestamp.h"
#include "intercomm/inter-node.h"
#include "nodes/bitmapset.h"
#include "optimizer/pgxcplan.h"
#include "utils/snapshot.h"
#include "libpq/libpq-node.h"

/* src/backend/intercomm/inter-comm.c */
extern bool HandleFinishCommand(const List *handle_list);
extern bool HandleFinishAsync(const List *handle_list);
extern int HandleSendBegin(NodeHandle * handle, GlobalTransactionId xid, TimestampTz timestamp, bool need_xact_block, bool *alreay_begin);
extern int HandleSendGXID(NodeHandle *handle, GlobalTransactionId xid);
extern int HandleSendTimestamp(NodeHandle *handle, TimestampTz timestamp);
extern int HandleSendSnapshot(NodeHandle *handle, Snapshot snapshot);
extern int HandleSendQueryTree(NodeHandle *handle, Snapshot snapshot, const char *query, StringInfo query_tree);

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
	struct NodeMixHandle   *mix_handle;			/* "mix_handle" is current NodeMixHandle depends
												 * on oid list input, just for one query in the
												 * transaction block */
	struct NodeMixHandle   *all_handle;			/* "all_handle" include all the NodeHandle within
												 * the transaction block, it is used to 2PC */
} InterXactStateData;

typedef InterXactStateData *InterXactState;

#define IsAbortBlockState(state)	(((InterXactState) (state))->block_state & IBLOCK_ABORT)

extern bool IsTwoPhaseCommitNeeded(void);
extern const char *GetTopInterXactGID(void);
extern bool IsTopInterXactHastmp(void);
extern void TopInterXactTmpSet(bool hastmp);
extern void ResetInterXactState(InterXactState state);
extern void FreeInterXactState(InterXactState state);
extern InterXactState MakeTopInterXactState(void);
extern InterXactState MakeInterXactState(MemoryContext context, const List *oid_list);
extern InterXactState MakeInterXactState2(InterXactState state, const List *oid_list);
extern InterXactState ExecInterXactUtility(RemoteQuery *node, InterXactState state);
extern void InterXactSetGID(InterXactState state, const char *gid);
extern Oid *InterXactBeginNodes(InterXactState state, bool include_self, int *node_num);
extern void InterXactSaveError(InterXactState state, const char *fmt, ...)
	pg_attribute_printf(2, 3);
extern void InterXactSerializeSnapshot(StringInfo buf, Snapshot snapshot);
extern void InterXactBegin(InterXactState state);
extern void InterXactQuery(InterXactState state, Snapshot snapshot, const char *query, StringInfo query_tree);
extern void InterXactPrepare(const char *gid, Oid *nodes, int nnodes);
extern void InterXactCommit(const char *gid, Oid *nodes, int nnodes);
extern void InterXactAbort(const char *gid, Oid *nodes, int nnodes, bool ignore_error);

extern void RemoteXactCommit(int nnodes, Oid *nodeIds);
extern void RemoteXactAbort(int nnodes, Oid *nodeIds, bool normal);
extern void StartFinishPreparedRxact(const char *gid, int nnodes, Oid *nodeIds, bool isImplicit, bool isCommit);
extern void EndFinishPreparedRxact(const char *gid, int nnodes, Oid *nodeIds, bool isMissingOK, bool isCommit);
/* src/backend/intercomm/inter-query.c */
#endif /* INTER_COMM_H */
