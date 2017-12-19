#ifndef NODE_CLUSTER_REDUCE_H
#define NODE_CLUSTER_REDUCE_H

extern void RegisterReduceCleanup(void *crstate);
extern void UnregisterReduceCleanup(void);
extern void ReduceCleanup(void);
extern ClusterReduceState *ExecInitClusterReduce(ClusterReduce *node, EState *estate, int eflags);
extern TupleTableSlot *ExecClusterReduce(ClusterReduceState *node);
extern void ExecEndClusterReduce(ClusterReduceState *node);
extern void ExecClusterReduceMarkPos(ClusterReduceState *node);
extern void ExecClusterReduceRestrPos(ClusterReduceState *node);
extern void ExecConnectReduce(PlanState *node);
extern void ExecReScanClusterReduce(ClusterReduceState *node);
extern void TopDownDriveClusterReduce(PlanState *node);

#endif /* NODE_CLUSTER_REDUCE_H */
