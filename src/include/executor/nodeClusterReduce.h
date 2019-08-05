#ifndef NODE_CLUSTER_REDUCE_H
#define NODE_CLUSTER_REDUCE_H

extern ClusterReduceState *ExecInitClusterReduce(ClusterReduce *node, EState *estate, int eflags);
extern void ExecEndClusterReduce(ClusterReduceState *node);
extern void ExecClusterReduceMarkPos(ClusterReduceState *node);
extern void ExecClusterReduceRestrPos(ClusterReduceState *node);
extern void ExecReScanClusterReduce(ClusterReduceState *node);
extern void TopDownDriveClusterReduce(PlanState *node);

#endif /* NODE_CLUSTER_REDUCE_H */
