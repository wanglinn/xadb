#ifndef NODE_CLUSTER_REDUCE_H
#define NODE_CLUSTER_REDUCE_H

extern ClusterReduceState *ExecInitClusterReduce(ClusterReduce *node, EState *estate, int eflags);
extern void ExecEndClusterReduce(ClusterReduceState *node);
extern void ExecClusterReduceMarkPos(ClusterReduceState *node);
extern void ExecClusterReduceRestrPos(ClusterReduceState *node);
extern void ExecReScanClusterReduce(ClusterReduceState *node);
extern void TopDownDriveClusterReduce(PlanState *node);

/* parallel scan support */
extern void ExecClusterReduceEstimate(ClusterReduceState *node, ParallelContext *pcxt);
extern void ExecClusterReduceInitializeDSM(ClusterReduceState *node, ParallelContext *pcxt);
extern void ExecClusterReduceReInitializeDSM(ClusterReduceState *node, ParallelContext *pcxt);
extern void ExecClusterReduceInitializeWorker(ClusterReduceState *node,
											  ParallelWorkerContext *pwcxt);
#endif /* NODE_CLUSTER_REDUCE_H */
