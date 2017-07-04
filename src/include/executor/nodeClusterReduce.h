#ifndef NODE_CLUSTER_REDUCE_H
#define NODE_CLUSTER_REDUCE_H

extern ClusterReduceState *ExecInitClusterReduce(ClusterReduce *node, EState *estate, int eflags);
extern TupleTableSlot *ExecClusterReduce(ClusterReduceState *node);
extern void ExecEndClusterReduce(ClusterReduceState *node);

#endif /* NODE_CLUSTER_REDUCE_H */
