
#ifndef NODE_CLUSTER_MERGE_GAHTER_H
#define NODE_CLUSTER_MERGE_GAHTER_H

#include "nodes/execnodes.h"

extern ClusterMergeGatherState *ExecInitClusterMergeGather(ClusterMergeGather *node, EState *estate, int eflags);
extern void ExecEndClusterMergeGather(ClusterMergeGatherState *node);
extern void ExecReScanClusterMergeGather(ClusterMergeGatherState *node);

#endif /* NODE_CLUSTER_MERGE_GAHTER_H */
