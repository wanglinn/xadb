
/*-------------------------------------------------------------------------
 *
 * src/include/executor/nodeClusterGather.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef NODECLUSTERGATHER_H
#define NODECLUSTERGATHER_H

#include "nodes/execnodes.h"

ClusterGatherState *ExecInitClusterGather(ClusterGather *node, EState *estate, int flags);
extern TupleTableSlot *ExecClusterGather(ClusterGatherState *node);
extern void ExecEndClusterGather(ClusterGatherState *node);
extern void ExecReScanClusterGather(ClusterGatherState *node);

#endif /* NODECLUSTERGATHER_H */