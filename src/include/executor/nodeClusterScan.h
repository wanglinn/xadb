
/*-------------------------------------------------------------------------
 *
 * src/include/executor/nodeClusterScan.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef NODECLUSTERSCAN_H
#define NODECLUSTERSCAN_H

#include "nodes/execnodes.h"

ClusterScanState *ExecInitClusterScan(ClusterScan *node, EState *estate, int flags);
extern TupleTableSlot *ExecClusterScan(ClusterScanState *node);
extern void ExecEndClusterScan(ClusterScanState *node);
extern void ExecReScanClusterScan(ClusterScanState *node);

#endif /* NODECLUSTERSCAN_H */