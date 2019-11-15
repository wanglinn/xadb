#ifndef REDUCE_SCAN_H_
#define REDUCE_SCAN_H_

#include "nodes/execnodes.h"

extern ReduceScanState *ExecInitReduceScan(ReduceScan *node, EState *estate, int eflags);
extern void ExecEndReduceScan(ReduceScanState *node);
extern void ExecReduceScanMarkPos(ReduceScanState *node);
extern void ExecReduceScanRestrPos(ReduceScanState *node);
extern void ExecReScanReduceScan(ReduceScanState *node);

extern void FetchReduceScanOuter(ReduceScanState *node);
extern void BeginDriveClusterReduce(PlanState *node);
#endif /* REDUCE_SCAN_H_ */
