
#ifndef NODE_BATCH_SORT_H
#define NODE_BATCH_SORT_H

#include "nodes/execnodes.h"

extern BatchSortState *ExecInitBatchSort(BatchSort *node, EState *estate, int eflags);
extern void ExecEndBatchSort(BatchSortState *node);
extern void ExecReScanBatchSort(BatchSortState *node);

#endif							/* NODE_BATCH_SORT_H */