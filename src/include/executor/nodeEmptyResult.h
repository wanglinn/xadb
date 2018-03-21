#ifndef NODE_EMPTY_RESULT_H
#define NODE_EMPTY_RESULT_H

#include "nodes/execnodes.h"

extern EmptyResultState *ExecInitEmptyResult(EmptyResult *node, EState *estate, int eflags);
extern void ExecEndEmptyResult(EmptyResultState *node);
extern void ExecEmptyResultMarkPos(EmptyResultState *node);
extern void ExecEmptyResultRestrPos(EmptyResultState *node);
extern void ExecReScanEmptyResult(EmptyResultState *node);
extern Node* MultiExecEmptyResult(EmptyResultState *node);

extern Plan* MakeEmptyResultPlan(Plan *from);

#endif /* NODE_EMPTY_RESULT_H */
