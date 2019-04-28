#ifndef NODE_CONNECT_BY_H
#define NODE_CONNECT_BY_H

#include "nodes/execnodes.h"

extern ConnectByState* ExecInitConnectBy(ConnectByPlan *node, EState *estate, int eflags);
extern void ExecEndConnectBy(ConnectByState *node);
extern void ExecReScanConnectBy(ConnectByState *node);

#endif							/* NODE_CONNECT_BY_H */