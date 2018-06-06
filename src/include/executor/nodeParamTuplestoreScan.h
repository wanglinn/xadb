/*-------------------------------------------------------------------------
 *
 * nodeNamedtuplestorescan.h
 *
 * src/include/executor/nodeParamTuplestoreScan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEPARAMTUPLESTORESCAN_H
#define NODEPARAMTUPLESTORESCAN_H

#include "nodes/execnodes.h"

extern ParamTuplestoreScanState *ExecInitParamTuplestoreScan(ParamTuplestoreScan *node, EState *estate, int eflags);
extern void ExecEndParamTuplestoreScan(ParamTuplestoreScanState *node);
extern void ExecReScanParamTuplestoreScan(ParamTuplestoreScanState *node);

#endif							/* NODEPARAMTUPLESTORESCAN_H */
