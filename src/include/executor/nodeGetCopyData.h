
#ifndef NODE_GET_COPY_DATA_H_
#define NODE_GET_COPY_DATA_H_

extern ClusterGetCopyDataState* ExecInitClusterGetCopyData(ClusterGetCopyData *node, EState *estate, int flags);
extern TupleTableSlot *ExecClusterGetCopyData(ClusterGetCopyDataState *node);
extern void ExecEndClusterGetCopyData(ClusterGetCopyDataState *node);

#endif /* NODE_GET_COPY_DATA_H_ */
