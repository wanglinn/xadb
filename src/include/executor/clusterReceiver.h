#ifndef CLUSTER_RECEIVER_H
#define CLUSTER_RECEIVER_H

#include "tcop/dest.h"

extern DestReceiver *createClusterReceiver(void);
extern bool clusterRecvTuple(TupleTableSlot *slot, const char *msg, int len);

#endif /* CLUSTER_RECEIVER_H */

