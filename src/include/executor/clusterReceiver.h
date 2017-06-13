#ifndef CLUSTER_RECEIVER_H
#define CLUSTER_RECEIVER_H

#include "tcop/dest.h"
struct pg_conn;

extern DestReceiver *createClusterReceiver(void);
extern bool clusterRecvTuple(TupleTableSlot *slot, const char *msg, int len,
							 PlanState *ps, struct pg_conn *conn);
extern void serialize_instrument_message(PlanState *ps, StringInfo buf);

#endif /* CLUSTER_RECEIVER_H */

