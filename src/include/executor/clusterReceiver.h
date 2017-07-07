#ifndef CLUSTER_RECEIVER_H
#define CLUSTER_RECEIVER_H

#include "tcop/dest.h"
struct pg_conn;

extern DestReceiver *createClusterReceiver(void);
extern bool clusterRecvSetCheckEndMsg(DestReceiver *r, bool check);
extern bool clusterRecvRdcListenPort(struct pg_conn *conn, const char *msg, int len, int *port);
extern bool clusterRecvTuple(TupleTableSlot *slot, const char *msg, int len,
							 PlanState *ps, struct pg_conn *conn);
extern void serialize_rdc_listen_port_message(StringInfo buf, int port);
extern void serialize_instrument_message(PlanState *ps, StringInfo buf);
extern void serialize_slot_head_message(StringInfo buf, TupleDesc desc);
extern void serialize_slot_message(StringInfo buf, TupleTableSlot *slot);
extern void serialize_processed_message(StringInfo buf, uint64 processed);

#endif /* CLUSTER_RECEIVER_H */

