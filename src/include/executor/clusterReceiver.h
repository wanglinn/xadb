#ifndef CLUSTER_RECEIVER_H
#define CLUSTER_RECEIVER_H

#include "tcop/dest.h"

#define CLUSTER_MSG_TUPLE_DESC		'T'
#define CLUSTER_MSG_CONVERT_DESC	't'
#define CLUSTER_MSG_TUPLE_DATA		'D'
#define CLUSTER_MSG_CONVERT_TUPLE	'd'
#define CLUSTER_MSG_INSTRUMENT		'I'
#define CLUSTER_MSG_PROCESSED		'P'
#define CLUSTER_MSG_RDC_PORT		'p'

struct pg_conn;

typedef struct ClusterRecvState
{
	TupleTableSlot *base_slot;
	TupleTableSlot *convert_slot;
	struct TupleTypeConvert *convert;
	PlanState *ps;
	bool convert_slot_is_signal;
}ClusterRecvState;

extern DestReceiver *createClusterReceiver(void);
extern ClusterRecvState *createClusterRecvState(PlanState *ps);
extern void freeClusterRecvState(ClusterRecvState *state);
extern bool clusterRecvSetCheckEndMsg(DestReceiver *r, bool check);
extern bool clusterRecvRdcListenPort(struct pg_conn *conn, const char *msg, int len, int *port);
extern bool clusterRecvTuple(TupleTableSlot *slot, const char *msg, int len,
							 PlanState *ps, struct pg_conn *conn);
extern bool clusterRecvTupleEx(ClusterRecvState *state, const char *msg, int len, struct pg_conn *conn);
extern void serialize_rdc_listen_port_message(StringInfo buf, int port);
extern void serialize_instrument_message(PlanState *ps, StringInfo buf);
#define serialize_slot_head_message(buf, desc) serialize_tuple_desc(buf, desc, CLUSTER_MSG_TUPLE_DESC)
#define serialize_slot_convert_head(buf, desc) serialize_tuple_desc(buf, desc, CLUSTER_MSG_CONVERT_DESC)
extern void serialize_tuple_desc(StringInfo buf, TupleDesc desc, char msg_type);
extern void compare_slot_head_message(const char *msg, int len, TupleDesc desc);
extern TupleDesc restore_slot_head_message(const char *msg, int len);
extern void serialize_slot_message(StringInfo buf, TupleTableSlot *slot, char msg_type);
extern TupleTableSlot* restore_slot_message(const char *msg, int len, TupleTableSlot *slot);
extern void serialize_processed_message(StringInfo buf, uint64 processed);

#endif /* CLUSTER_RECEIVER_H */

