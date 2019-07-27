/*-------------------------------------------------------------------------
 *
 * dynamicreduce.h
 *	  Dynamic reduce tuples in cluster
 *
 * Portions Copyright (c) 2019, AntDB Development Group
 *
 * src/include/utils/dynamicreduce.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DYNAMIC_REDUCE_H_
#define DYNAMIC_REDUCE_H_

#include "lib/stringinfo.h"
#include "storage/shm_mq.h"

#define ADB_DYNAMIC_REDUCE_QUERY_SIZE	(64*1024)	/* 64K */

#define DR_MSG_SEND	0x1
#define DR_MSG_RECV	0x2

typedef struct DynamicReduceNodeInfo
{
	Oid			node_oid;
	int			pid;
	uint16		port;
	NameData	host;
	NameData	name;
}DynamicReduceNodeInfo;

typedef struct DynamicReduceMQData
{
	char	worker_sender_mq[ADB_DYNAMIC_REDUCE_QUERY_SIZE];
	char	reduce_sender_mq[ADB_DYNAMIC_REDUCE_QUERY_SIZE];
}DynamicReduceMQData,*DynamicReduceMQ;

struct tupleDesc;
struct TupleTableSlot;	/* avoid include tuptable.h */
struct OidBufferData;			/* avoid include pg_list.h */
extern PGDLLIMPORT bool is_reduce_worker;

#define IsDynamicReduceWorker()		(is_reduce_worker)

extern void DynamicReduceWorkerMain(Datum main_arg);
extern uint16 StartDynamicReduceWorker(void);
extern void StopDynamicReduceWorker(void);
extern void ResetDynamicReduceWork(void);
extern void DynamicReduceConnectNet(const DynamicReduceNodeInfo *info, uint32 count);

extern void DynamicReduceStartNormalPlan(int plan_id, struct dsm_segment *seg, DynamicReduceMQ mq, struct tupleDesc *desc);

extern bool DynamicReduceRecvTuple(shm_mq_handle *mqh, struct TupleTableSlot *slot, StringInfo buf,
								   Oid *nodeoid, bool nowait);
extern int DynamicReduceSendOrRecvTuple(shm_mq_handle *mqsend, shm_mq_handle *mqrecv,
										StringInfo send_buf, struct TupleTableSlot *slot_recv, StringInfo recv_buf);
extern bool DynamicReduceSendMessage(shm_mq_handle *mqh, Size nbytes, void *data, bool nowait);

extern void SerializeEndOfPlanMessage(StringInfo buf);
extern bool SendEndOfPlanMessageToMQ(shm_mq_handle *mqh, bool nowait);
extern bool SendRejectPlanMessageToMQ(shm_mq_handle *mqh, bool nowait);

extern void SerializeDynamicReducePlanData(StringInfo buf, const void *data, uint32 len, struct OidBufferData *target);
extern void SerializeDynamicReduceSlot(StringInfo buf, struct TupleTableSlot *slot, struct OidBufferData *target);

extern void SerializeDynamicReduceNodeInfo(StringInfo buf, const DynamicReduceNodeInfo *info, uint32 count);
extern uint32 RestoreDynamicReduceNodeInfo(StringInfo buf, DynamicReduceNodeInfo **info);

#endif /* DYNAMIC_REDUCE_H_ */
