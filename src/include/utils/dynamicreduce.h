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

#include "access/attnum.h"
#include "executor/tuptable.h"
#include "lib/oidbuffer.h"
#include "lib/stringinfo.h"
#include "storage/buffile.h"
#include "storage/shm_mq.h"
#include "utils/dsa.h"
#include "utils/sharedtuplestore.h"

#define ADB_DYNAMIC_REDUCE_QUERY_SIZE	(64*1024)	/* 64K */

#define DR_MSG_INVALID		0x0
#define DR_MSG_SEND			0x1		/* send success */
#define DR_MSG_RECV			0x2		/* recv a tuple */
#define DR_MSG_RECV_SF		0x4		/* recv a shared file */
#define DR_MSG_RECV_STS		0x8		/* recv a shared tuple store */

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

/* for SharedFileSet plan */
typedef struct DynamicReduceSFSData
{
	DynamicReduceMQData	mq;
	SharedFileSet		sfs;
}DynamicReduceSFSData, *DynamicReduceSFS;

typedef struct DynamicReduceSTSData
{
	DynamicReduceSFSData	sfs;
	char					padding[sizeof(DynamicReduceSFSData) % MAXIMUM_ALIGNOF ?
									MAXIMUM_ALIGNOF-sizeof(DynamicReduceSFSData)%MAXIMUM_ALIGNOF : 0];
	/* shared tuplestore start address */
	char					sts[FLEXIBLE_ARRAY_MEMBER];
}DynamicReduceSTSData, *DynamicReduceSTS;
#define DRSTSD_SIZE(npart, count)													\
	(StaticAssertExpr(offsetof(DynamicReduceSTSData, sts) % MAXIMUM_ALIGNOF == 0,	\
					  "sts not align to max"),										\
	offsetof(DynamicReduceSTSData, sts) + MAXALIGN(sts_estimate(npart)) * (count))

#define DRSTSD_ADDR(st, npart, offset)	\
	(SharedTuplestore*)((char*)st + MAXALIGN(sts_estimate(npart)) * offset)

struct SharedTuplestoreAccessor;	/* avoid include sharedtuplestore.h */
typedef struct DynamicReduceIOBuffer
{
	shm_mq_handle		   *mqh_sender;
	shm_mq_handle		   *mqh_receiver;
	TupleTableSlot		   *slot_remote;
	struct ExprContext	   *econtext;
	struct ReduceExprState *expr_state;
	TupleTableSlot		   *(*FetchLocal)(void *user_data, struct ExprContext *econtext);
	void				   *user_data;
	struct BufFile		   *shared_file;
	struct SharedTuplestoreAccessor
						   *sts;
	dsa_pointer				sts_dsa_ptr;
	OidBufferData			tmp_buf;
	StringInfoData			send_buf;
	StringInfoData			recv_buf;
	uint32					shared_file_no;
	bool					eof_local;
	bool					eof_remote;
}DynamicReduceIOBuffer;

typedef union DynamicReduceRecvInfo
{
	int8	i8;
	uint8	u8;
	int16	i16;
	uint16	u16;
	int32	i32;
	uint32	u32;
	int64	i64;
	uint64	u64;
	Oid		oid;
	dsa_pointer dp;
	void   *pointer;
}DynamicReduceRecvInfo;

extern PGDLLIMPORT bool is_reduce_worker;

#define IsDynamicReduceWorker()		(is_reduce_worker)

extern void DynamicReduceWorkerMain(Datum main_arg);
extern uint16 StartDynamicReduceWorker(void);
extern void StopDynamicReduceWorker(void);
extern void ResetDynamicReduceWork(void);
extern void DynamicReduceStartParallel(void);
extern void DynamicReduceConnectNet(const DynamicReduceNodeInfo *info, uint32 count);
extern const Oid* DynamicReduceGetCurrentWorkingNodes(uint32 *count);

extern Size EstimateDynamicReduceStateSpace(void);
extern void SerializeDynamiceReduceState(Size maxsize, char *start_address);
extern void RestoreDynamicReduceState(void *state);

extern void DynamicReduceStartNormalPlan(int plan_id, struct dsm_segment *seg, DynamicReduceMQ mq, TupleDesc desc, List *work_nodes, bool cache_on_disk);
extern void DynamicReduceStartMergePlan(int plan_id, struct dsm_segment *seg, DynamicReduceMQ mq, TupleDesc desc, List *work_nodes,
										int numCols, AttrNumber *sortColIdx, Oid *sortOperators, Oid *collations, bool *nullsFirst);
extern void DynamicReduceStartParallelPlan(int plan_id, struct dsm_segment *seg, DynamicReduceMQ mq, TupleDesc desc, List *work_nodes, int parallel_max, bool cache_on_disk);
extern void DynamicReduceStartSharedTuplestorePlan(int plan_id, struct dsm_segment *seg, DynamicReduceSTS sts, TupleDesc desc,
										List *work_nodes, int npart, int reduce_part);
extern void DynamicReduceStartSharedFileSetPlan(int plan_id, struct dsm_segment *seg, DynamicReduceSFS sfs, TupleDesc desc, List *work_nodes);
extern char* DynamicReduceSFSFileName(char *name, Oid nodeoid);
extern TupleTableSlot *DynamicReduceReadSFSTuple(TupleTableSlot *slot, BufFile *file, StringInfo buf);
extern void DynamicReduceWriteSFSTuple(TupleTableSlot *slot, BufFile *file);

extern uint8 DynamicReduceRecvTuple(shm_mq_handle *mqh, TupleTableSlot *slot, StringInfo buf,
									DynamicReduceRecvInfo *info, bool nowait);
extern int DynamicReduceSendOrRecvTuple(shm_mq_handle *mqsend, shm_mq_handle *mqrecv,
										StringInfo send_buf, TupleTableSlot *slot_recv,
										StringInfo recv_buf, DynamicReduceRecvInfo *info);
extern bool DynamicReduceSendMessage(shm_mq_handle *mqh, Size nbytes, void *data, bool nowait);

extern void SerializeEndOfPlanMessage(StringInfo buf);
extern bool SendEndOfPlanMessageToMQ(shm_mq_handle *mqh, bool nowait);
extern bool SendRejectPlanMessageToMQ(shm_mq_handle *mqh, bool nowait);

extern void SerializeDynamicReducePlanData(StringInfo buf, const void *data, uint32 len, struct OidBufferData *target);
extern void SerializeDynamicReduceSlot(StringInfo buf, TupleTableSlot *slot, struct OidBufferData *target);

extern void SerializeDynamicReduceNodeInfo(StringInfo buf, const DynamicReduceNodeInfo *info, uint32 count);
extern uint32 RestoreDynamicReduceNodeInfo(StringInfo buf, DynamicReduceNodeInfo **info);

/* in dr_fetch.c */
extern void DynamicReduceInitFetch(DynamicReduceIOBuffer *io, dsm_segment *seg, TupleDesc desc,
								   void *send_addr, Size send_size, void *recv_addr, Size recv_size);
extern void DynamicReduceClearFetch(DynamicReduceIOBuffer *io);
extern TupleTableSlot* DynamicReduceFetchSlot(DynamicReduceIOBuffer *io);
extern TupleTableSlot* DynamicReduceFetchLocal(DynamicReduceIOBuffer *io);
extern struct SharedTuplestoreAccessor* DynamicReduceOpenSharedTuplestore(dsa_pointer ptr);
extern void DynamicReduceCloseSharedTuplestore(struct SharedTuplestoreAccessor *stsa, dsa_pointer ptr);

/* in dr_shm.c */
extern dsm_segment* DynamicReduceGetSharedMemory(void);
extern SharedFileSet* DynamicReduceGetSharedFileSet(void);
#define DynamicReduceSharedFileName(name,fileno) DynamicReduceSFSFileName(name, fileno)

#endif /* DYNAMIC_REDUCE_H_ */
