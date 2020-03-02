/*--------------------------------------------------------------------------
 * dr_private.h
 *	header file for dynamic reduce private
 *
 * src/include/utils/dr_private.h
 *--------------------------------------------------------------------------
 */
#ifndef DR_PRIVATE_H_
#define DR_PRIVATE_H_

#include "postgres.h"

#ifdef WITH_RDMA
#include <rdma/rsocket.h>
#elif defined HAVE_SYS_EPOLL_H
#include <sys/epoll.h>
#include <signal.h>
#define DR_USING_EPOLL 1
#endif

#include "access/tupdesc.h"
#include "lib/oidbuffer.h"
#include "lib/stringinfo.h"
#include "port/atomics.h"
#include "storage/latch.h"
#include "storage/shm_mq.h"
#include "utils/hsearch.h"

/* Identifier for shared memory segments used by this extension. */
#define ADB_DYNAMIC_REDUCE_MQ_MAGIC		0xa553038f
#define ADB_DR_MQ_BACKEND_SENDER		0
#define ADB_DR_MQ_WORKER_SENDER			1
#ifndef DR_USING_EPOLL
#define INVALID_EVENT_SET_POS			(-1)
#endif /* DR_USING_EPOLL */
#define INVALID_PLAN_ID					(-1)

#define DR_WAIT_EVENT_SIZE_STEP			32
#define DR_HTAB_DEFAULT_SIZE			1024
#define DR_SOCKET_BUF_SIZE_START		32768
#define DR_SOCKET_BUF_SIZE_STEP			4096

/* worker and reduce MQ message */
#define ADB_DR_MQ_MSG_CONFIRM			'\x01'
#define ADB_DR_MQ_MSG_PORT				'\x02'
#define ADB_DR_MQ_MSG_RESET				'\x10'
#define ADB_DR_MQ_MSG_STARTUP			'\x11'
#define ADB_DR_MQ_MSG_CONNECT			'\x12'
#define ADB_DR_MQ_MSG_START_PLAN_NORMAL	'\x13'
#define ADB_DR_MQ_MSG_START_PLAN_SFS	'\x14'
#define ADB_DR_MQ_MSG_START_PLAN_PARALLEL '\x15'
#define ADB_DR_MQ_MSG_START_PLAN_STS	'\x16'

#define ADB_DR_MSG_INVALID				'\x00'
#define ADB_DR_MSG_NODEOID				'\x01'
#define ADB_DR_MSG_TUPLE				'\x02'
#define ADB_DR_MSG_SHARED_FILE_NUMBER	'\x03'
#define ADB_DR_MSG_SHARED_TUPLE_STORE	'\x04'
#define ADB_DR_MSG_END_OF_PLAN			'\x05'
#define ADB_DR_MSG_ATTACH_PLAN			'\x06'

#define DR_PLAN_SEND_WORKING			0x01	/* sending tuple */
#define DR_PLAN_SEND_GENERATE_CACHE		0x02	/* waiting generate send cached data */
#define DR_PLAN_SEND_SENDING_CACHE		0x03	/* waiting send cached data */
#define DR_PLAN_SEND_GENERATE_EOF		0x04	/* waiting generate send end of plan data */
#define DR_PLAN_SEND_SENDING_EOF		0x05	/* waiting send EOF data */
#define DR_PLAN_SEND_ENDED				0x06	/* all data sended */

#define DR_PLAN_RECV_WAITING_ATTACH		0x01	/* waiting backend notify attach message */
#define DR_PLAN_RECV_WORKING			0x02	/* receiving tuple */
#define DR_PLAN_RECV_ENDED				0x03	/* got EOF data */

/* define DRD_CONNECT 1 */
#ifdef DRD_CONNECT
#define DR_CONNECT_DEBUG(rest) ereport_domain(LOG_SERVER_ONLY, PG_TEXTDOMAIN("DynamicReduce"), rest)
#else
#define DR_CONNECT_DEBUG(rest) ((void)true)
#endif

/* define DRD_PLAN 1 */
#ifdef DRD_PLAN
#define DR_PLAN_DEBUG(rest)	ereport_domain(LOG_SERVER_ONLY, PG_TEXTDOMAIN("DynamicReduce"), rest)
#else
#define DR_PLAN_DEBUG(rest)	((void)true)
#endif

/* #define DRD_PLAN_EOF 1 */
#ifdef DRD_PLAN_EOF
#define DR_PLAN_DEBUG_EOF(rest) ereport_domain(LOG_SERVER_ONLY, PG_TEXTDOMAIN("DynamicReduce"), rest)
#else
#define DR_PLAN_DEBUG_EOF(rest)	((void)true)
#endif

/* define DRD_NODE 1 */
#ifdef DRD_NODE
#define DR_NODE_DEBUG(rest)	ereport_domain(LOG_SERVER_ONLY, PG_TEXTDOMAIN("DynamicReduce"), rest)
#else
#define DR_NODE_DEBUG(rest)	((void)true)
#endif

/* #define DRD_PLAN_ATTACH 1 */
#ifdef DRD_PLAN_ATTACH
#define DR_PLAN_DEBUG_ATTACH(rest) ereport_domain(LOG_SERVER_ONLY, PG_TEXTDOMAIN("DynamicReduce"), rest)
#else
#define DR_PLAN_DEBUG_ATTACH(rest)	((void)true)
#endif

typedef enum DR_STATUS
{
	DRS_STARTUPED = 1,		/* ready for listen */
	DRS_LISTENED,			/* listen created */
	DRS_RESET,
	DRS_CONNECTING,			/* connecting network */
	DRS_CONNECTED			/* ready for reduce */
}DR_STATUS;

/* event structs */
typedef enum DR_EVENT_DATA_TYPE
{
	DR_EVENT_DATA_POSTMASTER = 1,
	DR_EVENT_DATA_LATCH,
	DR_EVENT_DATA_LISTEN,
	DR_EVENT_DATA_NODE
}DR_EVENT_DATA_TYPE;

typedef enum DR_NODE_STATUS
{
	DRN_ACCEPTED = 1,		/* initialized for accept */
	DRN_CONNECTING,			/* connecting from local */
	DRN_WORKING,			/* sended or received node oid */
	DRN_FAILED,				/* got error message, don't try send or recv any message */
	DRN_WAIT_CLOSE
}DR_NODE_STATUS;

#if (defined DR_USING_EPOLL) || (defined WITH_RDMA) 
#define DROnEventArgs	struct DREventData *base, uint32_t events
#define DROnPreWaitArgs	struct DREventData *base
#define DROnErrorArgs	struct DREventData *base
#else /* DR_USING_EPOLL */
#define DROnEventArgs	WaitEvent *ev
#define DROnPreWaitArgs	struct DREventData *base, int pos
#define DROnErrorArgs	struct DREventData *base, int pos
#endif /* DR_USING_EPOLL */


#ifdef WITH_RDMA
#define START_POOL_ALLOC	256
#define STEP_POLL_ALLOC		8
typedef enum RPOLL_EVENTS
{
	RPOLL_EVENT_ADD,
	RPOLL_EVENT_DEL,
	RPOLL_EVENT_MOD
}RPOLL_EVENTS;
#endif /* WITH_RDMA */

typedef struct DREventData
{
	DR_EVENT_DATA_TYPE	type;
#if (defined DR_USING_EPOLL) || (defined WITH_RDMA) 
	pgsocket			fd;				/* maybe PGINVALID_SOCKET */
#endif /* DR_USING_EPOLL */
	void (*OnEvent)(DROnEventArgs);
	void (*OnPreWait)(DROnPreWaitArgs);	/* can be null */
	void (*OnError)(DROnErrorArgs);		/* can be null */
}DREventData;

typedef DREventData DRPostmasterEventData;

typedef struct DRListenEventData
{
	DREventData	base;
	uint16		port;
	bool		noblock;
}DRListenEventData;
//static int dr_listen_pos = INVALID_EVENT_SET_POS;

typedef struct DRLatchEventData
{
	DREventData		base;

	OidBufferData	net_oid_buf;	/* not include myself */
	OidBufferData	work_oid_buf;	/* not include myself */
	OidBufferData	work_pid_buf;	/* not include myself */
}DRLatchEventData;
extern DRLatchEventData *dr_latch_data;

typedef struct DRPlanCacheData
{
	int				plan_id;		/* data for plan ID */
	uint32			file_no;		/* shared file number */
	BufFile		   *file;			/* cached data */
	StringInfoData	buf;			/* buffer for read */
	bool			got_eof;		/* got end of plan message */
	bool			locked;			/* begin read? */
}DRPlanCacheData;

typedef struct DRNodeEventData
{
	DREventData		base;
	StringInfoData	sendBuf;
	DR_NODE_STATUS	status;
	StringInfoData	recvBuf;
	Oid				nodeoid;
	int				owner_pid;	/* remote owner */

	/*
	 * when it is not INVALID_PLAN_ID,
	 * it waiting plan connect to me or send buffer changed
	 */
	int				waiting_plan_id;

#if (defined DR_USING_EPOLL) || (defined WITH_RDMA) 
	uint32_t		waiting_events;
#endif /* DR_USING_EPOLL */

	struct addrinfo *addrlist;
	struct addrinfo *addr_cur;
	HTAB		   *cached_data;
}DRNodeEventData;

typedef struct PlanWorkerInfo
{
	int				worker_id;			/* -1 for main */

	/*
	 * when not InvalidOid,
	 * waiting for remote node connect or send buffer change
	 */
	Oid				waiting_node;

	shm_mq_handle  *worker_sender;
	shm_mq_handle  *reduce_sender;
	TupleTableSlot *slot_plan_src;		/* type convert for recv tuple in from plan */
	TupleTableSlot *slot_plan_dest;		/* type convert for recv tuple out from plan */
	TupleTableSlot *slot_node_src;		/* type convert for recv tuple in from other node */
	TupleTableSlot *slot_node_dest;		/* type convert for recv tuple out from other node */
	void		   *last_data;			/* last received data from worker */
	Oid			   *dest_oids;
	uint32			dest_count;
	uint32			dest_cursor;
	Size			last_size;			/* last received data size */
	StringInfoData	sendBuffer;			/* cached data for send to worker */
	char			last_msg_type;		/* last message type from backend */
	uint8			plan_recv_state;	/* see DR_PLAN_RECV_XXX */
	uint8			plan_send_state;	/* see DR_PLAN_SEND_XXX */
	bool			got_eof;			/* got ADB_DR_MSG_END_OF_PLAN message */
	void		   *private;			/* private data for special plan */
}PlanWorkerInfo;

typedef struct PlanInfo PlanInfo;
struct PlanInfo
{
	int				plan_id;

	/*
	 * when not InvalidOid,
	 * waiting for remote node connect or send buffer change
	 */
	Oid				waiting_node;

	void (*OnLatchSet)(PlanInfo *pi);
	bool (*OnNodeRecvedData)(PlanInfo *pi, const char *data, int len, Oid nodeoid);
	void (*OnNodeIdle)(PlanInfo *pi, WaitEvent *we, DRNodeEventData *ned);
	bool (*OnNodeEndOfPlan)(PlanInfo *pi, Oid nodeoid);
	void (*OnPlanError)(PlanInfo *pi);
	void (*OnDestroy)(PlanInfo *pi);
	void (*OnPreWait)(PlanInfo *pi);
	bool (*GenerateCacheMsg)(PlanWorkerInfo *pwi, PlanInfo *pi);
	void (*ProcessCachedData)(PlanInfo *pi, DRPlanCacheData *data, Oid nodeoid);

	dsm_segment		   *seg;
	OidBufferData		end_of_plan_nodes;
	OidBufferData		working_nodes;		/* not include PGXCNodeOid */
	PlanWorkerInfo	   *pwi;

	void			   *private;
	union
	{
		/* for parallel */
		struct
		{
			uint32		count_pwi;			/* for parallel */
			uint32		end_count_pwi;		/* got end of plan message count */
			uint32		last_pwi;			/* last put to message pwi */
			bool		remote_eof;			/* got eof message from all remote? */
			bool		local_eof;			/* got eof message from all attached worker? */
		};

		/* for shared tuplestore */
		struct
		{
			MemoryContext	sts_context;
			StringInfoData	sts_tup_buf;
		};
	};
};

#define CHECK_WORKER_IN_WROKING(pwi_, pi)									\
	if ((pwi_)->plan_send_state < DR_PLAN_SEND_WORKING ||					\
		(pwi_)->plan_send_state >= DR_PLAN_SEND_ENDED)						\
		ereport(ERROR,														\
				(errcode(ERRCODE_INTERNAL_ERROR),							\
				 errmsg("plan %d worker %d rstate %d sstate %d is not in sending tuple state",	\
						(pi)->plan_id, (pwi_)->worker_id,					\
						(pwi_)->plan_recv_state, (pwi_)->plan_send_state)))

typedef struct DynamicReduceSharedTuplestore
{
	pg_atomic_uint32	attached;
	char				padding[MAXIMUM_ALIGNOF-sizeof(pg_atomic_uint32)];
	char				sts[FLEXIBLE_ARRAY_MEMBER];
}DynamicReduceSharedTuplestore;

extern Oid					PGXCNodeOid;	/* avoid include pgxc.h */

/* public variables */
#ifdef WITH_RDMA
extern volatile Size poll_max;
extern Size poll_count;
extern struct pollfd * volatile poll_fd;
#elif defined DR_USING_EPOLL
extern int					dr_epoll_fd;
extern struct epoll_event  *dr_epoll_events;
#else
extern struct WaitEventSet *dr_wait_event_set;
extern struct WaitEvent	   *dr_wait_event;
#endif /* DR_USING_EPOLL */
extern Size					dr_wait_count;
extern Size					dr_wait_max;
extern DR_STATUS			dr_status;
extern pid_t				dr_reduce_pid;

/* public shared memory variables in dr_shm.c */
extern dsm_segment		   *dr_mem_seg;
extern shm_mq_handle	   *dr_mq_backend_sender;
extern shm_mq_handle	   *dr_mq_worker_sender;
extern SharedFileSet	   *dr_shared_fs;
extern struct dsa_area	   *dr_dsa;

/* public function */
void DRCheckStarted(void);
#ifdef DR_USING_EPOLL
#define DREnlargeWaitEventSet() ((void)true)
#else
void DREnlargeWaitEventSet(void);
#endif
void DRGetEndOfPlanMessage(PlanInfo *pi, PlanWorkerInfo *pwi);

/* public plan functions in plan_public.c */
bool DRSendPlanWorkerMessage(PlanWorkerInfo *pwi, PlanInfo *pi);
bool DRRecvPlanWorkerMessage(PlanWorkerInfo *pwi, PlanInfo *pi);
void DRSendWorkerMsgToNode(PlanWorkerInfo *pwi, PlanInfo *pi, DRNodeEventData *ned);
void ActiveWaitingPlan(DRNodeEventData *ned);
TupleTableSlot* DRStoreTypeConvertTuple(TupleTableSlot *slot, const char *data, uint32 len, HeapTuple head);
void DRSerializePlanInfo(int plan_id, dsm_segment *seg, void *addr, Size size, List *work_nodes, StringInfo buf);
PlanInfo* DRRestorePlanInfo(StringInfo buf, void **shm, Size size, void(*clear)(PlanInfo*));
void DRSetupPlanWorkInfo(PlanInfo *pi, PlanWorkerInfo *pwi, DynamicReduceMQ mq, int worker_id, uint8 recv_state);
void DRInitPlanSearch(void);
PlanInfo* DRPlanSearch(int planid, HASHACTION action, bool *found);
void DRPlanSeqInit(HASH_SEQ_STATUS *seq);

void DRClearPlanWorkInfo(PlanInfo *pi, PlanWorkerInfo *pwi);
void DRClearPlanInfo(PlanInfo *pi);
void OnDefaultPlanPreWait(PlanInfo *pi);
void OnDefaultPlanLatch(PlanInfo *pi);
void OnDefaultPlanIdleNode(PlanInfo *pi, WaitEvent *w, DRNodeEventData *ned);


/* normal plan functions in plan_normal.c */
void DRStartNormalPlanMessage(StringInfo msg);

/* SharedFileSet plan functions in plan_sfs.c */
struct BufFile;
void DRStartSFSPlanMessage(StringInfo msg);
void DynamicReduceWriteSFSMinTuple(struct BufFile *file, MinimalTuple mtup);
void DynamicReduceWriteSFSMsgTuple(struct BufFile *file, const char *data, int len);

/* parallel plan functions in plan_parallel.c */
void DRStartParallelPlanMessage(StringInfo msg);

/* shared tuplestore plan functions in plan_sts.c */
void DRStartSTSPlanMessage(StringInfo msg);
bool DRReadSFSTupleData(struct BufFile *file, StringInfo buf);

/* connect functions in dr_connect.c */
void FreeNodeEventInfo(DRNodeEventData *ned);
void ConnectToAllNode(const DynamicReduceNodeInfo *info, uint32 count);
DRListenEventData* GetListenEventData(void);
#ifdef DR_USING_EPOLL
void DRCtlWaitEvent(pgsocket fd, uint32_t events, void *ptr, int ctl);
void CallConnectingOnError(void);
#endif /* DR_USING_EPOLL */

#ifdef WITH_RDMA
void RDRCtlWaitEvent(pgsocket fd, uint32_t events, void *ptr, RPOLL_EVENTS ctl);
void CallConnectingOnError(void);
void dr_create_rhandle_list(int num, bool is_realloc);
void* dr_rhandle_find(int num);
void dr_rhandle_add(int num, void *info);
#endif /* DR_USING_EPOLL */

/* node functions in dr_node.c */
void DROnNodeConectSuccess(DRNodeEventData *ned);
bool PutMessageToNode(DRNodeEventData *ned, char msg_type, const char *data, uint32 len, int plan_id);
ssize_t RecvMessageFromNode(DRNodeEventData *ned, pgsocket fd);
bool DRNodeFetchTuple(DRNodeEventData *ned, int fetch_plan_id, char **data, int *len);
void DRNodeReset(DRNodeEventData *ned);
void DRActiveNode(int planid);
void DRInitNodeSearch(void);
DRNodeEventData* DRSearchNodeEventData(Oid nodeoid, HASHACTION action, bool *found);

#if (defined DR_USING_EPOLL) || (defined WITH_RDMA)
void DRNodeSeqInit(HASH_SEQ_STATUS *seq);
#endif /* DR_USING_EPOLL */
void CleanNodePlanCacheData(DRPlanCacheData *cache, bool delete_file);
void DropNodeAllPlanCacheData(DRNodeEventData *ned, bool delete_file);

/* dynamic reduce utils functions in dr_utils.c */
bool DynamicReduceHandleMessage(void *data, Size len);
void DRConnectNetMsg(StringInfo msg);
void DRUtilsReset(void);
void DRUtilsAbort(void);

/* dynamic reduce shared memory functions in dr_shm.c */
void DRSetupShmem(void);
void DRResetShmem(void);
void DRAttachShmem(Datum datum, bool isDynamicReduce);
void DRDetachShmem(void);
uint32 DRNextSharedFileSetNumber(void);
void DRShmemResetSharedFile(void);

bool DRSendMsgToReduce(const char *data, Size len, bool nowait);
bool DRRecvMsgFromReduce(Size *sizep, void **datap, bool nowait);
bool DRSendMsgToBackend(const char *data, Size len, bool nowait);
bool DRRecvMsgFromBackend(Size *sizep, void **datap, bool nowait);

bool DRSendConfirmToBackend(bool nowait);
bool DRRecvConfirmFromReduce(bool nowait);

#endif							/* DR_PRIVATE_H_ */
