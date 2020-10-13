#ifndef RXACT_MGR_H
#define RXACT_MGR_H

#include "access/xlog.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"

typedef enum RemoteXactType
{
	RX_PREPARE = 1
	,RX_COMMIT
	,RX_ROLLBACK
	,RX_AUTO		/* test local transaction */
}RemoteXactType;

typedef struct RxactTransactionInfo
{
	char gid[NAMEDATALEN];	/* 2pc id */
	Oid *remote_nodes;		/* all remote nodes, include AGTM */
	bool *remote_success;	/* remote execute success ? */
	int count_nodes;		/* count of remote nodes */
	Oid db_oid;				/* transaction database Oid */
	RemoteXactType type;	/* remote 2pc type */
	TransactionId auto_tid;	/* when type is RX_AUTO */
	bool failed;			/* backend do it failed ? */
	bool is_cleanup;		/* is from clean up */
}RxactTransactionInfo;

extern void RemoteXactMgrMain(void) __attribute__((noreturn));

extern bool IsRXACTWorker(void);
extern bool RecordRemoteXact(const char *gid, Oid *node_oids, int count, RemoteXactType type, bool no_error);
extern bool RecordRemoteXactSuccess(const char *gid, RemoteXactType type, bool no_error);
extern bool RecordRemoteXactFailed(const char *gid, RemoteXactType type, bool no_error);
extern bool RecordRemoteXactAuto(const char *gid, TransactionId tid, bool no_error);
extern void DisconnectRemoteXact(void);
/* return list of RxactTransactionInfo */
extern List *RxactGetRunningList(void);
extern void FreeRxactTransactionInfo(RxactTransactionInfo *rinfo);
extern void FreeRxactTransactionInfoList(List *list);
extern bool RxactWaitGID(const char *gid, bool no_error);

/* xlog interfaces */
extern void rxact_redo(XLogReaderState *record);
extern void rxact_desc(StringInfo buf, XLogReaderState *record);
extern const char *rxact_identify(uint8 info);
extern void rxact_xlog_startup(void);
extern void rxact_xlog_cleanup(void);
extern void CheckPointRxact(int flags);

#endif /* RXACT_MGR_H */
