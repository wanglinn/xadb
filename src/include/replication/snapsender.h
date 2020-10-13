#ifndef SNAP_SENDER_H_
#define SNAP_SENDER_H_

#include "utils/snapshot.h"
#include "replication/snapcommon.h"

extern void SnapSenderMain(void) pg_attribute_noreturn();

extern Size SnapSenderShmemSize(void);
extern void SnapSenderShmemInit(void);

extern void SnapSendTransactionAssign(TransactionId txid, int txidnum, TransactionId parent);
extern void SnapSendTransactionFinish(TransactionId txid);

extern void SnapSendLockSendSock(void);
extern void SnapSendUnlockSendSock(void);
extern TransactionId SnapSendGetGlobalXmin(void);
extern void SnapSenderGetStat(StringInfo buf);
extern void isSnapSenderWaitNextIdOk(void);
extern Snapshot SnapSenderGetSnapshot(Snapshot snap, TransactionId *xminOld, TransactionId* xmaxOld,
			int *countOld);
extern void SnapSenderTransferLock(void **param, TransactionId xid, struct PGPROC *from);
#endif /* SNAP_SENDER_H_ */
