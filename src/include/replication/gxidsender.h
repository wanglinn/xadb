#ifndef TRANS_SENDER_H_
#define TRANS_SENDER_H_

#include "utils/snapshot.h"
#include "replication/snapcommon.h"

extern void GxidSenderMain(void) pg_attribute_noreturn();

extern Size GxidSenderShmemSize(void);
extern void GxidSenderShmemInit(void);

extern void SerializeFullAssignXid(StringInfo buf);
extern Snapshot GxidSenderGetSnapshot(Snapshot snap, TransactionId *xminOld, TransactionId* xmaxOld,
			int *countOld);

extern void GxidSendLockSendSock(void);
extern void GxidSendUnlockSendSock(void);
extern void GxidSenderTransferLock(SnapTransPara *param, struct PGPROC *from);
#endif /* TRANS_SENDER_H_ */
