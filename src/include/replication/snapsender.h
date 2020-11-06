#ifndef SNAP_SENDER_H_
#define SNAP_SENDER_H_

#include "utils/snapshot.h"
#include "replication/snapcommon.h"

extern void SnapSenderMain(void) pg_attribute_noreturn();

extern Size SnapSenderShmemSize(void);
extern void SnapSenderShmemInit(void);

extern void SnapSendTransactionAssign(TransactionId txid, int txidnum, TransactionId parent);
extern void SnapSendAddXip(TransactionId txid, int txidnum, TransactionId parent);
extern void SnapSendTransactionFinish(TransactionId txid, bool is_rxact);

extern void SnapSendLockSendSock(void);
extern void SnapSendUnlockSendSock(void);
extern TransactionId SnapSendGetGlobalXmin(void);
extern void SnapSenderGetStat(StringInfo buf);
extern void isSnapSenderWaitNextIdOk(void);
extern Snapshot SnapSenderGetSnapshot(Snapshot snap, TransactionId *xminOld, TransactionId* xmaxOld,
			int *countOld);
extern void SnapSenderTransferLock(void **param, TransactionId xid, struct PGPROC *from);

extern void SerializeFullAssignXid(TransactionId *xids, uint32 cnt, TransactionId *gs_xip, uint32 gs_cnt,
							TransactionId *ss_xip, uint32 ss_cnt,
							TransactionId *sf_xip, uint32 sf_cnt, StringInfo buf);
extern TransactionId *SnapSenderGetAllXip(uint32 *cnt_num);

extern void SnapSenderGetAllAssingFinish(TransactionId	*ss_xid_assgin,uint32 *ss_cnt_assign,
				TransactionId	*ss_xid_finish, uint32 *ss_cnt_finish);
#endif /* SNAP_SENDER_H_ */
