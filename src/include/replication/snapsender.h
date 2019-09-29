#ifndef SNAP_SENDER_H_
#define SNAP_SENDER_H_

extern void SnapSenderMain(void) pg_attribute_noreturn();

extern Size SnapSenderShmemSize(void);
extern void SnapSenderShmemInit(void);

extern void SnapSendTransactionAssignArray(TransactionId* xids, int xid_num, TransactionId parent);
extern void SnapSendTransactionAssign(TransactionId txid, int txidnum, TransactionId parent);
extern void SnapSendTransactionFinish(TransactionId txid);

#endif /* SNAP_SENDER_H_ */
