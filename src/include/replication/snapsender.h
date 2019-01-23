#ifndef SNAP_SENDER_H_
#define SNAP_SENDER_H_

extern void SnapSenderMain(void) pg_attribute_noreturn();

extern Size SnapSenderShmemSize(void);
extern void SnapSenderShmemInit(void);

extern void SnapSendTransactionAssign(TransactionId txid, TransactionId parent);
extern void SnapSendTransactionFinish(TransactionId txid);

#endif /* SNAP_SENDER_H_ */
