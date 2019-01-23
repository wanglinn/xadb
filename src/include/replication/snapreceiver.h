#ifndef SNAP_RECEIVER_H_
#define SNAP_RECEIVER_H_

#include "utils/snapshot.h"

/* prototypes for functions in snapreceiver.c */
extern void SnapReceiverMain(void) pg_attribute_noreturn();

extern Size SnapRcvShmemSize(void);
extern void SnapRcvShmemInit(void);
extern void ShutdownSnapRcv(void);
extern bool SnapRcvStreaming(void);
extern bool SnapRcvRunning(void);

extern Snapshot SnapRcvGetSnapshot(Snapshot snap);
extern bool SnapRcvWaitTopTransactionEnd(TransactionId txid, TimestampTz end);

#endif							/* SNAP_RECEIVER_H_ */