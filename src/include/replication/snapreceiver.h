#ifndef SNAP_RECEIVER_H_
#define SNAP_RECEIVER_H_

#include "storage/lockdefs.h"
#include "utils/snapshot.h"

extern int snap_receiver_sxmin_time;
/* prototypes for functions in snapreceiver.c */
extern void SnapReceiverMain(void) pg_attribute_noreturn();

extern Size SnapRcvShmemSize(void);
extern void SnapRcvShmemInit(void);
extern void ShutdownSnapRcv(void);
extern bool SnapRcvStreaming(void);
extern bool SnapRcvRunning(void);

extern Snapshot SnapRcvGetSnapshot(Snapshot snap);
extern bool SnapRcvWaitTopTransactionEnd(TransactionId txid, TimestampTz end);

struct LOCKTAG;
struct PGPROC;
extern void* SnapRcvBeginTransferLock(void);
extern void SnapRcvInsertTransferLock(void* map, const struct LOCKTAG *tag, LOCKMASK holdMask);
extern bool SnapRcvIsHoldLock(void *map, const struct LOCKTAG *tag);
extern void SnapRcvTransferLock(void *map, TransactionId xid, struct PGPROC *from);
extern void SnapRcvEndTransferLock(void* map);
extern TransactionId SnapRcvGetGlobalXmin(void);
#endif							/* SNAP_RECEIVER_H_ */