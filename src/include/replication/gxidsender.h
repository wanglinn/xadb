#ifndef TRANS_SENDER_H_
#define TRANS_SENDER_H_

#include "utils/snapshot.h"

extern void GxidSenderMain(void) pg_attribute_noreturn();

extern Size GxidSenderShmemSize(void);
extern void GxidSenderShmemInit(void);

extern void SerializeFullAssignXid(StringInfo buf);
extern Snapshot GxidSenderGetSnapshot(Snapshot snap, TransactionId *xminOld, TransactionId* xmaxOld,
			int *countOld);

#endif /* TRANS_SENDER_H_ */
