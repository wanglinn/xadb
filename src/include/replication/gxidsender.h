#ifndef TRANS_SENDER_H_
#define TRANS_SENDER_H_

#include "utils/snapshot.h"

extern void GxidSenderMain(void) pg_attribute_noreturn();

extern Size GxidSenderShmemSize(void);
extern void GxidSenderShmemInit(void);

#endif /* TRANS_SENDER_H_ */
