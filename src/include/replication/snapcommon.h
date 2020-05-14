#ifndef SNAP_COMMON_H_
#define SNAP_COMMON_H_

#include "storage/lockdefs.h"
#include "storage/sinval.h"
#include "storage/lwlock.h"
#include "storage/lock.h"
#include "utils/snapshot.h"
#include "lib/ilist.h"
#include "utils/dsa.h"
#include "utils/builtins.h"

typedef struct SnapCommonLock
{
	dsa_pointer		first_lock_info;
	dsa_pointer		last_lock_info;
	dsa_handle		handle_lock_info;
	LWLock			lock_lock_info;
	LWLock			lock_proc_link;
}SnapCommonLock;

typedef struct SnapHoldLock
{
	LOCKTAG		tag;
	LOCKMASK	holdMask;
}SnapHoldLock;

typedef enum ClientStatus
{
	CLIENT_STATUS_CONNECTED = 1,
	CLIENT_STATUS_STREAMING = 2,
	CLIENT_STATUS_EXITING = 3
}ClientStatus;

typedef struct SnapLockIvdInfo
{
	dsa_pointer					next;
	TransactionId				xid;
	uint32						lock_count;
	uint32						ivd_msg_count;
	SharedInvalidationMessage	msgs[FLEXIBLE_ARRAY_MEMBER];
	/* SnapHoldLock [FLEXIBLE_ARRAY_MEMBER] in the end of struct*/
}SnapLockIvdInfo;

//#define FORCE_SNAP_DEBUG 1
#ifdef SNAP_FORCE_DEBUG_LOG
#define SNAP_FORCE_DEBUG_LOG(rest) ereport_domain(LOG, PG_TEXTDOMAIN("SnapForce"), rest)
#else
#define SNAP_FORCE_DEBUG_LOG(rest)	((void)true)
#endif

//#define SNAP_SYNC_DEBUG_LOG 1
#ifdef SNAP_SYNC_DEBUG_LOG
#define SNAP_SYNC_DEBUG_LOG(rest) ereport_domain(LOG, PG_TEXTDOMAIN("SnapSync"), rest)
#else
#define SNAP_SYNC_DEBUG_LOG(rest)	((void)true)
#endif

#define SNAP_IVDMSG_SIZE(count)	(sizeof(SharedInvalidationMessage)*(count))
#define SNAP_LOCK_IVDMSG_SIZE(lock_count, ivd_msg_count) (MAXALIGN(offsetof(SnapLockIvdInfo,msgs) \
							+ SNAP_IVDMSG_SIZE(ivd_msg_count) \
							+ (sizeof(SnapHoldLock)*(lock_count))))

#define XID_ARRAY_STEP_SIZE 1024
#define XID_PRINT_XID_LINE_NUM 50
#define SYNC_KEY_SAFE_GAP 2147483647
struct LOCKTAG;
extern void
SnapCollcectInvalidMsgItem(void **param,
			const SharedInvalidationMessage *msgs, int n);
extern void SnapEndTransferLockIvdMsg(void **param);
extern bool SnapIsHoldLock(void *map, const struct LOCKTAG *tag);
extern void SnapTransferLock(SnapCommonLock *comm_lock, void **param,
			TransactionId xid, struct PGPROC *from);
extern void SnapReleaseTransactionLocks(SnapCommonLock *comm_lock, TransactionId xid);
extern void SnapReleaseSnapshotTxidLocks(SnapCommonLock *comm_lock, TransactionId *xip,
				uint32 count, TransactionId lastxid);
extern void SnapReleaseAllTxidLocks(SnapCommonLock *comm_lock);
extern dsa_area* SnapGetLockArea(SnapCommonLock *comm_lock);
#endif							/* SNAP_RECEIVER_H_ */