#include "postgres.h"

#include "access/rmgr.h"
#include "access/xact.h"
#include "access/xlogrecord.h"
#include "access/transam.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/postmaster.h"
#include "replication/snapcommon.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/proclist.h"
#include "storage/shmem.h"
#include "utils/dsa.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/tqual.h"
#include "pgxc/pgxc.h"

static void* SnapBeginTransferLock(void);
static void SnapInsertTransferLock(void* map, const LOCKTAG *tag, LOCKMASK holdMask);

typedef struct SnapTransPara
{
	SharedInvalidationMessage	*msgs;
	int							msg_num;
	void						*map;
	TransactionId				xid;
}SnapTransPara;

static void SnapReleaseLock(SnapCommonLock *comm_lock, SnapLockIvdInfo *lock)
{
	uint32			i;
	LOCKMODE		mode;
	SnapHoldLock	*hold;
	char			*lock_start;

	LWLockAcquire(&comm_lock->lock_proc_link, LW_EXCLUSIVE);
	SendSharedInvalidMessages(lock->msgs, lock->ivd_msg_count);

	lock_start = (char*)(lock->msgs) + SNAP_IVDMSG_SIZE(lock->ivd_msg_count);
	for (i=0;i<lock->lock_count;++i)
	{
		hold = (SnapHoldLock*)(lock_start + i * sizeof(SnapHoldLock));
		for (mode=0;mode<=MAX_LOCKMODES;++mode)
		{
			if (hold->holdMask & LOCKBIT_ON(mode))
				TryReleaseLock(&hold->tag, mode, MyProc);
		}
	}
	LWLockRelease(&comm_lock->lock_proc_link);
}

dsa_area* SnapGetLockArea(SnapCommonLock *comm_lock)
{
	static dsa_area *lock_area = NULL;
	if (lock_area == NULL)
	{
		MemoryContext old_context = MemoryContextSwitchTo(TopMemoryContext);
		dsa_handle handle;
		LWLockAcquire(&comm_lock->lock_lock_info, LW_SHARED);
		handle = comm_lock->handle_lock_info;
		LWLockRelease(&comm_lock->lock_lock_info);
		if (handle == DSM_HANDLE_INVALID)
		{
			LWLockAcquire(&comm_lock->lock_lock_info, LW_EXCLUSIVE);
			if (comm_lock->handle_lock_info == DSM_HANDLE_INVALID)
			{
				lock_area = dsa_create(LWTRANCHE_SNAPSHOT_COMMON_DSA);
				comm_lock->handle_lock_info = dsa_get_handle(lock_area);
				dsa_pin(lock_area);
			}else
			{
				handle = comm_lock->handle_lock_info;
			}
			LWLockRelease(&comm_lock->lock_lock_info);
		}

		if (lock_area == NULL)
		{
			Assert(handle != DSM_HANDLE_INVALID);
			lock_area = dsa_attach(handle);
		}
		dsa_pin_mapping(lock_area);
		MemoryContextSwitchTo(old_context);
	}
	Assert(lock_area != NULL);

	return lock_area;
}

void
SnapCollcectInvalidMsgItem(void **param_io,
			const SharedInvalidationMessage *msgs, int n)
{
	SnapTransPara *param = NULL;

	if (!param_io)
		return;

	if (!(*param_io))
		*param_io = (SnapTransPara*)palloc0(sizeof(SnapTransPara));

	param = *param_io;
	if (param->msgs == NULL)
		param->msgs = palloc0(sizeof(SharedInvalidationMessage) * n);
	else
		param->msgs = repalloc(param->msgs, sizeof(SharedInvalidationMessage) * (n+ param->msg_num));

	memcpy(param->msgs + param->msg_num, &msgs[0], sizeof(SharedInvalidationMessage)*n);
	param->msg_num += n;
}

void SnapEndTransferLockIvdMsg(void **param_io)
{
	SnapTransPara *param = (SnapTransPara*)(*param_io);
	if (!param)
		return;

	if (param->msgs)
		pfree(param->msgs);
	param->msg_num = 0;

	if (param->map)
		hash_destroy(param->map);
	
	pfree(param);
}

static void* SnapBeginTransferLock(void)
{
	HASHCTL		ctl;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(LOCKTAG);
	ctl.entrysize = sizeof(SnapHoldLock);
	return hash_create("snapshot hold lock",
					   MaxBackends,
					   &ctl,
					   HASH_ELEM|HASH_BLOBS);
}

static void SnapInsertTransferLock(void* map, const LOCKTAG *tag, LOCKMASK holdMask)
{
	bool found;
	SnapHoldLock *hold;

	if (tag->locktag_type != LOCKTAG_RELATION ||
		!(holdMask & LOCKBIT_ON(AccessExclusiveLock)))
		return;

	hold = hash_search(map, tag, HASH_ENTER, &found);
	if (found)
		elog(ERROR, "locktag already exist");
	hold->holdMask = holdMask;
}

bool SnapIsHoldLock(void *param_io, const LOCKTAG *tag)
{
	SnapTransPara *param = (SnapTransPara*)(param_io);
	if (!param || !param->map)
		return false;

	return hash_search(param->map, tag, HASH_FIND, NULL) != NULL;
}

void SnapTransferLock(SnapCommonLock *comm_lock, void **param_io,
			TransactionId xid, struct PGPROC *from)
{
	uint32					i;
	volatile dsa_pointer	pointer;
	SnapLockIvdInfo			*info,*prev;
	SnapHoldLock			*hold;
	HASH_SEQ_STATUS			seq_state;
	PGPROC					*newproc;
	int						hash_num;
	dsa_area				*lock_area;
	char					*lock_start;
	SnapTransPara			*param;

	if (!param_io)
		return;

	param = (SnapTransPara*)(*param_io);
	param->map = SnapBeginTransferLock();
	param->xid = xid;
	EnumProcLocks(from, SnapInsertTransferLock, param->map);

	hash_num = 0;
	if (param->map)
		hash_num = hash_get_num_entries(param->map);
	if (hash_num <= 0 && param->msg_num == 0)
		return;

	lock_area = SnapGetLockArea(comm_lock);
	pointer = dsa_allocate0(lock_area, SNAP_LOCK_IVDMSG_SIZE(hash_num, param->msg_num));
	PG_TRY();
	{
		info = dsa_get_address(lock_area, pointer);
		info->xid = param->xid;
		info->lock_count = (uint32)hash_num;
		info->ivd_msg_count = (uint32)(param->msg_num);
		info->next = InvalidDsaPointer;

		if (param->msg_num > 0)
			memcpy(info->msgs, param->msgs, sizeof(SharedInvalidationMessage) * param->msg_num);

		if (hash_num > 0)
		{
			lock_start = ((char*)(info->msgs)) + SNAP_IVDMSG_SIZE(param->msg_num);
			hash_seq_init(&seq_state, param->map);
			i = 0;
			while ((hold = hash_seq_search(&seq_state)) != NULL)
			{
				Assert(i < info->lock_count);
				*(SnapHoldLock*)(lock_start + i * sizeof(SnapHoldLock)) = *hold;
				++i;
			}
		}

		LWLockAcquire(&comm_lock->lock_lock_info, LW_EXCLUSIVE);
#ifdef USE_ASSERT_CHECKING
		{
			dsa_pointer dp = comm_lock->first_lock_info;
			while (dp != InvalidDsaPointer)
			{
				prev = dsa_get_address(lock_area, dp);
				Assert(prev->xid != param->xid);
				dp = prev->next;
			}
		}
#endif /* USE_ASSERT_CHECKING */
		if (comm_lock->last_lock_info == InvalidDsaPointer)
		{
			Assert(comm_lock->first_lock_info == InvalidDsaPointer);
			comm_lock->first_lock_info = pointer;
		}else
		{
			Assert(comm_lock->first_lock_info != InvalidDsaPointer);
			prev = dsa_get_address(lock_area, comm_lock->last_lock_info);
			Assert(prev->next == InvalidDsaPointer);
			prev->next = pointer;
		}
		comm_lock->last_lock_info = pointer;
		LWLockRelease(&comm_lock->lock_lock_info);
	}PG_CATCH();
	{
		dsa_free(lock_area, pointer);
		PG_RE_THROW();
	}PG_END_TRY();

	if (hash_num > 0)
	{
		if (!IsGTMNode())
			newproc = GetSnapshotProcess();
		else
			newproc = GetGxidProcess();
		
		hash_seq_init(&seq_state, param->map);
		LWLockAcquire(&comm_lock->lock_proc_link, LW_EXCLUSIVE);

		while((hold = hash_seq_search(&seq_state)) != NULL)
			TransferLock(&hold->tag, from, newproc);

		LWLockRelease(&comm_lock->lock_proc_link);
	}
}

void SnapReleaseTransactionLocks(SnapCommonLock *comm_lock, TransactionId xid)
{
	MemoryContext		old_context = MemoryContextSwitchTo(TopMemoryContext);
	SnapLockIvdInfo		*lock,*prev;
	dsa_pointer			dp, prev_dp;
	dsa_area			*lock_area;

	lock_area = SnapGetLockArea(comm_lock);
	lock = prev = NULL;
	LWLockAcquire(&comm_lock->lock_lock_info, LW_EXCLUSIVE);
	dp = comm_lock->first_lock_info;
	prev_dp = InvalidDsaPointer;

	while (dp != InvalidDsaPointer)
	{
		lock = dsa_get_address(lock_area, dp);
		if (lock->xid == xid)
		{
			if (!prev)
			{
				Assert(comm_lock->first_lock_info == dp);
				comm_lock->first_lock_info = lock->next;
			}
			if (comm_lock->last_lock_info == dp)
			{
				if (lock->next == InvalidDsaPointer && prev_dp != InvalidDsaPointer)
					comm_lock->last_lock_info = prev_dp;
				else
					comm_lock->last_lock_info = lock->next;
			}

			if (prev)
			{
				prev->next = lock->next;
			}
			break;
		}
		prev = lock;
		prev_dp = dp;
		dp = lock->next;
		lock = NULL;
	}
	LWLockRelease(&comm_lock->lock_lock_info);

	if (lock != NULL)
	{
		Assert(dsa_get_address(lock_area, dp) == lock);
		SnapReleaseLock(comm_lock, lock);
		dsa_free(lock_area, dp);
	}

	MemoryContextSwitchTo(old_context);
}

void SnapReleaseSnapshotTxidLocks(SnapCommonLock *comm_lock, TransactionId *xip,
			uint32 count, TransactionId lastxid)
{
	SnapLockIvdInfo   *lock,*prev;
	SnapshotData	snap;
	dsa_pointer		dp, prev_dp;
	uint32			i;
	dsa_area 		*lock_area;

	lock_area = SnapGetLockArea(comm_lock);
	MemSet(&snap, 0, sizeof(snap));
	TransactionIdAdvance(lastxid);
	snap.xmin = snap.xmax = lastxid;
	for (i=0;i<count;++i)
	{
		lastxid = xip[i];
		if (NormalTransactionIdPrecedes(lastxid, snap.xmin))
			snap.xmin = lastxid;
	}

	lock = prev = NULL;
	LWLockAcquire(&comm_lock->lock_lock_info, LW_EXCLUSIVE);
	dp = comm_lock->first_lock_info;
	prev_dp = InvalidDsaPointer;
	while (dp != InvalidDsaPointer)
	{
		lock = dsa_get_address(lock_area, dp);
		if (XidInMVCCSnapshot(lock->xid, &snap) == false)
		{
			dsa_pointer next = lock->next;
			if (prev)
			{
				prev->next = lock->next;
			}else
			{
				Assert(comm_lock->first_lock_info == dp);
				comm_lock->first_lock_info = lock->next;
			}
			if (comm_lock->last_lock_info == dp)
			{
				if (lock->next == InvalidDsaPointer && prev_dp != InvalidDsaPointer)
					comm_lock->last_lock_info = prev_dp;
				else
					comm_lock->last_lock_info = lock->next;
			}
				
			SnapReleaseLock(comm_lock, lock);
			dsa_free(lock_area, dp);
			prev_dp = dp;
			dp = next;
		}else
		{
			prev = lock;
			prev_dp = dp;
			dp = lock->next;
		}
	}
	LWLockRelease(&comm_lock->lock_lock_info);
}

void SnapReleaseAllTxidLocks(SnapCommonLock *comm_lock)
{
	SnapLockIvdInfo   *lock,*prev;
	dsa_pointer		dp, prev_dp, next;
	dsa_area 		*lock_area;

	lock_area = SnapGetLockArea(comm_lock);
	lock = prev = NULL;
	LWLockAcquire(&comm_lock->lock_lock_info, LW_EXCLUSIVE);
	dp = comm_lock->first_lock_info;
	prev_dp = InvalidDsaPointer;
	while (dp != InvalidDsaPointer)
	{
		lock = dsa_get_address(lock_area, dp);
		next = lock->next;
		if (prev)
		{
			prev->next = lock->next;
		}else
		{
			Assert(comm_lock->first_lock_info == dp);
			comm_lock->first_lock_info = lock->next;
		}
		if (comm_lock->last_lock_info == dp)
		{
			if (lock->next == InvalidDsaPointer && prev_dp != InvalidDsaPointer)
				comm_lock->last_lock_info = prev_dp;
			else
				comm_lock->last_lock_info = lock->next;
		}
			
		SnapReleaseLock(comm_lock, lock);
		dsa_free(lock_area, dp);
		prev_dp = dp;
		dp = next;
	}
	LWLockRelease(&comm_lock->lock_lock_info);
}