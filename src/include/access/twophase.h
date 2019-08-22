/*-------------------------------------------------------------------------
 *
 * twophase.h
 *	  Two-phase-commit related declarations.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/twophase.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TWOPHASE_H
#define TWOPHASE_H

#include "access/xlogdefs.h"
#include "access/xact.h"
#include "datatype/timestamp.h"
#include "storage/lock.h"

/*
 * GlobalTransactionData is defined in twophase.c; other places have no
 * business knowing the internal definition.
 */
typedef struct GlobalTransactionData *GlobalTransaction;

/* GUC variable */
extern PGDLLIMPORT int max_prepared_xacts;

extern Size TwoPhaseShmemSize(void);
extern void TwoPhaseShmemInit(void);

extern void AtAbort_Twophase(void);
extern void PostPrepare_Twophase(void);

extern PGPROC *TwoPhaseGetDummyProc(TransactionId xid);
extern BackendId TwoPhaseGetDummyBackendId(TransactionId xid);
#ifdef ADB
extern List *GetPreparedXidList(void);
extern bool IsXidInPreparedState(TransactionId xid);
extern GlobalTransaction MarkAsPreparing(TransactionId xid, const char *gid,
				TimestampTz prepared_at,
				Oid owner, Oid databaseid,
				int nodecnt, Oid *nodeIds);
extern void StartRemoteXactPrepare(const char *gid, Oid *nodes, int count);
extern void EndRemoteXactPrepare(TransactionId xid, GlobalTransaction gxact);
extern void EndRemoteXactPrepareExt(TransactionId xid, const char *gid, Oid *nodes, int count, bool implicit);
#else
extern GlobalTransaction MarkAsPreparing(TransactionId xid, const char *gid,
				TimestampTz prepared_at,
				Oid owner, Oid databaseid);
#endif

extern void StartPrepare(GlobalTransaction gxact);
extern void EndPrepare(GlobalTransaction gxact);
extern bool StandbyTransactionIdIsPrepared(TransactionId xid);

extern TransactionId PrescanPreparedTransactions(TransactionId **xids_p,
							int *nxids_p);
extern void ParsePrepareRecord(uint8 info, char *xlrec,
				   xl_xact_parsed_prepare *parsed);
extern void StandbyRecoverPreparedTransactions(void);
extern void RecoverPreparedTransactions(void);

extern void CheckPointTwoPhase(XLogRecPtr redo_horizon);

extern void FinishPreparedTransaction(const char *gid, bool isCommit);
#if defined(ADB) || defined(AGTM)
extern void FinishPreparedTransactionExt(const char *gid, bool isCommit, bool isMissingOK);
#endif /* ADB */

extern void PrepareRedoAdd(char *buf, XLogRecPtr start_lsn,
			   XLogRecPtr end_lsn, RepOriginId origin_id);
extern void PrepareRedoRemove(TransactionId xid, bool giveWarning);
extern void restoreTwoPhaseData(void);
#endif							/* TWOPHASE_H */
