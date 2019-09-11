#ifndef AGTM_H
#define AGTM_H

#include "postgres.h"
#include "miscadmin.h"
#include "fmgr.h"

#include "access/clog.h"
#include "agtm/agtm_msg.h"
#include "agtm/agtm_protocol.h"
#include "catalog/pg_database.h"
#include "datatype/timestamp.h"
#include "lib/stringinfo.h"
#include "nodes/primnodes.h"
#include "tcop/dest.h"
#include "utils/snapshot.h"

#ifdef  AGTM_DEBUG
#define CLIENT_AGTM_TIMEOUT 3600
#else
#define CLIENT_AGTM_TIMEOUT 20
#endif

#define IsNormalDatabase()	(MyDatabaseId != InvalidOid &&		\
							 MyDatabaseId != TemplateDbOid)

#define IsUnderAGTM()		(!useLocalXid&&!isRestoreMode && \
							(isPGXCCoordinator || isPGXCDataNode) &&	\
							  IsUnderPostmaster &&						\
							  IsNormalDatabase() &&						\
							  IsNormalProcessingMode())

/* Type of sequence name used when dropping it */
typedef enum AGTM_SequenceKeyType
{
	AGTM_SEQ_FULL_NAME,	/* Full sequence key */
	AGTM_SEQ_DB_NAME 	/* DB name part of sequence key */
} AGTM_SequenceKeyType;

/*
 * get gixd from AGTM
 */
extern TransactionId agtm_GetGlobalTransactionId(bool isSubXact);

/*
 * get Snapshot info from AGTM
 */
extern Snapshot agtm_GetGlobalSnapShot(Snapshot snapshot);

/*
 * get transaction status from AGTM by transaction ID.
 */
extern XidStatus agtm_TransactionIdGetStatus(TransactionId xid, XLogRecPtr *lsn);

/*
 * synchronize transaction ID with AGTM.
 */
extern Oid agtm_SyncLocalNextXid(TransactionId *local_xid, TransactionId *agtm_xid);

/*
 * synchronize transaction ID with AGTM.
 */
extern Oid agtm_SyncClusterNextXid(TransactionId *cluster_xid, TransactionId *agtm_xid);

/*
 * synchronize transaction ID with AGTM.
 */
extern Datum sync_cluster_xid(PG_FUNCTION_ARGS);

/*
 * synchronize transaction ID with AGTM.
 */
extern Datum sync_local_xid(PG_FUNCTION_ARGS);

/*
 * show cluster nextXid.
 */
extern Datum show_cluster_xid(PG_FUNCTION_ARGS);

/*
 * create sequence on agtm
 */
 extern void agtm_CreateSequence(const char * seqName, const char * database,
 					const char * schema , List * seqOptions);

/*
 * alter sequence on agtm
 */
 extern void agtm_AlterSequence(const char * seqName, const char * database,
 					const char * schema , List * seqOptions);

/*
 * delete sequence on agtm
 */
 extern void agtm_DropSequence(const char * seqName, const char * database, const char * schema);

 extern void agtms_DropSequenceByDataBase(const char * database);

/*
 * rename sequence on agtm
 */
extern void agtm_RenameSequence(const char * seqName, const char * database,
							const char * schema, const char* newName, SequenceRenameType type);

/* this function only called when alter database XXX rename to XXX happened*/
extern void agtm_RenameSeuqneceByDataBase(const char * oldDatabase,
											const char * newDatabase);

/*
 * get next Sequence from AGTM
 */
extern AGTM_Sequence agtm_GetSeqNextVal(const char *seqname, const char * database,	const char * schema,
										int64 min, int64 max, int64 cache, int64 inc, bool cycle, int64 *cached);

/*
 * get current Sequence from AGTM
 */
extern AGTM_Sequence agtm_GetSeqCurrVal(const char *seqname, const char * database,	const char * schema);

/*
 * get last Sequence from AGTM
 */
extern AGTM_Sequence agtm_GetSeqLastVal(const char *seqname, const char * database,	const char * schema);

/*
 * set Sequence current value
 */

extern AGTM_Sequence agtm_SetSeqVal(const char *seqname, const char * database,
			const char * schema, AGTM_Sequence nextval);

extern AGTM_Sequence agtm_SetSeqValCalled(const char *seqname, const char * database,
			const char * schema, AGTM_Sequence nextval, bool iscalled);

/*
 * agtm reset Sequence cache
 */
extern void agtm_ResetSequenceCaches(void);

/*
 * get timestamp from AGTM
 */
extern Timestamp agtm_GetTimestamptz(void);

/*--------------------------------------2 pc API--------------------------------------*/

/*
 * begin transaction on AGTM
 */
extern void agtm_BeginTransaction(void);

/*
 * prepare commit transaction on AGTM
 */
extern void agtm_PrepareTransaction(const char *prepared_gid);

/*
 * commit transcation on AGTM
 */
extern void agtm_CommitTransaction(const char *prepared_gid, bool missing_ok);

/*
 * rollback transacton on AGTM
 */
extern void agtm_AbortTransaction(const char *prepared_gid, bool missing_ok, bool no_error);

/*
 * process command
 */
void ProcessAGtmCommand(StringInfo input_message, CommandDest dest);

/*-------------------------------------- tool function --------------------------------------*/

extern void parse_seqOption_to_string(List * seqOptions, StringInfo strOption);

extern AGTM_Sequence
get_seqnextval_from_gtmcorrd(const char *seqname, const char * database);
extern AGTM_Sequence
set_seqnextval_from_gtmcorrd(const char *seqname, const char * database, AGTM_Sequence nextval);
extern void disconnect_gtmcoord(int code, Datum arg);

#endif

