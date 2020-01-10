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

/*--------------------------------------2 pc API--------------------------------------*/

/*
 * process command
 */
void ProcessAGtmCommand(StringInfo input_message, CommandDest dest);

/*-------------------------------------- tool function --------------------------------------*/

extern AGTM_Sequence
get_seqnextval_from_gtmcorrd(const char *seqname, const char * database,	const char * schema,
				   int64 min, int64 max, int64 cache, int64 inc, bool cycle, int64 *cached);
AGTM_Sequence
set_seqnextval_from_gtmcorrd(const char *seqname, const char * database,
			const char * schema, AGTM_Sequence nextva, bool iscalled);
extern void disconnect_gtmcoord(int code, Datum arg);

#endif

