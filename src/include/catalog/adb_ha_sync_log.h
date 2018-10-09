/*-------------------------------------------------------------------------
 *
 * adb_ha_sync_log.h
 *	  definition of the system "adb_ha_sync_log" relation (adb_ha_sync_log)
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2016 ADB Development Group
 *
 * src/include/catalog/adb_ha_sync_log.h
 *-------------------------------------------------------------------------
 */
#ifndef ADB_HA_SYNC_LOG_H
#define ADB_HA_SYNC_LOG_H

#include "catalog/genbki.h"
#include "catalog/adb_ha_sync_log_d.h"

#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "utils/portal.h"
#include "utils/timestamp.h"

CATALOG(adb_ha_sync_log,9117,AdbHaSyncLogRelationId) BKI_SHARED_RELATION BKI_WITHOUT_OIDS
{
	int64			gxid;

	int32			cmdid;

	/* the create time of current record */
	timestamptz		create_time;

	/* the sync time of current record */
	timestamptz		finish_time;

	/* the grammar of the record */
	char			sql_gram;

	/* see below ADB_SQL_* */
	char			sql_kind;

	/* current schema of the sql */
	NameData		sql_schema;
#ifdef CATALOG_VARLEN

	/* the sql string of current record */
	text			query_sql;

	/* the params of current sql */
	text			params;
#endif
} FormData_adb_ha_sync_log;

typedef FormData_adb_ha_sync_log *Form_adb_ha_sync_log;

#ifdef EXPOSE_TO_CLIENT_CODE

/* default is pg grammar */
#define ADB_SQL_GRAM_DEFAULT		'P'
/* oracle grammar */
#define ADB_SQL_GRAM_ORACLE			'O'

/* simple sql via exec_simple_query */
#define ADB_SQL_KIND_SIMPLE			'S'
/* extension sql via exec_execute_message */
#define ADB_SQL_KIND_EXECUTE		'E'

#define IsValidAdbSqlKind(kind) \
	((kind) == ADB_SQL_KIND_SIMPLE || \
	 (kind) == ADB_SQL_KIND_EXECUTE)

#endif							/* EXPOSE_TO_CLIENT_CODE */

extern void AddAdbHaSyncLog(TimestampTz create_time,
							ADB_MULTI_GRAM_ARG_COMMA(ParseGrammar sql_gram)
							char sql_kind,
							const char *query_sql,
							ParamListInfo params);

extern bool AdbHaSyncLogWalkerPortal(Portal portal);

#endif /* ADB_HA_SYNC_LOG_H */
