#ifndef ADB_STAT_STATEMENTS_H
#define ADB_STAT_STATEMENTS_H

#include "postgres.h"

#define ADBSS_NAME "adb_stat_statements"
#define ADBSS_QUERYID_INDEX_NAME "adb_stat_statements_queryid"

#define ADBSS_DUMMY_QUERY "<adbss dummy query>"
#define isAdbssDummyQuery(query) \
	(query != NULL &&            \
	 pg_strcasecmp(query, ADBSS_DUMMY_QUERY) == 0)

typedef enum AdbssAttributes
{
	Anum_adbss_userid = 1,
	Anum_adbss_dbid,
	Anum_adbss_queryid,
	Anum_adbss_planid,
	Anum_adbss_calls,
	Anum_adbss_rows,
	Anum_adbss_total_time,
	Anum_adbss_min_time,
	Anum_adbss_max_time,
	Anum_adbss_mean_time,
	Anum_adbss_last_execution,
#define Anum_adbss_offset (Anum_adbss_last_execution) /* the above value */
#define Anum_adbss_minimal(anum) (anum - Anum_adbss_offset)
	Anum_adbss_query,
	Anum_adbss_plan,
	Anum_adbss_explain_format,
	Anum_adbss_explain_plan,
	Anum_adbss_bound_params,
	Anum_adbss_boundary /* must be the last */
#define Natts_adbss (Anum_adbss_boundary - 1)
} AdbssAttributes;

typedef struct RelationPlan
{
	char *schemaname;
	char *relname;
	char *planname;
	char *attname;
	Oid relid;
	Index varattno;
	Index scanrelid;
} RelationPlan;

#endif
