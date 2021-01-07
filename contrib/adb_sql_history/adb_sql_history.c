#include "postgres.h"

#include <math.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_type.h"
#include "commands/explain.h"
#include "commands/extension.h"
#include "executor/instrument.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planner.h"
#include "parser/analyze.h"
#include "parser/parsetree.h"
#include "parser/scanner.h"
#include "parser/scansup.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "replication/walsender.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/spin.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/timestamp.h"
#ifdef ADBMGRD
#include "postmaster/adbmonitor.h"
#endif /* ADBMGRD */

PG_MODULE_MAGIC;

#if PG_VERSION_NUM >= 100000
#include "catalog/index.h"
#include "utils/queryenvironment.h"
#endif

#if PG_VERSION_NUM >= 120000
#include "catalog/pg_extension_d.h"
#endif

#define ADBSH_DEFAULT_MAX_RECORD (5)

PG_FUNCTION_INFO_V1(adb_sql_history);

#define ADBSH_NAME "adb_sql_history"

#define ADBSS_DUMMY_QUERY "<adbss dummy query>"
#define isAdbssDummyQuery(query) (query != NULL && pg_strcasecmp(query, ADBSS_DUMMY_QUERY) == 0)

typedef enum AdbssAttributes
{
    Anum_adbss_pid = 1,
    Anum_adbss_dbid,
    Anum_adbss_query,
    Anum_adbss_last_time,
    Anum_adbss_calls
} AdbssAttributes;

/*---- GUC variables ----*/
typedef enum
{
    ADBSS_TRACK_NONE, /* track no statements */
    ADBSS_TRACK_TOP,  /* only top level statements */
    ADBSS_TRACK_ALL   /* all statements, including nested ones */
} AdbssTrackLevel;

static const struct config_enum_entry adbss_track_options[] = {{"none", ADBSS_TRACK_NONE, false},
                                                               {"top", ADBSS_TRACK_TOP, false},
                                                               {"all", ADBSS_TRACK_ALL, false},
                                                               {NULL, 0, false}};

static bool adbSHEnabled = false;
static bool adbSHLoadlibrary = false;

static int adbssTrackLevel = ADBSS_TRACK_NONE;
static int adbSHSqlNum = ADBSH_DEFAULT_MAX_RECORD;

typedef struct SQLHistoryItem
{
    pid_t pid;
    Oid dbid;
    TimestampTz lasttime;
    int64 ecount;
    slock_t mutex;
    char sql[FLEXIBLE_ARRAY_MEMBER];
} SQLHistoryItem;

#define SQL_HISTORY_ITEM_SIZE() MAXALIGN(offsetof(SQLHistoryItem, sql) + pgstat_track_activity_query_size)

#define NATTS_NUM Anum_adbss_calls

SQLHistoryItem **my_sql_history = NULL;
bool sql_history_set = false;
char *shmem = NULL;

/*---- Function declarations ----*/
void _PG_init(void);
void _PG_fini(void);

static void shmem_startup(void);
static void adbsh_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count, bool execute_once);
static Size sql_history_shm_size(void);
static void bind_my_sql_history(void);
static void define_custom_param(void);
static void save_sql(QueryDesc *queryDesc);
static bool find_sql_pos(char *sql, int *idx);
static void insert_sql(int sql_idx, char *sql);
static void update_sql(int sql_idx);
static char *get_querytext(QueryDesc *queryDesc);

/* Current nesting depth of ExecutorRun ecount */
static int nested_level = 0;

/* Saved hook values in case of unload */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;

#define adbssAvailable()                                                                                               \
    ((adbSHEnabled) &&                                                                                                 \
     (adbssTrackLevel == ADBSS_TRACK_ALL || (adbssTrackLevel == ADBSS_TRACK_TOP && nested_level == 0)))

#define adbssQueryAvailable()                                                                                          \
    ((adbSHLoadlibrary) && (adbssTrackLevel == ADBSS_TRACK_ALL || adbssTrackLevel == ADBSS_TRACK_TOP))
/*
 * Module load callback
 */
void _PG_init(void)
{
    if (!process_shared_preload_libraries_in_progress)
        return;

    define_custom_param();

    adbSHLoadlibrary = true;

    EmitWarningsOnPlaceholders(ADBSH_NAME);

    /*
     * Request additional shared resources.  (These are no-ops if we're not in
     * the postmaster process.)  We'll allocate or attach to the shared
     * resources in adbsh_shmem_startup().
     */
    RequestAddinShmemSpace(sql_history_shm_size());

    /*
     * Install hooks.
     */
    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = shmem_startup;
    prev_ExecutorRun = ExecutorRun_hook;
    ExecutorRun_hook = adbsh_ExecutorRun;
}

/*
 * Module unload callback
 */
void _PG_fini(void)
{
    /* Uninstall hooks. */
    shmem_startup_hook = prev_shmem_startup_hook;
    ExecutorRun_hook = prev_ExecutorRun;
}

Datum adb_sql_history(PG_FUNCTION_ARGS)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    TupleDesc tupdesc;
    Tuplestorestate *tupstore;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;
    int i = 0;
    int j = 0;
    char *ptr;
    SQLHistoryItem *sql_item = NULL;
    Datum values[NATTS_NUM] = {0};
    bool nulls[NATTS_NUM] = {0};

    if (!adbssQueryAvailable())
    {
        ereport(COMMERROR, errmsg("preload adb_sql_history failed. Please check your postgresql.conf"));
        PG_RETURN_NULL();
    }

    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
    {
        ereport(COMMERROR, errmsg("set-valued function called in context that cannot accept a set"));
        PG_RETURN_NULL();
    }
    if (!(rsinfo->allowedModes & SFRM_Materialize))
    {
        ereport(COMMERROR, errmsg("materialize mode required, but it is not allowed in this context"));
        PG_RETURN_NULL();
    }

    /* Switch into long-lived context to construct returned data structures */
    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
    {
        ereport(COMMERROR, errmsg("return type must be a row type"));
        PG_RETURN_NULL();
    }
    /* Switch into long-lived context to construct returned data structures */

    tupstore = tuplestore_begin_heap(true, false, work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;

    MemoryContextSwitchTo(oldcontext);

    for (i = 1; i <= MaxBackends; i++)
    {
        ptr = shmem + (SQL_HISTORY_ITEM_SIZE() * adbSHSqlNum) * (i - 1);
        for (j = 0; j < adbSHSqlNum; j++)
        {
            MemSet(values, 0, sizeof(values));
            MemSet(nulls, 0, sizeof(nulls));

            sql_item = (SQLHistoryItem *)ptr;

            if (!sql_item)
            {
                PG_RETURN_NULL();
            }

            SpinLockAcquire(&sql_item->mutex);
            if (strlen(sql_item->sql) > 0)
            {
                values[Anum_adbss_pid - 1] = Int64GetDatumFast(sql_item->pid);
                values[Anum_adbss_dbid - 1] = ObjectIdGetDatum(sql_item->dbid);
                values[Anum_adbss_query - 1] = CStringGetTextDatum(sql_item->sql);

                values[Anum_adbss_last_time - 1] = TimestampTzGetDatum(sql_item->lasttime);
                values[Anum_adbss_calls - 1] = Int64GetDatumFast(sql_item->ecount);

                nulls[Anum_adbss_pid - 1] = false;
                nulls[Anum_adbss_dbid - 1] = false;
                nulls[Anum_adbss_query - 1] = false;
                nulls[Anum_adbss_last_time - 1] = false;
                nulls[Anum_adbss_calls - 1] = false;

                tuplestore_putvalues(tupstore, tupdesc, values, nulls);
            }
            SpinLockRelease(&sql_item->mutex);

            ptr += SQL_HISTORY_ITEM_SIZE();
        }
    }

    tuplestore_donestoring(tupstore);

    PG_RETURN_NULL();
}

static Size sql_history_shm_size(void)
{
    Size size = mul_size(SQL_HISTORY_ITEM_SIZE(), adbSHSqlNum);
    if (MaxBackends == 0)
    {
        /* not call InitializeMaxBackends() */
        Size max_backends = MaxConnections + autovacuum_max_workers + 1 +
#if defined(ADBMGRD)
                            /* and adb monitor launcher */
                            adbmonitor_max_workers + 1 +
#endif
                            max_worker_processes + max_wal_senders;
        size = mul_size(size, max_backends);
    }
    else
    {
        size = mul_size(size, MaxBackends);
    }
    return size;
}

static void shmem_startup(void)
{
    SQLHistoryItem *item;
    char *ptr;
    int i;
    bool found;

    if (prev_shmem_startup_hook)
        (*prev_shmem_startup_hook)();

    shmem = ShmemInitStruct(ADBSH_NAME, sql_history_shm_size(), &found);
    if (unlikely(found == false))
    {
        ptr = shmem;
        i = MaxBackends * adbSHSqlNum;
        while (i > 0)
        {
            item = (SQLHistoryItem *)ptr;
            ptr += SQL_HISTORY_ITEM_SIZE();
            item->sql[0] = '\0';
            item->lasttime = 0;
            SpinLockInit(&item->mutex);
            --i;
        }
    }
}
/*
 * ExecutorRun hook: all we need do is track nesting depth
 */
static void adbsh_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count, bool execute_once)
{
    PG_TRY();
    {
        bind_my_sql_history();

        if (my_sql_history && adbssAvailable())
            save_sql(queryDesc);

        nested_level++;

        if (prev_ExecutorRun)
            prev_ExecutorRun(queryDesc, direction, count, execute_once);
        else
            standard_ExecutorRun(queryDesc, direction, count, execute_once);

        nested_level--;
    }
    PG_CATCH();
    {
        if (nested_level > 0)
            nested_level--;
        PG_RE_THROW();
    }
    PG_END_TRY();
}
static void bind_my_sql_history(void)
{
    char *ptr;
    int i;

    if (MyBackendId != InvalidBackendId && !sql_history_set)
    {
        my_sql_history = MemoryContextAlloc(TopMemoryContext, sizeof(my_sql_history[0]) * adbSHSqlNum);
        ptr = shmem + (SQL_HISTORY_ITEM_SIZE() * adbSHSqlNum) * (MyBackendId - 1);

        for (i = 0; i < adbSHSqlNum; ++i)
        {
            my_sql_history[i] = (SQLHistoryItem *)ptr;
            SpinLockAcquire(&my_sql_history[i]->mutex);
            MemSet(my_sql_history[i]->sql, 0, pgstat_track_activity_query_size);
            my_sql_history[i]->lasttime = 0;
            my_sql_history[i]->pid = 0;
            my_sql_history[i]->dbid = 0;
            my_sql_history[i]->ecount = 0;
            SpinLockRelease(&my_sql_history[i]->mutex);
            ptr += SQL_HISTORY_ITEM_SIZE();
        }

        sql_history_set = true;
    }
}

static void define_custom_param(void)
{
    DefineCustomBoolVariable("adb_sql_history.enable", "enable adb_sql_history.", NULL, &adbSHEnabled, true, PGC_SUSET,
                             0, NULL, NULL, NULL);
    DefineCustomEnumVariable("adb_sql_history.track", "Selects which statements are tracked by adb_sql_history.", NULL,
                             &adbssTrackLevel, ADBSS_TRACK_TOP, adbss_track_options, PGC_SUSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("adb_sql_history.sql_num", "Sets the sql number of one process tracked by adb_sql_history.",
                            NULL, &adbSHSqlNum, ADBSH_DEFAULT_MAX_RECORD, 1, INT_MAX, PGC_POSTMASTER, 0, NULL, NULL,
                            NULL);
}
static void save_sql(QueryDesc *queryDesc)
{
    bool found = false;
    char *sql = NULL;
    int sql_idx = 0;

    sql = get_querytext(queryDesc);

    if (sql == NULL || strlen(sql) == 0)
        return;

    if (strncmp(sql, "<cluster query>", strlen(sql)) == 0)
        return;

    found = find_sql_pos(sql, &sql_idx);
    if (!found)
        insert_sql(sql_idx, sql);
    else
        update_sql(sql_idx);
    return;
}
static bool find_sql_pos(char *sql, int *idx)
{
    int i = 0;
    bool sql_found = false;
    SQLHistoryItem *sql_item = NULL;
    TimestampTz min_time = 0;
    bool empty_found = false;

    if (strlen(sql) == 0)
    {
        ereport(COMMERROR, errmsg("find_sql, sql is null"));
        return false;
    }

    for (i = 0; i < adbSHSqlNum; i++)
    {
        sql_item = (SQLHistoryItem *)my_sql_history[i];
        if (strlen(sql_item->sql) > 0)
        {
            if (0 == pg_strcasecmp(sql_item->sql, sql))
            {
                sql_found = true;
                *idx = i;
                break;
            }
        }
        else
        {
            if (!empty_found)
            {
                empty_found = true;
                *idx = i;
                break;
            }
        }
    }

    if (!sql_found && !empty_found)
    {
        /* list is full, find the youngest time */
        for (i = 0; i < adbSHSqlNum; i++)
        {
            /*do not need add slock_t of SQLHistoryItem here, because only do read here, and no other place will call
             * this function*/
            sql_item = (SQLHistoryItem *)my_sql_history[i];

            if (i == 0)
            {
                min_time = sql_item->lasttime;
                *idx = i;
            }
            else
            {
                if (sql_item->lasttime < min_time)
                {
                    min_time = sql_item->lasttime;
                    *idx = i;
                }
            }
        }
    }

    return sql_found;
}
static void insert_sql(int sql_idx, char *sql)
{
    SQLHistoryItem *sql_item = NULL;

    if (sql_idx >= adbSHSqlNum)
    {
        ereport(COMMERROR, errmsg("sql_idx is not smaller than adbSHSqlNum, this should not happen"));
        return;
    }
    sql_item = (SQLHistoryItem *)my_sql_history[sql_idx];
    SpinLockAcquire(&sql_item->mutex);
    sql_item->pid = MyProcPid;
    sql_item->dbid = MyDatabaseId;
    StrNCpy(sql_item->sql, sql, pgstat_track_activity_query_size);
    sql_item->lasttime = GetCurrentTimestamp();
    sql_item->ecount = 1;
    SpinLockRelease(&sql_item->mutex);
}

static void update_sql(int sql_idx)
{
    SQLHistoryItem *sql_item = NULL;
    if (sql_idx >= adbSHSqlNum)
    {
        ereport(COMMERROR, errmsg("sql_idx is not smaller than adbSHSqlNum, this should not happen"));
        return;
    }
    sql_item = (SQLHistoryItem *)my_sql_history[sql_idx];
    SpinLockAcquire(&sql_item->mutex);
    sql_item->lasttime = GetCurrentTimestamp();
    sql_item->ecount++;
    SpinLockRelease(&sql_item->mutex);
}
static char *get_querytext(QueryDesc *queryDesc)
{
    const char *query;
    int query_location, query_len;
    char *queryText;

    query = queryDesc->sourceText;
    if (!query)
        return NULL;

    query_location = queryDesc->plannedstmt->stmt_location;
    query_len = queryDesc->plannedstmt->stmt_len;

    /*
     * Confine our attention to the relevant part of the string, if the query
     * is a portion of a multi-statement source string.
     *
     * First apply starting offset, unless it's -1 (unknown).
     */
    if (query_location >= 0)
    {
        if (query_location > strlen(query))
        {
            ereport(COMMERROR, errmsg("query_location is bigger than lenth of query, this should not happen"));
            return NULL;
        }
        query += query_location;
        /* Length of 0 (or -1) means "rest of string" */
        if (query_len <= 0)
            query_len = strlen(query);
        else
        {
            if (query_len > strlen(query))
            {
                ereport(COMMERROR, errmsg("query_len is bigger than lenth of query, this should not happen"));
                return NULL;
            }
        }
    }
    else
    {
        /* If query location is unknown, distrust query_len as well */
        query_location = 0;
        query_len = strlen(query);
    }

    /*
     * Discard leading and trailing whitespace, too.  Use scanner_isspace()
     * not libc's isspace(), because we want to match the lexer's behavior.
     */
    while (query_len > 0 && scanner_isspace(query[0]))
        query++, query_location++, query_len--;
    while (query_len > 0 && scanner_isspace(query[query_len - 1]))
        query_len--;

    queryText = palloc(query_len + 1);
    memcpy(queryText, query, query_len);
    queryText[query_len] = '\0';
    return queryText;
}
