
#include "postgres.h"

#include <math.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/hash.h"
#include "executor/instrument.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "parser/analyze.h"
#include "parser/parsetree.h"
#include "parser/scanner.h"
#include "parser/scansup.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/spin.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "commands/extension.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_extension.h"
#include "catalog/indexing.h"
#include "catalog/pg_type.h"
#include "optimizer/planner.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "access/tupdesc.h"
#include "adb_stat_statements.h"
#include "utils/timestamp.h"
#include "nodes/nodeFuncs.h"
#include "utils/ps_status.h"
#include "executor/execCluster.h"
#include "commands/explain.h"

#if PG_VERSION_NUM >= 100000
#include "utils/queryenvironment.h"
#include "catalog/index.h"
#endif

#if PG_VERSION_NUM >= 120000
#include "catalog/pg_extension_d.h"
#endif

#ifdef ADB
#include "pgxc/pgxc.h"
#include "optimizer/pgxcplan.h"
#endif

#define ADBSS_DEFAULT_SHM_SIZE (1 * 1024 * 1024)
#define ADBSS_DEFAULT_MAX_RECORD (5000)
#define ADBSS_DEFAULT_MAX_LENGTH (1 * 1024 * 1024)
#define NOT_FOUND_STR "<NOT_FOUND>"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(adb_stat_statements);
PG_FUNCTION_INFO_V1(adb_stat_statements_reset);
PG_FUNCTION_INFO_V1(explain_plan);
PG_FUNCTION_INFO_V1(explain_rtable_of_plan);
PG_FUNCTION_INFO_V1(explain_plan_nodes_of_plan);
PG_FUNCTION_INFO_V1(explain_rtable_of_query);
PG_FUNCTION_INFO_V1(explain_rtable_plan_of_query);

typedef struct AdbssOids
{
	Oid schemaOid;
	Oid tableOid;
	Oid queryidIndexOid;
} AdbssOids;

typedef struct AdbssSharedState
{
	LWLock lock;
	size_t mem[FLEXIBLE_ARRAY_MEMBER]; /* avoid byte align */
} AdbssSharedState;

typedef struct AdbssKey
{
	Oid userid;		/* user OID */
	Oid dbid;		/* database OID */
	uint64 queryid; /* query identifier */
	uint64 planid;	/* plan identifier */
} AdbssKey;

typedef struct AdbssCounters
{
	uint64 calls;
	uint64 rows;
	double total_time;
	double min_time;
	double max_time;
	double mean_time;
	TimestampTz last_execution;
} AdbssCounters;

typedef struct AdbssEntry
{
	AdbssKey key;			/* hash key of entry - MUST BE FIRST */
	AdbssCounters counters; /* the statistics for this query */
	dsa_pointer tupleInDsa; /* store other attributes in dsa */
	slock_t mutex;			/* protects the record only */
} AdbssEntry;

/*---- Function declarations ----*/

void _PG_init(void);
void _PG_fini(void);

static void adbss_shmem_startup(void);
static void adbss_relcache_hook(Datum arg, Oid relid);
static void adbss_post_parse_analyze(ParseState *pstate, Query *query);
static PlannedStmt *adbss_planner_hook(Query *parse,
									   int cursorOptions,
									   ParamListInfo boundParams);
static void adbss_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void adbss_ExecutorRun(QueryDesc *queryDesc,
							  ScanDirection direction,
							  uint64 count, bool execute_once);
static void adbss_ExecutorFinish(QueryDesc *queryDesc);
static void adbss_ExecutorEnd(QueryDesc *queryDesc);

static inline Size adbss_memsize(void);

static bool adbssTrackable(QueryDesc *queryDesc);
static void adbssStoreToTable(QueryDesc *queryDesc);
static bool checkAdbssAttrs(TupleDesc adbssRelDesc);
static void insertAdbssPlan(Relation adbssRel,
							CatalogIndexState indstate,
							QueryDesc *queryDesc,
							char *planString,
							AdbssKey *adbssKey,
							bool omitSavePlan);
static void updateAdbssPlan(Relation adbssRel,
							CatalogIndexState indstate,
							HeapTuple oldTuple,
							QueryDesc *queryDesc,
							bool omitSavePlan);
static bool findAdbssPlan(Relation adbssRel,
						  Relation adbssQueryidIndex,
						  Snapshot snapshot,
						  char *planString,
						  AdbssKey *adbssKey,
						  HeapTuple *foundTuple,
						  bool omitSavePlan);

static void adbssStoreToDsa(QueryDesc *queryDesc);
static TupleDesc form_adbss_text_tuple_desc(void);
static MinimalTuple form_adbss_text_tuple(TupleDesc textTupleDesc,
										  QueryDesc *queryDesc,
										  char *plan,
										  bool omitSavePlan);
static AdbssEntry *entry_alloc(AdbssKey *key);
static void entry_dealloc(void);
static void freeTupleInDsa(AdbssEntry *entry);
static void saveTupleToDsa(AdbssEntry *entry,
						   MinimalTuple textTuple, dsa_pointer dp);
static int adbss_entry_cmp(const void *lhs, const void *rhs);
static bool isPlanTooLarge(uint64 queryId, size_t planLength);
static Datum ParamList2TextArr(const ParamListInfo from);
static bool is_alter_extension_cmd(Node *stmt);
static bool is_drop_extension_stmt(Node *stmt);
static bool is_create_extension_stmt(Node *stmt);
static bool useTableForStorage(void);
static void initializeAdbssOids(void);
static void invalidateAdbssOids(void);
static Oid getAdbssRelOid(Oid schemaOid, const char *relName);
static Oid getAdbssTableOid(Oid schemaOid, const char *relName);
static Oid getAdbssIndexOid(Oid schemaOid, const char *relName);
static Oid getAdbssExtensionSchemaOid(void);
static uint64 adbss_hash_string(const char *str, int len);
static char *getQueryText(QueryDesc *queryDesc);
static char *explainQueryDesc(QueryDesc *queryDesc,
							  ExplainFormat format);
static bool relationExpressionWalker(Node *node, List **relationPlans);
static bool relationPlanWalker(Plan *plan,
							   PlannedStmt *stmt,
							   List **relationPlans);

/*---- Local variables ----*/

/* Current nesting depth of ExecutorRun calls */
static int nested_level = 0;

/* Saved hook values in case of unload */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static planner_hook_type prev_planner_hook = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

/* Links to shared memory state */
static AdbssSharedState *adbssState = NULL;
static HTAB *adbssHtab = NULL;
static dsa_area *adbssDsaArea = NULL;
static bool adbssEnabled = true;
static bool adbssDropped = false;
static AdbssOids adbssOids = {
	InvalidOid, /* schema_oid */
	InvalidOid, /* tableOid */
	InvalidOid	/* queryidIndexOid */
};

/*---- GUC variables ----*/

typedef enum
{
	ADBSS_TRACK_NONE, /* track no statements */
	ADBSS_TRACK_TOP,  /* only top level statements */
	ADBSS_TRACK_ALL	  /* all statements, including nested ones */
} AdbssTrackLevel;

static const struct config_enum_entry adbss_track_options[] =
	{
		{"none", ADBSS_TRACK_NONE, false},
		{"top", ADBSS_TRACK_TOP, false},
		{"all", ADBSS_TRACK_ALL, false},
		{NULL, 0, false}};

static const struct config_enum_entry adbss_format_options[] = {
	{"text", EXPLAIN_FORMAT_TEXT, false},
	{"xml", EXPLAIN_FORMAT_XML, false},
	{"json", EXPLAIN_FORMAT_JSON, false},
	{"yaml", EXPLAIN_FORMAT_YAML, false},
	{NULL, 0, false}};

static int adbssMaxRecord = ADBSS_DEFAULT_MAX_RECORD;
static int adbssMaxLength = ADBSS_DEFAULT_MAX_LENGTH;
static int adbssTrackLevel; /* tracking level */
static bool adbssExplainAnalyze = true;
static bool adbssExplainVerbose = false;
static bool adbssExplainBuffers = false;
static bool adbssExplainTriggers = false;
static bool adbssExplainTiming = true;
static int adbssExplainFormat = EXPLAIN_FORMAT_TEXT;

#define adbssAvailable()                    \
	(adbssEnabled && !adbssDropped &&       \
	 (adbssTrackLevel == ADBSS_TRACK_ALL || \
	  (adbssTrackLevel == ADBSS_TRACK_TOP && nested_level == 0)))

#define adbssOidValid() \
	(adbssOids.schemaOid != InvalidOid)

#ifdef ADB
#define transactionAllowed()                                    \
	((GetCurrentTransactionIdIfAny() != InvalidTransactionId || \
	  IsConnFromApp() ||                                        \
	  in_cluster_mode) &&                                       \
	 (!RecoveryInProgress()))
#else
#define transactionAllowed() (!RecoveryInProgress())
#endif

/*
 * Module load callback
 */
void _PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomBoolVariable("adb_stat_statements.enabled",
							 "enable adb_stat_statements.",
							 NULL,
							 &adbssEnabled,
							 true,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("adb_stat_statements.max_plans",
							"Sets the maximum number of plans tracked by adb_stat_statements.",
							NULL,
							&adbssMaxRecord,
							ADBSS_DEFAULT_MAX_RECORD,
							100,
							INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("adb_stat_statements.max_length",
							"Sets the maximum length of one plan tracked by adb_stat_statements.",
							NULL,
							&adbssMaxLength,
							ADBSS_DEFAULT_MAX_LENGTH,
							100 * 1024,
							INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomEnumVariable("adb_stat_statements.track",
							 "Selects which statements are tracked by adb_stat_statements.",
							 NULL,
							 &adbssTrackLevel,
							 ADBSS_TRACK_TOP,
							 adbss_track_options,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("adb_stat_statements.explain_analyze",
							 "Sets EXPLAIN ANALYZE of plans tracked by adb_stat_statements.",
							 NULL,
							 &adbssExplainAnalyze,
							 true,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("adb_stat_statements.explain_verbose",
							 "Sets EXPLAIN VERBOSE of plans tracked by adb_stat_statements.",
							 NULL,
							 &adbssExplainVerbose,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("adb_stat_statements.explain_buffers",
							 "Sets EXPLAIN buffers usage of plans tracked by adb_stat_statements.",
							 "This has no effect unless explain_analyze is also set.",
							 &adbssExplainBuffers,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("adb_stat_statements.explain_triggers",
							 "Sets Include trigger statistics of plans tracked by adb_stat_statements.",
							 "This has no effect unless explain_analyze is also set.",
							 &adbssExplainTriggers,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("adb_stat_statements.explain_timing",
							 "Sets EXPLAIN TIMING of plans tracked by adb_stat_statements.",
							 "This has no effect unless explain_analyze is also set.",
							 &adbssExplainTiming,
							 true,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomEnumVariable("adb_stat_statements.explain_format",
							 "Sets EXPLAIN format of plan tracked by adb_stat_statements.",
							 NULL,
							 &adbssExplainFormat,
							 EXPLAIN_FORMAT_TEXT,
							 adbss_format_options,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	EmitWarningsOnPlaceholders(ADBSS_NAME);

	/*
	 * Request additional shared resources.  (These are no-ops if we're not in
	 * the postmaster process.)  We'll allocate or attach to the shared
	 * resources in adbss_shmem_startup().
	 */
	RequestAddinShmemSpace(adbss_memsize());

	/*
	 * Install hooks.
	 */
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = adbss_shmem_startup;
	prev_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = adbss_post_parse_analyze;
	prev_planner_hook = planner_hook;
	planner_hook = adbss_planner_hook;
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = adbss_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = adbss_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = adbss_ExecutorFinish;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = adbss_ExecutorEnd;
}

/*
 * Module unload callback
 */
void _PG_fini(void)
{
	/* Uninstall hooks. */
	shmem_startup_hook = prev_shmem_startup_hook;
	post_parse_analyze_hook = prev_post_parse_analyze_hook;
	planner_hook = prev_planner_hook;
	ExecutorStart_hook = prev_ExecutorStart;
	ExecutorRun_hook = prev_ExecutorRun;
	ExecutorFinish_hook = prev_ExecutorFinish;
	ExecutorEnd_hook = prev_ExecutorEnd;

	invalidateAdbssOids();
}
/*
 * Estimate shared memory space needed.
 */
static inline Size adbss_memsize(void)
{
	return add_size(ADBSS_DEFAULT_SHM_SIZE,
					hash_estimate_size(adbssMaxRecord,
									   sizeof(AdbssEntry)));
}

static void adbss_shmem_startup(void)
{
	bool found;
	HASHCTL info;
	MemoryContext oldcontext;

	if (prev_shmem_startup_hook)
		(*prev_shmem_startup_hook)();

	/* reset in case this is a restart within the postmaster */
	adbssState = NULL;
	adbssHtab = NULL;
	adbssDsaArea = NULL;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	adbssState = ShmemInitStruct(ADBSS_NAME,
								 ADBSS_DEFAULT_SHM_SIZE,
								 &found);
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	if (found)
	{
		adbssDsaArea = dsa_attach_in_place(adbssState->mem, NULL);
	}
	else
	{
		/* First time through ... */
		LWLockInitialize(&adbssState->lock, LWLockNewTrancheId());
		adbssDsaArea = dsa_create_in_place((void *)adbssState->mem,
										   ADBSS_DEFAULT_SHM_SIZE - offsetof(AdbssSharedState, mem),
										   LWLockNewTrancheId(),
										   NULL);
		dsa_pin(adbssDsaArea);
	}
	MemoryContextSwitchTo(oldcontext);

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(AdbssKey);
	info.entrysize = sizeof(AdbssEntry);
	adbssHtab = ShmemInitHash(ADBSS_NAME " hash",
							  adbssMaxRecord, adbssMaxRecord,
							  &info,
							  HASH_ELEM | HASH_BLOBS);

	LWLockRelease(AddinShmemInitLock);
	LWLockRegisterTranche(adbssState->lock.tranche, ADBSS_NAME);
	dsa_pin_mapping(adbssDsaArea);
}

static void adbss_relcache_hook(Datum arg, Oid relid)
{
	if (relid == InvalidOid ||
		(relid == adbssOids.tableOid) ||
		(relid == adbssOids.queryidIndexOid))
		invalidateAdbssOids();
}

static void
adbss_post_parse_analyze(ParseState *pstate, Query *query)
{
	if (prev_post_parse_analyze_hook)
		prev_post_parse_analyze_hook(pstate, query);

	if (query->commandType == CMD_UTILITY)
	{
		/* ... ALTER EXTENSION adb_stat_statements */
		if (is_alter_extension_cmd(query->utilityStmt))
		{
			invalidateAdbssOids();
		}
		/* ... DROP EXTENSION adb_stat_statements */
		else if (is_drop_extension_stmt(query->utilityStmt))
		{
			invalidateAdbssOids();
			adbssDropped = true;
		}
		else if (is_create_extension_stmt(query->utilityStmt))
		{
			adbssDropped = false;
		}
	}
}

static PlannedStmt *adbss_planner_hook(Query *parse,
									   int cursorOptions,
									   ParamListInfo boundParams)
{
	PlannedStmt *result;

	if (prev_planner_hook)
		result = prev_planner_hook(parse, cursorOptions, boundParams);
	else
	{
#ifdef ADB
		/*
		 * A Coordinator receiving a query from another Coordinator
		 * is not allowed to go into PGXC planner.
		 */
		if (IsCnMaster())
			result = pgxc_planner(parse, cursorOptions, boundParams);
		else
#endif
			result = standard_planner(parse, cursorOptions, boundParams);
	}
	/* 
	 * As far as I know, pgxc_planner does not save the queryId to PlannedStmt.
	 * If the value of a SQL's PlannedStmt->queryId not been assigned, 
	 * The Instrumentation data will be ignored by pg_stat_statements.
	 * And for the reason of integration with pg_stat_statements,
	 * this plugin (adb_stat_statments) uses the same queryId generate by
	 * pg_stat_statements(see pgss_post_parse_analyze). 
	 */
	result->queryId = parse->queryId;
	return result;
}

static void adbss_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	if (queryDesc->plannedstmt->queryId != UINT64CONST(0) &&
		adbssTrackable(queryDesc))
	{
		/* Enable per-node instrumentation iff analyze is required. */
		if (adbssExplainAnalyze && (eflags & EXEC_FLAG_EXPLAIN_ONLY) == 0)
		{
			if (adbssExplainTiming)
				queryDesc->instrument_options |= INSTRUMENT_TIMER;
			else
				queryDesc->instrument_options |= INSTRUMENT_ROWS;
			if (adbssExplainBuffers)
				queryDesc->instrument_options |= INSTRUMENT_BUFFERS;
		}
	}
	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
}

/*
 * ExecutorRun hook: all we need do is track nesting depth
 */
static void
adbss_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count,
				  bool execute_once)
{
	nested_level++;
	PG_TRY();
	{
		if (prev_ExecutorRun)
			prev_ExecutorRun(queryDesc, direction, count, execute_once);
		else
			standard_ExecutorRun(queryDesc, direction, count, execute_once);
		nested_level--;
	}
	PG_CATCH();
	{
		nested_level--;
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * ExecutorFinish hook: all we need do is track nesting depth
 */
static void
adbss_ExecutorFinish(QueryDesc *queryDesc)
{
	nested_level++;
	PG_TRY();
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
		nested_level--;
	}
	PG_CATCH();
	{
		nested_level--;
		PG_RE_THROW();
	}
	PG_END_TRY();
}

static void adbss_ExecutorEnd(QueryDesc *queryDesc)
{
	if (queryDesc->plannedstmt->queryId == UINT64CONST(0) ||
		!adbssTrackable(queryDesc))
		goto done;

	if (isAdbssDummyQuery(queryDesc->sourceText))
	{
		/* pg_stat_statements should not save this sql */
		queryDesc->plannedstmt->queryId = UINT64CONST(0);
		goto done;
	}

	/*
	 * Make sure stats accumulation is done.  (Note: it's okay if several
	 * levels of hook all do this.)
	 */
	if (queryDesc->totaltime)
		InstrEndLoop(queryDesc->totaltime);

	if (useTableForStorage())
	{
		if (!transactionAllowed())
			goto done;

		adbssStoreToTable(queryDesc);
	}
	else
		adbssStoreToDsa(queryDesc);

done:
	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

static bool adbssTrackable(QueryDesc *queryDesc)
{
	if (!adbssAvailable())
		return false;
	if (!adbssOidValid())
	{
		initializeAdbssOids();
		return adbssOidValid();
	}
	return true;
}

static void adbssStoreToTable(QueryDesc *queryDesc)
{
	HeapTuple oldTuple;
	Relation adbssRel = NULL;
	Relation adbssQueryidIndex = NULL;
	Snapshot snapshot = NULL;
	CatalogIndexState indstate = NULL;
	LOCKMODE heap_lock;
	PlannedStmt *plannedstmt;
	bool found;
	uint64 queryId;
	AdbssKey adbssKey;
	char *planString;
	size_t planLength;
	bool omitSavePlan;
	uint64 planid;

	heap_lock = AccessExclusiveLock;
	adbssRel = heap_open(adbssOids.tableOid, heap_lock);
	if (!checkAdbssAttrs(RelationGetDescr(adbssRel)))
	{
		ereport(WARNING, (errmsg(ADBSS_NAME " extension installed incorrectly")));
		goto clean;
	}

	indstate = CatalogOpenIndexes(adbssRel);
	if (indstate == NULL || indstate->ri_NumIndices != 1)
	{
		ereport(WARNING, (errmsg(ADBSS_NAME " extension installed incorrectly")));
		goto clean;
	}
	adbssQueryidIndex = indstate->ri_IndexRelationDescs[0];
	if (adbssQueryidIndex->rd_id != adbssOids.queryidIndexOid)
	{
		ereport(WARNING, (errmsg(ADBSS_NAME " extension installed incorrectly")));
		goto clean;
	}

	snapshot = RegisterSnapshot(GetLatestSnapshot());

	plannedstmt = queryDesc->plannedstmt;
	queryId = plannedstmt->queryId;
	planString = nodeToString(plannedstmt);
	planLength = strlen(planString);
	omitSavePlan = isPlanTooLarge(queryId, planLength);
	planid = adbss_hash_string((const char *)planString, planLength);

	adbssKey.userid = GetUserId();
	adbssKey.dbid = MyDatabaseId;
	adbssKey.queryid = queryId;
	adbssKey.planid = planid;

	/* Try to find already planned statement */
	found = findAdbssPlan(adbssRel,
						  adbssQueryidIndex,
						  snapshot,
						  planString,
						  &adbssKey,
						  &oldTuple,
						  omitSavePlan);
	if (found)
		updateAdbssPlan(adbssRel,
						indstate,
						oldTuple,
						queryDesc,
						omitSavePlan);
	else
		insertAdbssPlan(adbssRel,
						indstate,
						queryDesc,
						planString,
						&adbssKey,
						omitSavePlan);

clean:
	if (snapshot)
		UnregisterSnapshot(snapshot);
	if (indstate)
		CatalogCloseIndexes(indstate);
	if (adbssRel)
		heap_close(adbssRel, heap_lock);
}

#define checkAdbssRelDescAttr(Anum_attr, attr_typid)                        \
	if (TupleDescAttr(adbssRelDesc, Anum_attr - 1)->attisdropped ||         \
		TupleDescAttr(adbssRelDesc, Anum_attr - 1)->atttypid != attr_typid) \
		goto done;

static bool checkAdbssAttrs(TupleDesc adbssRelDesc)
{
	bool ok = false;
	if (adbssRelDesc->natts != Natts_adbss)
		goto done;
	checkAdbssRelDescAttr(Anum_adbss_userid, OIDOID);
	checkAdbssRelDescAttr(Anum_adbss_dbid, OIDOID);
	checkAdbssRelDescAttr(Anum_adbss_queryid, INT8OID);
	checkAdbssRelDescAttr(Anum_adbss_planid, INT8OID);
	checkAdbssRelDescAttr(Anum_adbss_calls, INT8OID);
	checkAdbssRelDescAttr(Anum_adbss_rows, INT8OID);
	checkAdbssRelDescAttr(Anum_adbss_total_time, FLOAT8OID);
	checkAdbssRelDescAttr(Anum_adbss_min_time, FLOAT8OID);
	checkAdbssRelDescAttr(Anum_adbss_max_time, FLOAT8OID);
	checkAdbssRelDescAttr(Anum_adbss_mean_time, FLOAT8OID);
	checkAdbssRelDescAttr(Anum_adbss_last_execution, TIMESTAMPTZOID);
	checkAdbssRelDescAttr(Anum_adbss_query, TEXTOID);
	checkAdbssRelDescAttr(Anum_adbss_plan, TEXTOID);
	checkAdbssRelDescAttr(Anum_adbss_explain_format, INT4OID);
	checkAdbssRelDescAttr(Anum_adbss_explain_plan, TEXTOID);
	checkAdbssRelDescAttr(Anum_adbss_bound_params, TEXTARRAYOID);

	ok = true;

done:
	return ok;
}

static void insertAdbssPlan(Relation adbssRel,
							CatalogIndexState indstate,
							QueryDesc *queryDesc,
							char *planString,
							AdbssKey *adbssKey,
							bool omitSavePlan)
{
	HeapTuple tuple;
	Datum values[Natts_adbss] = {0};
	bool nulls[Natts_adbss] = {0};
	char *queryText;
	double totalTime;
	char *explainPlanString;

	values[Anum_adbss_userid - 1] = ObjectIdGetDatum(GetUserId());
	values[Anum_adbss_dbid - 1] = ObjectIdGetDatum(MyDatabaseId);
	values[Anum_adbss_queryid - 1] = UInt64GetDatum(adbssKey->queryid);
	values[Anum_adbss_planid - 1] = UInt64GetDatum(adbssKey->planid);
	values[Anum_adbss_calls - 1] = Int64GetDatum(1);
	values[Anum_adbss_rows - 1] = Int64GetDatum(queryDesc->estate->es_processed);

	if (queryDesc->totaltime)
		totalTime = queryDesc->totaltime->total * 1000.0; /* convert to msec */
	else
		totalTime = 0;
	values[Anum_adbss_total_time - 1] = Float8GetDatum(totalTime);
	values[Anum_adbss_min_time - 1] = Float8GetDatum(totalTime);
	values[Anum_adbss_max_time - 1] = Float8GetDatum(totalTime);
	values[Anum_adbss_mean_time - 1] = Float8GetDatum(totalTime);
	values[Anum_adbss_last_execution - 1] = TimestampTzGetDatum(GetCurrentTimestamp());

	queryText = getQueryText(queryDesc);
	if (queryText)
		values[Anum_adbss_query - 1] = CStringGetTextDatum(queryText);
	else
		nulls[Anum_adbss_query - 1] = true;

	if (omitSavePlan)
	{
		nulls[Anum_adbss_plan - 1] = true;
		nulls[Anum_adbss_explain_format] = true;
		nulls[Anum_adbss_explain_plan - 1] = true;
		nulls[Anum_adbss_bound_params - 1] = true;
	}
	else
	{
		if (planString)
			values[Anum_adbss_plan - 1] = CStringGetTextDatum(planString);
		else
			nulls[Anum_adbss_plan - 1] = true;

		values[Anum_adbss_explain_format - 1] = Int32GetDatum(adbssExplainFormat);

		explainPlanString = explainQueryDesc(queryDesc, adbssExplainFormat);
		if (explainPlanString)
			values[Anum_adbss_explain_plan - 1] = CStringGetTextDatum(explainPlanString);
		else
			nulls[Anum_adbss_explain_plan - 1] = true;

		if (queryDesc->params && queryDesc->params->numParams > 0)
			values[Anum_adbss_bound_params - 1] = ParamList2TextArr(queryDesc->params);
		else
			nulls[Anum_adbss_bound_params - 1] = true;
	}

	tuple = heap_form_tuple(RelationGetDescr(adbssRel), values, nulls);

	CatalogTupleInsertWithInfo(adbssRel, tuple, indstate);
}

static void updateAdbssPlan(Relation adbssRel,
							CatalogIndexState indstate,
							HeapTuple oldTuple,
							QueryDesc *queryDesc,
							bool omitSavePlan)
{
	HeapTuple newTuple;
	double totalTime;
	double tempTime;
	uint64 calls;
	uint64 rows;
	char *explainPlanString;
	Datum values[Natts_adbss] = {0};
	bool nulls[Natts_adbss] = {0};
	bool repls[Natts_adbss] = {0};
	Datum value;
	bool isnull;
	bool updateSavedPlan = false;

	Assert(oldTuple);
	Assert(queryDesc);

	value = heap_getattr(oldTuple, Anum_adbss_calls, RelationGetDescr(adbssRel), &isnull);
	if (isnull)
		calls = 0;
	else
		calls = DatumGetInt64(value);
	calls += 1;
	values[Anum_adbss_calls - 1] = Int64GetDatum(calls);
	repls[Anum_adbss_calls - 1] = true;

	value = heap_getattr(oldTuple, Anum_adbss_rows, RelationGetDescr(adbssRel), &isnull);
	if (isnull)
		rows = 0;
	else
		rows = DatumGetInt64(value);
	rows += queryDesc->estate->es_processed;
	values[Anum_adbss_rows - 1] = Int64GetDatum(rows);
	repls[Anum_adbss_rows - 1] = true;

	if (queryDesc->totaltime)
	{
		totalTime = queryDesc->totaltime->total * 1000.0; /* convert to msec */
		value = heap_getattr(oldTuple, Anum_adbss_total_time, RelationGetDescr(adbssRel), &isnull);
		if (isnull)
			tempTime = 0;
		else
			tempTime = DatumGetFloat8(value);
		values[Anum_adbss_total_time - 1] = Float8GetDatum(tempTime + totalTime);
		repls[Anum_adbss_total_time - 1] = true;

		value = heap_getattr(oldTuple, Anum_adbss_min_time, RelationGetDescr(adbssRel), &isnull);
		if (isnull)
			tempTime = 0;
		else
			tempTime = DatumGetFloat8(value);
		if (tempTime > totalTime)
		{
			values[Anum_adbss_min_time - 1] = Float8GetDatum(totalTime);
			repls[Anum_adbss_min_time - 1] = true;
		}

		value = heap_getattr(oldTuple, Anum_adbss_max_time, RelationGetDescr(adbssRel), &isnull);
		if (isnull)
			tempTime = 0;
		else
			tempTime = DatumGetFloat8(value);
		if (tempTime < totalTime)
		{
			values[Anum_adbss_max_time - 1] = Float8GetDatum(totalTime);
			repls[Anum_adbss_max_time - 1] = true;
			updateSavedPlan = true;
		}

		value = heap_getattr(oldTuple, Anum_adbss_mean_time, RelationGetDescr(adbssRel), &isnull);
		if (isnull)
			tempTime = 0;
		else
			tempTime = DatumGetFloat8(value);
		tempTime += ((totalTime - tempTime) / calls);
		values[Anum_adbss_mean_time - 1] = Float8GetDatum(tempTime);
		repls[Anum_adbss_mean_time - 1] = true;
	}

	values[Anum_adbss_last_execution - 1] = TimestampTzGetDatum(GetCurrentTimestamp());
	repls[Anum_adbss_last_execution - 1] = true;

	if (updateSavedPlan && !omitSavePlan)
	{
		values[Anum_adbss_explain_format - 1] = Int32GetDatum(adbssExplainFormat);
		repls[Anum_adbss_explain_format - 1] = true;

		explainPlanString = explainQueryDesc(queryDesc, adbssExplainFormat);
		if (explainPlanString)
		{
			values[Anum_adbss_explain_plan - 1] = CStringGetTextDatum(explainPlanString);
			repls[Anum_adbss_explain_plan - 1] = true;
		}

		if (queryDesc->params && queryDesc->params->numParams > 0)
		{
			values[Anum_adbss_bound_params - 1] = ParamList2TextArr(queryDesc->params);
			repls[Anum_adbss_bound_params - 1] = true;
		}
	}

	newTuple = heap_modify_tuple(oldTuple, RelationGetDescr(adbssRel), values, nulls, repls);
	CatalogTupleUpdateWithInfo(adbssRel, &oldTuple->t_self, newTuple, indstate);
}

static bool findAdbssPlan(Relation adbssRel,
						  Relation adbssQueryidIndex,
						  Snapshot snapshot,
						  char *planString,
						  AdbssKey *adbssKey,
						  HeapTuple *foundTuple,
						  bool omitSavePlan)
{
	HeapTuple tuple;
	TupleDesc tupleDesc;
	ScanKeyData scanKey;
	char *storedPlan;
	uint64 storedPlanid;
	IndexScanDesc queryidIndexScanDesc;
	bool found;
	bool isnull;
	Datum value;

	tupleDesc = RelationGetDescr(adbssRel);
	ScanKeyInit(&scanKey, 1, BTEqualStrategyNumber, F_INT8EQ, UInt64GetDatum(adbssKey->queryid));
	queryidIndexScanDesc = index_beginscan(adbssRel, adbssQueryidIndex, snapshot, 1, 0);
	index_rescan(queryidIndexScanDesc, &scanKey, 1, NULL, 0);

	found = false;
	while ((tuple = index_getnext(queryidIndexScanDesc, ForwardScanDirection)) != NULL)
	{
		value = heap_getattr(tuple, Anum_adbss_userid, tupleDesc, &isnull);
		if (isnull || DatumGetObjectId(value) != adbssKey->userid)
			continue;
		value = heap_getattr(tuple, Anum_adbss_dbid, tupleDesc, &isnull);
		if (isnull || DatumGetObjectId(value) != adbssKey->dbid)
			continue;
		value = heap_getattr(tuple, Anum_adbss_planid, tupleDesc, &isnull);
		if (isnull)
			continue;
		storedPlanid = DatumGetUInt64(value);
		if (storedPlanid == adbssKey->planid)
		{
			if (omitSavePlan)
			{
				// identical plan
				found = true;
				*foundTuple = heap_copytuple(tuple);
				break;
			}
			else
			{
				value = heap_getattr(tuple, Anum_adbss_plan, tupleDesc, &isnull);
				if (isnull)
					continue;
				storedPlan = TextDatumGetCString(DatumGetTextPP(value));
				if (pg_strcasecmp(storedPlan, planString) == 0)
				{
					// identical plan
					found = true;
					*foundTuple = heap_copytuple(tuple);
					break;
				}
			}
		}
	}
	index_endscan(queryidIndexScanDesc);
	return found;
}

static void adbssStoreToDsa(QueryDesc *queryDesc)
{
	AdbssKey key;
	AdbssEntry *entry;
	uint64 queryId;
	PlannedStmt *plannedstmt;
	char *planString;
	size_t planLength;
	bool omitSavePlan;
	uint64 planid;
	double totalTime;
	double tempTime;

	/* Safety check... */
	if (!adbssState || !adbssHtab)
		return;
	/* Be consistent with pg_stat_statements */
	queryId = queryDesc->plannedstmt->queryId;
	plannedstmt = queryDesc->plannedstmt;
	planString = nodeToString(plannedstmt);
	planLength = strlen(planString);
	omitSavePlan = isPlanTooLarge(queryId, planLength);
	planid = adbss_hash_string((const char *)planString, planLength);

	/* Set up key for hashtable search */
	key.userid = GetUserId();
	key.dbid = MyDatabaseId;
	key.queryid = queryId;
	key.planid = planid;

	/* Lookup the hash table entry with shared lock. */
	LWLockAcquire(&adbssState->lock, LW_SHARED);

	entry = (AdbssEntry *)hash_search(adbssHtab, &key, HASH_FIND, NULL);

	/* Create new entry, if not present */
	if (!entry)
	{
		/* Need exclusive lock to make a new hashtable entry - promote */
		LWLockRelease(&adbssState->lock);
		LWLockAcquire(&adbssState->lock, LW_EXCLUSIVE);

		/* OK to create a new hashtable entry. */
		entry = entry_alloc(&key);
		/* 
		 * Be careful the entry may be created by other backend.
		 * Test the value of dsa_pointer to see whether the entry 
		 * was created by other backend.
		 */
		if (entry && entry->tupleInDsa == InvalidDsaPointer)
		{
			TupleDesc textTupleDesc;
			MinimalTuple textTuple;
			dsa_pointer dp;

			textTupleDesc = form_adbss_text_tuple_desc();
			textTuple = form_adbss_text_tuple(textTupleDesc,
											  queryDesc,
											  planString,
											  omitSavePlan);
			/* try to allocate dsa */
			dp = dsa_allocate_extended(adbssDsaArea,
									   textTuple->t_len,
									   DSA_ALLOC_NO_OOM);
			if (dp == InvalidDsaPointer)
			{
				ereport(WARNING,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("out of memory"),
						 errdetail("Failed on DSA request")));
				hash_search(adbssHtab, &key, HASH_REMOVE, NULL);
				entry = NULL;
			}
			else
			{
				saveTupleToDsa(entry, textTuple, dp);
			}
			FreeTupleDesc(textTupleDesc);
			heap_free_minimal_tuple(textTuple);
		}
	}

	if (entry)
	{
		bool updateSavedPlan = false;
		/*
		 * Grab the spinlock while updating the counters (see comment about
		 * locking rules at the head of the file)
		 */
		volatile AdbssEntry *e = (volatile AdbssEntry *)entry;

		SpinLockAcquire(&e->mutex);

		e->counters.calls += 1;
		e->counters.rows += queryDesc->estate->es_processed;
		if (queryDesc->totaltime)
		{
			totalTime = queryDesc->totaltime->total * 1000.0; /* convert to msec */
			e->counters.total_time += totalTime;
			if (e->counters.calls == 1)
			{
				e->counters.min_time = totalTime;
				e->counters.max_time = totalTime;
				e->counters.mean_time = totalTime;
			}
			else
			{
				/* calculate min and max time */
				if (e->counters.min_time > totalTime)
					e->counters.min_time = totalTime;
				if (e->counters.max_time < totalTime)
				{
					e->counters.max_time = totalTime;
					updateSavedPlan = true;
				}
				tempTime = e->counters.mean_time;
				e->counters.mean_time +=
					(totalTime - tempTime) / e->counters.calls;
			}
		}
		else
		{
			if (e->counters.calls == 1)
			{
				e->counters.total_time = 0;
				e->counters.min_time = 0;
				e->counters.max_time = 0;
				e->counters.mean_time = 0;
			}
		}

		e->counters.last_execution = GetCurrentTimestamp();

		if (updateSavedPlan && !omitSavePlan)
		{
			TupleDesc textTupleDesc;
			MinimalTuple textTuple;
			dsa_pointer dp;

			textTupleDesc = form_adbss_text_tuple_desc();
			textTuple = form_adbss_text_tuple(textTupleDesc, queryDesc, planString, omitSavePlan);

			/* try to allocate dsa */
			dp = dsa_allocate_extended(adbssDsaArea,
									   textTuple->t_len,
									   DSA_ALLOC_NO_OOM);
			if (dp == InvalidDsaPointer)
			{
				/* out of memory */
			}
			else
			{
				freeTupleInDsa(entry);
				saveTupleToDsa(entry, textTuple, dp);
			}
			FreeTupleDesc(textTupleDesc);
			heap_free_minimal_tuple(textTuple);
		}

		SpinLockRelease(&e->mutex);
	}

	LWLockRelease(&adbssState->lock);
}

static TupleDesc form_adbss_text_tuple_desc(void)
{
	TupleDesc textTupleDesc;
	textTupleDesc = CreateTemplateTupleDesc(Natts_adbss - Anum_adbss_offset, false);

	TupleDescInitEntry(textTupleDesc,
					   (AttrNumber)Anum_adbss_minimal(Anum_adbss_query),
					   "query",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(textTupleDesc,
					   (AttrNumber)Anum_adbss_minimal(Anum_adbss_plan),
					   "plan",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(textTupleDesc,
					   (AttrNumber)Anum_adbss_minimal(Anum_adbss_explain_format),
					   "explain_format",
					   INT4OID, -1, 0);
	TupleDescInitEntry(textTupleDesc,
					   (AttrNumber)Anum_adbss_minimal(Anum_adbss_explain_plan),
					   "explain_plan",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(textTupleDesc,
					   (AttrNumber)Anum_adbss_minimal(Anum_adbss_bound_params),
					   "boundParams",
					   TEXTOID, -1, 1);
	return textTupleDesc;
}

static MinimalTuple form_adbss_text_tuple(TupleDesc textTupleDesc,
										  QueryDesc *queryDesc,
										  char *planString,
										  bool omitSavePlan)
{
	MinimalTuple textTuple;
	Datum values[Natts_adbss - Anum_adbss_offset] = {0};
	bool nulls[Natts_adbss - Anum_adbss_offset] = {0};
	char *queryText;
	char *explainPlanString;

	queryText = getQueryText(queryDesc);
	if (queryText)
		values[Anum_adbss_minimal(Anum_adbss_query) - 1] = CStringGetTextDatum(queryText);
	else
		nulls[Anum_adbss_minimal(Anum_adbss_query) - 1] = true;

	if (omitSavePlan)
	{
		nulls[Anum_adbss_minimal(Anum_adbss_plan) - 1] = true;
		nulls[Anum_adbss_minimal(Anum_adbss_explain_format) - 1] = true;
		nulls[Anum_adbss_minimal(Anum_adbss_explain_plan) - 1] = true;
		nulls[Anum_adbss_minimal(Anum_adbss_bound_params) - 1] = true;
	}
	else
	{
		if (planString)
			values[Anum_adbss_minimal(Anum_adbss_plan) - 1] = CStringGetTextDatum(planString);
		else
			nulls[Anum_adbss_minimal(Anum_adbss_plan) - 1] = true;

		values[Anum_adbss_minimal(Anum_adbss_explain_format) - 1] = Int32GetDatum(adbssExplainFormat);

		explainPlanString = explainQueryDesc(queryDesc, adbssExplainFormat);
		if (explainPlanString)
			values[Anum_adbss_minimal(Anum_adbss_explain_plan) - 1] = CStringGetTextDatum(explainPlanString);
		else
			nulls[Anum_adbss_minimal(Anum_adbss_explain_plan) - 1] = true;

		if (queryDesc->params && queryDesc->params->numParams > 0)
			values[Anum_adbss_minimal(Anum_adbss_bound_params) - 1] = ParamList2TextArr(queryDesc->params);
		else
			nulls[Anum_adbss_minimal(Anum_adbss_bound_params) - 1] = true;
	}

	textTuple = heap_form_minimal_tuple(textTupleDesc, values, nulls);

	return textTuple;
}

/**
 * Allocate a new hashtable entry.
 * caller must hold an exclusive lock on adbssState->lock
 */
static AdbssEntry *entry_alloc(AdbssKey *key)
{
	AdbssEntry *entry;
	bool found;

	Assert(key);

	/* Make space if needed */
	while (hash_get_num_entries(adbssHtab) >= adbssMaxRecord)
		entry_dealloc();

	/* Find or create an entry with desired hash code */
	entry = (AdbssEntry *)hash_search(adbssHtab, key, HASH_ENTER, &found);

	if (!found && entry != NULL)
	{
		/* New entry, initialize it */

		/* reset the statistics */
		memset(&entry->counters, 0, sizeof(AdbssCounters));
		/* set a invalid value can indicate this is a new entry */
		entry->tupleInDsa = InvalidDsaPointer;
		/* re-initialize the mutex each time ... we assume no one using it */
		SpinLockInit(&entry->mutex);
	}
	return entry;
}

/*
 * Deallocate least-used entries.
 * Caller must hold an exclusive lock on adbssState->lock.
 */
static void entry_dealloc(void)
{
	HASH_SEQ_STATUS hash_seq;
	AdbssEntry **entries;
	AdbssEntry *entry;
	int nvictims;
	int i;

	entries = palloc(hash_get_num_entries(adbssHtab) * sizeof(AdbssEntry *));

	i = 0;
	hash_seq_init(&hash_seq, adbssHtab);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
		entries[i++] = entry;

	/* Sort into increasing order by last_execution */
	qsort(entries, i, sizeof(AdbssEntry *), adbss_entry_cmp);

	/* Now zap an appropriate fraction of lowest-usage entries */
	nvictims = Max(10, i * 5 / 100);
	nvictims = Min(nvictims, i);

	for (i = 0; i < nvictims; i++)
	{
		entry = hash_search(adbssHtab, &entries[i]->key, HASH_REMOVE, NULL);
		freeTupleInDsa(entry);
	}

	pfree(entries);
}

static void freeTupleInDsa(AdbssEntry *entry)
{
	if (entry->tupleInDsa != InvalidDsaPointer)
	{
		dsa_free(adbssDsaArea, entry->tupleInDsa);
		entry->tupleInDsa = InvalidDsaPointer;
	}
}

/**
 * save the dsa_pointer for the next time to retrieve 
 */
static void saveTupleToDsa(AdbssEntry *entry,
						   MinimalTuple textTuple, dsa_pointer dp)
{
	char *dsa_address;

	entry->tupleInDsa = dp;
	/* copy text tuple to dsa */
	dsa_address = dsa_get_address(adbssDsaArea, dp);
	memcpy(dsa_address, textTuple, textTuple->t_len);
}

/*
 * qsort comparator for sorting into increasing last_execution order
 */
static int adbss_entry_cmp(const void *lhs, const void *rhs)
{
	TimestampTz l_execution = (*(AdbssEntry *const *)lhs)->counters.last_execution;
	TimestampTz r_execution = (*(AdbssEntry *const *)rhs)->counters.last_execution;

	if (l_execution < r_execution)
		return -1;
	else if (l_execution > r_execution)
		return +1;
	else
		return 0;
}

static bool isPlanTooLarge(uint64 queryId, size_t planLength)
{
	bool large = planLength > adbssMaxLength;

	if (large)
		ereport(LOG,
				(errmsg("The plan of SQL " INT64_FORMAT " is large to " UINT64_FORMAT ",  omit it",
						queryId,
						planLength)));
	return large;
}

static Datum ParamList2TextArr(const ParamListInfo from)
{
	char *str;
	Datum *datums;
	bool *nulls;
	ArrayType *arr;
	int i;
	int lb;
	Oid output;
	bool isvarlean;
	Assert(from->numParams > 0);

	datums = palloc((sizeof(datums[0]) * from->numParams) + (sizeof(nulls[0]) * from->numParams) /* for nulls */);
	nulls = (bool *)((char *)datums + (sizeof(datums[0]) * from->numParams));

	for (i = 0; i < from->numParams; ++i)
	{
		ParamExternData *prm = &from->params[i];
		nulls[i] = prm->isnull;
		if (prm->isnull == false)
		{
			getTypeOutputInfo(prm->ptype, &output, &isvarlean);
			str = OidOutputFunctionCall(output, prm->value);
			datums[i] = PointerGetDatum(cstring_to_text(str));
			pfree(str);
		}
	}

	lb = 1; /* array low bound */
	arr = construct_md_array(datums,
							 nulls,
							 1,				   /* 1D */
							 &from->numParams, /* array count */
							 &lb,
							 TEXTOID,
							 -1,	/* typlen, see pg_type.h */
							 false, /* typbyval, see pg_type.h */
							 'i');	/* typalign, see pg_type.h */
	for (i = 0; i < from->numParams; ++i)
	{
		if (from->params[i].isnull == false)
			pfree(DatumGetPointer(datums[i]));
	}
	pfree(datums);

	return PointerGetDatum(arr);
}

static bool is_alter_extension_cmd(Node *stmt)
{
	if (!stmt)
		return false;

	if (!IsA(stmt, AlterExtensionStmt))
		return false;

	if (pg_strcasecmp(((AlterExtensionStmt *)stmt)->extname, ADBSS_NAME) == 0)
		return true;

	return false;
}

static bool is_drop_extension_stmt(Node *stmt)
{
	char *objname;
	DropStmt *ds = (DropStmt *)stmt;

	if (!stmt)
		return false;

	if (!IsA(stmt, DropStmt))
		return false;

#if PG_VERSION_NUM < 100000
	objname = strVal(linitial(linitial(ds->objects)));
#else
	objname = strVal(linitial(ds->objects));
#endif

	if (ds->removeType == OBJECT_EXTENSION &&
		pg_strcasecmp(objname, ADBSS_NAME) == 0)
		return true;

	return false;
}

static bool is_create_extension_stmt(Node *stmt)
{
	CreateExtensionStmt *ds = (CreateExtensionStmt *)stmt;

	if (!stmt)
		return false;

	if (!IsA(stmt, CreateExtensionStmt))
		return false;

	if (pg_strcasecmp(ds->extname, ADBSS_NAME) == 0)
		return true;

	return false;
}

static bool useTableForStorage(void)
{
	return adbssOids.schemaOid != InvalidOid &&
		   adbssOids.tableOid != InvalidOid &&
		   adbssOids.queryidIndexOid != InvalidOid;
}

static void initializeAdbssOids(void)
{
	static bool relcache_callback_hooked = false;

	adbssOids.schemaOid = getAdbssExtensionSchemaOid();
	adbssOids.tableOid = getAdbssTableOid(adbssOids.schemaOid, ADBSS_NAME);
	adbssOids.queryidIndexOid = getAdbssIndexOid(adbssOids.schemaOid, ADBSS_QUERYID_INDEX_NAME);
	if (adbssOids.schemaOid != InvalidOid)
		ereport(LOG, (errmsg(ADBSS_NAME " extension initialized successfully")));
	if (!relcache_callback_hooked)
	{
		CacheRegisterRelcacheCallback(adbss_relcache_hook, PointerGetDatum(NULL));
		relcache_callback_hooked = true;
	}
}

static void invalidateAdbssOids(void)
{
	adbssOids.schemaOid = InvalidOid;
	adbssOids.tableOid = InvalidOid;
	adbssOids.queryidIndexOid = InvalidOid;
	ereport(LOG, (errmsg(ADBSS_NAME " extension invalidate CachedOids")));
}

static Oid getAdbssRelOid(Oid schemaOid, const char *relName)
{
	if (schemaOid == InvalidOid)
		schemaOid = getAdbssExtensionSchemaOid();

	if (schemaOid == InvalidOid)
		return InvalidOid;

	return get_relname_relid(relName, schemaOid);
}

static Oid getAdbssTableOid(Oid schemaOid, const char *relName)
{
	Oid relOid;

	relOid = getAdbssRelOid(schemaOid, relName);
	if (relOid == InvalidOid)
		return InvalidOid;
	if (get_rel_relkind(relOid) != RELKIND_RELATION)
		return InvalidOid;

	return relOid;
}

static Oid getAdbssIndexOid(Oid schemaOid, const char *relName)
{
	Oid relOid;

	relOid = getAdbssRelOid(schemaOid, relName);
	if (relOid == InvalidOid)
		return InvalidOid;
	if (get_rel_relkind(relOid) != RELKIND_INDEX)
		return InvalidOid;

	return relOid;
}

/*
 * Return sr_plan schema's Oid or InvalidOid if that's not possible.
 */
static Oid getAdbssExtensionSchemaOid()
{
	Oid result;
	Relation rel;
	SysScanDesc scandesc;
	HeapTuple tuple;
	ScanKeyData entry[1];
	Oid ext_schema;
	LOCKMODE heap_lock = AccessShareLock;

	/* It's impossible to fetch extension's schema now */
	if (!IsTransactionState())
		return InvalidOid;

	ext_schema = get_extension_oid(ADBSS_NAME, true);
	if (ext_schema == InvalidOid)
		return InvalidOid; /* exit if adb_stat_statements does not exist */

#if PG_VERSION_NUM >= 120000
	ScanKeyInit(&entry[0],
				Anum_pg_extension_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(ext_schema));
#else
	ScanKeyInit(&entry[0],
				ObjectIdAttributeNumber,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(ext_schema));
#endif

	rel = heap_open(ExtensionRelationId, heap_lock);
	scandesc = systable_beginscan(rel, ExtensionOidIndexId, true,
								  NULL, 1, entry);

	tuple = systable_getnext(scandesc);

	/* We assume that there can be at most one matching tuple */
	if (HeapTupleIsValid(tuple))
		result = ((Form_pg_extension)GETSTRUCT(tuple))->extnamespace;
	else
		result = InvalidOid;

	systable_endscan(scandesc);

	heap_close(rel, heap_lock);

	return result;
}

static char *getQueryText(QueryDesc *queryDesc)
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
		Assert(query_location <= strlen(query));
		query += query_location;
		/* Length of 0 (or -1) means "rest of string" */
		if (query_len <= 0)
			query_len = strlen(query);
		else
			Assert(query_len <= strlen(query));
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

static char *explainQueryDesc(QueryDesc *queryDesc,
							  ExplainFormat format)
{
	ExplainState *es = NewExplainState();

	es->analyze = (queryDesc->instrument_options && adbssExplainAnalyze);
	es->verbose = adbssExplainVerbose;
	es->buffers = (es->analyze && adbssExplainBuffers);
	es->timing = (es->analyze && adbssExplainTiming);
	es->summary = es->analyze;
	es->format = format;

	ExplainBeginOutput(es);
	ExplainPrintPlan(es, queryDesc);
	if (es->analyze && adbssExplainTriggers)
		ExplainPrintTriggers(es, queryDesc);
	if (es->costs)
		ExplainPrintJITSummary(es, queryDesc);
	ExplainEndOutput(es);

	/* Remove last line break */
	if (es->str->len > 0 && es->str->data[es->str->len - 1] == '\n')
		es->str->data[--es->str->len] = '\0';

	/* Fix JSON to output an object */
	if (format == EXPLAIN_FORMAT_JSON)
	{
		es->str->data[0] = '{';
		es->str->data[es->str->len - 1] = '}';
	}
	return es->str->data;
}

static uint64 adbss_hash_string(const char *str, int len)
{
	return DatumGetUInt64(hash_any_extended((const unsigned char *)str,
											len, 0));
}

/*
 * Retrieve statement statistics.
 */
Datum
	adb_stat_statements(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
	TupleDesc tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	Oid userid;
	bool is_allowed_role = false;

	HASH_SEQ_STATUS hash_seq;
	AdbssEntry *entry;

	userid = GetUserId();
	/* Superusers or members of pg_read_all_stats members are allowed */
	is_allowed_role = is_member_of_role(userid, DEFAULT_ROLE_READ_ALL_STATS);

	/* hash table must exist already */
	if (!adbssState || !adbssHtab)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg(ADBSS_NAME " must be loaded via shared_preload_libraries")));

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not "
						"allowed in this context")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	LWLockAcquire(&adbssState->lock, LW_SHARED);

	hash_seq_init(&hash_seq, adbssHtab);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		Datum values[Natts_adbss];
		bool nulls[Natts_adbss];
		AdbssCounters tmp;

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		/* copy counters to a local variable to keep locking time short */
		{
			volatile AdbssEntry *e = (volatile AdbssEntry *)entry;

			SpinLockAcquire(&e->mutex);
			tmp = e->counters;
			SpinLockRelease(&e->mutex);
		}

		if (tmp.calls == 0)
			continue;

		values[Anum_adbss_userid - 1] = ObjectIdGetDatum(entry->key.userid);
		values[Anum_adbss_dbid - 1] = ObjectIdGetDatum(entry->key.dbid);

		values[Anum_adbss_calls - 1] = Int64GetDatumFast(tmp.calls);
		values[Anum_adbss_rows - 1] = Int64GetDatumFast(tmp.rows);
		values[Anum_adbss_total_time - 1] = Float8GetDatumFast(tmp.total_time);
		values[Anum_adbss_min_time - 1] = Float8GetDatumFast(tmp.min_time);
		values[Anum_adbss_max_time - 1] = Float8GetDatumFast(tmp.max_time);
		values[Anum_adbss_mean_time - 1] = Float8GetDatumFast(tmp.mean_time);
		values[Anum_adbss_last_execution - 1] = TimestampTzGetDatum(tmp.last_execution);

		if (is_allowed_role || entry->key.userid == userid)
		{
			TupleDesc textTupleDesc;
			MinimalTuple minimalTuple;
			TupleTableSlot *slot;
			values[Anum_adbss_queryid - 1] = Int64GetDatumFast(entry->key.queryid);
			values[Anum_adbss_planid - 1] = Int64GetDatumFast(entry->key.planid);

			if (entry->tupleInDsa != InvalidDsaPointer)
			{
				textTupleDesc = form_adbss_text_tuple_desc();
				minimalTuple = (MinimalTuple)dsa_get_address(adbssDsaArea, entry->tupleInDsa);
				slot = MakeSingleTupleTableSlot(textTupleDesc);
				ExecStoreMinimalTuple(minimalTuple, slot, false);

				values[Anum_adbss_query - 1] = slot_getattr(slot,
															Anum_adbss_minimal(Anum_adbss_query),
															&nulls[Anum_adbss_query - 1]);

				values[Anum_adbss_plan - 1] = slot_getattr(slot,
														   Anum_adbss_minimal(Anum_adbss_plan),
														   &nulls[Anum_adbss_plan - 1]);

				values[Anum_adbss_explain_format - 1] = slot_getattr(slot,
																	 Anum_adbss_minimal(Anum_adbss_explain_format),
																	 &nulls[Anum_adbss_explain_format - 1]);

				values[Anum_adbss_explain_plan - 1] = slot_getattr(slot,
																   Anum_adbss_minimal(Anum_adbss_explain_plan),
																   &nulls[Anum_adbss_explain_plan - 1]);

				values[Anum_adbss_bound_params - 1] = slot_getattr(slot,
																   Anum_adbss_minimal(Anum_adbss_bound_params),
																   &nulls[Anum_adbss_bound_params - 1]);
				FreeTupleDesc(textTupleDesc);
			}
			else
			{
				nulls[Anum_adbss_query - 1] = true;
				nulls[Anum_adbss_plan - 1] = true;
				nulls[Anum_adbss_explain_format - 1] = true;
				nulls[Anum_adbss_explain_plan - 1] = true;
				nulls[Anum_adbss_bound_params - 1] = true;
			}
		}
		else
		{
			/*
			 * Don't show text, but hint as to the reason for not doing
			 * so if it was requested
			 */
			Datum hintMessage = CStringGetTextDatum("<insufficient privilege>");
			nulls[Anum_adbss_queryid - 1] = true;
			nulls[Anum_adbss_planid - 1] = true;
			values[Anum_adbss_query - 1] = hintMessage;
			values[Anum_adbss_plan - 1] = hintMessage;
			values[Anum_adbss_explain_format - 1] = Int32GetDatum(adbssExplainFormat);
			values[Anum_adbss_explain_plan - 1] = hintMessage;
			values[Anum_adbss_bound_params - 1] = hintMessage;
		}

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/* clean up and return the tuplestore */
	LWLockRelease(&adbssState->lock);

	tuplestore_donestoring(tupstore);

	return (Datum)0;
}

Datum explain_plan(PG_FUNCTION_ARGS)
{
	char *plan;
	char *format;
	ExplainState *es;
	PlannedStmt *pl_stmt = NULL;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	plan = text_to_cstring(PG_GETARG_TEXT_PP(0));
	pl_stmt = stringToNode(plan);

	es = NewExplainState();
	es->analyze = false;
	es->costs = true;
	es->verbose = false;
	es->buffers = false;
	es->timing = false;
	es->summary = false;
	if (PG_ARGISNULL(1))
		es->format = EXPLAIN_FORMAT_TEXT;
	else
	{
		format = PG_GETARG_CSTRING(1);
		if (strcmp(format, "text") == 0)
			es->format = EXPLAIN_FORMAT_TEXT;
		else if (strcmp(format, "xml") == 0)
			es->format = EXPLAIN_FORMAT_XML;
		else if (strcmp(format, "json") == 0)
			es->format = EXPLAIN_FORMAT_JSON;
		else if (strcmp(format, "yaml") == 0)
			es->format = EXPLAIN_FORMAT_YAML;
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unrecognized value for output format \"%s\"", format),
					 errhint("supported formats: 'text', 'xml', 'json', 'yaml'")));
	}
	if (pl_stmt == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not find saved plan")));

	ExplainBeginOutput(es);
#if PG_VERSION_NUM >= 100000
	ExplainOnePlan(pl_stmt, NULL, es, ADBSS_DUMMY_QUERY, NULL, NULL, NULL);
#else
	ExplainOnePlan(pl_stmt, NULL, es, ADBSS_DUMMY_QUERY, NULL, NULL);
#endif
	ExplainEndOutput(es);
	Assert(es->indent == 0);
	return CStringGetTextDatum(es->str->data);
}

/*
 * Reset all statement statistics.
 */
Datum
	adb_stat_statements_reset(PG_FUNCTION_ARGS)
{
	HASH_SEQ_STATUS hash_seq;
	AdbssEntry *entry;

	if (!adbssState || !adbssHtab)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg(ADBSS_NAME " must be loaded via shared_preload_libraries")));

	LWLockAcquire(&adbssState->lock, LW_EXCLUSIVE);

	hash_seq_init(&hash_seq, adbssHtab);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		hash_search(adbssHtab, &entry->key, HASH_REMOVE, NULL);
		freeTupleInDsa(entry);
	}

	LWLockRelease(&adbssState->lock);

	PG_RETURN_VOID();
}

#define getRelNameNspName(relid, relName, nspName) \
	relName = get_rel_name(relid);                 \
	if (relName)                                   \
	{                                              \
		Oid nspid = get_rel_namespace(relid);      \
		if (nspid != InvalidOid)                   \
		{                                          \
			nspName = get_namespace_name(nspid);   \
		}                                          \
	}

static char *extractTextAttrFromDsa(AdbssEntry *entry, int attnum)
{
	TupleDesc textTupleDesc;
	MinimalTuple minimalTuple;
	TupleTableSlot *slot;
	Datum value;
	bool isnull;

	if (entry == NULL || entry->tupleInDsa == InvalidDsaPointer)
		return NULL;

	textTupleDesc = form_adbss_text_tuple_desc();
	minimalTuple = (MinimalTuple)dsa_get_address(adbssDsaArea, entry->tupleInDsa);
	slot = MakeSingleTupleTableSlot(textTupleDesc);
	ExecStoreMinimalTuple(minimalTuple, slot, false);
	value = slot_getattr(slot,
						 Anum_adbss_minimal(attnum),
						 &isnull);

	FreeTupleDesc(textTupleDesc);

	if (!isnull)
		return TextDatumGetCString(value);

	return NULL;
}

Datum explain_rtable_of_plan(PG_FUNCTION_ARGS)
{
	AdbssKey key;
	AdbssEntry *entry;
	char *planString;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
	TupleDesc tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	PlannedStmt *plannedstmt;
	ListCell *rteCell;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not "
						"allowed in this context")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	key.userid = PG_GETARG_OID(0);
	key.dbid = PG_GETARG_OID(1);
	key.queryid = PG_GETARG_INT64(2);
	key.planid = PG_GETARG_INT64(3);

	/* Lookup the hash table entry with shared lock. */
	LWLockAcquire(&adbssState->lock, LW_SHARED);

	entry = (AdbssEntry *)hash_search(adbssHtab, &key, HASH_FIND, NULL);

	planString = extractTextAttrFromDsa(entry, Anum_adbss_plan);
	if (!planString)
		goto releaseLock;

	plannedstmt = stringToNode(planString);

	if (!IsA(plannedstmt, PlannedStmt))
		goto releaseLock;

	foreach (rteCell, plannedstmt->rtable)
	{
		Datum values[2];
		bool nulls[2];
		int i = 0;
		RangeTblEntry *rte = (RangeTblEntry *)lfirst(rteCell);
		char *nspName = NULL;
		char *relName = NULL;

		if (!IsA(rte, RangeTblEntry))
			continue;
		if (rte->rtekind != RTE_RELATION)
			continue;
		if (rte->relkind != RELKIND_RELATION &&
			rte->relkind != RELKIND_MATVIEW)
			continue;

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		getRelNameNspName(rte->relid, relName, nspName);

		if (relName != NULL && nspName != NULL)
		{
			values[i++] = CStringGetDatum(pstrdup(nspName));
			values[i++] = CStringGetDatum(pstrdup(relName));
		}
		else
		{
			values[i++] = CStringGetDatum(NOT_FOUND_STR);
			values[i++] = CStringGetDatum(NOT_FOUND_STR);
		}
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

releaseLock:
	LWLockRelease(&adbssState->lock);
	PG_RETURN_VOID();
}

Datum explain_plan_nodes_of_plan(PG_FUNCTION_ARGS)
{
	AdbssKey key;
	AdbssEntry *entry;
	char *planString;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
	TupleDesc tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	PlannedStmt *plannedstmt;
	List *relationPlans = NIL;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not "
						"allowed in this context")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	key.userid = PG_GETARG_OID(0);
	key.dbid = PG_GETARG_OID(1);
	key.queryid = PG_GETARG_INT64(2);
	key.planid = PG_GETARG_INT64(3);

	/* Lookup the hash table entry with shared lock. */
	LWLockAcquire(&adbssState->lock, LW_SHARED);

	entry = (AdbssEntry *)hash_search(adbssHtab, &key, HASH_FIND, NULL);

	planString = extractTextAttrFromDsa(entry, Anum_adbss_plan);
	if (!planString)
		goto releaseLock;

	plannedstmt = stringToNode(planString);

	if (!IsA(plannedstmt, PlannedStmt))
		goto releaseLock;

	relationPlanWalker(plannedstmt->planTree, plannedstmt, &relationPlans);
	if (relationPlans != NIL)
	{
		ListCell *relationPlanCell;

		foreach (relationPlanCell, relationPlans)
		{
			Datum values[4];
			bool nulls[4];
			int i = 0;
			RelationPlan *relationPlan = (RelationPlan *)lfirst(relationPlanCell);

			memset(values, 0, sizeof(values));
			memset(nulls, 0, sizeof(nulls));

			values[i++] = CStringGetDatum(relationPlan->schemaname);
			values[i++] = CStringGetDatum(relationPlan->relname);
			if (relationPlan->attname)
				values[i++] = CStringGetDatum(relationPlan->attname);
			else
				nulls[i++] = true;
			values[i++] = CStringGetTextDatum(relationPlan->planname);

			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		}
	}

releaseLock:
	LWLockRelease(&adbssState->lock);
	PG_RETURN_VOID();
}

Datum explain_rtable_of_query(PG_FUNCTION_ARGS)
{
	char *query_string;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
	TupleDesc tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	List *parsetree_list;
	ListCell *parsetree_item;
#if defined(ADB_MULTI_GRAM)
	ParseGrammar grammar = PARSE_GRAM_POSTGRES;
#endif

	if (PG_ARGISNULL(0))
		PG_RETURN_VOID();

	query_string = text_to_cstring(PG_GETARG_TEXT_PP(0));

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not "
						"allowed in this context")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

#ifdef ADB_MULTI_GRAM
	parsetree_list = parse_query_auto_gram(query_string, &grammar);
#else
	parsetree_list = pg_parse_query(query_string);
#endif

	/*
	 * Run through the raw parsetree(s) and process each one.
	 */
	foreach (parsetree_item, parsetree_list)
	{
		RawStmt *parsetree = lfirst_node(RawStmt, parsetree_item);
		List *querytree_list,
			*plantree_list;
		ListCell *cell;

		set_ps_display("explain query", false);

#ifdef ADB_MULTI_GRAM
		querytree_list = pg_analyze_and_rewrite_for_gram(parsetree,
														 query_string,
														 NULL, 0, NULL,
														 grammar);
		current_grammar = grammar;
#else
		querytree_list = pg_analyze_and_rewrite(parsetree, query_string,
												NULL, 0, NULL);
#endif

#ifdef ADB
		plantree_list = pg_plan_queries(querytree_list,
										CURSOR_OPT_PARALLEL_OK | CURSOR_OPT_CLUSTER_PLAN_SAFE,
										NULL);
#else
		plantree_list = pg_plan_queries(querytree_list,
										CURSOR_OPT_PARALLEL_OK, NULL);
#endif
		/* Explain every plan */
		foreach (cell, plantree_list)
		{
			ListCell *rteCell;
			PlannedStmt *plannedStmt = (PlannedStmt *)lfirst(cell);

			if (!IsA(plannedStmt, PlannedStmt))
				continue;
			foreach (rteCell, plannedStmt->rtable)
			{
				Datum values[2];
				bool nulls[2];
				int i = 0;
				RangeTblEntry *rte = (RangeTblEntry *)lfirst(rteCell);
				char *nspName = NULL;
				char *relName = NULL;

				if (!IsA(rte, RangeTblEntry))
					continue;
				if (rte->rtekind != RTE_RELATION)
					continue;
				if (rte->relkind != RELKIND_RELATION &&
					rte->relkind != RELKIND_MATVIEW)
					continue;

				memset(values, 0, sizeof(values));
				memset(nulls, 0, sizeof(nulls));

				getRelNameNspName(rte->relid, relName, nspName);
				if (relName != NULL && nspName != NULL)
				{
					values[i++] = CStringGetDatum(pstrdup(nspName));
					values[i++] = CStringGetDatum(pstrdup(relName));
				}
				else
				{
					values[i++] = CStringGetDatum(NOT_FOUND_STR);
					values[i++] = CStringGetDatum(NOT_FOUND_STR);
				}
				tuplestore_putvalues(tupstore, tupdesc, values, nulls);
			}
		}
	}

	PG_RETURN_VOID();
}

Datum explain_rtable_plan_of_query(PG_FUNCTION_ARGS)
{
	char *query_string;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
	TupleDesc tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	List *parsetree_list;
	ListCell *parsetree_item;
#if defined(ADB_MULTI_GRAM)
	ParseGrammar grammar = PARSE_GRAM_POSTGRES;
#endif

	if (PG_ARGISNULL(0))
		PG_RETURN_VOID();

	query_string = text_to_cstring(PG_GETARG_TEXT_PP(0));

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not "
						"allowed in this context")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

#ifdef ADB_MULTI_GRAM
	parsetree_list = parse_query_auto_gram(query_string, &grammar);
#else
	parsetree_list = pg_parse_query(query_string);
#endif

	/*
	 * Run through the raw parsetree(s) and process each one.
	 */
	foreach (parsetree_item, parsetree_list)
	{
		RawStmt *parsetree = lfirst_node(RawStmt, parsetree_item);
		List *querytree_list,
			*plantree_list;
		ListCell *cell;

		set_ps_display("explain query", false);

#ifdef ADB_MULTI_GRAM
		querytree_list = pg_analyze_and_rewrite_for_gram(parsetree,
														 query_string,
														 NULL, 0, NULL,
														 grammar);
		current_grammar = grammar;
#else
		querytree_list = pg_analyze_and_rewrite(parsetree, query_string,
												NULL, 0, NULL);
#endif

#ifdef ADB
		plantree_list = pg_plan_queries(querytree_list,
										CURSOR_OPT_PARALLEL_OK | CURSOR_OPT_CLUSTER_PLAN_SAFE,
										NULL);
#else
		plantree_list = pg_plan_queries(querytree_list,
										CURSOR_OPT_PARALLEL_OK, NULL);
#endif
		/* Explain every plan */
		foreach (cell, plantree_list)
		{
			List *relationPlans = NIL;
			PlannedStmt *plannedStmt = (PlannedStmt *)lfirst(cell);

			if (!IsA(plannedStmt, PlannedStmt))
				continue;

			relationPlanWalker(plannedStmt->planTree, plannedStmt, &relationPlans);
			if (relationPlans != NIL)
			{
				ListCell *relationPlanCell;

				foreach (relationPlanCell, relationPlans)
				{
					Datum values[4];
					bool nulls[4];
					int i = 0;
					RelationPlan *relationPlan = (RelationPlan *)lfirst(relationPlanCell);

					memset(values, 0, sizeof(values));
					memset(nulls, 0, sizeof(nulls));

					values[i++] = CStringGetDatum(relationPlan->schemaname);
					values[i++] = CStringGetDatum(relationPlan->relname);
					if (relationPlan->attname)
						values[i++] = CStringGetDatum(relationPlan->attname);
					else
						nulls[i++] = true;
					values[i++] = CStringGetTextDatum(relationPlan->planname);

					tuplestore_putvalues(tupstore, tupdesc, values, nulls);
				}
			}
		}
	}

	PG_RETURN_VOID();
}

static bool relationExpressionWalker(Node *node, List **relationPlans)
{
	if (node == NULL)
		return false;

	if (IsA(node, Var))
	{
		Var *var = (Var *)node;
		List *plans = *relationPlans;
		ListCell *cell;

		foreach (cell, plans)
		{
			RelationPlan *relationPlan = (RelationPlan *)lfirst(cell);
			if (relationPlan->scanrelid == var->varno)
			{
				TupleDesc tupleDesc;
				Relation relation;

				relationPlan->varattno = var->varattno;
				if (relationPlan->relname != NULL && relationPlan->schemaname != NULL)
				{
					relation = heap_open(relationPlan->relid, AccessShareLock);
					tupleDesc = RelationGetDescr(relation);
					if (var->varattno > tupleDesc->natts)
						relationPlan->attname = NULL;
					else
						relationPlan->attname =
							pstrdup(NameStr(TupleDescAttr(tupleDesc, var->varattno - 1)->attname));
					heap_close(relation, AccessShareLock);
				}
				else
				{
					relationPlan->attname = NULL;
				}
			}
		}
	}
	return expression_tree_walker((Node *)node, relationExpressionWalker, relationPlans);
}

static bool relationPlanWalker(Plan *plan, PlannedStmt *stmt, List **relationPlans)
{
	if (plan == NULL)
		return false;

	if (IsA(plan, SeqScan))
	{
		ListCell *rteCell;
		int i = 0;
		SeqScan *seqScan;

		seqScan = (SeqScan *)plan;
		foreach (rteCell, stmt->rtable)
		{
			i++;
			if (seqScan->scanrelid == i)
			{
				RangeTblEntry *rte = (RangeTblEntry *)lfirst(rteCell);
				RelationPlan *relationPlan;

				if (!IsA(rte, RangeTblEntry))
					break;
				if (rte->rtekind != RTE_RELATION)
					break;
				if (rte->relkind != RELKIND_RELATION &&
					rte->relkind != RELKIND_MATVIEW)
					break;

				/* initilize to zero */
				relationPlan = palloc0(sizeof(RelationPlan));
				relationPlan->relid = rte->relid;
				getRelNameNspName(rte->relid, relationPlan->relname, relationPlan->schemaname);
				relationPlan->planname = pstrdup("Seq Scan");
				relationPlan->scanrelid = seqScan->scanrelid;
				*relationPlans = lappend(*relationPlans, relationPlan);

				expression_tree_walker((Node *)plan->qual, relationExpressionWalker, relationPlans);
			}
		}
	}

	return plan_tree_walker(plan, (Node *)stmt, relationPlanWalker, relationPlans);
}