#ifndef RDC_GLOBALS_H
#define RDC_GLOBALS_H

#include <unistd.h>

#include "c.h"
#include "utils/elog.h"
#include "utils/palloc.h"		/* for palloc/pfree */

#define ENABLE_LIST_COMPAT
#include "rdc_list.h"

#define IS_AF_INET(fam) ((fam) == AF_INET)

#if defined(safe_pfree)
#undef safe_pfree
#endif
#define safe_pfree(ptr)			\
	do {						\
		if ((ptr) != NULL)		\
			pfree((ptr));		\
		(ptr) = NULL;			\
	} while(0)

/* define our text domain for translations */
#if defined(TEXTDOMAIN)
#undef TEXTDOMAIN
#endif
#define TEXTDOMAIN			PG_TEXTDOMAIN("adb_reduce")
#define RDC_VERSION_NUM		PG_VERSION_NUM

typedef struct RdcPort  RdcPort;
typedef struct PlanPort PlanPort;

#ifdef DEBUG_ADB
typedef struct ReduceInfo
{
	int			rnid;			/* reduce node id start with 0 */
	char	   *host;			/* reduce server host */
	int			port;			/* reduce server port */
} ReduceInfo;
#endif
/* define adb reduce run options */
typedef struct ReduceOptionsData
{
	sigjmp_buf *rdc_work_stack;
	char	   *lhost;					/* can be null */
	int			lport;					/* 0 means random port */
	int			work_mem;				/* for tupstorestate, Unit: KB */
	int			log_min_messages;		/* for log output */

	RdcPort	   *parent_watch;			/* for interprocess communication with parent */
	RdcPort	   *log_watch;				/* for log record */

	int			rdc_num;
#ifdef DEBUG_ADB
	ReduceInfo *rdc_infos;
#endif
	RdcPort	  **rdc_nodes;				/* for reduce group */
	List	   *pln_nodes;				/* for plan node */
} ReduceOptionsData, *ReduceOptions;

extern ReduceOptions	MyReduceOpts;
extern int				MyReduceId;
extern pgsocket			MyListenSock;
extern pgsocket			MyParentSock;
extern pgsocket			MyLogSock;
extern int				MyListenPort;

#define HOLD_INTERRUPTS()  (InterruptHoldoffCount++)

#define RESUME_INTERRUPTS() \
do { \
	Assert(InterruptHoldoffCount > 0); \
	InterruptHoldoffCount--; \
} while(0)

#define CHECK_FOR_INTERRUPTS()		\
	do { 							\
		if (InterruptPending)		\
			rdc_ProcessInterrupts();	\
	} while(0)

#define HOLD_CANCEL_INTERRUPTS()  (QueryCancelHoldoffCount++)

#define RESUME_CANCEL_INTERRUPTS() \
do { \
	Assert(QueryCancelHoldoffCount > 0); \
	QueryCancelHoldoffCount--; \
} while(0)

extern PGDLLIMPORT volatile bool InterruptPending;
extern PGDLLIMPORT volatile bool QueryCancelPending;
extern PGDLLIMPORT volatile bool ProcDiePending;
extern PGDLLIMPORT volatile bool ClientConnectionLost;
extern PGDLLIMPORT volatile uint32 CritSectionCount;
extern PGDLLIMPORT volatile uint32 InterruptHoldoffCount;
extern PGDLLIMPORT volatile uint32 QueryCancelHoldoffCount;

extern void rdc_ProcessInterrupts(void);

/*
 * These declarations supports the assertion-related macros in c.h.
 * assert_enabled is here because that file doesn't have PGDLLIMPORT in the
 * right place, and ExceptionalCondition must be present, for the backend only,
 * even when assertions are not enabled.
 */
extern PGDLLIMPORT bool assert_enabled;

typedef uintptr_t Datum;
/*
 * DatumGetPointer
 *		Returns pointer value of a datum.
 */

#define DatumGetPointer(X) ((Pointer) (X))

/*
 * PointerGetDatum
 *		Returns datum representation for a pointer.
 */

#define PointerGetDatum(X) ((Datum) (X))

extern void ExceptionalCondition(const char *conditionName,
								 const char *errorType,
								 const char *fileName, int lineNumber) __attribute__((noreturn));

#endif	/* RDC_GLOBALS_H */
