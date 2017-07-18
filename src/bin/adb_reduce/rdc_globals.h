#ifndef RDC_GLOBALS_H
#define RDC_GLOBALS_H

#include <unistd.h>

#include "c.h"
#include "utils/elog.h"
#include "utils/palloc.h"		/* for palloc/pfree */

#define ENABLE_LIST_COMPAT
#include "rdc_list.h"

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
#define TEXTDOMAIN		PG_TEXTDOMAIN("adb_reduce")

typedef struct RdcPort  RdcPort;
typedef struct PlanPort PlanPort;
typedef struct RdcMask  RdcMask;
typedef struct RdcNode  RdcNode;

/* define adb reduce run options */
typedef struct ReduceOptionsData
{
	sigjmp_buf *rdc_work_stack;
	char	   *lhost;					/* can be null */
	int			lport;					/* 0 means random port */
	int			work_mem;				/* for tupstorestate, Unit: KB */
	int			log_min_messages;		/* for log output */
	int			Log_error_verbosity;
	int			Log_destination;
	bool		redirection_done;

	RdcPort	   *boss_watch;				/* for interprocess communication with boss */
	RdcPort	   *log_watch;				/* for log record */

	int			rdc_num;
	RdcNode	   *rdc_nodes;				/* for reduce group */
	List	   *pln_nodes;				/* for plan node */
} ReduceOptionsData, *RdcOptions;

#define pg_time_t time_t

extern RdcOptions	MyRdcOpts;
extern pgsocket		MyListenSock;
extern pgsocket		MyLogSock;
extern int			MyListenPort;
extern int			MyProcPid;
extern int			MyBossPid;
extern pg_time_t	MyStartTime;

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

extern PGDLLEXPORT volatile uint32 ClientConnectionLostType;
extern PGDLLEXPORT volatile int64 ClientConnectionLostID;

extern void rdc_ProcessInterrupts(void);

extern void SetRdcPsStatus(const char * format, ...) pg_attribute_printf(1, 2);

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
