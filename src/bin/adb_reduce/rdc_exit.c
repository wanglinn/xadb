#include "rdc_globals.h"
#include "rdc_exit.h"

#include <stdlib.h>

bool		rdc_exit_inprogress = false;

/*
 * This flag tracks whether we've called atexit() in the current process
 * (or in the parent postmaster).
 */
static bool atexit_callback_setup = false;

/* local functions */
static void rdc_exit_prepare(int code);
static void atexit_callback(void);

#define MAX_ON_EXITS 20

struct ONEXIT
{
	rdc_on_exit_callback function;
	Datum		arg;
};

static struct ONEXIT on_rdc_exit_list[MAX_ON_EXITS];
static int	on_rdc_exit_index = 0;

static void
rdc_exit_prepare(int code)
{
	/*
	 * Once we set this flag, we are committed to exit.  Any ereport() will
	 * NOT send control back to the main loop, but right back here.
	 */
	rdc_exit_inprogress = true;

	/*
	 * Forget any pending cancel or die requests; we're doing our best to
	 * close up shop already.  Note that the signal handlers will not set
	 * these flags again, now that proc_exit_inprogress is set.
	 */
	InterruptPending = false;
	ProcDiePending = false;
	QueryCancelPending = false;
	InterruptHoldoffCount = 1;
	CritSectionCount = 0;

	/*
	 * Also clear the error context stack, to prevent error callbacks from
	 * being invoked by any elog/ereport calls made during proc_exit. Whatever
	 * context they might want to offer is probably not relevant, and in any
	 * case they are likely to fail outright after we've done things like
	 * aborting any open transaction.  (In normal exit scenarios the context
	 * stack should be empty anyway, but it might not be in the case of
	 * elog(FATAL) for example.)
	 */
	error_context_stack = NULL;

	/*
	 * call all the registered callbacks.
	 *
	 * Note that since we decrement on_proc_exit_index each time, if a
	 * callback calls ereport(ERROR) or ereport(FATAL) then it won't be
	 * invoked again when control comes back here (nor will the
	 * previously-completed callbacks).  So, an infinite loop should not be
	 * possible.
	 */
	while (--on_rdc_exit_index >= 0)
		(*on_rdc_exit_list[on_rdc_exit_index].function) (code,
														 on_rdc_exit_list[on_rdc_exit_index].arg);

	on_rdc_exit_index = 0;
}

void
rdc_exit(int code)
{
	rdc_exit_prepare(code);

	exit(code);
}

static void
atexit_callback(void)
{
	/* Clean up everything that must be cleaned up */
	/* ... too bad we don't know the real exit code ... */
	rdc_exit_prepare(-1);
}

void
on_rdc_exit(rdc_on_exit_callback function, Datum arg)
{
	if (on_rdc_exit_index >= MAX_ON_EXITS)
		ereport(FATAL,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg_internal("out of on_rdc_exit slots")));

	on_rdc_exit_list[on_rdc_exit_index].function = function;
	on_rdc_exit_list[on_rdc_exit_index].arg = arg;

	++on_rdc_exit_index;

	if (!atexit_callback_setup)
	{
		atexit(atexit_callback);
		atexit_callback_setup = true;
	}
}
