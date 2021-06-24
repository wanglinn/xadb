#include "agent.h"
#include "mb/pg_wchar.h"

#ifdef USE_ASSERT_CHECKING
bool assert_enabled = true;
#endif /* USE_ASSERT_CHECKING */

volatile sig_atomic_t InterruptPending = false;
volatile sig_atomic_t QueryCancelPending = false;
volatile sig_atomic_t ProcDiePending = false;
volatile uint32 QueryCancelHoldoffCount = 0;
volatile uint32 CritSectionCount = 0;
volatile sig_atomic_t LogMemoryContextPending = false;

int
pg_mbcliplen(const char *mbstr, int len, int limit)
{
	int			l = 0;

	len = Min(len, limit);
	while (l < len && mbstr[l])
		l++;
	return l;
}
