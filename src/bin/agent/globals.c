#include "agent.h"
#include "mb/pg_wchar.h"

#ifdef USE_ASSERT_CHECKING
bool assert_enabled = true;
#endif /* USE_ASSERT_CHECKING */

volatile bool InterruptPending = false;
volatile bool QueryCancelPending = false;
volatile bool ProcDiePending = false;
volatile uint32 QueryCancelHoldoffCount = 0;
volatile uint32 CritSectionCount = 0;

int
pg_mbcliplen(const char *mbstr, int len, int limit)
{
	int			l = 0;

	len = Min(len, limit);
	while (l < len && mbstr[l])
		l++;
	return l;
}
