#include "get_uptime.h"
#include <time.h>
#include <sys/sysctl.h>

time_t get_uptime(void)
{
	struct timeval ts;
	size_t len = sizeof(ts);
	int mib[2] = {CTL_KERN, KERN_BOOTTIME};
	if (sysctl(mib, 2, &ts, &len, NULL, 0) == 0)
		return time(NULL)-ts.tv_sec;
	return 0;
}
