#ifndef AGTM_UTILS_H
#define AGTM_UTILS_H

#include "agtm/agtm_msg.h"

extern const char *gtm_util_message_name(AGTM_MessageType type);
extern const char *gtm_util_result_name(AGTM_ResultType type);
extern enum CommandTag gtm_util_message_tag(AGTM_MessageType type);
#endif
