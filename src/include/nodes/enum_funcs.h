#ifndef ENUM_FUNCS_H
#define ENUM_FUNCS_H

#pragma push_macro("BEGIN_ENUM")
#pragma push_macro("END_ENUM")
#pragma push_macro("ENUM_VALUE")

#define BEGIN_ENUM(enum_type)	\
	enum enum_type;				\
	extern const char *get_enum_string_##enum_type(enum enum_type value);
#include "nodes/enum_define.h"

#pragma pop_macro("BEGIN_ENUM")
#pragma pop_macro("END_ENUM")
#pragma pop_macro("ENUM_VALUE")

#endif /* ENUM_FUNCS_H */
