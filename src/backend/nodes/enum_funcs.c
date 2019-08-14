
#include "postgres.h"

#include "nodes/execnodes.h"
#include "nodes/nodes.h"
#include "nodes/primnodes.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"
#include "nodes/replnodes.h"
#ifdef ADB
#include "pgxc/locator.h"
#include "optimizer/pgxcplan.h"
#endif /* ADB */
#include "optimizer/planmain.h"

#include "nodes/enum_funcs.h"

#define BEGIN_ENUM(enum_type)										\
	const char *get_enum_string_##enum_type(enum enum_type value)	\
	{																\
		switch(value)												\
		{
#define ENUM_VALUE(value)											\
		case value:												\
			return #value;
#define END_ENUM(enum_type)											\
		default:													\
			break;													\
		}															\
		return NULL;												\
	}

#include "nodes/enum_define.h"
#undef BEGIN_ENUM
#undef END_ENUM
#undef ENUM_VALUE
