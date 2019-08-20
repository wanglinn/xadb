#include "postgres.h"

#include "access/xlogdefs.h"
#include "catalog/pg_type.h"
#include "commands/event_trigger.h"
#include "lib/stringinfo.h"
#include "nodes/bitmapset.h"
#include "nodes/enum_funcs.h"
#include "nodes/extensible.h"
#include "nodes/nodes.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "nodes/relation.h"
#include "nodes/replnodes.h"
#include "nodes/value.h"
#include "parser/mgr_node.h"
#include "utils/lsyscache.h"

#ifdef ADB
#include "optimizer/pgxcplan.h"
#include "optimizer/planmain.h"
#include "optimizer/reduceinfo.h"
#endif

#ifdef ADBMGRD
#include "parser/mgr_node.h"
#endif /* ADBMGRD */

#define TABLE_STOP 2

#define MARK_INFO(m)	{#m,m}
typedef struct MarkInfo
{
	const char *mark;
	size_t		val;
}MarkInfo;

typedef void(*outputfunc)(const void *node, StringInfo str, int space);
typedef void(*outputscalar)(StringInfo str, const void *p);

static void printNode(const void *obj, StringInfo str, int space);



static void outputNodeBegin(StringInfo str, const char *type, int space)
{
	if(str->len && str->data[str->len-1] != '\n')
		appendStringInfoChar(str, '\n');
	appendStringInfoSpaces(str, space);
	appendStringInfo(str, "{%s\n", type);
}

static void outputNodeEnd(StringInfo str, const char *type, int space)
{
	appendStringInfoSpaces(str, space);
	appendStringInfoString(str, "}\n");
}

static void outputNode(const void *node, outputfunc func, const char *type, StringInfo str, int space)
{
	if(node)
	{
		if (strcmp(type, "Expr") == 0)
			return;
		outputNodeBegin(str, type, space);
		(*func)(node, str, space+TABLE_STOP);
		outputNodeEnd(str, type, space);
	}else
	{
		appendStringInfoString(str, "<>\n");
	}
}

static void outputNodeArray(void ** const node, size_t count, outputfunc func, const char *type, StringInfo str, int space)
{
	size_t i;
	int space2 = space+TABLE_STOP;
	appendStringInfoString(str, "{\n");
	for(i=0;i<count;++i)
		outputNode(node[i], func, type, str, space2);
	appendStringInfoSpaces(str, space);
	appendStringInfoString(str, "}\n");
}

static void outputBitmapset(const Bitmapset *node, StringInfo str, int space)
{
	Bitmapset *a = bms_copy(node);
	const char *tmp = "{";
	int n;
	while((n=bms_first_member(a)) >= 0)
	{
		appendStringInfoString(str, tmp);
		appendStringInfo(str, "%d", n);
		tmp = ",";
	}
	bms_free(a);
	if(tmp[0] == '{')
		appendStringInfoChar(str, '{');
	appendStringInfoString(str, "}\n");
}

static void appendObjectMebName(StringInfo str, const char *name, int space)
{
	appendStringInfoSpaces(str, space);
	appendStringInfoChar(str, ':');
	appendStringInfoString(str, name);
	appendStringInfoChar(str, ' ');
}

#define appendObjectMebEnd(str_, name_, space_)	appendStringInfoChar(str, '\n')

static void outputString(StringInfo str, const char *name, const char *value, int space)
{
	appendObjectMebName(str, name, space);
	if(value)
		appendStringInfo(str, "\"%s\"", value);
	else
		appendStringInfoString(str, "<>");
	appendObjectMebEnd(str, name, space);
}

static void outputValue(const Value *value, StringInfo str, int space)
{
	if(value == NULL)
	{
		appendStringInfoString(str, "<>\n");
		return;
	}
	appendStringInfoChar(str, '{');
	switch(nodeTag(value))
	{
	case T_Value:
		appendStringInfoString(str, "Value");
		break;
	case T_Integer:
		appendStringInfo(str, "Integer %d", intVal(value));
		break;
	case T_Float:
		appendStringInfo(str, "Float %s", strVal(value));
		break;
	case T_String:
		appendStringInfo(str, "String \"%s\"", strVal(value));
		break;
	case T_BitString:
		appendStringInfo(str, "BitString \"%s\"", strVal(value));
		break;
	case T_Null:
		appendStringInfoString(str, "Null");
		break;
	default:
		ereport(ERROR, (errmsg("unknown Value type %d", nodeTag(value))));
		break;
	}
	appendStringInfoString(str, "}\n");
}

static void outputListObject(const List *list, StringInfo str, int space, outputfunc func)
{
	const ListCell *lc;
	int space2;
	Assert(func);
	if(list == NULL)
	{
		appendStringInfoString(str, "<>\n");
		return;
	}
	Assert(IsA(list, List));

	space2=space+TABLE_STOP;
	outputNodeBegin(str, "List", space);
	foreach(lc, list)
		(*func)(lfirst(lc), str, space2);
	outputNodeEnd(str, "List", space);
}

static void outputList(const List *list, StringInfo str, int space)
{
	const ListCell *lc;
	if(list == NULL)
	{
		appendStringInfoString(str, "<>\n");
		return;
	}

	if(IsA(list, List))
	{
		outputListObject(list, str, space, printNode);
	}else if(IsA(list, OidList))
	{
		char tmp = '{';
		foreach(lc, list)
		{
			appendStringInfoChar(str, tmp);
			appendStringInfo(str, "%u", lfirst_oid(lc));
			tmp = ',';
		}
		if(tmp == '{')
			appendStringInfoChar(str, tmp);
		appendStringInfoString(str, "}\n");
	}else if(IsA(list, IntList))
	{
		char tmp = '{';
		foreach(lc, list)
		{
			appendStringInfoChar(str, tmp);
			appendStringInfo(str, "%d", lfirst_int(lc));
			tmp = ',';
		}
		if(tmp == '{')
			appendStringInfoChar(str, tmp);
		appendStringInfoString(str, "}\n");
	}else
	{
		ereport(ERROR, (errmsg("Unknown list type %d", nodeTag(list))));
	}
}

static void outputDatum(StringInfo str, Datum datum, Oid type, bool isnull)
{
	if(isnull)
	{
		appendStringInfoString(str, "NULL");
	}else
	{
		char *val;
		Oid			typoutput;
		bool		typIsVarlena;
		getTypeOutputInfo(type, &typoutput, &typIsVarlena);
		val = OidOutputFunctionCall(typoutput, datum);
		appendStringInfoString(str, val);
		pfree(val);
	}
	appendStringInfoChar(str, '\n');
}

static void outputAclMode(StringInfo str, const AclMode *value)
{
	static const MarkInfo acl[]=
	{
		MARK_INFO(ACL_INSERT),
		MARK_INFO(ACL_SELECT),
		MARK_INFO(ACL_UPDATE),
		MARK_INFO(ACL_DELETE),
		MARK_INFO(ACL_TRUNCATE),
		MARK_INFO(ACL_REFERENCES),
		MARK_INFO(ACL_TRIGGER),
		MARK_INFO(ACL_EXECUTE),
		MARK_INFO(ACL_USAGE),
		MARK_INFO(ACL_CREATE),
		MARK_INFO(ACL_CREATE_TEMP),
		MARK_INFO(ACL_CONNECT)
	};
	size_t i;
	AclMode v;
	char tmp;
	Assert(value);

	v = *value;
	if(v == 0)
	{
		appendStringInfoString(str, "0\n");
		return;
	}

	tmp = ' ';
	for(i=0;i<lengthof(acl);++i)
	{
		if(v & acl[i].val)
		{
			appendStringInfoChar(str, tmp);
			appendStringInfoString(str, acl[i].mark);
			tmp='|';
		}
	}
}

#ifdef ADB
static void outputReduceType(StringInfo str, const char *type)
{
	const char *name;
#define CASE_REDUCE_TYPE(t)		\
	case REDUCE_TYPE_##t:		\
		name = #t;				\
		break

	switch(*type)
	{
	CASE_REDUCE_TYPE(HASHMAP);
	CASE_REDUCE_TYPE(HASH);
	CASE_REDUCE_TYPE(LIST);
	CASE_REDUCE_TYPE(RANGE);
	CASE_REDUCE_TYPE(MODULO);
	CASE_REDUCE_TYPE(REPLICATED);
	CASE_REDUCE_TYPE(RANDOM);
	CASE_REDUCE_TYPE(COORDINATOR);
	default:
		name = "UNKNOWN";
		break;
	}

	appendStringInfoString(str, name);
#undef CASE_REDUCE_TYPE
}
#endif /* ADB */

static void outputbool(StringInfo str, const bool *value)
{
	appendStringInfoString(str, *value ? "true":"false");
}

static void outputchar(StringInfo str, const char *value)
{
	Assert(value && str);
	if(isprint(*value))
		appendStringInfo(str, "'%c'", *value);
	else
		appendStringInfo(str, "%d", *value);
}

static void output_scalar_array(StringInfo str, const void *p
	, outputscalar func, size_t size, size_t count)
{
	size_t i;
	if(p == NULL)
	{
		appendStringInfoString(str, "<>\n");
		return;
	}

	appendStringInfoChar(str, '[');
	for(i=0;i<count;++i)
	{
		(*func)(str, p);
		p = ((char*)p) + size;
		appendStringInfoChar(str, ' ');
	}
	appendStringInfoString(str, "]\n");
}

#define SIMPLE_OUTPUT_DECLARE(type, fmt)					\
static void output##type(StringInfo str, const type *value)	\
{															\
	appendStringInfo(str, fmt, *value);						\
}
SIMPLE_OUTPUT_DECLARE(int, "%d")
SIMPLE_OUTPUT_DECLARE(Cost, "%g")
SIMPLE_OUTPUT_DECLARE(double, "%g")
SIMPLE_OUTPUT_DECLARE(long, "%ld")
SIMPLE_OUTPUT_DECLARE(Index, "%u")
SIMPLE_OUTPUT_DECLARE(Oid, "%u")
SIMPLE_OUTPUT_DECLARE(AttrNumber, "%d")
SIMPLE_OUTPUT_DECLARE(int32, "%d")
SIMPLE_OUTPUT_DECLARE(int16, "%d")
SIMPLE_OUTPUT_DECLARE(uint16, "%u")
SIMPLE_OUTPUT_DECLARE(uint32, "%u")
#ifdef HAVE_LONG_INT_64
SIMPLE_OUTPUT_DECLARE(uint64, "%lu")
#elif defined(HAVE_LONG_LONG_INT_64)
SIMPLE_OUTPUT_DECLARE(uint64, "%llu")
#endif
SIMPLE_OUTPUT_DECLARE(BlockNumber, "%u")
SIMPLE_OUTPUT_DECLARE(Selectivity, "%g")
SIMPLE_OUTPUT_DECLARE(bits32, "%08x")
SIMPLE_OUTPUT_DECLARE(StrategyNumber, "%u");

/* declare functions */
#define BEGIN_NODE(type)	\
	static void output##type(const type *node, StringInfo str, int space);
#define BEGIN_STRUCT(type)	BEGIN_NODE(type)
#define NODE_SAME(t1,t2)
#include "nodes/nodes_define.h"
#include "nodes/nodes_undef.h"

/* functions */
#define BEGIN_NODE(type) 								\
	static void output##type(const type *node, StringInfo str, int space)	\
	{																		\
		Assert(node && str);												\

#define END_NODE(type)						\
	}

#undef BEGIN_STRUCT
#define BEGIN_STRUCT(type)					\
	static void output##type(const type *node, StringInfo str, int space)	\
	{																		\
		if (node)															\
		{																	\
			outputNodeBegin(str, #type, space);								\
			space += TABLE_STOP;

#define END_STRUCT(type)													\
			outputNodeEnd(str, #type, space);								\
			space -= TABLE_STOP;											\
		}else																\
		{																	\
			appendStringInfoString(str, "<>\n");							\
		}																	\
	}

#define NODE_SAME(t1,t2)
#define NODE_BASE2(type, meb)	\
	outputNode(&(node->meb), (outputfunc)output##type, #type, str, space);

#define NODE_NODE(t,m)									\
		appendObjectMebName(str, #m, space);			\
		printNode(node->m, str, space+TABLE_STOP);

#define NODE_NODE_MEB(t,m)								\
		appendObjectMebName(str, #m, space);			\
		printNode(&(node->m), str, space+TABLE_STOP);

#define NODE_NODE_ARRAY(t,m,l)							\
		outputNodeArray((void **const )node->m, l, (outputfunc)printNode, #t, str, space);

#define NODE_BITMAPSET(t,m)								\
		appendObjectMebName(str, #m, space);			\
		outputBitmapset(node->m, str, space+TABLE_STOP);

#define NODE_BITMAPSET_ARRAY(t,m,l)						\
		do												\
		{												\
			size_t count = (l);							\
			size_t i;									\
			appendObjectMebName(str, #m, space);		\
			appendStringInfoChar(str, '{');				\
			for(i=0;i<count;++i)						\
				outputBitmapset(node->m[i], str, space);\
			appendStringInfoString(str, "}\n");			\
		}while(0);

#define NODE_STRUCT(t,m)								\
		appendObjectMebName(str, #m, space);			\
		outputNode(node->m, (outputfunc)output##t, #t, str, space);

#define NODE_STRUCT_ARRAY(t,m,l)							\
		outputNodeArray((void **const )node->m, l, (outputfunc)output##t, #t, str, space);

#define NODE_STRUCT_LIST(t,m)							\
		appendObjectMebName(str, #m, space);			\
		outputListObject(node->m, str, space, (outputfunc)output##t);

#define NODE_STRUCT_MEB(t,m)								\
		appendObjectMebName(str, #m, space);				\
		outputNode(&(node->m), (outputfunc)output##t, #t, str, space);

#define NODE_STRING(m) outputString(str, #m, node->m, space);

#define NODE_SCALAR(t,m)						\
		appendObjectMebName(str, #m, space);	\
		output##t(str, &(node->m));				\
		appendObjectMebEnd(str, #m, space);

#define NODE_SCALAR_POINT(t,m,l)				\
		appendObjectMebName(str, #m, space);	\
		output_scalar_array(str, node->m, (outputscalar)output##t, sizeof(t), l);
#define NODE_LOCATION NODE_SCALAR


#define NODE_OTHER_POINT(t,m)					\
		appendObjectMebName(str, #m, space);	\
		appendStringInfo(str, "%p\n", node->m);

#define NODE_DATUM(t,m,o,n) 					\
		appendObjectMebName(str, #m, space);	\
		outputDatum(str, node->m, o, n);

#define NODE_ENUM(t, m)										\
		do													\
		{													\
			const char *s = get_enum_string_##t(node->m);	\
			appendObjectMebName(str, #m, space);			\
			if (s)											\
				appendStringInfoString(str, s);				\
			else											\
				appendStringInfo(str, "%u", node->m);		\
			appendStringInfoChar(str, '\n');				\
		}while(0);

#include "nodes/def_no_all_struct.h"
#undef NO_STRUCT_ParamListInfoData
#undef NO_STRUCT_ParamExternData
#undef NO_STRUCT_QualCost
#undef NO_STRUCT_MergeScanSelCache
#undef NO_STRUCT_PartitionPruneStep
#define NO_NODE_Path
#define NO_NODE_EquivalenceClass
#define NO_NODE_IndexOptInfo
#include "nodes/struct_define.h"
#ifdef ADB
BEGIN_STRUCT(ReduceKeyInfo)
	NODE_NODE(Expr,key)
	NODE_OID(opclass,opclass)
	NODE_OID(opfamily,opfamily)
	NODE_OID(collation,collation)
END_STRUCT(ReduceKeyInfo)
BEGIN_STRUCT(ReduceInfo)
	NODE_NODE(List,storage_nodes)
	NODE_NODE(List,exclude_exec)
	NODE_NODE(List,values)
	NODE_RELIDS(Relids,relids)
	NODE_SCALAR(ReduceType,type)
	NODE_STRUCT_ARRAY(ReduceKeyInfo, keys, NODE_ARG_->nkey)
END_STRUCT(ReduceInfo)
#endif /* ADB */
BEGIN_NODE(Path)
	NODE_ENUM(NodeTag,pathtype)
	NODE_OTHER_POINT(RelOptInfo,parent)	/* don't print parent */
	NODE_NODE(PathTarget,pathtarget)
	NODE_NODE(ParamPathInfo,param_info)
	NODE_SCALAR(bool,parallel_aware)
	NODE_SCALAR(bool,parallel_safe)
	NODE_SCALAR(int,parallel_workers)
	NODE_SCALAR(double,rows)
	NODE_SCALAR(Cost,startup_cost)
	NODE_SCALAR(Cost,total_cost)
	NODE_NODE(List,pathkeys)
#ifdef ADB
	NODE_STRUCT_LIST(ReduceInfo, reduce_info_list)
	NODE_SCALAR(bool,reduce_is_valid)
#endif
END_NODE(Path)
BEGIN_NODE(EquivalenceClass)
	NODE_NODE(List,ec_opfamilies)
	NODE_OID(collation,ec_collation)
	NODE_NODE(List,ec_members)
	NODE_OTHER_POINT(List,ec_sources)		/* don't print parent */
	NODE_OTHER_POINT(List,ec_derives)
	NODE_RELIDS(Relids,ec_relids)
	NODE_SCALAR(bool,ec_has_const)
	NODE_SCALAR(bool,ec_has_volatile)
	NODE_SCALAR(bool,ec_below_outer_join)
	NODE_SCALAR(bool,ec_broken)
	NODE_SCALAR(Index,ec_sortref)
	NODE_NODE(EquivalenceClass,ec_merged)
END_NODE(EquivalenceClass)
BEGIN_NODE(IndexOptInfo)
	NODE_SCALAR(Oid,indexoid)
	NODE_SCALAR(Oid,reltablespace)
	NODE_OTHER_POINT(RelOptInfo,rel) /* don't print parent */
	NODE_SCALAR(BlockNumber,pages)
	NODE_SCALAR(double,tuples)
	NODE_SCALAR(int,tree_height)
	NODE_SCALAR(int,ncolumns)
	NODE_SCALAR_POINT(int,indexkeys,NODE_ARG_->ncolumns)
	NODE_SCALAR_POINT(Oid,indexcollations,NODE_ARG_->ncolumns)
	NODE_SCALAR_POINT(Oid,opfamily,NODE_ARG_->ncolumns)
	NODE_SCALAR_POINT(Oid,opcintype,NODE_ARG_->ncolumns)
	NODE_SCALAR_POINT(Oid,sortopfamily,NODE_ARG_->ncolumns)
	NODE_SCALAR_POINT(bool,reverse_sort,NODE_ARG_->ncolumns)
	NODE_SCALAR_POINT(bool,nulls_first,NODE_ARG_->ncolumns)
	NODE_SCALAR_POINT(bool,canreturn,NODE_ARG_->ncolumns)
	NODE_SCALAR(Oid,relam)
	NODE_NODE(List,indexprs)
	NODE_NODE(List,indpred)
	NODE_NODE(List,indextlist)
	NODE_NODE(List,indrestrictinfo)
	NODE_SCALAR(bool,predOK)
	NODE_SCALAR(bool,unique)
	NODE_SCALAR(bool,immediate)
	NODE_SCALAR(bool,hypothetical)
	NODE_SCALAR(bool,amcanorderbyop)
	NODE_SCALAR(bool,amoptionalkey)
	NODE_SCALAR(bool,amsearcharray)
	NODE_SCALAR(bool,amsearchnulls)
	NODE_SCALAR(bool,amhasgettuple)
	NODE_SCALAR(bool,amhasgetbitmap)
	NODE_OTHER_POINT(void,amcostestimate)
END_NODE(IndexOptInfo)
#include "nodes/nodes_define.h"
#include "nodes/nodes_undef.h"
#undef NO_NODE_Path
#undef NO_NODE_EquivalenceClass
#undef NO_NODE_IndexOptInfo

static void printNode(const void *obj, StringInfo str, int space)
{
	Assert(str);
	if(obj == NULL)
	{
		appendStringInfoString(str, "<>\n");
		return;
	}
	switch(nodeTag(obj))
	{
	#define CASE_TYPE(type,fun)							\
		case T_##type:									\
			outputNode(obj, (outputfunc)output##fun, #type, str, space);\
			break;
	#define BEGIN_NODE(type)	CASE_TYPE(type,type)
	#define NODE_SAME(t1,t2)	CASE_TYPE(t1,t2)
	#define NO_NODE_JoinPath
	#include "nodes/nodes_define.h"
	case T_Value:
	case T_Integer:
	case T_Float:
	case T_String:
	case T_BitString:
	case T_Null:
		if(str->len > 0 && str->data[str->len-1]=='\n')
			appendStringInfoSpaces(str, space);
		outputValue(obj, str, space);
		break;
	case T_List:
	case T_OidList:
	case T_IntList:
		outputList(obj, str, space);
		break;
	default:
		ereport(ERROR, (errmsg("unknown node type %d\n", nodeTag(obj))));
	}
}

char *printObject(const void *obj)
{
	StringInfoData str;
	initStringInfo(&str);
	printNode(obj, &str, 0);
	return str.data;
}

#ifdef ADB
char *printReduceInfo(ReduceInfo *rinfo)
{
	StringInfoData buf;
	initStringInfo(&buf);

	outputReduceInfo(rinfo, &buf, 0);

	return buf.data;
}

char *printReduceInfoList(List *list)
{
	StringInfoData buf;
	initStringInfo(&buf);

	outputListObject(list, &buf, TABLE_STOP, (outputfunc)outputReduceInfo);

	return buf.data;
}

#endif /* ADB */
