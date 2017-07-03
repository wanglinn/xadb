/*
 * saveNode and loadNode
 */

#include "postgres.h"
#include "libpq/pqformat.h"

#include "access/htup_details.h"
#include "access/transam.h"
#include "catalog/namespace.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "mb/pg_wchar.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "optimizer/pgxcplan.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "nodes/extensible.h"
#include "nodes/replnodes.h"
#include "commands/event_trigger.h"

#define IS_OID_BUILTIN(oid_) (oid_ < FirstNormalObjectId)

/* not support Node */
#define NO_NODE_PlannerInfo
#define NO_NODE_RelOptInfo
#define NO_NODE_RestrictInfo
#include "nodes/def_no_all_struct.h"
#undef NO_STRUCT_QualCost

/* declare static functions */
#define BEGIN_NODE(type) 										\
	static void save_##type(StringInfo buf, const type *node	\
					, bool (*hook)(), const void *context);		\
	static type* load_##type(StringInfo buf, type *node			\
					, void* (*hook)(), const void *context);
#define NODE_SAME(t1, t2)
#define BEGIN_STRUCT(type) BEGIN_NODE(type)
#include "nodes/nodes_define.h"
#include "nodes/struct_define.h"
#include "nodes/nodes_undef.h"

/* save functions */
#define SAVE_IS_NULL()			pq_sendbyte(buf, true)
#define SAVE_IS_NOT_NULL()		pq_sendbyte(buf, false)
#define SAVE_BOOL(b_)			pq_sendbyte(buf, (b_) ? true:false)

#define BEGIN_NODE(type) 											\
static void save_##type(StringInfo buf, const type *node			\
					, bool (*hook)(), const void *context)			\
{																	\
	AssertArg(node);
#define END_NODE(type)												\
}

#define BEGIN_STRUCT(type)											\
static void save_##type(StringInfo buf, const type *node			\
					, bool (*hook)(), const void *context)			\
{																	\
	if(node == NULL)												\
	{																\
		SAVE_IS_NULL();												\
		return;														\
	}
#define END_STRUCT(type)											\
}

#define NODE_SAME(t1,t2)
#define NODE_BASE(base)											\
		save_##base(buf, (const base*)node, hook, context);
#define NODE_NODE(t,m)											\
	do{															\
		if(hook == NULL || node->m == NULL						\
			|| (*hook)(buf, node->m, context) == false)			\
			saveNodeAndHook(buf, (Node*)node->m, hook, context);\
	}while(0);
#define NODE_NODE_MEB(t,m)			not support yet//saveNode(buf, (Node*)&(node->m));
#define NODE_NODE_ARRAY(t,m,l)		not support yet//SAVE_ARRAY(t,m,l,saveNode,Node);
#define NODE_BITMAPSET(t,m)			save_node_bitmapset(buf, node->m);
#define NODE_BITMAPSET_ARRAY(t,m,l)	not support yet//SAVE_ARRAY(t,m,l,save_node_bitmapset,Bitmapset);
#define NODE_SCALAR(t,m)			pq_sendbytes(buf, (const char*)&(node->m), sizeof(node->m));
#define NODE_SCALAR_POINT(t,m,l)								\
	do{															\
		uint32 len = (l);										\
		if(len)													\
		{														\
			Assert(node->m);									\
			pq_sendbytes(buf, (const char*)node->m				\
						, len * sizeof(node->m[0]));			\
		}														\
	}while(0);
#define NODE_OTHER_POINT(t,m)		not support
#define NODE_STRING(m)											\
	do{															\
		if(node->m)												\
		{														\
			Assert(node->m != NULL);							\
			SAVE_IS_NOT_NULL();									\
			save_node_string(buf, node->m);						\
		}else													\
		{														\
			SAVE_IS_NULL();										\
		}														\
	}while(0);
#define NODE_STRUCT(t,m)				save_##t(buf, node->m, hook, context);
#define NODE_STRUCT_ARRAY(t,m,l)		not support yet//SAVE_ARRAY(t,m,l,save_##t, t)
#define NODE_STRUCT_LIST(t,m)									\
	do{															\
		if(node->m != NIL)										\
		{														\
			ListCell *lc;										\
			SAVE_IS_NOT_NULL();									\
			pq_sendbytes(buf, (const char*)&(node->m->length)	\
				, sizeof(node->m->length));						\
			foreach(lc, node->m)								\
				save_##t(buf, lfirst(lc));						\
		}else													\
		{														\
			SAVE_IS_NULL();										\
		}														\
	}while(0);
#define NODE_STRUCT_MEB(t,m)			save_##t(buf, &node->m, hook, context);
#define NODE_ENUM(t,m)					NODE_SCALAR(t,m)
#define NODE_DATUM(t,m,o,n)			not support
#define NODE_OID(t, m)					save_oid_##t(buf, node->m);

/*#define SAVE_ARRAY(t,m,l,f,t2)										\
	do{																	\
		uint32 i,len = (l);												\
		pq_sendbytes(buf, (const char*)&len, sizeof(len));	\
		Assert((len == 0 && node->m == NULL) || (len > 0 && node->m != NULL));\
		for(i=0;i<len;++i)												\
			f(buf, (const t2*)node->m[i]);									\
	}while(0);*/

void save_node_string(StringInfo buf, const char *str)
{
	int len = strlen(str);
	appendBinaryStringInfo(buf, str, len+1);
}

void save_node_bitmapset(StringInfo buf, const Bitmapset *node)
{
	if(node == NULL)
	{
		SAVE_IS_NULL();
	}else
	{
		SAVE_IS_NOT_NULL();
		pq_sendbytes(buf, (const char*)&node->nwords, sizeof(node->nwords));
		pq_sendbytes(buf, (const char*)node->words, sizeof(node->words[0])*(node->nwords));
	}
}

void save_namespace(StringInfo buf, Oid nsp)
{
	Form_pg_namespace nspForm;
	HeapTuple tup;

	if(!OidIsValid(nsp))
		ereport(ERROR, (errmsg("can not save invalid OID for namespace")));

	if(IS_OID_BUILTIN(nsp))
	{
		SAVE_BOOL(true);
		pq_sendbytes(buf, (char*)&nsp, sizeof(nsp));
	}else
	{
		/* search namespace*/
		tup = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(nsp));
		if(!HeapTupleIsValid(tup))
			ereport(ERROR, (errmsg("Can not find namespace id %u", (unsigned)nsp)));
		nspForm = (Form_pg_namespace)GETSTRUCT(tup);
		Assert(nspForm);

		/* save namespace and type name */
		save_node_string(buf, NameStr(nspForm->nspname));
		ReleaseSysCache(tup);
	}
}

void save_oid_type(StringInfo buf, Oid typid)
{
	Type type;
	Form_pg_type typ;

	if(!OidIsValid(typid))
	{
		SAVE_IS_NULL();
		return;
	}
	SAVE_IS_NOT_NULL();

	/* get pg_type cache */
	type = typeidType(typid);
	Assert(HeapTupleIsValid(type));
	typ = (Form_pg_type)GETSTRUCT(type);
	Assert(typ);

	save_namespace(buf, typ->typnamespace);
	save_node_string(buf, NameStr(typ->typname));
	ReleaseSysCache(type);
}

void save_oid_collation(StringInfo buf, Oid collation)
{
	Form_pg_collation form_collation;
	HeapTuple	tuple;

	if(IS_OID_BUILTIN(collation))
	{
		SAVE_BOOL(true);
		pq_sendbytes(buf, (char*)&collation, sizeof(collation));
	}else
	{
		SAVE_BOOL(false);

		/*tuple = systable_getnext(scandesc);*/
		tuple = SearchSysCache1(COLLOID, ObjectIdGetDatum(collation));
		if(!HeapTupleIsValid(tuple))
		{
			ereport(ERROR, (errmsg("Can not found collation %u", collation)));
		}
		form_collation = (Form_pg_collation)GETSTRUCT(tuple);
		save_namespace(buf, form_collation->collnamespace);
		save_node_string(buf, NameStr(form_collation->collname));
		pq_sendint(buf, form_collation->collencoding, sizeof(form_collation->collencoding));

		ReleaseSysCache(tuple);
	}
}

void save_oid_proc(StringInfo buf, Oid proc)
{
	HeapTuple	proctup;
	Form_pg_proc procform;
	oidvector  *oidArray;
	int i,count;

	if(IS_OID_BUILTIN(proc))
	{
		SAVE_BOOL(true);
		pq_sendbytes(buf, (char*)&proc, sizeof(proc));
	}else
	{
		SAVE_BOOL(false);
		proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(proc));
		if (!HeapTupleIsValid(proctup))
			ereport(ERROR, (errmsg("cache lookup failed for function %u", proc)));
		procform = (Form_pg_proc) GETSTRUCT(proctup);
		save_namespace(buf, procform->pronamespace);
		save_node_string(buf, NameStr(procform->proname));

		/* save return type for check in load */
		save_oid_type(buf, procform->prorettype);

		/* save arg(s) type */
		pq_sendint(buf, procform->pronargs, sizeof(procform->pronargs));
		oidArray = &(procform->proargtypes);
		count = oidArray->dim1;
		Assert(count == procform->pronargs);
		for(i=0;i<count;++i)
			save_oid_type(buf, oidArray->values[i]);
		ReleaseSysCache(proctup);
	}
}

void save_oid_operator(StringInfo buf, Oid op)
{
	HeapTuple opertup;
	Form_pg_operator operform;
	if(IS_OID_BUILTIN(op))
	{
		SAVE_BOOL(true);
		pq_sendbytes(buf, (char*)&op, sizeof(op));
	}else
	{
		opertup = SearchSysCache1(OPEROID, ObjectIdGetDatum(op));
		if (!HeapTupleIsValid(opertup))
			elog(ERROR, "cache lookup failed for operator %u", op);
		operform = (Form_pg_operator) GETSTRUCT(opertup);

		/* save result type for check in load */
		save_oid_type(buf, operform->oprresult);

		save_namespace(buf, operform->oprnamespace);
		save_node_string(buf, NameStr(operform->oprname));

		save_oid_type(buf, operform->oprleft);
		save_oid_type(buf, operform->oprright);
	}
}

static void save_datum(StringInfo buf, Oid typid, Datum datum)
{
	int16 typlen;
	bool byval;

	get_typlenbyval(typid, &typlen, &byval);
	pq_sendint(buf, typlen, sizeof(typlen));

	if(typlen > 0)
	{
		if(byval)
		{
			pq_sendbytes(buf, (char*)&datum, SIZEOF_DATUM);
		}else
		{
			pq_sendbytes(buf, DatumGetPointer(datum), typlen);
		}
	}else if(typlen == -2)
	{
		/* a null-terminated C string */
		int len = strlen(DatumGetCString(datum));
		++len;
		pq_sendbytes(buf, DatumGetCString(datum), len);
	}else if(typlen == -1)
	{
		/* "varlena" type */
		TupleDesc desc;
		Size need_size;
		uint16 infomask;
		bool isnull;

		desc = CreateTemplateTupleDesc(1, false);
		TupleDescInitEntry(desc, 1, "???", typid, -1, 0);

		isnull = false;
		need_size = heap_compute_data_size(desc, &datum, &isnull);
		enlargeStringInfo(buf, (int)need_size);

		infomask = 0;
		heap_fill_tuple(desc, &datum, &isnull, buf->data+buf->len, need_size, &infomask, NULL);
		buf->len += need_size;

		FreeTupleDesc(desc);
	}else
	{
		ereport(ERROR, (errmsg("unknown type length %d", typlen)));
	}
}

static void save_ParamExternData(StringInfo buf, const ParamExternData *node)
{
	AssertArg(node);
	pq_sendbytes(buf, (const char*)&(node->pflags), sizeof(node->pflags));
	save_oid_type(buf, node->ptype);
	if(node->isnull)
	{
		SAVE_IS_NULL();
	}else
	{
		SAVE_IS_NOT_NULL();
		save_datum(buf, node->ptype, node->value);
	}
}

BEGIN_STRUCT(ParamListInfoData)
	NODE_SCALAR(int, numParams)
	do{
		int i;
		for(i=0;i<node->numParams;++i)
			save_ParamExternData(buf, &(node->params[i]));
	}while(0);
END_STRUCT(ParamListInfoData)

static void save_Integer(StringInfo buf, const Value *node)
{
	AssertArg(node);
	pq_sendbytes(buf, (const char*)&(intVal(node)), sizeof(intVal(node)));
}

static void save_String(StringInfo buf, const Value *node)
{
	AssertArg(node);
	save_node_string(buf, strVal(node));
}

BEGIN_NODE(List)
	do{
		ListCell *lc;
		pq_sendbytes(buf, (const char *)&(node->length), sizeof(node->length));
		if(hook)
		{
			foreach(lc, node)
			{
				if(lfirst(lc) == NULL || (*hook)(buf, lfirst(lc), context) == false)
					saveNodeAndHook(buf, lfirst(lc), hook, context);
			}
		}else
		{
			foreach(lc,node)
				saveNodeAndHook(buf, lfirst(lc), NULL, context);
		}
	}while(0);
END_NODE(List)

static void save_IntList(StringInfo buf, const List *node)
{
	ListCell *lc;
	AssertArg(node);
	pq_sendbytes(buf, (const char *)&(node->length), sizeof(node->length));
	foreach(lc,node)
		pq_sendbytes(buf, (const char*)&lfirst_int(lc), sizeof(lfirst_int(lc)));
}
static void save_OidList(StringInfo buf, const List *node)
{
	ListCell *lc;
	AssertArg(node);
	pq_sendbytes(buf, (const char *)&(node->length), sizeof(node->length));
	foreach(lc,node)
		pq_sendbytes(buf, (const char*)&lfirst_oid(lc), sizeof(lfirst_oid(lc)));
}

BEGIN_NODE(Const)
	NODE_OID(type,consttype);
	NODE_SCALAR(int32,consttypmod)
	NODE_SCALAR(Oid,constcollid)
	NODE_SCALAR(int,constlen)
	if(node->constisnull)
	{
		SAVE_IS_NULL();
	}else
	{
		SAVE_IS_NOT_NULL();
		save_datum(buf, node->consttype, node->constvalue);
	}
	NODE_SCALAR(bool,constbyval)
	NODE_SCALAR(int,location)
END_NODE(Const)

BEGIN_NODE(OidVectorLoopExpr)
	NODE_SCALAR(bool, signalRowMode)
	save_datum(buf, OIDVECTOROID, node->vector);
END_NODE(OidVectorLoopExpr)

BEGIN_NODE(A_Const)
	NODE_SCALAR(NodeTag, val.type);
	switch(node->val.type)
	{
	case T_Integer:
		save_Integer(buf, &(node->val));
		break;
	case T_Float:
	case T_String:
	case T_BitString:
		save_String(buf, &(node->val));
		break;
	case T_Null:
		break;
	default:
		ereport(ERROR, (errmsg("unknown node type %d\n", node->val.type)
			,errcode(ERRCODE_INTERNAL_ERROR)));
	}
	NODE_SCALAR(int, location);
END_NODE(A_Const)

#define NO_NODE_A_Const
#define NO_NODE_Const
#define NO_NODE_OidVectorLoopExpr
#include "nodes/nodes_define.h"
#include "nodes/struct_define.h"
#include "nodes/nodes_undef.h"
#undef NO_NODE_A_Const
#undef NO_NODE_Const
#undef NO_NODE_OidVectorLoopExpr

void saveNode(StringInfo buf, const Node *node)
{
	saveNodeAndHook(buf, node, NULL, NULL);
}

void saveNodeAndHook(StringInfo buf, const Node *node
					, bool (*hook)(), const void *context)
{
	AssertArg(buf);
	if(node == NULL)
	{
		SAVE_IS_NULL();
		return;
	}

	SAVE_IS_NOT_NULL();
	pq_sendbytes(buf, (const char*)&(node->type), sizeof(node->type));
	switch(nodeTag(node))
	{
#define CASE_TYPE(type, fun)							\
	case T_##type:										\
		save_##fun(buf, (type *)node, hook, context);	\
		break
#define BEGIN_NODE(type)	CASE_TYPE(type,type);
#define NODE_SAME(t1,t2)	CASE_TYPE(t1,t2);
#define NO_NODE_JoinPath
	case T_Integer:
		save_Integer(buf, (Value*)node);break;
	case T_Float:
	case T_String:
	case T_BitString:
		save_String(buf, (Value*)node);break;
	case T_Null:
		break;
	case T_List:
		save_List(buf, (const List*)node, hook, context);break;
	case T_IntList:
		save_IntList(buf, (const List*)node);break;
	case T_OidList:
		save_OidList(buf, (const List*)node);break;
#include "nodes/nodes_define.h"
#include "nodes/nodes_undef.h"
#undef NO_NODE_JoinPath
	default:
		ereport(ERROR, (errmsg("unknown node type %d\n", (int)nodeTag(node))));
	}
}

/* load functions */
#define LOAD_IS_NULL() pq_getmsgbyte(buf)
#define LOAD_BOOL() pq_getmsgbyte(buf)

#define BEGIN_NODE(type)									\
	static type* load_##type(StringInfo buf, type *node		\
					, void* (*hook)(), const void *context)	\
	{														\
		AssertArg(buf && node);
#define END_NODE(type)										\
		return node;										\
	}
#define BEGIN_STRUCT(type)			BEGIN_NODE(type)
#define END_STRUCT(type)			END_NODE(type)
#define NODE_SAME(t1,t2)
#define NODE_BASE(base)				load_##base(buf, (base*)node, hook, context);
#define NODE_NODE(t,m)				node->m = (t*)loadNodeAndHook(buf, hook, context);
#define NODE_NODE_MEB(t,m)			not support yet
#define NODE_NODE_ARRAY(t,m,l)		not support yet
#define NODE_BITMAPSET(t,m)			node->m = load_Bitmapset(buf);
#define NODE_BITMAPSET_ARRAY(t,m,l)	not support yet
#define NODE_SCALAR(t,m)			pq_copymsgbytes(buf, (char*)&(node->m), sizeof(node->m));
#define NODE_SCALAR_POINT(t,m,l)							\
	do{														\
		uint32 len = (l);									\
		if(len)												\
		{													\
			len *= sizeof(node->m[0]);						\
			node->m = palloc(len);							\
			pq_copymsgbytes(buf, (char*)node->m, len);		\
		}													\
	}while(0);
#define NODE_OTHER_POINT(t,m)		not support
#define NODE_STRING(m)										\
	do{														\
		if(LOAD_IS_NULL())									\
			node->m = NULL;									\
		else												\
			node->m = load_node_string(buf, true);			\
	}while(0);
#define NODE_STRUCT(t,m)									\
	do{														\
		if(LOAD_IS_NULL())									\
			node->m = NULL;									\
		else												\
			node->m = load_##t(buf, palloc0(sizeof(node->m[0])), hook, context);	\
	}while(0);
#define NODE_STRUCT_ARRAY(t,m,l)		not support yet//SAVE_ARRAY(t,m,l,save_##t, t)
#define NODE_STRUCT_LIST(t,m)								\
	do{														\
		node->m = NIL;										\
		if(!LOAD_IS_NULL())									\
		{													\
			int i,length;									\
			t *v;											\
			pq_copymsgbytes(buf, (char*)&length, sizeof(length));\
			for(i=0;i<length;i++)							\
			{												\
				if(LOAD_IS_NULL())							\
					v = NULL;								\
				else										\
					v = load_##t(buf, palloc0(sizeof(*v)));	\
				node->m = lappend(node->m, v);				\
			}												\
		}													\
	}while(0);
#define NODE_STRUCT_MEB(t,m)			(void)load_##t(buf,&(node->m), hook, context);
#define NODE_ENUM(t,m)					NODE_SCALAR(t,m)
#define NODE_DATUM(t,m,o,n)				not support
#define NODE_OID(t,m)					node->m = load_oid_##t(buf);

char * load_node_string(StringInfo buf, bool need_dup)
{
	char *str;
	int len;
	AssertArg(buf && buf->data);

	str = (buf->data + buf->cursor);
	len = strlen(str);
	if (buf->cursor + len >= buf->len)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid string in message")));
	buf->cursor += (len + 1);

	if(need_dup)
		str = pnstrdup(str, len);
	return str;
}

Bitmapset* load_Bitmapset(StringInfo buf)
{
	Bitmapset *node;
	int nwords;
	AssertArg(buf);
	if(LOAD_IS_NULL())
		return NULL;
	pq_copymsgbytes(buf, (char*)&nwords, sizeof(nwords));
	node = palloc(offsetof(Bitmapset, words) + nwords * sizeof(node->words[0]));
	pq_copymsgbytes(buf, (char*)(node->words), nwords * sizeof(node->words[0]));
	node->nwords = nwords;
	return node;
}

Oid load_namespace(StringInfo buf)
{
	Oid oid;
	if(LOAD_BOOL())
	{
		pq_copymsgbytes(buf, (char*)&oid, sizeof(oid));
	}else
	{
		const char *nsp_name = load_node_string(buf, false);
		oid = LookupExplicitNamespace(nsp_name, false);
	}
	return oid;
}

Oid load_oid_type(StringInfo buf)
{
	const char *str_type;
	HeapTuple tup;
	Oid typid,namespaceId;

	if(LOAD_IS_NULL())
		return InvalidOid;

	namespaceId = load_namespace(buf);
	if(!OidIsValid(namespaceId))
		ereport(ERROR, (errmsg("Load an invalid namespace id")));

	str_type = load_node_string(buf, false);
	tup = SearchSysCache2(TYPENAMENSP, CStringGetDatum(str_type)
		, ObjectIdGetDatum(namespaceId));
	if(!HeapTupleIsValid(tup))
	{
		ereport(ERROR, (errmsg("Can not found type \"%s\" at namespace %u", str_type, (unsigned)namespaceId)));
	}

	typid = HeapTupleGetOid(tup);
	ReleaseSysCache(tup);
	return typid;
}

Oid load_oid_collation(StringInfo buf)
{
	Oid oid;
	if(LOAD_BOOL())
	{
		pq_copymsgbytes(buf, (char*)&oid, sizeof(oid));
	}else
	{
		const char *coll_name;
		NameData name;
		Oid nsp;
		int32 encoding;
		HeapTuple tup;

		nsp = load_namespace(buf);
		coll_name = load_node_string(buf, false);
		namestrcpy(&name, coll_name);
		encoding = pq_getmsgint(buf, sizeof(encoding));

		tup = SearchSysCache3(COLLNAMEENCNSP
				, NameGetDatum(&name)
				, Int32GetDatum(encoding)
				, ObjectIdGetDatum(nsp));
		if(!HeapTupleIsValid(tup))
		{
			ereport(ERROR, (errmsg("Can not collation \"%s\" for encoding \"%s\" in namespace \"%s\""
				, NameStr(name), pg_encoding_to_char(encoding), get_namespace_name(nsp))));
		}
		oid = HeapTupleGetOid(tup);
		ReleaseSysCache(tup);
	}
	return oid;
}

Oid load_oid_proc(StringInfo buf)
{
	Oid oid;
	if(LOAD_BOOL())
	{
		pq_copymsgbytes(buf, (char*)&oid, sizeof(oid));
	}else
	{
		Oid nsp;
		Oid rettype;
		oidvector *vector;
		NameData name;
		int16 i,nargs;
		const char *proc_name;
		Oid *args;
		HeapTuple tup;

		nsp = load_namespace(buf);
		proc_name = load_node_string(buf, false);
		namestrcpy(&name, proc_name);

		rettype = load_oid_type(buf);

		nargs = pq_getmsgint(buf, sizeof(nargs));
		if(nargs > 0)
		{
			args = palloc(sizeof(Oid)*nargs);
			for(i=0;i<nargs;++i)
				args[i] = load_oid_type(buf);
		}else if(nargs < 0)
		{
			ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION)
				, errmsg("Invalid count of Oid %d", nargs)));
		}else
		{
			args = NULL;
		}
		vector = buildoidvector(args, nargs);

		tup = SearchSysCache3(PROCNAMEARGSNSP
			, NameGetDatum(&name)
			, PointerGetDatum(vector)
			, ObjectIdGetDatum(nsp));
		if(!HeapTupleIsValid(tup))
		{
			ereport(ERROR, (errmsg("Can not load function %s at namespace %s"
				, funcname_signature_string(NameStr(name), nargs, NIL, args), get_namespace_name(nsp))));
		}
		oid = HeapTupleGetOid(tup);
		ReleaseSysCache(tup);

		/* test return type */
		if(rettype != get_func_rettype(oid))
		{
			ereport(ERROR, (errmsg("function %s.%s return type is not %s"
					, get_namespace_name(nsp)
					, funcname_signature_string(NameStr(name), nargs, NIL, args)
					, format_type_be(rettype))
				, errhint("return type is %s", format_type_be(get_func_rettype(oid)))));
		}

		if(args)
			pfree(args);
		pfree(vector);
	}
	return oid;
}

Oid load_oid_operator(StringInfo buf)
{
	Oid oid;
	if(LOAD_BOOL())
	{
		pq_copymsgbytes(buf, (char*)&oid, sizeof(oid));
	}else
	{
		HeapTuple tup;
		Form_pg_operator form_oper;
		const char *opr_name;
		NameData name;
		Oid nsp;
		Oid rettype;
		Oid left;
		Oid right;

		rettype = load_oid_type(buf);
		nsp = load_namespace(buf);
		opr_name = load_node_string(buf, false);
		namestrcpy(&name, opr_name);

		left = load_oid_type(buf);
		right = load_oid_type(buf);

		tup = SearchSysCache4(OPERNAMENSP
			, NameGetDatum(&name)
			, ObjectIdGetDatum(left)
			, ObjectIdGetDatum(right)
			, ObjectIdGetDatum(nsp));
		if(!HeapTupleIsValid(tup))
		{
			ereport(ERROR, (errmsg("Can not load opeator %s", NameStr(name))
				,errhint("left %s, right %s"
					, OidIsValid(left) ? format_type_be(left) : "invalid"
					, OidIsValid(right) ? format_type_be(right) : "invalid")));
		}
		oid = HeapTupleGetOid(tup);

		form_oper = (Form_pg_operator)GETSTRUCT(tup);
		if(rettype != form_oper->oprresult)
		{
			ereport(ERROR,
				(errmsg("operator %u result type is not %s", oid, format_type_be(rettype))
				,errhint("it result type %s", format_type_be(form_oper->oprresult))));
		}
		ReleaseSysCache(tup);
	}

	return oid;
}

static Datum load_datum(StringInfo buf, Oid typid)
{
	Datum datum;
	int16 typlen,typlen2;
	bool byval;

	get_typlenbyval(typid, &typlen, &byval);
	typlen2 = (int16)pq_getmsgint(buf, sizeof(typlen2));
	if(typlen2 != typlen)
	{
		ereport(ERROR, (errmsg("local type %s length %d not equal load length"
			, format_type_be(typid), typlen)));
	}

	if(typlen > 0)
	{
		if(byval)
		{
			Assert(typlen <= SIZEOF_DATUM);
			pq_copymsgbytes(buf, (char*)&datum, SIZEOF_DATUM);
		}else
		{
			datum = PointerGetDatum(palloc(typlen));
			pq_copymsgbytes(buf, DatumGetPointer(datum), typlen);
		}
	}else if(typlen == -2)
	{
		/* a null-terminated C string */
		char *str = buf->data + buf->cursor;
		int len = strlen(str);
		str = pnstrdup(str, len);
		buf->cursor += len+1;
		datum = CStringGetDatum(str);
	}else if(typlen == -1)
	{
		/* "varlena" type */
		void *p = (buf->data + buf->cursor);
		void *var;
		int len = VARSIZE_ANY(p);
		var = palloc(len);
		pq_copymsgbytes(buf, var, len);
		datum = PointerGetDatum(var);
	}else
	{
		ereport(ERROR, (errmsg("unknown type length %d", typlen)));
	}

	return datum;
}

static ParamExternData* load_ParamExternData(StringInfo buf, ParamExternData *node)
{
	AssertArg(node);
	pq_copymsgbytes(buf, (char*)&(node->pflags), sizeof(node->pflags));
	node->ptype = load_oid_type(buf);
	if(LOAD_IS_NULL())
	{
		node->isnull = true;
		node->value = (Datum)0;
	}else
	{
		node->isnull = false;
		node->value = load_datum(buf, node->ptype);
	}
	return node;
}

BEGIN_STRUCT(ParamListInfoData)
	NODE_SCALAR(int, numParams);
	node = repalloc(node, offsetof(ParamListInfoData, params)
		+ node->numParams * (sizeof(node->params[0])));
	do{
		int i;
		for(i=0;i<node->numParams;++i)
			(void)load_ParamExternData(buf, &(node->params[i]));
	}while(0);
END_STRUCT(ParamListInfoData)

static Value* load_Integer(StringInfo buf, Value *node)
{
	AssertArg(node);
	pq_copymsgbytes(buf, (char*)&(intVal(node)), sizeof(intVal(node)));
	return node;
}

static Value* load_String(StringInfo buf, Value *node)
{
	AssertArg(node);
	strVal(node) = load_node_string(buf, true);
	return node;
}

static List* load_List(StringInfo buf, void* (*hook)(), const void *context)
{
	List *list = NIL;
	int i,length;
	pq_copymsgbytes(buf, (char*)&length, sizeof(length));
	for(i=0;i<length;++i)
		list = lappend(list, loadNodeAndHook(buf, hook, context));
	return list;
}

static List* load_OidList(StringInfo buf)
{
	List *list = NIL;
	int i,length;
	Oid oid;
	pq_copymsgbytes(buf, (char*)&length, sizeof(length));
	for(i=0;i<length;++i)
	{
		pq_copymsgbytes(buf, (char*)&oid, sizeof(oid));
		list = lappend_oid(list, oid);
	}
	return list;
}

static List* load_IntList(StringInfo buf)
{
	List *list = NIL;
	int i,length,val;
	pq_copymsgbytes(buf, (char*)&length, sizeof(length));
	for(i=0;i<length;++i)
	{
		pq_copymsgbytes(buf, (char*)&val, sizeof(val));
		list = lappend_int(list, val);
	}
	return list;
}

BEGIN_NODE(Const)
	NODE_OID(type,consttype);
	NODE_SCALAR(int32,consttypmod)
	NODE_SCALAR(Oid,constcollid)
	NODE_SCALAR(int,constlen)
	if(LOAD_IS_NULL())
	{
		node->constisnull = true;
		node->constvalue = (Datum)0;
	}else
	{
		node->constisnull = false;
		node->constvalue = load_datum(buf, node->consttype);
	}
	NODE_SCALAR(bool,constbyval)
	NODE_SCALAR(int,location)
END_NODE(Const)

BEGIN_NODE(OidVectorLoopExpr)
	NODE_SCALAR(bool, signalRowMode)
	node->vector = load_datum(buf, OIDVECTOROID);
END_NODE(OidVectorLoopExpr)

BEGIN_NODE(A_Const)
	NODE_SCALAR(NodeTag, val.type);
	switch(node->val.type)
	{
	case T_Integer:
		(void)load_Integer(buf, &(node->val));
		break;
	case T_Float:
	case T_String:
	case T_BitString:
		(void)load_String(buf, &(node->val));
		break;
	case T_Null:
		break;
	default:
		ereport(ERROR, (errmsg("unknown node type %d\n", node->val.type)
			,errcode(ERRCODE_INTERNAL_ERROR)));
	}
	NODE_SCALAR(int, location);
END_NODE(A_Const)

#define NO_NODE_Const
#define NO_NODE_A_Const
#define NO_NODE_OidVectorLoopExpr
#include "nodes/nodes_define.h"
#include "nodes/struct_define.h"
#include "nodes/nodes_undef.h"
#undef NO_NODE_Const
#undef NO_NODE_A_Const
#undef NO_NODE_OidVectorLoopExpr

Node* loadNode(StringInfo buf)
{
	return loadNodeAndHook(buf, NULL, NULL);
}

Node* loadNodeAndHook(struct StringInfoData* buf
							, void* (*hook)(), const void *context)
{
	Node *node;
	NodeTag tag;
	AssertArg(buf);

	if(LOAD_IS_NULL())
		return NULL;

	pq_copymsgbytes(buf, (char*)&tag, sizeof(tag));
	if(hook == NULL
		|| (node = (*hook)(buf, tag, context)) == NULL)
		node = loadNodeAndHookWithTag(buf, hook, context, tag);
	return node;
}

Node* loadNodeAndHookWithTag(struct StringInfoData *buf, void* (*hook)()
							, const void *context, NodeTag tag)
{
	switch(tag)
	{
	case T_Integer:
		return (Node*)load_Integer(buf, makeInteger(0));
	case T_Float:
	case T_String:
	case T_BitString:
	case T_Null:
		{
			Value *value = palloc0(sizeof(Value));
			NodeSetTag(value, tag);
			if(tag != T_Null)
				value = load_String(buf, value);
			return (Node*)value;
		}
	case T_List:
		return (Node*)load_List(buf, hook, context);
	case T_IntList:
		return (Node*)load_IntList(buf);
	case T_OidList:
		return (Node*)load_OidList(buf);
#undef CASE_TYPE
#define CASE_TYPE(type, fun)							\
	case T_##type:										\
		return (Node*)load_##fun(buf, makeNode(type), hook, context)
#define BEGIN_NODE(type)	CASE_TYPE(type,type);
#define NODE_SAME(t1,t2)	CASE_TYPE(t1,t2);
#define NO_NODE_JoinPath
#define NO_NODE_PlannerInfo
#include "nodes/nodes_define.h"
#include "nodes/nodes_undef.h"
#undef NO_NODE_JoinPath
#undef NO_NODE_PlannerInfo
	default:
		ereport(ERROR, (errmsg("unknown node type %d\n", (int)tag)));
	}
	return NULL;
}

void SaveParamList(struct StringInfoData *buf, ParamListInfo paramLI)
{
	int			nparams;
	int			i;

	/* Write number of parameters. */
	if (paramLI == NULL || paramLI->numParams <= 0)
	{
		SAVE_IS_NULL();
		return;
	}else
	{
		nparams = paramLI->numParams;
	}

	for (i = 0; i < nparams; i++)
	{
		ParamExternData *prm = &paramLI->params[i];

		if(bms_is_member(i, paramLI->paramMask))
		{
			/* give hook a chance in case parameter is dynamic */
			if (!OidIsValid(prm->ptype) && paramLI->paramFetch != NULL)
				(*paramLI->paramFetch) (paramLI, i + 1);
		}
	}
	SAVE_IS_NOT_NULL();
	save_ParamListInfoData(buf, paramLI, NULL, NULL);
}

ParamListInfo LoadParamList(struct StringInfoData *buf)
{
	ParamListInfo info;
	if(LOAD_IS_NULL())
		return NULL;
	info = palloc(sizeof(*info));
	return load_ParamListInfoData(buf, info, NULL, NULL);
}
