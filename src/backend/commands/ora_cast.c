#include "postgres.h"

#include "access/genam.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/ora_cast.h"
#include "catalog/pg_proc_d.h"
#include "catalog/pg_type_d.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"


static void DropOracleCastLocal(DropStmt *stmt);
static Oid TypenameGetTypOid(TypeName *typname, bool *find);

static Oid GetCastType(TypeName *typeName, ParseState *pstate, char *typtype, const char *context)
{
	Oid			typeoid;
	AclResult	aclresult;
	char		type;

	typeoid = typenameTypeId(pstate, typeName);
	type = get_typtype(typeoid);
	if (type == TYPTYPE_PSEUDO)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("%s data type %s is a pseudo-type", _(context),
						TypeNameToString(typeName)),
				 parser_errposition(pstate, typeName->location)));

	aclresult = pg_type_aclcheck(typeoid, GetUserId(), ACL_USAGE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error_type(aclresult, typeoid);

	*typtype = type;
	return typeoid;
}

static void CreateOracleCastLocal(CreateOracleCastStmt *stmt, ParseState *pstate)
{
	Oid			sourcetypeid;
	Oid			targettypeid;
	Oid			func_oid;
	Oid			trunc_func_oid;
	HeapTuple	tuple;
	int			nargs;
	char		sourcetyptype;
	char		targettyptype;
	Relation	rel;
	Datum		datums[Natts_ora_cast];
	bool		nulls[Natts_ora_cast];
	ObjectAddress	myself;
	ObjectAddress	referenced;

	sourcetypeid = GetCastType(stmt->source_type, pstate, &sourcetyptype, "source");
	targettypeid = GetCastType(stmt->target_type, pstate, &targettyptype, "target");

	if (stmt->func)
	{
		func_oid = LookupFuncWithArgs(OBJECT_FUNCTION, stmt->func, false);
		nargs = CheckCastFunction(func_oid, sourcetypeid, targettypeid);
	}else
	{
		CheckCastWithBinary(sourcetypeid, targettypeid,
							sourcetyptype, targettyptype);
		func_oid = InvalidOid;
		nargs = 0;
	}

	/*
	 * Allow source and target types to be same only for length coercion
	 * functions.  We assume a multi-arg function does length coercion.
	 */
	if (sourcetypeid == targettypeid && nargs < 2)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("source data type and target data type are the same")));

	trunc_func_oid = LookupFuncWithArgs(OBJECT_FUNCTION, stmt->trunc_func, false);
	if (!OraCoercionContextIsValid(stmt->coerce_context))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("invalid convert coerce %d", stmt->coerce_context)));

	rel = table_open(OraCastRelationId, RowExclusiveLock);
	tuple = SearchSysCache3(ORACASTSOURCETARGET,
							ObjectIdGetDatum(sourcetypeid),
							ObjectIdGetDatum(targettypeid),
							CharGetDatum(stmt->coerce_context));
	myself.objectId = InvalidOid;
	if (tuple)
	{
		HeapTuple	newtup;
		bool		reps[Natts_ora_cast];

		if (stmt->replace == false)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("cast from type %s to type %s with %s already exists",
							format_type_be(sourcetypeid),
							format_type_be(targettypeid),
							GetOraCastCoercionName(stmt->coerce_context))));

		myself.objectId = ((Form_ora_cast)GETSTRUCT(tuple))->castid;
		deleteDependencyRecordsFor(OraCastRelationId, myself.objectId, false);

		MemSet(reps, false, sizeof(reps));
		datums[Anum_ora_cast_castfunc-1] = ObjectIdGetDatum(func_oid);
		reps[Anum_ora_cast_castfunc-1] = true;
		nulls[Anum_ora_cast_castfunc-1] = false;
		datums[Anum_ora_cast_casttruncfunc-1] = ObjectIdGetDatum(trunc_func_oid);
		reps[Anum_ora_cast_casttruncfunc-1] = true;
		nulls[Anum_ora_cast_casttruncfunc-1] = false;

		newtup = heap_modify_tuple(tuple, RelationGetDescr(rel), datums, nulls, reps);
		CatalogTupleUpdate(rel, &tuple->t_self, newtup);
		heap_freetuple(newtup);
		ReleaseSysCache(tuple);
	}else
	{
		Relation		irel = index_open(OraCastIdIndexId, AccessShareLock);
		IndexScanDesc	scan = index_beginscan(rel, irel, SnapshotAny, 0, 0);
		TupleTableSlot *slot = table_slot_create(rel, NULL);

		MemSet(nulls, true, sizeof(nulls));

		/* generate a new id */
		index_rescan(scan, NULL, 0, NULL, 0);
		if (index_getnext_slot(scan, BackwardScanDirection, slot) == false)
		{
			myself.objectId = 1;
			datums[Anum_ora_cast_castid-1] = ObjectIdGetDatum(myself.objectId);
			nulls[Anum_ora_cast_castid-1] = false;
		}else
		{
			Datum d = slot_getattr(slot,
								   Anum_ora_cast_castid,
								   &nulls[Anum_ora_cast_castid-1]);
			Assert(nulls[Anum_ora_cast_castid-1] == false);
			myself.objectId = DatumGetObjectId(d) + 1;
			datums[Anum_ora_cast_castid-1] = ObjectIdGetDatum(myself.objectId);
		}
		ExecDropSingleTupleTableSlot(slot);
		index_endscan(scan);
		index_close(irel, AccessShareLock);

		datums[Anum_ora_cast_castsource-1] = ObjectIdGetDatum(sourcetypeid);
		nulls[Anum_ora_cast_castsource-1] = false;
		datums[Anum_ora_cast_casttarget-1] = ObjectIdGetDatum(targettypeid);
		nulls[Anum_ora_cast_casttarget-1] = false;
		datums[Anum_ora_cast_castfunc-1] = ObjectIdGetDatum(func_oid);
		nulls[Anum_ora_cast_castfunc-1] = false;
		datums[Anum_ora_cast_casttruncfunc-1] = ObjectIdGetDatum(trunc_func_oid);
		nulls[Anum_ora_cast_casttruncfunc-1] = false;
		datums[Anum_ora_cast_castcontext-1] = CharGetDatum(stmt->coerce_context);
		nulls[Anum_ora_cast_castcontext-1] = false;

		tuple = heap_form_tuple(RelationGetDescr(rel), datums, nulls);
		CatalogTupleInsert(rel, tuple);
		heap_freetuple(tuple);
	}

	/* make dependency entries */
	myself.classId = OraCastRelationId;
	Assert(OidIsValid(myself.objectId));
	myself.objectSubId = 0;

	/* dependency on source type */
	referenced.classId = TypeRelationId;
	referenced.objectId = sourcetypeid;
	referenced.objectSubId = 0;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	/* dependency on target type */
	referenced.classId = TypeRelationId;
	referenced.objectId = targettypeid;
	referenced.objectSubId = 0;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	/* dependency on function */
	if (OidIsValid(func_oid))
	{
		referenced.classId = ProcedureRelationId;
		referenced.objectId = func_oid;
		referenced.objectSubId = 0;
		recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
	}
	if (OidIsValid(trunc_func_oid))
	{
		referenced.classId = ProcedureRelationId;
		referenced.objectId = trunc_func_oid;
		referenced.objectSubId = 0;
		recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
	}

	/* dependency on extension */
	recordDependencyOnCurrentExtension(&myself, false);

	/* Post creation hook for new cast */
	InvokeObjectPostCreateHook(OraCastRelationId, myself.objectId, 0);

	table_close(rel, RowExclusiveLock);
}

void CreateOracleCast(CreateOracleCastStmt *stmt, ParseState *pstate)
{
	CreateOracleCastLocal(stmt, pstate);
}

void DropOracleCastById(Oid castid)
{
	Relation	rel;
	ScanKeyData	key;
	SysScanDesc	scan;
	HeapTuple	tuple;

	rel = table_open(OraCastRelationId, RowExclusiveLock);

	ScanKeyInit(&key,
				Anum_ora_cast_castid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(castid));
	scan = systable_beginscan(rel, OraCastIdIndexId, true, NULL, 1, &key);

	tuple = systable_getnext(scan);
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "could not find tuple for oracle cast %u", castid);
	CatalogTupleDelete(rel, &tuple->t_self);

	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);
}

const char *GetOraCastCoercionName(char coerce)
{
	switch((OraCoercionContext)coerce)
	{
	case ORA_COERCE_DEFAULT:
		return "default";
	case ORA_COERCE_OPERATOR:
		return "operator";
		break;
	case ORA_COERCE_COMMON_FUNCTION:
		return "common function";
	case ORA_COERCE_SPECIAL_FUNCTION:
		return "special function";
	case ORA_COERCE_NOUSE:
		return "nouse";
	}

	return "unknown";
}

Oid GetOracleCastOid(List *objects, bool missing_ok)
{
	TypeName   *sourcetypename;
	TypeName   *targettypename;
	Datum		castcontext;
	Datum		sourcetyptype;
	Datum		targettyptype;
	ListCell   *lc;
	Oid			oid, result;
	bool		find_oid;


	lc = list_head(objects);
	Assert(IsA(lfirst(lc), Integer));
	castcontext = CharGetDatum(intVal(lfirst(lc)));

	lc = lnext(objects, lc);
	Assert(IsA(lfirst(lc), TypeName));
	sourcetypename = lfirst_node(TypeName, lc);
	oid = TypenameGetTypOid(sourcetypename, &find_oid);
	if (!find_oid)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("type \"%s\" does not exist",TypeNameToString(sourcetypename))));
	sourcetyptype = ObjectIdGetDatum(oid);

	lc = lnext(objects, lc);
	Assert(IsA(lfirst(lc), TypeName));
	targettypename = lfirst_node(TypeName, lc);
	oid = TypenameGetTypOid(targettypename, &find_oid);
	if (!find_oid)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("type \"%s\" does not exist",TypeNameToString(targettypename))));
	targettyptype = ObjectIdGetDatum(oid);

	result = GetSysCacheOid3(ORACASTSOURCETARGET, Anum_ora_cast_castid,
							 sourcetyptype,
							 targettyptype,
							 castcontext);

	if (!OidIsValid(result) &&
		missing_ok == false)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("convert not exist")));

	return result;
}


void DropOracleCast(DropStmt *stmt)
{
	DropOracleCastLocal(stmt);
}


static void DropOracleCastLocal(DropStmt *stmt)
{
	TypeName   *sourcetypename;
	TypeName   *targettypename;
	Datum		castcontext;
	Datum		sourcetyptype;
	Datum		targettyptype;
	HeapTuple	tuple;
	Relation	oracast_rel;
	ListCell   *lc;
	Oid			oid;
	bool		find_oid;


	lc = list_head(stmt->objects);
	Assert(IsA(lfirst(lc), Integer));
	castcontext = CharGetDatum(intVal(lfirst(lc)));

	lc = lnext(stmt->objects, lc);
	Assert(IsA(lfirst(lc), TypeName));
	sourcetypename = lfirst_node(TypeName, lc);
	oid = TypenameGetTypOid(sourcetypename, &find_oid);
	if (!find_oid)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("type \"%s\" does not exist",TypeNameToString(sourcetypename))));
	sourcetyptype = ObjectIdGetDatum(oid);

	lc = lnext(stmt->objects, lc);
	Assert(IsA(lfirst(lc), TypeName));
	targettypename = lfirst_node(TypeName, lc);
	oid = TypenameGetTypOid(targettypename, &find_oid);
	if (!find_oid)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("type \"%s\" does not exist",TypeNameToString(targettypename))));
	targettyptype = ObjectIdGetDatum(oid);

	oracast_rel = table_open(OraCastRelationId, RowExclusiveLock);
	tuple = SearchSysCache3(ORACASTSOURCETARGET,
							sourcetyptype,
							targettyptype,
							castcontext);

	if (HeapTupleIsValid(tuple))
	{
		CatalogTupleDelete(oracast_rel, &tuple->t_self);
		ReleaseSysCache(tuple);
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
					errmsg("oracle convert not exist.")));

	table_close(oracast_rel, NoLock);
}


static Oid
TypenameGetTypOid(TypeName *typname, bool *find)
{
	char		*typeName;
	char		*schemaname;
	Oid			typeNamespace;
	Oid			typoid;


	/* deconstruct the name list */
	DeconstructQualifiedName(typname->names, &schemaname, &typeName);

	if (strcmp(typeName, "any") == 0)
	{
		*find = true;
		return (Oid) 0;
	}

	if (schemaname)
	{
		/* Convert list of names to a name and namespace */
		typeNamespace = QualifiedNameGetCreationNamespace(typname->names, &typeName);

		typoid = GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid,
								 CStringGetDatum(typeName),
								 ObjectIdGetDatum(typeNamespace));
	}
	else
	{
		/* Unqualified type name, so search the search path */
		typoid = TypenameGetTypid(typeName);
	}

	if (OidIsValid(typoid) && get_typisdefined(typoid))
	{
		*find = true;
		return typoid;
	}
	else
	{
		*find = false;
		return InvalidOid;
	}
}