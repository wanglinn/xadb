/*-------------------------------------------------------------------------
 *
 * oracoerce.c
 *	  try to coerce from source type to target type with oracle grammar.
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2014-2017, ADB Development Group of ASIA
 *
 * IDENTIFICATION
 *	  src/backend/oraschema/oracoerce.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "nodes/makefuncs.h"
#include "catalog/namespace.h"
#include "nodes/nodeFuncs.h"
#include "oraschema/oracoerce.h"
#include "parser/parse_coerce.h"
#include "parser/parse_oper.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

/* Data Type Precedence
 * Oracle uses data type precedence to determine implicit data type conversion,
 * which is discussed in the section that follows. Oracle data types take the
 * following precedence:
 *
 *		Datetime and interval data types
 *		BINARY_DOUBLE
 *		BINARY_FLOAT
 *		NUMBER
 *		Character data types
 *		All other built-in data types
 */
typedef enum OraTypePrecedence
{
	LV_LEAST,
	LV_CHARACTER,
	LV_NUMBER,
	LV_FLOAT,
	LV_DOUBLE,
	LV_DATETIME_INTERVAL
} OraTypePrecedence;

typedef struct OraPrecedence
{
	Oid	typoid;
	int	level;
} OraPrecedence;

typedef enum OraOperKind
{
	ORA_OPER_UNKNOWN,
	ORA_OPER_ARITHMETIC,
	ORA_OPER_PATTERN,
} OraOperKind;

#define ORA_MIN(A, B) 					((B) < (A) ? (B) : (A))
#define ORA_MAX(A, B) 					((B) > (A) ? (B) : (A))
#define ORA_PRECEDENCE(a, b) 			{(a), (b)},
#define IsArithmeticOperator(opname)	(GetOraOperationKind((opname)) == ORA_OPER_ARITHMETIC)
#define IsPatternOperator(opname)		(GetOraOperationKind((opname)) == ORA_OPER_PATTERN)

static const OraPrecedence OraPrecedenceMap[] =
{
	ORA_PRECEDENCE(TIMESTAMPTZOID, LV_DATETIME_INTERVAL)
	ORA_PRECEDENCE(TIMESTAMPOID, LV_DATETIME_INTERVAL)
	ORA_PRECEDENCE(ORADATEOID, LV_DATETIME_INTERVAL)
	ORA_PRECEDENCE(INTERNALOID, LV_DATETIME_INTERVAL)
	ORA_PRECEDENCE(FLOAT8OID, LV_DOUBLE)
	ORA_PRECEDENCE(FLOAT4OID, LV_FLOAT)
	ORA_PRECEDENCE(NUMERICOID, LV_NUMBER)
	ORA_PRECEDENCE(TEXTOID, LV_CHARACTER)
	ORA_PRECEDENCE(VARCHAROID, LV_CHARACTER)
	ORA_PRECEDENCE(ORACLE_VARCHAR2OID, LV_CHARACTER)
	ORA_PRECEDENCE(ORACLE_NVARCHAR2OID, LV_CHARACTER)
	ORA_PRECEDENCE(BPCHAROID, LV_CHARACTER)
	ORA_PRECEDENCE(CHAROID, LV_CHARACTER)
};

static const int NumOraPrecedenceMap = lengthof(OraPrecedenceMap);

static OraCoercionContext CurrentCoercionContext = ORA_COERCE_DEFAULT;

static Oid GetOraCoercionFuncID(Oid srctype,
								Oid targettype,
								char context,
								bool needtrunc);
static OraOperKind GetOraOperationKind(List *opname);
static int GetOraPrecedence(Oid typoid);
static int OraTypePerferred(Oid typoid1, Oid typoid2);
static void TryOraOperatorCoercion(ParseState *pstate,	/* in */
								   List *opname,		/* in */
								   Node *lexpr,			/* in */
								   Node *rexpr,			/* in */
								   CoerceDrct drct,		/* in */
								   Node **ret_lexpr,	/* out */
								   Node **ret_rexpr		/* out */
								   );
static Oid OraFunctionCoercion(Oid srctype, Oid targettype, bool needtrunc);
static Oid OraOperatorCoercion(Oid srctype, Oid targettype, bool needtrunc);

OraCoercionContext
OraCoercionContextSwitchTo(OraCoercionContext context)
{
	OraCoercionContext	old = CurrentCoercionContext;

	AssertArg(OraCoercionContextIsValid(context));
	CurrentCoercionContext = context;

	return old;
}

bool
IsOraFunctionCoercionContext(void)
{
	switch (CurrentCoercionContext)
	{
		case ORA_COERCE_COMMON_FUNCTION:
		case ORA_COERCE_SPECIAL_FUNCTION:
			return true;
		default:
			break;
	}
	return false;
}

static Oid
GetOraCoercionFuncID(Oid srctype,
					 Oid targettype,
					 char context,
					 bool needtrunc)
{
	HeapTuple		tuple;
	Form_ora_cast	castForm;
	Oid				castFunc;

	tuple = SearchSysCache3(ORACASTSOURCETARGET,
							ObjectIdGetDatum(srctype),
							ObjectIdGetDatum(targettype),
							CharGetDatum(context));
	if (!HeapTupleIsValid(tuple))
		return InvalidOid;			/* no cast */
	castForm = (Form_ora_cast) GETSTRUCT(tuple);

	if (needtrunc)
		castFunc = castForm->casttruncfunc;
	else
		castFunc = castForm->castfunc;

	ReleaseSysCache(tuple);

	return castFunc;
}

Oid
OraFindCoercionFunction(Oid srctype, Oid targettype)
{
	Oid		castFunc = InvalidOid;

	switch (CurrentCoercionContext)
	{
		case ORA_COERCE_NOUSE:
			return InvalidOid;
		case ORA_COERCE_COMMON_FUNCTION:
			castFunc = GetOraCoercionFuncID(srctype,
											targettype,
											ORA_COERCE_COMMON_FUNCTION,
											true);
			break;
		case ORA_COERCE_SPECIAL_FUNCTION:
			castFunc = GetOraCoercionFuncID(srctype,
											targettype,
											ORA_COERCE_COMMON_FUNCTION,
											false);
			break;
		case ORA_COERCE_OPERATOR:
			castFunc = GetOraCoercionFuncID(srctype,
											targettype,
											ORA_COERCE_OPERATOR,
											false);
			break;
		case ORA_COERCE_DEFAULT:
			break;
		default:
			Assert(0);
			break;
	}

	if (!OidIsValid(castFunc))
		castFunc = GetOraCoercionFuncID(srctype,
										targettype,
										ORA_COERCE_DEFAULT,
										false);

	return castFunc;
}

static OraOperKind
GetOraOperationKind(List *opname)
{
	char		*nspname = NULL;
	char		*objname = NULL;
	int			 objlen = 0;
	OraOperKind	 result = ORA_OPER_UNKNOWN;

	DeconstructQualifiedName(opname, &nspname, &objname);

	if (objname)
		objlen = strlen(objname);

	switch (objlen)
	{
		case 1:
			if (strncmp(objname, "+", objlen) == 0 ||
				strncmp(objname, "-", objlen) == 0 ||
				strncmp(objname, "*", objlen) == 0 ||
				strncmp(objname, "/", objlen) == 0)
				result = ORA_OPER_ARITHMETIC;
			break;
		case 2:
			if (strncmp(objname, "~~", objlen) == 0)
				result = ORA_OPER_PATTERN;
			break;
		case 3:
			if (strncmp(objname, "!~~", objlen) == 0)
				result = ORA_OPER_PATTERN;
			break;
		default:
			break;
	}

	return result;
}

static void
TryOraOperatorCoercion(ParseState *pstate,	/* in */
					   List *opname,		/* in */
					   Node *lexpr,			/* in */
					   Node *rexpr,			/* in */
					   CoerceDrct drct,		/* in */
					   Node **ret_lexpr,	/* out */
					   Node **ret_rexpr		/* out */
					   )
{
	Oid 	ltypeId = InvalidOid;
	Oid 	rtypeId = InvalidOid;
	Oid 	coerce_func_oid = InvalidOid;

	Assert(ret_lexpr && ret_rexpr);
	if (!IsOracleParseGram(pstate) ||
		!lexpr ||
		!rexpr)
	{
		*ret_lexpr = lexpr;
		*ret_rexpr = rexpr;
		return ;
	}

	ltypeId = exprType(lexpr);
	rtypeId = exprType(rexpr);

	/*
	 * convert character data to numeric data
	 * during arithmetic operations.
	 */
	if (IsArithmeticOperator(opname))
	{
		if (ltypeId == UNKNOWNOID)
			*ret_lexpr = coerce_to_common_type(pstate, lexpr, NUMERICOID, "ORACLE OPERATOR");
		else
		if (TypeCategory(ltypeId) == TYPCATEGORY_STRING)
		{
			coerce_func_oid = OraOperatorCoercion(ltypeId, NUMERICOID, false);
			if (OidIsValid(coerce_func_oid))
				*ret_lexpr = (Node *)makeFuncExpr(coerce_func_oid,
												  get_func_rettype(coerce_func_oid),
												  list_make1(lexpr),
												  InvalidOid,
												  InvalidOid,
												  COERCE_EXPLICIT_CALL);
			else
				*ret_lexpr = lexpr;
		} else
			*ret_lexpr = lexpr;

		if (rtypeId == UNKNOWNOID)
			*ret_rexpr = coerce_to_common_type(pstate, rexpr, NUMERICOID, "ORACLE OPERATOR");
		else
		if (TypeCategory(rtypeId) == TYPCATEGORY_STRING)
		{
			coerce_func_oid = OraOperatorCoercion(rtypeId, NUMERICOID, false);
			if (OidIsValid(coerce_func_oid))
				*ret_rexpr = (Node *)makeFuncExpr(coerce_func_oid,
												  get_func_rettype(coerce_func_oid),
												  list_make1(rexpr),
												  InvalidOid,
												  InvalidOid,
												  COERCE_EXPLICIT_CALL);
			else
				*ret_rexpr = rexpr;
		} else
			*ret_rexpr = rexpr;

		return ;
	}

	/*
	 * convert numeric data to character data
	 * during pattern operations.
	 */
	if (IsPatternOperator(opname) &&
		(TypeCategory(ltypeId) != TYPCATEGORY_STRING ||
		 TypeCategory(rtypeId) != TYPCATEGORY_STRING))
	{
		coerce_func_oid = OraFunctionCoercion(ltypeId, TEXTOID, false);
		if (OidIsValid(coerce_func_oid))
			*ret_lexpr = (Node *)makeFuncExpr(coerce_func_oid,
											  get_func_rettype(coerce_func_oid),
											  list_make1(lexpr),
											  InvalidOid,
											  InvalidOid,
											  COERCE_EXPLICIT_CALL);
		else
			*ret_lexpr = lexpr;

		coerce_func_oid = OraFunctionCoercion(rtypeId, TEXTOID, false);
		if (OidIsValid(coerce_func_oid))
			*ret_rexpr = (Node *)makeFuncExpr(coerce_func_oid,
											  get_func_rettype(coerce_func_oid),
											  list_make1(rexpr),
											  InvalidOid,
											  InvalidOid,
											  COERCE_EXPLICIT_CALL);
		else
			*ret_rexpr = rexpr;

		return ;
	}

	if (ltypeId == rtypeId)
	{
		*ret_lexpr = lexpr;
		*ret_rexpr = rexpr;
		return ;
	}

	/*
	 * try to coerce left to right.
	 */
	if (drct == ORA_COERCE_L2R ||
		drct == ORA_COERCE_S2S)
	{
		coerce_func_oid = OraOperatorCoercion(ltypeId, rtypeId, false);
		if (OidIsValid(coerce_func_oid))
		{
			*ret_lexpr = (Node *)makeFuncExpr(coerce_func_oid,
											  get_func_rettype(coerce_func_oid),
											  list_make1(lexpr),
											  InvalidOid,
											  InvalidOid,
											  COERCE_EXPLICIT_CALL);
			*ret_rexpr = rexpr;
			return ;
		}
	}

	/*
	 * try to coerce right to left.
	 */
	if (drct == ORA_COERCE_R2L ||
		drct == ORA_COERCE_S2S)
	{
		coerce_func_oid = OraOperatorCoercion(rtypeId, ltypeId, false);
		if (OidIsValid(coerce_func_oid))
		{
			*ret_lexpr = lexpr;
			*ret_rexpr = (Node *)makeFuncExpr(coerce_func_oid,
											  get_func_rettype(coerce_func_oid),
											  list_make1(rexpr),
											  InvalidOid,
											  InvalidOid,
											  COERCE_EXPLICIT_CALL);
			return ;
		}
	}

	/*
	 * can not coerce from any side to side
	 * return default.
	 */
	*ret_lexpr = lexpr;
	*ret_rexpr = rexpr;
	return ;
}

static int
GetOraPrecedence(Oid typoid)
{
	int i;
	for (i = 0; i < NumOraPrecedenceMap; i++)
	{
		if (typoid == OraPrecedenceMap[i].typoid)
			return OraPrecedenceMap[i].level;
	}
	return LV_LEAST;
}

static int
OraTypePerferred(Oid typoid1, Oid typoid2)
{
	return GetOraPrecedence(typoid1) - GetOraPrecedence(typoid2);
}

List *
transformNvlArgs(ParseState *pstate, List *args)
{
	List 	   *result = NIL;
	Node 	   *larg = NULL,
			   *rarg = NULL,
			   *cnode = NULL;
	Oid		   ltypid = InvalidOid,
			   rtypid = InvalidOid;
	int32	   ltypmod = -1,
			   rtypmod = -1,
			   typmod = -1;

	Assert(pstate && args);
	Assert(list_length(args) == 2);

	larg = (Node *)linitial(args);
	rarg = (Node *)lsecond(args);
	if (exprType(larg) == UNKNOWNOID)
		larg = coerce_to_common_type(pstate, larg, TEXTOID, "NVL");
	if (exprType(rarg) == UNKNOWNOID)
		rarg = coerce_to_common_type(pstate, rarg, TEXTOID, "NVL");
	ltypid = exprType(larg);
	rtypid = exprType(rarg);
	ltypmod = exprTypmod(larg);
	rtypmod = exprTypmod(rarg);
	typmod = ORA_MAX(ltypmod, rtypmod);

	if (ltypid == rtypid)
	{
		if (ltypmod == rtypmod)
			return list_make2(larg, rarg);

		cnode = coerce_to_target_type(pstate,
									  larg,
									  ltypid,
									  ltypid,
									  typmod,
									  COERCION_IMPLICIT,
									  COERCE_IMPLICIT_CAST,
									  -1);
		if (!cnode)
			cnode = larg;
		result = lappend(result, cnode);

		cnode = coerce_to_target_type(pstate,
									  rarg,
									  rtypid,
									  ltypid,
									  typmod,
									  COERCION_IMPLICIT,
									  COERCE_IMPLICIT_CAST,
									  -1);
		if (!cnode)
			cnode = rarg;
		result = lappend(result, cnode);

		return result;
	}

	/*
	* If expr1 is character data, then Oracle Database converts expr2 to the
	* data type of expr1 before comparing them and returns VARCHAR2 in the
	* character set of expr1.
	*
	* If expr1 is numeric, then Oracle Database determines which argument has
	* the highest numeric precedence, implicitly converts the other argument
	* to that data type, and returns that data type
	*/
	if (TypeCategory(ltypid) == TYPCATEGORY_STRING ||
		OraTypePerferred(ltypid, rtypid) > 0)
	{
		if (can_coerce_type(1, &rtypid, &ltypid, COERCION_IMPLICIT))
		{
			cnode = coerce_to_target_type(pstate,
										  rarg,
										  rtypid,
										  ltypid,
										  ltypmod,
										  COERCION_IMPLICIT,
										  COERCE_IMPLICIT_CAST,
										  -1);
			if (!cnode)
				cnode = rarg;
			result = lappend(result, larg);
			result = lappend(result, cnode);

			return result;
		}
		return list_make2(larg, rarg);
	}

	/*
	* right expr has highest numeric precedence
	*/
	if (OraTypePerferred(ltypid, rtypid) < 0)
	{
		if (can_coerce_type(1, &ltypid, &rtypid, COERCION_IMPLICIT))
		{
			cnode = coerce_to_target_type(pstate,
										  larg,
										  ltypid,
										  rtypid,
										  rtypmod,
										  COERCION_IMPLICIT,
										  COERCE_IMPLICIT_CAST,
										  -1);
			if (!cnode)
				cnode = larg;
			result = lappend(result, cnode);
			result = lappend(result, rarg);

			return result;
		}
		return list_make2(larg, rarg);
	}

	/*
	* left expr and right expr have the same numeric precedence
	* try to implicitly converts to the data type of the right expr.
	*/
	if (can_coerce_type(1, &ltypid, &rtypid, COERCION_IMPLICIT))
	{
		cnode = coerce_to_target_type(pstate,
									  larg,
									  ltypid,
									  rtypid,
									  rtypmod,
									  COERCION_IMPLICIT,
									  COERCE_IMPLICIT_CAST,
									  -1);
		if (!cnode)
			cnode = larg;
		result = lappend(result, cnode);
		result = lappend(result, rarg);

		return result;
	}

	/*
	* left expr and right expr have the same numeric precedence
	* try to implicitly converts to the data type of the left expr.
	*/
	if (can_coerce_type(1, &rtypid, &ltypid, COERCION_EXPLICIT))
	{
		cnode = coerce_to_target_type(pstate,
									  rarg,
									  rtypid,
									  ltypid,
									  ltypmod,
									  COERCION_EXPLICIT,
									  COERCE_EXPLICIT_CALL,
									  -1);
		if (!cnode)
			cnode = rarg;
		result = lappend(result, larg);
		result = lappend(result, cnode);

		return result;
	}

	return list_make2(larg, rarg);
}

List *
transformNvl2Args(ParseState *pstate, List *args)
{
	List *result = NIL;
	void *arg1 = NULL,
		 *arg2 = NULL,
		 *arg3 = NULL;

	Assert(pstate && args);
	Assert(list_length(args) == 3);

	arg1 = linitial(args);
	arg2 = lsecond(args);
	arg3 = lthird(args);

	if (exprType(arg2) == UNKNOWNOID)
		arg2 = coerce_to_common_type(pstate, (Node *)arg2, TEXTOID, "NVL2");
	if (exprType(arg3) == UNKNOWNOID)
		arg3 = coerce_to_common_type(pstate, (Node *)arg3, TEXTOID, "NVL2");

	result = lcons(arg1, transformNvlArgs(pstate, list_make2(arg2, arg3)));

	return result;
}

Node *
transformOraAExprOp(ParseState * pstate,
					List *opname,
					Node *lexpr,
					Node *rexpr,
					int location)
{
	Node			   *result = NULL;
	OraCoercionContext	oldContext;

	Assert(pstate);
	Assert(lexpr && rexpr);
	Assert(IsOracleParseGram(pstate));

	oldContext = OraCoercionContextSwitchTo(ORA_COERCE_NOUSE);
	result = (Node *) make_op2(pstate,
							   opname,
							   lexpr,
							   rexpr,
							   pstate->p_last_srf,
							   location,
							   true);
	if (result)
	{
		(void) OraCoercionContextSwitchTo(oldContext);
		return result;
	}

	PG_TRY();
	{
		if (IsA(lexpr, CaseTestExpr))
			TryOraOperatorCoercion(pstate,
								   opname,
								   lexpr,
								   rexpr,
								   ORA_COERCE_L2R,
								   &lexpr,
								   &rexpr);
		else
			TryOraOperatorCoercion(pstate,
								   opname,
								   lexpr,
								   rexpr,
								   ORA_COERCE_S2S,
								   &lexpr,
								   &rexpr);

		result = (Node *) make_op(pstate,
								  opname,
								  lexpr,
								  rexpr,
								  pstate->p_last_srf,
								  location);
	} PG_CATCH();
	{
		(void) OraCoercionContextSwitchTo(oldContext);
		PG_RE_THROW();
	} PG_END_TRY();
	(void) OraCoercionContextSwitchTo(oldContext);

	return result;
}

static Oid
OraFunctionCoercion(Oid srctype, Oid targettype, bool needtrunc)
{
	Oid castFunc;

	castFunc = GetOraCoercionFuncID(srctype,
									targettype,
									ORA_COERCE_COMMON_FUNCTION,
									needtrunc);
	if (!OidIsValid(castFunc))
		castFunc = GetOraCoercionFuncID(srctype,
										targettype,
										ORA_COERCE_DEFAULT,
										needtrunc);
	return castFunc;
}

static Oid
OraOperatorCoercion(Oid srctype, Oid targettype, bool needtrunc)
{
	Oid castFunc;

	castFunc = GetOraCoercionFuncID(srctype,
									targettype,
									ORA_COERCE_OPERATOR,
									needtrunc);
	if (!OidIsValid(castFunc))
		castFunc = GetOraCoercionFuncID(srctype,
										targettype,
										ORA_COERCE_DEFAULT,
										needtrunc);
	return castFunc;
}
