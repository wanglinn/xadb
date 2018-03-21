/*-------------------------------------------------------------------------
 *
 * oracoerce.h
 *		Coerce routines for Oracle grammar.
 *
 * Copyright (c) 2014-2017, ADB Development Group
 *
 * IDENTIFICATION
 *		src/include/oraschema/oracoerce.h
 *
 */
#ifndef ORA_COERCE_H
#define ORA_COERCE_H

#include "catalog/ora_cast.h"
#include "parser/parse_node.h"

#define IS_CHARACTER_TYPE(typ)	\
	((typ) == TEXTOID ||		\
	 (typ) == VARCHAROID ||		\
	 (typ) == VARCHAR2OID ||	\
	 (typ) == BPCHAROID ||		\
	 (typ) == NVARCHAR2OID ||	\
	 (typ) == CHAROID)

#define IS_NUMERIC_TYPE(typ)	\
	((typ) == INT2OID ||		\
	 (typ) == INT4OID ||		\
	 (typ) == INT8OID ||		\
	 (typ) == FLOAT4OID ||		\
	 (typ) == FLOAT8OID ||		\
	 (typ) == NUMERICOID)

typedef enum CoerceDrct
{
	ORA_COERCE_L2R,				/* coerce from left to right */
	ORA_COERCE_R2L,				/* coerce from right to left */
	ORA_COERCE_S2S				/* coerce from side to side */
} CoerceDrct;

extern OraCoercionContext OraCoercionContextSwitchTo(OraCoercionContext context);
extern bool IsOraFunctionCoercionContext(void);
extern Oid OraFindCoercionFunction(Oid sourceTypeId, Oid targetTypeId);
extern List *transformNvlArgs(ParseState *pstate, List *args);
extern List *transformNvl2Args(ParseState *pstate, List *args);
extern Node *transformOraAExprOp(ParseState * pstate,
								 List *opname,
								 Node *lexpr,
								 Node *rexpr,
								 int location);
#endif /* ORA_COERCE_H */
