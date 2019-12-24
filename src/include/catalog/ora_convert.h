#ifndef ORA_CONVERT_H
#define ORA_CONVERT_H

/* oracle grammar implicit convert */

#include "catalog/genbki.h"
#include "catalog/ora_convert_d.h"

struct OraImplicitConvertStmt;	/* avoid include parsenode.h */
void ExecImplicitConvert(struct OraImplicitConvertStmt *stmt);
#ifdef ADB
void ClusterExecImplicitConvert(StringInfo mem_toc);
#endif /* ADB */

CATALOG(ora_convert,6116,OraConvertRelationId) BKI_WITHOUT_OIDS
{
	/* implicit convert kind, ORA_CONVERT_KIND_XXX */
	char		cvtkind;

	NameData	cvtname;

	oidvector	cvtfrom BKI_LOOKUP(pg_type);

#ifdef CATALOG_VARLEN
	oidvector	cvtto BKI_LOOKUP(pg_type);
#endif
} FormData_ora_convert;

typedef FormData_ora_convert *Form_ora_convert;

#ifdef EXPOSE_TO_CLIENT_CODE

#define ORA_CONVERT_KIND_OPERATOR	'o'
#define ORA_CONVERT_KIND_FUNCTION	'f'
/*
 * common for combin decode,nvl2... functions
 * and UNION INTERSECT, MINUS Operators
 * cvtname is empty
 */
#define ORA_CONVERT_KIND_COMMON		'c'
/*
 * special functions
 * decode(arg, cmp1, ret1, cmp2, ret2, def) check ('s', 'decode', 'ret1 ret2')
 * 										 => check ('s', 'decode', 'pret def')
 */
#define ORA_CONVERT_KIND_SPECIAL_FUN 's'

#endif							/* EXPOSE_TO_CLIENT_CODE */

#endif