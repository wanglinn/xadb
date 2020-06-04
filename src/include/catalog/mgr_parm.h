
#ifndef MGR_PARM_H
#define MGR_PARM_H

#include "catalog/genbki.h"
#include "catalog/mgr_parm_d.h"

CATALOG(mgr_parm,4812,ParmRelationId)
{
	/* parm type:c/d/g/'*' for all/'#' for datanode and coordinator*/
	char		parmtype;

	/* parm name */
	NameData	parmname;

	/* parm value */
	NameData	parmvalue;

	/*backend, user, internal, postmaster, superuser, sighup*/
	NameData	parmcontext;

	/*bool, enum, string, integer, real*/
	NameData	parmvartype;

#ifdef CATALOG_VARLEN
	/* param unit */
	text		parmunit;

	/* param minimal value */
	text		parmminval;

	/* param maximal value */
	text		parmmaxval;

	/* param enumerate value */
	text		parmenumval;
#endif
} FormData_mgr_parm;

/* ----------------
 *		Form_mgr_parm corresponds to a pointer to a tuple with
 *		the format of mgr_parm relation.
 * ----------------
 */
typedef FormData_mgr_parm *Form_mgr_parm;

#ifdef EXPOSE_TO_CLIENT_CODE

#define PARM_TYPE_GTMCOOR 			'G'
#define PARM_TYPE_COORDINATOR		'C'
#define PARM_TYPE_DATANODE			'D'

#endif							/* EXPOSE_TO_CLIENT_CODE */

#endif /* MGR_PARM_H */
