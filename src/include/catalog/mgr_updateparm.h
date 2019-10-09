
#ifndef MGR_UPDATEPARM_H
#define MGR_UPDATEPARM_H

#include "catalog/genbki.h"
#include "catalog/mgr_updateparm_d.h"

CATALOG(mgr_updateparm,4801,UpdateparmRelationId) BKI_WITHOUT_OIDS
{
	/* updateparm nodename */
	NameData	updateparmnodename;

	char		updateparmnodetype;

	NameData	updateparmkey;

	text	updateparmvalue;
} FormData_mgr_updateparm;

/* ----------------
 *		Form_mgr_updateparm corresponds to a pointer to a tuple with
 *		the format of mgr_updateparm relation.
 * ----------------
 */
typedef FormData_mgr_updateparm *Form_mgr_updateparm;

#ifdef EXPOSE_TO_CLIENT_CODE

#define MACRO_STAND_FOR_ALL_NODENAME "*"

#endif							/* EXPOSE_TO_CLIENT_CODE */

#endif /* MGR_UPDATEPARM_H */
