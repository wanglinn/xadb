
#ifndef MGR_HBA_H
#define MGR_HBA_H

#include "catalog/genbki.h"
#include "catalog/mgr_hba_d.h"

CATALOG(mgr_hba,4800,HbaRelationId)
{
	Oid			oid;

	/* node name */
	NameData	nodename;

	/* storing a line of pg_hba.conf */
	text		hbavalue;
} FormData_mgr_hba;

/* ----------------
 *		Form_mgr_updateparm corresponds to a pointer to a tuple with
 *		the format of mgr_updateparm relation.
 * ----------------
 */
typedef FormData_mgr_hba *Form_mgr_hba;


#endif /* MGR_HBA_H */
