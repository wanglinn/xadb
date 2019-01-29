#ifndef ADB_SLOT_H
#define ADB_SLOT_H

#ifdef BUILD_BKI
#include "catalog/buildbki.h"
#else /* BUILD_BKI */
#include "catalog/genbki.h"
#endif /* BUILD_BKI */

#define AdbSlotRelationId		9030

CATALOG(adb_slot,9030) BKI_SHARED_RELATION BKI_WITHOUT_OIDS
{
	int32		slotid;
	int32		slotstatus;
	NameData	slotnodename;
}FormData_adb_slot;

typedef FormData_adb_slot *Form_adb_slot;

#define Natts_adb_slot			3

#define Anum_adb_slotid			1
#define Anum_adb_slotstatus		2
#define Anum_adb_slotnodename	3

#endif							/* ADB_SLOT_H */