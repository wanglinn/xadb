#ifndef ADB_SLOT_H
#define ADB_SLOT_H

#include "catalog/genbki.h"
#include "catalog/adb_slot_d.h"

CATALOG(adb_slot,9030,AdbSlotRelationId) BKI_SHARED_RELATION
{
	int32		slotid;
	int32		slotstatus;
	NameData	slotnodename;
}FormData_adb_slot;

typedef FormData_adb_slot *Form_adb_slot;

#endif							/* ADB_SLOT_H */