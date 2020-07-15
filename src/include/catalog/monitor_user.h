
#ifndef MONITOR_USER_H
#define MONITOR_USER_H

#include "catalog/genbki.h"
#include "catalog/monitor_user_d.h"

#include "utils/timestamp.h"

CATALOG(monitor_user,9798,MuserRelationId)
{
	Oid			oid;
	
	/*the user name*/
	NameData				username;

	/*1: ordinary users, 2: db manager*/
	int32					usertype;

	timestamptz				userstarttime;

	timestamptz				userendtime;

	NameData				usertel;

	NameData				useremail;

	NameData				usercompany;

	NameData				userdepart;

	NameData				usertitle;

#ifdef CATALOG_VARLEN

	text						userpassword;

	/*plan for the query*/
	text						userdesc;
#endif /* CATALOG_VARLEN */
} FormData_monitor_user;

/* ----------------
 *		FormData_monitor_user corresponds to a pointer to a tuple with
 *		the format of Form_monitor_user relation.
 * ----------------
 */
typedef FormData_monitor_user *Form_monitor_user;

#endif /* MONITOR_USER_H */
