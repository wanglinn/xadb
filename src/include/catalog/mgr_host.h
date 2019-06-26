
#ifndef MGR_HOST_H
#define MGR_HOST_H

#include "catalog/genbki.h"
#include "catalog/mgr_host_d.h"

#define HostRelationId 4802

CATALOG(mgr_host,4802,HostRelationId)
{
	/* host name */
	NameData	hostname;

	/* host user */
	NameData	hostuser;

	/* host port */
	int32		hostport;

	/* host protocol of connection */
	char		hostproto;

	/* agent port */
	int32		hostagentport;

	/* a flag that indication "adb doctor extension" whether work or not. */
	bool		allowcure;

#ifdef CATALOG_VARLEN
	/* host address */
	text		hostaddr;

	/*host home*/
	text		hostadbhome;
#endif /* CATALOG_VARLEN */
} FormData_mgr_host;

/* ----------------
 *		Form_mgr_host corresponds to a pointer to a tuple with
 *		the format of mgr_host relation.
 * ----------------
 */
typedef FormData_mgr_host *Form_mgr_host;

#ifdef EXPOSE_TO_CLIENT_CODE

#define HOST_PROTOCOL_TELNET			't'
#define HOST_PROTOCOL_SSH				's'
#define AGENTDEFAULTPORT				5430

#endif							/* EXPOSE_TO_CLIENT_CODE */

#endif /* MGR_HOST_H */
