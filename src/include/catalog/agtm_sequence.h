#ifndef AGTM_CATALOG_SEQUENCE_H
#define AGTM_CATALOG_SEQUENCE_H

#include "catalog/genbki.h"

#include "agtm/agtm_msg.h"
#include "nodes/pg_list.h"

CATALOG(agtm_sequence,4053,AgtmSequenceRelationId)
{
	NameData database;
	NameData schema;
	NameData sequence;
} FormData_agtm_sequence;

/* ----------------
 *		Form_agtm_sequence corresponds to a pointer to a tuple with
 *		the format of agtm_sequence relation.
 * ----------------
 */
typedef FormData_agtm_sequence *Form_agtm_sequence;

extern Oid SequenceSystemClassOid(char* schema, char* sequence);

#endif
