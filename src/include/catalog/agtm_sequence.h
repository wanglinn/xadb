#ifndef AGTM_CATALOG_SEQUENCE_H
#define AGTM_CATALOG_SEQUENCE_H

#include "catalog/genbki.h"
#include "catalog/agtm_sequence_d.h"

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

extern Oid AddAgtmSequence(const char* database,
				const char* schema, const char* sequence);

extern Oid DelAgtmSequence(const char* database,
				const char* schema, const char* sequence);

extern void DelAgtmSequenceByOid(Oid oid);

extern List * DelAgtmSequenceByDatabse(const char* database);

extern bool SequenceIsExist(const char* database,
				const char* schema, const char* sequence);

extern Oid SequenceSystemClassOid(const char* database,
				const char* schema, const char* sequence);

extern void UpdateSequenceInfo(const char* database,
				const char* schema, const char* sequence, const char * value, AgtmNodeTag type);

extern void UpdateSequenceDbExist(const char* oldName, const char* newName);
#endif
