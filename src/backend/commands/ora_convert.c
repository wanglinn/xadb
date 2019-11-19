/*-------------------------------------------------------------------------
 *
 * ora_convert.c
 * 
 *		Oracle implicit conversion
 *		Management of implicit conversion of different functions or 
 *		expression value parameters in Oracle syntax.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"


#include "access/relscan.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/ora_convert.h"
#include "nodes/parsenodes.h"

#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"



void ExecImplicitConvert(OraImplicitConvertStmt *stmt);



void ExecImplicitConvert(OraImplicitConvertStmt *stmt)
{
	Relation			convert_rel;
	TupleDesc			rel_dsc;
	HeapScanDesc		rel_scan;
	ScanKeyData			key[2];
	HeapTuple			tuple, newtuple;
	Form_ora_convert	ora_convert;
	oidvector			*oldcvtfrom;
	oidvector			*oldcvtto;
	oidvector			*newcvtfrom;
	oidvector			*newcvtto;
	int					cvtfromList_count = 0;
	int					cvttoList_count = 0;
	Datum				cvttoDatum;
	bool				isNull;
	int					i;
	bool				compareCvtfrom = true;
	bool				compareCvtto = true;


	cvtfromList_count = list_length(stmt->cvtfrom);
	
	if (stmt->cvtto)
		cvttoList_count = list_length(stmt->cvtto);
	
	if (cvtfromList_count < 1  || (stmt->action != ICONVERT_DELETE && cvttoList_count < 1))
		elog(ERROR, "The number of unqualified parameters or inconsistent parameters.");

	/* Check parameter */
	if (stmt->action != ICONVERT_DELETE && cvtfromList_count != cvttoList_count && 
		(stmt->cvtkind == ORA_CONVERT_KIND_FUNCTION || stmt->cvtkind == ORA_CONVERT_KIND_OPERATOR))
		elog(ERROR, "The number of unqualified parameters or inconsistent parameters.");
	else
	{
		Oid			*fromOids, *toOids;
		ListCell	*cell;

		fromOids = palloc(cvtfromList_count * sizeof(Oid));
		cell = list_head(stmt->cvtfrom);
		for (i = 0; i < cvtfromList_count; i++)
		{
			fromOids[i] = TypenameGetTypid((char*)lfirst(cell));
			if (fromOids[i] == InvalidOid)
				elog(ERROR, "Added data type not included.");
			cell = lnext(cell);
		}
		newcvtfrom = buildoidvector(fromOids, cvtfromList_count);
		pfree(fromOids);

		if (stmt->cvtto)
		{
			toOids = palloc(cvttoList_count * sizeof(Oid));
			cell = list_head(stmt->cvtto);
			for (i = 0; i < cvttoList_count; i++)
			{
				toOids[i] = TypenameGetTypid((char*)lfirst(cell));
				if (toOids[i] == InvalidOid)
					elog(ERROR, "Added data type not included.");
				cell = lnext(cell);
			}
			newcvtto = buildoidvector(toOids, cvttoList_count);
			pfree(toOids);
		}
	}

	Assert(stmt->cvtname);
	ScanKeyInit(&key[0]
			,Anum_ora_convert_cvtkind
			,BTEqualStrategyNumber
			,F_CHAREQ
			,CharGetDatum(stmt->cvtkind));
	ScanKeyInit(&key[1]
			,Anum_ora_convert_cvtname
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,CStringGetDatum(stmt->cvtname));
			
	convert_rel = heap_open(OraConvertRelationId, RowExclusiveLock);
	rel_scan = heap_beginscan_catalog(convert_rel, 2, key);
	rel_dsc = RelationGetDescr(convert_rel);

	
	while ((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		ora_convert= (Form_ora_convert)GETSTRUCT(tuple);
		Assert(ora_convert);

		/* get tuple cvtfrom */
		oldcvtfrom = &(ora_convert->cvtfrom);

		/* get tuple cvtto */
		cvttoDatum = heap_getattr(tuple, Anum_ora_convert_cvtto, rel_dsc, &isNull);
		if (isNull)
		{
			heap_endscan(rel_scan);
			heap_close(convert_rel, RowExclusiveLock);
			elog(ERROR, "column cvtto is null.");
			return;
		}
		oldcvtto = (oidvector *) DatumGetPointer(cvttoDatum);

		/* compare cvtfrom */
		if (newcvtfrom->dim1 == oldcvtfrom->dim1)
		{
			for (i = 0; i < cvtfromList_count; i++)
			{
				if (oid_cmp(&(newcvtfrom->values[i]), &(oldcvtfrom->values[i])) != 0)
				{
					compareCvtfrom = false;
					break;
				}
			}
		}
		else
			compareCvtfrom = false;
		
		if (stmt->cvtto)
		{
			/* compare cvtto */
			if (newcvtto->dim1 == oldcvtto->dim1)
			{
				for (i = 0; i < cvttoList_count; i++)
				{
					if (oid_cmp(&(newcvtto->values[i]), &(oldcvtto->values[i])) != 0)
					{
						compareCvtto = false;
						break;
					}
				}
			}
			else
				compareCvtto = false;
		}

		/* 
		 * we think that the 'cvtfrom' value of the same function name is unique.
		 * Record found ?
		 */
		if (compareCvtfrom)
			break;
		else
			compareCvtfrom = compareCvtto = true;
	}
	
	switch (stmt->action)
	{
		case ICONVERT_CREATE:
			{
				Datum		datum[Natts_ora_convert];
				bool		nulls[Natts_ora_convert];

				datum[Anum_ora_convert_cvtkind -1] = CharGetDatum(stmt->cvtkind);
				datum[Anum_ora_convert_cvtname -1] = CharGetDatum(stmt->cvtname);
				datum[Anum_ora_convert_cvtfrom -1] = PointerGetDatum(newcvtfrom);
				datum[Anum_ora_convert_cvtto -1] = PointerGetDatum(newcvtto);
				nulls[0] = nulls[1] = nulls[2] = nulls[3] = false;
				newtuple = heap_form_tuple(rel_dsc, datum, nulls);
				CatalogTupleInsert(convert_rel, newtuple);
				heap_freetuple(newtuple);
			}
			break;
		case ICONVERT_UPDATE:
			if (tuple && compareCvtfrom)
			{
				Datum		datum[Natts_ora_convert];
				bool		nulls[Natts_ora_convert];
				
				datum[Anum_ora_convert_cvtkind -1] = CharGetDatum(stmt->cvtkind);
				datum[Anum_ora_convert_cvtname -1] = CharGetDatum(stmt->cvtname);
				datum[Anum_ora_convert_cvtfrom -1] = PointerGetDatum(newcvtfrom);
				datum[Anum_ora_convert_cvtto -1] = PointerGetDatum(newcvtto);
				nulls[0] = nulls[1] = nulls[2] = nulls[3] = false;
				newtuple = heap_form_tuple(rel_dsc, datum, nulls);
				CatalogTupleUpdate(convert_rel, &tuple->t_self, newtuple);
				heap_freetuple(newtuple);
			}
			else
			{
				heap_endscan(rel_scan);
				heap_close(convert_rel, RowExclusiveLock);
				elog(ERROR, "UPDATE 0");
			}
			
			break;
		case ICONVERT_DELETE:
			if (tuple && compareCvtfrom)
			{
				CatalogTupleDelete(convert_rel, &tuple->t_self);
			}
			else
			{
				if (!stmt->if_exists)
				{
					heap_endscan(rel_scan);
					heap_close(convert_rel, RowExclusiveLock);
					elog(ERROR, "DELETE 0");
				}
			}
			
			break;
		default:
			elog(WARNING, "unrecognized commandType: %d",
					(int) stmt->action);
			break;
	}
	heap_endscan(rel_scan);
	heap_close(convert_rel, RowExclusiveLock);
}