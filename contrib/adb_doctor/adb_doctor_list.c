/*--------------------------------------------------------------------------
 *
 * Copyright (c) 2018-2019, Asiainfo Database Innovation Lab
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "adb_doctor_list.h"

void pfreeAdbDoctorList(AdbDoctorList *src, bool freeData)
{
	dlist_mutable_iter miter;
	AdbDoctorLink *link;

	if (src == NULL)
		return;

	dlist_foreach_modify(miter, &src->head)
	{
		link = dlist_container(AdbDoctorLink, wi_links, miter.cur);
		deleteFromAdbDoctorList(src, miter);
		pfreeAdbDoctorLink(link, freeData);
	}
	pfree(src);
	src = NULL;
}

/*
 * freeData==true means pfree inner linked data.
 * if function pfreeData is NULL, call pfree(data) directly.
 */
void pfreeAdbDoctorLink(AdbDoctorLink *src, bool freeData)
{
	if (src == NULL)
		return;
	if (freeData)
	{
		if (src->pfreeData == NULL)
		{
			ereport(DEBUG1,
					(errmsg("Function pfreeData is NULL, may cause memory leakage! ")));
			pfree(src->data);
		}
		else
		{
			src->pfreeData(src->data);
		}
	}
	pfree(src);
	src = NULL;
}

/* 
 * append all the data linked in src to dest. 
 * if drain=true, drain all the data in src, then pfree it.
 * if drain=false, copy out the data from src and the append to dest.
 */
void appendAdbDoctorList(AdbDoctorList *destList, AdbDoctorList *srcList, bool drain)
{
	dlist_mutable_iter miter;
	AdbDoctorLink *srcLink;
	AdbDoctorLink *destLink;

	if (srcList == NULL)
		return;

	dlist_foreach_modify(miter, &srcList->head)
	{
		srcLink = dlist_container(AdbDoctorLink, wi_links, miter.cur);
		if (drain)
		{
			deleteFromAdbDoctorList(srcList, miter);
			destLink = srcLink;
		}
		else
		{
			destLink = newAdbDoctorLink(NULL, NULL);
			memcpy(destLink, srcLink, sizeof(AdbDoctorLink));
		}

		pushTailAdbDoctorList(destList, destLink);
	}
	if (drain)
		pfreeAdbDoctorList(srcList, false);
}

AdbDoctorList *cloneAdbDoctorList(AdbDoctorList *list)
{
	AdbDoctorList *cloneList;
	AdbDoctorLink *cloneLink;

	dlist_iter iter;
	AdbDoctorLink *link;

	cloneList = newAdbDoctorList();

	dlist_foreach(iter, &list->head)
	{
		cloneLink = palloc0(sizeof(AdbDoctorLink));

		link = dlist_container(AdbDoctorLink, wi_links, iter.cur);

		memcpy(cloneLink, link, sizeof(AdbDoctorLink));

		pushTailAdbDoctorList(cloneList, cloneLink);
	}
	return cloneList;
}