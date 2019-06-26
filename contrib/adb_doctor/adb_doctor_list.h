
/*--------------------------------------------------------------------------
 *
 * 
 * Copyright (c) 2018-2019, Asiainfo Database Innovation Lab
 *
 * -------------------------------------------------------------------------
 */
#ifndef ADB_DOCTOR_LIST_H
#define ADB_DOCTOR_LIST_H

#include "lib/ilist.h"
#include "utils/palloc.h"

typedef struct AdbDoctorList
{
	int num; /* the number of element in list */
	dlist_head head;
} AdbDoctorList;

/*
 * by warpping data into  AdbDoctorLink, 
 * data will not mess up even though it is linked to many dlists.
 * it is recommended to use newAdbDoctorLink() to malloc this struct.
 */
typedef struct AdbDoctorLink
{
	dlist_node wi_links; /* used to for dlink */
	/* inner linked data, declared as "void *" to minimize casting annoyances */
	void *data;
	/* pfree function, if it is NULL, call pfree(data) directly. */
	void (*pfreeData)(void *data);
} AdbDoctorLink;

static inline AdbDoctorList *newAdbDoctorList()
{
	AdbDoctorList *list = palloc0(sizeof(AdbDoctorList));
	dlist_init(&list->head);
	return list;
}

static inline void deleteFromAdbDoctorList(AdbDoctorList *list, dlist_mutable_iter miter)
{
	dlist_delete(miter.cur);
	list->num--;
}

static inline AdbDoctorLink *newAdbDoctorLink(void *data, void (*pfreeData)(void *))
{
	AdbDoctorLink *link = palloc0(sizeof(AdbDoctorLink));
	link->data = data;
	link->pfreeData = pfreeData;
	return link;
}

static inline void pushTailAdbDoctorList(AdbDoctorList *list, AdbDoctorLink *link)
{
	dlist_push_tail(&list->head, &link->wi_links);
	list->num++;
}

static inline void pushHeadAdbDoctorList(AdbDoctorList *list, void *data, void (*pfreeData)(void *))
{
	AdbDoctorLink *link = newAdbDoctorLink(data, pfreeData);
	dlist_push_head(&list->head, &link->wi_links);
	list->num++;
}

static inline void pushDataTailAdbDoctorList(AdbDoctorList *list, void *data, void (*pfreeData)(void *))
{
	AdbDoctorLink *link = newAdbDoctorLink(data, pfreeData);
	dlist_push_tail(&list->head, &link->wi_links);
	list->num++;
}

static inline void pushDataHeadAdbDoctorList(AdbDoctorList *list, void *data, void (*pfreeData)(void *))
{
	AdbDoctorLink *link = newAdbDoctorLink(data, pfreeData);
	dlist_push_head(&list->head, &link->wi_links);
	list->num++;
}

extern void pfreeAdbDoctorList(AdbDoctorList *src, bool freeData);
extern void pfreeAdbDoctorLink(AdbDoctorLink *src, bool freeData);

/* 
 * append all the data linked in src to dest. 
 * if drain=true, drain all the data in src, then pfree it.
 * if drain=false, copy out the data from src and the append to dest.
 */
extern void appendAdbDoctorList(AdbDoctorList *dest, AdbDoctorList *src, bool drain);
extern AdbDoctorList *cloneAdbDoctorList(AdbDoctorList *list);

#endif