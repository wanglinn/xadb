/*--------------------------------------------------------------------------
 *
 * Copyright (c) 2018-2019, Asiainfo Database Innovation Lab
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "storage/dsm.h"
#include "storage/shm_toc.h"
#include "storage/spin.h"
#include "utils/resowner.h"
#include "adb_doctor_data.h"

/* 
 * create a shm with size segsize (estimated by shm_toc_estimator), 
 * create a shm_toc for convenience to insert data into shm. memcpy data with
 * size datasize (according to actual size) of data into this shm with the tocKey,
 * then return a pointer to a struct which contain these things.
 */
AdbDoctorBgworkerDataShm *
setupAdbDoctorBgworkerDataShm(AdbDoctorBgworkerData *data)
{
	Size segsize;
	Size datasize;
	Size nkeys = 0;
	shm_toc_estimator e;
	dsm_segment *seg;
	shm_toc *toc;
	uint64 tocKey = 0;
	AdbDoctorBgworkerDataShm *dataShm;
	AdbDoctorBgworkerData *dataInShm;

	shm_toc_initialize_estimator(&e);

	datasize = sizeof(AdbDoctorBgworkerData);
	shm_toc_estimate_chunk(&e, datasize);
	nkeys++;

	shm_toc_estimate_keys(&e, nkeys);
	segsize = shm_toc_estimate(&e);

	seg = dsm_create(segsize, 0);
	toc = shm_toc_create(ADB_DOCTOR_SHM_DATA_MAGIC,
						 dsm_segment_address(seg),
						 segsize);

	/* insert data into shm */
	dataInShm = shm_toc_allocate(toc, datasize);
	shm_toc_insert(toc, tocKey++, dataInShm);
	memcpy(dataInShm, data, datasize);
	SpinLockInit(&dataInShm->mutex);

	/* this struct contain these things about shm */
	dataShm = palloc0(sizeof(AdbDoctorBgworkerDataShm));
	dataShm->seg = seg;
	dataShm->toc = toc;
	dataShm->dataInShm = dataInShm;

	return dataShm;
}

AdbDoctorBgworkerData *
attachAdbDoctorBgworkerDataShm(Datum main_arg, char *name)
{
	dsm_segment *seg;
	shm_toc *toc;
	AdbDoctorBgworkerData *bgworkerDataInShm;
	AdbDoctorBgworkerData *bgworkerData;
	uint64 tocKey = 0;

	CurrentResourceOwner = ResourceOwnerCreate(NULL, MyBgworkerEntry->bgw_name);
	seg = dsm_attach(DatumGetUInt32(main_arg));
	if (seg == NULL)
		ereport(ERROR,
				(errmsg("unable to map individual dynamic shared memory segment.")));

	toc = shm_toc_attach(ADB_DOCTOR_SHM_DATA_MAGIC, dsm_segment_address(seg));
	if (toc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("bad magic number in dynamic shared memory segment.")));

	bgworkerDataInShm = shm_toc_lookup(toc, tocKey++, false);

	SpinLockAcquire(&bgworkerDataInShm->mutex);
	bgworkerData = palloc0(sizeof(AdbDoctorBgworkerData));
	/* this shm will be detached, copy out all the data */
	memcpy(bgworkerData, bgworkerDataInShm, sizeof(AdbDoctorBgworkerData));
	/* If true, launcher process know this worker is ready. */
	bgworkerDataInShm->ready = true;
	SpinLockRelease(&bgworkerDataInShm->mutex);

	dsm_detach(seg);
	return bgworkerData;
}

bool isIdenticalDoctorMgrNode(MgrNodeWrapper *data1, MgrNodeWrapper *data2)
{
	return data1->oid == data2->oid &&
		   strcmp(NameStr(data1->form.nodename),
				  NameStr(data2->form.nodename)) == 0 &&
		   data1->form.nodehost == data2->form.nodehost &&
		   data1->form.nodetype == data2->form.nodetype &&
		   strcmp(NameStr(data1->form.nodesync),
				  NameStr(data2->form.nodesync)) == 0 &&
		   data1->form.nodeport == data2->form.nodeport &&
		   data1->form.nodeinited == data2->form.nodeinited &&
		   data1->form.nodemasternameoid == data2->form.nodemasternameoid &&
		   data1->form.nodeincluster == data2->form.nodeincluster &&
		   strcmp(NameStr(data1->form.nodezone),
				  NameStr(data2->form.nodezone)) == 0 &&
		   data1->form.allowcure == data2->form.allowcure &&
		   strcmp(NameStr(data1->form.curestatus),
				  NameStr(data2->form.curestatus)) == 0 &&
		   strcmp(data1->nodepath, data2->nodepath) == 0 &&
		   isIdenticalDoctorMgrHost(data1->host, data2->host);
}

bool isIdenticalDoctorMgrHost(MgrHostWrapper *data1, MgrHostWrapper *data2)
{
	return data1->form.oid == data2->form.oid &&
		   strcmp(NameStr(data1->form.hostname),
				  NameStr(data2->form.hostname)) == 0 &&
		   strcmp(NameStr(data1->form.hostuser),
				  NameStr(data2->form.hostuser)) == 0 &&
		   data1->form.hostport == data2->form.hostport &&
		   data1->form.hostproto == data2->form.hostproto &&
		   data1->form.hostagentport == data2->form.hostagentport &&
		   strcmp(data1->hostaddr, data2->hostaddr) == 0 &&
		   strcmp(data1->hostadbhome, data2->hostadbhome) == 0;
}

bool isIdenticalDoctorMgrNodes(dlist_head *list1, dlist_head *list2)
{
	dlist_mutable_iter miter1;
	dlist_mutable_iter miter2;
	dlist_iter iter;
	MgrNodeWrapper *data1;
	MgrNodeWrapper *data2;
	MgrNodeWrapper *data;
	dlist_head cmpList1 = DLIST_STATIC_INIT(cmpList1);
	dlist_head cmpList2 = DLIST_STATIC_INIT(cmpList2);

	/* Link these data to a new dlist by different dlist_node 
	 * to avoid messing up these data */
	dlist_foreach(iter, list1)
	{
		data = dlist_container(MgrNodeWrapper, link, iter.cur);
		dlist_push_tail(&cmpList1, &data->cmpLink);
	}
	dlist_foreach(iter, list2)
	{
		data = dlist_container(MgrNodeWrapper, link, iter.cur);
		dlist_push_tail(&cmpList2, &data->cmpLink);
	}
	dlist_foreach_modify(miter1, &cmpList1)
	{
		data1 = dlist_container(MgrNodeWrapper, cmpLink, miter1.cur);
		dlist_foreach_modify(miter2, &cmpList2)
		{
			data2 = dlist_container(MgrNodeWrapper, cmpLink, miter2.cur);
			if (isIdenticalDoctorMgrNode(data1, data2))
			{
				dlist_delete(miter1.cur);
				dlist_delete(miter2.cur);
				break;
			}
			else
			{
				continue;
			}
		}
	}
	return dlist_is_empty(&cmpList1) && dlist_is_empty(&cmpList2);
}

bool isIdenticalDoctorMgrHosts(dlist_head *list1, dlist_head *list2)
{
	dlist_mutable_iter miter1;
	dlist_mutable_iter miter2;
	dlist_iter iter;
	MgrHostWrapper *data1;
	MgrHostWrapper *data2;
	MgrHostWrapper *data;
	dlist_head cmpList1 = DLIST_STATIC_INIT(cmpList1);
	dlist_head cmpList2 = DLIST_STATIC_INIT(cmpList2);

	/* Link these data to a new dlist by different dlist_node 
	 * to avoid messing up these data */
	dlist_foreach(iter, list1)
	{
		data = dlist_container(MgrHostWrapper, link, iter.cur);
		dlist_push_tail(&cmpList1, &data->cmpLink);
	}
	dlist_foreach(iter, list2)
	{
		data = dlist_container(MgrHostWrapper, link, iter.cur);
		dlist_push_tail(&cmpList2, &data->cmpLink);
	}
	dlist_foreach_modify(miter1, &cmpList1)
	{
		data1 = dlist_container(MgrHostWrapper, cmpLink, miter1.cur);
		dlist_foreach_modify(miter2, &cmpList2)
		{
			data2 = dlist_container(MgrHostWrapper, cmpLink, miter2.cur);
			if (isIdenticalDoctorMgrHost(data1, data2))
			{
				dlist_delete(miter1.cur);
				dlist_delete(miter2.cur);
				break;
			}
			else
			{
				continue;
			}
		}
	}
	return dlist_is_empty(&cmpList1) && dlist_is_empty(&cmpList2);
}

void pfreeAdbDoctorBgworkerData(AdbDoctorBgworkerData *src)
{
	if (src)
	{
		if (src->displayName)
		{
			pfree(src->displayName);
			src->displayName = NULL;
		}
		pfree(src);
		src = NULL;
	}
}

void pfreeAdbDoctorBgworkerStatus(AdbDoctorBgworkerStatus *src, bool freeData)
{
	if (src)
	{
		if (src->handle)
		{
			pfree(src->handle);
			src->handle = NULL;
		}
		if (freeData)
		{
			if (src->bgworkerData)
			{
				pfreeAdbDoctorBgworkerData(src->bgworkerData);
				src->bgworkerData = NULL;
			}
		}
		pfree(src);
		src = NULL;
	}
}

void logAdbDoctorBgworkerData(AdbDoctorBgworkerData *src,
							  char *title, int elevel)
{
	char *rTitle = "";
	if (title != NULL && strlen(title) > 0)
		rTitle = title;
	ereport(elevel,
			(errmsg("%s    mutex:%c,type:%d,oid:%d,displayName:%s,commonShmHandle:%d,ready:%d",
					rTitle,
					src->mutex,
					src->type,
					src->oid,
					src->displayName,
					src->commonShmHandle,
					src->ready)));
}