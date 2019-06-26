/*--------------------------------------------------------------------------
 *
 * 
 * Copyright (c) 2018-2019, Asiainfo Database Innovation Lab
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "adb_doctor_data.h"

Size sizeofAdbDoctorBgworkerData(AdbDoctorBgworkerData *data)
{
    Adb_Doctor_Bgworker_Type type;
    Size size;
    type = data->type;
    if (type == ADB_DOCTOR_BGWORKER_TYPE_NODE_MONITOR)
    {
        size = sizeof(AdbDoctorNodeData);
    }
    else if (type == ADB_DOCTOR_BGWORKER_TYPE_HOST_MONITOR)
    {
        size = sizeof(AdbDoctorHostData);
    }
    else if (type == ADB_DOCTOR_BGWORKER_TYPE_SWITCHER)
    {
        size = sizeof(AdbDoctorSwitcherData);
    }
    else
    {
        size = 0;
        ereport(ERROR,
                (errmsg("unrecognized type:%d", type)));
    }
    return size;
}

bool isSameAdbDoctorBgworkerData(AdbDoctorBgworkerData *data1, AdbDoctorBgworkerData *data2)
{
    if (data1->type == ADB_DOCTOR_BGWORKER_TYPE_NODE_MONITOR)
    {
        return data1->type == data2->type &&
               ((AdbDoctorNodeData *)data1)->wrapper->oid == ((AdbDoctorNodeData *)data2)->wrapper->oid;
    }
    else if (data1->type == ADB_DOCTOR_BGWORKER_TYPE_HOST_MONITOR)
    {
        return data1->type == data2->type;
    }
    else if (data1->type == ADB_DOCTOR_BGWORKER_TYPE_SWITCHER)
    {
        return data1->type == data2->type &&
               ((AdbDoctorNodeData *)data1)->wrapper->oid == ((AdbDoctorNodeData *)data2)->wrapper->oid;
    }
    else
    {
        return false;
    }
}

bool equalsAdbMgrNodeWrapper(AdbMgrNodeWrapper *data1, AdbMgrNodeWrapper *data2)
{
    return data1->oid == data2->oid &&
           strcmp(NameStr(data1->fdmn.nodename), NameStr(data2->fdmn.nodename)) == 0 &&
           data1->fdmn.nodehost == data2->fdmn.nodehost &&
           data1->fdmn.nodetype == data2->fdmn.nodetype &&
           strcmp(NameStr(data1->fdmn.nodesync), NameStr(data2->fdmn.nodesync)) == 0 &&
           data1->fdmn.nodeport == data2->fdmn.nodeport &&
           data1->fdmn.nodeinited == data2->fdmn.nodeinited &&
           data1->fdmn.nodemasternameoid == data2->fdmn.nodemasternameoid &&
           data1->fdmn.nodeincluster == data2->fdmn.nodeincluster &&
           data1->fdmn.nodereadonly == data2->fdmn.nodereadonly &&
           strcmp(NameStr(data1->fdmn.nodezone), NameStr(data2->fdmn.nodezone)) == 0 &&
           strcmp(data1->nodepath, data2->nodepath) == 0 &&
           strcmp(NameStr(data1->hostuser), NameStr(data2->hostuser)) == 0 &&
           strcmp(data1->hostaddr, data2->hostaddr) == 0;
}

bool equalsAdbMgrHostWrapper(AdbMgrHostWrapper *data1, AdbMgrHostWrapper *data2)
{
    return data1->oid == data2->oid &&
           strcmp(NameStr(data1->fdmh.hostname), NameStr(data2->fdmh.hostname)) == 0 &&
           strcmp(NameStr(data1->fdmh.hostuser), NameStr(data2->fdmh.hostuser)) == 0 &&
           data1->fdmh.hostport == data2->fdmh.hostport &&
           data1->fdmh.hostproto == data2->fdmh.hostproto &&
           data1->fdmh.hostagentport == data2->fdmh.hostagentport &&
           strcmp(data1->hostaddr, data2->hostaddr) == 0 &&
           strcmp(data1->hostadbhome, data2->hostadbhome) == 0;
}

bool equalsAdbDoctorBgworkerData(AdbDoctorBgworkerData *data1, AdbDoctorBgworkerData *data2)
{
    Assert(data1->type == data2->type);
    if (data1->type == ADB_DOCTOR_BGWORKER_TYPE_NODE_MONITOR)
    {
        return equalsAdbDoctorNodeData((AdbDoctorNodeData *)data1, (AdbDoctorNodeData *)data2);
    }
    else if (data1->type == ADB_DOCTOR_BGWORKER_TYPE_HOST_MONITOR)
    {
        return equalsAdbDoctorHostData((AdbDoctorHostData *)data1, (AdbDoctorHostData *)data2);
    }
    else if (data1->type == ADB_DOCTOR_BGWORKER_TYPE_SWITCHER)
    {
        return equalsAdbDoctorSwitcherData((AdbDoctorSwitcherData *)data1, (AdbDoctorSwitcherData *)data2);
    }
    else
    {
        return false;
    }
}

bool equalsAdbDoctorHostData(AdbDoctorHostData *data1, AdbDoctorHostData *data2)
{
    bool result;

    if (dlist_is_empty(&data1->list->head) ||
        dlist_is_empty(&data2->list->head) ||
        data1->list->num != data2->list->num)
    {
        return false;
    }

    AdbDoctorList *cloneList1;
    AdbDoctorList *cloneList2;

    dlist_mutable_iter iter1;
    AdbDoctorLink *link1;
    dlist_mutable_iter iter2;
    AdbDoctorLink *link2;

    cloneList1 = cloneAdbDoctorList(data1->list);
    cloneList2 = cloneAdbDoctorList(data2->list);

    dlist_foreach_modify(iter1, &cloneList1->head)
    {
        link1 = dlist_container(AdbDoctorLink, wi_links, iter1.cur);
        dlist_foreach_modify(iter2, &cloneList2->head)
        {
            link2 = dlist_container(AdbDoctorLink, wi_links, iter2.cur);
            if (equalsAdbMgrHostWrapper(link1->data, link2->data))
            {
                deleteFromAdbDoctorList(cloneList1, iter1);
                pfreeAdbDoctorLink(link1, false);
                deleteFromAdbDoctorList(cloneList2, iter2);
                pfreeAdbDoctorLink(link2, false);
                break;
            }
            else
            {
                continue;
            }
        }
    }

    result = dlist_is_empty(&cloneList1->head) && dlist_is_empty(&cloneList2->head);

    pfreeAdbDoctorList(cloneList1, false);
    pfreeAdbDoctorList(cloneList2, false);

    return result;
}

bool equalsAdbDoctorNodeData(AdbDoctorNodeData *data1, AdbDoctorNodeData *data2)
{
    return data1->header.type == data2->header.type &&
           equalsAdbMgrNodeWrapper(data1->wrapper, data2->wrapper);
}

bool equalsAdbDoctorSwitcherData(AdbDoctorSwitcherData *data1, AdbDoctorSwitcherData *data2)
{
    return data1->header.type == data2->header.type &&
           equalsAdbMgrNodeWrapper(data1->wrapper, data2->wrapper);
}

void pfreeAdbMgrHostWrapper(AdbMgrHostWrapper *src)
{
    if (src == NULL)
        return;
    pfree(src->hostaddr);
    pfree(src->hostadbhome);
    pfree(src);
    src = NULL;
}

void pfreeAdbMgrNodeWrapper(AdbMgrNodeWrapper *src)
{
    if (src == NULL)
        return;
    pfree(src->nodepath);
    pfree(src->hostaddr);
    pfree(src);
    src = NULL;
}

void pfreeAdbDoctorHostData(AdbDoctorHostData *src)
{
    if (src == NULL)
        return;
    pfreeAdbDoctorList(src->list, true);
    pfree(src);
    src = NULL;
}

void pfreeAdbDoctorBgworkerData(AdbDoctorBgworkerData *src)
{
    if (src == NULL)
        return;
    if (src->type == ADB_DOCTOR_BGWORKER_TYPE_NODE_MONITOR)
        pfreeAdbDoctorNodeData((AdbDoctorNodeData *)src);
    else if (src->type == ADB_DOCTOR_BGWORKER_TYPE_HOST_MONITOR)
        pfreeAdbDoctorHostData((AdbDoctorHostData *)src);
    else if (src->type == ADB_DOCTOR_BGWORKER_TYPE_SWITCHER)
        pfreeAdbDoctorSwitcherData((AdbDoctorSwitcherData *)src);
    else
        pfree(src);
    src = NULL;
}

void pfreeAdbDoctorBgworkerStatus(AdbDoctorBgworkerStatus *src, bool freeData)
{
    if (src == NULL)
        return;
    pfree(src->name);
    pfree(src->handle);
    if (freeData)
        pfreeAdbDoctorBgworkerData(src->data);
    pfree(src);
    src = NULL;
}

void pfreeAdbDoctorNodeData(AdbDoctorNodeData *src)
{
    if (src == NULL)
        return;
    pfreeAdbMgrNodeWrapper(src->wrapper);
    pfree(src);
    src = NULL;
}

void pfreeAdbDoctorSwitcherData(AdbDoctorSwitcherData *src)
{
    if (src == NULL)
        return;
    pfreeAdbMgrNodeWrapper(src->wrapper);
    pfree(src);
    src = NULL;
}

void logAdbDoctorHostData(AdbDoctorHostData *src, char *title, int elevel)
{
    dlist_iter iter;
    AdbDoctorLink *link;
    AdbMgrHostWrapper *data;

    if (title != NULL && strlen(title) > 0)
        ereport(elevel, (errmsg("%s, size:%d", title, src->list->num)));

    dlist_foreach(iter, &src->list->head)
    {
        link = dlist_container(AdbDoctorLink, wi_links, iter.cur);
        data = link->data;
        ereport(elevel,
                (errmsg("	 	 oid:%d,hostname:%s,hostuser:%s,hostaddr:%s,hostagentport:%d,hostadbhome:%s",
                        data->oid,
                        data->fdmh.hostname.data,
                        data->fdmh.hostuser.data,
                        data->hostaddr,
                        data->fdmh.hostagentport,
                        data->hostadbhome)));
    }
}

void logAdbDoctorNodeData(AdbDoctorNodeData *src, char *title, int elevel)
{
    char *rTitle = "";
    if (title != NULL && strlen(title) > 0)
        rTitle = title;
    ereport(elevel,
            (errmsg("%s oid:%d,nodename:%s,nodetype:%c,hostaddr:%s,nodeport:%d,curestatus:%s",
                    rTitle,
                    src->wrapper->oid,
                    src->wrapper->fdmn.nodename.data,
                    src->wrapper->fdmn.nodetype,
                    src->wrapper->hostaddr,
                    src->wrapper->fdmn.nodeport,
                    src->wrapper->fdmn.curestatus.data)));
}

void logAdbDoctorSwitcherData(AdbDoctorSwitcherData *src, char *title, int elevel)
{
    char *rTitle = "";
    if (title != NULL && strlen(title) > 0)
        rTitle = title;
    ereport(elevel,
            (errmsg("%s oid:%d,nodename:%s,nodetype:%c,hostaddr:%s,nodeport:%d,curestatus:%s",
                    rTitle,
                    src->wrapper->oid,
                    src->wrapper->fdmn.nodename.data,
                    src->wrapper->fdmn.nodetype,
                    src->wrapper->hostaddr,
                    src->wrapper->fdmn.nodeport,
                    src->wrapper->fdmn.curestatus.data)));
}

void logAdbDoctorBgworkerData(AdbDoctorBgworkerData *src, char *title, int elevel)
{
    if (title != NULL && strlen(title) > 0)
        ereport(elevel, (errmsg("%s", title)));
    if (src->type == ADB_DOCTOR_BGWORKER_TYPE_NODE_MONITOR)
    {
        logAdbDoctorNodeData((AdbDoctorNodeData *)src, "    NodeData    ", elevel);
    }
    else if (src->type == ADB_DOCTOR_BGWORKER_TYPE_HOST_MONITOR)
    {
        logAdbDoctorHostData((AdbDoctorHostData *)src, "    hosts", elevel);
    }
    else if (src->type == ADB_DOCTOR_BGWORKER_TYPE_SWITCHER)
    {
        logAdbDoctorSwitcherData((AdbDoctorSwitcherData *)src, "    SwitcherData    ", elevel);
    }
}

void logAdbDoctorBgworkerDataList(AdbDoctorList *src, char *title, int elevel)
{
    dlist_iter iter;
    AdbDoctorLink *link;
    ereport(elevel, (errmsg("%s size:%d", title, src->num)));
    dlist_foreach(iter, &src->head)
    {
        link = dlist_container(AdbDoctorLink, wi_links, iter.cur);

        logAdbDoctorBgworkerData(link->data, NULL, elevel);
    }
}

void appendAdbDoctorBgworkerData(AdbDoctorList *destList, AdbDoctorBgworkerData *data)
{
    AdbDoctorLink *link;
    if (data != NULL)
    {
        link = newAdbDoctorLink(data, (void (*)(void *))pfreeAdbDoctorBgworkerData);
        pushTailAdbDoctorList(destList, link);
    }
}