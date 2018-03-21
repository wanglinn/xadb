/*-------------------------------------------------------------------------
 *
 * sysattr.h
 *	  POSTGRES system attribute definitions.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/sysattr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SYSATTR_H
#define SYSATTR_H


/*
 * Attribute numbers for the system-defined attributes
 */
#define SelfItemPointerAttributeNumber			(-1)
#define ObjectIdAttributeNumber					(-2)
#define MinTransactionIdAttributeNumber			(-3)
#define MinCommandIdAttributeNumber				(-4)
#define MaxTransactionIdAttributeNumber			(-5)
#define MaxCommandIdAttributeNumber				(-6)
#define TableOidAttributeNumber					(-7)
#ifdef ADB
#define XC_NodeIdAttributeNumber				(-8)
#define ADB_RowIdAttributeNumber				(-9)
#define ADB_InfoMaskAttributeNumber				(-10)
#define FirstLowInvalidHeapAttributeNumber		(-11)
#else
#define FirstLowInvalidHeapAttributeNumber		(-8)
#endif

#endif							/* SYSATTR_H */
