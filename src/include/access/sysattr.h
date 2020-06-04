/*-------------------------------------------------------------------------
 *
 * sysattr.h
 *	  POSTGRES system attribute definitions.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
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
#define MinTransactionIdAttributeNumber			(-2)
#define MinCommandIdAttributeNumber				(-3)
#define MaxTransactionIdAttributeNumber			(-4)
#define MaxCommandIdAttributeNumber				(-5)
#define TableOidAttributeNumber					(-6)
#ifdef ADB
	#define XC_NodeIdAttributeNumber				(-7)
	#ifdef ADB_GRAM_ORA
		#define ADB_RowIdAttributeNumber			(-8)
		#define ADB_InfoMaskAttributeNumber			(-9)
		#define FirstLowInvalidHeapAttributeNumber	(-10)
	#else /* ADB_GRAM_ORA */
		#define ADB_InfoMaskAttributeNumber			(-8)
		#define FirstLowInvalidHeapAttributeNumber	(-9)
	#endif /* ADB_GRAM_ORA */
#elif defined(ADB_GRAM_ORA) /* else adb */
	#define ADB_RowIdAttributeNumber				(-7)
	#define ADB_InfoMaskAttributeNumber				(-8)
	#define FirstLowInvalidHeapAttributeNumber		(-9)
#else /* else ADB, else ADB_GRAM_ORA */
	#define FirstLowInvalidHeapAttributeNumber		(-7)
#endif

#endif							/* SYSATTR_H */
