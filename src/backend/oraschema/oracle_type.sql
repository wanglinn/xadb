/*
 * Oracle Types
 *
 * Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Copyright (c) 2014-2016, ADB Development Group
 *
 * src/backend/oraschema/oracle_type.sql
 */

/* CREATE CAST FOR oracle.nvarchar2 */
CREATE CAST (oracle.nvarchar2 AS text) WITHOUT FUNCTION AS IMPLICIT;

CREATE CAST (text AS oracle.nvarchar2) WITHOUT FUNCTION AS IMPLICIT;

CREATE CAST (oracle.nvarchar2 AS bpchar) WITHOUT FUNCTION AS IMPLICIT;

CREATE CAST (bpchar AS oracle.nvarchar2) WITHOUT FUNCTION AS IMPLICIT;

CREATE CAST (oracle.nvarchar2 AS varchar) WITHOUT FUNCTION AS IMPLICIT;

CREATE CAST (varchar AS oracle.nvarchar2) WITHOUT FUNCTION AS IMPLICIT;

CREATE CAST (oracle.nvarchar2 AS real) WITH INOUT AS IMPLICIT;

CREATE CAST (real AS oracle.nvarchar2) WITH INOUT AS IMPLICIT;

CREATE CAST (oracle.nvarchar2 AS double precision) WITH INOUT AS IMPLICIT;

CREATE CAST (double precision AS oracle.nvarchar2) WITH INOUT AS IMPLICIT;

CREATE CAST (oracle.nvarchar2 AS integer) WITH INOUT AS IMPLICIT;

CREATE CAST (integer AS oracle.nvarchar2) WITH INOUT AS IMPLICIT;

CREATE CAST (oracle.nvarchar2 AS smallint) WITH INOUT AS IMPLICIT;

CREATE CAST (smallint AS oracle.nvarchar2) WITH INOUT AS IMPLICIT;

CREATE CAST (oracle.nvarchar2 AS bigint) WITH INOUT AS IMPLICIT;

CREATE CAST (bigint AS oracle.nvarchar2) WITH INOUT AS IMPLICIT;

CREATE CAST (oracle.nvarchar2 AS numeric) WITH INOUT AS IMPLICIT;

CREATE CAST (numeric AS oracle.nvarchar2) WITH INOUT AS IMPLICIT;

CREATE CAST (oracle.nvarchar2 AS date) WITH INOUT AS IMPLICIT;

CREATE CAST (date AS oracle.nvarchar2) WITH INOUT AS IMPLICIT;

CREATE CAST (oracle.nvarchar2 AS timestamp) WITH INOUT AS IMPLICIT;

CREATE CAST (timestamp AS oracle.nvarchar2) WITH INOUT AS IMPLICIT;

CREATE CAST (oracle.nvarchar2 AS timestamptz) WITH INOUT AS IMPLICIT;

CREATE CAST (timestamptz AS oracle.nvarchar2) WITH INOUT AS IMPLICIT;

CREATE CAST (oracle.nvarchar2 AS interval) WITH INOUT AS IMPLICIT;

CREATE CAST (interval AS oracle.nvarchar2) WITH INOUT AS IMPLICIT;

CREATE CAST (oracle.nvarchar2 AS oracle.nvarchar2) WITH FUNCTION pg_catalog.nvarchar2(oracle.nvarchar2, integer, boolean) AS IMPLICIT;

/* CREATE CAST FOR ORACLE.VARCHAR2 */
CREATE CAST (oracle.varchar2 AS text) WITHOUT FUNCTION AS IMPLICIT;

CREATE CAST (text AS oracle.varchar2) WITHOUT FUNCTION AS IMPLICIT;

CREATE CAST (oracle.varchar2 AS bpchar) WITHOUT FUNCTION AS IMPLICIT;

CREATE CAST (bpchar AS oracle.varchar2) WITHOUT FUNCTION AS IMPLICIT;

CREATE CAST (oracle.varchar2 AS varchar) WITHOUT FUNCTION AS IMPLICIT;

CREATE CAST (varchar AS oracle.varchar2) WITHOUT FUNCTION AS IMPLICIT;

CREATE CAST (oracle.varchar2 AS real) WITH INOUT AS IMPLICIT;

CREATE CAST (real AS oracle.varchar2) WITH INOUT AS IMPLICIT;

CREATE CAST (oracle.varchar2 AS double precision) WITH INOUT AS IMPLICIT;

CREATE CAST (double precision AS oracle.varchar2) WITH INOUT AS IMPLICIT;

CREATE CAST (oracle.varchar2 AS integer) WITH INOUT AS IMPLICIT;

CREATE CAST (integer AS oracle.varchar2) WITH INOUT AS IMPLICIT;

CREATE CAST (oracle.varchar2 AS smallint) WITH INOUT AS IMPLICIT;

CREATE CAST (smallint AS oracle.varchar2) WITH INOUT AS IMPLICIT;

CREATE CAST (oracle.varchar2 AS bigint) WITH INOUT AS IMPLICIT;

CREATE CAST (bigint AS oracle.varchar2) WITH INOUT AS IMPLICIT;

CREATE CAST (oracle.varchar2 AS numeric) WITH INOUT AS IMPLICIT;

CREATE CAST (numeric AS oracle.varchar2) WITH INOUT AS IMPLICIT;

CREATE CAST (oracle.varchar2 AS date) WITH INOUT AS IMPLICIT;

CREATE CAST (date AS oracle.varchar2) WITH INOUT AS IMPLICIT;

CREATE CAST (oracle.varchar2 AS timestamp) WITH INOUT AS IMPLICIT;

CREATE CAST (timestamp AS oracle.varchar2) WITH INOUT AS IMPLICIT;

CREATE CAST (oracle.varchar2 AS timestamptz) WITH INOUT AS IMPLICIT;

CREATE CAST (timestamptz AS oracle.varchar2) WITH INOUT AS IMPLICIT;

CREATE CAST (oracle.varchar2 AS interval) WITH INOUT AS IMPLICIT;

CREATE CAST (interval AS oracle.varchar2) WITH INOUT AS IMPLICIT;

CREATE CAST (oracle.varchar2 AS oracle.varchar2) WITH FUNCTION pg_catalog.varchar2(oracle.varchar2, integer, boolean) AS IMPLICIT;

/* CREATE CAST FOR RID */
CREATE CAST (oracle.rid AS varchar) WITH INOUT AS IMPLICIT;

CREATE CAST (varchar AS oracle.rid) WITH INOUT AS IMPLICIT;

CREATE CAST (oracle.rid AS oracle.varchar2) WITH INOUT AS IMPLICIT;

CREATE CAST (oracle.varchar2 AS oracle.rid) WITH INOUT AS IMPLICIT;

CREATE CAST (oracle.rid AS oracle.nvarchar2) WITH INOUT AS IMPLICIT;

CREATE CAST (oracle.nvarchar2 AS oracle.rid) WITH INOUT AS IMPLICIT;

CREATE CAST (oracle.rid AS bpchar) WITH INOUT AS IMPLICIT;

CREATE CAST (bpchar AS oracle.rid) WITH INOUT AS IMPLICIT;

/* COPY "type1 + type2 AS type3 + type3" TO "type2 + type1 AS type3 + type3" */
INSERT INTO ora_convert
  SELECT 'o','+',CAST(concat(cvtfrom[1], ' ', cvtfrom[0]) AS oidvector),cvtto
  FROM ora_convert
  WHERE cvtkind='o' AND cvtname = '+' AND cvtto[0] = cvtto[1] AND cvtfrom[0] != cvtfrom[1];

/* COPY "type1 + type2 AS type3 + type3" TO "type1 - type2 AS type3 + type3"
   COPY "type1 + type2 AS type3 + type3" TO "type1 * type2 AS type3 + type3"
   COPY "type1 + type2 AS type3 + type3" TO "type1 / type2 AS type3 + type3" */
INSERT INTO ora_convert
  SELECT 'o', unnest('{-,*,/}'::name[]), cvtfrom, cvtto
  FROM ora_convert
  WHERE cvtkind='o' AND cvtname = '+' AND cvtto[0] = cvtto[1];

/* COPY "type1 + date AS type2 + date" TO "date + type1 AS date + type2" */
INSERT INTO ora_convert
  SELECT 'o', '+',
    cast(concat(cvtfrom[1], ' ', cvtfrom[0]) as oidvector),
    cast(concat(cvtto[1], ' ', cvtto[0]) as oidvector)
  FROM ora_convert
  WHERE cvtkind = 'o' AND cvtname = '+' AND 'oracle.date'::regtype = ANY(cvtto);

/* COPY "type1 + date AS type2 + date" TO "type1 - date AS type2 - date" */
INSERT INTO ora_convert
  SELECT 'o', '-', cvtfrom, cvtto
  FROM ora_convert
  WHERE cvtkind = 'o' AND cvtname = '+' AND 'oracle.date'::regtype = ANY(cvtto);

/* COPY "type1 = type2 AS type3 = type4" TO "type2 = type1 AS type4 + type3" */
INSERT INTO ora_convert
  SELECT 'o', '=',
    cast(concat(cvtfrom[1], ' ', cvtfrom[0]) as oidvector),
    cast(concat(cvtto[1], ' ', cvtto[0]) as oidvector)
  FROM ora_convert
  WHERE cvtkind = 'o' AND cvtname = '=';

/* >, >=, <, <=, <> and != from = */
INSERT INTO ora_convert
  SELECT 'o', unnest('{>, >=, <, <=, <>, !=}'::name[]), cvtfrom, cvtto
  FROM ora_convert
  WHERE cvtkind = 'o' AND cvtname = '=';

/* common combin */
INSERT INTO ora_convert
  SELECT 'c', '', CAST(concat(cvtfrom[1], ' ', cvtfrom[0]) AS oidvector), cvtto
  FROM ora_convert
  WHERE cvtkind = 'c' AND cvtname = '';

/* COPY: ~~(like)  Any permutation and combination of existing type conversions */
INSERT INTO ora_convert
  SELECT 'o', '~~',
    CAST(concat(f1, ' ', f2) AS oidvector), 
    CAST(concat('text'::regtype::int, ' ', 'text'::regtype::int) AS oidvector)
  FROM (SELECT t1.cvtfrom[0] AS f1,t2.cvtfrom[0] AS f2 
        FROM (SELECT cvtname,cvtfrom FROM ora_convert WHERE cvtname='~~' AND cvtfrom[0]<>'numeric'::regtype::int) t1
        FULL JOIN (SELECT cvtname,cvtfrom FROM ora_convert WHERE cvtname='~~' AND cvtfrom[0]<>'numeric'::regtype::int) t2 
        ON t1.cvtname=t2.cvtname)	t3
  WHERE f1<>f2
  UNION
  SELECT 'o', '~~',
    CAST(concat(f1, ' ', f2) AS oidvector), 
    CAST(concat('numeric'::regtype::int, ' ', 'text'::regtype::int) AS oidvector)
  FROM (SELECT t1.cvtfrom[0] AS f1,t2.cvtfrom[0] AS f2 
        FROM (SELECT cvtname,cvtfrom FROM ora_convert WHERE cvtname='~~' AND cvtfrom[0]='numeric'::regtype::int) t1
        FULL JOIN (SELECT cvtname,cvtfrom FROM ora_convert WHERE cvtname='~~' AND cvtfrom[0]<>'numeric'::regtype::int) t2 
        ON t1.cvtname=t2.cvtname)	t3
  WHERE f1<>f2;
