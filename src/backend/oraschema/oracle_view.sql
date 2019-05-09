/*
 * Oracle Views
 *
 * Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Copyright (c) 2014-2016, ADB Development Group
 *
 * src/backend/oraschema/oracle_views.sql
 */
CREATE OR REPLACE VIEW oracle.dual ("DUMMY") AS
    SELECT
        'X'::oracle.varchar2(1);

CREATE RULE insert_dual AS ON INSERT TO oracle.dual DO INSTEAD NOTHING;
CREATE RULE update_dual AS ON UPDATE TO oracle.dual DO INSTEAD NOTHING;
CREATE RULE delete_dual AS ON DELETE TO oracle.dual DO INSTEAD NOTHING;

GRANT SELECT ON oracle.dual TO PUBLIC;

CREATE OR REPLACE VIEW  oracle.dba_tables
AS
SELECT
upper(nsp.nspname::text)::oracle.varchar2(30) AS owner,
upper(cls.relname::text)::oracle.varchar2(30) AS table_name,
cls.reltuples AS  NUM_ROWS,
cls.relpages AS  BLOCKS
FROM pg_class cls
JOIN pg_roles rol ON rol.oid = cls.relowner
JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace
where 1=1
AND cls.relkind='r'
AND (nsp.nspname <> ALL (ARRAY['information_schema'::name, 'pg_catalog'::name,'oracle'::name]))
AND nsp.nspname !~~ 'pg_toast%'::text;

GRANT SELECT ON oracle.dba_tables TO PUBLIC;

CREATE OR REPLACE VIEW  oracle.user_tables
AS
SELECT *
FROM oracle.dba_tables
WHERE owner=upper(CURRENT_USER);

GRANT SELECT ON oracle.user_tables TO PUBLIC;

CREATE OR REPLACE VIEW  oracle.dba_tab_columns
AS
SELECT
    upper(clm.table_schema::text) AS owner,
    upper(clm.table_name::text) AS table_name,
    upper(clm.column_name::text) AS column_name,
    clm.ordinal_position AS COLUMN_ID,
    upper(clm.data_type::text) AS data_type,
    clm.numeric_precision AS data_precision,
    clm.numeric_scale AS data_scale,
    COALESCE(clm.character_maximum_length,clm.numeric_precision) AS  DATA_LENGTH,
    CASE  clm.is_nullable
       WHEN 'YES'::TEXT then 'Y'::TEXT
       WHEN 'NO'::TEXT then 'N'::TEXT
       ELSE NULL::text
    END AS NULLABLE,
    --COALESCE(clm.is_nullable,'Y','N') AS NULLABLE,
    clm.column_default AS DATA_DEFAULT
   FROM information_schema.columns clm
     JOIN pg_roles rol ON rol.rolname = clm.table_schema::name
  WHERE (clm.table_schema::text <> ALL (ARRAY['information_schema'::name, 'pg_catalog'::name,'oracle'::name]))
  AND clm.table_schema::text !~~ 'pg_toast%'::text
  ORDER BY clm.table_schema, clm.table_name,clm.ordinal_position;

GRANT SELECT ON oracle.dba_tab_columns TO PUBLIC;

CREATE OR REPLACE VIEW  oracle.user_tab_columns
AS
SELECT *
FROM oracle.dba_tab_columns
WHERE owner=upper(CURRENT_USER);

GRANT SELECT ON oracle.user_tab_columns TO PUBLIC;

CREATE OR REPLACE VIEW  oracle.dba_ind_columns
AS
select
    upper(nsp.nspname) as INDEX_OWNER,
    upper(i.relname) AS INDEX_NAME,
    upper(t.relnamespace::regnamespace::text) AS TABLE_OWNER,
    upper(t.relname)  AS  TABLE_NAME,
    upper(a.attname) AS COLUMN_NAME,
    a.attnum AS COLUMN_POSITION
from
    pg_class t,
    pg_class i,
    pg_index ix,
    pg_attribute a,
    pg_namespace nsp
where
     t.oid = ix.indrelid
    and nsp.oid = i.relnamespace
    and i.oid = ix.indexrelid
    and a.attrelid = t.oid
    and a.attnum = ANY(ix.indkey)
    and t.relkind = 'r'
    and i.relkind = 'i'
    AND (nsp.nspname <> ALL (ARRAY['information_schema'::name, 'pg_catalog'::name,'oracle'::name]))
    AND nsp.nspname !~~ 'pg_toast%'::text
order by
    t.relname,
    i.relname;

GRANT SELECT ON oracle.dba_ind_columns TO PUBLIC;

CREATE OR REPLACE VIEW  oracle.user_ind_columns
AS
SELECT *
FROM oracle.dba_ind_columns
WHERE INDEX_OWNER=upper(CURRENT_USER);

GRANT SELECT ON oracle.user_ind_columns TO PUBLIC;

CREATE OR REPLACE VIEW  oracle.dba_indexes
AS
select
    upper(nsp.nspname) as OWNER,
    upper(i.relname) AS INDEX_NAME,
    upper(am.amname) AS INDEX_TYPE,
    upper(t.relnamespace::regnamespace::text) AS TABLE_OWNER,
    upper(t.relname)  AS  TABLE_NAME,
    'TABLE' AS  TABLE_TYPE,
    CASE  ix.indisunique
       WHEN true then 'UNIQUE'::TEXT
       WHEN false then 'NONUNIQUE'::TEXT
       ELSE NULL::text
    END AS UNIQUENESS
from
    pg_class t,
    pg_class i,
    pg_index ix,
    pg_am   am,
    pg_namespace nsp
where
     t.oid = ix.indrelid
    and nsp.oid = i.relnamespace
    and i.oid = ix.indexrelid
    and t.relkind = 'r'
    and i.relkind = 'i'
    AND (nsp.nspname <> ALL (ARRAY['information_schema'::name, 'pg_catalog'::name,'oracle'::name]))
    AND nsp.nspname !~~ 'pg_toast%'::text
    and am.oid=i.relam
order by
    t.relname,
    i.relname;

GRANT SELECT ON oracle.dba_indexes TO PUBLIC;

CREATE OR REPLACE VIEW  oracle.user_indexes
AS
SELECT *
FROM oracle.dba_indexes
WHERE OWNER=upper(CURRENT_USER);

GRANT SELECT ON oracle.user_indexes TO PUBLIC;

CREATE OR REPLACE VIEW  oracle.dba_constraints
AS
SELECT upper(con.table_schema::text) AS OWNER,
   upper(con.table_name::text) AS TABLE_NAME,
   upper(con.constraint_name::text) AS CONSTRAINT_NAME,
   'P'::oracle.varchar2(10) AS CONSTRAINT_TYPE,
   upper(con.table_schema::text) AS INDEX_OWNER,
   upper(con.constraint_name::text) AS INDEX_NAME
  FROM information_schema.constraint_table_usage con
    JOIN pg_roles rol ON rol.rolname = con.table_schema::name
 WHERE (con.table_schema::text <> ALL (ARRAY['information_schema'::name, 'pg_catalog'::name,'oracle'::name]))
 AND con.table_schema::text !~~ 'pg_toast%'::text
 ORDER BY con.table_schema, con.table_name;

GRANT SELECT ON oracle.dba_constraints TO PUBLIC;

CREATE OR REPLACE VIEW  oracle.user_constraints
AS
SELECT *
FROM oracle.dba_constraints
WHERE OWNER=upper(CURRENT_USER);

GRANT SELECT ON oracle.user_constraints TO PUBLIC;

CREATE OR REPLACE VIEW  oracle.dba_objects
AS
SELECT upper(nsp.nspname::text)::oracle.varchar2(30) AS owner,
    upper(c.relname::text)::oracle.varchar2(30) AS object_name,
    CASE c.relkind
        WHEN 'r' THEN 'TABLE'
        WHEN 'v' THEN 'VIEW'
        WHEN 'm' THEN 'MATERIALIZED VIEW'
        WHEN 'i' THEN 'INDEX'
        WHEN 'S' THEN 'SEQUENCE'
        WHEN 's' THEN 'SPECIAL'
        WHEN 'f' THEN 'FOREIGN TABLE'
        WHEN 'p' THEN 'PARTITIONED TABLE'
        WHEN 'c' THEN 'TYPE'
    END as OBJECT_TYPE
    FROM pg_class c
     JOIN pg_roles rol ON rol.oid = c.relowner
     JOIN pg_namespace nsp ON nsp.oid = c.relnamespace
  WHERE (nsp.nspname <> ALL (ARRAY['information_schema'::name, 'pg_catalog'::name]))
  AND nsp.nspname !~~ 'pg_toast%'::text
UNION ALL
SELECT upper(n.nspname::text)::oracle.varchar2(30) AS owner,
   upper(p.proname) AS object_name,
   CASE
       WHEN p.prorettype = 'pg_catalog.trigger'::pg_catalog.regtype THEN 'TRIGGER'
       ELSE 'FUNCTION'
   END as OBJECT_TYPE
FROM pg_catalog.pg_proc p
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
WHERE pg_catalog.pg_function_is_visible(p.oid)
      AND(n.nspname <> ALL (ARRAY['information_schema'::name, 'pg_catalog'::name,'oracle'::name]));

GRANT SELECT ON oracle.dba_objects TO PUBLIC;

CREATE OR REPLACE VIEW  oracle.user_objects
AS
SELECT *
FROM oracle.dba_objects
WHERE OWNER=upper(CURRENT_USER);

GRANT SELECT ON oracle.user_objects TO PUBLIC;

CREATE OR REPLACE VIEW  oracle.dba_cons_columns AS
    SELECT UPPER(conclm.table_schema) AS owner,
        UPPER(conclm.table_name) AS table_name,
        UPPER(conclm.constraint_name) AS constraint_name,
        UPPER(conclm.column_name) AS column_name,
        clm.ordinal_position::int AS position
    FROM information_schema.constraint_column_usage conclm
    JOIN information_schema.columns clm
    ON clm.column_name = conclm.column_name
    AND clm.table_name = conclm.table_name
    JOIN pg_roles rol ON rol.rolname = conclm.table_schema
    WHERE conclm.table_schema NOT IN ('information_schema', 'pg_catalog','oracle')
    AND conclm.table_schema NOT LIKE 'pg_toast%'
    ORDER BY conclm.table_schema, conclm.table_name;

GRANT SELECT ON oracle.dba_cons_columns TO PUBLIC;

CREATE OR REPLACE VIEW  oracle.user_cons_columns
AS
SELECT *
FROM oracle.dba_cons_columns
WHERE OWNER=upper(CURRENT_USER);

GRANT SELECT ON oracle.user_cons_columns TO PUBLIC;

CREATE OR REPLACE FUNCTION oracle.raise_application_error(error_number int, error_msg varchar(2048), flag boolean default false)
    RETURNS void
    AS $$
    BEGIN
        IF error_number < -20999 or error_number > -20000 THEN
            raise exception  'error number between -20999 and -20000';
        END IF;
        IF flag = false THEN
            raise exception 'ORA%: %' , $1, $2;
        END IF;
    END;
    $$
    LANGUAGE plpgsql
    IMMUTABLE
    STRICT;

GRANT EXECUTE   ON FUNCTION  oracle.raise_application_error  TO PUBLIC;
