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
        'X'::varchar2(1);

CREATE RULE insert_dual AS ON INSERT TO oracle.dual DO INSTEAD NOTHING;
CREATE RULE update_dual AS ON UPDATE TO oracle.dual DO INSTEAD NOTHING;
CREATE RULE delete_dual AS ON DELETE TO oracle.dual DO INSTEAD NOTHING;

CREATE OR REPLACE VIEW oracle.all_constraints AS
    SELECT UPPER(con.table_schema) AS owner,
        UPPER(con.table_name) AS table_name,
        UPPER(con.constraint_name) AS constraint_name,
        'P'::varchar2(10) AS constraint_type
    FROM information_schema.constraint_table_usage con
    JOIN pg_roles rol
    ON 1 = 1
    WHERE con.table_schema NOT IN ('information_schema', 'pg_catalog', 'oracle')
    AND con.table_schema NOT LIKE 'pg_toast%'
    AND rol.rolname = CURRENT_USER
    ORDER BY con.table_schema, con.table_name;

CREATE OR REPLACE VIEW oracle.all_cons_columns AS
    SELECT UPPER(conclm.table_schema) AS owner,
        UPPER(conclm.table_name) AS table_name,
        UPPER(conclm.constraint_name) AS constraint_name,
        UPPER(conclm.column_name) AS column_name,
        clm.ordinal_position::int AS position
    FROM information_schema.constraint_column_usage conclm
    JOIN information_schema.columns clm
    ON clm.column_name = conclm.column_name
    AND clm.table_name = conclm.table_name
    JOIN pg_roles rol
    ON 1 = 1
    WHERE conclm.table_schema NOT IN ('information_schema', 'pg_catalog', 'oracle')
    AND conclm.table_schema NOT LIKE 'pg_toast%'
    AND rol.rolname = CURRENT_USER
    ORDER BY conclm.table_schema, conclm.table_name;

CREATE OR REPLACE VIEW oracle.all_tab_cols AS
    SELECT UPPER(clm.column_name) AS COLUMN_NAME,
        UPPER(clm.data_type) AS DATA_TYPE,
        clm.numeric_precision AS DATA_PRECISION,
        clm.numeric_scale AS DATA_SCALE,
        UPPER(clm.table_name) AS table_name,
        UPPER(table_schema) AS owner
    FROM information_schema.columns clm
    JOIN pg_roles rol
    ON 1 = 1
    WHERE clm.table_schema NOT IN ('information_schema', 'pg_catalog', 'oracle')
    AND clm.table_schema NOT LIKE 'pg_toast%'
    AND rol.rolname = CURRENT_USER
    ORDER BY clm.table_schema, clm.table_name;

CREATE OR REPLACE VIEW oracle.all_objects AS
    SELECT UPPER(nsp.nspname)::varchar2(30) AS owner,
        UPPER(cls.relname)::varchar2(30) AS object_name,
        CASE cls.relkind
            WHEN 'r' THEN 'TABLE'
            WHEN 'v' THEN 'VIEW'
        END AS object_type
    FROM pg_class cls
    JOIN pg_roles rol
    ON rol.oid = cls.relowner
    JOIN pg_namespace nsp
    ON nsp.oid = cls.relnamespace
    WHERE nsp.nspname NOT IN ('information_schema', 'pg_catalog', 'oracle')
    AND nsp.nspname NOT LIKE 'pg_toast%'
    AND rol.rolname = CURRENT_USER
    ORDER BY nsp.nspname, cls.relname;

GRANT SELECT ON oracle.dual TO PUBLIC;
GRANT SELECT ON oracle.all_constraints TO PUBLIC;
GRANT SELECT ON oracle.all_cons_columns TO PUBLIC;
GRANT SELECT ON oracle.all_tab_cols TO PUBLIC;
GRANT SELECT ON oracle.all_objects TO PUBLIC;

