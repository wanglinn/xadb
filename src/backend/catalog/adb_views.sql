/*
 * PGXC system view to look for prepared transaction GID list in a cluster
 */
CREATE OR REPLACE FUNCTION pg_catalog.pgxc_prepared_xact()
RETURNS setof text
AS $$
DECLARE
	text_output text;
	row_data record;
	row_name record;
	query_str text;
	query_str_nodes text;
	BEGIN
		--Get all the node names
		query_str_nodes := 'SELECT node_name FROM pgxc_node WHERE node_type = ''D'' AND node_master_oid=0';
		FOR row_name IN EXECUTE(query_str_nodes) LOOP
			query_str := 'EXECUTE DIRECT ON ("' || row_name.node_name || '") ''SELECT gid FROM pg_prepared_xact()''';
			FOR row_data IN EXECUTE(query_str) LOOP
				return next row_data.gid;
			END LOOP;
		END LOOP;
		return;
	END; $$
LANGUAGE 'plpgsql';

CREATE VIEW pg_catalog.pgxc_prepared_xacts AS
    SELECT DISTINCT * from pg_catalog.pgxc_prepared_xact();

CREATE OR REPLACE VIEW pg_catalog.rxact_get_running AS
    SELECT r.gid,r.database,n.node_name,r.backend,r.type,r.status[r.i]
      FROM (SELECT *,generate_series(1,array_length(nodes,1)) AS i
        FROM (SELECT r.gid,d.datname AS database,r.type,r.backend,r.nodes,r.status
          FROM pg_catalog.rxact_get_running() AS r
            LEFT JOIN pg_catalog.pg_database AS d
            ON r.dbid=d.oid
          ) AS r
      ) AS r
        LEFT JOIN pg_catalog.pgxc_node AS n
        ON r.nodes[r.i] = n.oid;

CREATE VIEW pg_catalog.pg_aux_tables AS
    SELECT
        N.nspname AS schemaname,
        U.auxrelid::regclass AS tablename,
        pg_get_userbyid(C.relowner) AS tableowner,
        T.spcname AS tablespace,
        C.relhasindex AS hasindexes,
        C.relhasrules AS hasrules,
        C.relhastriggers AS hastriggers,
        C.relrowsecurity AS rowsecurity,
        U.relid::regclass AS "master tablename",
        A.attname AS "auxiliary column"
    FROM pg_aux_class U LEFT JOIN pg_class C ON (U.auxrelid = C.oid)
         LEFT JOIN pg_attribute A ON (U.relid = A.attrelid AND U.attnum = A.attnum)
         LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
         LEFT JOIN pg_tablespace T ON (T.oid = C.reltablespace)
    WHERE C.relkind = 'r';
