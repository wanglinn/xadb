CREATE OR REPLACE VIEW rxact_get_running AS
    SELECT r.gid,r.database,CASE WHEN n.node_name IS NULL THEN 'AGTM' ELSE n.node_name END,r.backend,r.type,r.status[r.i]
      FROM (SELECT *,generate_series(1,array_length(nodes,1)) AS i
        FROM (SELECT r.gid,d.datname AS database,r.type,r.backend,r.nodes,r.status
          FROM pg_catalog.rxact_get_running() AS r
            LEFT JOIN pg_catalog.pg_database AS d
            ON r.dbid=d.oid
          ) AS r
      ) AS r
        LEFT JOIN pg_catalog.pgxc_node AS n
        ON r.nodes[r.i] = n.oid;
