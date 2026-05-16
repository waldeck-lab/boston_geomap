#!/usr/bin/env python3
import sqlite3
import sys

db = sys.argv[1] if len(sys.argv) > 1 else "../stage/db/geomap.sqlite"

def q(conn, sql, args=()):
    cur = conn.execute(sql, args)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    print("\n" + sql.strip())
    print(" | ".join(cols))
    for r in rows:
        print(" | ".join("" if v is None else str(v) for v in r))
    return rows

with sqlite3.connect(db) as conn:
    conn.row_factory = sqlite3.Row

    q(conn, """
    SELECT
      COUNT(*) AS raw_rows,
      COUNT(DISTINCT occurrence_id) AS distinct_occurrences,
      COUNT(DISTINCT taxon_id) AS taxa,
      MIN(year) AS min_year,
      MAX(year) AS max_year,
      SUM(COALESCE(individual_count, 1)) AS raw_individual_sum
    FROM observations_raw
    WHERE year BETWEEN 2000 AND 2026
    """)

    q(conn, """
    SELECT
      zoom,
      COUNT(*) AS raw_rows,
      COUNT(DISTINCT taxon_id) AS taxa,
      SUM(COALESCE(individual_count, 1)) AS raw_individual_sum
    FROM observations_raw
    WHERE year BETWEEN 2000 AND 2026
    GROUP BY zoom
    ORDER BY zoom
    """)

    q(conn, """
    SELECT
      zoom,
      slot_id,
      COUNT(*) AS grid_rows,
      COUNT(DISTINCT taxon_id) AS taxa,
      SUM(observations_count) AS grid_obs_sum
    FROM taxon_grid
    WHERE year BETWEEN 2000 AND 2026
      AND slot_id = 0
    GROUP BY zoom, slot_id
    ORDER BY zoom
    """)

    q(conn, """
    SELECT
      r.zoom,
      r.tile_x AS x,
      r.tile_y AS y,
      COUNT(*) AS raw_rows,
      COUNT(DISTINCT r.taxon_id) AS raw_taxa,
      SUM(COALESCE(r.individual_count, 1)) AS raw_individuals,
      COALESCE(g.grid_obs, 0) AS grid_obs,
      COALESCE(g.grid_taxa, 0) AS grid_taxa,
      SUM(COALESCE(r.individual_count, 1)) - COALESCE(g.grid_obs, 0) AS lost_individuals
    FROM observations_raw r
    LEFT JOIN (
      SELECT zoom, x, y,
             SUM(observations_count) AS grid_obs,
             COUNT(DISTINCT taxon_id) AS grid_taxa
      FROM taxon_grid
      WHERE year BETWEEN 2000 AND 2026
        AND slot_id = 0
      GROUP BY zoom, x, y
    ) g
      ON g.zoom = r.zoom
     AND g.x = r.tile_x
     AND g.y = r.tile_y
    WHERE r.year BETWEEN 2000 AND 2026
      AND r.zoom = 14
    GROUP BY r.zoom, r.tile_x, r.tile_y
    ORDER BY lost_individuals DESC
    LIMIT 30
    """)

    q(conn, """
    SELECT
      r.zoom,
      r.tile_x AS x,
      r.tile_y AS y,
      r.taxon_id,
      COUNT(*) AS raw_rows,
      SUM(COALESCE(r.individual_count, 1)) AS raw_individuals,
      COALESCE(g.grid_obs, 0) AS grid_obs,
      SUM(COALESCE(r.individual_count, 1)) - COALESCE(g.grid_obs, 0) AS lost_individuals
    FROM observations_raw r
    LEFT JOIN (
      SELECT zoom, x, y, taxon_id,
             SUM(observations_count) AS grid_obs
      FROM taxon_grid
      WHERE year BETWEEN 2000 AND 2026
        AND slot_id = 0
      GROUP BY zoom, x, y, taxon_id
    ) g
      ON g.zoom = r.zoom
     AND g.x = r.tile_x
     AND g.y = r.tile_y
     AND g.taxon_id = r.taxon_id
    WHERE r.year BETWEEN 2000 AND 2026
      AND r.zoom = 14
    GROUP BY r.zoom, r.tile_x, r.tile_y, r.taxon_id
    HAVING lost_individuals > 0
    ORDER BY lost_individuals DESC
    LIMIT 30
    """)
