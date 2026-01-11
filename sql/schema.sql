-- ------------------------------------------------------------------------------
-- MIT License
-- 
-- Copyright (c) 2025 Jonas Waldeck
-- 
-- Permission is hereby granted, free of charge, to any person obtaining a copy
-- of this software and associated documentation files (the "Software"), to deal
-- in the Software without restriction, including without limitation the rights
-- to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
-- copies of the Software, and to permit persons to whom the Software is
-- furnished to do so, subject to the following conditions:
-- 
-- The above copyright notice and this permission notice shall be included in all
-- copies or substantial portions of the Software.
-- 
-- THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND.

-- -*- coding: utf-8 -*-
-- ------------------------------------------------------------------------------


-- File: sql/schema.sql

PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

-- -------------------------
-- Dimensions
-- -------------------------
CREATE TABLE IF NOT EXISTS taxon_dim (
  taxon_id INTEGER PRIMARY KEY,
  scientific_name TEXT,
  swedish_name TEXT,
  updated_at_utc TEXT NOT NULL
);

-- -------------------------
-- Raw per-taxon grid layers
-- -------------------------
-- year: 0 = all-years aggregate, otherwise actual year
-- slot_id: 0 = all-time (within that year bucket), 1..48 = time buckets
CREATE TABLE IF NOT EXISTS taxon_grid (
  taxon_id INTEGER NOT NULL,
  zoom INTEGER NOT NULL,
  year INTEGER NOT NULL,
  slot_id INTEGER NOT NULL,
  x INTEGER NOT NULL,
  y INTEGER NOT NULL,
  observations_count INTEGER NOT NULL,
  taxa_count INTEGER NOT NULL,
  bbox_top_lat REAL NOT NULL,
  bbox_left_lon REAL NOT NULL,
  bbox_bottom_lat REAL NOT NULL,
  bbox_right_lon REAL NOT NULL,
  fetched_at_utc TEXT NOT NULL,
  PRIMARY KEY (taxon_id, zoom, year, slot_id, x, y)
);

CREATE TABLE IF NOT EXISTS taxon_layer_state (
  taxon_id INTEGER NOT NULL,
  zoom INTEGER NOT NULL,
  year INTEGER NOT NULL,
  slot_id INTEGER NOT NULL,
  last_fetch_utc TEXT NOT NULL,
  payload_sha256 TEXT NOT NULL,
  grid_cell_count INTEGER NOT NULL,
  PRIMARY KEY (taxon_id, zoom, year, slot_id)
);

-- -------------------------
-- Derived hotmap layers
-- -------------------------
CREATE TABLE IF NOT EXISTS grid_hotmap (
  zoom INTEGER NOT NULL,
  year INTEGER NOT NULL,
  slot_id INTEGER NOT NULL,
  x INTEGER NOT NULL,
  y INTEGER NOT NULL,
  coverage INTEGER NOT NULL,
  score REAL NOT NULL,
  bbox_top_lat REAL NOT NULL,
  bbox_left_lon REAL NOT NULL,
  bbox_bottom_lat REAL NOT NULL,
  bbox_right_lon REAL NOT NULL,
  updated_at_utc TEXT NOT NULL,
  PRIMARY KEY (zoom, year, slot_id, x, y)
);

CREATE TABLE IF NOT EXISTS hotmap_taxa_set (
  zoom INTEGER NOT NULL,
  year INTEGER NOT NULL,
  slot_id INTEGER NOT NULL,
  taxon_id INTEGER NOT NULL,
  PRIMARY KEY (zoom, year, slot_id, taxon_id)
);

-- -------------------------
-- Views for taxa listing per hotmap cell (year-aware)
--
-- IMPORTANT:
-- We DROP first so view definitions always update when schema.sql changes.
-- SQLite's "CREATE VIEW IF NOT EXISTS" will otherwise keep old definitions forever.
-- -------------------------

DROP VIEW IF EXISTS grid_hotmap_taxa_names_v;
DROP VIEW IF EXISTS grid_hotmap_v;
DROP VIEW IF EXISTS grid_hotspot_taxa_v;
DROP VIEW IF EXISTS grid_taxa_v;

CREATE VIEW grid_taxa_v AS
SELECT
  tg.zoom, tg.year, tg.slot_id, tg.x, tg.y, tg.taxon_id,
  COALESCE(td.scientific_name, '') AS scientific_name,
  COALESCE(td.swedish_name, '') AS swedish_name,
  tg.observations_count
FROM taxon_grid tg
LEFT JOIN taxon_dim td ON td.taxon_id = tg.taxon_id;

CREATE VIEW grid_hotspot_taxa_v AS
SELECT
  gh.zoom, gh.year, gh.slot_id, gh.x, gh.y,
  gh.coverage, gh.score,
  gh.bbox_top_lat, gh.bbox_left_lon, gh.bbox_bottom_lat, gh.bbox_right_lon,
  gt.taxon_id, gt.scientific_name, gt.swedish_name, gt.observations_count,
  gh.updated_at_utc
FROM grid_hotmap gh
JOIN grid_taxa_v gt
  ON gt.zoom=gh.zoom
 AND gt.year=gh.year
 AND gt.slot_id=gh.slot_id
 AND gt.x=gh.x
 AND gt.y=gh.y;

-- “hotmap cells with some extra rollups” (optional)
CREATE VIEW grid_hotmap_v AS
SELECT
  h.zoom, h.year, h.slot_id, h.x, h.y, h.coverage, h.score,
  h.bbox_top_lat    AS topLeft_lat,
  h.bbox_left_lon   AS topLeft_lon,
  h.bbox_bottom_lat AS bottomRight_lat,
  h.bbox_right_lon  AS bottomRight_lon,
  (h.bbox_top_lat + h.bbox_bottom_lat) / 2.0 AS centroid_lat,
  (h.bbox_left_lon + h.bbox_right_lon) / 2.0 AS centroid_lon,
  COALESCE(SUM(
    CASE WHEN s.taxon_id IS NOT NULL THEN t.observations_count ELSE 0 END
  ), 0) AS obs_total,
  GROUP_CONCAT(
    CASE WHEN s.taxon_id IS NOT NULL THEN CAST(t.taxon_id AS TEXT) END,
    ';'
  ) AS taxa_list,
  h.updated_at_utc
FROM grid_hotmap h
LEFT JOIN taxon_grid t
  ON t.zoom=h.zoom
 AND t.year=h.year
 AND t.slot_id=h.slot_id
 AND t.x=h.x
 AND t.y=h.y
LEFT JOIN hotmap_taxa_set s
  ON s.zoom=t.zoom
 AND s.year=t.year
 AND s.slot_id=t.slot_id
 AND s.taxon_id=t.taxon_id
GROUP BY
  h.zoom, h.year, h.slot_id, h.x, h.y, h.coverage, h.score,
  h.bbox_top_lat, h.bbox_left_lon, h.bbox_bottom_lat, h.bbox_right_lon,
  h.updated_at_utc;

-- “taxa names per hotmap cell” (this is what many UIs want)
CREATE VIEW grid_hotmap_taxa_names_v AS
SELECT
  h.zoom, h.year, h.slot_id, h.x, h.y,
  t.taxon_id,
  COALESCE(d.scientific_name,'') AS scientific_name,
  COALESCE(d.swedish_name,'')    AS swedish_name,
  t.observations_count
FROM grid_hotmap h
JOIN taxon_grid t
  ON t.zoom=h.zoom
 AND t.year=h.year
 AND t.slot_id=h.slot_id
 AND t.x=h.x
 AND t.y=h.y
JOIN hotmap_taxa_set s
  ON s.zoom=t.zoom
 AND s.year=t.year
 AND s.slot_id=t.slot_id
 AND s.taxon_id=t.taxon_id
LEFT JOIN taxon_dim d
  ON d.taxon_id=t.taxon_id;

-- Helpful indexes (SQLite will also index PKs, but these help common filters)

CREATE INDEX IF NOT EXISTS idx_taxon_grid_cell
ON taxon_grid(zoom, year, slot_id, x, y);

CREATE INDEX IF NOT EXISTS idx_taxon_grid_taxon
ON taxon_grid(taxon_id, zoom, year, slot_id);

CREATE INDEX IF NOT EXISTS idx_taxon_grid_cell_obs
ON taxon_grid(zoom, year, slot_id, x, y, observations_count DESC);

CREATE INDEX IF NOT EXISTS idx_grid_hotmap_slot
ON grid_hotmap(zoom, year, slot_id, coverage, score);

CREATE INDEX IF NOT EXISTS idx_hotmap_taxa_set_slot
ON hotmap_taxa_set(zoom, year, slot_id, taxon_id);

CREATE INDEX IF NOT EXISTS idx_grid_hotmap_lookup
ON grid_hotmap (zoom, year, slot_id);

CREATE INDEX IF NOT EXISTS idx_taxon_grid_lookup
ON taxon_grid (taxon_id, zoom, year, slot_id);
