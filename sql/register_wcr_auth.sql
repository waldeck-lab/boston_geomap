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


-- File: sql/register_wcr_auth.sql

-- Invoke/register:  sqlite3 backend/resources/db/wcr_db.sqlite < boston_geomap/sql/register_wcr_auth.sql

INSERT OR IGNORE INTO auth_clients (client_key, display_name, description)
VALUES ('geomap', 'GeoMap Backend', 'Grid map and observation backend');

INSERT OR IGNORE INTO auth_grants (client_key, grant_name, description) VALUES
('geomap', 'read', 'Read restricted Geomap data'),
('geomap', 'jobs.read', 'Read Geomap job status'),
('geomap', 'jobs.run', 'Start Geomap rebuild/import jobs'),
('geomap', 'jobs.cancel', 'Cancel Geomap jobs'),
('geomap', 'admin', 'Full Geomap access');
