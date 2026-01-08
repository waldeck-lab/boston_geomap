#!/usr/bin/env python3
# script:rebuild_hotmaps_from_db.py 

# MIT License
#
# Copyright (c) 2025 Jonas Waldeck
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND.

# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from geomap import storage  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description="Rebuild grid_hotmap from existing taxon_grid (offline).")
    ap.add_argument("--db", required=True, help="Path to geomap.sqlite")
    ap.add_argument("--zooms", default="15,14,13", help="Comma-separated zooms")
    ap.add_argument("--slots", default="", help="Comma-separated slot_ids to rebuild (e.g. 0,21,22,23)")
    ap.add_argument("--taxon-ids", default="", help="Comma-separated taxon_ids. If empty, infer from DB.")
    ap.add_argument("--alpha", type=float, default=2.0)
    ap.add_argument("--beta", type=float, default=0.5)
    args = ap.parse_args()

    db_path = Path(args.db).expanduser().resolve()
    zooms = [int(z.strip()) for z in args.zooms.split(",") if z.strip()]
    zooms = sorted(set(zooms), reverse=True)

    slots = None
    if args.slots.strip():
        slots = [int(s.strip()) for s in args.slots.split(",") if s.strip()]
        slots = sorted(set(slots))

    taxon_ids = None
    if args.taxon_ids.strip():
        taxon_ids = [int(t.strip()) for t in args.taxon_ids.split(",") if t.strip()]
        taxon_ids = sorted(set(taxon_ids))

    conn = storage.connect(db_path)
    conn.isolation_level = None
    try:
        storage.ensure_schema(conn)

        if taxon_ids is None:
            taxon_ids = [r[0] for r in conn.execute("SELECT DISTINCT taxon_id FROM taxon_grid ORDER BY taxon_id;")]
        if not taxon_ids:
            print("No taxon_id found in taxon_grid. Nothing to do.")
            return 2

        if slots is None:
            slots = [r[0] for r in conn.execute("SELECT DISTINCT slot_id FROM taxon_grid ORDER BY slot_id;")]

        print(f"[rebuild] db={db_path}")
        print(f"[rebuild] taxon_ids={len(taxon_ids)}")
        print(f"[rebuild] zooms={zooms}")
        print(f"[rebuild] slots={slots}")
        print(f"[rebuild] alpha={args.alpha} beta={args.beta}")

        for slot_id in slots:
            for z in zooms:
                with conn:
                    storage.rebuild_hotmap(conn, z, slot_id, taxon_ids, alpha=args.alpha, beta=args.beta)
                n = conn.execute(
                    "SELECT COUNT(*) FROM grid_hotmap WHERE zoom=? AND slot_id=?;",
                    (z, slot_id),
                ).fetchone()[0]
                print(f"[rebuild] slot={slot_id} zoom={z} grid_hotmap rows={int(n)}")

        print("[rebuild] done")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
