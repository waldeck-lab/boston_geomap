#!/usr/bin/env python3

# geomap:taxon_lists.py 

# MIT License
#
# Copyright (c) 2026 Jonas Waldeck
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

from pathlib import Path
from typing import Dict, Iterable, Tuple, Optional, List, Set
import csv



def read_taxon_ids_from_csv(csv_path: Path) -> List[int]:
    """
    Reads taxon ids from CSV where first column contains taxon_id.
    First row is assumed header.
    """

    if not csv_path.exists():
        raise FileNotFoundError(str(csv_path))

    out: List[int] = []
    seen: Set[int] = set()

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)

        header = next(reader, None)
        if header is None:
            return []

        for row in reader:
            if not row:
                continue

            try:
                tid = int(float(row[0]))
            except Exception:
                continue

            if tid <= 0:
                continue

            if tid in seen:
                continue

            seen.add(tid)
            out.append(tid)

    return out


def chunked(values: List[int], batch_size: int):
    if batch_size <= 0:
        yield values
        return

    for i in range(0, len(values), batch_size):
        yield values[i:i + batch_size]
    
