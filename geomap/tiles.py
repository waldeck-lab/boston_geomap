# geomap:tiles.py

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

import math
from typing import Tuple

def tile_bbox_latlon(x: int, y: int, z: int) -> Tuple[float, float, float, float]:
    """
    Returns (top_lat, left_lon, bottom_lat, right_lon) for slippy tiles (Web Mercator).
    """
    n = 2 ** z
    left_lon = x / n * 360.0 - 180.0
    right_lon = (x + 1) / n * 360.0 - 180.0

    def lat_from_ytile(yy: int) -> float:
        lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * yy / n)))
        return math.degrees(lat_rad)

    top_lat = lat_from_ytile(y)
    bottom_lat = lat_from_ytile(y + 1)
    return top_lat, left_lon, bottom_lat, right_lon
