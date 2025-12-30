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

from __future__ import annotations

import math
from dataclasses import dataclass


EARTH_RADIUS_KM = 6371.0088

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))

# def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
#     """
#     Great-circle distance between two points (degrees) using Haversine formula.
#     Returns kilometers.
#     """
#     phi1 = math.radians(lat1)
#     phi2 = math.radians(lat2)
#     dphi = math.radians(lat2 - lat1)
#     dlmb = math.radians(lon2 - lon1)

#     a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlmb / 2.0) ** 2
#     c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
#     return EARTH_RADIUS_KM * c


def distance_weight_exp(d_km: float, d0_km: float) -> float:
    """
    Exponential decay weight: w = exp(-d/d0).
    d0_km is the characteristic distance (bigger => slower decay).
    """
    if d0_km <= 0:
        return 0.0
    return math.exp(-d_km / d0_km)


def distance_weight_rational(d_km: float, d0_km: float, gamma: float) -> float:
    """
    Rational decay: w = 1 / (1 + d/d0)^gamma.
    Often feels nicer than exp for map exploration.
    """
    if d0_km <= 0:
        return 0.0
    if gamma <= 0:
        gamma = 1.0
    return 1.0 / ((1.0 + d_km / d0_km) ** gamma)
