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

# geomap/timeslots.py
from __future__ import annotations
from dataclasses import dataclass
from datetime import date
import calendar

@dataclass(frozen=True)
class TimeSlot:
    slot_id: int          # 1..48
    month: int            # 1..12
    quartile: int         # 1..4
    start_day: int        # 1..31
    end_day: int          # 1..31

def month_quartile_slot(month: int, day: int) -> int:
    # 1-7 => q1, 8-14 => q2, 15-21 => q3, else => q4
    q = 1 if day <= 7 else 2 if day <= 14 else 3 if day <= 21 else 4
    return (month - 1) * 4 + q

def slot_to_month_quartile(slot_id: int) -> tuple[int, int]:
    if not (1 <= slot_id <= 48):
        raise ValueError("slot_id must be 1..48")
    month = (slot_id - 1) // 4 + 1
    q = (slot_id - 1) % 4 + 1
    return month, q

def slot_bounds(month: int, quartile: int, *, year_for_days: int = 2001) -> TimeSlot:
    if quartile not in (1, 2, 3, 4):
        raise ValueError("quartile must be 1..4")
    if month not in range(1, 13):
        raise ValueError("month must be 1..12")

    start_day = {1: 1, 2: 8, 3: 15, 4: 22}[quartile]
    last = calendar.monthrange(year_for_days, month)[1]
    end_day = {1: 7, 2: 14, 3: 21, 4: last}[quartile]
    slot_id = (month - 1) * 4 + quartile
    return TimeSlot(slot_id, month, quartile, start_day, end_day)

def slot_from_date(d: date) -> int:
    return month_quartile_slot(d.month, d.day)

def format_slot(slot_id: int) -> str:
    m, q = slot_to_month_quartile(slot_id)
    return f"{m}.{q}"
