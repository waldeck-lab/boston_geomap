# geomap:sos_filters_ext.py

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
from typing import Any, Dict
from geomap.timeslots import slot_bounds

def build_timeslot_filter(slot_id: int) -> Dict[str, Any]:
    ts = slot_bounds(*__slot_to_mq(slot_id))  # or slot_to_month_quartile
    # TEMPLATE: update keys once you confirm SOS SearchFilter schema.
    #
    # Option A: if API supports "month/day" without year:
    return {
        "searchFilter": {
            "dateFilter": {
                "from": {"month": ts.month, "day": ts.start_day},
                "to":   {"month": ts.month, "day": ts.end_day},
            }
        }
    }

def __slot_to_mq(slot_id: int) -> tuple[int,int]:
    month = (slot_id - 1) // 4 + 1
    q = (slot_id - 1) % 4 + 1
    return month, q
