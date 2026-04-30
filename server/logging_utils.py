# server/logging_utils.py

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

from logging.handlers import RotatingFileHandler
from pathlib import Path

import logging
import time

from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional

LOCAL_TZ = ZoneInfo("Europe/Stockholm")


def local_now_ts() -> float:
    return time.time()


def local_now_iso() -> str:
    return datetime.now(LOCAL_TZ).isoformat()


def iso_or_none(ts: Optional[float]) -> Optional[str]:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=LOCAL_TZ).isoformat()

class _LocalTZFormatter(logging.Formatter):
    # Produces: 2026-01-03T19:43:55.067+01:00

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=LOCAL_TZ)
        return dt.isoformat(timespec="milliseconds")

def setup_server_logger(
    name: str = "geomap-server",
    log_dir: Path | None = None,
    level: int = logging.INFO,
) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.handlers:
        return logger  # already configured

    fmt = _LocalTZFormatter(
        "%(asctime)s %(levelname)-5s %(name)s: %(message)s"
    )
    
    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # File handler
    if log_dir:
        log_dir.mkdir(parents=True, exist_ok=True)
        fh = RotatingFileHandler(
            log_dir / "server/server.log",
            maxBytes=5_000_000,
            backupCount=5,
        )
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger
