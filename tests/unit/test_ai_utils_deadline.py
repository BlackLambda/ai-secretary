"""Unit tests for deadline parsing in lib/ai_utils.py"""

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from lib.ai_utils import parse_deadline, drop_items_with_past_deadlines


NOW = datetime(2026, 3, 16, 12, 0, 0, tzinfo=timezone.utc)


def test_parse_deadline_iso_utc():
    dt = parse_deadline("2026-06-01T10:00:00Z", now=NOW)
    assert dt is not None
    assert dt.year == 2026
    assert dt.month == 6


def test_parse_deadline_date_only():
    dt = parse_deadline("2026-06-01", now=NOW)
    assert dt is not None
    assert dt.hour == 23
    assert dt.minute == 59


def test_parse_deadline_none():
    assert parse_deadline(None) is None


def test_parse_deadline_invalid_string():
    assert parse_deadline("not a date") is None


def test_parse_deadline_dict_graph_style():
    dt = parse_deadline({"dateTime": "2026-07-15T09:00:00Z"}, now=NOW)
    assert dt is not None
    assert dt.month == 7


def test_drop_items_future_kept():
    items = [{"task": "future task", "deadline": "2027-01-01"}]
    kept, dropped = drop_items_with_past_deadlines(items, now=NOW)
    assert len(kept) == 1
    assert len(dropped) == 0


def test_drop_items_past_dropped():
    items = [{"task": "old task", "deadline": "2025-01-01"}]
    kept, dropped = drop_items_with_past_deadlines(items, now=NOW)
    assert len(kept) == 0
    assert len(dropped) == 1


def test_drop_items_no_deadline_kept():
    items = [{"task": "no deadline task"}]
    kept, dropped = drop_items_with_past_deadlines(items, now=NOW)
    assert len(kept) == 1
    assert len(dropped) == 0
