"""Unit tests for lib/task_vectorization.py"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from lib.task_vectorization import (
    dot,
    text_sha256,
    hash_embedding,
    extract_card_text,
    compute_focus_vector,
)


def test_dot_basic():
    assert dot([1, 0, 0], [0, 1, 0]) == pytest.approx(0.0)
    assert dot([1, 2, 3], [1, 2, 3]) == pytest.approx(14.0)


def test_dot_mismatched_length():
    # dot() raises ValueError on mismatched lengths
    with pytest.raises(ValueError):
        dot([1, 2, 3], [1, 2])


def test_text_sha256_stable():
    h1 = text_sha256("hello world")
    h2 = text_sha256("hello world")
    assert h1 == h2
    assert len(h1) == 64


def test_text_sha256_different_inputs():
    assert text_sha256("a") != text_sha256("b")


def test_hash_embedding_length():
    vec = hash_embedding("some task text", dim=64)
    assert len(vec) == 64


def test_hash_embedding_deterministic():
    v1 = hash_embedding("task a", dim=32)
    v2 = hash_embedding("task a", dim=32)
    assert v1 == v2


def test_extract_card_text_from_dict():
    card = {
        "type": "Outlook",
        "data": {"event_name": "Write report", "executive_summary": "Q4 analysis"},
    }
    text = extract_card_text(card)
    assert "Write report" in text


def test_compute_focus_vector_returns_list():
    vectors_by_id = {
        "card1": {"vector": hash_embedding("project alpha", dim=32)},
        "card2": {"vector": hash_embedding("quarterly review", dim=32)},
    }
    feedback_by_id = {"card1": "like", "card2": "like"}
    vec, stats = compute_focus_vector(vectors_by_id, feedback_by_id, dim=32)
    assert isinstance(vec, list)
    assert len(vec) == 32
