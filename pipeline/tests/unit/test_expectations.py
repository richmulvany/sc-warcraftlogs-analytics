"""Tests for shared expectation definitions."""

from pipeline.expectations.common_expectations import VALID_ID, VALID_TIMESTAMP


def test_valid_id_is_tuple_of_two_strings():
    assert isinstance(VALID_ID, tuple)
    assert len(VALID_ID) == 2
    assert all(isinstance(s, str) for s in VALID_ID)


def test_valid_timestamp_expression_contains_null_check():
    _, expression = VALID_TIMESTAMP
    assert "IS NOT NULL" in expression.upper()
