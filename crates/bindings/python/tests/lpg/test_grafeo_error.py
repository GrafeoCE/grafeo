"""Tests for GrafeoError: structured error codes on the Python side (L24).

`GrafeoError` is a subclass of `RuntimeError` so legacy `except RuntimeError:`
paths keep working. New code can catch `GrafeoError` and inspect
`e.error_code` (string, e.g. "GRAFEO-Q001") and `e.is_retryable` (bool).
"""

import grafeo
import pytest


def test_grafeo_error_is_runtime_error_subclass():
    assert issubclass(grafeo.GrafeoError, RuntimeError)


def test_parse_error_raises_grafeo_error_with_query_code():
    db = grafeo.GrafeoDB()
    with pytest.raises(grafeo.GrafeoError) as exc:
        db.execute("THIS IS NOT VALID GQL")
    err = exc.value
    # Legacy callers can still catch RuntimeError
    assert isinstance(err, RuntimeError)
    # New callers can inspect structured attributes
    assert err.error_code.startswith("GRAFEO-Q")
    assert err.is_retryable is False


def test_semantic_error_raises_grafeo_error():
    db = grafeo.GrafeoDB()
    with pytest.raises(grafeo.GrafeoError) as exc:
        db.execute("SESSION SET SCHEMA nonexistent_schema_xyz")
    err = exc.value
    # Error code present, stable prefix
    assert err.error_code.startswith("GRAFEO-")
    assert isinstance(err.is_retryable, bool)


def test_legacy_runtime_error_catch_still_works():
    db = grafeo.GrafeoDB()
    with pytest.raises(RuntimeError):
        db.execute("NOT VALID")
