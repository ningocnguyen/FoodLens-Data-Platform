"""Tests for the Open Food Facts API client."""

from __future__ import annotations

from typing import Any

import pytest

from src.api_client import OpenFoodFactsClient


class FakeResponse:
    def __init__(
        self,
        status_code: int,
        payload: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self.payload = payload or {}
        self.headers = headers or {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self) -> dict[str, Any]:
        return self.payload


def test_fetch_category_page_retries_503(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify transient 503 responses are retried."""

    client = OpenFoodFactsClient(
        base_url="https://example.test",
        user_agent="FoodLensTests/1.0",
        max_retries=3,
    )

    responses = [
        FakeResponse(503, headers={"Retry-After": "0"}),
        FakeResponse(200, payload={"products": [{"code": "123"}]}),
    ]

    monkeypatch.setattr(
        client.session,
        "get",
        lambda *args, **kwargs: responses.pop(0),
    )
    monkeypatch.setattr("src.api_client.time.sleep", lambda seconds: None)

    products = client.fetch_category_page(
        category="chocolates",
        page=1,
        page_size=10,
    )

    assert products == [{"code": "123"}]
    assert responses == []


def test_fetch_category_page_raises_after_retry_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify repeated 503 responses fail with a clear pipeline error."""

    client = OpenFoodFactsClient(
        base_url="https://example.test",
        user_agent="FoodLensTests/1.0",
        max_retries=2,
    )

    monkeypatch.setattr(
        client.session,
        "get",
        lambda *args, **kwargs: FakeResponse(503, headers={"Retry-After": "0"}),
    )
    monkeypatch.setattr("src.api_client.time.sleep", lambda seconds: None)

    with pytest.raises(RuntimeError, match="unavailable after 2 attempts"):
        client.fetch_category_page(
            category="chocolates",
            page=1,
            page_size=10,
        )
