"""Open Food Facts API client"""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)


def _parse_retry_after(value: str | None, default_seconds: int) -> int:
    """Return a usable Retry-After value in seconds."""

    if value is None:
        return default_seconds

    try:
        return max(0, int(value))
    except ValueError:
        logger.warning(
            "Open Food Facts returned an invalid Retry-After header: %s",
            value,
        )
        return default_seconds


class OpenFoodFactsClient:
    """Retrieve food products from Open Food Facts"""

    def __init__(
        self,
        base_url: str,
        user_agent: str,
        timeout_seconds: int = 30,
        max_retries: int = 3,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": user_agent,
                "Accept": "application/json",
            }
        )

    def fetch_category_page(
        self,
        category: str,
        page: int,
        page_size: int,
    ) -> list[dict[str, Any]]:
        """Retrieve one page of product records"""

        url = f"{self.base_url}/api/v2/search"

        parameters = {
            "categories_tags_en": category,
            "page": page,
            "page_size": page_size,
            "fields": (
                "code,"
                "product_name,"
                "brands,"
                "categories,"
                "countries,"
                "ingredients_text,"
                "allergens,"
                "nutrition_grades,"
                "last_modified_t,"
                "nutriments"
            ),
        }

        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(
                    "Requesting category=%s page=%s",
                    category,
                    page,
                )

                response = self.session.get(
                    url,
                    params=parameters,
                    timeout=self.timeout_seconds,
                )

                if response.status_code == 503:
                    if attempt == self.max_retries:
                        break

                    retry_after = _parse_retry_after(
                        response.headers.get("Retry-After"),
                        default_seconds=10 * attempt,
                    )

                    logger.warning(
                        "Open Food Facts returned 503. "
                        "Waiting %s seconds before retrying attempt %s/%s.",
                        retry_after,
                        attempt + 1,
                        self.max_retries,
                    )

                    time.sleep(retry_after)
                    continue

                response.raise_for_status()

                payload = response.json()
                products = payload.get("products", [])

                if not isinstance(products, list):
                    raise TypeError("API response did not contain a products list.")

                logger.info(
                    "Received %s products from page %s",
                    len(products),
                    page,
                )

                return products

            except requests.RequestException as error:
                logger.warning(
                    "Attempt %s/%s failed: %s",
                    attempt,
                    self.max_retries,
                    error,
                )

                if attempt == self.max_retries:
                    raise

                time.sleep(5 * attempt)

        raise RuntimeError(
            f"Open Food Facts was unavailable after {self.max_retries} attempts."
        )
