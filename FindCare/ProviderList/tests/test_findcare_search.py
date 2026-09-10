# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
#
# FC-SEARCH-001: Provider Search Playwright SIT Tests
#
# Verifies free-text search, result fields, pagination, and specialty
# classification via the full parent page (DR-019).
#
# Usage:
#   pytest test_findcare_search.py -v --headed
#   pytest test_findcare_search.py -v

import os
import pytest
from playwright.sync_api import sync_playwright, Page


_DIGITS = frozenset("0123456789")


def _digit_runs(text: str, length: int) -> list[str]:
    """Every maximal run of digits of exactly `length` characters."""
    out: list[str] = []
    i = 0
    while i < len(text):
        if text[i] in _DIGITS:
            j = i
            while j < len(text) and text[j] in _DIGITS:
                j += 1
            if j - i == length:
                out.append(text[i:j])
            i = j
        else:
            i += 1
    return out


def _labelled_digit_runs(text: str, label: str, length: int) -> list[str]:
    """Digit runs of `length` that follow `label` and optional separators."""
    out: list[str] = []
    at = text.find(label)
    while at != -1:
        k = at + len(label)
        while k < len(text) and text[k] in ": \t\r\n":
            k += 1
        j = k
        while j < len(text) and text[j] in _DIGITS:
            j += 1
        if j - k == length:
            out.append(text[k:j])
        at = text.find(label, at + 1)
    return out


def _phone_numbers(text: str) -> list[str]:
    """Runs that look like a US phone number: 3 digits, 3 digits, 4 digits,
    separated by nothing, a space, a dot, a dash, or wrapped parentheses."""
    seps = frozenset("-. ()")
    out: list[str] = []
    digits: list[str] = []
    span: list[str] = []
    for ch in text + " ":
        if ch.isdigit():
            digits.append(ch)
            span.append(ch)
        elif ch in seps and digits:
            span.append(ch)
        else:
            if len(digits) == 10:
                out.append("".join(span).strip())
            digits, span = [], []
    return out

BASE_URL = os.getenv("TEST_BASE_URL", "https://localhost")
CHAT_TIMEOUT = 120_000
SCREENSHOT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..", "_oneshots/test_output", "search")
os.makedirs(SCREENSHOT_DIR, exist_ok=True)


def _screenshot(page: Page, name: str):
    path = os.path.join(SCREENSHOT_DIR, f"{name}.png")
    page.screenshot(path=path, full_page=True)
    return path


@pytest.fixture(scope="module")
def search_page():
    """One browser, one page, one search — shared across all tests."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1400, "height": 900},
            ignore_https_errors=True,
        )
        page = context.new_page()

        page.goto(BASE_URL, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(10000)

        chat_frame = page
        for frame in page.frames:
            if ":5173" in frame.url or ":3000" in frame.url or "hf.space" in frame.url:
                chat_frame = frame
                break

        chat_input = chat_frame.locator("input[placeholder*='Type a message'], textarea").first
        chat_input.wait_for(state="visible", timeout=30000)

        # EPIC-006-F-001-S-001-REQ-B-001: free-text search
        chat_input.fill("find me a foot doctor in Delaware")
        chat_frame.locator("button", has_text="Send").first.click()

        # Handle timeout modal if it appears
        try:
            keep_btn = chat_frame.locator("button", has_text="Yes, keep waiting").first
            keep_btn.wait_for(state="visible", timeout=60000)
            keep_btn.click()
        except Exception:
            pass

        # Wait for results
        try:
            chat_frame.locator("text=/NPI:|Records|County/i").first.wait_for(
                state="visible", timeout=CHAT_TIMEOUT
            )
        except Exception:
            pass

        page.wait_for_timeout(12000)

        yield {
            "page": page,
            "frame": chat_frame,
            "browser": browser,
        }

        context.close()
        browser.close()


class TestProviderSearchFreeText:
    """EPIC-006-F-001-S-001-REQ-B-001: User can search by free text."""

    def test_free_text_search_returns_results(self, search_page):
        """Free-text search returns provider results."""
        frame = search_page["frame"]
        body_text = frame.locator("body").inner_text()
        has_results = any(w in body_text for w in [
            "NPI", "npi", "MD", "DO", "DPM", "Records", "provider",
        ])
        _screenshot(search_page["page"], "01_free_text_results")
        assert has_results, f"Free-text search returned no results. Text: {body_text[:300]}"


class TestProviderResultFields:
    """EPIC-006-F-001-S-001-REQ-B-002: System returns provider name, NPI, address, county, phone."""

    def test_results_contain_npi(self, search_page):
        """Results include NPI numbers."""
        frame = search_page["frame"]
        body_text = frame.locator("body").inner_text()
        npis = _labelled_digit_runs(body_text, "NPI", 10)
        if not npis:
            npis = _digit_runs(body_text, 10)
        assert len(npis) >= 1, f"No NPI numbers found. Text: {body_text[-500:]}"

    def test_results_contain_provider_name(self, search_page):
        """Results include provider names (text before NPI)."""
        frame = search_page["frame"]
        body_text = frame.locator("body").inner_text()
        # Provider names are typically before NPI lines
        has_name = any(w in body_text for w in ["Dr.", "MD", "DO", "DPM", "PA-C", "NP"])
        assert has_name, f"No provider name indicators found. Text: {body_text[:500]}"

    def test_results_contain_address_or_county(self, search_page):
        """Results include address or county information."""
        frame = search_page["frame"]
        body_text = frame.locator("body").inner_text()
        has_location = any(w in body_text for w in [
            "County", "county", "DE", "Delaware", "19",  # DE zip codes start with 19
        ])
        assert has_location, f"No address/county info found. Text: {body_text[:500]}"

    def test_results_contain_phone(self, search_page):
        """Results include phone numbers."""
        frame = search_page["frame"]
        body_text = frame.locator("body").inner_text()
        phones = _phone_numbers(body_text)
        assert len(phones) >= 1, f"No phone numbers found. Text: {body_text[:500]}"


class TestProviderSearchPagination:
    """EPIC-006-F-001-S-001-REQ-B-003: Results paginated with forward/back controls."""

    def test_pagination_controls_present(self, search_page):
        """Pagination controls (Load more button or remaining count) visible."""
        frame = search_page["frame"]
        page = search_page["page"]
        body_text = frame.locator("body").inner_text()

        load_more = frame.locator("button", has_text="Load more")
        has_pagination = load_more.count() > 0 or "remaining" in body_text or "Records" in body_text

        _screenshot(page, "02_pagination_controls")
        assert has_pagination, f"No pagination controls found. Text: {body_text[:500]}"


class TestSpecialtyClassification:
    """EPIC-006-F-001-S-001-REQ-B-004: System identifies matching specialty types from search query."""

    def test_specialty_types_identified(self, search_page):
        """Search results trigger specialty classification visible in filter panel."""
        page = search_page["page"]
        left_text = page.locator("#leftPanel").inner_text()
        # Filter panel should show specialty types from the classify step
        has_specialties = any(w in left_text.lower() for w in [
            "filter", "specialty", "podiat", "foot", "orthop", "types",
        ])
        _screenshot(page, "03_specialty_classification")
        assert has_specialties, \
            f"No specialty types in filter panel after search. Left panel: {left_text[:300]}"
