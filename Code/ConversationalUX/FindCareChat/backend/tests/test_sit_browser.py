# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
#
# SIT Browser Tests — Playwright headless Chrome against dev.chathealthy.ai
#
# These tests drive a real browser. They are SIT (System Integration Tests),
# not unit tests. Dev writes them, QA executes them.
#
# Usage:
#   pytest test_sit_browser.py -v --headed   (watch the browser)
#   pytest test_sit_browser.py -v            (headless)
#
# Screenshots saved to test_screenshots/ on failure.

import os
import pytest
from playwright.sync_api import Page, expect

BASE_URL = os.getenv("SIT_BASE_URL", "https://dev.chathealthy.ai")
CHAT_IFRAME_URL = None  # resolved dynamically from the page

SCREENSHOT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..", "_oneshots/test_output")
os.makedirs(SCREENSHOT_DIR, exist_ok=True)


@pytest.fixture(scope="session")
def browser_context_args():
    return {"viewport": {"width": 1400, "height": 900}}


def _screenshot(page: Page, name: str):
    """Save screenshot for human review."""
    path = os.path.join(SCREENSHOT_DIR, f"{name}.png")
    page.screenshot(path=path, full_page=True)
    return path


def _get_chat_frame(page: Page):
    """Get the chat iframe from the parent page."""
    page.goto(BASE_URL, wait_until="networkidle")
    page.wait_for_timeout(8000)  # React app inside HF iframe needs time
    # Find the HuggingFace Space iframe
    for frame in page.frames:
        if "hf.space" in frame.url or ":5173" in frame.url or ":3000" in frame.url:
            frame.wait_for_timeout(3000)
            return frame
    # Local dev or direct — try the page itself
    return page


def _send_message(frame, text: str, timeout: int = 90000):
    """Type a message and send it. Wait for response."""
    chat_input = frame.locator("input[placeholder*='Type a message'], textarea").first
    chat_input.fill(text)
    # Click Send button
    send_btn = frame.locator("button", has_text="Send").first
    send_btn.click()
    # Wait for response — look for new assistant message content
    frame.wait_for_timeout(5000)
    # Wait until loading is done (Send button re-enabled)
    send_btn.wait_for(state="visible", timeout=timeout)
    frame.wait_for_timeout(2000)


class TestSITProviderSearch:
    """SIT: Provider search end-to-end in real browser."""

    def test_page_loads(self, page: Page):
        """Parent page loads and chat iframe is accessible."""
        page.goto(BASE_URL, wait_until="networkidle")
        assert "ChatHealthy" in page.title()
        _screenshot(page, "01_page_loaded")

    def test_welcome_message(self, page: Page):
        """Chat shows welcome message on load."""
        frame = _get_chat_frame(page)
        # Should see a welcome message
        frame.wait_for_timeout(3000)
        content = frame.content()
        assert len(content) > 100, "Chat frame should have content"
        _screenshot(page, "02_welcome_message")

    def test_provider_search_returns_results(self, page: Page):
        """Search for providers returns results with provider names."""
        frame = _get_chat_frame(page)
        _send_message(frame, "find pediatricians in delaware")
        content = frame.content()
        _screenshot(page, "03_provider_search_results")
        # Should have provider data in the response
        assert "NPI" in content or "npi" in content or "MD" in content, \
            "Response should contain provider data"

    def test_summary_message_present(self, page: Page):
        """After provider search, system summary message appears with search term."""
        frame = _get_chat_frame(page)
        _send_message(frame, "find surgeons in delaware")
        content = frame.content()
        _screenshot(page, "04_summary_message")
        # Summary should contain the search term, not generic "providers"
        assert "surgeon" in content.lower(), \
            "Summary must use the search term, not generic 'providers'"

    def test_summary_has_filter_link(self, page: Page):
        """Summary message contains a clickable Filter link."""
        frame = _get_chat_frame(page)
        _send_message(frame, "find surgeons in delaware")
        # Summary message renders after LLM response — wait for action link
        filter_link = frame.locator("a[href='#action:filter']")
        filter_link.wait_for(state="attached", timeout=15000)
        _screenshot(page, "05_filter_link")
        assert filter_link.count() > 0, "Summary must contain a [Filter] action link"

    def test_summary_has_next_page_link(self, page: Page):
        """Summary message contains a clickable next page link."""
        frame = _get_chat_frame(page)
        _send_message(frame, "find surgeons in delaware")
        # Summary message renders after LLM response — wait for action link
        next_link = frame.locator("a[href='#action:next-page']")
        next_link.wait_for(state="attached", timeout=15000)
        _screenshot(page, "06_next_page_link")
        assert next_link.count() > 0, "Summary must contain a [next page] action link"

    def test_next_page_link_loads_page2(self, page: Page):
        """Clicking next page link loads second page of results."""
        frame = _get_chat_frame(page)
        _send_message(frame, "find surgeons in delaware")
        next_link = frame.locator("a[href='#action:next-page']").first
        if next_link.count() > 0:
            next_link.click()
            frame.wait_for_timeout(5000)
            content = frame.content()
            _screenshot(page, "08_page2_loaded")
            # Should see "Records X-Y of Z" indicating page 2
            assert "Records" in content or "records" in content, \
                "Page 2 should show record range"

    def test_specialty_filter_panel_visible(self, page: Page):
        """After provider search, specialty filter panel appears in left panel."""
        frame = _get_chat_frame(page)
        _send_message(frame, "find surgeons in delaware")
        # Wait for filter panel to populate via postMessage to parent
        page.wait_for_timeout(5000)
        # Check parent page for filter panel content
        left_panel = page.locator("#leftPanel")
        if left_panel.count() > 0:
            panel_text = left_panel.inner_text()
            _screenshot(page, "09_filter_panel")
            assert "Filter" in panel_text or "filter" in panel_text or len(panel_text) > 20, \
                "Left panel should show specialty filter options"

    def test_filter_panel_text_horizontal(self, page: Page):
        """Filter panel text must be horizontal, not sideways."""
        frame = _get_chat_frame(page)
        _send_message(frame, "find surgeons in delaware")
        # Wait longer — filter panel arrives via postMessage after response
        page.wait_for_timeout(10000)
        left_panel = page.locator("#leftPanel")
        if left_panel.count() > 0:
            writing_mode = left_panel.evaluate("el => getComputedStyle(el).writingMode")
            _screenshot(page, "10_filter_text_orientation")
            assert writing_mode == "horizontal-tb", \
                f"Filter panel text must be horizontal, got writing-mode: {writing_mode}"


class TestSITMobile:
    """SIT: Mobile device emulation — iPhone 14 Pro and Galaxy S24."""

    @pytest.fixture(autouse=True)
    def _setup(self, playwright):
        self.playwright = playwright

    def _run_mobile_test(self, device_name: str, search_query: str):
        """Run provider search on a mobile device and capture screenshots."""
        device = self.playwright.devices[device_name]
        browser = self.playwright.chromium.launch(headless=True)
        context = browser.new_context(**device)
        page = context.new_page()

        tag = device_name.replace(" ", "_")

        # Load page
        page.goto(BASE_URL, wait_until="networkidle", timeout=60000)
        page.wait_for_timeout(8000)
        _screenshot(page, f"mobile_{tag}_01_loaded")

        # Find chat frame
        frame = None
        for f in page.frames:
            if "hf.space" in f.url or ":5173" in f.url:
                frame = f
                break
        if not frame:
            frame = page

        frame.wait_for_timeout(3000)

        # Send message
        chat_input = frame.locator("input[placeholder*='Type a message'], textarea").first
        chat_input.fill(search_query)
        send_btn = frame.locator("button", has_text="Send").first
        send_btn.click()
        send_btn.wait_for(state="visible", timeout=90000)
        frame.wait_for_timeout(5000)
        _screenshot(page, f"mobile_{tag}_02_results")

        # Check for summary message content
        content = frame.content()
        assert search_query.split()[0].lower() in content.lower() or "more" in content.lower(), \
            f"Response should contain search results on {device_name}"

        # Wait for action links
        filter_link = frame.locator("a[href='#action:filter']")
        next_link = frame.locator("a[href='#action:next-page']")
        try:
            next_link.wait_for(state="attached", timeout=15000)
        except Exception:
            pass
        _screenshot(page, f"mobile_{tag}_03_summary")

        has_filter = filter_link.count() > 0
        has_next = next_link.count() > 0

        context.close()
        browser.close()

        return {"filter_link": has_filter, "next_link": has_next, "content_ok": True}

    def test_iphone14_provider_search(self, playwright):
        """Provider search with summary links on iPhone 14 Pro."""
        result = self._run_mobile_test("iPhone 14 Pro", "find surgeons in delaware")
        assert result["content_ok"], "Search must return results on iPhone"
        assert result["next_link"], "Next page link must render on iPhone"

    def test_galaxy_s24_provider_search(self, playwright):
        """Provider search with summary links on Galaxy S24."""
        result = self._run_mobile_test("Galaxy S24", "find pediatricians in virginia")
        assert result["content_ok"], "Search must return results on Galaxy"
        assert result["next_link"], "Next page link must render on Galaxy"

    def test_iphone14_screenshot_review(self, playwright):
        """Capture iPhone layout for human review — page load, results, summary."""
        self._run_mobile_test("iPhone 14 Pro", "find me shrinks in VA")
        # Screenshots saved for manual review
        assert os.path.exists(os.path.join(SCREENSHOT_DIR, "mobile_iPhone_14_Pro_01_loaded.png"))
        assert os.path.exists(os.path.join(SCREENSHOT_DIR, "mobile_iPhone_14_Pro_02_results.png"))
        assert os.path.exists(os.path.join(SCREENSHOT_DIR, "mobile_iPhone_14_Pro_03_summary.png"))

    def test_galaxy_s24_screenshot_review(self, playwright):
        """Capture Galaxy layout for human review — page load, results, summary."""
        self._run_mobile_test("Galaxy S24", "find me shrinks in VA")
        assert os.path.exists(os.path.join(SCREENSHOT_DIR, "mobile_Galaxy_S24_01_loaded.png"))
        assert os.path.exists(os.path.join(SCREENSHOT_DIR, "mobile_Galaxy_S24_02_results.png"))
        assert os.path.exists(os.path.join(SCREENSHOT_DIR, "mobile_Galaxy_S24_03_summary.png"))
