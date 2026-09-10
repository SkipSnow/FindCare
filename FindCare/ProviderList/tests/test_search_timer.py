# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
#
# EPIC-006-F-001-S-001-REQ-B-006: Search timer runs continuously until results displayed.
# Timer must not stall between classify and DB search.
#
# Playwright SIT test against full parent page (DR-019).
#
# Usage:
#   pytest test_search_timer.py -v --headed   (watch the browser)
#   pytest test_search_timer.py -v            (headless)

import os
import pytest
from playwright.sync_api import Page, expect

BASE_URL = os.getenv("SIT_BASE_URL", "https://localhost")

SCREENSHOT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..", "_oneshots/test_output")
os.makedirs(SCREENSHOT_DIR, exist_ok=True)


def _get_chat_frame(page: Page):
    """Get the chat iframe from the parent page."""
    page.goto(BASE_URL, wait_until="networkidle")
    page.wait_for_timeout(5000)
    for frame in page.frames:
        if "hf.space" in frame.url or ":5173" in frame.url or ":3000" in frame.url:
            frame.wait_for_timeout(3000)
            return frame
    return page


class TestSearchTimer:
    """EPIC-006-F-001-S-001-REQ-B-006: Timer runs until results displayed."""

    def test_timer_runs_until_results_displayed(self, page: Page):
        """Timer counts continuously from search submit to results render.
        Must not stall between classify and DB search."""
        frame = _get_chat_frame(page)

        # Type a search query
        input_el = frame.locator("input[placeholder='Type a message...']")
        input_el.fill("find me a doctor for a broken ankle in DE")
        frame.locator("button:has-text('Send')").click()

        # Timer should start — wait for the seconds counter to appear
        timer_el = frame.locator("text=/\\d+s/")
        timer_el.wait_for(timeout=5000)

        # Capture first reading
        first_text = timer_el.text_content()
        first_seconds = int(first_text.replace("s", ""))

        # Wait 3 seconds and verify timer advanced (did not stall)
        page.wait_for_timeout(3000)

        # Timer should still be visible if search is still running
        # OR results should be displayed (timer cleared because search completed)
        results_visible = frame.locator("text=/Available Providers/").is_visible()

        if not results_visible:
            # Search still running — timer must have advanced
            current_text = timer_el.text_content()
            current_seconds = int(current_text.replace("s", ""))
            assert current_seconds > first_seconds, (
                f"Timer stalled at {first_seconds}s — did not advance after 3 seconds. "
                f"timer clears too early between classify and DB search."
            )

        # Wait for results to appear (max 30s for slow DB without vector index)
        results_header = frame.locator("text=/Available Providers/")
        results_header.wait_for(timeout=30000)

        # Once results are visible, timer should be gone (cleared)
        # The searching phase div with the timer should no longer be rendered
        expect(frame.locator("text=/Waiting for response/")).not_to_be_visible(timeout=2000)

        page.screenshot(path=os.path.join(SCREENSHOT_DIR, "timer_test_results.png"))
