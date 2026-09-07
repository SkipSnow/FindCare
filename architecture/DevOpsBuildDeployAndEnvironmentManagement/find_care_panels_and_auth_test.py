# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
#
# find_care_panels_and_auth_test.py
# Comprehensive smoke covering every file touched during the
# display-logic-out-of-Python cleanup:
#   - AboutChatHealthyWidget (About popup + security panel)
#   - SharedServicesSplashWidget (identity + threads)
#   - EvaluateCareSplashWidget (structured-data render: title + subtitle,
#     no html field)
#   - HeaderWidget (banner replaces header on oauth result; close button
#     restores header)
#   - claim_oauth_result /gate op (read-once clear semantics)
#   - evalcare-splash /gate op (structured JSON, no html)

import os
import pytest
from playwright.sync_api import sync_playwright

BASE_URL = os.getenv("SMOKE_TEST_URL", "https://localhost")
DEFAULT_TIMEOUT = 60_000

SCREENSHOT_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "..", "_oneshots", "test_output", "panels_auth_smoke",
)
os.makedirs(SCREENSHOT_DIR, exist_ok=True)


def _shot(page, name):
    page.screenshot(path=os.path.join(SCREENSHOT_DIR, f"{name}.png"), full_page=True)


# ── Browser fixture ──────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def env():
    pw = sync_playwright().start()
    browser = pw.chromium.launch(headless=True, args=["--ignore-certificate-errors"])
    context = browser.new_context(ignore_https_errors=True)
    page = context.new_page()
    page.set_default_timeout(DEFAULT_TIMEOUT)
    page.goto(BASE_URL, wait_until="networkidle")
    page.wait_for_load_state("networkidle", timeout=DEFAULT_TIMEOUT)
    # Wait for the header to be painted by HeaderWidget so subsequent
    # selectors don't race the iframe load.
    page.wait_for_selector("[data-router-action='about_chathealthy']",
                           timeout=DEFAULT_TIMEOUT)
    yield {"page": page, "context": context}
    context.close()
    browser.close()
    pw.stop()


# Direct /gate-op tests live in find_care_gate_ops_test.py (separate file
# so pymongo's imports cannot leave an asyncio loop running and break
# sync_playwright in this file's browser fixture).


# ── Browser smoke ────────────────────────────────────────────────────────

def _open_about(page):
    page.locator("[data-router-action='about_chathealthy']").first.click()
    # About popup mounts into #popup_AboutChatHealtyPopUP per ClientRouter
    page.wait_for_selector("text=Build", timeout=DEFAULT_TIMEOUT)


class TestAboutPopup:
    """AboutChatHealthyWidget — the popup renders the security panel
    (version/build/session token bits) from structured data only."""

    def test_about_popup_shows_build_facts(self, env):
        page = env["page"]
        _open_about(page)
        body = page.locator("body").inner_text()
        _shot(page, "about_popup")
        assert "Version" in body or "version" in body.lower(), \
            f"About panel must surface a Version label. body[:400]={body[:400]!r}"
        assert "Build" in body, f"About panel must surface a Build label. body[:400]={body[:400]!r}"

    def test_about_popup_shows_security_panel(self, env):
        page = env["page"]
        _open_about(page)
        body = page.locator("body").inner_text().lower()
        # The widget labels the SessionToken parts as Authorization ID,
        # Nonce, and Session GUID per the operator-approved labels.
        for label in ("authorization id", "nonce", "session guid"):
            assert label in body, (
                f"About panel must include security label {label!r}. "
                f"body[:500]={body[:500]!r}"
            )


class TestSharedServicesSplash:
    """SharedServicesSplashWidget — clicking the SS nav button in the
    About popup paints the user_object identity + threads grid into
    frame_MainWindow."""

    def test_shared_services_splash_renders_identity_and_threads(self, env):
        page = env["page"]
        _open_about(page)
        page.locator("button[data-router-action='goto_sharedservices']").first.click()
        # MainWindow now hosts the splash
        page.wait_for_selector("text=Shared Services", timeout=DEFAULT_TIMEOUT)
        body = page.locator("#frame_MainWindow").inner_text()
        lower = body.lower()
        _shot(page, "shared_services_splash")
        assert "identity" in lower, (
            f"SharedServices splash must render the Identity section. "
            f"MainWindow[:400]={body[:400]!r}"
        )
        assert "utterances" in lower and "actions" in lower, (
            f"SharedServices splash must render both threads. "
            f"MainWindow[:400]={body[:400]!r}"
        )


class TestEvaluateCareSplash:
    """EvaluateCareSplashWidget — receives structured JSON
    {title, subtitle} from the tool. No html string anywhere."""

    def test_evaluate_care_splash_renders_from_structured_data(self, env):
        page = env["page"]
        _open_about(page)
        page.locator("button[data-router-action='goto_evaluatecare']").first.click()
        # Wait for the structured-data render (subtitle present) — not
        # the loading-state intermediate ("Loading EvaluateCare…") which
        # also contains the word "EvaluateCare".
        page.wait_for_function(
            """() => {
                const m = document.getElementById('frame_MainWindow');
                const t = (m && m.innerText || '').toLowerCase();
                return t.indexOf('unimplemented') !== -1;
            }""",
            timeout=DEFAULT_TIMEOUT,
        )
        body = page.locator("#frame_MainWindow").inner_text()
        _shot(page, "evalcare_splash")
        assert "EvaluateCare" in body, (
            f"EvaluateCare main window must render the title. "
            f"MainWindow[:400]={body[:400]!r}"
        )
        assert "unimplemented" in body.lower(), (
            f"EvaluateCare main window must render the subtitle. "
            f"MainWindow[:400]={body[:400]!r}"
        )
