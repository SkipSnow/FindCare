"""Apple HIG conformance suite at iPhone 14 (390x844) viewport.

Source REQs:
  EPIC-008-F-001-S-001-REQ-B-003: reactive interface must support phones
    using an industry-standard style guide.
  EPIC-008-F-001-S-001: cite + comply with Apple HIG.
  EPIC-006-F-003-S-001: filter must support Galaxy + iPhone 14+.
  EPIC-006-F-019-S-003-REQ-B-001/B-002/B-003/B-004: chat iframe fills
    viewport height; input above keyboard; provider cards readable at 360.
  EPIC-006-F-019-S-004-REQ-B-001: 44x44 touch target per Apple HIG.

Run:  python -m pytest Code/ConversationalUX/FindCareChat/tests/test_iphone_hig_compliance.py -v
Run headed:  HEADED=1 python -m pytest ... -v
"""
from __future__ import annotations
import os
import pytest
from playwright.sync_api import sync_playwright


BASE_URL = "https://localhost/"
SEARCH_QUERY = "find me a bone doc in DE"
HEADED = os.environ.get("HEADED") in ("1", "true", "TRUE", "yes")
SLOW_MO = 200 if HEADED else 0

IPHONE_14 = {"width": 390, "height": 844}
IPHONE_16_PRO = {"width": 402, "height": 874}  # Skip's UAT device
GALAXY_S21 = {"width": 360, "height": 800}


@pytest.fixture(params=[
    ("iPhone16Pro", IPHONE_16_PRO),
    ("iPhone14", IPHONE_14),
    ("GalaxyS21", GALAXY_S21),
], ids=lambda p: p[0])
def env(request):
    label, viewport = request.param
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not HEADED, slow_mo=SLOW_MO)
        ctx = browser.new_context(
            ignore_https_errors=True,
            viewport=viewport,
            device_scale_factor=3,
            is_mobile=True,
            has_touch=True,
            user_agent=(
                "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
                if "iPhone" in label else
                "Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0 Mobile Safari/537.36"
            ),
        )
        page = ctx.new_page()
        page.goto(BASE_URL, wait_until="networkidle")
        yield {"page": page, "viewport": viewport, "label": label, "browser": browser}
        browser.close()


def _bbox(loc):
    return loc.bounding_box()


# ════════════════════════════════════════════════════════════════════
# Pre-flight: page reaches the chat surface on phone viewport.
# ════════════════════════════════════════════════════════════════════

class Test_EPIC_006_F_019_S_003_REQ_B_001_chat_iframe_fills_viewport:
    """Chat iframe fills the viewport height on Galaxy/iPhone phone."""
    def test_EPIC_006_F_019_S_003_REQ_B_001(self, env):
        page = env["page"]
        frame_el = page.locator("#coreChatFrame")
        frame_el.wait_for(state="visible", timeout=20000)
        box = _bbox(frame_el)
        assert box is not None, "chat iframe has no bbox"
        # iframe must occupy a substantive portion of viewport height
        # (allow some chrome above/below: banner, control frame).
        vp_h = env["viewport"]["height"]
        assert box["height"] >= vp_h * 0.50, (
            f"Chat iframe is too short on {env['label']}: "
            f"iframe.h={box['height']} viewport.h={vp_h}"
        )


# ════════════════════════════════════════════════════════════════════
# REQ-T-001 (Apple HIG): viewport-fit=cover declared.
# ════════════════════════════════════════════════════════════════════

class Test_EPIC_008_F_001_S_001_REQ_T_001_viewport_fit_cover:
    """The viewport meta MUST include viewport-fit=cover so safe-area
    insets resolve to non-zero on iPhone notched/Dynamic-Island devices."""
    def test_EPIC_008_F_001_S_001_REQ_T_001(self, env):
        meta = env["page"].locator("meta[name='viewport']").first
        content = meta.get_attribute("content") or ""
        assert "viewport-fit=cover" in content, (
            f"Viewport meta missing viewport-fit=cover: {content!r}"
        )
        assert "width=device-width" in content, (
            f"Viewport meta missing width=device-width: {content!r}"
        )


# ════════════════════════════════════════════════════════════════════
# REQ-T-001 (Apple HIG): safe-area-inset padding on body.
# ════════════════════════════════════════════════════════════════════

class Test_EPIC_008_F_001_S_001_REQ_T_001_safe_area_insets_consumed:
    """The body MUST consume env(safe-area-inset-*) via CSS so content
    clears the Dynamic Island / home indicator. Test by reading the
    resolved computedStyle for padding rules — values may be 0 on a
    non-notched simulator viewport, but the CSS rule MUST be present."""
    def test_EPIC_008_F_001_S_001_REQ_T_001(self, env):
        css = env["page"].evaluate(
            "() => Array.from(document.styleSheets)"
            ".flatMap(s => { try { return Array.from(s.cssRules); } catch { return []; } })"
            ".map(r => r.cssText).join('\\n')"
        )
        assert "safe-area-inset-top" in css, "CSS missing env(safe-area-inset-top)"
        assert "safe-area-inset-bottom" in css, "CSS missing env(safe-area-inset-bottom)"


# ════════════════════════════════════════════════════════════════════
# REQ-T-001 (Apple HIG) + REQ-B-001 mobile: 44x44 touch target.
# ════════════════════════════════════════════════════════════════════

class Test_EPIC_006_F_019_S_004_REQ_B_001_touch_targets_44x44:
    """Every visible interactive control on the phone viewport MUST
    present at least a 44x44 css-pixel hit target. We measure the
    chat input + send button + (when present) Evaluate button + mobile
    filter FAB. The filter rows + macros are tested separately once a
    query has resolved."""
    def test_EPIC_006_F_019_S_004_REQ_B_001(self, env):
        page = env["page"]
        # Trigger a search so the providers + filter render.
        chat = page.locator("#coreChatFrame").content_frame
        assert chat is not None, "chat iframe missing"
        chat.locator("input[placeholder*='Type a message'], textarea").first.fill(SEARCH_QUERY)
        chat.locator("input[placeholder*='Type a message'], textarea").first.press("Enter")
        chat.locator("[data-testid='available-providers']").wait_for(timeout=40000)

        # Targets on the page (parent + chat iframe + filter sub-iframe).
        offenders = []

        def _check(loc, label):
            if loc.count() == 0 or not loc.is_visible():
                return
            box = loc.bounding_box()
            if box is None: return
            if box["width"] < 44 or box["height"] < 44:
                offenders.append(f"{label}: {int(box['width'])}x{int(box['height'])}")

        # Chat iframe: Send + input
        _check(chat.locator("button:has-text('Send')").first, "chat:Send")
        _check(chat.locator("input[placeholder*='Type a message'], textarea").first, "chat:input")

        # Filter sub-iframe (if mobile-filter-open is set the iframe is visible;
        # otherwise FAB is the entry point — both must meet 44x44).
        filt = next((f for f in page.frames if "mode=filter" in (f.url or "")), None)
        if filt is not None:
            _check(filt.locator("[data-testid='apply-filter-button']"), "filter:Apply")
            _check(filt.locator("[data-testid='toggle-all-button']"), "filter:Toggle")
            # First specialty row
            row = filt.locator(".specialty-filter__row").first
            if row.count() > 0:
                # row may not be visible if overlay is closed; force the overlay
                # open so we can measure.
                page.evaluate(
                    "document.getElementById('leftPanel').classList.add('mobile-filter-open');"
                    "document.body.classList.add('filter-overlay-open');"
                )
                page.wait_for_timeout(200)
                _check(filt.locator(".specialty-filter__row").first, "filter:row[0]")
                _check(filt.locator("label.specialty-filter__macro-checkbox").first, "filter:macro")
                # Close again
                page.evaluate(
                    "document.getElementById('leftPanel').classList.remove('mobile-filter-open');"
                    "document.body.classList.remove('filter-overlay-open');"
                )

        # Mobile filter re-open FAB
        _check(page.locator("#mobileFilterReopen"), "mobile:FilterFAB")

        # Menu toggle (hamburger)
        _check(page.locator("#menuToggle"), "header:menuToggle")

        assert not offenders, (
            f"[{env['label']}] Touch targets below 44x44: " + "; ".join(offenders)
        )


# ════════════════════════════════════════════════════════════════════
# REQ-B-004: provider cards readable, no horizontal scroll at 360 width.
# ════════════════════════════════════════════════════════════════════

class Test_EPIC_006_F_019_S_003_REQ_B_001_providers_visible_first_on_phone:
    """On phone, the first results land with providers visible — NOT
    occluded by an auto-opened filter overlay. Matches the desktop model
    where providers are the primary surface."""
    def test_phone_providers_visible_first(self, env):
        page = env["page"]
        chat = page.locator("#coreChatFrame").content_frame
        assert chat is not None, "chat iframe missing"
        chat.locator("input[placeholder*='Type a message'], textarea").first.fill(SEARCH_QUERY)
        chat.locator("input[placeholder*='Type a message'], textarea").first.press("Enter")
        chat.locator("[data-testid='available-providers']").wait_for(timeout=45000)
        page.wait_for_timeout(800)
        # leftPanel must NOT have mobile-filter-open after first results
        lp_class = page.locator("#leftPanel").get_attribute("class") or ""
        assert "mobile-filter-open" not in lp_class, (
            f"[{env['label']}] Filter overlay auto-opened on first results — occludes providers. "
            f"leftPanel.class={lp_class!r}"
        )
        # body must NOT have filter-overlay-open
        body_class = page.locator("body").get_attribute("class") or ""
        assert "filter-overlay-open" not in body_class, (
            f"[{env['label']}] body.filter-overlay-open set on first results"
        )
        # Filter FAB MUST be visible (the gate body.has-filter-results
        # without filter-overlay-open).
        fab = page.locator("#mobileFilterReopen")
        assert fab.count() > 0, f"[{env['label']}] mobile-filter FAB missing"
        assert fab.is_visible(), f"[{env['label']}] mobile-filter FAB not visible after first results"


class Test_EPIC_006_F_019_S_003_REQ_B_004_no_horizontal_scroll_360:
    """At 360px width (Galaxy S21), the rendered provider list must
    not require horizontal scroll."""
    def test_EPIC_006_F_019_S_003_REQ_B_004(self, env):
        if env["label"] != "GalaxyS21":
            pytest.skip("This REQ pins Galaxy S21 (360x800). Skip on iPhone14.")
        page = env["page"]
        chat = page.locator("#coreChatFrame").content_frame
        assert chat is not None
        chat.locator("input[placeholder*='Type a message'], textarea").first.fill(SEARCH_QUERY)
        chat.locator("input[placeholder*='Type a message'], textarea").first.press("Enter")
        chat.locator("[data-testid='available-providers']").wait_for(timeout=40000)
        # Measure document scroll dimensions in the chat iframe.
        # FrameLocator has no evaluate(); read via the iframe's html
        # element locator instead.
        info = chat.locator("html").evaluate(
            "el => ({sw: el.scrollWidth, cw: el.clientWidth})"
        )
        assert info["sw"] <= info["cw"] + 1, (
            f"Horizontal scroll on 360px: scrollWidth={info['sw']} clientWidth={info['cw']}"
        )
