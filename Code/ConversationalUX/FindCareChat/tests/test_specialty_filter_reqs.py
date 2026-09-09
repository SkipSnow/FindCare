"""SpecialtyFilter feature — per-REQ Playwright regression suite.

Source: EPIC-006-F-003 requirements testable from the browser front-end.
Tool-internal reqs (vector search, AI normalize, embedding, clinical
relevance) are excluded except where they violate the Pydantic contract
that the widget consumes (REQ-T-005: no institutions in the rendered list).

Conventions per EPIC-006-F-003-S-006:
  REQ-T-001 — test file location is the per-service tests/ directory
  REQ-T-002 — one test per REQ, named by REQ id

Each class IS a single REQ. The class name carries the full ancestry
(epic.feature.story.req) plus a short descriptor. The single test method
inside each class also carries the full REQ id.

Each test gets a fresh page session via the function-scoped fixture, so
no state leaks across tests.

Run headed:    HEADED=1 python -m pytest Code/ConversationalUX/FindCareChat/tests/test_specialty_filter_reqs.py -v
Run headless:  python -m pytest Code/ConversationalUX/FindCareChat/tests/test_specialty_filter_reqs.py -v
"""
from __future__ import annotations
import os
import pytest
from playwright.sync_api import sync_playwright


BASE_URL = os.environ.get("SMOKE_TEST_URL", "https://localhost/")
SEARCH_QUERY = "find me a bone doc in DE"
HEADED = os.environ.get("HEADED") in ("1", "true", "TRUE", "yes")
SLOW_MO = 250 if HEADED else 0  # slow mode helps watch the suite


@pytest.fixture
def env():
    """Fresh browser session + query per test. Slow but isolated."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not HEADED, slow_mo=SLOW_MO)
        ctx = browser.new_context(ignore_https_errors=True)
        page = ctx.new_page()
        page.goto(BASE_URL, wait_until="networkidle")
        chat = page.locator("#coreChatFrame").content_frame
        assert chat is not None, "chat iframe not found"
        chat.locator("input[placeholder*='Type a message'], textarea").first.fill(SEARCH_QUERY)
        chat.locator("input[placeholder*='Type a message'], textarea").first.press("Enter")
        filt = None
        t = 0
        while t < 60000:
            page.wait_for_timeout(500); t += 500
            filt = next((f for f in page.frames if "mode=filter" in (f.url or "")), None)
            if filt is not None:
                try:
                    filt.locator("[data-testid='specialty-filter']").wait_for(timeout=2000)
                    break
                except Exception:
                    pass
        assert filt is not None, "filter sub-iframe not rendered"
        chat.locator("[data-testid='available-providers']").wait_for(timeout=30000)
        yield {"page": page, "chat": chat, "filt": filt}
        browser.close()


def _int(filt, testid: str) -> int:
    v = filt.locator(f"[data-testid='{testid}'] .specialty-filter__count-value").inner_text().strip()
    return int("".join(c for c in v if c.isdigit()) or "0")


# ════════════════════════════════════════════════════════════════════
# STORY S-001 — Display Specification
# ════════════════════════════════════════════════════════════════════

class Test_EPIC_006_F_002_S_001_REQ_T_002_panel_LEFT_desktop:
    def test_EPIC_006_F_002_S_001_REQ_T_002(self, env):
        leftPanel = env["page"].locator("#leftPanel")
        box = leftPanel.bounding_box()
        viewport = env["page"].viewport_size
        assert box is not None and viewport is not None
        assert box["x"] + box["width"] / 2 < viewport["width"] / 2, \
            f"leftPanel not on left side: x={box['x']} w={box['width']} viewport_w={viewport['width']}"


# REQ-T-004: panel takes 58% of left-panel vertical (header 18% / body 40%);
# header is 3 rows top-to-bottom at 20% / 50% / 30%. The session-bug had
# the header consuming ~60% of the filter (the count cell intercepted
# pointer events and the body collapsed to one row). This guard enforces
# the proportions on the live render. A ±4 percentage-point tolerance
# absorbs rounding + sub-pixel grid resolution.
class Test_EPIC_006_F_002_S_001_REQ_T_004_proportions:
    def test_EPIC_006_F_002_S_001_REQ_T_004(self, env):
        filt = env["filt"]
        spec = filt.locator("[data-testid='specialty-filter']").bounding_box()
        header = filt.locator(".specialty-filter__header").bounding_box()
        body = filt.locator("[data-testid='specialty-list']").bounding_box()
        assert spec and header and body
        # Inside the filter: header is 31% of the filter (= 18% of left
        # panel) and body is 69% (= 40% of left panel).
        header_pct = header["height"] / spec["height"] * 100
        body_pct = body["height"] / spec["height"] * 100
        assert abs(header_pct - 31) <= 4, (
            f"header is {header_pct:.1f}% of filter; REQ-T-004 requires 31% (= 18% of LP)"
        )
        assert abs(body_pct - 69) <= 4, (
            f"body is {body_pct:.1f}% of filter; REQ-T-004 requires 69% (= 40% of LP)"
        )
        # Inside the header: rows are 20% / 50% / 30% of header height.
        r1 = filt.locator(".specialty-filter__title-row").bounding_box()
        r2 = filt.locator(".specialty-filter__count-row").bounding_box()
        r3 = filt.locator(".specialty-filter__controls-row").bounding_box()
        assert r1 and r2 and r3
        r1_pct = r1["height"] / header["height"] * 100
        r2_pct = r2["height"] / header["height"] * 100
        r3_pct = r3["height"] / header["height"] * 100
        for label, pct, target in (
            ("title row", r1_pct, 20),
            ("count row", r2_pct, 50),
            ("controls row", r3_pct, 30),
        ):
            assert abs(pct - target) <= 4, (
                f"{label} is {pct:.1f}% of header; REQ-T-004 requires {target}%"
            )


class Test_EPIC_006_F_002_S_001_REQ_T_005_count_all_possible:
    def test_EPIC_006_F_002_S_001_REQ_T_005(self, env):
        cell = env["filt"].locator("[data-testid='count-all-possible']")
        assert cell.count() > 0
        assert _int(env["filt"], "count-all-possible") > 0, "All Possible value is 0"


class Test_EPIC_006_F_002_S_001_REQ_T_006_count_prescribers:
    def test_EPIC_006_F_002_S_001_REQ_T_006(self, env):
        cell = env["filt"].locator("[data-testid='count-all-prescribers']")
        assert cell.count() > 0


class Test_EPIC_006_F_002_S_001_REQ_T_007_count_your_choices:
    def test_EPIC_006_F_002_S_001_REQ_T_007(self, env):
        cell = env["filt"].locator("[data-testid='count-your-choices']")
        assert cell.count() > 0


class Test_EPIC_006_F_002_S_001_REQ_T_008_uncheck_check_all_label_flip:
    def test_EPIC_006_F_002_S_001_REQ_T_008(self, env):
        btn = env["filt"].locator("[data-testid='toggle-all-button']")
        assert btn.count() > 0
        # Initial state: ≥1 checked (Prescribers-macro-on default).
        # Click Uncheck All → 0 checked → label MUST be 'Check All'.
        for _ in range(2):
            label = btn.inner_text().strip()
            if label == "Uncheck All":
                btn.click(); env["page"].wait_for_timeout(300)
            else:
                break
        assert btn.inner_text().strip() == "Check All", \
            f"After clearing checks, label MUST be 'Check All', got {btn.inner_text()!r}"
        btn.click(); env["page"].wait_for_timeout(300)
        assert btn.inner_text().strip() == "Uncheck All", \
            f"After Check All, label MUST be 'Uncheck All', got {btn.inner_text()!r}"


class Test_EPIC_006_F_002_S_001_REQ_T_009_prescribers_macro_present:
    def test_EPIC_006_F_002_S_001_REQ_T_009(self, env):
        mac = env["filt"].locator("[data-testid='macro-prescribers']")
        assert mac.count() > 0


class Test_EPIC_006_F_002_S_001_REQ_T_010_homeopathic_below_prescribers:
    def test_EPIC_006_F_002_S_001_REQ_T_010(self, env):
        pres = env["filt"].locator("[data-testid='macro-prescribers']").bounding_box()
        homeo = env["filt"].locator("[data-testid='macro-homeopathic']").bounding_box()
        assert pres and homeo
        assert homeo["y"] > pres["y"], \
            f"Homeopathic MUST be BELOW Prescribers: pres.y={pres['y']} homeo.y={homeo['y']}"


class Test_EPIC_006_F_002_S_001_REQ_T_011_header_no_truncation:
    def test_EPIC_006_F_002_S_001_REQ_T_011(self, env):
        header = env["filt"].locator(".specialty-filter__header")
        clip = header.evaluate("el => ({clip: el.scrollWidth > el.clientWidth + 1})")
        assert not clip["clip"], "header content overflows (truncation)"


class Test_EPIC_006_F_002_S_001_REQ_T_012_specialty_rows_are_checkboxes:
    def test_EPIC_006_F_002_S_001_REQ_T_012(self, env):
        rows = env["filt"].locator(".specialty-filter__row")
        assert rows.count() > 0
        for i in range(min(rows.count(), 5)):
            r = rows.nth(i)
            assert r.get_attribute("aria-checked") in ("true", "false")
            assert r.get_attribute("data-spec-code")


class Test_EPIC_006_F_002_S_001_REQ_T_013_scroll_beyond_max:
    def test_EPIC_006_F_002_S_001_REQ_T_013(self, env):
        body = env["filt"].locator("[data-testid='specialty-list']")
        info = body.evaluate("el => ({sh: el.scrollHeight, ch: el.clientHeight})")
        rows_count = env["filt"].locator(".specialty-filter__row").count()
        if rows_count > 10:
            assert info["sh"] > info["ch"], \
                f"With {rows_count} rows the body MUST scroll: sh={info['sh']} ch={info['ch']}"


class Test_EPIC_006_F_002_S_001_REQ_T_014_all_possible_matches_total:
    def test_EPIC_006_F_002_S_001_REQ_T_014(self, env):
        total_rows = env["filt"].locator(".specialty-filter__row").count()
        all_possible = _int(env["filt"], "count-all-possible")
        assert total_rows == all_possible, f"All Possible ({all_possible}) != rows ({total_rows})"


class Test_EPIC_006_F_002_S_001_REQ_T_015_count_label_over_value:
    def test_EPIC_006_F_002_S_001_REQ_T_015(self, env):
        for tid in ("count-all-possible", "count-all-prescribers", "count-your-choices"):
            cell = env["filt"].locator(f"[data-testid='{tid}']")
            lbl_box = cell.locator(".specialty-filter__count-label").bounding_box()
            val_box = cell.locator(".specialty-filter__count-value").bounding_box()
            assert lbl_box and val_box, f"{tid}: missing bbox"
            assert lbl_box["y"] < val_box["y"], \
                f"{tid}: label MUST be above value (label.y={lbl_box['y']}, value.y={val_box['y']})"


class Test_EPIC_006_F_002_S_001_REQ_T_016_counts_react_to_row_toggle:
    def test_EPIC_006_F_002_S_001_REQ_T_016(self, env):
        before = _int(env["filt"], "count-your-choices")
        env["filt"].locator(".specialty-filter__row").first.click()
        env["page"].wait_for_timeout(300)
        after = _int(env["filt"], "count-your-choices")
        assert before != after, f"Your Choices MUST change on toggle: before={before} after={after}"


class Test_EPIC_006_F_002_S_001_REQ_T_017_apply_filter_at_bottom:
    def test_EPIC_006_F_002_S_001_REQ_T_017(self, env):
        apply_btn = env["filt"].locator("[data-testid='apply-filter-button']")
        assert apply_btn.count() > 0
        spec_box = env["filt"].locator("[data-testid='specialty-filter']").bounding_box()
        btn_box = apply_btn.bounding_box()
        assert spec_box and btn_box
        assert btn_box["y"] > spec_box["y"], "Apply Filter not at bottom of filter region"


# ════════════════════════════════════════════════════════════════════
# STORY S-002 — Present Initial Filter
# ════════════════════════════════════════════════════════════════════

class Test_EPIC_006_F_002_S_002_REQ_T_002_default_check_state_per_macro:
    """Default per Prescribers-macro-on: can_prescribe rows CHECKED,
    others UNCHECKED. Fresh-state fixture means this is the initial render."""
    def test_EPIC_006_F_002_S_002_REQ_T_002(self, env):
        rows = env["filt"].locator(".specialty-filter__row")
        mismatches = []
        for i in range(rows.count()):
            r = rows.nth(i)
            cp = r.get_attribute("data-can-prescribe") == "true"
            ac = r.get_attribute("aria-checked") == "true"
            if cp != ac:
                mismatches.append(r.get_attribute("data-spec-code"))
        assert not mismatches, \
            "Initial-state pattern violated (Prescribers-macro-on default): " + ", ".join(mismatches[:10])


class Test_EPIC_006_F_002_S_002_REQ_T_005_no_institutions_pydantic_contract:
    """Pydantic-contract: front-end filter rows MUST NOT include
    institutions/facilities/agencies. Front-end can't introspect
    SpecialtyMetaData.Section so we check the rendered specialty NAMES
    for institutional keywords. Real categorical check sits server-side
    via entity_type_code='1' on /search (Bug 4 fix)."""
    INST_WORDS = ("hospital", "facility", "agency", "clinic, ", "ambulance",
                  "laboratory", "supplier", "nursing facility",
                  "treatment center", "treatment facility")
    def test_EPIC_006_F_002_S_002_REQ_T_005(self, env):
        rows = env["filt"].locator(".specialty-filter__row")
        offenders = []
        for i in range(rows.count()):
            r = rows.nth(i)
            name = (r.inner_text() or "").lower()
            for kw in self.INST_WORDS:
                if kw in name:
                    offenders.append(f"{r.get_attribute('data-spec-code')}: contains {kw!r}")
                    break
        assert not offenders, \
            "REQ-T-005 institutional names in filter list: " + "; ".join(offenders[:10])


# ════════════════════════════════════════════════════════════════════
# STORY S-003 — User Manages Filter
# ════════════════════════════════════════════════════════════════════

class Test_EPIC_006_F_002_S_003_REQ_T_001_uncheck_individual_specialty:
    def test_EPIC_006_F_002_S_003_REQ_T_001(self, env):
        rows = env["filt"].locator(".specialty-filter__row[aria-checked='true']")
        assert rows.count() > 0, "no checked rows to uncheck — initial state should have ≥1"
        code = rows.first.get_attribute("data-spec-code")
        target = env["filt"].locator(f".specialty-filter__row[data-spec-code='{code}']")
        target.click()
        # Poll for the aria-checked attribute to flip (React re-render race).
        deadline = 3000
        while deadline > 0:
            if target.get_attribute("aria-checked") == "false":
                break
            env["page"].wait_for_timeout(100); deadline -= 100
        assert target.get_attribute("aria-checked") == "false", "row did not uncheck on click"


class Test_EPIC_006_F_002_S_003_REQ_T_004_macro_toggle_no_backend:
    def test_EPIC_006_F_002_S_003_REQ_T_004(self, env):
        page = env["page"]
        captured = {"classify": 0, "search": 0}
        def _on_request(req):
            if req.method != "POST": return
            if req.url.endswith("/nucc/classify"): captured["classify"] += 1
            elif req.url.endswith("/search"): captured["search"] += 1
        page.on("request", _on_request)
        try:
            env["filt"].locator("[data-testid='macro-prescribers']").click()
            page.wait_for_timeout(800)
            env["filt"].locator("[data-testid='macro-prescribers']").click()
            page.wait_for_timeout(800)
        finally:
            page.remove_listener("request", _on_request)
        assert captured["classify"] == 0 and captured["search"] == 0, \
            f"Macro toggle MUST NOT trigger backend: {captured}"


class Test_EPIC_006_F_002_S_003_REQ_T_006_count_all_possible_value:
    def test_EPIC_006_F_002_S_003_REQ_T_006(self, env):
        all_possible = _int(env["filt"], "count-all-possible")
        total_rows = env["filt"].locator(".specialty-filter__row").count()
        assert all_possible == total_rows, f"All Possible {all_possible} != rows {total_rows}"


class Test_EPIC_006_F_002_S_003_REQ_T_007_count_prescribers_value:
    def test_EPIC_006_F_002_S_003_REQ_T_007(self, env):
        all_pres = _int(env["filt"], "count-all-prescribers")
        rows_pres = env["filt"].locator(".specialty-filter__row[data-can-prescribe='true']").count()
        assert all_pres == rows_pres, f"All Prescribers {all_pres} != rows-with-can_prescribe {rows_pres}"


class Test_EPIC_006_F_002_S_003_REQ_T_008_count_your_choices_value:
    def test_EPIC_006_F_002_S_003_REQ_T_008(self, env):
        choices = _int(env["filt"], "count-your-choices")
        checked = env["filt"].locator(".specialty-filter__row[aria-checked='true']").count()
        assert choices == checked, f"Your Choices {choices} != checked rows {checked}"


class Test_EPIC_006_F_002_S_003_REQ_T_010_client_side_filtering_no_backend:
    def test_EPIC_006_F_002_S_003_REQ_T_010(self, env):
        page = env["page"]
        captured = {"classify": 0, "search": 0}
        def _on_request(req):
            if req.method != "POST": return
            if req.url.endswith("/nucc/classify"): captured["classify"] += 1
            elif req.url.endswith("/search"): captured["search"] += 1
        page.on("request", _on_request)
        try:
            row = env["filt"].locator(".specialty-filter__row").first
            row.click(); page.wait_for_timeout(600)
            row.click(); page.wait_for_timeout(600)
        finally:
            page.remove_listener("request", _on_request)
        assert captured["classify"] == 0 and captured["search"] == 0, \
            f"Row toggle MUST NOT trigger backend: {captured}"


# ════════════════════════════════════════════════════════════════════
# STORY S-004 — User Submits Filter
# ════════════════════════════════════════════════════════════════════

class Test_EPIC_006_F_002_S_004_REQ_T_003_apply_filter_greyed_popup:
    POPUP_TEXT = ("There are no changes to apply, try a new prompt if this is the wrong list. "
                  "The AI agent will help you craft the right question")
    def test_EPIC_006_F_002_S_004_REQ_T_003(self, env):
        apply_btn = env["filt"].locator("[data-testid='apply-filter-button']")
        # Fresh state: no changes → button must be greyed.
        assert apply_btn.get_attribute("data-dirty") == "false", \
            f"Apply Filter must be greyed when no changes; data-dirty={apply_btn.get_attribute('data-dirty')!r}"
        apply_btn.click(); env["page"].wait_for_timeout(400)
        popup = env["filt"].locator("[data-testid='apply-filter-no-changes-popup']")
        assert popup.count() > 0 and popup.is_visible(), "no-changes popup did not appear"
        assert self.POPUP_TEXT in popup.inner_text(), f"popup text mismatch: {popup.inner_text()!r}"
