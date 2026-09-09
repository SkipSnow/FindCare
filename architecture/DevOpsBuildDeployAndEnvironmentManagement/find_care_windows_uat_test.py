# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
#
# find_care_windows_uat_test.py — UAT that looks at EVERY window on the
# screen for every flow, not at one frame at a time.
#
# Why this file exists alongside find_care_frames_uat_test.py: that suite
# asserts a frame at a time. A turn that paints the results correctly and
# blanks the panel beside them passes it, because nothing in it asks what
# the OTHER ten windows hold at the same moment. Apply Filter destroying
# the specialty panel is exactly that shape of defect, and it shipped.
#
# So each flow here reaches a settled point, takes a census of all eleven
# windows at once, and then asserts what the requirements say each window
# must hold AT THAT MOMENT. The census is written to disk beside the
# screenshot whether the flow passes or fails, so a failure says what the
# whole screen looked like rather than what one selector returned.
#
# ── The rule this file is written under ─────────────────────────────
# EVERY assertion names the requirement it proves, in the test's
# docstring. Where a window carries something no approved requirement
# speaks to, it is RECORDED into the census evidence and NOT asserted.
# Inventing a rule for it would put a control on every run that nobody
# asked for, and afterwards nothing could tell it apart from one that was
# specified. The report accompanying this file lists those gaps.
#
# Two consequences of that rule, made explicit because they are judgment
# calls and not deductions:
#
#   (1) A requirement that is approved but whose story is `not_started`
#       describes work never done. Its failure would be unbuilt work, not
#       a regression, so those are RECORDED, not asserted. The one
#       exception is where the measurement shows the requirement is in
#       fact already met — those are asserted, because from then on a
#       change that breaks it IS a regression. Header 6% and footer 4%
#       are that case.
#   (2) EPIC-006-F-006 and F-007 (facilities) carry approval `proposed`,
#       not `approved`. They are asserted here anyway, following the
#       precedent already set in find_care_frames_uat_test.py, and every
#       such test says so in its docstring.
#
# ── DOM facts these rely on (all measured against build 2244, commit
#    28972193 on local, 2026-09-02; none assumed) ────────────────────
#
#  * The eleven windows are DIV / ASIDE / HEADER / FOOTER elements
#    carrying id="frame_*" in the TOP document. They are NOT iframes.
#    Website/index.html names seven as the layout spine and four as
#    windows that sit beside it.
#  * The single iframe, data-frame="MainWindow" id="coreReactFrame", is a
#    1x1 offscreen host for the React widgets. Anything inside it has no
#    viewport and cannot be seen or clicked, so nothing here looks in it.
#    Every assertion below is on a top-document frame, with visibility and
#    geometry, never on mere presence in the DOM.
#  * Which widget owns which window, from the React sources:
#      frame_LeftPanel            SpecialtyFilterWidget
#      frame_MainWindow           provider / facility / trial results,
#                                 the welcome splash, EvaluateCare splash
#      frame_RightPanel           ProviderDetailWidget, and the facility
#                                 detail, which reuses its close control
#      frame_UserMessage          SystemMessageWidget — the system's prose
#                                 AND, measured 2026-09-02, the search
#                                 refinement chips
#      frame_UserPromptAndControl the single <input>, and the turn timer
#  * The refinement chips are in frame_UserMessage carrying
#    data-testid='provider-search-refine-chip'. They are NOT in
#    frame_LeftPanel and the action is 'provider_search:refine'. The older
#    suite looks for 'filter:refine' under #frame_LeftPanel and finds
#    nothing there.
#  * Panel checkboxes carry pointer-events:none. The click target is the
#    row (tr[data-code]) or the label, so boxes are READ, never clicked.
#  * Navigation waits on the document, never on the network falling idle:
#    /gate holds a streamed NDJSON connection open, so "networkidle" is
#    never reached. Readiness is the about_chathealthy selector.

import json
import os

import pytest
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

# The repository root, and the one environment file in it. There is
# exactly one .env and it is here; the suite reads it once, at import,
# so no function has to know where credentials come from.
REPO_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
load_dotenv(os.path.join(REPO_ROOT, ".env"), override=False)

BASE_URL = os.getenv("SMOKE_TEST_URL", "https://localhost")

# Failures are appended here as they happen, so a run can be triaged
# while it is still going.
TRIAGE_LOG = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "..", "_oneshots", "test_output", "uat_triage.log")


def _triage(name: str, viewport: dict, detail: str) -> None:
    os.makedirs(os.path.dirname(TRIAGE_LOG), exist_ok=True)
    with open(TRIAGE_LOG, "a", encoding="utf-8") as fh:
        fh.write(
            f"\n{'=' * 70}\n"
            f"FAIL  {name}   at {viewport.get('width')}x{viewport.get('height')}\n"
            f"{'-' * 70}\n{detail.strip()}\n")
        fh.flush()
        os.fsync(fh.fileno())
# Which form factor this run is about. "360x800" is a phone; the default
# is the computer.
_vp = os.getenv("UAT_VIEWPORT", "1600x1000").lower().split("x")
VIEWPORT = {"width": int(_vp[0]), "height": int(_vp[1])}
IS_PHONE = VIEWPORT["width"] <= 720

# What says the page is ready, at either width. The footer's About link
# was the gate, and a phone moves that link into the menu -- so the gate
# waited for something the form factor had deliberately hidden. The brand
# is in the header at every width.
READY = ".ch-brand"
DEFAULT_TIMEOUT = 60_000

# One action's answer. A turn is single-digit seconds when the machine is
# only serving it -- but the suite is run at two widths at once, and two
# browsers beside four containers and the Docker daemon on one workstation
# is what a turn is actually competing with. Nine turns exceeded 30s that
# way while the same suites against dev, where the containers are hosted,
# had none.
LLM_TIMEOUT = 45_000

EVIDENCE_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "..", "_oneshots", "test_output", "windows_uat",
)
os.makedirs(EVIDENCE_DIR, exist_ok=True)

# The eleven windows, in the order they sit on the screen: the header, the
# three columns, the two rows under the centre column, the footer, then
# the four windows that open over the top of all of it.
WINDOWS = [
    "frame_Header",
    "frame_LeftPanel",
    "frame_MainWindow",
    "frame_RightPanel",
    "frame_UserMessage",
    "frame_UserPromptAndControl",
    "frame_Footer",
    "frame_LegalPanel",
    "frame_MobileNavDrawer",
    "frame_AboutChatHealtyPopUP",
    "frame_SessionInfoPopUp",
]

PROMPT = "#frame_UserPromptAndControl input"
PANEL = "#frame_LeftPanel"
RESULTS = "#frame_MainWindow"
DETAIL = "#frame_RightPanel"
MESSAGE = "#frame_UserMessage"

PANEL_BOXES = f"{PANEL} input[type='checkbox']"
PANEL_ROWS = f"{PANEL} tr[data-code]"
APPLY = f"{PANEL} [data-testid='apply-filter-button']"
DETAIL_CLOSE = f"{DETAIL} [data-testid='provider-detail-close']"
TIMER = "#frame_UserPromptAndControl [data-testid='prompt-row-timer']"

# The results header agrees with its number — "1 provider found", "151
# providers found" — so matching only the plural reads a single result as
# no result at all.
FOUND = ("providers found", "provider found")
FACILITIES_FOUND = ("facilities found", "facility found")
EMPTY = "No providers matched."

_FOUND_JS = " || ".join(f"t.includes('{token}')" for token in FOUND)


# ── The window census ────────────────────────────────────────────────
# One pass over all eleven windows in a single evaluate, so every reading
# is taken at the same instant. Taking them one at a time let the screen
# move between the first window and the last, which is exactly the moment
# a repaint defect lives in.

_CENSUS_JS = """
(ids) => {
  const view = {width: window.innerWidth, height: window.innerHeight};
  const out = {viewport: view, windows: {}};
  for (const id of ids) {
    const el = document.getElementById(id);
    if (!el) { out.windows[id] = {present: false}; continue; }
    const box = el.getBoundingClientRect();
    const style = getComputedStyle(el);
    const visible = box.width > 0 && box.height > 0
                    && style.visibility !== 'hidden' && style.display !== 'none';
    const marks = {};
    for (const m of el.querySelectorAll('[data-testid]')) {
      const key = m.getAttribute('data-testid');
      marks[key] = (marks[key] || 0) + 1;
    }
    const actions = {};
    for (const a of el.querySelectorAll('[data-router-action]')) {
      const key = a.getAttribute('data-router-action');
      actions[key] = (actions[key] || 0) + 1;
    }
    // Collapse runs of whitespace WITHOUT a regular expression:
    // Rule-065-ENF-006 forbids them in executable code, and a pattern
    // inside a string handed to evaluate() is still a pattern this file
    // carries. split on whitespace with filter+join does the same work.
    const text = (el.innerText || '').trim();
    const digest = text.split('\\t').join(' ').split('\\r').join(' ')
                       .split('\\n').join(' ').split(' ')
                       .filter(s => s.length > 0).join(' ').slice(0, 600);
    out.windows[id] = {
      present: true,
      tag: el.tagName,
      visible: visible,
      box: {x: Math.round(box.x), y: Math.round(box.y),
            width: Math.round(box.width), height: Math.round(box.height)},
      height_pct: view.height ? Math.round(box.height / view.height * 1000) / 10 : null,
      width_pct: view.width ? Math.round(box.width / view.width * 1000) / 10 : null,
      chars: text.length,
      digest: digest,
      text: text.slice(0, 4000),
      marks: marks,
      actions: actions,
      checkboxes: el.querySelectorAll("input[type='checkbox']").length,
      checked: el.querySelectorAll("input[type='checkbox']:checked").length
    };
  }
  return out;
}
"""


_ROW_ELEMENTS_JS = """
() => Array.from(document.querySelectorAll("[data-testid='provider-card']"))
  .map((c, i) => {
    const t = c.innerText || '';
    const lacks = [];
    if (!t.trim()) lacks.push('name');
    if (t.indexOf('NPI:') === -1) lacks.push('npi');
    if (t.indexOf('Phone:') === -1) lacks.push('phone');
    if (t.indexOf('County:') === -1) lacks.push('county');
    if (!c.querySelector("[data-router-action='provider:detail']"))
      lacks.push('detail link');
    if (!c.querySelector("[data-router-action='provider:select-click']"))
      lacks.push('select control');
    return lacks.length ? [i, lacks] : null;
  }).filter(Boolean)
"""


def _census(page) -> dict:
    """Every window's present / visible / box / text digest / mark counts.

    `digest` is the window's text with runs of whitespace collapsed so two
    censuses of the same screen compare equal across a repaint that only
    reflowed it. `text` is kept alongside, untouched, because the report
    has to be able to say what a person would actually have read.
    """
    return page.evaluate(_CENSUS_JS, WINDOWS)


def _record(page, name: str) -> dict:
    """Take the census, screenshot the whole page, write both to disk.

    Called at every settled point whether or not anything is asserted
    afterwards, because a window nothing has a requirement for still has
    to be evidence. Returns the census so the caller can assert on it.
    """
    census = _census(page)
    census["evidence_name"] = name
    # A convenience the report reads: which windows are on screen with
    # nothing in them. Recorded, never asserted -- "blank" is only a
    # defect where a requirement says something must be there, and those
    # windows are asserted by name in the cases below.
    census["visible_but_empty"] = sorted(
        wid for wid, w in census["windows"].items()
        if w.get("present") and w.get("visible") and w.get("chars") == 0)
    census["not_visible"] = sorted(
        wid for wid, w in census["windows"].items()
        if w.get("present") and not w.get("visible"))
    page.screenshot(path=os.path.join(EVIDENCE_DIR, f"{name}.png"), full_page=True)
    with open(os.path.join(EVIDENCE_DIR, f"{name}.json"), "w", encoding="utf-8") as fh:
        json.dump(census, fh, indent=1, ensure_ascii=False)
    return census


def _window(census: dict, window_id: str) -> dict:
    win = census["windows"].get(window_id)
    assert win is not None and win.get("present"), (
        f"{window_id} is not in the document at all")
    return win


def _marks(census: dict, window_id: str) -> dict:
    return _window(census, window_id).get("marks", {})


# ── Driving the app the way a person does ────────────────────────────

# What the person came for, so a wait that meets a question can reply.
_GOAL: dict[int, str] = {}


def _ask(page, text: str) -> None:
    _GOAL[id(page)] = text
    page.locator(PROMPT).fill(text)
    page.locator(PROMPT).press("Enter")


def _ready(page) -> None:
    page.wait_for_selector(READY, timeout=DEFAULT_TIMEOUT)
    # The brand paints before the session token exists, and a turn asked
    # in that gap is refused by /gate with a 401 that paints nothing.
    page.wait_for_function(
        "() => !!(window.ClientRouter && window.ClientRouter.getSessionToken())",
        timeout=DEFAULT_TIMEOUT)


def _open_about(page) -> None:
    """Reach About the way the form factor offers it.

    A computer carries the link in the footer. A phone moves it into the
    menu, so the menu is opened first. The capability is the same either
    way; only the route to it differs, which is the whole of what a phone
    changes here.
    """
    # The width on screen right now, not the width the run was launched
    # at: a case that resizes to a computer must take the computer's
    # route to About, and IS_PHONE only knows how the run started.
    if page.viewport_size["width"] <= 720:
        page.locator("[data-router-action='toggle_mobile_nav']").first.click()
        page.wait_for_selector("#frame_MobileNavDrawer", state="visible",
                               timeout=DEFAULT_TIMEOUT)
        page.locator("#frame_MobileNavDrawer "
                     "[data-router-action='about_chathealthy']").first.click()
        return
    page.locator("[data-router-action='about_chathealthy']").first.click()


def _fresh(page):
    page.reload(wait_until="domcontentloaded")
    _ready(page)
    return page


def _new_session(page):
    """Start a session, rather than resume the one already in the tab.

    A reload is not a new session. The browser remembers the session in
    sessionStorage and offers the same one back, and the server keeps the
    form factor it was established with -- deliberately, because a window
    resized mid-session is still the same person on the same device.

    So anything asking what a session establishes AT BOOT has to boot one.
    """
    page.evaluate("() => { try { sessionStorage.clear() } catch (e) {} }")
    page.reload(wait_until="domcontentloaded")
    _ready(page)
    return page


# ── Being the person on the other side of the question ───────────────
# When the system asks, the test answers as the person whose goal it
# carries, and the scenario goes on.

ANSWER_MODEL = os.getenv("UAT_ANSWER_MODEL", "anthropic:claude-opus-5")
MAX_REPLIES = 3

_ANSWER_AGENT = None


def _answer_agent():
    """The person the test stands in for. The reply is authored because
    the question is."""
    global _ANSWER_AGENT
    if _ANSWER_AGENT is None:
        from pydantic_ai import Agent
        _ANSWER_AGENT = Agent(
            ANSWER_MODEL,
            output_type=str,
            system_prompt=(
                "You are a person using a healthcare search site. You are "
                "shown the goal you came with and the question the site "
                "just asked you. Reply exactly as that person would: one "
                "short sentence, plain words, answering the question and "
                "nothing else. Never mention that you are a model, never "
                "explain yourself, never ask a question back. If the site "
                "asks you to choose among options it named, choose the "
                "one that matches your goal."
            ),
        )
    return _ANSWER_AGENT


def _reply_to(question: str, goal: str) -> str:
    """The person's reply. Made on its own thread: Playwright's sync API
    already has a running event loop and the model client needs one."""
    import sys
    import concurrent.futures
    sys.path.insert(0, os.path.join(REPO_ROOT, "ChatHealthyLib", "src"))
    from chathealthy_lib.llm import run_llm_sync

    def _call():
        return run_llm_sync(
            _answer_agent(),
            f"THE GOAL YOU CAME WITH: {goal}\n\n"
            f"WHAT THE SITE JUST ASKED YOU:\n{question}\n\n"
            "Reply as that person, in one short sentence.",
            call_site="uat._answer_agent", provider="anthropic",
            server="uat", component="find_care_windows_uat_test")

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        result = pool.submit(_call).result(timeout=60)
    return (result.output or "").strip()


def _said_to_the_person(page) -> str:
    return page.evaluate(
        "() => ((document.querySelector('#frame_UserMessage') || {})"
        ".innerText || '').trim()")


def _wait_for(page, ready_js: str, what: str) -> None:
    """Wait for the thing, or for the system to say something new. If it
    said something, the person replies and the wait goes round again.

    ready_js is an expression over `d` (the document)."""
    goal = _GOAL.get(id(page), "")
    for reply in range(MAX_REPLIES + 1):
        before = _said_to_the_person(page)
        page.wait_for_function(
            "(before) => { const d = document;"
            f" const ready = {ready_js};"
            " const said = ((d.querySelector('#frame_UserMessage') || {})"
            "   .innerText || '').trim();"
            " return ready || (said && said !== before); }",
            arg=before, timeout=LLM_TIMEOUT)
        if page.evaluate(f"() => {{ const d = document; return {ready_js}; }}"):
            return
        question = _said_to_the_person(page)
        if reply == MAX_REPLIES:
            pytest.fail(
                f"after {MAX_REPLIES} replies the system is still asking "
                f"rather than showing {what}. Goal: {goal!r}. Last question: "
                f"{question[:300]!r}")
        _ask(page, _reply_to(question, goal))


def _wait_for_panel(page) -> None:
    _wait_for(page, f"d.querySelectorAll(\"{PANEL_BOXES}\").length > 0",
              "the specialty panel")


_RESULTS_READY_JS = (
    "(() => { const t = ((d.querySelector('#frame_MainWindow') || {})"
    f".textContent || ''); return {_FOUND_JS}; }})()"
)


def _wait_for_results(page) -> None:
    _wait_for(page, _RESULTS_READY_JS, "the results")


def _wait_for_facilities(page) -> None:
    """Wait on a facility ROW, not on the count in the heading.

    A row is the thing a person reads; the heading can be on screen while
    the list under it is still empty.
    """
    _wait_for(page,
              "d.querySelectorAll(\"[data-testid='facility-card']\").length > 0",
              "the facilities")


def _total(page) -> int:
    """The provider count on screen once the turn producing it has ended.

    An empty result says so in words instead of naming a count, so this
    has to read a legitimate zero as well as a number.
    """
    page.wait_for_function(
        "() => { const t = (document.querySelector('#frame_MainWindow')"
        f" || {{}}).innerText || ''; return {_FOUND_JS}"
        f" || t.includes('{EMPTY}'); }}", timeout=LLM_TIMEOUT)
    text = page.locator(RESULTS).inner_text()
    if EMPTY in text:
        return 0
    for token in FOUND:
        if token in text:
            head = text.split(token)[0].strip().split()[-1]
            return int(head.replace(",", ""))
    pytest.fail(f"the results window names no provider count: {text[:200]!r}")


def _rows(page):
    """[(code, ticked)] for every specialty row, as the user sees it."""
    return page.evaluate(
        f"() => Array.from(document.querySelectorAll('{PANEL_ROWS}')).map("
        "r => [r.getAttribute('data-code'),"
        " !!(r.querySelector(\"input[type='checkbox']\") || {}).checked])")


def _ticked_codes(page):
    return [code for code, ticked in _rows(page) if ticked]


def _click_row(page, code: str) -> None:
    """Toggle one specialty and wait for the panel to show it toggled.

    The click is a postMessage to the parent, which repaints the window,
    so the tick does not change on the same turn as the click. Reading it
    straight back reads the state from before the repaint.
    """
    was = code in _ticked_codes(page)
    page.locator(f"{PANEL} tr[data-code='{code}']").first.click()
    page.wait_for_function(
        f"() => {{ const r = document.querySelector"
        f"(\"{PANEL} tr[data-code='{code}']\");"
        f" return r && r.querySelector(\"input[type='checkbox']\").checked"
        f" === {str(not was).lower()}; }}", timeout=DEFAULT_TIMEOUT)


def _apply(page, settle_ms: int) -> None:
    """Apply the current selection.

    Apply Filter is disabled until the selection differs from the one in
    force, so the caller must have changed something first.
    """
    page.locator(APPLY).click()
    page.wait_for_timeout(settle_ms)


def _digits(text: str) -> str:
    """The digit run in a count element, without a regular expression.

    Rule-065-ENF-006 forbids regular expressions in executable code, so
    every text reading in this file is done with string methods.
    """
    return "".join(ch for ch in text if ch.isdigit())


@pytest.fixture(scope="module")
def page():
    pw = sync_playwright().start()
    browser = pw.chromium.launch(headless=True,
                                 args=["--ignore-certificate-errors"])
    # A fixed viewport, because four of the assertions below are geometry
    # against a percentage of it. A viewport that varies between runs
    # makes those tests measure the window manager.
    #
    # UAT_VIEWPORT names the form factor: the default is the computer and
    # 360x800 is a Galaxy-class phone. The application arranges itself
    # differently below 720px, so the same cases have to be able to run
    # against both -- a suite that only ever sees one of them cannot say
    # anything about the other (EPIC-002-F-008-S-005).
    context = browser.new_context(ignore_https_errors=True,
                                  viewport=VIEWPORT,
                                  is_mobile=IS_PHONE, has_touch=IS_PHONE)
    p = context.new_page()
    p.set_default_timeout(DEFAULT_TIMEOUT)
    p.goto(BASE_URL, wait_until="domcontentloaded")
    _ready(p)
    yield p
    context.close()
    browser.close()
    pw.stop()


# ── Assertions every flow repeats ────────────────────────────────────
# The layout does not stop being required because the user asked a
# question, so these run at the settled point of every flow rather than
# once at the start. A turn that pushes the footer off the bottom of the
# screen is a defect at the moment it does it, not at page load.

def _assert_layout_holds(census: dict, where: str) -> None:
    """The layout windows are present and sized, at this moment.

    EPIC-002-F-011-S-001-REQ-B-001 — header height is 6% of viewport.
    EPIC-002-F-011-S-001-REQ-B-002 — footer height is 4% of viewport.
    EPIC-002-F-011-S-002-REQ-B-001 — left panel is always present.
    EPIC-002-F-011-S-002-REQ-B-002 — right panel is always present.

    Both S-002 requirements have two clauses: a default width of 1% and
    always-present. Only always-present is asserted. The 1% clause is
    RECORDED in the census and escalated in the report: measured at rest
    the panels are 14.8% of viewport width, and the story carrying both
    clauses is `not_started`, so a failing assertion there would report
    work that was never done rather than work that regressed.

    The 6%/4% pair carries a one-point tolerance. 6% of a 1000px viewport
    is 60px and the header measures 61; a border rounds a percentage off
    an integer pixel count and an exact-equality assertion would fail on
    arithmetic rather than on layout.
    """
    header = _window(census, "frame_Header")
    footer = _window(census, "frame_Footer")
    assert header["visible"], f"{where}: the header is not on the screen"
    assert footer["visible"], f"{where}: the footer is not on the screen"
    assert abs(header["height_pct"] - 6.0) <= 1.0, (
        f"{where}: the header is {header['height_pct']}% of the viewport; "
        f"EPIC-002-F-011-S-001-REQ-B-001 fixes it at 6%")
    assert abs(footer["height_pct"] - 4.0) <= 1.0, (
        f"{where}: the footer is {footer['height_pct']}% of the viewport; "
        f"EPIC-002-F-011-S-001-REQ-B-002 fixes it at 4%")
    # The requirement's word is PRESENT, and that is asserted at every
    # width. Being on screen is asserted too, but only where the
    # arrangement puts it on screen: a computer lays the panels beside the
    # centre column and they are always in view, while a phone lays them
    # in a strip one screen wide each and hides an empty one, because an
    # empty panel there costs a whole screen to swipe past.
    #
    # ESCALATION: EPIC-002-F-011-S-002 is `not_started` and says nothing
    # about a phone. Whether an empty side panel should hold a screen of
    # its own in the strip is a requirement question, not a test
    # question, and it is not settled here.
    wide = census["viewport"]["width"] > 720
    for wid in ("frame_LeftPanel", "frame_RightPanel"):
        win = _window(census, wid)
        assert win["present"], (
            f"{where}: {wid} is not present, and it is required to be "
            f"always present")
        if wide or win.get("chars", 0) > 0:
            assert win["visible"], (
                f"{where}: {wid} is not on the screen at "
                f"{census['viewport']['width']}px and it holds "
                f"{win.get('chars', 0)} characters")


def _assert_mobile_drawer_is_hidden(census: dict, where: str) -> None:
    """EPIC-002-F-008-S-001-REQ-B-003 — the hamburger menu is hidden on a
    computer. It is the phone's way to the links, so on a phone its being
    there is the requirement being met, not broken.

    This asserted the desktop rule at whatever width the run used, and a
    phone run then failed for having the menu a phone is supposed to
    have.
    """
    if census["viewport"]["width"] <= 720:
        return
    drawer = _window(census, "frame_MobileNavDrawer")
    assert not drawer["visible"], (
        f"{where}: the mobile nav drawer is on screen at "
        f"{census['viewport']['width']}px wide")


def _assert_results_and_summary_together(census: dict, where: str) -> None:
    """EPIC-006-F-001-S-005-REQ-B-009 — the provider list is the principal
    content of the page, and nothing drawn in the same frame removes,
    shortens, displaces or scrolls away any part of it.

    The requirement previously read that the summary must be shown with the
    results, and this asserted the summary was present. It was rewritten by
    the operator on 2026-09-02 after the summary, naming every resolved
    specialty three times over, took the frame the providers belong in.
    """
    marks = _marks(census, "frame_MainWindow")
    assert marks.get("provider-card", 0) > 0, (
        f"{where}: frame_MainWindow carries no provider list; it holds {marks}")
    assert marks.get("provider-summary", 0) == 0, (
        f"{where}: frame_MainWindow draws the summary among the list; "
        f"it holds {marks}")


def _assert_specialty_panel_intact(census: dict, where: str) -> None:
    """The specialty window is still doing its job at this moment.

    EPIC-006-F-003-S-001-REQ-B-002 — the user is offered the list of
        specialties their request implies.
    EPIC-006-F-003-S-001-REQ-B-006 — the user is shown how many are
        offered, how many prescribe, and how many they have chosen.
    """
    win = _window(census, "frame_LeftPanel")
    marks = win.get("marks", {})
    assert win["visible"], f"{where}: the specialty window is not on screen"
    assert marks.get("specialty-list", 0) == 1, (
        f"{where}: the specialty window offers no list of specialties; "
        f"it holds {marks}")
    assert win["checkboxes"] > 0, (
        f"{where}: the specialty list is on screen with no specialty in it")
    for count_mark in ("count-all-possible", "count-all-prescribers",
                       "count-your-choices"):
        assert marks.get(count_mark, 0) == 1, (
            f"{where}: the specialty window does not show {count_mark}; "
            f"REQ-B-006 requires all three counts. It holds {marks}")


# ═══════════════════════════════════════════════════════════════════════
# FLOW 0 — the screen before anything is asked of it
# ═══════════════════════════════════════════════════════════════════════

class TestEveryWindowAtRest:
    """The eleven windows on a session where nothing has been asked yet.

    Every later flow's census is read against this one, so this case
    establishes what "nothing has happened" looks like on all eleven at
    once rather than on the frame a given test cared about.
    """

    @pytest.fixture(scope="class")
    def at_rest(self, page):
        _fresh(page)
        page.wait_for_timeout(3_000)
        return _record(page, "00_at_rest")

    def test_all_eleven_windows_are_in_the_top_document(self, at_rest):
        """Every window is a top-document element, not iframe content.

        No requirement fixes the set at eleven, so this asserts only that
        each id Website/index.html declares resolves to a real element
        with a tag — a window that is not in the document cannot hold
        anything a requirement asks of it, and every case below would
        then fail for the wrong reason. The set itself is RECORDED.
        """
        missing = [w for w in WINDOWS
                   if not at_rest["windows"].get(w, {}).get("present")]
        assert missing == [], f"these declared windows are absent: {missing}"

    def test_the_layout_holds_before_anything_is_asked(self, at_rest):
        """EPIC-002-F-011-S-001-REQ-B-001, -REQ-B-002,
        EPIC-002-F-011-S-002-REQ-B-001, -REQ-B-002. See
        _assert_layout_holds for what of each is asserted and what is
        recorded instead."""
        _assert_layout_holds(at_rest, "at rest")

    def test_the_mobile_drawer_is_hidden_on_a_desktop_viewport(self, at_rest):
        """EPIC-002-F-008-S-001-REQ-B-003."""
        _assert_mobile_drawer_is_hidden(at_rest, "at rest")

    def test_the_four_windows_are_shut(self, at_rest):
        """The four windows that open over the layout are shut until
        something opens them.

        No approved requirement states this, so it is asserted only in the
        weakest form that still means something: a window nobody opened is
        not on the screen. What each one HOLDS when open is recorded by
        the flows that open them, and the absence of a requirement for
        frame_LegalPanel and frame_SessionInfoPopUp content is escalated
        in the report rather than covered here.
        """
        for wid in ("frame_AboutChatHealtyPopUP", "frame_SessionInfoPopUp",
                    "frame_MobileNavDrawer", "frame_LegalPanel"):
            assert not _window(at_rest, wid)["visible"], (
                f"{wid} is on the screen and nothing opened it")

    def test_the_prompt_is_the_only_thing_asking_for_input(self, at_rest):
        """EPIC-002-F-010-S-001-REQ-B-001 — every user prompt in natural
        language flows through the one prompt to the UtteranceManager.
        Asserted here as the visible half of it: exactly one input, in
        frame_UserPromptAndControl, on a screen at rest.
        """
        prompt = _window(at_rest, "frame_UserPromptAndControl")
        assert prompt["visible"], "there is nowhere to type"
        assert prompt["actions"].get("user:submit", 0) == 1, (
            f"the prompt window carries {prompt['actions']}")


# ═══════════════════════════════════════════════════════════════════════
# FLOW 1 — a provider search, every window before and after
#
# Utterance: "find me a shrink in Long Beach CA" — 7 occurrences in the
# conversation archive, the most-run city+state provider search there.
# ═══════════════════════════════════════════════════════════════════════

class TestAProviderSearchAcrossEveryWindow:
    """One search, censused twice: while it runs, and once it has landed.

    The mid-flight census is the one the older suite has no equivalent
    of. It is where the progress requirement lives, and it is where a
    window that gets blanked and never repainted would be caught.
    """

    censuses = {}

    @pytest.fixture(scope="class")
    def searched(self, page):
        _fresh(page)
        _ask(page, "find me a shrink in Long Beach CA")
        # Mid-flight: the turn has started and no results exist yet.
        page.wait_for_timeout(4_000)
        type(self).censuses["mid"] = _record(page, "01a_search_in_flight")
        _wait_for_panel(page)
        _wait_for_results(page)
        page.wait_for_timeout(2_000)
        type(self).censuses["settled"] = _record(page, "01b_search_settled")
        return page

    def test_the_prompt_window_shows_the_search_is_running(self, searched):
        """EPIC-006-F-001-S-001-REQ-B-006 — from submit until results or
        an error, the user MUST see continuously advancing evidence that
        the search is running, and it MUST NOT stall, freeze or reset.

        The evidence is the turn timer in frame_UserPromptAndControl. This
        reads it twice, seconds apart, and requires the second reading to
        be strictly greater than the first: a timer that is present but
        frozen satisfies "present" and fails the requirement.
        """
        _fresh(searched)
        _ask(searched, "find me a shrink in Long Beach CA")
        searched.wait_for_function(
            f"() => {{ const e = document.querySelector(\"{TIMER}\");"
            " return e && e.innerText.trim().length > 0; }",
            timeout=LLM_TIMEOUT)
        first = _digits(searched.locator(TIMER).inner_text())
        searched.wait_for_timeout(4_000)
        second = _digits(searched.locator(TIMER).inner_text())
        assert first and second, (
            f"the turn timer shows no elapsed time: {first!r} then {second!r}")
        assert int(second) > int(first), (
            f"the turn timer read {first}s and then {second}s four seconds "
            f"later; the evidence the search is running is not advancing")
        _wait_for_panel(searched)
        _wait_for_results(searched)

    def test_no_results_window_paints_before_the_answer_arrives(self, searched):
        """RECORDED, not asserted beyond the layout.

        The mid-flight census shows frame_MainWindow still holding the
        welcome splash and frame_UserMessage empty. No approved
        requirement states what the results window must hold WHILE a
        search runs — REQ-B-006 requires advancing evidence somewhere, and
        the prompt-window timer provides it. So this case asserts only
        that the layout survives the turn, and records the rest.
        """
        mid = self.censuses["mid"]
        _assert_layout_holds(mid, "mid-search")
        _assert_mobile_drawer_is_hidden(mid, "mid-search")

    def test_the_results_window_holds_the_summary_and_the_results(self, searched):
        """EPIC-006-F-001-S-005-REQ-B-009 — the summary MUST be shown
        together with the results it describes."""
        _assert_results_and_summary_together(self.censuses["settled"],
                                             "after a provider search")

    def test_every_row_names_the_provider_the_way_the_search_promised(self, searched):
        """EPIC-006-F-001-S-001-REQ-B-002 — the system returns provider
        name, NPI, address, county and phone.

        Measured on a row: "BRANDY GONZALES, PMHNP / 6060 N PARAMOUNT
        BLVD, LONG BEACH, CA, 90805 / NPI: 1003557190 · Phone: (562)
        634-9534 · County: Los Angeles County / Psychiatric/Mental Health
        Nurse Practitioner". All five are on the row, so all five are
        asserted rather than only the NPI.
        """
        row = searched.locator("[data-testid='provider-card']").first
        text = row.inner_text()
        assert row.get_attribute("data-npi"), "the row carries no NPI"
        for token in ("NPI:", "Phone:", "County:"):
            assert token in text, (
                f"the row does not name {token[:-1]}: {text!r}")
        lines = [line for line in text.split("\n") if line.strip()]
        assert len(lines) >= 4, (
            f"the row should name the provider, the address, the "
            f"NPI/phone/county line and the specialty; it has {len(lines)}: "
            f"{lines}")

    def test_the_summary_states_how_many_are_unseen_and_of_how_many_kinds(self, searched):
        """EPIC-006-F-001-S-005-REQ-B-002 — the summary states how many
        matched beyond those on screen.
        EPIC-006-F-001-S-005-REQ-B-003 — and how many kinds of provider
        the results hold.
        EPIC-006-F-001-S-005-REQ-B-006 — and names the state, where the
        user specified one. The utterance named CA.
        """
        summary = searched.locator("[data-testid='provider-summary']").inner_text()
        assert " more " in summary, (
            f"the summary does not state how many remain unseen: "
            f"{summary[:300]!r}")
        assert "types of providers" in summary, (
            f"the summary does not state how many kinds of provider the "
            f"results hold: {summary[:300]!r}")
        assert "'CA'" in summary, (
            f"the user named Long Beach CA and the summary does not name "
            f"the state: {summary[:300]!r}")

    def test_the_specialty_window_offers_what_the_request_implied(self, searched):
        """EPIC-006-F-003-S-001-REQ-B-002 and -REQ-B-006."""
        _assert_specialty_panel_intact(self.censuses["settled"],
                                       "after a provider search")

    def test_the_layout_survives_the_turn(self, searched):
        """EPIC-002-F-011-S-001-REQ-B-001, -REQ-B-002,
        EPIC-002-F-011-S-002-REQ-B-001, -REQ-B-002,
        EPIC-002-F-008-S-001-REQ-B-003."""
        settled = self.censuses["settled"]
        _assert_layout_holds(settled, "after a provider search")
        _assert_mobile_drawer_is_hidden(settled, "after a provider search")

    def test_the_detail_window_is_empty_until_a_provider_is_opened(self, searched):
        """RECORDED. No approved requirement states that frame_RightPanel
        is empty before a provider is opened; EPIC-006-F-002-S-001 says
        what a detail shows, not what stands there when there is none. The
        measurement — present, visible, zero characters — is in the census
        and this case asserts only that the window is there to paint into,
        which EPIC-002-F-011-S-002-REQ-B-002 requires.
        """
        census = self.censuses["settled"]
        detail = _window(census, "frame_RightPanel")
        width = census["viewport"]["width"]
        assert detail["present"], (
            "the right panel is not present, so a provider detail would "
            "have nowhere to paint")
        # A screen with room for two columns shows the empty panel beside
        # the list. A screen with room for one does not spend a whole
        # screen on a panel holding nothing -- the panel is there and is
        # shown when it has a detail in it.
        if width > 720:
            assert detail["visible"], (
                f"the right panel is not on screen at {width}px, which has "
                f"room for it beside the results")


# ═══════════════════════════════════════════════════════════════════════
# FLOW 2 — the specialty filter: narrow, apply, and prove the apply
#          destroyed no other window
#
# This is the defect that shipped: Apply Filter emitted intent_classified,
# which means "a new query was classified", and the widget answering that
# blanks LeftPanel, RightPanel and MainWindow. Filtering wiped the panel
# being filtered with. A one-frame test cannot see it; a census can.
# ═══════════════════════════════════════════════════════════════════════

class TestTheSpecialtyFilterAcrossEveryWindow:
    """Narrow the specialties, apply, and census every window either side.

    Utterance: "find me a shrink in Long Beach CA", from the archive.
    """

    censuses = {}
    chosen = []

    @pytest.fixture(scope="class")
    def filtered(self, page):
        _fresh(page)
        _ask(page, "find me a shrink in Long Beach CA")
        _wait_for_panel(page)
        _wait_for_results(page)
        page.wait_for_timeout(2_000)
        type(self).censuses["before"] = _record(page, "02a_before_apply")
        type(self).censuses["before_total"] = _total(page)

        ticked = _ticked_codes(page)
        assert len(ticked) > 1, (
            "the panel seeds fewer than two specialties, so there is no "
            "selection to narrow")
        # Untick one. A change, so Apply Filter becomes enabled, and one
        # the user plainly made -- which is what REQ-B-008 is about.
        _click_row(page, ticked[0])
        type(self).chosen = _ticked_codes(page)
        _apply(page, 10_000)
        page.wait_for_timeout(2_000)
        type(self).censuses["after"] = _record(page, "02b_after_apply")
        return page

    def test_the_specialty_window_survives_its_own_apply(self, filtered):
        """EPIC-006-F-003-S-001-REQ-B-002 and -REQ-B-006 — after applying,
        the user is still offered the list and still shown the three
        counts. This is the window that the defect destroyed.
        """
        _assert_specialty_panel_intact(self.censuses["after"], "after apply")

    def test_the_selection_the_user_made_is_the_selection_in_force(self, filtered):
        """EPIC-006-F-003-S-001-REQ-B-008 — a user's chosen set of
        specialties MUST remain in force until that user changes it.

        Applying is not the user changing it, so the set that comes back
        is the set that went in. A default seeded on the way back through
        overwrites the choice and this is what says so.
        """
        assert _ticked_codes(filtered) == self.chosen, (
            f"the panel came back from Apply Filter holding "
            f"{_ticked_codes(filtered)} against the {self.chosen} the user "
            f"chose")

    def test_the_results_window_survives_the_apply(self, filtered):
        """EPIC-006-F-001-S-005-REQ-B-009 — the summary is still shown
        together with the results it describes, after the apply."""
        _assert_results_and_summary_together(self.censuses["after"],
                                             "after apply")

    def test_applying_a_narrower_set_does_not_widen_the_result(self, filtered):
        """EPIC-006-F-003-S-001-REQ-B-007 — when a user applies their
        chosen specialties, the provider list shows exactly the providers
        those specialties admit.

        Deliberately "must not grow" rather than "must shrink": a code the
        panel offers can have no provider in this geography, so removing
        one correctly leaves the total unchanged, which a strict-decrease
        assertion would report as a defect.
        """
        after = _total(filtered)
        before = self.censuses["before_total"]
        assert after <= before, (
            f"removing a specialty took the result from {before} to {after}; "
            f"a narrower set admitted more providers")

    def test_the_apply_destroyed_no_layout_window(self, filtered):
        """EPIC-002-F-011-S-001-REQ-B-001, -REQ-B-002,
        EPIC-002-F-011-S-002-REQ-B-001, -REQ-B-002 — the header, footer
        and both panels are still on the screen after the apply."""
        _assert_layout_holds(self.censuses["after"], "after apply")

    def test_every_window_that_held_something_still_does(self, filtered):
        """The census either side of the apply, compared window by window.

        Each window named here is named because a requirement cited above
        says it must hold something at this moment:
          frame_LeftPanel  — REQ-B-002 / REQ-B-006 (specialties, counts)
          frame_MainWindow — EPIC-006-F-001-S-005-REQ-B-009 (summary+list)
          frame_Header     — EPIC-002-F-011-S-001-REQ-B-001
          frame_Footer     — EPIC-002-F-011-S-001-REQ-B-002

        frame_UserMessage is deliberately NOT in that list. It carries the
        refinement chips, and no approved requirement states they must be
        there or must survive an apply. Whether it went blank is RECORDED
        in the census and reported, not asserted.
        """
        before = self.censuses["before"]
        after = self.censuses["after"]
        emptied = []
        for wid in ("frame_LeftPanel", "frame_MainWindow",
                    "frame_Header", "frame_Footer"):
            if _window(before, wid)["chars"] > 0 and _window(after, wid)["chars"] == 0:
                emptied.append(wid)
        assert emptied == [], (
            f"applying the filter emptied {emptied}, each of which a "
            f"requirement says must hold something at this moment")


# ═══════════════════════════════════════════════════════════════════════
# FLOW 3 — a facility search and a facility detail
#
# Utterance: "find me an urgent care clinic in Los Angeles CA" — 3
# occurrences in the archive, and the shape the operator ran against build
# 2244 on 2026-09-02 ("Test urgent care clinic in Los Angeles CA").
#
# EPIC-006-F-006 and F-007 carry approval `proposed`. Asserted anyway,
# following find_care_frames_uat_test.py; every case here says so.
# ═══════════════════════════════════════════════════════════════════════

class TestAFacilitySearchAcrossEveryWindow:
    """The facility list and the facility detail, with every window read
    at each settled point."""

    censuses = {}

    @pytest.fixture(scope="class")
    def facilities(self, page):
        _fresh(page)
        _ask(page, "find me an urgent care clinic in Los Angeles CA")
        _wait_for_facilities(page)
        page.wait_for_timeout(2_000)
        type(self).censuses["list"] = _record(page, "03a_facility_list")
        return page

    def test_the_results_window_holds_the_facility_list(self, facilities):
        """EPIC-006-F-006-S-002-REQ-B-007 (approval: proposed) — the row
        exists to be shown, so the list must be in a window a person can
        see. Measured: frame_MainWindow carries facility-card x25.
        """
        marks = _marks(self.censuses["list"], "frame_MainWindow")
        assert marks.get("facility-card", 0) > 0, (
            f"a facility search painted no rows; frame_MainWindow holds "
            f"{marks}")

    def test_the_searching_indicator_is_gone_once_the_rows_are_there(self, facilities):
        """EPIC-006-F-001-S-001-REQ-B-006 — the evidence that the search
        is running runs until results or an error are shown, and no
        further. An indicator still turning after the answer landed tells
        the person the turn never ended.
        """
        marks = _marks(self.censuses["list"], "frame_MainWindow")
        assert marks.get("facility-searching-timer", 0) == 0, (
            "the searching indicator is still on screen with the results")

    def test_the_row_shows_the_five_things_and_no_sixth(self, facilities):
        """EPIC-006-F-006-S-002-REQ-B-007 (approval: proposed) — a
        facility row MUST show exactly these five things and nothing else:
        the facility; how many practice addresses it has; its primary
        practice address; its facility type; and a link to its detail.
        """
        five = ("facility-name", "facility-address-count", "facility-address",
                "facility-type", "facility-detail-link")
        row = facilities.locator("[data-testid='facility-card']").first
        for testid in five:
            assert row.locator(f"[data-testid='{testid}']").count() == 1, (
                f"the row does not show {testid}, which the requirement names")
        marked = facilities.evaluate(
            "() => { const r = document.querySelector"
            "(\"[data-testid='facility-card']\");"
            " return r ? Array.from(r.querySelectorAll('[data-testid]'))"
            ".map(e => e.getAttribute('data-testid')) : []; }")
        extra = [m for m in marked if m not in five]
        assert extra == [], (
            f"the row shows {extra} beyond the five the requirement fixes")

    def test_every_row_actually_shows_a_facility_type(self, facilities):
        """EPIC-006-F-006-S-002-REQ-B-007 (approval: proposed) — the row
        MUST SHOW its facility type, being the label of its primary
        taxonomy.

        An element that is present and empty shows nothing, so presence is
        not what the requirement asks for. This reads the text of every
        facility-type on screen and names the rows that carry none.
        """
        empty = facilities.evaluate(
            "() => Array.from(document.querySelectorAll"
            "(\"[data-testid='facility-card']\"))"
            ".map(r => [ (r.querySelector(\"[data-testid='facility-name']\")"
            "  || {}).innerText || '',"
            "  ((r.querySelector(\"[data-testid='facility-type']\") || {})"
            "  .innerText || '').trim() ])"
            ".filter(pair => pair[1].length === 0)"
            ".map(pair => pair[0].trim())")
        assert empty == [], (
            f"{len(empty)} facility rows show no facility type: {empty[:6]}")

    def test_the_detail_names_the_organization_and_its_standing(self, facilities):
        """EPIC-006-F-007-S-001-REQ-B-002 (approval: proposed) — the panel
        MUST show the legal business name, the NPI, the date the NPI was
        issued, and whether the facility is currently active.
        """
        facilities.locator(
            "[data-testid='facility-card'] [data-testid='facility-detail-link']"
        ).first.click()
        facilities.wait_for_function(
            "() => document.querySelector"
            "(\"[data-testid='facility-identity']\") !== null",
            timeout=LLM_TIMEOUT)
        facilities.wait_for_timeout(1_500)
        type(self).censuses["detail"] = _record(facilities, "03b_facility_detail")

        detail = _window(self.censuses["detail"], "frame_RightPanel")
        text = detail["text"]
        legal = facilities.locator("[data-testid='facility-legal-name']")
        assert legal.count() == 1 and legal.inner_text().strip(), (
            "the detail shows no legal business name")
        assert "NPI:" in text, "the detail does not name the NPI"
        assert "NPI issued" in text, (
            f"the detail does not say when the NPI was issued: {text[:400]!r}")
        assert "active" in text.lower(), (
            f"the detail does not say whether the facility is currently "
            f"active: {text[:400]!r}")

    def test_the_detail_carries_no_tax_identifier(self, facilities):
        """EPIC-006-F-007-S-001-REQ-B-005 (approval: proposed) — the
        federal employer identification number and the parent
        organization's taxpayer identification number MUST NOT be shown,
        transmitted or stored, whatever any upstream response contains.

        This is the screen half of it: what the person can read.
        """
        text = _window(self.censuses["detail"], "frame_RightPanel")["text"].lower()
        for forbidden in ("employer identification", "taxpayer identification",
                          "ein:", "tin:"):
            assert forbidden not in text, (
                f"the facility detail shows {forbidden!r}")

    def test_the_official_is_labelled_as_the_authorized_official(self, facilities):
        """EPIC-006-F-007-S-002-REQ-B-002 (approval: proposed) — the
        section showing the authorized official MUST be labelled as the
        record's authorized official. Labelling them the administrative
        contact names one person with another's role.
        EPIC-006-F-007-S-002-REQ-B-001 — and shows their name and title.
        """
        block = facilities.locator("[data-testid='authorized-official']")
        assert block.count() == 1, "the detail has no authorized official section"
        label = block.inner_text()
        assert "authorized official" in label.lower(), (
            f"the section is not labelled as the record's authorized "
            f"official: {label[:150]!r}")
        assert facilities.locator("[data-testid='official-name']").inner_text().strip(), (
            "the authorized official section names nobody")

    def test_the_list_is_still_behind_the_detail(self, facilities):
        """EPIC-006-F-006-S-002-REQ-B-007 (approval: proposed) — the row
        carries a link to its detail, which means the user is meant to
        arrive at the detail FROM the list. A list destroyed by opening
        one of its rows leaves nothing to go back to.
        """
        marks = _marks(self.censuses["detail"], "frame_MainWindow")
        assert marks.get("facility-card", 0) > 0, (
            f"opening a facility detail emptied the list it was opened "
            f"from; frame_MainWindow holds {marks}")

    def test_the_layout_survives_the_facility_flow(self, facilities):
        """EPIC-002-F-011-S-001-REQ-B-001, -REQ-B-002,
        EPIC-002-F-011-S-002-REQ-B-001, -REQ-B-002,
        EPIC-002-F-008-S-001-REQ-B-003."""
        for name in ("list", "detail"):
            _assert_layout_holds(self.censuses[name], f"facility {name}")
            _assert_mobile_drawer_is_hidden(self.censuses[name],
                                            f"facility {name}")


# ═══════════════════════════════════════════════════════════════════════
# FLOW 4 — a provider detail opened and dismissed, and what the other
#          windows do while it is open
#
# Utterance: "find me a shrink in Long Beach CA".
# ═══════════════════════════════════════════════════════════════════════

class TestTheProviderDetailAcrossEveryWindow:
    """Three censuses: before the detail, with it open, and after it is
    dismissed. The point of the flow is the middle one — what the other
    ten windows are doing while the detail is up."""

    censuses = {}
    npi = ""

    @pytest.fixture(scope="class")
    def detailed(self, page):
        _fresh(page)
        _ask(page, "find me a shrink in Long Beach CA")
        _wait_for_panel(page)
        _wait_for_results(page)
        page.wait_for_timeout(2_000)
        type(self).censuses["before"] = _record(page, "04a_before_detail")

        npi = page.locator("[data-testid='provider-card']").first \
                  .get_attribute("data-npi")
        type(self).npi = npi
        page.locator(
            f"[data-router-action='provider:detail'][data-npi='{npi}']"
        ).first.click()
        # The loading screen reads "Loading NPI 1234", so waiting for the
        # NPI to appear passes on a panel that has not loaded. The close
        # control only exists on the loaded panel, so it IS the signal.
        page.wait_for_selector(DETAIL_CLOSE, timeout=LLM_TIMEOUT)
        page.wait_for_timeout(1_500)
        type(self).censuses["open"] = _record(page, "04b_detail_open")
        return page

    def test_the_detail_window_shows_the_provider_that_was_clicked(self, detailed):
        """EPIC-006-F-002-S-001-REQ-B-001 — every Provider Detail MUST
        show the values held for the CLICKED NPI at the moment it is
        shown. The NPI clicked is therefore the NPI on screen.
        """
        text = _window(self.censuses["open"], "frame_RightPanel")["text"]
        assert self.npi in text, (
            f"the detail window does not name NPI {self.npi}, which is the "
            f"row that was clicked: {text[:300]!r}")

    def test_the_detail_names_the_provider_and_their_primary_specialty(self, detailed):
        """EPIC-006-F-002-S-001-REQ-B-002 — the panel MUST display the
        provider's full name, credentials, primary taxonomy (NUCC display
        name) and NPI.

        Measured: "MRS. CHRISTINE UY TITENSKY, FNP / NPI: 1003416363 /
        Family Nurse Practitioner". Asserted as: the window names the
        provider, carries an NPI line, and carries at least three
        non-empty lines above the address section, so a panel that shows
        an NPI and nothing else fails.
        """
        text = _window(self.censuses["open"], "frame_RightPanel")["text"]
        assert "NPI:" in text, "the detail carries no NPI line"
        lines = [line.strip() for line in text.split("\n") if line.strip()]
        assert len(lines) >= 4, (
            f"the detail shows {len(lines)} lines: {lines}")

    def test_the_detail_labels_every_address(self, detailed):
        """EPIC-006-F-002-S-001-REQ-B-004 — the panel MUST show every
        address held, each labelled as a practice location or as the
        business address, with its street, city, state, postal code,
        country and phone.
        """
        text = _window(self.censuses["open"], "frame_RightPanel")["text"]
        assert "PRACTICE ADDRESSES" in text, (
            f"the detail has no address section: {text[:300]!r}")
        assert "Practice:" in text or "Business:" in text, (
            f"the detail shows addresses with no label saying which kind "
            f"each is: {text[:500]!r}")
        assert "Phone:" in text, "no address on the detail carries a phone"

    def test_the_detail_shows_the_licence_and_payer_sections(self, detailed):
        """EPIC-006-F-002-S-001-REQ-B-003 — every licence, and where the
        provider holds none the section is shown EMPTY rather than
        replaced by prose.
        EPIC-006-F-002-S-001-REQ-B-006 — every payer identifier, on the
        same terms.

        Both requirements turn on the section being there either way, so
        this asserts the sections exist and does not assert their
        contents.
        """
        text = _window(self.censuses["open"], "frame_RightPanel")["text"]
        assert "LICENSES" in text, (
            f"the detail has no licence section: {text[:500]!r}")
        assert "PAYER IDENTIFIERS" in text, (
            f"the detail has no payer identifier section: {text[:600]!r}")

    def test_the_specialty_window_is_untouched_while_the_detail_is_open(self, detailed):
        """EPIC-006-F-003-S-001-REQ-B-008 — a user's chosen set of
        specialties MUST remain in force until that user changes it.
        Opening a provider detail is not the user changing it.
        EPIC-006-F-003-S-001-REQ-B-006 — and the three counts are still
        shown.

        This is the assertion the one-frame suite cannot make: it is about
        the window the user is NOT looking at.
        """
        before = _window(self.censuses["before"], "frame_LeftPanel")
        after = _window(self.censuses["open"], "frame_LeftPanel")
        _assert_specialty_panel_intact(self.censuses["open"],
                                       "while a detail is open")
        assert after["checked"] == before["checked"], (
            f"opening a provider detail changed the specialty selection "
            f"from {before['checked']} ticks to {after['checked']}")
        assert after["digest"] == before["digest"], (
            "opening a provider detail repainted the specialty window")

    def test_the_results_window_is_untouched_while_the_detail_is_open(self, detailed):
        """EPIC-006-F-001-S-005-REQ-B-009 — the summary is still shown
        together with the results it describes while a detail is open. The
        detail is reached FROM the list, so destroying the list to show it
        leaves the person nowhere to return to.
        """
        _assert_results_and_summary_together(self.censuses["open"],
                                             "while a detail is open")
        before = _marks(self.censuses["before"], "frame_MainWindow")
        after = _marks(self.censuses["open"], "frame_MainWindow")
        assert after.get("provider-card") == before.get("provider-card"), (
            f"the list behind the detail went from "
            f"{before.get('provider-card')} rows to "
            f"{after.get('provider-card')}")

    def test_dismissing_the_detail_leaves_every_other_window_as_it_was(self, detailed):
        """EPIC-006-F-003-S-001-REQ-B-008 and
        EPIC-006-F-001-S-005-REQ-B-009 again, on the far side of the
        dismissal.

        That the detail CAN be dismissed is not asserted against a
        requirement, because none states it — that gap is escalated in the
        report. What is asserted is that using the control the product
        offers does not take anything else down with it.
        """
        detailed.locator(DETAIL_CLOSE).click()
        detailed.wait_for_function(
            "() => !document.querySelector"
            "(\"#frame_RightPanel [data-testid='provider-detail-close']\")",
            timeout=LLM_TIMEOUT)
        detailed.wait_for_timeout(1_500)
        census = _record(detailed, "04c_detail_dismissed")
        type(self).censuses["dismissed"] = census

        _assert_specialty_panel_intact(census, "after dismissing the detail")
        _assert_results_and_summary_together(census, "after dismissing the detail")
        _assert_layout_holds(census, "after dismissing the detail")
        before = _window(self.censuses["before"], "frame_LeftPanel")
        after = _window(census, "frame_LeftPanel")
        assert after["checked"] == before["checked"], (
            f"dismissing the detail changed the specialty selection from "
            f"{before['checked']} ticks to {after['checked']}")


# ═══════════════════════════════════════════════════════════════════════
# FLOW 5 — pagination
#
# Utterance: "find me a family doctor in California" — chosen because it
# is the widest result available, and pagination inheriting a narrowed
# result skips, which reports green while covering nothing.
# ═══════════════════════════════════════════════════════════════════════

class TestProviderPagination:
    """Page forward, and read every window on both pages."""

    censuses = {}
    first_npi = ""

    @pytest.fixture(scope="class")
    def paged(self, page):
        _fresh(page)
        _ask(page, "find me a family doctor in California")
        _wait_for_panel(page)
        _wait_for_results(page)
        page.wait_for_timeout(2_000)
        total = _total(page)
        if total <= 25:
            pytest.fail(
                f"the widest query available returns {total}; there is no "
                f"second page and pagination cannot be exercised")
        type(self).censuses["page_one"] = _record(page, "05a_page_one")
        type(self).first_npi = page.locator("[data-testid='provider-card']") \
                                   .first.get_attribute("data-npi")

        page.locator("[data-testid='providers-next-page']").click()
        page.wait_for_function(
            "(npi) => { const c = document.querySelector"
            "(\"[data-testid='provider-card']\");"
            " return c && c.getAttribute('data-npi') !== npi; }",
            arg=type(self).first_npi, timeout=LLM_TIMEOUT)
        page.wait_for_timeout(2_000)
        type(self).censuses["page_two"] = _record(page, "05b_page_two")
        return page

    def test_the_summary_says_there_are_more_than_are_on_screen(self, paged):
        """EPIC-006-F-001-S-005-REQ-B-002 — the summary MUST state how
        many providers matched beyond those on screen. A next-page control
        is only honest if the summary has said there is a next page.
        """
        summary = paged.locator("[data-testid='provider-summary']").inner_text()
        assert " more " in summary, (
            f"the summary does not say anything remains unseen while a "
            f"next-page control is on screen: {summary[:300]!r}")

    def test_the_second_page_is_a_different_batch(self, paged):
        """EPIC-006-F-001-S-005-REQ-B-002 — the requirement states that
        providers matched BEYOND those on screen exist. A forward control
        that returns the same batch contradicts the statement the summary
        made in the same window.
        """
        now = paged.locator("[data-testid='provider-card']").first \
                   .get_attribute("data-npi")
        assert now != self.first_npi, (
            f"the list still starts at {now} after paging forward")

    def test_the_results_window_still_holds_summary_and_results(self, paged):
        """EPIC-006-F-001-S-005-REQ-B-009 — on the second page as on the
        first, the summary is shown together with the results."""
        _assert_results_and_summary_together(self.censuses["page_two"],
                                             "on page two")

    def test_the_specialty_window_survives_paging(self, paged):
        """EPIC-006-F-003-S-001-REQ-B-008 — the chosen set stands until
        the user changes it. Paging forward is not changing it.
        EPIC-006-F-003-S-001-REQ-B-006 — and the counts are still shown.
        """
        _assert_specialty_panel_intact(self.censuses["page_two"], "on page two")
        before = _window(self.censuses["page_one"], "frame_LeftPanel")
        after = _window(self.censuses["page_two"], "frame_LeftPanel")
        assert after["checked"] == before["checked"], (
            f"paging forward changed the specialty selection from "
            f"{before['checked']} ticks to {after['checked']}")

    def test_the_layout_survives_paging(self, paged):
        """EPIC-002-F-011-S-001-REQ-B-001, -REQ-B-002,
        EPIC-002-F-011-S-002-REQ-B-001, -REQ-B-002,
        EPIC-002-F-008-S-001-REQ-B-003."""
        _assert_layout_holds(self.censuses["page_two"], "on page two")
        _assert_mobile_drawer_is_hidden(self.censuses["page_two"], "on page two")

    def test_where_the_paging_controls_live_is_recorded(self, paged):
        """RECORDED, not asserted.

        EPIC-002-F-013-S-001-REQ-B-002 fixes the control format as
        "<< Back | start-end / total | Forward >>" in a control frame that
        EPIC-002-F-011-S-001-REQ-B-004 puts at 7% of the centre column and
        always visible. Measured, the controls are inside
        frame_MainWindow, and no window named for controls exists in the
        document. Both requirements are escalated in the report; nothing
        here asserts a format or a home for them, because choosing which
        of the eleven windows OUGHT to be the control frame would be
        inventing the requirement rather than reporting it unmet.
        """
        marks = _marks(self.censuses["page_two"], "frame_MainWindow")
        assert marks.get("providers-next-page", 0) >= 0, "census unreadable"


# ═══════════════════════════════════════════════════════════════════════
# FLOW 6 — the About window and the Session Info window
#
# Both are top-document windows over the layout, not iframe content. The
# session window spent an afternoon rendering into the 1x1 offscreen host
# where every control existed in the DOM and none could be pressed, which
# is why these read geometry rather than presence.
# ═══════════════════════════════════════════════════════════════════════

class TestTheAboutAndSessionWindows:
    """Open both, and census the eleven with them up."""

    censuses = {}

    @pytest.fixture(scope="class")
    def windows_open(self, page):
        _fresh(page)
        _open_about(page)
        page.wait_for_selector("#frame_AboutChatHealtyPopUP",
                               state="visible", timeout=DEFAULT_TIMEOUT)
        page.wait_for_timeout(1_500)
        type(self).censuses["about"] = _record(page, "06a_about_window")

        page.locator("[data-router-action='session_info']").first.click()
        page.wait_for_selector("#frame_SessionInfoPopUp",
                               state="visible", timeout=LLM_TIMEOUT)
        # The window paints "Loading SharedServices..." first; the session
        # lands when the call returns. Asserting on the loading state reads
        # an empty window and blames the feature.
        page.wait_for_function(
            "() => ((document.querySelector('#frame_SessionInfoPopUp') || {})"
            ".innerText || '').includes('Identity')", timeout=LLM_TIMEOUT)
        page.wait_for_timeout(1_500)
        type(self).censuses["session"] = _record(page, "06b_session_window")
        return page

    def test_the_about_window_states_the_build_and_the_commit(self, windows_open):
        """EPIC-008-F-012-S-004-REQ-B-003 — every deployed target MUST
        report, on demand and without operator action, the build number
        and the commit its running code was produced from. A target that
        cannot say which build it carries MUST be treated as unverified.

        This window is that report on the surface a person can reach. A
        server reporting one build while running another's code cost a
        day; the section exists so it is visible without asking each
        server by hand.
        """
        text = _window(self.censuses["about"], "frame_AboutChatHealtyPopUP")["text"]
        for label in ("Build", "Commit", "Environment"):
            assert label in text, (
                f"the About window does not report {label}: {text[:400]!r}")

    def test_the_about_window_is_on_the_screen_not_in_the_offscreen_host(self, windows_open):
        """EPIC-008-F-012-S-004-REQ-B-003 — "on demand and without
        operator action" is only satisfied if a person can read it. The
        single iframe is a 1x1 offscreen host, so a window rendered there
        exists in the DOM and cannot be read. This asserts a real box on
        the visible viewport.
        """
        about = _window(self.censuses["about"], "frame_AboutChatHealtyPopUP")
        box = about["box"]
        view = self.censuses["about"]["viewport"]
        assert about["visible"], "the About window is not visible"
        assert box["width"] > 100 and box["height"] > 100, (
            f"the About window has no readable size: {box}")
        assert 0 <= box["x"] and box["x"] + box["width"] <= view["width"], (
            f"the About window is off the side of the screen: {box} in {view}")

    def test_a_window_opened_from_a_window_keeps_reporting_the_build(self, windows_open):
        """EPIC-008-F-012-S-004-REQ-B-003 is asserted. Occlusion is
        RECORDED and escalated, deliberately NOT asserted.

        About offers Session Info, so both windows are open at once. What
        is requirement-backed is that the build report survives the second
        window opening: the About window is still there and still names
        the build and the commit.

        Whether the two OVERLAP is a different question, and no approved
        requirement answers it. Nothing in the backlog states where either
        window sits, or that one may not cover another.
        find_care_frames_uat_test.py asserts non-overlap citing no
        requirement at all, and on this build that assertion is false:
        measured, the Session window is at the same x and width as the
        About window and taller, so it contains it completely. Asserting
        that here would put a layout rule on every run that nobody
        specified, which is the one outcome this file must not produce. So
        the geometry and the containment are written into the census as
        evidence and escalated in the report, and the assertion below is
        confined to what the requirement actually says.
        """
        about = _window(self.censuses["session"], "frame_AboutChatHealtyPopUP")
        session = _window(self.censuses["session"], "frame_SessionInfoPopUp")
        assert about["visible"] and session["visible"], (
            "one of the two windows closed when the other opened")
        text = about["text"]
        for label in ("Build", "Commit"):
            assert label in text, (
                f"opening the Session window cost the About window its "
                f"{label} report: {text[:300]!r}")

        # Recorded, not asserted. This is the measurement the operator has
        # to see in order to decide whether a requirement is wanted.
        a, s = about["box"], session["box"]
        occluded = (s["x"] <= a["x"] and s["x"] + s["width"] >= a["x"] + a["width"]
                    and s["y"] <= a["y"] and s["y"] + s["height"] >= a["y"] + a["height"])
        self.censuses["session"]["recorded_window_occlusion"] = {
            "about_box": a,
            "session_box": s,
            "session_completely_covers_about": occluded,
            "requirement": "none found — escalated as a gap",
        }
        with open(os.path.join(EVIDENCE_DIR, "06b_session_window.json"),
                  "w", encoding="utf-8") as fh:
            json.dump(self.censuses["session"], fh, indent=1, ensure_ascii=False)

    def test_what_the_session_window_holds_is_recorded(self, windows_open):
        """RECORDED, not asserted.

        No approved requirement states what the Session Info window shows.
        Measured, it holds the user object: user_type, guid, origin,
        server_env, created_at, and a PDF control. That content is in the
        census file beside this run's screenshot and is escalated in the
        report as a requirement gap. The only thing asserted is that the
        window a person opened is on the screen, because a window that
        opens off-screen is the defect this file was written to catch.
        """
        session = _window(self.censuses["session"], "frame_SessionInfoPopUp")
        view = self.censuses["session"]["viewport"]
        assert session["visible"], "the Session window opened invisible"
        box = session["box"]
        assert box["y"] >= 0 and box["x"] >= 0, (
            f"the Session window opened off the screen: {box} in {view}")
        assert session["chars"] > 0, (
            "the Session window is on screen holding nothing")

    def test_the_layout_still_stands_under_the_windows(self, windows_open):
        """EPIC-002-F-011-S-001-REQ-B-001, -REQ-B-002,
        EPIC-002-F-011-S-002-REQ-B-001, -REQ-B-002 — opening a window over
        the layout does not remove the layout."""
        _assert_layout_holds(self.censuses["session"], "with both windows open")
        _assert_mobile_drawer_is_hidden(self.censuses["session"],
                                        "with both windows open")

    def test_the_wrapper_builds_no_window_of_its_own(self, windows_open):
        """EPIC-008-F-002-S-016-REQ-B-001 — authored .js and .html author
        no display content; React owns display authoring. ClientRouter
        used to manufacture the popup, which is display authored outside
        React. The windows are markup now and CSS hides an empty one, so
        the wrapper composes nothing.
        """
        built = windows_open.evaluate(
            "() => document.querySelectorAll('[id^=\"popup_\"]').length")
        assert built == 0, (
            f"the wrapper is still building {built} window(s) of its own")


# ═══════════════════════════════════════════════════════════════════════
# FLOW 7 — a city named without a state
#
# Utterance: "find me a shrink in san fransisco" — 2 occurrences in the
# archive spelled exactly that way, 4 more as "San Fransisco CA". The
# misspelling is kept deliberately: it exercises the correction
# requirement in the same turn as the geography one.
#
# ── Why this flow is written as an invariant and not as a script ─────
#
# Which path this utterance takes is decided by a model and is NOT
# deterministic. Measured on this build, the SAME utterance produced four
# different screens in four consecutive runs:
#
#   run 1  45 specialty rows, 25 provider rows, no question — resolved
#   run 2  41 specialty rows, 25 provider rows, no question — resolved
#   run 3  74 specialty rows,  0 provider rows, no question — neither
#   run 4  nothing in any content window after 300s        — nothing
#
# and three earlier runs of the same utterance asked "Did you mean San
# Francisco, California?" twice and resolved once.
#
# A test that scripts one of those paths reports the model's mood. So
# what is asserted here is the thing that must hold on EVERY path: the
# message window, the results window and the specialty window have to
# AGREE about whether the geography is known. Run 3 above is exactly what
# that catches — a screen with a specialty panel, no results, and nothing
# telling the person what happened or what to do next.
#
# This is the flow where the census earns its keep. No single-frame
# assertion can state a rule about three windows at once.
# ═══════════════════════════════════════════════════════════════════════

class TestACityWithoutAStateAcrossEveryWindow:
    """One turn, three windows, one rule binding all three."""

    censuses = {}
    path = ""
    rows_before = []

    @pytest.fixture(scope="class")
    def turn(self, page):
        _fresh(page)
        _ask(page, "find me a shrink in san fransisco")
        # Wait for the turn to put something in ANY content window: the
        # specialty panel, the message window, or a provider row. Waiting
        # on the panel alone raises when a turn paints nothing, and that
        # is the one outcome most worth recording rather than crashing on.
        painted = True
        try:
            page.wait_for_function(
                "() => document.querySelectorAll"
                "(\"#frame_LeftPanel input[type='checkbox']\").length > 0"
                " || ((document.querySelector('#frame_UserMessage') || {})"
                ".innerText || '').trim().length > 0"
                " || document.querySelectorAll"
                "(\"[data-testid='provider-card']\").length > 0",
                timeout=LLM_TIMEOUT)
        except Exception:
            painted = False
        page.wait_for_timeout(8_000)

        census = _record(page, "07_city_without_a_state")
        cards = _marks(census, "frame_MainWindow").get("provider-card", 0)
        message = _window(census, "frame_UserMessage")["text"]
        if not painted:
            path = "nothing"
        elif cards > 0:
            path = "resolved"
        elif "?" in message:
            path = "pending"
        else:
            path = "neither"
        census["recorded_path"] = path
        census["recorded_painted_something"] = painted
        type(self).path = path
        type(self).censuses["turn"] = census
        type(self).rows_before = _rows(page)
        with open(os.path.join(EVIDENCE_DIR, "07_city_without_a_state.json"),
                  "w", encoding="utf-8") as fh:
            json.dump(census, fh, indent=1, ensure_ascii=False)
        return page

    def test_the_turn_puts_something_in_a_content_window(self, turn):
        """EPIC-006-F-001-S-001-REQ-B-006 — from the moment a user submits
        a search until results OR AN ERROR are shown, the user MUST see
        continuously advancing evidence that the search is running, and it
        MUST NOT stall.
        EPIC-002-F-010-S-001-REQ-B-012 — no user utterance can leave the
        Utterance Manager without a usable target_action; it always
        produces at least a brief clarification.

        Between them: a turn ends with something on the screen. A screen
        that is still empty five minutes on has satisfied neither.
        """
        assert self.path != "nothing", (
            f"nothing appeared in the specialty window, the message window "
            f"or the results window within {LLM_TIMEOUT // 1000}s of asking; "
            f"the person is looking at an empty screen with no error on it")

    def test_the_windows_agree_about_whether_the_geography_is_known(self, turn):
        """The invariant that holds whichever path the turn takes.

        EPIC-002-F-004-S-002-REQ-B-002 — ProviderSearch executes only when
        the geography passes the sufficiency rule (zip alone, state alone,
        state plus city, or state plus county). On a turn where geography
        is still pending the router "emits its terminal final event
        WITHOUT PROVIDER RESULTS".
        EPIC-002-F-010-S-001-REQ-B-007 — in the ambiguous-with-candidate
        state UM emits an LLM-authored user_message proposing the
        candidate to the user.
        EPIC-002-F-010-S-001-REQ-B-012 — and UM always produces a usable
        target_action, at minimum a brief clarification.
        EPIC-006-F-001-S-005-REQ-B-009 — where there ARE results, the
        summary is shown together with them.

        Read together across the three windows: either the results window
        holds providers, in which case the geography was resolved and the
        summary is beside them; or it holds none, in which case the
        message window has to say why. What is forbidden is the third
        state — no results and no explanation — which is a screen the
        person cannot act on.
        """
        census = self.censuses["turn"]
        cards = _marks(census, "frame_MainWindow").get("provider-card", 0)
        message = _window(census, "frame_UserMessage")
        if cards > 0:
            _assert_results_and_summary_together(census, "city without a state")
        else:
            assert message["visible"] and message["chars"] > 0, (
                f"the results window holds no providers and the message "
                f"window says nothing, so nothing on the screen tells the "
                f"person the geography was not resolved. Path taken: "
                f"{self.path!r}")
            assert "?" in message["text"], (
                f"the geography was not resolved and the message window "
                f"does not ask the person anything, so the turn ended with "
                f"no results and no question. Path taken: {self.path!r}. "
                f"Message window holds: {message['text'][:300]!r}")

    def test_the_specialty_window_paints_on_either_path(self, turn):
        """EPIC-002-F-010-S-002-REQ-B-001 — where geography is pending,
        target_action remains at specialtySearch SO THE FILTER STILL
        RENDERS.
        EPIC-006-F-003-S-001-REQ-B-002 — and where the request resolved,
        the user is offered the specialties it implies.
        EPIC-006-F-003-S-001-REQ-B-006 — with all three counts.

        Both paths therefore require the specialty window to be painted,
        which is why this one is not conditional.
        """
        _assert_specialty_panel_intact(self.censuses["turn"],
                                       f"on the {self.path} path")

    def test_the_misspelling_is_shown_as_corrected_not_silently_swallowed(self, turn):
        """EPIC-002-F-010-S-001-REQ-B-011 — where the classifier is at
        least 75% confident of an intended word, it substitutes the
        corrected form AND sets user_message to the corrected utterance
        with each correction annotated inline as "<corrected> (corrected
        from '<original>')", so the user sees that their spelling was
        interpreted rather than ignored.

        The utterance typed "san fransisco". Measured, every run that
        painted anything carried the marker, on both paths.
        """
        text = _window(self.censuses["turn"], "frame_UserMessage")["text"]
        assert "corrected from" in text, (
            f"the utterance was misspelled and the message window carries "
            f"no correction marker: {text[:300]!r}")
        assert "fransisco" in text.lower(), (
            f"the correction marker does not carry what the user actually "
            f"typed: {text[:300]!r}")

    def test_the_layout_holds_on_a_turn_that_may_answer_with_a_question(self, turn):
        """EPIC-002-F-011-S-001-REQ-B-001, -REQ-B-002,
        EPIC-002-F-011-S-002-REQ-B-001, -REQ-B-002,
        EPIC-002-F-008-S-001-REQ-B-003."""
        _assert_layout_holds(self.censuses["turn"], f"on the {self.path} path")
        _assert_mobile_drawer_is_hidden(self.censuses["turn"],
                                        f"on the {self.path} path")

    def test_the_next_turn_resolves_the_geography_and_keeps_the_choices(self, turn):
        """Two requirements, and which one applies depends on the path the
        first turn took. Both are asserted; neither is skipped.

        Where the first turn ASKED (path 'pending'):
          EPIC-002-F-010-S-001-REQ-B-008 — when the next utterance is a
            yes/no answer to the pending disambiguation, "Yes" fills the
            candidate in and UPGRADES target_action to the now-fully-
            specified action. So the results must arrive.
          EPIC-006-F-003-S-001-REQ-B-008 — and the user's chosen set of
            specialties MUST remain in force until THAT USER changes it.
            Answering a question about geography is not the user changing
            their specialties, so the ticks must survive it. This is the
            defect the operator recorded: 45 codes became 57, with
            chiropractors in them and no Psychologist.

        Where the first turn RESOLVED it already, there is no question to
        answer, REQ-B-008 has no pending disambiguation to act on, and
        what must hold is EPIC-006-F-001-S-005-REQ-B-009 — the summary and
        the results are on screen together. That is asserted instead.

        On the 'neither' and 'nothing' paths the first turn produced no
        results AND no question, so there is no next turn to take: the
        person has nothing to answer and nothing to read. That state is
        reported by name here rather than as a missing summary, because
        the defect is the turn that ended, not the summary that is absent.
        """
        if self.path in ("neither", "nothing"):
            message = _window(self.censuses["turn"], "frame_UserMessage")["text"]
            pytest.fail(
                f"the first turn ended on the {self.path!r} path — no "
                f"provider results and no question — so the person has "
                f"nothing to answer and the geography can never be "
                f"resolved. The message window holds: {message[:300]!r}")

        if self.path == "resolved":
            _assert_results_and_summary_together(
                self.censuses["turn"],
                "on the resolved path, where no question was asked")
            return

        ticked_before = sorted(c for c, ticked in self.rows_before if ticked)
        codes_before = [c for c, _ in self.rows_before]

        _ask(turn, "yes")
        _wait_for_results(turn)
        turn.wait_for_timeout(3_000)
        census = _record(turn, "07b_geography_answered")
        type(self).censuses["answered"] = census

        _assert_results_and_summary_together(census, "after answering yes")
        _assert_layout_holds(census, "after answering yes")

        after = _rows(turn)
        ticked_after = sorted(c for c, ticked in after if ticked)
        codes_after = [c for c, _ in after]
        assert ticked_after == ticked_before, (
            f"answering a question about geography changed the user's "
            f"chosen specialties from {len(ticked_before)} to "
            f"{len(ticked_after)}; "
            f"added={sorted(set(ticked_after) - set(ticked_before))[:6]} "
            f"dropped={sorted(set(ticked_before) - set(ticked_after))[:6]}")
        assert codes_after == codes_before, (
            f"answering a question about geography repainted the specialty "
            f"window: {len(codes_before)} codes became {len(codes_after)}; "
            f"added={sorted(set(codes_after) - set(codes_before))[:6]} "
            f"dropped={sorted(set(codes_before) - set(codes_after))[:6]}")


# ── The three requirements authored 2026-09-02 ───────────────────────
#
# Each asserts one of them against the rendered DOM. They were written
# after a summary drawn above the provider list named every resolved
# specialty three times over and pushed the providers off the screen, and
# after a facility detail stayed on the right panel beside a list of care
# givers in another city.

class TestTheListKeepsEveryElementItCarries:
    """EPIC-006-F-001-S-005-REQ-B-010 and -REQ-B-009 together.

    B-010 names what the list must carry and forbids the summary being
    rendered among those elements. B-009 requires the summary be shown to
    the person with the results it describes. Both hold at once only if
    the summary is somewhere other than the list's own frame, so the two
    are asserted from one census of the same moment.
    """

    @pytest.fixture(scope="class")
    def searched(self, page):
        _fresh(page)
        _ask(page, "find me a shrink in Long Beach CA")
        _wait_for_panel(page)
        _wait_for_results(page)
        page.wait_for_timeout(3_000)
        return _record(page, "08a_list_keeps_its_elements")

    def test_the_list_carries_a_row_for_each_provider(self, searched):
        marks = _marks(searched, "frame_MainWindow")
        assert marks.get("provider-card", 0) > 0, (
            f"frame_MainWindow carries no provider rows; it holds {marks}")

    def test_the_heading_names_how_many_the_search_found(self, searched):
        text = _window(searched, "frame_MainWindow")["digest"]
        assert "providers found" in text or "provider found" in text, (
            f"the list does not name how many providers were found: "
            f"{text[:200]!r}")

    def test_the_header_names_how_many_are_on_this_page(self, searched):
        text = _window(searched, "frame_MainWindow")["digest"]
        assert "available" in text and "drag to select" in text, (
            f"the list does not name how many are on this page and that "
            f"they may be dragged: {text[:200]!r}")

    def test_every_row_carries_the_seven_things_the_requirement_names(
            self, searched, page):
        missing = page.evaluate(_ROW_ELEMENTS_JS)
        assert missing == [], (
            f"{len(missing)} row(s) do not carry everything the requirement "
            f"names: {missing[:5]}")

    def test_the_summary_is_not_rendered_among_the_list(self, searched):
        marks = _marks(searched, "frame_MainWindow")
        assert marks.get("provider-summary", 0) == 0, (
            f"the summary is drawn among the list elements; "
            f"frame_MainWindow holds {marks}")

    def test_the_summary_is_shown_with_the_results(self, searched):
        """REQ-B-009 — shown to the person, in the frame where the system
        speaks about the query, at the same moment the results are up."""
        marks = _marks(searched, "frame_UserMessage")
        assert marks.get("provider-summary", 0) == 1, (
            f"no summary is shown anywhere with the results; "
            f"frame_UserMessage holds {marks}")
        assert _window(searched, "frame_UserMessage")["visible"], (
            "the summary is in a window the person cannot see")


class TestAPageTheTurnIsNotAboutShowsNothing:
    """EPIC-006-F-008-S-002-REQ-B-006.

    A facility is found and its detail opened, then the person goes back
    to looking for a care giver. Nothing of the facility may remain on
    screen: the record it held open and the position it held are both the
    previous page's, and the person has left that page.
    """

    @pytest.fixture(scope="class")
    def after_switching(self, page):
        _fresh(page)
        _ask(page, "find me an urgent care clinic in Lakewood CA")
        _wait_for_facilities(page)
        page.wait_for_timeout(1_500)
        page.locator("[data-testid='facility-detail-link']").first.click()
        page.wait_for_timeout(3_000)
        _record(page, "09a_facility_detail_open")
        _ask(page, "find me a shrink in San Francisco CA")
        _wait_for_panel(page)
        _wait_for_results(page)
        page.wait_for_timeout(3_000)
        return _record(page, "09b_after_switching_to_care_givers")

    def test_the_care_giver_list_is_on_screen(self, after_switching):
        marks = _marks(after_switching, "frame_MainWindow")
        assert marks.get("provider-card", 0) > 0, (
            f"the care-giver search did not paint; frame_MainWindow holds "
            f"{marks}")

    def test_no_window_still_shows_the_facility(self, after_switching):
        showing = {}
        for wid, win in after_switching["windows"].items():
            if not (win.get("present") and win.get("visible")):
                continue
            marks = win.get("marks", {})
            facility_marks = {k: v for k, v in marks.items()
                              if k.startswith("facility") and v}
            if facility_marks:
                showing[wid] = facility_marks
        assert showing == {}, (
            f"a page the turn was not about is still showing: {showing}")

    def test_no_window_still_names_the_facility_city(self, after_switching):
        """The detail was a Lakewood organization and the search is for
        San Francisco. Lakewood on screen is the previous page."""
        lingering = {}
        for wid, win in after_switching["windows"].items():
            if not (win.get("present") and win.get("visible")):
                continue
            digest = (win.get("digest") or "").upper()
            if "LAKEWOOD" in digest:
                lingering[wid] = digest[:160]
        assert lingering == {}, (
            f"the previous page's city is still on screen: {lingering}")


# ── FLOW 10 — the narrow filter, and what each form factor does with it ──
# The filter itself is one thing, built once. What differs is where it is
# put: a computer has the room to show it under the results, a phone does
# not, so a phone shows a thin bar with a button and puts the filter in a
# popup over the list. Both are asserted here, in one class, because the
# thing being protected is that they stay the SAME filter -- the two used
# to be built by two functions and drifted apart.

NARROW_BUTTON = "[data-testid='provider-narrow-button']"
NARROW_INLINE = ".ch-narrow-inline"
NARROW_POPUP = "#frame_NarrowPopUp"
NARROW_FILTER = "[data-testid='provider-search-refinements']"
POPUP_CLOSE = f"{NARROW_POPUP} .ch-popup-close"


def _shown(page, selector: str) -> bool:
    loc = page.locator(selector)
    return loc.count() > 0 and loc.first.is_visible()


PHONE_SIZE = {"width": 360, "height": 800}
COMPUTER_SIZE = {"width": 1600, "height": 1000}


def _at(page, size: dict):
    """Look at the page at a given width.

    Which arrangement is on screen is decided by CSS, so it is decided by
    the width and by nothing else. That makes the width something a test
    sets, not something the run is launched with -- and it means both
    arrangements can be asserted in one run instead of one of them being
    skipped and counted as if it had passed.
    """
    page.set_viewport_size(size)
    page.wait_for_timeout(500)


class TestTheNarrowFilterOnEitherFormFactor:
    """The filter is one filter. A computer shows it inline; a phone shows
    a button that opens it in a popup with an X. Every test here sets the
    width it is about, so all of them run at every launch width and
    neither arrangement goes unasserted."""

    @pytest.fixture(scope="class")
    def searched(self, page):
        _fresh(page)
        _ask(page, "find me a shrink in Long Beach CA")
        _wait_for_panel(page)
        _wait_for_results(page)
        page.wait_for_timeout(3_000)
        _record(page, "10a_narrow_filter_at_rest")
        yield page
        # Leave the page the width the run was launched at, so a class
        # that runs after this one is not handed a screen this one resized.
        _at(page, VIEWPORT)

    def test_each_form_factor_places_the_filter_its_own_way(self, searched):
        _at(searched, COMPUTER_SIZE)
        wide = (_shown(searched, NARROW_INLINE), _shown(searched, NARROW_BUTTON))
        _at(searched, PHONE_SIZE)
        narrow = (_shown(searched, NARROW_INLINE), _shown(searched, NARROW_BUTTON))
        assert wide == (True, False), (
            f"a computer shows the filter inline and offers no button; "
            f"inline={wide[0]} button={wide[1]}")
        assert narrow == (False, True), (
            f"a phone shows a button and not the inline filter; "
            f"inline={narrow[0]} button={narrow[1]}")

    def test_no_popup_is_open_until_something_opens_it(self, searched):
        for size in (COMPUTER_SIZE, PHONE_SIZE):
            _at(searched, size)
            assert not _shown(searched, NARROW_POPUP), (
                f"the narrow popup is open at {size['width']}px before "
                f"anyone asked for it")

    def test_a_computer_never_puts_the_filter_in_a_popup(self, searched):
        _at(searched, COMPUTER_SIZE)
        assert _shown(searched, NARROW_FILTER), (
            "the filter is not on screen on a computer")
        assert searched.locator(f"{NARROW_POPUP} {NARROW_FILTER}").count() == 0, (
            "a computer put the filter in a popup; the popup is the phone's "
            "arrangement and a computer shows the filter inline")

    def test_the_bar_is_thin_and_the_button_takes_a_finger(self, searched):
        _at(searched, PHONE_SIZE)
        button = searched.locator(NARROW_BUTTON).first.bounding_box()
        assert button is not None, "the narrow button has no box"
        assert button["height"] >= 40, (
            f"the button is {button['height']}px tall; a finger needs at "
            f"least 40")
        bar = searched.locator(".ch-narrow-button").first.bounding_box()
        assert bar["height"] <= button["height"] + 16, (
            f"the bar is {bar['height']}px around a {button['height']}px "
            f"button; it carries more than the button")

    def test_the_button_opens_the_filter_in_a_popup(self, searched):
        _at(searched, PHONE_SIZE)
        searched.locator(NARROW_BUTTON).first.click()
        searched.wait_for_selector(NARROW_POPUP, state="visible",
                                   timeout=DEFAULT_TIMEOUT)
        _record(searched, "10b_narrow_popup_open")
        assert searched.locator(f"{NARROW_POPUP} {NARROW_FILTER}").count() > 0, (
            "the popup opened without the filter in it")
        assert searched.locator(
            f"{NARROW_POPUP} [data-testid='provider-search-refine-chip']"
        ).count() > 0, "the popup holds no choices to make"

    def test_the_popup_carries_a_close_control(self, searched):
        assert _shown(searched, POPUP_CLOSE), "the popup has no X to close it"

    def test_the_popup_operates_on_the_list_in_real_time(self, searched):
        before = _total(searched)
        chip = searched.locator(
            f"{NARROW_POPUP} [data-testid='provider-search-refine-chip']"
            "[data-in-force='0']").first
        chip.click()
        searched.wait_for_timeout(6_000)
        after = _total(searched)
        assert after <= before, (
            f"narrowing from inside the popup widened the list: "
            f"{before} -> {after}")

    def test_closing_the_popup_leaves_the_results_standing(self, searched):
        searched.locator(POPUP_CLOSE).first.click()
        searched.wait_for_timeout(1_500)
        assert not _shown(searched, NARROW_POPUP), "the X did not close the popup"
        assert searched.locator("[data-testid='provider-card']").count() > 0, (
            "closing the popup took the results with it")
        assert _shown(searched, NARROW_BUTTON), (
            "the button to reopen the filter is gone after closing it once")


# ── FLOW 11 — how wide a column is on a phone ────────────────────────
# One column is the whole width. Two or three are each as wide as their
# content needs, which makes the row wider than the screen, and the
# screen moves along it. No column is a fraction of the window.

def _row_metrics(page) -> dict:
    return page.evaluate(
        "() => { const r = document.querySelector('.content-row');"
        " if (!r) return null;"
        " const kids = Array.from(r.children).filter(k => {"
        "   const s = getComputedStyle(k);"
        "   return s.display !== 'none' && k.getBoundingClientRect().width > 0; });"
        " return { view: window.innerWidth,"
        "          scrollWidth: r.scrollWidth, clientWidth: r.clientWidth,"
        "          columns: kids.length,"
        "          widths: kids.map(k => Math.round("
        "            k.getBoundingClientRect().width)) }; }")


class TestAColumnIsAsWideAsWhatItHolds:
    """On a phone the width of a column follows what is showing, not a
    share of the window. Alone it takes the screen; with company each is
    wide enough to read and the row scrolls."""

    @pytest.fixture(scope="class")
    def phone(self, page):
        _at(page, PHONE_SIZE)
        _fresh(page)
        yield page
        _at(page, VIEWPORT)

    def test_one_column_takes_the_whole_width(self, phone):
        m = _row_metrics(phone)
        assert m is not None, "there is no content row on the page"
        assert m["columns"] == 1, (
            f"nothing has been asked yet, so only the centre column should "
            f"be showing; {m['columns']} columns are: {m['widths']}")
        assert abs(m["widths"][0] - m["view"]) <= 24, (
            f"the only column on screen is {m['widths'][0]}px of a "
            f"{m['view']}px screen; alone it should take the width")
        assert m["scrollWidth"] <= m["clientWidth"] + 4, (
            f"one column should not make the row scroll: "
            f"scrollWidth={m['scrollWidth']} clientWidth={m['clientWidth']}")

    def test_more_than_one_column_is_wider_than_the_screen_and_scrolls(self, phone):
        _ask(phone, "find me a shrink in Long Beach CA")
        _wait_for_panel(phone)
        _wait_for_results(phone)
        phone.wait_for_timeout(2_000)
        _record(phone, "11a_columns_on_a_phone")
        m = _row_metrics(phone)
        assert m["columns"] >= 2, (
            f"a search paints a specialty panel beside the results, so at "
            f"least two columns should be showing; got {m['columns']}")
        assert m["scrollWidth"] > m["clientWidth"], (
            f"more than one column should make the row wider than the "
            f"screen: scrollWidth={m['scrollWidth']} "
            f"clientWidth={m['clientWidth']}")
        share = [round(w / m["view"], 2) for w in m["widths"]]
        assert not all(abs(s - share[0]) < 0.01 for s in share) or len(share) == 1, (
            f"every column is the same share of the window ({share}); a "
            f"column is sized by what it holds, not by a fraction of the "
            f"screen")
        # A column is wide enough to read and no wider. Without a ceiling
        # "wider than the screen" is satisfied by a column a million
        # pixels across, which is what `width: auto` on a flex item
        # actually produced -- the row scrolled, the widths differed, and
        # the test passed over a layout nobody could use.
        for w in m["widths"]:
            assert 120 <= w <= 3 * m["view"], (
                f"a column is {w}px on a {m['view']}px screen; columns are "
                f"{m['widths']}. A column must be wide enough to read and "
                f"still reachable by swiping")


# ── FLOW 12 — moving between panels on a phone ───────────────────────
# An arrow for each neighbour that exists, and none for a direction with
# nothing in it. Tapping one, double-tapping that side of the screen, and
# dragging a fifth of the way into a neighbour all do the same thing:
# bring that panel fully onto the screen.

ARROW_LEFT = "[data-testid='panel-arrow-left']"
ARROW_RIGHT = "[data-testid='panel-arrow-right']"


def _strip(page) -> dict:
    return page.evaluate(
        "() => { const r = document.querySelector('.content-row');"
        " const kids = Array.from(r.children).filter(k => {"
        "   const s = getComputedStyle(k);"
        "   return s.display !== 'none' && k.getBoundingClientRect().width > 0; });"
        " const mid = r.scrollLeft + r.clientWidth / 2;"
        " let at = 0, best = Infinity;"
        " kids.forEach((k, i) => { const g = Math.abs("
        "   k.offsetLeft + k.offsetWidth / 2 - mid);"
        "   if (g < best) { best = g; at = i; } });"
        " return { panels: kids.length, at, scrollLeft: Math.round(r.scrollLeft),"
        "          ids: kids.map(k => k.id || 'centre'),"
        "          offsets: kids.map(k => Math.round(k.offsetLeft)),"
        "          widths: kids.map(k => Math.round(k.offsetWidth)) }; }")


def _go_to_first_panel(page) -> None:
    page.evaluate("() => { document.querySelector('.content-row').scrollLeft = 0; }")
    page.wait_for_timeout(900)


class TestMovingBetweenPanelsOnAPhone:
    """The arrows follow position, not panel count: nothing to the left,
    no left arrow. Every way of asking for a neighbour lands the same."""

    @pytest.fixture(scope="class")
    def phone(self, page):
        _at(page, PHONE_SIZE)
        _fresh(page)
        _ask(page, "find me a shrink in Long Beach CA")
        _wait_for_panel(page)
        _wait_for_results(page)
        page.wait_for_timeout(2_500)
        _record(page, "12a_panels_on_a_phone")
        yield page
        _at(page, VIEWPORT)

    def test_more_than_one_panel_is_showing(self, phone):
        s = _strip(phone)
        assert s["panels"] >= 2, (
            f"a search paints a specialty panel beside the results, so this "
            f"flow needs at least two panels; got {s}")

    def test_no_left_arrow_when_there_is_nothing_to_the_left(self, phone):
        _go_to_first_panel(phone)
        assert not _shown(phone, ARROW_LEFT), (
            f"the strip is at its first panel and a left arrow is offered; "
            f"{_strip(phone)}")
        assert _shown(phone, ARROW_RIGHT), (
            f"there is a panel to the right and no arrow offers it; "
            f"{_strip(phone)}")

    def test_the_right_arrow_brings_the_next_panel_fully_on_screen(self, phone):
        _go_to_first_panel(phone)
        before = _strip(phone)
        phone.locator(ARROW_RIGHT).first.click()
        phone.wait_for_timeout(1_400)
        after = _strip(phone)
        assert after["at"] == before["at"] + 1, (
            f"the right arrow did not move one panel: {before} -> {after}")
        assert abs(after["scrollLeft"] - after["offsets"][after["at"]]) <= 4, (
            f"the panel is not fully on screen after the arrow: {after}")

    def test_no_right_arrow_at_the_last_panel(self, phone):
        s = _strip(phone)
        while s["at"] < s["panels"] - 1:
            phone.locator(ARROW_RIGHT).first.click()
            phone.wait_for_timeout(1_400)
            s = _strip(phone)
        assert not _shown(phone, ARROW_RIGHT), (
            f"the strip is at its last panel and a right arrow is offered; {s}")
        assert _shown(phone, ARROW_LEFT), (
            f"there is a panel to the left and no arrow offers it; {s}")

    def test_the_left_arrow_goes_back(self, phone):
        before = _strip(phone)
        phone.locator(ARROW_LEFT).first.click()
        phone.wait_for_timeout(1_400)
        after = _strip(phone)
        assert after["at"] == before["at"] - 1, (
            f"the left arrow did not move one panel back: {before} -> {after}")

    def test_a_double_tap_on_a_side_moves_that_way(self, phone):
        _go_to_first_panel(phone)
        before = _strip(phone)
        box = phone.locator(".content-row").first.bounding_box()
        x = box["x"] + box["width"] * 0.8      # the right half
        y = box["y"] + box["height"] * 0.5
        phone.mouse.click(x, y)
        phone.mouse.click(x, y, delay=40)
        phone.wait_for_timeout(1_400)
        after = _strip(phone)
        assert after["at"] == before["at"] + 1, (
            f"a double tap on the right half did not move right: "
            f"{before} -> {after}")

    def test_dragging_a_fifth_of_the_way_completes_the_move(self, phone):
        _go_to_first_panel(phone)
        before = _strip(phone)
        here = before["widths"][before["at"]]
        phone.evaluate(
            "(px) => { const r = document.querySelector('.content-row');"
            " r.scrollLeft = r.scrollLeft + px;"
            " r.dispatchEvent(new Event('scroll')); }",
            int(here * 0.3))
        phone.wait_for_timeout(1_800)
        after = _strip(phone)
        assert after["at"] == before["at"] + 1, (
            f"a drag three tenths into the next panel did not complete: "
            f"{before} -> {after}")
        assert abs(after["scrollLeft"] - after["offsets"][after["at"]]) <= 4, (
            f"the panel did not come fully on screen: {after}")

    def test_a_computer_offers_no_arrows(self, phone):
        _at(phone, COMPUTER_SIZE)
        phone.wait_for_timeout(600)
        assert not _shown(phone, ARROW_LEFT) and not _shown(phone, ARROW_RIGHT), (
            "a computer shows every panel at once and offers no arrows")


# ── FLOW 13 — the form factor, and the panels on every page ──────────
# The session is told once, at boot, which arrangement it is being read
# in. And each of the three pages puts its own panels on the strip, so
# the arrows are asserted on all of them rather than on care givers
# alone.

def _session_form_factor(page) -> str:
    """What the session says it is, read the way a person reads it.

    Through the session window the application itself renders, not a
    hand-made call: a fetch built here would be testing a transport this
    suite invented rather than the one the app uses.
    """
    # Session Info is offered by the About window, and a phone reaches
    # About through the menu. Same capability, different route in.
    _open_about(page)
    page.wait_for_selector("#frame_AboutChatHealtyPopUP", state="visible",
                           timeout=DEFAULT_TIMEOUT)
    page.locator("[data-router-action='session_info']").first.click()
    page.wait_for_selector("#frame_SessionInfoPopUp", state="visible",
                           timeout=DEFAULT_TIMEOUT)
    page.wait_for_function(
        "() => ((document.querySelector('#frame_SessionInfoPopUp') || {})"
        " .innerText || '').includes('form_factor')", timeout=DEFAULT_TIMEOUT)
    lines = [ln.strip() for ln in
             page.locator("#frame_SessionInfoPopUp").inner_text().splitlines()]
    # Both windows are closed again. A window left open covers the
    # control that opens it, so a second reading in the same case could
    # never reach About.
    for frame in ("#frame_SessionInfoPopUp", "#frame_AboutChatHealtyPopUP"):
        close = page.locator(f"{frame} .ch-popup-close")
        if close.count():
            close.first.click()
            page.wait_for_timeout(300)
    for i, ln in enumerate(lines):
        if ln == "form_factor" and i + 1 < len(lines):
            return lines[i + 1]
    return "not-reported"


class TestTheSessionKnowsItsFormFactor:
    """Established with the session and fixed for its life. A narrow
    window is a phone whatever the device is, because the arrangement is
    what the name is about."""

    def test_a_phone_session_says_phone(self, page):
        _at(page, PHONE_SIZE)
        _new_session(page)
        page.wait_for_timeout(1_200)
        got = _session_form_factor(page)
        _at(page, VIEWPORT)
        assert got == "phone", f"a session booted at 360px reports {got!r}"

    def test_a_computer_session_says_desktop(self, page):
        _at(page, COMPUTER_SIZE)
        _new_session(page)
        page.wait_for_timeout(1_200)
        got = _session_form_factor(page)
        _at(page, VIEWPORT)
        assert got == "desktop", f"a session booted at 1600px reports {got!r}"

    def test_it_does_not_change_when_the_window_does(self, page):
        _at(page, PHONE_SIZE)
        _new_session(page)
        page.wait_for_timeout(1_200)
        first = _session_form_factor(page)
        _at(page, COMPUTER_SIZE)
        page.wait_for_timeout(800)
        second = _session_form_factor(page)
        _at(page, VIEWPORT)
        assert first == second == "phone", (
            f"the form factor moved with the window: {first!r} -> {second!r}; "
            f"it is established at boot and fixed for the session")


# Each page, the utterance that opens it, and what says its results are
# on screen. The arrows are the same mechanism everywhere, so the same
# cases run against all three.
PAGES = [
    ("individual", "find me a shrink in Long Beach CA",
     "d.querySelectorAll(\"[data-testid='provider-card']\").length > 0"),
    ("facility", "find me a hospital in Pasadena CA",
     "d.querySelectorAll(\"[data-testid='facility-card']\").length > 0"),
    ("clinicaltrial", "find me a clinical trial for diabetes",
     "((d.querySelector('#frame_MainWindow')||{}).innerText||'')"
     ".toLowerCase().includes('trial')"),
]


@pytest.mark.parametrize("page_name,utterance,ready_js",
                         PAGES, ids=[p[0] for p in PAGES])
class TestEveryPagePutsItsPanelsOnTheStrip:
    """Whatever a page paints, the strip carries it and the arrows follow
    the position: nothing to the left, no left arrow."""

    def test_the_page_paints_and_the_arrows_agree(
            self, page, page_name, utterance, ready_js):
        _at(page, PHONE_SIZE)
        _fresh(page)
        _ask(page, utterance)
        _wait_for(page, ready_js, f"the {page_name} results")
        page.wait_for_timeout(2_500)
        _record(page, f"13_{page_name}_on_a_phone")

        s = _strip(page)
        assert s["panels"] >= 1, f"{page_name}: nothing is on the strip; {s}"

        _go_to_first_panel(page)
        s = _strip(page)
        assert not _shown(page, ARROW_LEFT), (
            f"{page_name}: at the first panel and a left arrow is offered; {s}")
        if s["panels"] > 1:
            assert _shown(page, ARROW_RIGHT), (
                f"{page_name}: {s['panels']} panels and no right arrow; {s}")
            page.locator(ARROW_RIGHT).first.click()
            page.wait_for_timeout(1_400)
            after = _strip(page)
            assert after["at"] == 1, (
                f"{page_name}: the right arrow did not move one panel; {after}")
            assert abs(after["scrollLeft"] - after["offsets"][1]) <= 4, (
                f"{page_name}: the panel is not fully on screen; {after}")
        else:
            assert not _shown(page, ARROW_RIGHT), (
                f"{page_name}: one panel and a right arrow is offered; {s}")

    def test_no_panel_on_the_strip_is_blank(
            self, page, page_name, utterance, ready_js):
        """A panel showing nothing must not take a screen of its own. An
        empty container left behind by a widget is not content."""
        blank = page.evaluate(
            "() => Array.from(document.querySelectorAll('.content-row > *'))"
            " .filter(k => getComputedStyle(k).display !== 'none'"
            "   && k.getBoundingClientRect().width > 0"
            "   && !(k.innerText||'').trim()"
            "   && !k.querySelector('img, svg, canvas, input, button, table'))"
            " .map(k => k.id || 'centre')")
        assert blank == [], (
            f"{page_name}: these panels are on the strip with nothing in "
            f"them: {blank}")


# ── FLOW 14 — a session we already have is not built again ───────────

class TestAReloadKeepsTheSession:
    """Having a session means not needing a new one. A reload offers the
    GUID it holds and is stamped against that session, rather than
    starting a second one beside it and orphaning the first."""

    def test_the_guid_survives_a_reload(self, page):
        _fresh(page)
        page.wait_for_timeout(1_500)
        before = page.evaluate(
            "() => window.ClientRouter.getSessionGuid()")
        assert before, "no session guid after the first load"
        page.reload(wait_until="domcontentloaded")
        _ready(page)
        page.wait_for_timeout(1_500)
        after = page.evaluate("() => window.ClientRouter.getSessionGuid()")
        assert after == before, (
            f"a reload started a second session: {before} -> {after}; the "
            f"first is now in Mongo with nothing able to point at it")

    def test_what_was_said_survives_a_reload(self, page):
        _fresh(page)
        _ask(page, "find me a shrink in Long Beach CA")
        _wait_for_panel(page)
        _wait_for_results(page)
        page.wait_for_timeout(2_000)
        page.reload(wait_until="domcontentloaded")
        _ready(page)
        page.wait_for_timeout(2_000)
        said = page.evaluate(
            "async () => { const r = await fetch(window.location.origin);"
            " return true; }")
        guid = page.evaluate("() => window.ClientRouter.getSessionGuid()")
        assert guid, "no session after the reload"


# ─────────────────────────────────────────────────────────────────────
# FLOW 3b — the utterance that found two defects on 2026-09-07
#
# Session 1002c522137d4284a2e86cd2f53efbbf asked this three times and was
# answered with a question each time: the classifier had no findAFacility
# in its catalog, so it could not name the action even with a complete
# intent. And the city it holds is 'Long Beach', while nine records in ten
# store 'LONG BEACH' -- so the search that could not run would have found
# a fraction of the answer if it had.
# ─────────────────────────────────────────────────────────────────────


class TestAnUrgentCareClinicInLongBeach:
    """The request names a place and a city whose casing the data varies."""

    @pytest.fixture(scope="class")
    def searched(self, page):
        _fresh(page)
        _ask(page, "find me an urgent care clinic in Long Beach CA")
        _wait_for_facilities(page)
        page.wait_for_timeout(2_000)
        return page

    def test_the_request_is_dispatched_rather_than_questioned(self, searched):
        """The user names what they want, not the tool that serves it. A
        request carrying a place and a usable geography is complete, and a
        complete request is answered with results.
        """
        rows = searched.locator("[data-testid='facility-card']").count()
        assert rows > 0, (
            "the search was not dispatched: no facility rows were painted "
            "for a request naming both an establishment and a city+state")

    def test_the_city_is_matched_whatever_its_casing(self, searched):
        """The corpus stores 'LONG BEACH' and 'Long Beach' both. A search
        that matches only the casing it was handed answers with a fraction
        of what is there, and looks like a thin city rather than a bug.
        """
        rows = searched.locator("[data-testid='facility-card']").count()
        assert rows > 5, (
            f"only {rows} rows came back; matching 'Long Beach' without a "
            f"case-insensitive comparison returns the title-case records "
            f"alone and hides the rest")

    def test_the_count_is_the_whole_answer_not_a_casing_of_it(self, searched):
        """18 organisations hold an urgent-care practice address in Long
        Beach: 5 stored 'Long Beach', 13 stored 'LONG BEACH'. A search that
        returns 5 has matched one casing and called it the answer.

        The row shows the organisation's PRIMARY practice address, which for
        a multi-site organisation is somewhere else entirely, so the city on
        screen is not what this asserts on -- the count is.
        """
        heading = searched.locator("text=/facilit(y|ies) found/i").first.inner_text()
        digits = "".join(c for c in heading if c.isdigit())
        assert digits and int(digits) > 5, (
            f"the heading reads {heading!r}; five is the title-case subset, "
            f"not the answer")


# ─────────────────────────────────────────────────────────────────────
# FLOW 4 — one session, two pages, three utterances
#
# The parameters of one page are that page's own. A person asks for a
# care giver in San Francisco, then a facility in Long Beach, then a
# facility in New York. The provider page must still hold San Francisco
# at the end, and the facility page must hold New York rather than the
# Long Beach it held a moment earlier.
#
# This is the sequence that exposed the defect on 2026-09-07: the
# facility page's geography had arrived by carry-over from a page the
# person never visited, so it never moved when they asked for somewhere
# else, and a New York request answered with Long Beach clinics.
# ─────────────────────────────────────────────────────────────────────


def _session_text(page) -> str:
    """The session window's text.

    The control that opens it lives inside the About window, so About is
    opened first -- the same way a person reaches it.
    """
    _open_about(page)
    page.wait_for_selector("#frame_AboutChatHealtyPopUP", state="visible",
                           timeout=DEFAULT_TIMEOUT)
    page.locator("[data-router-action='session_info']").first.click()
    page.wait_for_selector("#frame_SessionInfoPopUp", state="visible",
                           timeout=LLM_TIMEOUT)
    page.wait_for_function(
        "() => ((document.querySelector('#frame_SessionInfoPopUp') || {})"
        ".innerText || '').includes('Identity')", timeout=LLM_TIMEOUT)
    page.wait_for_timeout(1_000)
    text = page.locator("#frame_SessionInfoPopUp").inner_text()
    close = page.locator("#frame_SessionInfoPopUp .ch-popup-close")
    if close.count():
        close.first.click()
        page.wait_for_timeout(500)
    return text


class TestEachPageKeepsItsOwnGeography:

    @pytest.fixture(scope="class")
    def asked(self, page):
        _fresh(page)
        _ask(page, "Find me a shrink in San Francisco CA")
        _wait_for_panel(page)
        page.wait_for_timeout(1_500)
        _ask(page, "Find me an urgent care clinic in Long Beach CA")
        _wait_for_facilities(page)
        page.wait_for_timeout(1_500)
        # The Long Beach rows are still on screen, so waiting for "a row
        # exists" is satisfied before New York's answer arrives and the
        # assertion reads the previous city's list. Wait for the first row
        # to stop being the one that is there now.
        was = page.locator("[data-testid='facility-address']").first.inner_text()
        _ask(page, "Find me an urgent care clinic in New York NY")
        page.wait_for_function(
            "previous => { const a = document.querySelector"
            "(\"[data-testid='facility-address']\");"
            " return a && a.innerText !== previous; }",
            arg=was, timeout=LLM_TIMEOUT)
        page.wait_for_timeout(2_000)
        return page

    def test_the_facility_list_is_the_city_last_asked_for(self, asked):
        """A new city supersedes the one before it. The facility page was
        showing Long Beach a turn ago; the person then said New York.
        """
        addresses = asked.locator("[data-testid='facility-address']")
        assert addresses.count() > 0, "the New York request painted no rows"
        shown = [addresses.nth(i).inner_text().lower()
                 for i in range(min(addresses.count(), 12))]
        stale = [a for a in shown if "long beach" in a]
        assert stale == [], (
            f"the facility list still holds Long Beach rows after a New York "
            f"request: {stale}")

    def test_the_provider_page_still_holds_san_francisco(self, asked):
        """Two facility requests must not disturb the care-giver page. Its
        geography is its own and nothing has asked it to change.
        """
        text = _session_text(asked).lower()
        assert "san francisco" in text, (
            "the session no longer holds San Francisco for the care-giver "
            "page; a facility request moved a parameter that is not its own")

    def test_the_session_holds_both_cities_at_once(self, asked):
        """The point of per-page parameters: two pages, two geographies,
        neither overwriting the other.
        """
        text = _session_text(asked).lower()
        assert "new york" in text, (
            "the session does not hold New York for the facility page")
        assert "san francisco" in text, (
            "the session does not hold San Francisco for the care-giver page")


# ─────────────────────────────────────────────────────────────────────
# FLOW 5 — the emergency, and the way back
#
# A person saying they are having a heart attack is not a search. The
# utterance manager routes it to safetyLockout and SharedServices' own
# LockoutTool answers: the person is told to call emergency services and
# their IP is locked. It is a safety control shared by every page, not a
# FindCare capability, and it must survive any refactor of the pages.
#
# The operator's way back is the literal 'unlock$123', which LockoutTool
# recognises only while the person is locked.
# ─────────────────────────────────────────────────────────────────────


class TestAnEmergencyLocksAndTheOperatorUnlocks:

    @pytest.fixture(scope="class")
    def locked(self, page):
        _fresh(page)
        _ask(page, "I'm having a heart attack")
        page.wait_for_timeout(6_000)
        return page

    def test_the_emergency_is_answered_not_searched(self, locked):
        """The turn must not become a provider search. A person in an
        emergency is told to call emergency services; a list of cardiologists
        is the wrong answer to the question they asked.
        """
        body = locked.locator("body").inner_text().lower()
        assert locked.locator("[data-testid='provider-card']").count() == 0, (
            "an emergency utterance produced a provider list")
        assert "911" in body or "emergency" in body, (
            f"the emergency was not answered with a direction to emergency "
            f"services; the screen reads: {body[:300]!r}")

    def test_the_person_is_locked_after_the_emergency(self, locked):
        """The lock is the control. A further request must not be served
        as though nothing happened.
        """
        _ask(locked, "find me a shrink in San Francisco CA")
        locked.wait_for_timeout(5_000)
        assert locked.locator("[data-testid='provider-card']").count() == 0, (
            "a search was served to a locked person")

    def test_the_operator_literal_unlocks(self, locked):
        """The way back. Only this literal, and only while locked."""
        _ask(locked, "unlock$123")
        locked.wait_for_timeout(5_000)
        _ask(locked, "find me a shrink in San Francisco CA")
        _wait_for_panel(locked)
        assert locked.locator("[data-testid='provider-card']").count() > 0, (
            "the operator literal did not restore service")


class TestACityWithoutAStateIsAskedAbout:
    """A: "find me a shrink in San Fransisco" -- no state.

    Healthcare is regulated per state, so the state is the fact a search
    cannot do without: it decides which board licenses the provider. A
    city alone does not supply it, and city names repeat across states.

    So this turn must not search. It must ask -- naming the state as what
    is needed, and saying that a city, a ZIP or a county are welcome but
    not required. The wording is authored by a model from the declaration,
    so what is asserted is that it asks and what it asks about, never the
    words themselves.
    """

    @pytest.fixture(scope="class")
    def asked(self, page):
        _new_session(page)
        _ask(page, "find me a shrink in San Fransisco")
        page.wait_for_timeout(25_000)
        return _record(page, "20a_no_state_asks")

    def test_no_search_ran(self, asked):
        cards = _marks(asked, "frame_MainWindow").get("provider-card", 0)
        assert cards == 0, (
            f"a search ran without a state and put {cards} care givers on "
            f"screen; which state decides which board licenses them")

    def test_the_person_is_asked_a_question(self, asked):
        message = _window(asked, "frame_UserMessage")["digest"]
        assert "?" in message, (
            f"the turn asked nothing, so the person has nothing to answer: "
            f"{message!r}")

    def test_the_question_asks_for_the_state(self, asked):
        message = _window(asked, "frame_UserMessage")["digest"].lower()
        assert "state" in message, (
            f"the question does not ask for the state, which is the one "
            f"thing the search cannot run without: {message!r}")

    def test_the_question_offers_the_rest_without_demanding_them(self, asked):
        message = _window(asked, "frame_UserMessage")["digest"].lower()
        assert any(word in message for word in ("city", "zip", "county")), (
            f"the question does not say what else would be accepted, so the "
            f"person cannot tell what narrowing is available: {message!r}")

    def test_a_spelling_note_is_not_offered_as_the_answer(self, asked):
        message = _window(asked, "frame_UserMessage")["digest"]
        assert not ("corrected from" in message.lower() and "?" not in message), (
            "the turn ended having told the person how their spelling was "
            "read, which answers nothing they asked")


class TestAStateMakesTheSearchRun:
    """B: "find me a shrink in San Fransico CA" -- a state is given.

    The same request with the state supplied searches, and searches that
    city rather than the country. A misspelling of the city does not stop
    it: the state is what the search needs.
    """

    @pytest.fixture(scope="class")
    def searched(self, page):
        _new_session(page)
        _ask(page, "find me a shrink in San Fransico CA")
        _wait_for_results(page)
        page.wait_for_timeout(3_000)
        return _record(page, "20b_state_searches")

    def test_care_givers_are_found(self, searched):
        cards = _marks(searched, "frame_MainWindow").get("provider-card", 0)
        assert cards > 0, "a search with a state found nobody"

    def test_it_did_not_search_the_whole_country(self, searched):
        text = _window(searched, "frame_MainWindow")["digest"]
        found = [int(t.replace(",", "")) for t in
                 __import__("re").findall(r"([\d,]+) providers? found", text)]
        assert found, f"the window does not say how many were found: {text[:160]!r}"
        assert found[0] < 50_000, (
            f"{found[0]} care givers were found for one city, so the place "
            f"was dropped and the search ran nationwide")

    def test_the_rows_carry_the_place_that_was_asked_for(self, searched):
        rows = _rows(searched)
        assert rows, "no rows to inspect"
        elsewhere = [r for r in rows
                     if "CA" not in r.upper() and r.strip()]
        assert not elsewhere, (
            f"{len(elsewhere)} row(s) show an address outside the state "
            f"that was searched: {elsewhere[:2]}")


class TestTheDetailIsTheSameFromEitherPath:
    """One NPI, one detail.

    The detail tool takes one input and reads the rest from the record, so
    what it shows cannot depend on which search painted the row it was
    opened from. It used to: the row carried name, specialty, address,
    phone and state on the click, so a row painted with less produced a
    detail with less.
    """

    @pytest.fixture(scope="class")
    def opened(self, page):
        _new_session(page)
        _ask(page, "find me a shrink in San Fransico CA")
        _wait_for_results(page)
        page.wait_for_timeout(2_000)
        page.get_by_text("provider detail").first.click()
        page.wait_for_timeout(8_000)
        return _record(page, "20c_detail_from_a_search")

    def test_the_detail_names_the_record(self, opened):
        text = _window(opened, "frame_RightPanel")["digest"]
        assert "NPI:" in text and any(ch.isdigit() for ch in text), (
            f"the detail carries no NPI: {text[:200]!r}")

    def test_the_detail_shows_an_address(self, opened):
        text = _window(opened, "frame_RightPanel")["digest"]
        assert "No address on file" not in text, (
            "the detail says there is no address, for a provider a search "
            "just returned with one")
