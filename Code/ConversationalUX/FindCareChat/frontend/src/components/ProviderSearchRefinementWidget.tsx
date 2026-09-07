// Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
// Licensed under the FindCare Evaluation License (FEL-1.0).
//
// ProviderSearchRefinementWidget — the display half of ProviderSearchTool's
// refinements. That frame is where the system speaks to the person about
// their query: SystemMessageWidget writes the correction line there on
// kind:'prompt', replacing the frame, and this widget appends beneath it
// on kind:'providers'. Appending is what keeps the two independent -- no
// other widget is touched and no scaffold is shared.
//
// This is where the system tells the person how to narrow what they are
// looking at. Every choice carries its count over the current result,
// because a preference costs 90% of the list for orthopaedic surgery and
// 10% for nurse practitioners, and nobody should discover which after
// choosing. The counts are computed server-side and arrive on
// kind:'providers' as `refinements`.

import { useEffect } from 'react'
import { openPopup } from './popupFrame'

const TARGET = 'UserMessage'
const POPUP = 'NarrowPopUp'
const CONTAINER = 'provider_search_refinements'
const TEAL = '#0b7a75'
const TEAL_LIGHT_BG = '#e6f5ec'

const DIMENSION_TITLES: Record<string, string> = {
  provider_sex: 'Provider',
  insurance: 'Insurance',
  sole_proprietor: 'Practises alone',
}

function _esc(s: any): string {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;')
}

// Payer tokens arrive as BLUE_CROSS_BLUE_SHIELD -- the normalised form the
// query needs, not something to put in front of a person. The value stays
// as it is; only the label is made readable.
function _readable(token: string): string {
  return token.split('_')
    .map(w => w ? w.charAt(0) + w.slice(1).toLowerCase() : w)
    .join(' ')
}

function buildChipHtml(dim: string, row: Record<string, any>): string {
  const raw = String(row.label != null ? row.label : row.value)
  const label = _esc(row.label != null ? raw : _readable(raw))
  // A choice in force stays on the panel, filled rather than outlined, and
  // clicking it clears it. Hiding it left no way to undo a filter.
  const on = Boolean(row.in_force)
  return (
    `<span data-router-action="provider_search:refine" data-testid="provider-search-refine-chip"` +
    ` data-dim="${_esc(dim)}" data-value="${_esc(String(row.value))}"` +
    ` data-in-force="${on ? '1' : '0'}"` +
    ` title="${on ? 'Click to remove this filter' : 'Click to narrow'}"` +
    ` style="display:inline-block;margin:0.15em 0.3em 0.15em 0;` +
    `padding:0.2em 0.5em;border:0.0625em solid ${TEAL};border-radius:0.9em;` +
    `cursor:pointer;font-size:0.8em;white-space:nowrap;` +
    (on ? `background:${TEAL};color:#fff;font-weight:700;` : `color:${TEAL};`) +
    `">${on ? '&times; ' : ''}${label} <b>${Number(row.count) || 0}</b></span>`
  )
}

// The filter, exactly once. The web display of it is the one that is
// right, so it is built here and used unchanged in both places: shown
// inline on a computer, shown inside the popup on a phone. It was built
// twice -- once for the page and once for the window -- and two builders
// for one thing is two things that drift apart.
function buildFilterHtml(
  refinements: Record<string, Array<Record<string, any>>>,
  total: number,
): string {
  const dims = Object.keys(refinements).filter(k => (refinements[k] || []).length)
  if (!dims.length) return ''
  const blocks = dims.map(dim => {
    const chips = (refinements[dim] || []).map(r => buildChipHtml(dim, r)).join('')
    return (
      `<div style="margin-top:0.35em;">` +
      `<div style="font-size:0.75em;color:#6b7280;text-transform:uppercase;` +
      `letter-spacing:0.03em;">${_esc(DIMENSION_TITLES[dim] || dim)}</div>` +
      `<div>${chips}</div></div>`
    )
  }).join('')
  return (
    `<div data-testid="provider-search-refinements"` +
    ` style="padding:0.5em 0.8em;box-sizing:border-box;">` +
    `<div style="font-size:0.85em;color:#1f2937;font-weight:700;">` +
    `${Number(total) || 0} found — you can narrow by:</div>${blocks}</div>`
  )
}

function buildHintsHtml(
  refinements: Record<string, Array<Record<string, any>>>,
  total: number,
  summary?: string,
): string {
  // The summary the server wrote about this result. It belongs beside the
  // results and not among them: EPIC-006-F-001-S-005-REQ-B-009 requires it
  // be shown with the results it describes, and REQ-B-010 forbids it being
  // rendered among the elements of the list. This frame is where the
  // system already speaks to the person about their query, so it is shown
  // here and the list keeps its frame whole.
  const summaryHtml = summary
    ? `<div data-testid="provider-summary" class="ch-summary"` +
      ` style="padding:0.5em 0.8em;` +
      `border-top:0.25em solid ${TEAL};background:#fffdf7;box-sizing:border-box;` +
      `font-size:0.85em;color:#374151;">${_esc(summary)}</div>`
    : ''
  const filter = buildFilterHtml(refinements, total)
  if (!filter) return summaryHtml
  // A computer shows the filter where it is; a phone shows a button that
  // opens it, and the vertical space the filter took goes back to the
  // list. The bar is as thin as a finger target allows, and carries
  // nothing else.
  return (
    summaryHtml +
    `<style>
       .ch-narrow-button { display: none; }
       .ch-narrow-inline { border-top: 0.25em solid ${TEAL};
                           background: ${TEAL_LIGHT_BG}; }
       @media (max-width: 45em) {
         .ch-narrow-inline { display: none; }
         /* The summary is prose about the result and belongs beside the
            results, not in the bar. On a phone the bar carries the button
            and nothing else. */
         .ch-summary { display: none; }
         /* The bar is chrome, not a column. Its frame sits inside the
            centre column, which is as wide as the results need, so a
            button centred in that frame fell off the right of the
            screen. Pinned to the screen it stays in view. */
         .ch-narrow-button { display: flex; position: sticky; left: 0;
                             width: 100vw; box-sizing: border-box; }
       }
     </style>` +
    `<div class="ch-narrow-inline">${filter}</div>` +
    `<div class="ch-narrow-button" style="border-top:0.25em solid ${TEAL};` +
    `background:${TEAL_LIGHT_BG};padding:0.15em 0.5em;` +
    `align-items:center;justify-content:center;">` +
    `<button type="button"` +
    ` data-router-action="narrow_open" data-testid="provider-narrow-button"` +
    // Sized against the root, so it follows the font control like every
    // other length here rather than fixing a pixel count.
    ` style="width:75%;min-height:5rem;border:0.0625em solid ${TEAL};` +
    `border-radius:0.4em;background:#fff;color:${TEAL};font-weight:700;` +
    `font-size:0.9em;text-align:center;cursor:pointer;">` +
    `Narrow ${Number(total) || 0} results</button></div>`
  )
}

export default function ProviderSearchRefinementWidget() {
  useEffect(() => {
    window.parent.postMessage(
      { type: 'router:subscribe-broadcast', kind: 'providers' }, '*')
    // The frame is replaced on each new question by whoever writes the
    // correction line, taking this widget's container with it. Watching the
    // same event is how this widget knows to lay a fresh one down, without
    // reaching into the widget that owns the frame.
    window.parent.postMessage(
      { type: 'router:subscribe-broadcast', kind: 'prompt' }, '*')

    // kind:'providers' arrives once per page of results. The first lays the
    // container down beneath the correction line; every later one replaces
    // what is inside it, so paging updates the counts instead of stacking
    // another copy under them.
    let laid = false
    // The last counts seen, so the window can be opened from the button
    // and repainted as the list narrows without asking the server again.
    let lastRefinements: Record<string, Array<Record<string, any>>> = {}
    let lastTotal = 0
    let windowOpen = false
    // The correction line is written with a whole-frame replace, which
    // happens after the counts on a turn that corrects a spelling -- so the
    // hints were being wiped by it. Holding them means they go back under
    // the corrected sentence rather than disappearing at exactly the moment
    // the system had something to say.
    let held = ''

    function paint(content: string) {
      if (!content) return
      held = content
      if (laid) {
        window.parent.postMessage({
          type: 'router:merge', target: TARGET, region: CONTAINER, content,
        }, '*')
        return
      }
      window.parent.postMessage({
        type: 'router:render', target: TARGET, append: true, popup: false,
        content: `<div id="${CONTAINER}">${content}</div>`,
      }, '*')
      laid = true
    }

    function onMessage(ev: MessageEvent) {
      const msg = ev.data
      if (!msg || typeof msg !== 'object') return

      if (msg.type === 'router:event-broadcast' && msg.kind === 'prompt') {
        // The frame was just replaced, so the container is gone. Lay the
        // held counts back under the sentence rather than leaving the
        // person with nothing where the narrowings were.
        laid = false
        if (held) {
          const back = held
          held = ''
          paint(back)
        }
        return
      }

      if (msg.type === 'router:event-broadcast' && msg.kind === 'providers') {
        const data = msg.data || {}
        lastRefinements = data.refinements || {}
        lastTotal = Number(data.total_count || 0)
        paint(buildHintsHtml(lastRefinements, lastTotal,
                             String(data.summary_message || '')))
        // A choice made in the window narrows the list and the counts
        // change with it. Repainting an open window is what makes it
        // operate on the list in real time rather than going stale the
        // moment it is used.
        if (windowOpen) openPopup(POPUP, buildFilterHtml(lastRefinements, lastTotal))
        return
      }

      // The button under the results on a phone.
      if (msg.type === 'router:action' && msg.action === 'narrow_open') {
        windowOpen = true
        openPopup(POPUP, buildFilterHtml(lastRefinements, lastTotal))
        return
      }
      if (msg.type === 'router:action' && msg.action === 'popup_close'
          && String((msg.data || {}).target || '') === POPUP) {
        windowOpen = false
        return
      }

      if (msg.type !== 'router:action') return

      // A chip. This widget does not decide what the choice means -- it
      // reports which dimension and which value, and the server writes the
      // parameter and re-runs the search.
      if (msg.action === 'provider_search:refine') {
        const d = msg.data || {}
        if (!d.dim) return
        window.parent.postMessage({
          type: 'router:makeCall',
          op: 'refine_search',
          payload: {
            dimension: String(d.dim),
            value: String(d.value == null ? '' : d.value),
          },
          call_id: 'refine-' + Date.now(),
        }, '*')
        return
      }
    }

    window.addEventListener('message', onMessage)
    return () => window.removeEventListener('message', onMessage)
  }, [])

  return null
}
