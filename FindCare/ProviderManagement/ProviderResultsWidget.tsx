// Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
// Licensed under the FindCare Evaluation License (FEL-1.0).
//
// ProviderResultsWidget — subscribes to the SS gate broadcast for
// kind:'providers' and paints the provider list into frame_MainWindow
// via router:render. Also paints a "Searching..." indicator on submit.

import { useEffect } from 'react'

const TARGET = 'MainWindow'

function _esc(s: any): string {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;')
}

// renderSearching ported verbatim from prod _oneshots/prod_index.html
// lines 1791-1801. Flex-column centered layout; bare TEAL timer (no box);
// gray status footer. No invented styling.
function buildSearchingHtml(query: string, seconds: number): string {
  return `
    <div style="flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:12px;padding:1em;height:100%;">
      <div style="font-size:1em;color:#6b7280;">Searching for: <strong style="color:#374151;font-weight:600;">${_esc(query)}</strong></div>
      <div data-testid="searching-timer" style="font-size:1em;color:#0b7a75;font-weight:700;">${seconds}s</div>
      <div style="font-size:1em;color:#9ca3af;">Waiting for response...</div>
    </div>
  `
}

function buildResultsAreaHtml(providers: any[], totalCount?: number,
                              header?: string, hasMore?: boolean,
                              hasPrevious?: boolean): string {
  if (!providers.length) {
    return `<div style="padding:2em;color:#6b7280;font-style:italic;">No providers matched.</div>`
  }
  const count = totalCount || providers.length
  // The heading names the count. The summary the server built is prose
  // ABOUT that result and is painted beside it, not in place of it: a
  // result that stops naming its own count is one a person cannot read
  // at a glance.
  const heading = `${count} provider${count === 1 ? '' : 's'} found`
  // The list is the principal content of the page and nothing drawn in
  // this frame may displace it (EPIC-006-F-001-S-005-REQ-B-009). The
  // server's summary named every resolved specialty three times over and
  // took the frame the providers belong in, so it is not drawn here. It
  // still arrives on the broadcast payload, so nothing that has it is lost.
  // renderProviderRow ported verbatim from prod _oneshots/prod_index.html
  // line 1819-1835. Action data-* attributes carry the full card payload
  // the SS provider_detail_tool requires.
  // draggable=true + data-drag-payload=npi: ClientRouter wires
  // dragstart to write the NPI to dataTransfer for the
  // SelectedProvidersWidget's drop zone.
  const rows = providers.map(p => {
    const name = _esc(p.name || '')
    const addrLine = _esc([p.address, p.city, p.state, p.zip].filter(Boolean).join(', '))
    // The specialty the user chose in the filter, resolved on the server
    // where both the provider's taxonomy list and the chosen set are held.
    // The client holds no rule about which specialty is shown.
    const spec = _esc(p.matched_specialty_label || '')
    // The link carries the record's identity and nothing about how this
    // row happens to be painted. What the detail shows is the record's to
    // say, and a detail that varied with the row it was opened from was a
    // detail describing the search rather than the provider.
    const detailAttrs =
      `data-router-action="provider:detail"` +
      ` data-npi="${_esc(p.npi || '')}"`
    return (
      `<div data-testid="provider-card" data-npi="${_esc(p.npi || '')}" draggable="true" data-drag-payload="${_esc(p.npi || '')}" style="padding:0.5em 1em;border-bottom:0.0625em solid #eee;display:flex;justify-content:space-between;align-items:center;gap:1em;cursor:grab;">` +
        `<div style="flex:1;min-width:0;">` +
          `<div style="font-weight:600;color:#0b7a75;">${name}</div>` +
          `<div style="font-size:0.9em;color:#9ca3af;">${addrLine}</div>` +
          `<div style="font-size:0.85em;color:#6b7280;">NPI: ${_esc(p.npi || '')}${p.phone ? ' &middot; Phone: ' + _esc(p.phone) : ''}${p.county ? ' &middot; County: ' + _esc(p.county) : ''}</div>` +
          // The specialty this provider was matched on, named. The row
          // carried a taxonomy code and nothing that says what it means,
          // so a list of 337 people gave no way to tell a podiatrist from
          // a chiropractor without opening each one.
          `<div style="font-size:0.85em;color:#0b7a75;">${spec}</div>` +
          `<div style="font-size:0.66em;margin-top:0.25em;"><a href="#" ${detailAttrs} style="color:#0b7a75;text-decoration:underline;">provider detail</a></div>` +
        `</div>` +
        `<button data-router-action="provider:select-click" data-npi="${_esc(p.npi || '')}" title="Select for evaluation" style="background:#fff;border:0.0625em solid #0b7a75;color:#0b7a75;padding:0.3em 0.75em;border-radius:0.375em;cursor:pointer;font-size:0.85em;white-space:nowrap;flex-shrink:0;">↓ Select</button>` +
      `</div>`
    )
  }).join('')
  return (
    `<div style="display:flex;flex-direction:column;height:100%;min-height:0;">` +
      `<div style="padding:0.5em 1em;background:#f0fffe;border-bottom:0.125em solid #d8e2e1;color:#0b7a75;font-weight:600;flex-shrink:0;">${_esc(heading)}</div>` +
      `<div style="display:flex;align-items:center;justify-content:space-between;padding:1em;background:#fafafa;border-bottom:0.125em solid #eee;flex-shrink:0;">` +
        `<span style="font-weight:600;color:#0b7a75;text-transform:uppercase;">Available Providers</span>` +
        `<span style="color:#6b7280;">${providers.length} available — drag to select</span>` +
      `</div>` +
      `<div data-testid="available-providers" style="flex:1;overflow:auto;">${rows}</div>` +
      // Forward and back, both keyset pages off the position in view.
      (hasMore || hasPrevious
        ? `<div style="padding:0.6em 1em;border-top:0.125em solid #eee;text-align:center;flex-shrink:0;display:flex;gap:0.75em;justify-content:center;">` +
            (hasPrevious
              ? `<button data-router-action="providers:previous-page" data-testid="providers-previous-page" ` +
                `style="background:#fff;border:0.125em solid #0b7a75;color:#0b7a75;padding:0.4em 1.2em;` +
                `border-radius:0.375em;cursor:pointer;font-weight:700;">&larr; Previous providers</button>`
              : '') +
            (hasMore
              ? `<button data-router-action="providers:next-page" data-testid="providers-next-page" ` +
                `style="background:#fff;border:0.125em solid #0b7a75;color:#0b7a75;padding:0.4em 1.2em;` +
                `border-radius:0.375em;cursor:pointer;font-weight:700;">Show more providers &rarr;</button>`
              : '') +
          `</div>`
        : '') +
    `</div>`
  )
}

// Scaffold the widget paints into MainWindow (replace). Two named
// regions: results_area (fills with the provider list) and selected_strip
// (stays empty for SelectedProvidersWidget to merge into).
function buildScaffoldHtml(): string {
  return (
    `<div style="display:flex;flex-direction:column;height:100%;min-height:0;">` +
      `<div id="results_area" style="flex:1;min-height:0;overflow:hidden;"></div>` +
      `<div id="selected_strip" style="flex-shrink:0;"></div>` +
    `</div>`
  )
}

export default function ProviderResultsWidget() {
  // Where this page begins and ends. Both keys are the position: back asks
  // for the rows before the first, forward for those after the last.
  let firstNpiRef = ''
  let lastNpiRef = ''
  let hasMoreRef = false
  let hasPreviousRef = false
  useEffect(() => {
    // Widget claims MainWindow only when the server classifies the turn as
    // a provider search. Ownership arrives via kind:'intent_classified'
    // with a provider action; any other action means a different widget
    // owns the surface this turn and this widget stays quiet.
    window.parent.postMessage({ type: 'router:subscribe-broadcast', kind: 'intent_classified' }, '*')
    window.parent.postMessage({ type: 'router:subscribe-broadcast', kind: 'providers' }, '*')
    // Apply Filter narrows the query already in force. It cannot announce
    // intent_classified -- that means 'new query' and blanks all three
    // frames, including the specialty panel being filtered with.
    window.parent.postMessage({ type: 'router:subscribe-broadcast', kind: 'search_running' }, '*')
    window.parent.postMessage({ type: 'router:subscribe-broadcast', kind: 'specialties' }, '*')

    let currentQuery = ''
    let startTs = 0
    let timer: ReturnType<typeof setInterval> | null = null

    function postRender(content: string) {
      window.parent.postMessage({
        type: 'router:render',
        target: TARGET,
        append: false,
        popup: false,
        content,
      }, '*')
    }

    function postMergeResults(content: string) {
      window.parent.postMessage({
        type: 'router:merge',
        target: TARGET,
        region: 'results_area',
        content,
      }, '*')
    }

    function stopTimer() {
      if (timer) { clearInterval(timer); timer = null }
    }

    function startTimer(query: string) {
      stopTimer()
      currentQuery = query
      startTs = Date.now()
      postRender(buildSearchingHtml(query, 0))
      timer = setInterval(function () {
        const sec = Math.round((Date.now() - startTs) / 1000)
        postRender(buildSearchingHtml(currentQuery, sec))
      }, 1000)
    }

    // Actions this widget claims MainWindow for. Any other classified
    // action means another widget owns the turn.
    const PROVIDER_ACTIONS = new Set(['findAProvider', 'specialtySearch'])

    function onMessage(ev: MessageEvent) {
      const msg = ev.data
      if (!msg || typeof msg !== 'object') return
      if (msg.type === 'router:event-broadcast' && msg.kind === 'intent_classified') {
        const action = String((msg.data || {}).action || '')
        if (PROVIDER_ACTIONS.has(action)) {
          // Turn is ours — start the searching indicator using the criteria
          // the server classified. Chunks arrive on kind:'providers' below.
          const criteria = String((msg.data || {}).criteria || (msg.data || {}).condition || '')
          startTimer(criteria || action)
        } else {
          // Not ours — stand down so the owning widget can paint MainWindow.
          stopTimer()
        }
        return
      }
      if (msg.type === 'router:action' && msg.action === 'providers:next-page') {
        if (!lastNpiRef) return
        window.parent.postMessage({
          type: 'router:makeCall', op: 'provider_page',
          payload: { cursor: lastNpiRef, direction: 'forward' },
          call_id: 'provider-page-' + Date.now(),
        }, '*')
        return
      }
      if (msg.type === 'router:action' && msg.action === 'providers:previous-page') {
        if (!firstNpiRef) return
        window.parent.postMessage({
          type: 'router:makeCall', op: 'provider_page',
          payload: { cursor: firstNpiRef, direction: 'back' },
          call_id: 'provider-page-' + Date.now(),
        }, '*')
        return
      }
      if (msg.type === 'router:event-broadcast' && msg.kind === 'search_running') {
        const criteria = String((msg.data || {}).criteria || '')
        startTimer(criteria || 'your selected specialties')
        return
      }
      if (msg.type === 'router:event-broadcast' && msg.kind === 'providers') {
        stopTimer()
        const data = msg.data || {}
        const providers = Array.isArray(data.providers) ? data.providers : []
        firstNpiRef = String(data.first_npi || '')
        lastNpiRef = String(data.last_npi || '')
        hasMoreRef = Boolean(data.has_more)
        // Whether a page precedes this one is the search's answer, not
        // this widget's. Deriving it from first_npi said yes on page one,
        // because page one has a first row like every other page.
        hasPreviousRef = Boolean(data.has_previous)
        postRender(buildScaffoldHtml())
        postMergeResults(buildResultsAreaHtml(providers, data.total_count,
                                              data.summary_message, hasMoreRef,
                                              hasPreviousRef))
        return
      }
      if (msg.type === 'router:final') {
        stopTimer()
      }
    }
    window.addEventListener('message', onMessage)
    return () => { stopTimer(); window.removeEventListener('message', onMessage) }
  }, [])
  return null
}
