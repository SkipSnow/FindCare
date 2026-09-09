// Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
// Licensed under the FindCare Evaluation License (FEL-1.0).
//
// FacilityResultsWidget — paints the facility list into MainWindow.
//
// The sibling of ProviderResultsWidget and deliberately the same shape:
// it claims MainWindow on kind:'intent_classified' when the turn is a
// facility search, shows the searching indicator while the turn runs, and
// repaints on kind:'facilities'.
//
// The row shows exactly the five things EPIC-006-F-006-S-002-REQ-B-007
// names and nothing else. The server projects exactly those five, so this
// renders the fields it is given rather than selecting among them: a
// renderer that chose would be a second place the rule lived.

import { useEffect } from 'react'

const TARGET = 'MainWindow'

function _esc(s: any): string {
  return String(s == null ? '' : s)
    .split('&').join('&amp;')
    .split('<').join('&lt;')
    .split('>').join('&gt;')
    .split('"').join('&quot;')
}

function buildSearchingHtml(query: string, seconds: number): string {
  return `
    <div style="flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:12px;padding:1em;height:100%;">
      <div style="font-size:1em;color:#6b7280;">Searching for: <strong style="color:#374151;font-weight:600;">${_esc(query)}</strong></div>
      <div data-testid="facility-searching-timer" style="font-size:1em;color:#0b7a75;font-weight:700;">${seconds}s</div>
      <div style="font-size:1em;color:#9ca3af;">Waiting for response...</div>
    </div>
  `
}

function buildResultsHtml(facilities: any[], totalCount?: number,
                          header?: string, hasMore?: boolean,
                          hasPrevious?: boolean): string {
  if (!facilities.length) {
    return `<div data-testid="facility-empty" style="padding:2em;color:#6b7280;font-style:italic;">No facilities matched.</div>`
  }
  const count = totalCount || facilities.length
  // The heading agrees with its number, so a single result reads as one.
  // The summary the server built is prose beside it, never instead of it.
  const heading = `${count} ${count === 1 ? 'facility' : 'facilities'} found`
  const summary = header
    ? `<div data-testid="facility-summary" style="padding:0.4em 1em;background:#fffdf7;border-bottom:0.0625em solid #eee;color:#374151;font-size:0.9em;flex-shrink:0;">${_esc(header)}</div>`
    : ''

  const rows = facilities.map(f => {
    const npi = _esc(f.npi || '')
    const addressCount = Number(f.practice_address_count || 0)
    return (
      `<div data-testid="facility-card" data-npi="${npi}" style="padding:0.5em 1em;border-bottom:0.0625em solid #eee;">` +
        `<div data-testid="facility-name" style="font-weight:600;color:#0b7a75;">${_esc(f.facility || '')}</div>` +
        `<div data-testid="facility-address" style="font-size:0.9em;color:#374151;">${_esc(f.primary_practice_address || '')}</div>` +
        `<div data-testid="facility-address-count" style="font-size:0.85em;color:#6b7280;">` +
          `${addressCount} practice ${addressCount === 1 ? 'address' : 'addresses'}</div>` +
        `<div data-testid="facility-type" style="font-size:0.85em;color:#0b7a75;">${_esc(f.facility_type || '')}</div>` +
        `<div style="font-size:0.66em;margin-top:0.25em;">` +
          `<a href="#" data-router-action="facility:detail" data-npi="${npi}" ` +
          `data-testid="facility-detail-link" style="color:#0b7a75;text-decoration:underline;">facility detail</a>` +
        `</div>` +
      `</div>`
    )
  }).join('')

  return (
    `<div style="display:flex;flex-direction:column;height:100%;min-height:0;">` +
      `<div data-testid="facility-heading" style="padding:0.5em 1em;background:#f0fffe;border-bottom:0.125em solid #d8e2e1;color:#0b7a75;font-weight:600;flex-shrink:0;">${_esc(heading)}</div>` +
      `<div data-testid="available-facilities" style="flex:1;overflow:auto;">${rows}</div>` +
      (hasMore || hasPrevious
        ? `<div style="padding:0.6em 1em;border-top:0.125em solid #eee;text-align:center;flex-shrink:0;display:flex;gap:0.75em;justify-content:center;">` +
            (hasPrevious
              ? `<button data-router-action="facilities:previous-page" data-testid="facilities-previous-page" ` +
                `style="background:#fff;border:0.125em solid #0b7a75;color:#0b7a75;padding:0.4em 1.2em;` +
                `border-radius:0.375em;cursor:pointer;font-weight:700;">&larr; Previous facilities</button>`
              : '') +
            (hasMore
              ? `<button data-router-action="facilities:next-page" data-testid="facilities-next-page" ` +
                `style="background:#fff;border:0.125em solid #0b7a75;color:#0b7a75;padding:0.4em 1.2em;` +
                `border-radius:0.375em;cursor:pointer;font-weight:700;">Show more facilities &rarr;</button>`
              : '') +
          `</div>`
        : '') +
    `</div>`
  )
}

export default function FacilityResultsWidget() {
  let firstNpiRef = ''
  let lastNpiRef = ''
  useEffect(() => {
    window.parent.postMessage({ type: 'router:subscribe-broadcast', kind: 'intent_classified' }, '*')
    window.parent.postMessage({ type: 'router:subscribe-broadcast', kind: 'facilities' }, '*')

    let currentQuery = ''
    let startTs = 0
    let timer: ReturnType<typeof setInterval> | null = null

    function postRender(content: string) {
      window.parent.postMessage({
        type: 'router:render', target: TARGET, append: false, popup: false, content,
      }, '*')
    }
    function stopTimer() { if (timer) { clearInterval(timer); timer = null } }
    function startTimer(query: string) {
      stopTimer()
      currentQuery = query
      startTs = Date.now()
      postRender(buildSearchingHtml(query, 0))
      timer = setInterval(function () {
        postRender(buildSearchingHtml(currentQuery, Math.round((Date.now() - startTs) / 1000)))
      }, 1000)
    }

    function onMessage(ev: MessageEvent) {
      const msg = ev.data
      if (!msg || typeof msg !== 'object') return

      // The turn is ours only when the server says it classified a
      // facility search. Any other action means another widget owns
      // MainWindow this turn and this one stands down.
      if (msg.type === 'router:event-broadcast' && msg.kind === 'intent_classified') {
        const action = String((msg.data || {}).action || '')
        if (action === 'findAFacility') {
          startTimer(String((msg.data || {}).criteria || 'facilities'))
        } else {
          stopTimer()
        }
        return
      }
      if (msg.type === 'router:event-broadcast' && msg.kind === 'facilities') {
        stopTimer()
        const data = msg.data || {}
        const facilities = Array.isArray(data.facilities) ? data.facilities : []
        firstNpiRef = String(data.first_npi || '')
        lastNpiRef = String(data.last_npi || '')
        postRender(buildResultsHtml(facilities, data.total_count,
                                    data.summary_message,
                                    Boolean(data.has_more),
                                    Boolean(data.has_previous)))
        return
      }
      if (msg.type === 'router:action' && msg.action === 'facilities:next-page') {
        if (!lastNpiRef) return
        window.parent.postMessage({
          type: 'router:makeCall', op: 'facility_page',
          payload: { cursor: lastNpiRef, direction: 'forward' },
          call_id: 'facility-page-' + Date.now(),
        }, '*')
        return
      }
      if (msg.type === 'router:action' && msg.action === 'facilities:previous-page') {
        if (!firstNpiRef) return
        window.parent.postMessage({
          type: 'router:makeCall', op: 'facility_page',
          payload: { cursor: firstNpiRef, direction: 'back' },
          call_id: 'facility-page-' + Date.now(),
        }, '*')
        return
      }
      if (msg.type === 'router:final') stopTimer()
    }

    window.addEventListener('message', onMessage)
    return () => { stopTimer(); window.removeEventListener('message', onMessage) }
  }, [])
  return null
}
