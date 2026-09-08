// Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
// Licensed under the FindCare Evaluation License (FEL-1.0).
//
// SelectedProvidersWidget — fills MainWindow#selected_strip via
// ClientRouter.merge. The scaffold (with the selected_strip region)
// is painted by ProviderResultsWidget on kind:'providers'; this widget
// just paints into the named region.
//
// Server-side state lives on user_object.selected_providers; this widget
// is a pure view + drag-drop sink. On kind:'providers' it asks the
// ProviderSelectionTool to list current selection (server broadcasts
// kind:'selection_changed' which this widget catches).

import { useEffect } from 'react'

const TARGET = 'MainWindow'
const REGION = 'selected_strip'

function _esc(s: any): string {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;')
}

function buildStripHtml(selected: string[], max: number, lookup: Record<string, any>,
                        evaluateEnabled: boolean,
                        excluded: Record<string, boolean>): string {
  const cards = selected.map(npi => {
    const p = lookup[npi] || { npi }
    const name = _esc(p.name || npi)
    const spec = _esc(p.specialty || p.primary_specialty || '')
    // The specialty filter in force does not admit this provider. The row
    // is kept and marked, never removed: the person chose it, and a filter
    // that silently drops their own choice is what this state prevents.
    const isExcluded = excluded[npi] === true
    return (
      `<div data-testid="selected-card"${isExcluded ? ' data-excluded="true"' : ''} style="display:flex;align-items:center;gap:0.5em;padding:0.5em 1em;border-bottom:0.0625em solid #f0f0f0;${isExcluded ? 'background:#f9fafb;opacity:0.65;' : ''}">` +
        `<div style="flex:1;min-width:0;">` +
          `<div style="font-weight:600;color:${isExcluded ? '#6b7280' : '#0b7a75'};">${name}</div>` +
          (spec ? `<div style="font-size:0.85em;color:#6b7280;">${spec}</div>` : '') +
          (isExcluded ? `<div data-testid="excluded-by-filter" style="font-size:0.85em;color:#b45309;">Excluded by the specialty filter in force</div>` : '') +
          `<div style="font-size:0.85em;color:#9ca3af;">NPI: ${_esc(npi)}</div>` +
        `</div>` +
        `<a href="#" data-router-action="provider:deselect" data-npi="${_esc(npi)}" title="Remove from selection" style="color:#dc2626;text-decoration:none;font-size:1.25em;font-weight:700;padding:0.25em 0.5em;">✕</a>` +
      `</div>`
    )
  }).join('')
  const emptyMsg = selected.length === 0
    ? `<div style="padding:1.5em 1em;text-align:center;color:#9ca3af;font-style:italic;">Click ↓ Select or drag providers here for evaluation (max ${max})</div>`
    : ''
  // Evaluate button stays visible always so its target is obvious; disabled
  // until at least one provider is selected.
  // Whether the evaluation may be asked for is the selection tool's
  // answer. This strip shows the state; it does not decide it.
  const evalDisabled = !evaluateEnabled
  const evalBtn =
    `<button data-router-action="evaluate:selected" ${evalDisabled ? 'disabled' : ''} ` +
    `style="background:${evalDisabled ? '#d1d5db' : '#d97706'};color:#fff;border:none;border-radius:0.375em;` +
    `padding:0.4em 0.9em;font-weight:600;cursor:${evalDisabled ? 'not-allowed' : 'pointer'};font-size:0.9em;">` +
    `Evaluate These Providers →</button>`
  return (
    `<div data-testid="selected-providers" data-router-drop-action="provider:drop" style="display:flex;flex-direction:column;background:#fffdf7;">` +
      `<div style="display:flex;align-items:center;justify-content:space-between;gap:1em;padding:0.5em 1em;background:#fffbeb;border-top:0.125em solid #d8e2e1;border-bottom:0.0625em solid #d8e2e1;">` +
        `<span style="font-weight:600;color:#d97706;text-transform:uppercase;font-size:0.9em;">Selected for Evaluation</span>` +
        `<span style="color:#6b7280;font-size:0.9em;flex:1;">${selected.length} / ${max}</span>` +
        evalBtn +
      `</div>` +
      `<div style="min-height:6em;max-height:12em;overflow-y:auto;">${cards}${emptyMsg}</div>` +
    `</div>`
  )
}

export default function SelectedProvidersWidget() {
  useEffect(() => {
    window.parent.postMessage({ type: 'router:subscribe-broadcast', kind: 'providers' }, '*')
    window.parent.postMessage({ type: 'router:subscribe-broadcast', kind: 'selection_changed' }, '*')
    // user:submit actions reach widgets directly via the parent's postMessage
    // fanout (ClientRouter._bindActions), no subscribe needed.

    // Provider lookup: npi → card payload. Filled from the most recent
    // kind:'providers' broadcast so the selected strip can show name +
    // specialty without an extra round trip. Empty initially; cards for
    // NPIs not in the lookup fall back to "NPI: <npi>" only.
    let providerLookup: Record<string, any> = {}
    let maxSelected = 5
    let selectedCache: string[] = []
    let excludedByFilter: Record<string, boolean> = {}
    let evaluateEnabled = false
    let lastQuery = ''

    function postMerge(content: string) {
      window.parent.postMessage({
        type: 'router:merge',
        target: TARGET,
        region: REGION,
        content,
      }, '*')
    }

    function makeCall(op: string, payload: any) {
      window.parent.postMessage({
        type: 'router:makeCall',
        op,
        payload,
        call_id: 'sel-' + op + '-' + payload.verb,
      }, '*')
    }

    function onMessage(ev: MessageEvent) {
      const msg = ev.data
      if (!msg || typeof msg !== 'object') return

      // Capture the latest typed query so the EvaluateCare right-panel
      // caption can echo it ("2 providers · <query>").
      if (msg.type === 'router:action' && msg.action === 'user:submit') {
        const text = String(msg.data?.text || '').trim()
        if (text) lastQuery = text
        return
      }

      if (msg.type === 'router:event-broadcast' && msg.kind === 'providers') {
        const providers = Array.isArray(msg.data?.providers) ? msg.data.providers : []
        for (const p of providers) {
          if (p && p.npi) providerLookup[p.npi] = p
        }
        makeCall('provider_selection', { verb: 'list' })
        return
      }

      if (msg.type === 'router:event-broadcast' && msg.kind === 'selection_changed') {
        const selected: string[] = Array.isArray(msg.data?.selected) ? msg.data.selected : []
        if (typeof msg.data?.max_selected === 'number') maxSelected = msg.data.max_selected
        selectedCache = selected
        evaluateEnabled = msg.data?.evaluate_enabled === true
        // Computed on the server, where the selected provider's full
        // taxonomy list is held. The widget renders the flag; it does not
        // decide it.
        excludedByFilter = (msg.data?.excluded_by_filter && typeof msg.data.excluded_by_filter === 'object')
          ? msg.data.excluded_by_filter : {}
        postMerge(buildStripHtml(selected, maxSelected, providerLookup,
                                 evaluateEnabled, excludedByFilter))
        return
      }

      if (msg.type === 'router:action' && msg.action === 'provider:drop') {
        const npi = String(msg.data?.dropped || '').trim()
        if (npi) makeCall('provider_selection', { verb: 'select', npi })
        return
      }

      if (msg.type === 'router:action' && msg.action === 'provider:select-click') {
        const npi = String(msg.data?.npi || '').trim()
        if (npi) makeCall('provider_selection', { verb: 'select', npi })
        return
      }

      if (msg.type === 'router:action' && msg.action === 'provider:deselect') {
        const npi = String(msg.data?.npi || '').trim()
        if (npi) makeCall('provider_selection', { verb: 'deselect', npi })
        return
      }

      // Evaluate These Providers → hand off to EvaluateCare. The selected
      // NPIs live on user_object.selected_providers server-side; for the
      // right-panel detail we paint client-side from providerLookup +
      // selectedCache so the user sees their picks immediately without an
      // extra round trip. MainWindow paints the unimplemented placeholder.
      if (msg.type === 'router:action' && msg.action === 'evaluate:selected') {
        const cards = selectedCache.map((npi, i) => {
          const p = providerLookup[npi] || { npi }
          const name = _esc(p.name || npi)
          const addr = _esc([p.address, p.city, p.state, p.zip].filter(Boolean).join(', '))
          const county = _esc(p.county || '')
          const phone = _esc(p.phone || '')
          return (
            `<div style="margin-bottom:1em;padding-bottom:0.75em;border-bottom:0.0625em solid #f0f0f0;">` +
              `<div style="font-weight:600;color:#0b7a75;">${i + 1}. ${name}</div>` +
              (addr ? `<div style="font-size:0.9em;color:#374151;">${addr}</div>` : '') +
              (county ? `<div style="font-size:0.9em;color:#6b7280;">${county}</div>` : '') +
              (phone ? `<div style="font-size:0.9em;color:#6b7280;">Phone: ${phone}</div>` : '') +
              `<div style="font-size:0.85em;color:#9ca3af;">NPI: ${_esc(npi)}</div>` +
            `</div>`
          )
        }).join('')
        const rightContent = (
          `<div style="display:flex;flex-direction:column;height:100%;">` +
            `<div style="padding:0.5em 1em;background:#fffbeb;border-bottom:0.125em solid #d8e2e1;">` +
              `<div style="font-weight:700;color:#d97706;text-transform:uppercase;letter-spacing:0.05em;">EvaluateCare</div>` +
              `<div style="color:#6b7280;font-size:0.9em;">${selectedCache.length} provider${selectedCache.length === 1 ? '' : 's'}${lastQuery ? ' &middot; ' + _esc(lastQuery) : ''}</div>` +
            `</div>` +
            `<div style="flex:1;overflow:auto;padding:0.75em 1em;">${cards}</div>` +
          `</div>`
        )
        const mainContent = (
          `<div style="display:flex;flex-direction:column;height:100%;">` +
            `<div style="padding:0.5em 1em;background:#f0fffe;border-bottom:0.125em solid #d8e2e1;color:#0b7a75;font-weight:600;">EvaluateCare</div>` +
            `<div style="flex:1;display:flex;align-items:center;justify-content:center;color:#374151;font-weight:600;">EvaluateCare is still unimplemented.</div>` +
          `</div>`
        )
        window.parent.postMessage({
          type: 'router:render', target: 'MainWindow', append: false, popup: false,
          content: mainContent,
        }, '*')
        window.parent.postMessage({
          type: 'router:render', target: 'RightPanel', append: false, popup: false,
          content: rightContent,
        }, '*')
        return
      }
    }

    window.addEventListener('message', onMessage)
    return () => window.removeEventListener('message', onMessage)
  }, [])
  return null
}
