// Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
// Licensed under the FindCare Evaluation License (FEL-1.0).
//
// SelectedClinicalTrialsWidget — mirror of SelectedProvidersWidget for
// clinical trials. Fills MainWindow#selected_trials_strip via
// ClientRouter.merge. The scaffold (with the selected_trials_strip region)
// is painted by ClinicalTrialsWidget as part of every trial-detail render.
//
// Server-side state lives on user_object.selected_clinical_trials. This
// widget is a pure view + drag-drop sink. On kind:'clinical_trials_chunk'
// it snapshots a per-NCT lookup so the strip can show the trial name +
// sponsor without an extra round trip; on kind:'trial_selection_changed'
// it repaints the strip.

import { useEffect } from 'react'

const TARGET = 'MainWindow'
const REGION = 'selected_trials_strip'
const MAX_TRIAL_NAME_CHARS = 60

function _esc(s: any): string {
  // Rule-008 statement 4 forbids regex in front-end .tsx executable code,
  // so escape uses split/join.
  return String(s == null ? '' : s)
    .split('&').join('&amp;')
    .split('<').join('&lt;')
    .split('>').join('&gt;')
    .split('"').join('&quot;')
    .split("'").join('&#39;')
}

function _truncate(s: string, n: number): string {
  const t = String(s || '')
  if (t.length <= n) return t
  return t.slice(0, n) + '…'
}

function buildStripHtml(selected: string[], max: number, lookup: Record<string, any>,
                        evaluateEnabled: boolean): string {
  const cards = selected.map(nct => {
    const t = lookup[nct] || { nct_id: nct }
    // Row 1: name (truncated 60). Row 2: NCT ID. Row 3: sponsor.
    const name = _esc(_truncate(t.brief_title || t.official_title || nct, MAX_TRIAL_NAME_CHARS))
    const sponsor = _esc(t.lead_sponsor_name || '')
    return (
      `<div data-testid="selected-trial-card" style="display:flex;align-items:center;gap:0.5em;padding:0.5em 1em;border-bottom:0.0625em solid #f0f0f0;">` +
        `<div style="flex:1;min-width:0;">` +
          `<div style="font-weight:600;color:#0b7a75;">${name}</div>` +
          `<div style="font-size:0.85em;color:#6b7280;">NCT: ${_esc(nct)}</div>` +
          (sponsor ? `<div style="font-size:0.85em;color:#9ca3af;">${sponsor}</div>` : '') +
        `</div>` +
        `<a href="#" data-router-action="trial:deselect" data-nct-id="${_esc(nct)}" title="Remove from selection" ` +
        `style="color:#dc2626;text-decoration:none;font-size:1.25em;font-weight:700;padding:0.25em 0.5em;">✕</a>` +
      `</div>`
    )
  }).join('')
  const emptyMsg = selected.length === 0
    ? `<div style="padding:1.5em 1em;text-align:center;color:#9ca3af;font-style:italic;">Click ↓ Choose for evaluation or drag trials here (max ${max})</div>`
    : ''
  // Whether the evaluation may be asked for is the selection tool's
  // answer. This strip shows the state; it does not decide it.
  const evalDisabled = !evaluateEnabled
  const evalBtn =
    `<button data-router-action="evaluate:selected-trials" ${evalDisabled ? 'disabled' : ''} ` +
    `style="background:${evalDisabled ? '#d1d5db' : '#d97706'};color:#fff;border:none;border-radius:0.375em;` +
    `padding:0.4em 0.9em;font-weight:600;cursor:${evalDisabled ? 'not-allowed' : 'pointer'};font-size:0.9em;">` +
    `Evaluate These Trials →</button>`
  return (
    `<div data-testid="selected-clinical-trials" data-router-drop-action="trial:drop" ` +
    `style="display:flex;flex-direction:column;background:#fffdf7;">` +
      `<div style="display:flex;align-items:center;justify-content:space-between;gap:1em;padding:0.5em 1em;background:#fffbeb;border-top:0.125em solid #d8e2e1;border-bottom:0.0625em solid #d8e2e1;">` +
        `<span style="font-weight:600;color:#d97706;text-transform:uppercase;font-size:0.9em;">Selected for Evaluation</span>` +
        `<span style="color:#6b7280;font-size:0.9em;flex:1;">${selected.length} / ${max}</span>` +
        evalBtn +
      `</div>` +
      `<div style="min-height:6em;max-height:12em;overflow-y:auto;">${cards}${emptyMsg}</div>` +
    `</div>`
  )
}

export default function SelectedClinicalTrialsWidget() {
  useEffect(() => {
    window.parent.postMessage({ type: 'router:subscribe-broadcast', kind: 'clinical_trials_chunk' }, '*')
    window.parent.postMessage({ type: 'router:subscribe-broadcast', kind: 'trial_selection_changed' }, '*')

    // Trial lookup: nct_id → trial payload. Filled from clinical_trials_chunk
    // broadcasts so the selected strip can display name + sponsor without a
    // round trip. Cards for NCTs not in the lookup fall back to the NCT id
    // alone.
    const trialLookup: Record<string, any> = {}
    let maxSelected = 5
    let evaluateEnabled = false
    let selectedCache: string[] = []
    let lastQuery = ''

    function postMerge(content: string) {
      window.parent.postMessage({
        type: 'router:merge', target: TARGET, region: REGION, content,
      }, '*')
    }
    function makeCall(op: string, payload: any) {
      window.parent.postMessage({
        type: 'router:makeCall', op, payload,
        call_id: 'trial-sel-' + op + '-' + (payload.verb || 'x'),
      }, '*')
    }

    function onMessage(ev: MessageEvent) {
      const msg = ev.data
      if (!msg || typeof msg !== 'object') return

      if (msg.type === 'router:event-broadcast' && msg.kind === 'clinical_trials_chunk') {
        const data = msg.data || {}
        const trials = Array.isArray(data.trials) ? data.trials : []
        for (const t of trials) {
          if (t && t.nct_id) trialLookup[t.nct_id] = t
        }
        if (data.starts_new_set === true) {
          // Paint the strip immediately with the last-known selection so the
          // "Selected for Evaluation" surface is present on the initial trial
          // detail screen (uniform with the provider page). Then ask the
          // server for authoritative state — trial_selection_changed will
          // overwrite this paint with the fresh answer.
          postMerge(buildStripHtml(selectedCache, maxSelected, trialLookup, evaluateEnabled))
          makeCall('clinical_trial_selection', { verb: 'list' })
        }
        return
      }

      if (msg.type === 'router:event-broadcast' && msg.kind === 'trial_selection_changed') {
        const selected: string[] = Array.isArray(msg.data?.selected) ? msg.data.selected : []
        if (typeof msg.data?.max_selected === 'number') maxSelected = msg.data.max_selected
        selectedCache = selected
        evaluateEnabled = msg.data?.evaluate_enabled === true
        postMerge(buildStripHtml(selected, maxSelected, trialLookup, evaluateEnabled))
        return
      }

      if (msg.type === 'router:action' && msg.action === 'trial:drop') {
        const nct = String(msg.data?.dropped || '').trim()
        if (nct) makeCall('clinical_trial_selection', { verb: 'select', nct_id: nct })
        return
      }

      if (msg.type === 'router:action' && msg.action === 'trial:select-click') {
        const nct = String(msg.data?.nct_id || '').trim()
        if (nct) makeCall('clinical_trial_selection', { verb: 'select', nct_id: nct })
        return
      }

      if (msg.type === 'router:action' && msg.action === 'trial:deselect') {
        const nct = String(msg.data?.nct_id || '').trim()
        if (nct) makeCall('clinical_trial_selection', { verb: 'deselect', nct_id: nct })
        return
      }

      // Capture the classified prompt so the EvaluateCare right-panel
      // caption can echo it ("N trials · <criteria>").
      if (msg.type === 'router:action' && msg.action === 'user:submit') {
        const text = String(msg.data?.text || '').trim()
        if (text) lastQuery = text
        return
      }

      // Evaluate These Trials — mirror of Evaluate These Providers. Take
      // over the right frame with an EvaluateCare panel that lists the
      // selected trials (name / NCT ID / sponsor — same three rows as
      // the strip); paint MainWindow with the placeholder.
      if (msg.type === 'router:action' && msg.action === 'evaluate:selected-trials') {
        const cards = selectedCache.map((nct, i) => {
          const t = trialLookup[nct] || { nct_id: nct }
          const name = _esc(_truncate(t.brief_title || t.official_title || nct, MAX_TRIAL_NAME_CHARS))
          const sponsor = _esc(t.lead_sponsor_name || '')
          return (
            `<div style="margin-bottom:1em;padding-bottom:0.75em;border-bottom:0.0625em solid #f0f0f0;">` +
              `<div style="font-weight:600;color:#0b7a75;">${i + 1}. ${name}</div>` +
              `<div style="font-size:0.9em;color:#374151;">NCT: ${_esc(nct)}</div>` +
              (sponsor ? `<div style="font-size:0.9em;color:#6b7280;">${sponsor}</div>` : '') +
            `</div>`
          )
        }).join('')
        const rightContent = (
          `<div style="display:flex;flex-direction:column;height:100%;">` +
            `<div style="padding:0.5em 1em;background:#fffbeb;border-bottom:0.125em solid #d8e2e1;">` +
              `<div style="font-weight:700;color:#d97706;text-transform:uppercase;letter-spacing:0.05em;">EvaluateCare</div>` +
              `<div style="color:#6b7280;font-size:0.9em;">${selectedCache.length} trial${selectedCache.length === 1 ? '' : 's'}${lastQuery ? ' &middot; ' + _esc(lastQuery) : ''}</div>` +
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
