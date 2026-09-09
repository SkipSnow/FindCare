// Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
// Licensed under the FindCare Evaluation License (FEL-1.0).
//
// SystemMessageWidget — subscribes to kind:'prompt' broadcasts from the
// SS gateway and paints into frame_UserMessage. Spelling corrections
// shipped on the broadcast as data.corrections (array of {original,
// corrected}) are wrapped in red — logic ported verbatim from the prior
// Website/CorrectionsRenderer.js.

import { useEffect } from 'react'

const TARGET = 'UserMessage'

interface Correction {
  original?: string
  corrected?: string
}

function _esc(s: any): string {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;')
}

// Wrap any corrected fragments of `text` in <span style="color:#dc2626"> per
// prod's CorrectionsRenderer (Website/CorrectionsRenderer.js pre-port).
function renderCorrections(text: string, corrections: Correction[]): string {
  if (!corrections || !corrections.length) return _esc(text || '')
  if (text == null) return ''
  let remaining = String(text)
  let html = ''
  for (const c of corrections) {
    const original = c.original || ''
    if (!original) continue
    const marker = `(corrected from '${original}')`
    const markerIdx = remaining.indexOf(marker)
    if (markerIdx >= 0) {
      if (markerIdx > 0) html += _esc(remaining.substring(0, markerIdx))
      html += `<span style="color:#dc2626;">${_esc(marker)}</span>`
      remaining = remaining.substring(markerIdx + marker.length)
      continue
    }
    const lower = remaining.toLowerCase()
    const idx = lower.indexOf(original.toLowerCase())
    if (idx < 0) continue
    const matched = remaining.substring(idx, idx + original.length)
    if (idx > 0) html += _esc(remaining.substring(0, idx))
    html += _esc(c.corrected || '')
    html += `<span style="color:#dc2626;"> (corrected from '${_esc(matched)}')</span>`
    remaining = remaining.substring(idx + original.length)
  }
  if (remaining) html += _esc(remaining)
  return html
}

function buildPromptHtml(text: string, corrections: Correction[]): string {
  if (!text) return ''
  return `
    <div style="padding:0.5em 1em;background:#f0fffe;border-left:0.25em solid #0b7a75;color:#374151;">
      ${renderCorrections(text, corrections)}
    </div>
  `
}

export default function SystemMessageWidget() {
  useEffect(() => {
    window.parent.postMessage({ type: 'router:subscribe-broadcast', kind: 'prompt' }, '*')

    function onMessage(ev: MessageEvent) {
      const msg = ev.data
      if (!msg || typeof msg !== 'object') return
      if (msg.type !== 'router:event-broadcast' || msg.kind !== 'prompt') return
      const data = msg.data || {}
      const text = String(data.text || '').trim()
      const corrections = Array.isArray(data.corrections) ? data.corrections as Correction[] : []
      window.parent.postMessage({
        type: 'router:render',
        target: TARGET,
        append: false,
        popup: false,
        content: buildPromptHtml(text, corrections),
      }, '*')
    }
    window.addEventListener('message', onMessage)
    return () => window.removeEventListener('message', onMessage)
  }, [])
  return null
}
