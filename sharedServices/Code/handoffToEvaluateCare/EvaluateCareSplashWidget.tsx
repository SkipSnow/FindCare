// Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
// Licensed under the FindCare Evaluation License (FEL-1.0).
//
// EvaluateCareSplashWidget — on router:action 'goto_evaluatecare' fires
// router:makeCall(op:'evalcare-splash'). Subscribed to kind:'evalcare-splash'
// broadcast; paints into frame_MainWindow.

import { useEffect } from 'react'

const TARGET = 'MainWindow'

function _esc(s: any): string {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;')
}

function buildLoadingHtml(): string {
  return `
    <div style="display:flex;align-items:center;justify-content:center;height:100%;padding:2em;color:#0b7a75;">
      <div>Loading EvaluateCare…</div>
    </div>
  `
}

function buildEvalcareSplashHtml(data: any): string {
  // Tool returns STRUCTURED DATA ONLY: { data: { title, subtitle } }.
  // React builds the display from those fields.
  const inner = (data && data.data) || data || {}
  const title = _esc(inner.title || 'EvaluateCare')
  const subtitle = _esc(inner.subtitle || '')
  return `
    <div style="display:flex;flex-direction:column;align-items:center;justify-content:center;height:100%;padding:2em;text-align:center;">
      <div style="font-size:1em;font-weight:700;color:#1f2937;">${title}</div>
      ${subtitle ? `<div style="font-size:1em;font-weight:600;color:#6b7280;margin-top:1em;">${subtitle}</div>` : ''}
    </div>
  `
}

export default function EvaluateCareSplashWidget() {
  useEffect(() => {
    window.parent.postMessage({
      type: 'router:subscribe-broadcast',
      kind: 'evalcare-splash',
    }, '*')

    function postRender(content: string) {
      window.parent.postMessage({
        type: 'router:render',
        target: TARGET,
        append: false,
        popup: false,
        content,
      }, '*')
    }

    function onMessage(ev: MessageEvent) {
      const msg = ev.data
      if (!msg || typeof msg !== 'object') return
      if (msg.type === 'router:action' && msg.action === 'goto_evaluatecare') {
        postRender(buildLoadingHtml())
        window.parent.postMessage({
          type: 'router:makeCall',
          op: 'evalcare-splash',
          payload: {},
          call_id: 'evalcare-' + Date.now(),
        }, '*')
        return
      }
      if (msg.type === 'router:event-broadcast' && msg.kind === 'evalcare-splash') {
        postRender(buildEvalcareSplashHtml(msg.data || {}))
      }
    }
    window.addEventListener('message', onMessage)
    return () => window.removeEventListener('message', onMessage)
  }, [])
  return null
}
