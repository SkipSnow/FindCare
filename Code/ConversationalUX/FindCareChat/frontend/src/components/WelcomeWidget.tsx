// Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
// Licensed under the FindCare Evaluation License (FEL-1.0).
//
// WelcomeWidget — fetches FindCare's /welcome HTML (the Georgia-2em
// welcome line) and wraps it in prod's chat-bubble verbatim
// (_oneshots/prod_index.html lines 1776-1781): outer
// `padding:1em;max-width:50em;margin:1em auto` + inner
// `padding:1em;border-radius:2.25em 2.25em 2.25em 0.5em;background:#fff;
// border:0.125em solid #e5e7eb;font-size:1em;line-height:1.6`.
// Result: bubble near top of frame_MainWindow with the centered welcome
// text inside.

import { useEffect } from 'react'

const TARGET = 'MainWindow'

const FALLBACK_WELCOME_HTML =
  '<div style="margin-top:10vh;">' +
    '<p style="text-align:center;font-family:Georgia,\'Times New Roman\',serif;font-size:2em;line-height:1.3;margin:0;">' +
      'Find care with Caregivers, or at a facility in the United States, or find a clinical trial anywhere in the world.<br>' +
      "Let's talk about it." +
    '</p>' +
  '</div>'

function wrapBubble(innerHtml: string): string {
  return `
    <div style="padding:1em;max-width:50em;margin:1em auto;">
      <div style="padding:1em;border-radius:2.25em 2.25em 2.25em 0.5em;background:#fff;border:0.125em solid #e5e7eb;font-size:1em;line-height:1.6;">
        ${innerHtml}
      </div>
    </div>
  `
}

export default function WelcomeWidget() {
  useEffect(() => {
    // The React iframe is at FindCare origin; window.location.origin is
    // that origin, and `/welcome` is served by the same FindCare backend.
    // Same-origin fetch — no cross-origin parent access required.
    const fcOrigin = window.location.origin
    let cachedHtml = FALLBACK_WELCOME_HTML

    function paint(inner: string) {
      window.parent.postMessage({
        type: 'router:render',
        target: TARGET,
        append: false,
        popup: false,
        content: wrapBubble(inner),
      }, '*')
    }
    paint(FALLBACK_WELCOME_HTML)
    fetch(`${fcOrigin}/welcome`, { method: 'POST' })
      .then(r => r.ok ? r.json() : null)
      .then(d => {
        const html = d && typeof d.message === 'string' ? d.message : ''
        if (html) { cachedHtml = html; paint(html) }
      })
      .catch(() => {})

    // Widgets paint only on explicit paint directives, never inferred from
    // lifecycle events. The server emits kind:'show_welcome' whenever
    // MainWindow should return to the welcome bubble — e.g., UM authoring a
    // refinement prompt with no tool paint on this turn. Widgets that paint
    // MainWindow on their own (clinical trials, provider detail, etc.) do
    // NOT trigger this directive, so their content is not overwritten by an
    // incidental close.
    // The router:action 'goto_findcare' handler stays because that is a
    // direct user gesture (nav button click) with the same intent.
    window.parent.postMessage({ type: 'router:subscribe-broadcast', kind: 'show_welcome' }, '*')
    function postRenderTarget(target: string, content: string) {
      window.parent.postMessage({
        type: 'router:render',
        target,
        append: false,
        popup: false,
        content,
      }, '*')
    }
    function resetToInitial() {
      // Home = identical to first load: welcome bubble in MainWindow,
      // every other content frame blank.
      paint(cachedHtml)
      postRenderTarget('LeftPanel', '')
      postRenderTarget('RightPanel', '')
      postRenderTarget('UserMessage', '')
    }
    function onMessage(ev: MessageEvent) {
      const msg = ev.data
      if (!msg || typeof msg !== 'object') return
      if (msg.type === 'router:event-broadcast' && msg.kind === 'show_welcome') {
        paint(cachedHtml)
      } else if (msg.type === 'router:action' && msg.action === 'goto_findcare') {
        paint(cachedHtml)
      } else if (msg.type === 'router:action' && msg.action === 'goto_home') {
        resetToInitial()
      }
    }
    window.addEventListener('message', onMessage)
    return () => window.removeEventListener('message', onMessage)
  }, [])
  return null
}
