// Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
// Licensed under the FindCare Evaluation License (FEL-1.0).
//
// LegalPanelWidget — listens for router:action 'open_panel' with
// data-path and data-title. Fetches the static HTML page from the
// wrapper origin and renders it into a popup target via router:render.

import { useEffect } from 'react'
import { openPopup, closePopup } from '../../../../../../sharedServices/Code/displayChrome/popupFrame'

function _esc(s: any): string {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;')
}

function buildPopupContent(title: string, path: string): string {
  return `
    <div style="font-family:system-ui,-apple-system,sans-serif;">
      <h2 style="margin:0 0 0.5em 0;color:#0b7a75;font-size:1.25em;">${_esc(title)}</h2>
      <iframe src="/${_esc(path.replace(/^\//, ''))}"
              title="${_esc(title)}"
              style="width:100%;height:65vh;border:0.0625em solid #d8e2e1;border-radius:0.4em;background:#fff;"></iframe>
    </div>
  `
}

export default function LegalPanelWidget() {
  useEffect(() => {
    function onMessage(ev: MessageEvent) {
      const msg = ev.data
      if (!msg || typeof msg !== 'object') return
      if (msg.type !== 'router:action') return
      if (msg.action !== 'open_panel') return
      const path  = String((msg.data && msg.data.path)  || '').trim()
      const title = String((msg.data && msg.data.title) || path)
      if (!path) return
      const target = 'LegalPanel'
      openPopup(target, buildPopupContent(title, path))
    }
    window.addEventListener('message', onMessage)
    return () => window.removeEventListener('message', onMessage)
  }, [])
  return null
}
