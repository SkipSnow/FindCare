// Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
// Licensed under the FindCare Evaluation License (FEL-1.0).
//
// UserPromptWidget — owns frame_UserPromptAndControl. Renders the input
// form on mount (HTML ported verbatim from prod _oneshots/prod_index.html
// lines 954-957). On submit, fires router:makeCall(op:'utterance'). On
// router:final, re-enables the form.

import { useEffect, useRef } from 'react'

function renderPromptForm(disabled: boolean): string {
  const dis = disabled ? 'disabled' : ''
  return `
    <form id="userInputForm" data-router-action="user:submit"
          style="padding:0.67em 1em;border-top:0.125em solid #e5e7eb;display:flex;gap:8px;align-items:center;background:#fff;">
      <span id="userInputTimer" data-testid="prompt-row-timer"
            style="display:none;flex:0 0 auto;min-width:44px;padding:0.67em 1em;border-radius:6px;background:#f0fffe;border:0.125em solid #0b7a75;font-size:1em;font-weight:700;color:#0b7a75;text-align:center;">0s</span>
      <input id="userInput" name="text" type="text" autocomplete="off" ${dis}
             placeholder="Type a message..."
             style="flex:1;padding:0.67em 1em;border-radius:8px;border:0.125em solid #d1d5db;font-size:1em;outline:none;min-height:44px;" />
      <button id="userInputSubmit" type="submit" ${dis}
              style="padding:0.67em 1em;border-radius:8px;border:none;background:#0b7a75;color:#fff;font-size:1em;font-weight:600;cursor:pointer;min-height:44px;min-width:44px;">Send</button>
    </form>
  `
}

export default function UserPromptWidget() {
  const inflightRef = useRef(false)

  useEffect(() => {
    function paint(disabled: boolean) {
      window.parent.postMessage({
        type: 'router:render',
        target: 'UserPromptAndControl',
        append: false,
        popup: false,
        content: renderPromptForm(disabled),
      }, '*')
    }

    paint(false)

    function onMessage(ev: MessageEvent) {
      const msg = ev.data
      if (!msg || typeof msg !== 'object') return
      if (msg.type === 'router:action' && msg.action === 'user:submit') {
        if (inflightRef.current) return
        const text = String((msg.data && msg.data.text) || '').trim()
        if (!text) return
        inflightRef.current = true
        paint(true)
        window.parent.postMessage({
          type: 'router:makeCall',
          call_id: `prompt-${Date.now()}`,
          op: 'utterance',
          payload: { text },
        }, '*')
      }
      if (msg.type === 'router:final' || msg.type === 'router:error') {
        inflightRef.current = false
        paint(false)
      }
    }
    window.addEventListener('message', onMessage)
    return () => window.removeEventListener('message', onMessage)
  }, [])

  return null
}
