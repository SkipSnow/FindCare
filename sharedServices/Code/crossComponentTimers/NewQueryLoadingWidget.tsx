// Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
// Licensed under the FindCare Evaluation License (FEL-1.0).
//
// NewQueryLoadingWidget — owns the loading UX for every classified query.
//
// On kind:'intent_classified', the widget:
//   1. Blanks the three content frames (LeftPanel, RightPanel, MainWindow)
//      per Skip 2026-07-04 (Change C): the moment UM understands a new
//      query, the frames go white so the user sees a clean surface while
//      the tool runs.
//   2. Paints the classified prompt into UserMessage. If the classifier
//      returned spelling corrections, the corrected text is what shows.
//   3. Paints a cumulative timer into MainWindow (only place — Change D:
//      MainWindow timer appears only when MainWindow has no other content).
//      The moment a content widget paints its own content into MainWindow,
//      this timer is overwritten and stops updating there.
//   4. Reveals the prompt-row timer to the LEFT of the input box
//      (#userInputTimer inside UserPromptAndControl) and drives its
//      seconds counter for the full waiting period.
//
// Both timers are cumulative from the FIRST user:submit of this query up
// to router:final. They do not reset per phase.

import { useEffect, useRef } from 'react'

const MAIN_TIMER_ID = 'nq-main-timer'
const PROMPT_TIMER_ID = 'userInputTimer'

function buildMainWindowTimer(): string {
  return `
    <div style="display:flex;flex-direction:column;align-items:center;justify-content:center;height:100%;padding:2em;color:#0b7a75;">
      <div id="${MAIN_TIMER_ID}" style="font-size:2em;font-weight:700;">0s</div>
    </div>
  `
}

export default function NewQueryLoadingWidget() {
  const startTsRef = useRef<number | null>(null)
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null)

  useEffect(() => {
    function postRender(target: string, content: string) {
      window.parent.postMessage({
        type: 'router:render', target, append: false, popup: false, content,
      }, '*')
    }
    function postExec(code: string) {
      window.parent.postMessage({ type: 'router:exec', code }, '*')
    }

    function stopTicking() {
      if (timerRef.current) {
        clearInterval(timerRef.current)
        timerRef.current = null
      }
      startTsRef.current = null
      // Hide the prompt-row timer.
      postExec(`(function(){
        var t = document.querySelector('iframe[data-frame="MainWindow"]');
        if (t && t.contentWindow) return;
      })();`)
      // Prompt-row timer lives inside the UserPromptAndControl frame in
      // the parent DOM. Hide it there.
      postExec(`(function(){
        var el = document.getElementById('${PROMPT_TIMER_ID}');
        if (el) { el.style.display = 'none'; el.textContent = '0s'; }
      })();`)
    }

    function startTicking() {
      stopTicking()
      startTsRef.current = Date.now()
      // Show the prompt-row timer.
      postExec(`(function(){
        var el = document.getElementById('${PROMPT_TIMER_ID}');
        if (el) { el.style.display = 'inline-block'; el.textContent = '0s'; }
      })();`)
      timerRef.current = setInterval(() => {
        if (startTsRef.current === null) return
        const sec = Math.round((Date.now() - startTsRef.current) / 1000)
        const label = `${sec}s`
        // Merge into both timer regions. Merge is a no-op when the region
        // has been replaced (MainWindow overwritten by content). The
        // prompt-row timer is in the parent DOM so we go via router:exec.
        window.parent.postMessage({
          type: 'router:merge',
          target: 'MainWindow',
          region: MAIN_TIMER_ID,
          content: label,
        }, '*')
        postExec(`(function(){
          var el = document.getElementById('${PROMPT_TIMER_ID}');
          if (el) el.textContent = '${label}';
        })();`)
      }, 1000)
    }

    // Solid white fill so the frame's own CSS background (LeftPanel is
    // pastel blue by default) does not show through an empty innerHTML.
    const WHITE_FILL = '<div style="height:100%;width:100%;background:#fff;"></div>'

    function whiteOutAndEcho(prompt: string) {
      // Blank the three content frames per Change C. Solid white, not
      // an empty div — otherwise the frame's default background color
      // shows and the surface looks pastel-blue, not blank.
      postRender('LeftPanel', WHITE_FILL)
      postRender('RightPanel', WHITE_FILL)
      postRender('MainWindow', buildMainWindowTimer())
      // Echo the (possibly corrected) prompt into UserMessage.
      // Rule-008 statement 4 forbids regex in executable front-end code,
      // so escaping uses split/join instead of .replace(/.../g).
      const safe = String(prompt || '')
        .split('&').join('&amp;')
        .split('<').join('&lt;')
        .split('>').join('&gt;')
      postRender('UserMessage',
        `<div style="padding:0.5em 1em;color:#374151;font-style:italic;">${safe}</div>`
      )
    }

    window.parent.postMessage({
      type: 'router:subscribe-broadcast', kind: 'intent_classified',
    }, '*')

    function onMessage(ev: MessageEvent) {
      const msg = ev.data
      if (!msg || typeof msg !== 'object') return

      // Change D: prompt-row timer starts the MOMENT the user submits —
      // client-side, no server round-trip. Cumulative from here to
      // router:final. The white-out itself waits for intent_classified so
      // we do not wipe the previous query's content until UM has actually
      // understood the new one.
      if (msg.type === 'router:action' && msg.action === 'user:submit') {
        const text = String((msg.data || {}).text || '').trim()
        if (text) startTicking()
        return
      }

      if (msg.type === 'router:event-broadcast' && msg.kind === 'intent_classified') {
        const d = msg.data || {}
        // Prefer the classifier's corrected text; fall back to the raw
        // utterance the user typed.
        const echoed = String(
          d.corrected_text || d.corrected || d.text || d.utterance || ''
        )
        whiteOutAndEcho(echoed)
        // startTicking() is idempotent — if user:submit already started
        // the timer, this call is a no-op relative to the timer state
        // (stopTicking + startTicking resets t0). Guard so the timer
        // does not reset partway through.
        if (startTsRef.current === null) startTicking()
        return
      }

      if (msg.type === 'router:final' || msg.type === 'router:error') {
        stopTicking()
      }
    }
    window.addEventListener('message', onMessage)
    return () => {
      stopTicking()
      window.removeEventListener('message', onMessage)
    }
  }, [])
  return null
}
