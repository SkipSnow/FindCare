// Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
// Licensed under the FindCare Evaluation License (FEL-1.0).
//
// ContextSwitchWidget — leaving a service clears its screens; returning
// restores them exactly as they were left.
//
// Switching to EvaluateCare used to leave FindCare's provider list and
// specialty panel on screen, so the user was looking at one service's
// content under another service's ownership. Switching back left them at
// the top of a fresh list, having lost the page they were reading and any
// detail they had open.
//
// Restoration is a replay from user parameters, not a cached screen. The
// session already holds the panel, the ticks, the geography, the page
// cursors and the open detail, because every step wrote them down. A
// cached screen would be a second copy of those facts that could go stale
// against them.
//
// This widget owns only the gesture. The server owns what a restore means.

import { useEffect } from 'react'

// The frames that carry a service's content. Header and Footer are chrome
// and belong to no service, so they are never blanked.
const CONTENT_FRAMES = ['LeftPanel', 'MainWindow', 'RightPanel']

// Solid white, not empty: an empty frame shows its own CSS background
// (LeftPanel is pastel) and reads as a broken panel rather than a blank one.
const BLANK = '<div style="height:100%;width:100%;background:#fff;"></div>'

export default function ContextSwitchWidget() {
  useEffect(() => {
    function render(target: string, content: string) {
      window.parent.postMessage(
        { type: 'router:render', target, append: false, popup: false, content },
        '*',
      )
    }

    function blankContentFrames() {
      for (const frame of CONTENT_FRAMES) render(frame, BLANK)
    }

    function onMessage(ev: MessageEvent) {
      const msg = ev.data
      if (!msg || typeof msg !== 'object') return
      if (msg.type !== 'router:action') return

      // Leaving FindCare. Its content does not belong on EvaluateCare's
      // screen, and EvaluateCare paints its own once it owns the turn.
      if (msg.action === 'goto_evaluatecare') {
        blankContentFrames()
        return
      }

      // Returning. Ask for the replay and let it paint over whatever is
      // there.
      //
      // It does NOT blank first. Blanking then restoring looks tidy in the
      // code and is destructive on the screen: if there is nothing to
      // restore -- no search has been run yet -- or the replay is still in
      // flight, the user is left looking at three white frames. Clicking
      // "Find care" was harmless before this widget existed, and making it
      // clear the screen turned a navigation into a way to lose your work.
      //
      // A replay repaints every frame it owns, so there is nothing to clear
      // first. Leaving blanks; returning paints.
      if (msg.action === 'goto_findcare') {
        window.parent.postMessage({
          type: 'router:makeCall',
          op: 'restore_findcare',
          payload: {},
          call_id: 'restore-findcare-' + Date.now(),
        }, '*')
        return
      }
    }

    window.addEventListener('message', onMessage)
    return () => window.removeEventListener('message', onMessage)
  }, [])
  return null
}
