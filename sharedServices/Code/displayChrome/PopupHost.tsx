// Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
// Licensed under the FindCare Evaluation License (FEL-1.0).
//
// PopupCloser — the one place a popup is closed.
//
// The popup frames are markup in the wrapper and CSS hides a frame holding
// nothing, so closing is rendering no content into it. The control that
// asks for that is authored by React as part of the popup's content, and
// arrives here as an ordinary router action carrying the frame it closes.

import { useEffect } from 'react'
import { closePopup } from './popupFrame'

export default function PopupHost() {
  useEffect(() => {
    function onMessage(ev: MessageEvent) {
      const msg = ev.data
      if (!msg || typeof msg !== 'object') return
      if (msg.type !== 'router:action' || msg.action !== 'popup_close') return
      const target = (msg.data && msg.data.target) || ''
      if (target) closePopup(String(target))
    }
    window.addEventListener('message', onMessage)
    return () => window.removeEventListener('message', onMessage)
  }, [])
  return null
}
