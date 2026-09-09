// Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
// Licensed under the FindCare Evaluation License (FEL-1.0).
//
// PanelNavWidget — the two arrows that move a phone between panels.
//
// React authors them because they are display. Which one is on screen is
// decided by where the strip is scrolled, which no stylesheet can see, so
// the wrapper sets ch-can-left / ch-can-right on the body and this widget
// says what those states look like. Nothing here scrolls anything.

import { useEffect } from 'react'

const TARGET = 'Footer'
const TEAL = '#0b7a75'

// The breakpoint is in em against the browser's own default, matching
// the rest of the chrome. Every length here is relative; nothing in this
// file fixes a pixel count.
function buildArrowsHtml(): string {
  const arrow = (side: 'left' | 'right', glyph: string) =>
    `<button type="button" class="ch-panel-arrow ch-panel-${side}"` +
    ` data-router-action="panel_${side}"` +
    ` data-testid="panel-arrow-${side}"` +
    ` aria-label="Show the panel to the ${side}">${glyph}</button>`
  return `
    <style>
      .ch-panel-arrow { display: none; }
      @media (max-width: 45em) {
        .ch-panel-arrow {
          position: fixed; bottom: 0.6em; z-index: 40;
          min-width: 3.2em; min-height: 3.2em;
          border: none; border-radius: 50%;
          background: ${TEAL}; color: #fff;
          font-size: 1.6em; line-height: 1; cursor: pointer;
          box-shadow: 0 0.1em 0.4em rgba(0,0,0,0.35);
          align-items: center; justify-content: center;
        }
        .ch-panel-left  { left: 0.6em; }
        .ch-panel-right { right: 0.6em; }
        body.ch-can-left  .ch-panel-left  { display: flex; }
        body.ch-can-right .ch-panel-right { display: flex; }
      }
    </style>
    ${arrow('left', '&#8249;')}${arrow('right', '&#8250;')}
  `
}

export default function PanelNavWidget() {
  useEffect(() => {
    window.parent.postMessage({
      type: 'router:render', target: TARGET, append: true, popup: false,
      content: `<div id="panel_nav">${buildArrowsHtml()}</div>`,
    }, '*')
  }, [])
  return null
}
