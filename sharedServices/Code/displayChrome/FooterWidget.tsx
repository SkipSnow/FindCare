// Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
// Licensed under the FindCare Evaluation License (FEL-1.0).
//
// FooterWidget — renders the Footer frame chrome via ClientRouter.render.
// Per architecture: index.html is a lightweight container; this widget
// owns the Footer frame's content. Clicks on data-router-action elements
// are bound automatically by ClientRouter._bindActions and forwarded back
// to the React iframe as router:action messages.

import { useEffect } from 'react'

const TARGET = 'Footer'

function buildFooterHtml(): string {
  const lnk = (action: string, label: string, extra = '') =>
    `<a href="#" data-router-action="${action}" ${extra}
        style="color:#0b7a75;text-decoration:underline;cursor:pointer;">${label}</a>`
  const panel = (path: string, title: string) =>
    lnk('open_panel', title, `data-path="${path}" data-title="${title.replace(/"/g, '&quot;')}"`)
  // On a phone these links live in the menu instead, which is where the
  // header's own links already go at this width. The copyright stays at
  // the foot of the page. The breakpoint is the header's, so the two
  // halves of the chrome change together.
  return `
    <style>
      .ch-footer-links { display: contents; }
      @media (max-width: 720px) { .ch-footer-links { display: none; } }
    </style>
    <nav style="display:flex;align-items:center;justify-content:center;gap:1.5em;height:100%;font-size:0.85em;flex-wrap:wrap;">
      <span style="color:#6b7280;font-style:italic;">&copy; 2026 ChatHealthy.ai LLC</span>
      <span class="ch-footer-links">
        ${lnk('about_chathealthy', 'About ChatHealthy.ai')}
        ${panel('privacy.html',       'Privacy Policy')}
        ${panel('terms.html',         'Terms of Use')}
        ${panel('architecture.html',  'Architecture')}
      </span>
    </nav>
  `
}

export default function FooterWidget() {
  useEffect(() => {
    window.parent.postMessage({
      type: 'router:render',
      target: TARGET,
      append: false,
      popup: false,
      content: buildFooterHtml(),
    }, '*')
  }, [])
  return null
}
