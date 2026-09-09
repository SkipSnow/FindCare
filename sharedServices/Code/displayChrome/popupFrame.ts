// Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
// Licensed under the FindCare Evaluation License (FEL-1.0).
//
// The popup frames are markup in the wrapper and CSS hides one that holds
// nothing, so opening is rendering content into it and closing is rendering
// none. The close control is part of the content, because React authors
// content; the wrapper builds no chrome and toggles nothing.

export function withCloseControl(target: string, content: string): string {
  return `<a class="ch-popup-close" href="#" title="Close" aria-label="Close" ` +
    `data-router-action="popup_close" data-target="${target}" ` +
    `data-print-omit="1">&times;</a>` + content
}

export function openPopup(target: string, content: string): void {
  window.parent.postMessage({
    type: 'router:render', target, append: false,
    content: withCloseControl(target, content),
  }, '*')
}

export function closePopup(target: string): void {
  window.parent.postMessage(
    { type: 'router:render', target, append: false, content: '' }, '*')
}
