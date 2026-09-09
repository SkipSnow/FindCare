// Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
// Licensed under the FindCare Evaluation License (FEL-1.0).
//
// SessionDataWidget — on router:action 'goto_sharedservices' fires
// router:makeCall(op:'splash'). Subscribed to kind:'splash' broadcast;
// paints the user_object into frame_MainWindow.

import { useEffect } from 'react'
import { openPopup, closePopup } from '../displayChrome/popupFrame'

// Its own popup, beside About rather than painted over MainWindow.
const TARGET = 'SessionInfoPopUp'
// The element the PDF is taken from. The popup is React's, so printing it
// is React's too -- the wrapper needs no print handler.
const SESSION_PRINT_ROOT = 'session_print_root'
// The website answers for itself; nothing on a static site can be asked.
let websiteBuild: any = null

function _esc(s: any): string {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;')
}

function buildLoadingHtml(): string {
  return `
    <div style="display:flex;align-items:center;justify-content:center;height:100%;padding:2em;color:#0b7a75;">
      <div>Loading SharedServices…</div>
    </div>
  `
}

// Ported verbatim from the prod splash renderer
// (Website/splash_render.js + Website/splash.css before the architecture
// rewrite). Identity = ordered dl/dt/dd grid. Session Conversation
// History = two side-by-side threads (Utterances + Actions), each with
// max-height:15em and overflow-y:auto so they scroll independently —
// the two scrollbars that make the live transcript easy to read.

const IDENTITY_ORDER = [
  'user_type', 'guid', 'origin', 'server_env', 'form_factor',
  'created_at', 'expires_at',
]
const IDENTITY_HIDDEN = new Set(['token', 'signature'])

function _fmt(v: any): string {
  if (v === null || v === undefined) return ''
  if (typeof v === 'object') {
    try { return JSON.stringify(v, null, 2) } catch { return String(v) }
  }
  return String(v)
}

function buildIdentityHtml(identity: any): string {
  const ordered: string[] = []
  const seen = new Set<string>()
  for (const k of IDENTITY_ORDER) {
    if (k in identity) { ordered.push(k); seen.add(k) }
  }
  for (const k of Object.keys(identity)) {
    if (seen.has(k) || IDENTITY_HIDDEN.has(k)) continue
    ordered.push(k)
  }
  const rows = ordered.map(k =>
    `<dt style="font-weight:600;color:#374151;">${_esc(k)}</dt>` +
    `<dd style="margin:0;color:#1f2937;white-space:pre-wrap;word-break:break-all;">${_esc(_fmt(identity[k]))}</dd>`
  ).join('')
  return `
    <section style="margin-top:1em;">
      <h3 style="margin:0 0 0.5em 0;color:#0b7a75;">Identity</h3>
      <dl style="margin:0;display:grid;grid-template-columns:16.25em 1fr;gap:0.25em 1em;">${rows}</dl>
    </section>
  `
}

// Deployment Facts — each component and the build it is running. Named as
// components, because a target id says where something is deployed and not
// what it is. Asked once, when the session is rendered.
function buildDeploymentFactsHtml(rows: any[]): string {
  const body = (Array.isArray(rows) ? rows : []).map(r =>
    `<dt style="font-weight:600;color:#374151;">${_esc(r.component)}</dt>` +
    `<dd style="margin:0;color:${r.build ? '#1f2937' : '#b91c1c'};">` +
    `${_esc(r.build || 'not yet loaded')}</dd>`
  ).join('')
  const inner = body
    ? `<dl style="margin:0;display:grid;grid-template-columns:16.25em 1fr;gap:0.25em 1em;">${body}</dl>`
    : `<p style="margin:0;color:#9ca3af;font-style:italic;">No component answered.</p>`
  return `
    <section style="margin-top:1em;">
      <h3 style="margin:0 0 0.5em 0;color:#0b7a75;">Deployment Facts</h3>
      ${inner}
    </section>
  `
}

// The website is a component like the others and reports independently.
function withWebsite(rows: any[]): any[] {
  const build = websiteBuild && websiteBuild.build
  return [...(Array.isArray(rows) ? rows : []),
          { component: 'Website', build: build ? String(build) : '' }]
}

// The order attributes read best in within a page. Anything a page
// declares beyond these follows, alphabetically.
const ATTRIBUTE_ORDER = [
  'geography', 'complaint', 'offeredSpecialties', 'selectedSpecialtyCodes',
]

// The pages, in the order the panel groups them.
const PAGE_ORDER = ['individualProvider', 'facility', 'NUCC', 'clinicalTrial']

function fmtParameter(name: string, value: any): string {
  if (value === null || value === undefined) return '(not set)'
  if (name === 'geography') {
    const parts = ['state', 'city', 'county', 'zip']
      .filter(k => value[k]).map(k => `${k}: ${value[k]}`)
    return parts.length ? parts.join(', ') : '(not set)'
  }
  if (name === 'offeredSpecialties') {
    if (!Array.isArray(value) || !value.length) return '(none offered yet)'
    return value.map((s: any) =>
      `${s.name || '?'} (${s.code})${s.can_prescribe ? ' Rx' : ''}${s.homeopathic ? ' homeo' : ''}`
    ).join(', ')
  }
  if (name === 'selectedSpecialtyCodes') {
    if (!Array.isArray(value) || !value.length) return '(nothing narrowed - all offered apply)'
    return value.join(', ')
  }
  if (typeof value === 'string') return value.trim() || '(not set)'
  return _fmt(value)
}

// Only attributes carrying a value are shown: "in force" is having one,
// and showing every declared attribute would make this a form rather than
// a statement of what is true.
function inForce(entry: any): boolean {
  if (!entry) return false
  const v = entry.value
  if (v === null || v === undefined) return false
  if (typeof v === 'string' && v.trim() === '') return false
  if (Array.isArray(v) && v.length === 0) return false
  if (typeof v === 'object' && !Array.isArray(v) && Object.keys(v).length === 0) return false
  return true
}

function buildPageHtml(page: string, attributes: any): string {
  const names = ATTRIBUTE_ORDER.filter(k => k in attributes && inForce(attributes[k]))
    .concat(Object.keys(attributes)
      .filter(k => !ATTRIBUTE_ORDER.includes(k) && inForce(attributes[k]))
      .sort())
  if (!names.length) return ''
  const rows = names.map(k => {
    const entry = attributes[k]
    // A value that arrived by carry-over is shown with the page it came
    // from. That is a fact the change record already stored, not an
    // inference made here.
    const from = entry.carried_from
      ? `<span style="color:#b45309;"> (carried from ${_esc(entry.carried_from)})</span>` : ''
    return `<dt style="font-weight:600;color:#374151;">${_esc(k)}</dt>` +
      `<dd style="margin:0;color:#1f2937;white-space:pre-wrap;overflow-wrap:anywhere;">` +
      `${_esc(fmtParameter(k, entry.value))}${from}` +
      `<a href="#" data-router-action="parameter:clear" data-page="${_esc(page)}" data-name="${_esc(k)}" ` +
      `style="margin-left:0.5em;color:#0b7a75;font-size:0.85em;">clear</a>` +
      `</dd>`
  }).join('')
  return (
    `<div style="margin-top:0.5em;">` +
      `<h4 style="margin:0 0 0.15em 0;color:#0b7a75;">${_esc(page)}` +
        `<a href="#" data-router-action="parameter:clear-page" data-page="${_esc(page)}" ` +
        `style="margin-left:0.5em;color:#0b7a75;font-size:0.8em;font-weight:400;">clear page</a>` +
      `</h4>` +
      `<dl style="margin:0;display:grid;grid-template-columns:16.25em 1fr;gap:0.25em 1em;">${rows}</dl>` +
    `</div>`
  )
}

function buildParametersHtml(parameters: any): string {
  // The panel renders the parameter state as it is held: grouped by page,
  // because that is its shape, and every parameter shown with the page it
  // belongs to.
  const pages = (parameters && parameters.pages) || {}
  const ordered = PAGE_ORDER.filter(p => p in pages)
    .concat(Object.keys(pages).filter(p => !PAGE_ORDER.includes(p)).sort())
  const groups = ordered.map(p => buildPageHtml(p, pages[p] || {})).join('')
  const body = groups
    ? groups + `<p style="margin:0.5em 0 0 0;">` +
        `<a href="#" data-router-action="parameter:clear-all" style="color:#0b7a75;">clear every parameter of every page</a></p>`
    : `<p style="margin:0;color:#9ca3af;font-style:italic;">Nothing set yet.</p>`
  return `
    <section style="margin-top:1em;">
      <h3 style="margin:0 0 0.25em 0;color:#0b7a75;">User Parameters</h3>
      <p style="color:#6b7280;margin:0 0 0.5em 0;">What is true right now, whichever tool set it. Live state, not history.</p>
      ${body}
    </section>
  `
}

function buildRow(at: any, body: string, n: any): string {
  const nLabel = (n !== undefined && n !== null && n !== '')
    ? `<span style="color:#6b7280;">#${_esc(n)} </span>` : ''
  return `
    <div style="padding:0.4em 0.6em;border-top:0.125em solid #d1fae5;">
      <div style="color:#6b7280;line-height:1.1;">${nLabel}${_esc(at || '')}</div>
      <div style="color:#1f2937;line-height:1.2;overflow-wrap:anywhere;">${body}</div>
    </div>
  `
}

function buildUtteranceRow(it: any): string {
  const actor = _esc(it.actor || '?')
  const text = _esc(it.text || '')
  const body =
    `<strong style="color:${it.actor === 'system' ? '#d97706' : '#0b7a75'};">${actor}:</strong> ${text}`
  return buildRow(it.at, body, it.n)
}

function buildActionRow(it: any): string {
  let inJson = ''
  let outJson = ''
  try {
    if (it.input_json && Object.keys(it.input_json).length) {
      inJson = JSON.stringify(it.input_json)
      if (inJson.length > 400) inJson = inJson.slice(0, 400) + '…'
    }
  } catch { inJson = '' }
  try {
    if (it.output_json && Object.keys(it.output_json).length) {
      outJson = JSON.stringify(it.output_json)
      if (outJson.length > 400) outJson = outJson.slice(0, 400) + '…'
    }
  } catch { outJson = '' }
  const body =
    `<strong>${_esc(it.tool_name || '?')}</strong>` +
    (inJson ? ` (${_esc(inJson)})` : '') +
    (outJson ? ` → ${_esc(outJson)}` : '')
  return buildRow(it.at, body, it.n)
}

function buildThreadHtml(title: string, items: any[], formatter: (it: any) => string): string {
  const empty = !items || items.length === 0
  const body = empty
    ? `<div style="padding:0.5em 0.6em;color:#9ca3af;font-style:italic;">(no entries yet)</div>`
    : items.map(formatter).join('')
  return `
    <div style="border-left:0.25em solid #0b7a75;background:#f0fffe;padding:0.5em 0.6em;">
      <div style="font-weight:600;color:#0b7a75;margin-bottom:0.5em;text-transform:uppercase;letter-spacing:0.05em;">${_esc(title)}</div>
      <div style="max-height:15em;overflow-y:auto;padding-right:0.5em;">${body}</div>
    </div>
  `
}

function buildSessionHtml(data: any): string {
  const identity = (data && data.identity) || {}
  const threads = (data && data.threads) || {}
  const utterances = Array.isArray(threads.utterances) ? threads.utterances : []
  const actions    = Array.isArray(threads.actions)    ? threads.actions    : []
  const history = (threads.empty || (!utterances.length && !actions.length))
    ? `<p style="margin:0;color:#9ca3af;font-style:italic;">No utterances or actions yet. Type a prompt or click around to see this grow.</p>`
    : `<div style="display:grid;grid-template-columns:1fr 1fr;gap:0.75em;">
         ${buildThreadHtml('Utterances', utterances, buildUtteranceRow)}
         ${buildThreadHtml('Actions',    actions,    buildActionRow)}
       </div>`
  return `
    <div id="${SESSION_PRINT_ROOT}" style="display:flex;flex-direction:column;height:100%;min-height:0;padding:1em;box-sizing:border-box;max-width:102.5em;margin:0 auto;text-align:left;line-height:1.35;">
      <div class="ch-popup-drag" style="display:flex;align-items:center;justify-content:space-between;gap:1em;">
        <h2 style="margin:0;color:#0b7a75;">Shared Services — User Object</h2>
        <button type="button" data-router-action="session_pdf" data-print-omit="1"
          style="flex:none;background:#0b7a75;color:#fff;border:none;border-radius:0.3em;padding:0.4em 0.9em;cursor:pointer;font:inherit;">PDF</button>
      </div>
      <p style="color:#6b7280;margin:0 0 0.5em 0;">Live evidence that the entrance code completed. The cookie carries only the GUID; the user object lives in <code>admin.Sessions</code>.</p>
      ${buildIdentityHtml(identity)}
      ${buildDeploymentFactsHtml(withWebsite((data && data.deployment_facts) || []))}
      ${buildParametersHtml((data && data.parameters) || {})}
      <section style="margin-top:1em;flex:1;min-height:0;display:flex;flex-direction:column;">
        <h3 style="margin:0 0 0.5em 0;color:#0b7a75;">Session Conversation History</h3>
        ${history}
      </section>
    </div>
  `
}

export default function SessionDataWidget() {
  useEffect(() => {
    window.parent.postMessage({
      type: 'router:subscribe-broadcast',
      kind: 'session_data',
    }, '*')
    window.parent.postMessage({
      type: 'router:subscribe-broadcast',
      kind: 'parameters_changed',
    }, '*')

    function postRender(content: string) {
      openPopup(TARGET, content)
    }

    function onMessage(ev: MessageEvent) {
      const msg = ev.data
      if (!msg || typeof msg !== 'object') return
      if (msg.type === 'router:action' && msg.action === 'session_info') {
        postRender(buildLoadingHtml())
        window.parent.postMessage({ type: 'router:ask-build' }, '*')
        window.parent.postMessage({
          type: 'router:makeCall',
          op: 'session_data',
          payload: {},
          call_id: 'session_data-' + Date.now(),
        }, '*')
        return
      }
      if (msg.type === 'router:action' && msg.action === 'session_pdf') {
        window.parent.postMessage({
          type: 'router:download',
          op: 'session_pdf',
          filename: 'chatHealthySessionInfo.pdf',
        }, '*')
        return
      }
      // The panel is a surface, not a fourth writer. A control on it
      // sends a request through the gateway, which addresses the
      // parameters tool exactly as any other route does, so the same
      // validation and the same refusal apply.
      if (msg.type === 'router:action' && msg.action === 'parameter:clear') {
        window.parent.postMessage({
          type: 'router:makeCall', op: 'parameter_change',
          payload: { verb: 'clear', page: String((msg.data || {}).page || ''),
                     name: String((msg.data || {}).name || '') },
          call_id: 'parameter-clear-' + Date.now(),
        }, '*')
        return
      }
      if (msg.type === 'router:action' && msg.action === 'parameter:clear-page') {
        window.parent.postMessage({
          type: 'router:makeCall', op: 'parameter_change',
          payload: { verb: 'clear_page', page: String((msg.data || {}).page || '') },
          call_id: 'parameter-clear-page-' + Date.now(),
        }, '*')
        return
      }
      if (msg.type === 'router:action' && msg.action === 'parameter:clear-all') {
        window.parent.postMessage({
          type: 'router:makeCall', op: 'parameter_change',
          payload: { verb: 'clear_all' },
          call_id: 'parameter-clear-all-' + Date.now(),
        }, '*')
        return
      }
      // The response to a parameter change carries the new state and the
      // panel repaints from it in the same turn that made the change. It
      // is not refetched: a second read could disagree with what was just
      // acknowledged.
      if (msg.type === 'router:event-broadcast' && msg.kind === 'parameters_changed') {
        postRender(buildSessionHtml(msg.data || {}))
        return
      }
      if (msg.type === 'router:website-build') {
        websiteBuild = msg.build || {}
        return
      }
      if (msg.type === 'router:event-broadcast' && msg.kind === 'session_data') {
        postRender(buildSessionHtml(msg.data || {}))
      }
    }
    window.addEventListener('message', onMessage)
    return () => window.removeEventListener('message', onMessage)
  }, [])
  return null
}
