// Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
// Licensed under the FindCare Evaluation License (FEL-1.0).
//
// ProviderDetailWidget — on router:action 'provider:detail' fires
// router:makeCall(op:'provider-detail', payload:{npi}). Subscribed to
// kind:'provider-detail' broadcast; renders into frame_RightPanel.

import { useEffect } from 'react'

const TARGET = 'RightPanel'

function _esc(s: any): string {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;')
}

function buildLoadingHtml(npi: string): string {
  return `
    <div style="padding:1em;font-family:system-ui,-apple-system,sans-serif;">
      <div style="color:#0b7a75;font-weight:700;font-size:0.95em;margin-bottom:0.4em;">Provider detail</div>
      <div style="color:#6b7280;font-size:0.85em;">Loading NPI ${_esc(npi)}…</div>
    </div>
  `
}

// SS provider_detail_tool response shape:
//   { provider_name, npi, identity:{primary_taxonomy_display,...},
//     addresses:[{line1,line2,city,state,zip,phone,address_type,...}],
//     licenses:[{state, number}], insurance:[], research_sites:{...} }
function buildDetailHtml(data: any): string {
  const p = data || {}
  const identity = p.identity || {}
  const name = _esc(p.provider_name || '')
  const specialty = _esc(identity.primary_taxonomy_display || '')
  const sect = (label: string, body: string) =>
    `<div style="margin-top:0.75em;">
       <div style="font-size:0.8em;font-weight:700;color:#0b7a75;text-transform:uppercase;letter-spacing:0.05em;">${_esc(label)}</div>
       <div style="font-size:0.9em;color:#374151;margin-top:0.2em;">${body}</div>
     </div>`
  const addrs = (Array.isArray(p.addresses) ? p.addresses : [])
    .map((a: any, i: number) => {
      const label = a.address_type
        ? (String(a.address_type).charAt(0).toUpperCase() + String(a.address_type).slice(1))
        : (i === 0 ? 'Primary' : `Address ${i + 1}`)
      const line = [a.line1, a.line2, a.city, a.state, a.zip, a.country].filter(Boolean).map(_esc).join(', ')
      const tail = a.phone ? `Phone: ${_esc(a.phone)}` : ''
      // The county the address carries, and whether that county is urban
      // where that is known. The flag is absent when enrichment did not
      // resolve the address, and an absent flag renders as no urban
      // statement rather than as a claim of rural.
      const county = a.county || null
      const countyName = county && county.name ? _esc(county.name) : ''
      const urban = county && typeof county.urban === 'boolean'
        ? (county.urban ? 'Urban' : 'Rural') : ''
      const countyLine = countyName
        ? `<div style="font-size:0.85em;color:#6b7280;">County: ${countyName}${urban ? ' &middot; ' + urban : ''}</div>`
        : ''
      return `<div style="margin:0.3em 0;">
                <span style="font-weight:600;">${_esc(label)}:</span> ${line}
                ${tail ? `<div style="font-size:0.85em;color:#6b7280;">${tail}</div>` : ''}
                ${countyLine}
              </div>`
    }).join('') || `<div style="color:#6b7280;font-style:italic;">No address on file.</div>`
  const licenses = (Array.isArray(p.licenses) ? p.licenses : [])
    .map((l: any) => `${_esc(l.state || '')} ${_esc(l.number || '')}`.trim())
    .filter(Boolean).join(' &middot; ') || 'None on file.'
  // Four fields per row, so the person can see what the identifier is
  // rather than infer it. Never labelled as insurance accepted or as
  // network membership: nothing in this datum establishes either.
  const payerIdentifiers = (Array.isArray(p.insurance) ? p.insurance : [])
    .map((ins: any) =>
      `<div style="margin:0.3em 0;">
         <span style="font-weight:600;">${_esc(ins.coverage_kind || '')}</span>
         ${ins.issuer ? ' &middot; ' + _esc(ins.issuer) : ''}
         ${ins.state ? ' &middot; ' + _esc(ins.state) : ''}
         ${ins.identifier ? `<div style="font-size:0.85em;color:#6b7280;">Identifier: ${_esc(ins.identifier)}</div>` : ''}
       </div>`)
    .join('') || 'None on file.'
  // research_sites is a dict keyed by site slug ('healthgrades', etc.) with
  // {url, name, guidance}. Render each as a labeled link with the guidance
  // sentence underneath. Source: SS provider_detail_tool.Source schema.
  // A facility shows its own identity in place of a person's, and the
  // person the registry recorded as its authorized official -- labelled
  // as that role and no other (EPIC-006-F-007-S-002-REQ-B-002).
  const oi = p.organization_identity || null
  const organization = oi
    ? `<div data-testid="facility-identity" style="margin-top:0.75em;">
         <div style="font-size:0.8em;font-weight:700;color:#0b7a75;text-transform:uppercase;letter-spacing:0.05em;">Organization</div>
         <div style="font-size:0.9em;color:#374151;margin-top:0.2em;">
           <div data-testid="facility-legal-name"><span style="font-weight:600;">Legal business name:</span> ${_esc(oi.legal_business_name || '')}</div>
           ${oi.other_organization_name ? `<div>${_esc(oi.other_organization_name_kind || 'Other name')}: ${_esc(oi.other_organization_name)}</div>` : ''}
           <div>NPI issued: ${_esc(oi.enumeration_date || '')}</div>
           <div>${oi.status_active ? 'Currently active' : 'Not currently active'}</div>
           ${oi.is_subpart ? `<div>Part of: ${_esc(oi.parent_organization_name || '')}</div>` : ''}
         </div>
       </div>`
    : ''
  const ao = p.authorized_official || null
  const official = ao
    ? `<div data-testid="authorized-official" style="margin-top:0.75em;">
         <div style="font-size:0.8em;font-weight:700;color:#0b7a75;text-transform:uppercase;letter-spacing:0.05em;">The record's authorized official</div>
         <div style="font-size:0.9em;color:#374151;margin-top:0.2em;">
           <div data-testid="official-name">${_esc([ao.first_name, ao.middle_name, ao.last_name].filter(Boolean).join(' '))}</div>
           ${ao.title_or_position ? `<div>${_esc(ao.title_or_position)}</div>` : ''}
           ${ao.credential ? `<div>${_esc(ao.credential)}</div>` : ''}
         </div>
       </div>`
    : ''
  const rs = p.research_sites || {}
  // Every destination carries a URL: the detail service builds a site
  // only when it has one, and surfaces an unresolved state separately
  // rather than emitting a destination that goes nowhere.
  const rsEntries = Object.keys(rs).map(k => rs[k])
  const research = rsEntries.length
    ? rsEntries.map((s: any) =>
        `<div style="margin:0.3em 0;">
           <a href="${_esc(s.url || '')}" target="_blank" rel="noopener noreferrer"
              style="color:#0b7a75;text-decoration:underline;font-weight:600;">${_esc(s.name || '')}</a>
           ${s.guidance
             ? `<div style="font-size:0.85em;color:#6b7280;margin-top:0.1em;">${_esc(s.guidance)}</div>`
             : ''}
         </div>`).join('')
    : '<div style="color:#6b7280;font-style:italic;">None on file.</div>'
  return `
    <div style="padding:0.75em 1em;">
      <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:0.5em;">
        <div style="font-weight:700;color:#0b7a75;font-size:1.05em;">${name}</div>
        <button type="button" data-router-action="provider:detail-close"
                data-testid="provider-detail-close" title="Close"
                style="flex-shrink:0;background:#fff;border:0.125em solid #0b7a75;color:#0b7a75;
                       border-radius:0.375em;cursor:pointer;font-weight:700;line-height:1;
                       padding:0.15em 0.45em;font-size:0.95em;">&times;</button>
      </div>
      <div style="font-size:0.85em;color:#6b7280;">NPI: ${_esc(p.npi || '')}</div>
      <div style="font-size:0.85em;color:#374151;margin-top:0.2em;">${specialty}</div>
      ${organization}
      ${official}
      ${sect('Practice addresses', addrs)}
      ${sect('Licenses', licenses)}
      ${sect('Payer identifiers this provider carries', payerIdentifiers)}
      ${sect('Research', research)}
      ${p.unresolved_licensing_state
        ? sect('Licensing authority',
               `No licensing authority is held for ${_esc(p.unresolved_licensing_state)}.`)
        : ''}
    </div>
  `
}

// Solid white, not empty: an empty frame shows its own CSS background and
// reads as a broken panel rather than a closed one.
const BLANK = '<div style="height:100%;width:100%;background:#fff;"></div>'

export default function ProviderDetailWidget() {
  useEffect(() => {
    window.parent.postMessage({
      type: 'router:subscribe-broadcast',
      kind: 'provider-detail',
    }, '*')
    window.parent.postMessage({
      type: 'router:subscribe-broadcast',
      kind: 'provider_detail_close',
    }, '*')
    // A turn about another page takes this panel down
    // (EPIC-006-F-008-S-002-REQ-B-006). The server clears the record the
    // other page held open, but a parameter going away does not unpaint a
    // frame, so a facility detail stayed on screen beside a list of care
    // givers in another city until something else replaced it.
    window.parent.postMessage({
      type: 'router:subscribe-broadcast',
      kind: 'intent_classified',
    }, '*')

    function postRender(content: string) {
      window.parent.postMessage({
        type: 'router:render',
        target: TARGET,
        append: false,
        popup: false,
        content,
      }, '*')
    }

    // Whether a detail is on screen right now, so a close control on a
    // blank panel does not send a parameter write.
    let open = false

    function onMessage(ev: MessageEvent) {
      const msg = ev.data
      if (!msg || typeof msg !== 'object') return

      // The deliberate way out. The other way is the server taking it down
      // when the batch it belongs to is replaced -- that needs no gesture,
      // and scrolling within a batch is not one: the provider is still in
      // the list being presented, just further down it.
      if (msg.type === 'router:action' && msg.action === 'provider:detail-close') {
        if (!open) return
        open = false
        window.parent.postMessage({
          type: 'router:makeCall',
          op: 'provider_detail_close',
          payload: {},
          call_id: 'pdc-' + Date.now(),
        }, '*')
        return
      }

      // The server decides what closing means and says so; the panel
      // clears when told, not when clicked.
      if (msg.type === 'router:event-broadcast' &&
          msg.kind === 'provider_detail_close') {
        open = false
        postRender(BLANK)
        return
      }

      // A new question is a new page in view, and the record this panel
      // holds belongs to the one being left. It goes with it. Not gated on
      // this widget believing a detail is open: the flag tracks the panel
      // this widget last painted, and a detail opened from the facility
      // list on an earlier turn outlived it.
      if (msg.type === 'router:event-broadcast' &&
          msg.kind === 'intent_classified') {
        open = false
        postRender(BLANK)
        return
      }

      // The facility list's link. Same panel, opened for the facility
      // page, so the server projects the identity that page's records
      // carry and applies the registry check for an organization.
      if (msg.type === 'router:action' && msg.action === 'facility:detail') {
        const npi = String((msg.data || {}).npi || '').trim()
        if (!npi) return
        open = true
        postRender(buildLoadingHtml(npi))
        window.parent.postMessage({
          type: 'router:makeCall', op: 'provider-detail',
          payload: { npi, entity_type: '2' },
          call_id: 'fd-' + Date.now(),
        }, '*')
        return
      }
      if (msg.type === 'router:action' && msg.action === 'provider:detail') {
        const npi = String((msg.data || {}).npi || '').trim()
        if (!npi) return
        open = true
        postRender(buildLoadingHtml(npi))
        // One record, named once. The card's name, specialty, address,
        // phone and state used to travel with the click, so the same
        // provider showed differently depending on which search painted
        // the row -- and a row painted with less produced a detail with
        // less. Everything on this panel comes from the record the NPI
        // names, which is the only thing the detail needs.
        window.parent.postMessage({
          type: 'router:makeCall',
          op: 'provider-detail',
          payload: { npi, entity_type: '1' },
          call_id: 'pd-' + Date.now(),
        }, '*')
        return
      }
      // The paint is what makes it open, not the click -- a restore paints
      // one nobody clicked, and it has to be closable the same way.
      if (msg.type === 'router:event-broadcast' && msg.kind === 'provider-detail') {
        open = true
        postRender(buildDetailHtml(msg.data || {}))
      }
    }
    window.addEventListener('message', onMessage)
    return () => window.removeEventListener('message', onMessage)
  }, [])
  return null
}
