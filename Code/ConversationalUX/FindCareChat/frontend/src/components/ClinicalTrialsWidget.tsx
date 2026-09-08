// Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
// Licensed under the FindCare Evaluation License (FEL-1.0).
//
// Clinical trials React widget — POC of the new client-side architecture.
// Tool produces structured Pydantic data. This widget consumes the
// structured data, builds HTML for the Main Window (trial detail),
// Left Panel (bullet list + pagination), and Right Panel (section
// jump list), and hands each HTML fragment to the wrapper's
// ClientRouter via router:render postMessage.

import { useEffect, useRef } from 'react'

function esc(s: any): string {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;')
}

function _slug(s: string): string {
  return String(s || '').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '')
}

function firstSentence(text: string): string {
  const s = String(text || '').trim()
  if (!s) return ''
  const m = s.match(/^[\s\S]*?[.!?](?=\s|$)/)
  if (!m) return s
  return m[0].length < s.length ? m[0] + '...' : m[0]
}


function ageRange(trial: any): string {
  const a = (trial.minimum_age || '').trim()
  const b = (trial.maximum_age || '').trim()
  if (!a && !b) return '—'
  if (a && b) return `${a} – ${b}`
  return a || b
}

function buildLeftPanel(
  trials: any[],
  selectedIdx: number,
  hasPrev: boolean,
  hasMore: boolean,
  pageStart: number,
  totalEligible: number | null,
  searchContext: any,
  isPartial: boolean = false,
): string {
  const ctx = searchContext || {}
  // The criteria, said back in the tool's words. Composing them here meant
  // this panel decided what 'us' reads as and which criteria were worth
  // repeating, both of which are about the search, not about the panel.
  const queryStr = String(ctx.criteria_summary || '')
  const end = pageStart + trials.length - 1
  // Quantity-suffix gating: render "showing X to Y of Z" (or "of many"
  // when the tool capped its pre-filter fetch) ONLY after the final
  // chunk has landed. While the cache is still filling, totalEligible
  // is null and we omit the suffix entirely.
  let countStr = ''
  if (typeof totalEligible === 'number' && totalEligible > 0) {
    const totalLabel = isPartial ? 'many' : String(totalEligible)
    countStr = ` — showing ${pageStart} to ${end} of ${totalLabel}`
  }
  const header = queryStr
    ? `Clinical trials — ${esc(queryStr)}${esc(countStr)}`
    : `Clinical trials${esc(countStr)}`

  // Whole-card click target: the <li> itself carries data-router-action so
  // ClientRouter._bindActions binds a click handler that fires
  // 'trial:select' regardless of WHERE inside the card the user clicks
  // (NCT id, title, summary, conditions line, etc.). Without this, only
  // the small NCT id span was clickable.
  const items = trials.map((t: any, i: number) => {
    const nct = esc(t.nct_id || '(no id)')
    const title = esc(t.brief_title || '')
    const summary = esc(firstSentence(t.brief_summary || ''))
    const conds = esc((t.conditions || []).join(', ') || '—')
    const ages = esc(ageRange(t))
    const sex = esc(t.sex || '—')
    const selected = i === selectedIdx ? 'background:#f0fffe;' : ''
    return `
      <li data-router-action="trial:select" data-trial-idx="${i}"
          style="margin-bottom:0.75em;padding:0.4em;${selected}border-radius:0.3em;cursor:pointer;">
        <div draggable="true" data-drag-payload="${nct}" style="cursor:grab;">
          <span style="color:#0b7a75;font-weight:700;text-decoration:underline;">${nct}</span>
          <div style="font-size:0.9em;color:#374151;margin:0.15em 0 0.25em 0;">${title}</div>
        </div>
        ${summary ? `<div style="font-size:0.85em;color:#4b5563;font-style:italic;margin:0.15em 0 0.35em 0;">${summary}</div>` : ''}
        <ul style="margin:0.25em 0 0 1em;padding:0;font-size:0.9em;color:#4b5563;">
          <li><strong>Conditions:</strong> ${conds}</li>
          <li><strong>Age range:</strong> ${ages}</li>
          <li><strong>Sex:</strong> ${sex}</li>
        </ul>
      </li>`
  }).join('')
  const moreLink = hasMore
    ? `<a href="#" data-router-action="trial:page" data-direction="next"
          style="color:#0b7a75;text-decoration:underline;font-weight:700;cursor:pointer;">more trials</a>`
    : ''
  const backLink = hasPrev
    ? `<a href="#" data-router-action="trial:page" data-direction="prev"
          style="color:#0b7a75;text-decoration:underline;font-weight:700;cursor:pointer;">back</a>`
    : ''
  const ctrlRow = (moreLink || backLink)
    ? `<div style="margin-top:0.75em;font-size:0.95em;color:#374151;">${backLink}${(moreLink && backLink) ? ' &nbsp;|&nbsp; ' : ''}${moreLink}</div>`
    : ''

  return `
    <div style="padding:1em;box-sizing:border-box;">
      <div style="font-size:1em;font-weight:700;color:#0b7a75;text-transform:uppercase;margin-bottom:0.5em;">${header}</div>
      <ul style="list-style:disc;padding-left:1.25em;margin:0;">${items}</ul>
      ${ctrlRow}
    </div>`
}

function sectionTitle(title: string): string {
  const sid = 'ct-sec-' + _slug(title)
  return `<h3 id="${sid}" style="font-size:1em;color:#0b7a75;font-weight:700;border-bottom:0.125em solid #d8e2e1;margin:1em 0 0.5em;padding-bottom:0.25em;">${esc(title)}</h3>`
}

function buildTrialDetailInner(trial: any): string {
  if (!trial) return '<div style="padding:1em;color:#6b7280;">No trial selected.</div>'
  const di = trial.design_info || {}
  const ipd = trial.ipd_sharing || {}
  const yn = (v: any) => (v ? 'yes' : 'no')
  const inlineKV = (label: string, value: any) =>
    value ? `<div style="margin:0.4em 0;"><strong>${esc(label)}:</strong> ${esc(value)}</div>` : ''
  const facts = `
    <table style="width:100%;border-collapse:collapse;font-size:0.95em;margin-bottom:0.5em;"><tbody>
      <tr><td style="background:#f9fafb;color:#6b7280;font-weight:600;border:1px solid #e5e7eb;padding:0.25em 0.5em;">NCT ID</td>
          <td style="padding:0.25em 0.5em;border:1px solid #e5e7eb;color:#374151;">${esc(trial.nct_id)}</td>
          <td style="background:#f9fafb;color:#6b7280;font-weight:600;border:1px solid #e5e7eb;padding:0.25em 0.5em;">Status</td>
          <td style="padding:0.25em 0.5em;border:1px solid #e5e7eb;color:#374151;">${esc(trial.overall_status)}</td></tr>
      <tr><td style="background:#f9fafb;color:#6b7280;font-weight:600;border:1px solid #e5e7eb;padding:0.25em 0.5em;">Phase</td>
          <td style="padding:0.25em 0.5em;border:1px solid #e5e7eb;color:#374151;">${esc((trial.phases || []).join(', ') || '—')}</td>
          <td style="background:#f9fafb;color:#6b7280;font-weight:600;border:1px solid #e5e7eb;padding:0.25em 0.5em;">Sponsor</td>
          <td style="padding:0.25em 0.5em;border:1px solid #e5e7eb;color:#374151;">${esc(trial.lead_sponsor_name)}</td></tr>
      <tr><td style="background:#f9fafb;color:#6b7280;font-weight:600;border:1px solid #e5e7eb;padding:0.25em 0.5em;">Sex</td>
          <td style="padding:0.25em 0.5em;border:1px solid #e5e7eb;color:#374151;">${esc(trial.sex || '—')}</td>
          <td style="background:#f9fafb;color:#6b7280;font-weight:600;border:1px solid #e5e7eb;padding:0.25em 0.5em;">Healthy vol.</td>
          <td style="padding:0.25em 0.5em;border:1px solid #e5e7eb;color:#374151;">${yn(trial.healthy_volunteers)}</td></tr>
      <tr><td style="background:#f9fafb;color:#6b7280;font-weight:600;border:1px solid #e5e7eb;padding:0.25em 0.5em;">Min age</td>
          <td style="padding:0.25em 0.5em;border:1px solid #e5e7eb;color:#374151;">${esc(trial.minimum_age || '—')}</td>
          <td style="background:#f9fafb;color:#6b7280;font-weight:600;border:1px solid #e5e7eb;padding:0.25em 0.5em;">Max age</td>
          <td style="padding:0.25em 0.5em;border:1px solid #e5e7eb;color:#374151;">${esc(trial.maximum_age || '—')}</td></tr>
    </tbody></table>
  `
  const sections: string[] = []
  // Top row: Choose-for-evaluation button. Mirrors the provider-detail
  // "↓ Select" pattern — one click adds this trial to the user's
  // selected_clinical_trials list on user_object. The strip below picks
  // it up via the trial_selection_changed broadcast.
  const nctId = esc(trial.nct_id || '')
  sections.push(
    `<div style="display:flex;justify-content:flex-end;margin-bottom:0.5em;">` +
      `<button data-router-action="trial:select-click" data-nct-id="${nctId}" ` +
      `style="background:#fff;border:0.0625em solid #0b7a75;color:#0b7a75;padding:0.4em 0.9em;` +
      `border-radius:0.375em;cursor:pointer;font-size:0.9em;font-weight:600;">↓ Choose for evaluation</button>` +
    `</div>`
  )
  // ct-record-top: the anchor the right-panel jump list AND the trial:select
  // handler use to scroll the record to its actual top (the trial title)
  // rather than to the first section header partway down the record.
  sections.push(`<h2 id="ct-record-top" style="margin:0 0 0.5em 0;color:#0b7a75;font-size:1.25em;">${esc(trial.brief_title || '')}</h2>`)
  if (trial.official_title && trial.official_title !== trial.brief_title) {
    sections.push(`<div style="font-style:italic;color:#6b7280;margin-bottom:0.5em;">${esc(trial.official_title)}</div>`)
  }
  sections.push(facts)
  if (trial.brief_summary) {
    sections.push(sectionTitle('Brief summary'))
    sections.push(`<div style="white-space:pre-wrap;color:#374151;margin-bottom:0.5em;">${esc(trial.brief_summary)}</div>`)
  }
  if (trial.detailed_description) {
    sections.push(sectionTitle('Detailed description'))
    sections.push(`<div style="white-space:pre-wrap;color:#374151;margin-bottom:0.5em;">${esc(trial.detailed_description)}</div>`)
  }
  if ((trial.conditions || []).length) {
    sections.push(sectionTitle('Conditions'))
    sections.push(`<div style="color:#374151;margin-bottom:0.5em;">${esc((trial.conditions || []).join(', '))}</div>`)
  }
  if (trial.eligibility_criteria) {
    sections.push(sectionTitle('Eligibility'))
    sections.push(`<div style="white-space:pre-wrap;color:#374151;margin-bottom:0.5em;">${esc(trial.eligibility_criteria)}</div>`)
  }
  if ((trial.locations || []).length) {
    sections.push(sectionTitle(`Sites (${trial.locations.length})`))
    const siteRows = (trial.locations || []).map((l: any) => {
      const where = [l.facility, [l.city, l.state, l.zip].filter(Boolean).join(', '), l.country].filter(Boolean).join(' — ')
      const status = l.status ? ` <em>(${esc(l.status)})</em>` : ''
      return `<li>${esc(where)}${status}</li>`
    }).join('')
    sections.push(`<ul style="margin:0.25em 0 0.5em 1.25em;color:#374151;">${siteRows}</ul>`)
  }
  if (trial.study_url) {
    sections.push(`<div style="margin-top:1em;"><a href="${esc(trial.study_url)}" target="_blank" rel="noopener noreferrer" style="color:#0b7a75;font-weight:600;">View on ClinicalTrials.gov →</a></div>`)
  }
  return `<div style="max-width:70em;margin:0 auto;padding:1em;background:#fff;border:0.125em solid #e5e7eb;border-radius:0.5em;font-size:1em;line-height:1.6;color:#1f2937;">${sections.join('')}</div>`
}

// Scaffold the widget paints into MainWindow (replace). Two named regions:
// trial_detail_area (filled with the trial-detail HTML from
// buildTrialDetailInner) and selected_trials_strip (SelectedClinicalTrials
// Widget merges into this — a subsequent trial:select never touches the
// strip because we only re-merge trial_detail_area, not re-render the
// whole MainWindow).
function buildScaffoldHtml(): string {
  return (
    `<div style="display:flex;flex-direction:column;height:100%;min-height:0;">` +
      `<div id="trial_detail_area" style="flex:1;min-height:0;overflow:auto;"></div>` +
      `<div id="selected_trials_strip" style="flex-shrink:0;"></div>` +
    `</div>`
  )
}

function buildRightPanel(trial: any): string {
  if (!trial) return ''
  // Overview is the first jump target and points at the record-top anchor
  // (the trial title), so clicking it scrolls to the top of the record
  // rather than to the first section header partway down the record.
  const jumpTargets: Array<{label: string, sid: string}> = [
    {label: 'Overview', sid: 'ct-record-top'},
  ]
  if (trial.brief_summary)         jumpTargets.push({label: 'Brief summary',        sid: 'ct-sec-' + _slug('Brief summary')})
  if (trial.detailed_description)  jumpTargets.push({label: 'Detailed description', sid: 'ct-sec-' + _slug('Detailed description')})
  if ((trial.conditions || []).length) jumpTargets.push({label: 'Conditions', sid: 'ct-sec-' + _slug('Conditions')})
  if (trial.eligibility_criteria)  jumpTargets.push({label: 'Eligibility',          sid: 'ct-sec-' + _slug('Eligibility')})
  if ((trial.locations || []).length) {
    const sitesLabel = `Sites (${trial.locations.length})`
    jumpTargets.push({label: sitesLabel, sid: 'ct-sec-' + _slug(sitesLabel)})
  }
  if (jumpTargets.length <= 1) return ''  // Overview-only is not worth showing.
  const items = jumpTargets.map(({label, sid}) =>
    `<li style="margin:0.25em 0;"><a href="#" data-router-action="trial:scroll" data-anchor="${esc(sid)}" style="color:#0b7a75;text-decoration:underline;cursor:pointer;">${esc(label)}</a></li>`
  ).join('')
  return `
    <div style="padding:1em;box-sizing:border-box;">
      <div style="font-size:1em;font-weight:700;color:#0b7a75;text-transform:uppercase;margin-bottom:0.5em;">Trial sections</div>
      <ul style="list-style:none;padding:0;margin:0;font-size:0.9em;">${items}</ul>
    </div>`
}

export default function ClinicalTrialsWidget() {
  // Widget produces no JSX (returns null) — every visible surface is
  // painted via postMessage to the parent. So every "state" value below
  // is a ref, not useState: nothing triggers a React re-render, so
  // useState offers no benefit and its dep-array plumbing would leak
  // subscriptions on every change (fixed here — the effect subscribes
  // once at mount and reads live values from these refs).
  const cacheRef = useRef<any[]>([])
  const trialsRef = useRef<any[]>([])
  const selectedIdxRef = useRef<number>(0)
  const totalEligibleRef = useRef<number | null>(null)
  const isPartialRef = useRef<boolean>(false)
  const pageSizeRef = useRef<number>(10)
  const pageStartRef = useRef<number>(1)
  const lastQueryRef = useRef<any>(null)
  const lastPageDirectionRef = useRef<'next' | 'prev' | null>(null)

  useEffect(() => {
    function postRender(target: string, content: string) {
      window.parent.postMessage({
        type: 'router:render',
        target: target,
        append: false,
        popup: false,
        content: content,
      }, '*')
    }
    function postSubscribe(kind: string) {
      window.parent.postMessage({ type: 'router:subscribe-broadcast', kind: kind }, '*')
    }
    function postMergeTrialDetail(content: string) {
      window.parent.postMessage({
        type: 'router:merge',
        target: 'MainWindow',
        region: 'trial_detail_area',
        content,
      }, '*')
    }

    postSubscribe('clinical_trials_chunk')
    postSubscribe('intent_classified')

    // What the person is told is decided where the failure happened. The
    // mode names what was being attempted; the widget selects its wording
    // from the mode and invents no text of its own.
    const FAILURE_WORDING: Record<string, string> = {
      clinical_trials_list_not_produced:
        'The list of clinical trials for this condition could not be produced.',
      clinical_trials_list_not_extended:
        'The list of clinical trials could not be extended past what is shown.',
    }

    function renderFailure(mode: string) {
      const wording = FAILURE_WORDING[mode]
      if (!wording) return
      postRender('LeftPanel',
        `<div data-testid="clinical-trials-failure" style="padding:1em;color:#b45309;">${esc(wording)}</div>`)
    }

    function renderSlice(start: number) {
      // start is 1-based pageStart per the existing UX. Convert to a 0-based
      // cache offset, slice page_size items, and paint the three panels.
      const offset = Math.max(0, start - 1)
      const slice = cacheRef.current.slice(offset, offset + pageSizeRef.current)
      const hasPrev = start > 1
      const hasMore = offset + slice.length < cacheRef.current.length
      const ctx = lastQueryRef.current || {}
      postRender('LeftPanel', buildLeftPanel(
        slice, 0, hasPrev, hasMore, start, totalEligibleRef.current, ctx,
        isPartialRef.current,
      ))
      // Paint the scaffold (two named regions) then merge the trial
      // detail into #trial_detail_area. Subsequent trial:select calls
      // merge into the same region, leaving #selected_trials_strip
      // undisturbed so SelectedClinicalTrialsWidget's paint survives.
      postRender('MainWindow', buildScaffoldHtml())
      postMergeTrialDetail(buildTrialDetailInner(slice[0] || null))
      postRender('RightPanel', buildRightPanel(slice[0] || null))
      trialsRef.current = slice
      selectedIdxRef.current = 0
    }

    function applyChunk(data: any) {
      const incoming = Array.isArray(data.trials) ? data.trials : []
      const chunkIndex: number = typeof data.chunk_index === 'number' ? data.chunk_index : 0
      const isFinal: boolean = !!data.is_final

      if (chunkIndex === 0) {
        cacheRef.current = incoming.slice()
        const ctx = data.search_context || lastQueryRef.current || {}
        lastQueryRef.current = {
          condition: ctx.condition || '',
          age_years: ctx.age_years ?? null,
          sex: ctx.sex || null,
          geographic_scope: ctx.geographic_scope || null,
        }
        // page_size locks to the first chunk's length so subsequent
        // pagination math stays consistent with what the tool sent.
        pageSizeRef.current = incoming.length || 5
        pageStartRef.current = 1
        renderSlice(1)
      } else {
        // Subsequent chunk - append to the cache silently. No repaint
        // unless the user has already paged past the existing cache.
        cacheRef.current = cacheRef.current.concat(incoming)
      }

      if (isFinal) {
        const te = typeof data.total_eligible === 'number' ? data.total_eligible : cacheRef.current.length
        totalEligibleRef.current = te
        isPartialRef.current = !!data.is_partial
        // Repaint the left panel so the "of N" / "of many" suffix
        // appears for the first time. Center+Right panel keep their
        // current rendering.
        const ps = pageStartRef.current || 1
        const offset = Math.max(0, ps - 1)
        const slice = cacheRef.current.slice(offset, offset + (pageSizeRef.current || 5))
        const hasPrev = ps > 1
        const hasMore = offset + slice.length < cacheRef.current.length
        postRender('LeftPanel', buildLeftPanel(
          slice, selectedIdxRef.current, hasPrev, hasMore, ps, te,
          lastQueryRef.current || {}, !!data.is_partial,
        ))
      }
    }

    // Loading UX (white-out + prompt-row timer + MainWindow timer) is
    // owned by NewQueryLoadingWidget. This widget only paints trial
    // content when clinical_trials_chunk arrives.

    function onMessage(ev: MessageEvent) {
      const msg = ev.data
      if (!msg || typeof msg !== 'object') return

      if (msg.type === 'router:error') {
        renderFailure(String(msg.mode || ''))
        return
      }

      if (msg.type === 'router:event-broadcast') {
        if (msg.kind === 'clinical_trials_chunk') {
          applyChunk(msg.data || {})
        }
        if (msg.kind === 'intent_classified') {
          const d = msg.data || {}
          if (d.action === 'findClinicalTrials') {
            lastQueryRef.current = {
              condition: d.condition || '',
              age_years: d.age_years ?? null,
              sex: d.sex || null,
              geographic_scope: d.geographic_scope || null,
            }
          }
        }
      }

      if (msg.type === 'router:action') {
        if (msg.action === 'trial:select') {
          const idx = parseInt(String((msg.data || {}).trial_idx || '0'), 10)
          const t = trialsRef.current[idx]
          if (!t) return
          selectedIdxRef.current = idx
          // Merge the new trial detail into #trial_detail_area only. The
          // scaffold and #selected_trials_strip are untouched so the
          // "Selected for Evaluation" strip persists across selections.
          postMergeTrialDetail(buildTrialDetailInner(t))
          postRender('RightPanel', buildRightPanel(t))
          // Move the selection highlight in the LEFT panel by toggling a
          // background style on the target <li> directly. A single DOM
          // mutation via router:exec is instant and does not re-parse the
          // list; repainting the whole list to change one row was the
          // left-panel flicker source.
          window.parent.postMessage({
            type: 'router:exec',
            code: `(function(){
              var all = document.querySelectorAll('li[data-router-action="trial:select"]');
              for (var i = 0; i < all.length; i++) { all[i].style.background = ''; }
              var sel = document.querySelector('li[data-router-action="trial:select"][data-trial-idx="${idx}"]');
              if (sel) sel.style.background = '#f0fffe';
              var top = document.getElementById('ct-record-top');
              if (top && top.scrollIntoView) top.scrollIntoView({behavior:'auto', block:'start'});
            })();`,
          }, '*')
        }
        if (msg.action === 'trial:scroll') {
          const anchorId = String((msg.data || {}).anchor || '')
          if (anchorId) {
            window.parent.postMessage({
              type: 'router:exec',
              code: `var a = document.getElementById('${anchorId}'); if (a && a.scrollIntoView) a.scrollIntoView({behavior:'smooth', block:'start'});`,
            }, '*')
          }
        }
        if (msg.action === 'trial:page') {
          // Pagination is client-side state slicing over the cache
          // populated by streamed chunks - no server round-trip. If the
          // user races the cache fill (clicks More before the next chunk
          // arrives) the widget still has the current page rendered; the
          // next render after the chunk lands will catch up.
          const dir = String((msg.data || {}).direction || '')
          const ps = pageStartRef.current
          const psize = pageSizeRef.current
          let newPageStart = ps
          if (dir === 'next') {
            const candidate = ps + (trialsRef.current.length || psize)
            if (candidate - 1 >= cacheRef.current.length) return
            newPageStart = candidate
          } else if (dir === 'prev') {
            newPageStart = Math.max(1, ps - psize)
          } else {
            return
          }
          lastPageDirectionRef.current = dir === 'next' ? 'next' : 'prev'
          pageStartRef.current = newPageStart
          renderSlice(newPageStart)
        }
      }

    }

    window.addEventListener('message', onMessage)
    return () => {
      window.removeEventListener('message', onMessage)
    }
  }, [])

  return null
}
