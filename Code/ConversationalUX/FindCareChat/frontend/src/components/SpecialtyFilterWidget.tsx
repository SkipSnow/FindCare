// Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
// Licensed under the FindCare Evaluation License (FEL-1.0).
//
// SpecialtyFilterWidget — owns frame_LeftPanel.
//
// On kind:'specialties' broadcast: paints the full filter panel into
// frame_LeftPanel via router:render. Layout, colors, and control surface
// ported verbatim from FindCare/SpecialtyFilter/SpecialtyFilter.tsx
// (lines 87-435) — same TEAL palette, same three-row header table
// (title / counts / controls), same scrollable rows, same Apply button.
//
// State (specialties + checked map + pristine baseline for dirty detection)
// lives in widget closure. Each interactive control carries a
// data-router-action attribute; ClientRouter._bindActions binds clicks
// and posts router:action back. Widget mutates state and re-renders.

import { useEffect, useRef } from 'react'

const TARGET = 'LeftPanel'

const TEAL = '#0b7a75'
const TEAL_LIGHT_BG = '#e6f5ec'
const TEAL_LIGHT_BORDER = '#c9e0d3'
const ROW_DIVIDER = '#f0f0f0'

interface SpecialtyGroups {
  all_codes: string[]
  prescriber_codes: string[]
  homeopathic_codes: string[]
  default_selected_codes: string[]
}

interface Specialty {
  code: string
  name: string
  can_prescribe?: boolean
  homeopathic?: boolean
  homeopathic_general?: boolean
}

function _esc(s: any): string {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;')
}

function buildFilterHtml(
  specs: Specialty[],
  checked: Record<string, boolean>,
  isDirty: boolean,
  groups: SpecialtyGroups,
): string {
  // Which codes make up a set is the tool's answer, carried here. This
  // panel groups by nothing and classifies nothing: it ticks the codes it
  // was handed and counts what is ticked.
  const allCodes         = groups.all_codes
  const prescriberCodes  = groups.prescriber_codes
  const homeopathicCodes = groups.homeopathic_codes
  const allPossible    = allCodes.length
  const allPrescribers = prescriberCodes.length
  const yourChoices    = allCodes.filter(c => checked[c]).length

  const prescribersChecked  = prescriberCodes.length > 0 &&
    prescriberCodes.every(c => checked[c])
  const homeopathicChecked  = homeopathicCodes.length > 0 &&
    homeopathicCodes.every(c => checked[c])

  // Label per prod: "Check All" when not every row is checked (clicking
  // checks the remainder); "Uncheck All" only when every row is checked.
  // Disabled when nothing is checked (no Uncheck target).
  const allChecked = allCodes.length > 0 && allCodes.every(c => checked[c])
  const anyChecked = yourChoices > 0
  const labelIsCheckAll = !allChecked
  const toggleAllLabel = labelIsCheckAll ? 'Check All' : 'Uncheck All'
  const toggleAllDisabled = !anyChecked && !labelIsCheckAll

  const countCell = (testid: string, label: string, value: number, color: string, i: number) => `
    <td data-testid="${testid}"
        style="padding:0.35em 0.3em;text-align:center;vertical-align:middle;width:33.333%;
               border-left:${i === 0 ? '0.5em' : '0.25em'} solid ${TEAL_LIGHT_BG};
               border-right:${i === 2 ? '0.5em' : '0.25em'} solid ${TEAL_LIGHT_BG};">
      <div style="background:#ffffff;border:0.125em solid ${TEAL_LIGHT_BORDER};border-radius:0.4em;padding:0.35em 0.2em;">
        <div style="font-size:0.9em;color:#4a5568;text-transform:uppercase;letter-spacing:0.02em;line-height:1.1;word-spacing:100vw;">${_esc(label)}</div>
        <div style="font-size:1.1em;font-weight:700;color:${color};line-height:1.1;margin-top:0.2em;">${value}</div>
      </div>
    </td>`

  const rows = specs.map(s => {
    const c = _esc(s.code || '')
    const isChecked = !!checked[s.code]
    return `
      <tr data-router-action="filter:toggle-row" data-code="${c}"
          data-spec-code="${c}"
          style="cursor:pointer;">
        <td style="padding:0.1em 0.8em;border-bottom:0.125em solid ${ROW_DIVIDER};color:#1f2937;font-size:0.8em;line-height:1.15;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">
          ${_esc(s.name || s.code || '')}
        </td>
        <td style="padding:0.1em 0.8em;border-bottom:0.125em solid ${ROW_DIVIDER};text-align:right;width:2.4em;">
          <input type="checkbox" ${isChecked ? 'checked' : ''} readonly tabindex="-1"
                 style="width:1.2em;height:1.2em;accent-color:${TEAL};margin:0;pointer-events:none;" />
        </td>
      </tr>`
  }).join('')

  const applyBtnBg = isDirty
    ? `linear-gradient(180deg, #0b9a94, ${TEAL})`
    : '#e5e7eb'
  const applyBtnColor  = isDirty ? '#fff' : '#6b7280'
  const applyBtnBorder = isDirty ? 'none' : '0.125em solid #cbd5d5'
  const applyBtnCursor = isDirty ? 'pointer' : 'not-allowed'
  const applyDisabled  = isDirty ? '' : 'disabled'

  return `
    <div style="display:flex;flex-direction:column;height:100%;width:100%;background:#fff;box-sizing:border-box;">
      <table style="width:100%;border-collapse:separate;border-spacing:0;table-layout:fixed;background:${TEAL_LIGHT_BG};border-bottom:0.25em solid ${TEAL};">
        <tbody>
          <tr>
            <td colspan="3" style="padding:0.6em 0.8em;">
              <span style="font-size:1.2em;font-weight:700;color:${TEAL};">Choose Specialties</span>
            </td>
          </tr>
          <tr>
            ${countCell('count-all-possible',    'All possible',    allPossible,    '#1f2937', 0)}
            ${countCell('count-all-prescribers', 'All prescribers', allPrescribers, '#1f2937', 1)}
            ${countCell('count-your-choices',    'Your choices',    yourChoices,    TEAL,      2)}
          </tr>
          <tr>
            <td style="padding:0.5em 0.4em 0.6em 0.8em;vertical-align:middle;width:33.333%;">
              <button type="button" data-router-action="filter:toggle-all" data-testid="toggle-all-button"
                      ${toggleAllDisabled ? 'disabled' : ''}
                      style="width:100%;background:#ffffff;border:0.125em solid ${TEAL};border-radius:0.4em;font-size:0.9em;font-weight:700;color:${TEAL};cursor:${toggleAllDisabled ? 'not-allowed' : 'pointer'};padding:0.5em 0.4em;opacity:${toggleAllDisabled ? 0.45 : 1};">
                ${toggleAllLabel}
              </button>
            </td>
            <td style="padding:0.5em 0.4em 0.6em 0.4em;vertical-align:middle;width:33.333%;">
              <label data-router-action="filter:macro-prescribers"
                     style="display:flex;align-items:center;gap:0.4em;color:#1f2937;cursor:pointer;user-select:none;font-size:0.9em;">
                <input type="checkbox" ${prescribersChecked ? 'checked' : ''} data-testid="macro-prescribers"
                       style="width:1.2em;height:1.2em;accent-color:${TEAL};margin:0;pointer-events:none;" />
                Prescribers
              </label>
            </td>
            <td style="padding:0.5em 0.8em 0.6em 0.4em;vertical-align:middle;width:33.333%;">
              <label data-router-action="filter:macro-homeopathic"
                     style="display:flex;align-items:center;gap:0.4em;color:#1f2937;cursor:pointer;user-select:none;font-size:0.9em;">
                <input type="checkbox" ${homeopathicChecked ? 'checked' : ''} data-testid="macro-homeopathic"
                       style="width:1.2em;height:1.2em;accent-color:${TEAL};margin:0;pointer-events:none;" />
                Homeopathic
              </label>
            </td>
          </tr>
        </tbody>
      </table>

      <div data-testid="specialty-list"
           style="flex:1 1 auto;min-height:0;overflow-y:auto;overflow-x:hidden;">
        <table style="width:100%;border-collapse:separate;border-spacing:0;table-layout:fixed;">
          <tbody>${rows}</tbody>
        </table>
      </div>

      <div style="flex:0 0 auto;padding:0.5em 0.8em;border-top:0.25em solid ${TEAL};box-sizing:border-box;">
        <button type="button" data-router-action="filter:apply" data-testid="apply-filter-button"
                ${applyDisabled}
                style="width:100%;padding:0.4em 0.6em;border-radius:0.5em;border:${applyBtnBorder};
                       background:${applyBtnBg};color:${applyBtnColor};font-size:1em;font-weight:700;
                       cursor:${applyBtnCursor};min-height:2.4em;">
          Apply Filter
        </button>
      </div>
    </div>
  `
}

export default function SpecialtyFilterWidget() {
  // Refs, matching ClinicalTrialsWidget. This widget produces no JSX and
  // paints by postMessage, so nothing triggers a re-render: useState buys
  // nothing and its dep-array plumbing leaks subscriptions. One mechanism
  // for cached view state across the widgets.
  //
  // This is a CACHE, not the record. It exists so a tick repaints without a
  // round-trip. The applied selection is recorded server-side on the intent
  // when Apply fires; losing these on remount costs a redraw, not a fact.
  const specialtiesRef = useRef<Specialty[]>([])
  // What the tool said the sets are. Held so every gesture answers from
  // the same lists the panel was painted with.
  const groupsRef = useRef<SpecialtyGroups>({
    all_codes: [], prescriber_codes: [],
    homeopathic_codes: [], default_selected_codes: [],
  })
  const checkedRef = useRef<Record<string, boolean>>({})
  const pristineRef = useRef<Record<string, boolean>>({})

  useEffect(() => {

    function isDirty(): boolean {
      const keys = new Set([...Object.keys(checkedRef.current), ...Object.keys(pristineRef.current)])
      for (const k of keys) {
        if (!!checkedRef.current[k] !== !!pristineRef.current[k]) return true
      }
      return false
    }

    function repaint() {
      window.parent.postMessage({
        type: 'router:render',
        target: TARGET,
        append: false,
        popup: false,
        content: buildFilterHtml(specialtiesRef.current, checkedRef.current,
                                 isDirty(), groupsRef.current),
      }, '*')
    }

    function toggleCodes(codes: string[], desired: boolean) {
      const next = { ...checkedRef.current }
      for (const c of codes) next[c] = desired
      checkedRef.current = next
    }

    window.parent.postMessage({
      type: 'router:subscribe-broadcast',
      kind: 'specialties',
    }, '*')

    function onMessage(ev: MessageEvent) {
      const msg = ev.data
      if (!msg || typeof msg !== 'object') return

      if (msg.type === 'router:event-broadcast' && msg.kind === 'specialties') {
        const data = msg.data || {}
        specialtiesRef.current = Array.isArray(data.specialties) ? data.specialties as Specialty[] : []
        checkedRef.current  = {}
        pristineRef.current = {}
        // A restore carries the ticks the user left; a fresh panel has
        // none and falls back to the prescriber default. Without this a
        // return from EvaluateCare repainted the panel with the default
        // selection and silently discarded what they had chosen.
        groupsRef.current = {
          all_codes: Array.isArray(data.all_codes) ? data.all_codes : [],
          prescriber_codes: Array.isArray(data.prescriber_codes) ? data.prescriber_codes : [],
          homeopathic_codes: Array.isArray(data.homeopathic_codes) ? data.homeopathic_codes : [],
          default_selected_codes: Array.isArray(data.default_selected_codes)
            ? data.default_selected_codes : [],
        }
        // A restore carries the ticks the person left; a fresh panel
        // carries none and takes the tool's default. Which codes that is
        // was decided where the search rule lives, so the two cannot
        // disagree about what a fresh panel searches under.
        const restored: string[] = Array.isArray(data.selected_codes)
          ? data.selected_codes : []
        const seedCodes = restored.length > 0
          ? restored : groupsRef.current.default_selected_codes
        const ticked = new Set(seedCodes)
        for (const code of groupsRef.current.all_codes) {
          const seed = ticked.has(code)
          checkedRef.current[code]  = seed
          pristineRef.current[code] = seed
        }
        repaint()
        return
      }

      if (msg.type !== 'router:action') return

      if (msg.action === 'filter:toggle-row') {
        const code = String((msg.data && msg.data.code) || '')
        if (!code) return
        checkedRef.current = { ...checkedRef.current, [code]: !checkedRef.current[code] }
        repaint()
        return
      }

      if (msg.action === 'filter:toggle-all') {
        const everyCode = groupsRef.current.all_codes
        const allChecked = everyCode.length > 0 && everyCode.every(c => checkedRef.current[c])
        toggleCodes(everyCode, !allChecked)
        repaint()
        return
      }

      if (msg.action === 'filter:macro-prescribers') {
        const codes = groupsRef.current.prescriber_codes
        const allOn = codes.length > 0 && codes.every(c => checkedRef.current[c])
        toggleCodes(codes, !allOn)
        repaint()
        return
      }

      if (msg.action === 'filter:macro-homeopathic') {
        const codes = groupsRef.current.homeopathic_codes
        const allOn = codes.length > 0 && codes.every(c => checkedRef.current[c])
        toggleCodes(codes, !allOn)
        repaint()
        return
      }

      if (msg.action === 'filter:apply') {
        const chosen = groupsRef.current.all_codes.filter(c => checkedRef.current[c])
        window.parent.postMessage({
          type: 'router:makeCall',
          op: 'apply_filter',
          // Only the selection. The panel is not sent back: the server
          // holds it on the intent and reuses it because the query has not
          // changed. Sending it would be a second copy of one fact, and the
          // two would drift.
          payload: { selected_codes: chosen },
          call_id: 'filter-apply-' + Date.now(),
        }, '*')
        pristineRef.current = { ...checkedRef.current }
        repaint()
        return
      }
    }

    window.addEventListener('message', onMessage)
    return () => window.removeEventListener('message', onMessage)
  }, [])
  return null
}
