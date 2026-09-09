# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
#
# ProviderDetailService — EPIC-006-F-002 (data management + display).
#
# Flow per S-002:
#   1. Fetch the full live NPPES record by NPI.
#   2. Compare against the stored provider record in Mongo.
#   3. On divergence, write the live values back into the stored record
#      preserving / re-running pipeline enrichments per the matrix.
#   4. Project the (current) stored record into the display payload via
#      ProviderDetailOutput.from_stored — each display type owns its
#      conversion next to its definition.
#   5. Embedding runs as a BackgroundTask scheduled by the FastAPI
#      handler after the response returns.

from typing import Optional

from chathealthy_lib import ChatHealthyLoggingService
from chathealthy_lib.exceptions import ChatHealthyException
import urllib.parse

import requests

from . import provider_record_sync
from .provider_detail_models import ProviderDetailOutput

log = ChatHealthyLoggingService()


STATE_BOARDS = {
    "AK": ("https://www.commerce.alaska.gov/cbp/main/search/professional", "Alaska State Medical Board"),
    "AL": ("https://www.albme.gov/consumers/licensee-search/", "Alabama Board of Medical Examiners & Medical Licensure Commission"),
    "AR": ("https://armedicalboard.adh.arkansas.gov/public/verify/default.aspx", "Arkansas State Medical Board"),
    "AZ": ("https://azbomprod.azmd.gov/glsuiteweb/clients/azbom/public/webverificationsearch.aspx", "Arizona Medical Board"),
    "CA": ("https://www.mbc.ca.gov/Breeze/License_Verification.aspx", "Medical Board of California"),
    "CO": ("https://apps.colorado.gov/dora/licensing/Lookup/LicenseLookup.aspx", "Colorado Medical Board"),
    "CT": ("https://www.elicense.ct.gov/Lookup/LicenseLookup.aspx", "Connecticut Medical Examining Board"),
    "DC": ("https://app.hpla.doh.dc.gov/Weblookupcs/", "District of Columbia Board of Medicine"),
    "DE": ("https://delpros.delaware.gov/oh_verifylicense", "Delaware Board of Medical Licensure and Discipline"),
    "FL": ("https://mqa-internet.doh.state.fl.us/MQASearchServices/HealthCareProviders", "Florida Board of Medicine"),
    "GA": ("https://gateway.medicalboard.georgia.gov/verification/search.aspx", "Georgia Composite Medical Board"),
    "HI": ("https://mypvl.dcca.hawaii.gov/public-license-search/", "Hawaii Medical Board"),
    "IA": ("https://amanda-portal.idph.state.ia.us/ibm/portal/", "Iowa Board of Medicine"),
    "ID": ("https://apps-dopl.idaho.gov/IBOMPortal/AgencyAdditional.aspx?Agency=425&AgencyLinkID=570", "Idaho Board of Medicine"),
    "IL": ("https://online-dfpr.micropact.com/lookup/licenselookup.aspx", "Illinois Department of Financial and Professional Regulation"),
    "IN": ("https://mylicense.in.gov/everification/", "Medical Licensing Board of Indiana"),
    "KS": ("https://www.kansas.gov/ssrv-ksbhada/search.html", "Kansas State Board of Healing Arts"),
    "KY": ("https://kbml.ky.gov/physician/Pages/Physician-Profile-Verification-of-Physician-License.aspx", "Kentucky Board of Medical Licensure"),
    "LA": ("https://apps.lsbme.la.gov/verifications/results.aspx", "Louisiana State Board of Medical Examiners"),
    "MA": ("https://www.mass.gov/how-to/verify-a-physician-or-acupuncture-license", "Massachusetts Board of Registration in Medicine"),
    "MD": ("https://www.mbp.state.md.us/mbp_ah/verification.aspx", "Maryland Board of Physicians"),
    "ME": ("https://www.maine.gov/md/licensure/license-verification", "Maine Board of Licensure in Medicine"),
    "MI": ("https://www.michigan.gov/lara/i-need-to/find-or-verify-a-licensed-professional-or-business", "Michigan Board of Medicine"),
    "MN": ("https://mn.gov/boards/medical-practice/verify/index.jsp", "Minnesota Board of Medical Practice"),
    "MO": ("https://pr.mo.gov/licensee-search.asp", "Missouri Board of Registration for the Healing Arts"),
    "MS": ("https://gateway.msbml.ms.gov/verification/search.aspx", "Mississippi State Board of Medical Licensure"),
    "MT": ("https://ebizws.mt.gov/PUBLICPORTAL/searchform?mylist=licenses", "Montana Board of Medical Examiners"),
    "NC": ("https://portal.ncmedboard.org/verification/search.aspx", "North Carolina Medical Board"),
    "ND": ("https://www.ndbom.org/public/find_verify/verify.asp", "North Dakota Board of Medicine"),
    "NE": ("https://www.nebraska.gov/LISSearch/search.cgi", "Nebraska Board of Medicine and Surgery"),
    "NH": ("https://www.oplc.nh.gov/license-lookup", "New Hampshire Board of Medicine"),
    "NJ": ("https://newjersey.mylicense.com/verification/", "New Jersey Division of Consumer Affairs — Board of Medical Examiners"),
    "NM": ("https://nmrldlpi.my.site.com/nmmb/s/searchlicense", "New Mexico Medical Board"),
    "NV": ("https://nsbme.us.thentiacloud.net/webs/nsbme/register/", "Nevada State Board of Medical Examiners"),
    "NY": ("https://www.op.nysed.gov/services/verifications/online-verification-searches", "New York State Board for Medicine"),
    "OH": ("https://elicense.ohio.gov/oh_verifylicense", "State Medical Board of Ohio"),
    "OK": ("https://www.okmedicalboard.org/search", "Oklahoma State Board of Medical Licensure and Supervision"),
    "OR": ("https://omb.oregon.gov/clients/ormb/public/verification.aspx", "Oregon Medical Board"),
    "PA": ("https://www.pals.pa.gov/#/page/search", "Pennsylvania State Board of Medicine"),
    "PR": ("https://orcps.salud.pr.gov/mbps/verificacion", "Junta de Licenciamiento y Disciplina Médica de Puerto Rico"),
    "RI": ("https://health.ri.gov/find/licensees/index.php", "Rhode Island Board of Medical Licensure and Discipline"),
    "SC": ("https://verify.llronline.com/", "South Carolina Department of Labor, Licensing and Regulation — Board of Medical Examiners"),
    "SD": ("https://www.sdbmoe.gov/sdbmoe-licensee-lookup/", "South Dakota Board of Medical and Osteopathic Examiners"),
    "TN": ("https://apps.health.tn.gov/Licensure/", "Tennessee Board of Medical Examiners"),
    "TX": ("https://www.tmb.texas.gov/resources/for-the-public/look-up-a-license", "Texas Medical Board"),
    "UT": ("https://verify.commerce.utah.gov/verify/Search.aspx", "Utah Physicians Licensing Board"),
    "VA": ("https://dhp.virginiainteractive.org/lookup/index", "Virginia Board of Medicine"),
    "VT": ("https://mpb.health.vermont.gov/Lookup/LicenseLookup.aspx", "Vermont Board of Medical Practice"),
    "WA": ("https://wmc.wa.gov/licensing/verification-requests", "Washington Medical Commission"),
    "WI": ("https://licensesearch.wi.gov/", "Wisconsin Medical Examining Board"),
    "WV": ("https://wvbom.wv.gov/public/search/index.asp", "West Virginia Board of Medicine"),
    "WY": ("https://wyomedboard.wyo.gov/consumers/license-lookup", "Wyoming Board of Medicine"),
    # The outlying sovereignties. EPIC-006-F-002-S-001-REQ-B-007 fixes the
    # table's domain as every jurisdiction the registry can write into a
    # practice address, so the fifty states and DC do not complete it: a
    # provider practising in Guam or the Virgin Islands was offered no
    # licensing destination at all.
    "AS": ("https://www.americansamoa.gov/health", "American Samoa Department of Health"),
    "GU": ("https://ghs.guam.gov/guam-board-medical-examiners", "Guam Board of Medical Examiners"),
    "MP": ("https://chcc.gov.mp/hcplb.php", "Commonwealth Healthcare Corporation Health Care Professions Licensing Board"),
    "VI": ("https://dlca.vi.gov/business-licensing/", "United States Virgin Islands Board of Medical Examiners"),
}


# The authority that licenses or certifies a PLACE, per
# EPIC-006-F-007-S-001-REQ-B-007. A second table beside STATE_BOARDS,
# keyed the same way and complete over the same domain, because the
# authority that licenses a person is generally not the authority that
# licenses a facility: a state medical board licenses a physician and a
# state health department licenses a hospital. Reusing the practitioner
# table here would send a person to an authority that holds no record of
# the facility.
FACILITY_BOARDS = {
    "AK": ("https://health.alaska.gov/dph/Director/Pages/facilities/default.aspx", "Alaska Health Facilities Licensing and Certification"),
    "AL": ("https://www.alabamapublichealth.gov/providerstandards/", "Alabama Department of Public Health, Provider Standards"),
    "AR": ("https://healthy.arkansas.gov/programs-services/topics/health-facility-services/", "Arkansas Department of Health, Health Facility Services"),
    "AS": ("https://www.americansamoa.gov/health", "American Samoa Department of Health"),
    "AZ": ("https://azdhs.gov/licensing/index.php", "Arizona Department of Health Services, Division of Licensing"),
    "CA": ("https://www.cdph.ca.gov/Programs/CHCQ/LCP/Pages/LCP.aspx", "California Department of Public Health, Licensing and Certification"),
    "CO": ("https://cdphe.colorado.gov/health-facility-licensing", "Colorado Department of Public Health and Environment, Health Facilities"),
    "CT": ("https://portal.ct.gov/DPH/Facility-Licensing--Investigations/Facility-Licensing-and-Investigations-Section", "Connecticut Department of Public Health, Facility Licensing and Investigations"),
    "DC": ("https://dchealth.dc.gov/service/health-care-facilities-licensing", "District of Columbia Department of Health, Health Care Facilities Division"),
    "DE": ("https://www.dhss.delaware.gov/dhss/dltcrp/", "Delaware Division of Health Care Quality"),
    "FL": ("https://ahca.myflorida.com/health-care-policy-and-oversight/bureau-of-health-facility-regulation", "Florida Agency for Health Care Administration"),
    "GA": ("https://dch.georgia.gov/divisionsoffices/healthcare-facility-regulation", "Georgia Department of Community Health, Healthcare Facility Regulation"),
    "GU": ("https://dphss.guam.gov/", "Guam Department of Public Health and Social Services"),
    "HI": ("https://health.hawaii.gov/ohca/", "Hawaii Office of Health Care Assurance"),
    "IA": ("https://hhs.iowa.gov/health-facilities", "Iowa Department of Health and Human Services, Health Facilities"),
    "ID": ("https://healthandwelfare.idaho.gov/providers/licensing-certification", "Idaho Department of Health and Welfare, Licensing and Certification"),
    "IL": ("https://dph.illinois.gov/topics-services/health-care-regulation.html", "Illinois Department of Public Health, Health Care Regulation"),
    "IN": ("https://www.in.gov/health/hcq/", "Indiana Department of Health, Health Care Quality and Regulatory Commission"),
    "KS": ("https://www.kdhe.ks.gov/216/Health-Facilities", "Kansas Department of Health and Environment, Health Facilities"),
    "KY": ("https://www.chfs.ky.gov/agencies/os/oig/dhc/Pages/default.aspx", "Kentucky Division of Health Care, Office of Inspector General"),
    "LA": ("https://ldh.la.gov/page/health-standards-section", "Louisiana Department of Health, Health Standards Section"),
    "MA": ("https://www.mass.gov/orgs/bureau-of-health-care-safety-and-quality", "Massachusetts Bureau of Health Care Safety and Quality"),
    "MD": ("https://health.maryland.gov/ohcq/Pages/home.aspx", "Maryland Office of Health Care Quality"),
    "ME": ("https://www.maine.gov/dhhs/dlc", "Maine Division of Licensing and Certification"),
    "MI": ("https://www.michigan.gov/lara/bureau-list/bchs", "Michigan Bureau of Community and Health Systems"),
    "MN": ("https://www.health.state.mn.us/facilities/regulation/index.html", "Minnesota Department of Health, Health Regulation Division"),
    "MO": ("https://health.mo.gov/safety/healthservicesregulation/", "Missouri Bureau of Health Services Regulation"),
    "MP": ("https://chcc.gov.mp/", "Commonwealth Healthcare Corporation, Northern Mariana Islands"),
    "MS": ("https://msdh.ms.gov/page/30,0,82.html", "Mississippi State Department of Health, Health Facilities Licensure"),
    "MT": ("https://dphhs.mt.gov/qad/licensure/index", "Montana Department of Public Health and Human Services, Licensure Bureau"),
    "NC": ("https://info.ncdhhs.gov/dhsr/", "North Carolina Division of Health Service Regulation"),
    "ND": ("https://www.hhs.nd.gov/health-facilities", "North Dakota Health and Human Services, Health Facilities"),
    "NE": ("https://dhhs.ne.gov/licensure/Pages/Health-Facilities.aspx", "Nebraska Division of Public Health, Health Facility Licensure"),
    "NH": ("https://www.dhhs.nh.gov/programs-services/health-care-facility-licensing", "New Hampshire Health Facilities Administration"),
    "NJ": ("https://www.nj.gov/health/healthfacilities/", "New Jersey Division of Health Facilities Survey and Field Operations"),
    "NM": ("https://www.nmhealth.org/about/dhi/hflc/", "New Mexico Health Facility Licensing and Certification Bureau"),
    "NV": ("https://dpbh.nv.gov/Reg/HealthFacilities/Health_Facilities_-_Home/", "Nevada Division of Public and Behavioral Health, Health Care Quality and Compliance"),
    "NY": ("https://profiles.health.ny.gov/", "New York State Department of Health, Health Facility Profiles"),
    "OH": ("https://odh.ohio.gov/know-our-programs/health-care-facility-regulation", "Ohio Department of Health, Health Care Facility Regulation"),
    "OK": ("https://oklahoma.gov/health/protective-health/medical-facilities-service.html", "Oklahoma State Department of Health, Medical Facilities Service"),
    "OR": ("https://www.oregon.gov/oha/PH/PROVIDERPARTNERRESOURCES/HEALTHCAREPROVIDERSFACILITIES/HEALTHCAREHEALTHCAREREGULATIONQUALITYIMPROVEMENT/Pages/index.aspx", "Oregon Health Care Regulation and Quality Improvement"),
    "PA": ("https://www.pa.gov/agencies/health/programs/facilities.html", "Pennsylvania Department of Health, Division of Health Facilities"),
    "PR": ("https://www.salud.pr.gov/", "Puerto Rico Department of Health, Secretaria Auxiliar de Reglamentacion y Acreditacion"),
    "RI": ("https://health.ri.gov/licenses/", "Rhode Island Department of Health, Center for Health Facilities Regulation"),
    "SC": ("https://dph.sc.gov/environment/health-regulation/healthcare-facility-licensing", "South Carolina Department of Public Health, Healthcare Facility Licensing"),
    "SD": ("https://doh.sd.gov/providers/licensure/", "South Dakota Department of Health, Office of Licensure and Certification"),
    "TN": ("https://www.tn.gov/health/health-program-areas/health-professional-boards/hcf-board.html", "Tennessee Board for Licensing Health Care Facilities"),
    "TX": ("https://www.hhs.texas.gov/providers/health-care-facilities-regulation", "Texas Health and Human Services, Health Care Facilities Regulation"),
    "UT": ("https://hslic.utah.gov/", "Utah Health Facility Licensing and Certification"),
    "VA": ("https://www.vdh.virginia.gov/licensure-certification/", "Virginia Department of Health, Office of Licensure and Certification"),
    "VI": ("https://doh.vi.gov/", "United States Virgin Islands Department of Health"),
    "VT": ("https://www.healthvermont.gov/health-statistics-vital-records/health-care-systems-reporting/licensing-health-care-facilities", "Vermont Division of Licensing and Protection"),
    "WA": ("https://doh.wa.gov/licenses-permits-and-certificates/facilities-new-renew-or-update", "Washington State Department of Health, Facility Licensing"),
    "WI": ("https://www.dhs.wisconsin.gov/regulations/index.htm", "Wisconsin Division of Quality Assurance"),
    "WV": ("https://ohflac.wvdhhr.org/", "West Virginia Office of Health Facility Licensure and Certification"),
    "WY": ("https://health.wyo.gov/aging/hls/", "Wyoming Department of Health, Healthcare Licensing and Surveys"),
}


def resolve_taxonomy_display_names(
    codes: list[str],
    specialty_meta_coll_callable,
) -> dict[str, str]:
    """Join NUCC codes against SpecialtyMetaData to get display names."""
    if not codes or specialty_meta_coll_callable is None:
        return {}
    try:
        coll = specialty_meta_coll_callable()
        docs = coll.find(
            {"Code": {"$in": list(set(codes))}},
            {"Code": 1, "Display Name": 1, "_id": 0},
        )
        return {
            (d.get("Code") or ""): (d.get("Display Name") or "")
            for d in docs
        }
    except Exception as exc:
        # Mode 1 (REQ-B-008): taxonomy display name lookup; if it fails,
        # the detail panel renders without specialty display names (codes
        # still show). Graceful, non-blocking degradation.
        log.info(
            "taxonomy display name lookup failed: %s", exc,
            exc=ChatHealthyException(
                mode="taxonomy_display_name_lookup_failed",
                message=f"taxonomy display name lookup failed: {exc}",
                component="ProviderDetailService",
                exception=exc,
            ),
        )
        return {}


def _every_address(record: dict) -> list[dict]:
    """Every address on a record, practice first then business.

    v4 splits the single addresses[] into practice_addresses[] and a lone
    business_address. One helper so every caller reads the two fields the
    same way, rather than each remembering to look in both places.
    """
    out = list(record.get("practice_addresses") or [])
    business = record.get("business_address")
    if business:
        out.append(business)
    return out


class ProviderDetailService:
    """Provider detail lookup with compare-and-write-back cycle."""

    @staticmethod
    def display_name(stored: Optional[dict]) -> str:
        """The provider's name as the card writes it, read from the record.

        Same parts Identity.from_stored uses, so the header and the
        identity block below it cannot say different things.
        """
        if not stored:
            return ""
        # An organization's name is its legal business name. Assembling
        # personal name parts for one yields an empty header, and the
        # header is what the panel is titled with.
        if str(stored.get("entity_type_code") or "1") == "2":
            return (stored.get("provider_organization_name_legal_business_name")
                    or "").strip()
        parts = [
            (stored.get("provider_first_name") or "").strip(),
            (stored.get("provider_middle_name") or "").strip(),
            (stored.get("provider_last_name_legal_name") or "").strip(),
        ]
        name = " ".join(p for p in parts if p)
        credential = (stored.get("provider_credential_text") or "").strip()
        if name and credential:
            return f"{name}, {credential}"
        return name or credential

    def lookup(
        self,
        provider_name: str = "",
        npi: str = "",
        state: str = "",
        provider_coll=None,
        specialty_meta_coll=None,
        schedule_background_task=None,
        entity_type: str = "1",
        **kwargs,
    ) -> dict:
        stored = None
        sync_summary = None

        # EPIC-006-F-007-S-003-REQ-B-003: a Facility Detail is not shown
        # for an NPI the authoritative registry reports is not an
        # organization. The check is on the LIVE answer rather than on the
        # stored record, because the stored record is what would be wrong
        # in this case. It is distinct from the registry not answering,
        # where the stored record is shown: not answering leaves us no
        # reason to doubt the record; answering with a different entity
        # type gives us one.
        if npi and entity_type == "2":
            live = self.fetch_live(npi)
            if live is not None and self.live_entity_type(live) != "2":
                # REQ-B-004: the person is told the facility was not found.
                return {"not_found": True, "npi": npi,
                        "message": f"No facility found for NPI {npi}."}

        if npi and provider_coll is not None:
            try:
                stored, sync_summary = self.sync_cycle(npi, provider_coll)
            except Exception as exc:
                # Mode 2 (REQ-B-008): the live-vs-stored sync cycle (the
                # F-025-S-002 reconciliation) failed; stored stays None and
                # the user sees an empty detail panel. Operator MUST know —
                # the live-refresh contract is the headline feature of
                # provider detail.
                log.error(
                    "provider_detail sync cycle failed for NPI %s: %s",
                    npi, exc,
                    exc=ChatHealthyException(
                        mode="provider_detail_sync_cycle_failed",
                        message=f"provider_detail sync cycle failed for NPI {npi}: {exc}",
                        component="ProviderDetailService",
                        exception=exc,
                    ),
                    if_not_debug_log=True,
                )

        if sync_summary is not None:
            log.info(
                "provider_detail sync NPI=%s divergence=%s "
                "new_addresses_resolved=%d google_maps_calls=%d "
                "google_maps_failures=%d",
                npi,
                sync_summary["divergence"],
                sync_summary["new_addresses_resolved"],
                sync_summary["google_maps_calls"],
                sync_summary["google_maps_failures"],
            )
            if (
                sync_summary["embedding_text_changed"]
                and schedule_background_task is not None
            ):
                schedule_background_task(
                    provider_record_sync.embed_after_response, provider_coll, npi,
                )

        if stored is not None:
            codes = [
                (t.get("code") or "").strip()
                for t in stored.get("taxonomies") or []
            ]
            code_to_display = resolve_taxonomy_display_names(
                codes, specialty_meta_coll,
            )
            primary_code = ProviderDetailOutput.primary_taxonomy_code(stored)
            primary_display = code_to_display.get(primary_code, "")
            primary_state = (
                ProviderDetailOutput.primary_practice_state(stored)
                or (state or "").upper()
            )
        else:
            code_to_display = {}
            primary_display = ""
            primary_state = (state or "").upper()

        # A detail opened from a recorded selection carries the NPI and no
        # card, so the name comes from the record -- which is where it
        # should come from anyway. Every other name field on the panel is
        # already read from `stored`; taking the header and the research
        # links from a card that may have been painted some time ago is how
        # they would disagree with the rest of the panel.
        provider_name = provider_name or self.display_name(stored)

        research_sites, unresolved_state = self.build_research_sites(
            provider_name=provider_name,
            npi=npi,
            state=primary_state,
            entity_type=entity_type,
        )

        return ProviderDetailOutput.from_stored(
            provider_name=provider_name,
            npi=npi,
            stored=stored,
            code_to_display=code_to_display,
            primary_taxonomy_display=primary_display,
            research_sites=research_sites,
            unresolved_licensing_state=unresolved_state,
        )

    @staticmethod
    def live_entity_type(live_record: dict) -> str:
        """What the registry says this NPI is enumerated as.

        The version=2.1 response states the enumeration type as NPI-1 for
        an individual and NPI-2 for an organization. Anything else is
        neither, and is reported as it was given rather than guessed at.
        """
        stated = str((live_record or {}).get("enumeration_type") or "").strip()
        if stated == "NPI-2":
            return "2"
        if stated == "NPI-1":
            return "1"
        return stated

    def sync_cycle(self, npi: str, coll):
        """Returns (stored_record, sync_summary). stored_record is the
        post-write provider record; sync_summary is None when live fetch
        failed (no comparison ran)."""
        live_record = self.fetch_live(npi)
        if live_record is None:
            stored = coll.find_one({"npi": npi})
            return stored, None

        stored = coll.find_one({"npi": npi})
        if stored is None:
            return None, None

        live_proj = provider_record_sync.live_to_comparable(live_record)
        stored_proj = provider_record_sync.stored_to_comparable(stored)
        divergence = provider_record_sync.compare(live_proj, stored_proj)

        sync_summary = {
            "divergence": provider_record_sync.has_any_divergence(divergence),
            "embedding_text_changed": False,
            "new_addresses_resolved": 0,
            "google_maps_calls": 0,
            "google_maps_failures": 0,
        }

        if sync_summary["divergence"]:
            new_doc = provider_record_sync.merge_for_writeback(live_record, stored)
            sync_summary["embedding_text_changed"] = (
                provider_record_sync.build_embedding_text(new_doc)
                != provider_record_sync.build_embedding_text(stored)
            )
            stored_addr_keys = {
                (
                    (a.get("line1") or "").strip(),
                    (a.get("city") or "").strip(),
                    (a.get("state") or "").strip(),
                    (a.get("zip") or "")[:5],
                )
                for a in _every_address(stored)
            }
            for a in _every_address(new_doc):
                key = (
                    a.get("line1", ""),
                    a.get("city", ""),
                    a.get("state", ""),
                    a.get("zip", ""),
                )
                if key not in stored_addr_keys:
                    sync_summary["new_addresses_resolved"] += 1
                    sync_summary["google_maps_calls"] += 1
                    if not (a.get("county") or {}).get("name"):
                        sync_summary["google_maps_failures"] += 1
            # A merge that changed nothing is not written. The county
            # resolution runs on every open of a record still missing one,
            # and an address the geocoder cannot answer would otherwise
            # rewrite the document every time it was looked at.
            #
            # Provenance is left out of the comparison because
            # merge_for_writeback stamps it unconditionally: comparing the
            # whole document would find a difference every time and say
            # nothing about whether anything real had changed.
            without_provenance = {k: v for k, v in new_doc.items()
                                  if k != "provenance"}
            was = {k: v for k, v in stored.items()
                   if k not in ("provenance", "_id")}
            if without_provenance != was:
                provider_record_sync.write_back(coll, npi, new_doc)
                stored = coll.find_one({"npi": npi})

        return stored, sync_summary

    def fetch_live(self, npi: str) -> dict | None:
        try:
            resp = requests.get(
                f"https://npiregistry.cms.hhs.gov/api/?number={npi}&version=2.1",
                timeout=10,
            )
        except Exception as exc:
            # Mode 2 (REQ-B-008): NPPES live fetch failed (network/timeout);
            # we fall back to the stored record without a reconciliation
            # pass — user sees potentially stale data. NPPES is a critical
            # data source; operator MUST know about any outage.
            log.error(
                "NPPES live fetch failed for NPI %s: %s", npi, exc,
                exc=ChatHealthyException(
                    mode="nppes_fetch_failed",
                    message=f"NPPES live fetch failed for NPI {npi}: {exc}",
                    component="ProviderDetailService",
                    exception=exc,
                ),
                if_not_debug_log=True,
            )
            return None
        if resp.status_code != 200:
            log.warning(
                "NPPES live fetch non-200 for NPI %s: %d",
                npi, resp.status_code,
            )
            return None
        try:
            data = resp.json()
        except Exception as exc:
            # Mode 2 (REQ-B-008): NPPES returned non-JSON; we fall back to
            # the stored record without reconciliation. NPPES API contract
            # break — operator MUST know.
            log.error(
                "NPPES live fetch malformed JSON for NPI %s: %s",
                npi, exc,
                exc=ChatHealthyException(
                    mode="nppes_fetch_malformed_json",
                    message=f"NPPES live fetch malformed JSON for NPI {npi}: {exc}",
                    component="ProviderDetailService",
                    exception=exc,
                ),
                if_not_debug_log=True,
            )
            return None
        results = data.get("results") or []
        if not results:
            log.warning("NPPES live fetch zero results for NPI %s", npi)
            return None
        return results[0]

    def build_research_sites(
        self,
        provider_name: str,
        npi: str,
        state: str,
        entity_type: str = "1",
    ) -> tuple[dict, str]:
        """The destinations, and the practice state that resolved to none.

        Every destination returned carries a URL. A destination without one
        goes nowhere, so it is not built rather than built and filtered out
        later by whoever paints it.

        The second value is empty when the table covered the state. It is
        the state itself when it did not, so a gap in the table surfaces
        instead of quietly removing a link.
        """
        name_q = urllib.parse.quote_plus(provider_name or "")
        sites = {
            "healthgrades": {
                "url": (
                    f"https://www.healthgrades.com/find-a-doctor?"
                    f"what={name_q}&where={state}"
                ),
                "name": "Healthgrades",
                "guidance": (
                    "Search by the provider's full name and state. "
                    "Healthgrades uses fuzzy matching and may show "
                    "similar names — verify the provider name and "
                    "address match before relying on reviews."
                ),
            },
            "npi_registry": {
                "url": (
                    f"https://npiregistry.cms.hhs.gov/search?number={npi}"
                    if npi
                    else f"https://npiregistry.cms.hhs.gov/search?"
                         f"name_type=ind&first_name={name_q}"
                ),
                "name": "NPI Registry (CMS)",
                "guidance": (
                    "The official federal registry. Search by NPI "
                    "number for exact match. Shows license state, "
                    "specialty, and practice address. This is the "
                    "most reliable source for verifying a provider's "
                    "credentials."
                ),
            },
        }
        state_upper = (state or "").upper()
        # A place is licensed by a different authority from a person, so
        # the table is chosen by what this panel is showing.
        boards = FACILITY_BOARDS if entity_type == "2" else STATE_BOARDS
        if state_upper in boards:
            board_url, board_name = boards[state_upper]
            sites["state_medical_board"] = {
                "url": board_url,
                "name": board_name,
                "guidance": (
                    f"Verify standing to operate with the {board_name}."
                    if entity_type == "2" else
                    f"Verify active licensure with the {board_name}. "
                    "Confirm board certification and check for "
                    "disciplinary actions."
                ),
            }
            return sites, ""
        # A practice state absent from the table is a defect in the table,
        # not an absence of an authority. Emitting the state we could not
        # resolve surfaces the gap; silently omitting the destination
        # removes a link and says nothing.
        return sites, state_upper
