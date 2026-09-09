# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
#
# ClinicalTrialsService — UAT Feature 3: Clinical Trials Search (+ travel time)
#
# Extracted from main.py as part of ARCH-001 Phase 4.
# Host-independent — no FastAPI, no HuggingFace dependencies.
#
# Design: ARCH-001, business component: EvaluateCareQuality

from chathealthy_lib import ChatHealthyLoggingService
from chathealthy_lib.exceptions import ChatHealthyException
import os

import requests

log = ChatHealthyLoggingService()


class ClinicalTrialsService:
    """Clinical trials search with optional travel info.

    Dependencies: ClinicalTrials.gov API, Google Routes API.
    """

    def __init__(self, google_maps_api_key: str = ""):
        self._api_key = google_maps_api_key or os.getenv("GOOGLE_MAPS_API_KEY", "")

    def _get_travel_info(self, origin: str, destinations: list[str]) -> dict[str, dict]:
        """Call Google Routes API for drive distance/time."""
        if not self._api_key or not destinations:
            return {}
        headers = {
            "X-Goog-Api-Key": self._api_key,
            "X-Goog-FieldMask": "routes.distanceMeters,routes.duration",
            "Content-Type": "application/json",
        }
        results = {}
        for dest in destinations:
            try:
                body = {
                    "origin": {"address": origin},
                    "destination": {"address": dest},
                    "travelMode": "DRIVE",
                    "routingPreference": "TRAFFIC_UNAWARE",
                }
                resp = requests.post(
                    "https://routes.googleapis.com/directions/v2:computeRoutes",
                    json=body, headers=headers, timeout=10,
                )
                if resp.status_code != 200:
                    continue
                data = resp.json()
                routes = data.get("routes", [])
                if not routes:
                    continue
                r = routes[0]
                meters = r.get("distanceMeters", 0)
                miles = meters / 1609.34
                secs = int(r.get("duration", "0s").replace("s", ""))
                hrs = secs // 3600
                mins = (secs % 3600) // 60
                results[dest] = {
                    "distance": f"{miles:.0f} miles",
                    "duration": f"{hrs}h {mins}m" if hrs else f"{mins}m",
                }
            except Exception as exc:
                # Mode 1 (REQ-B-008): travel info for one destination is
                # optional decoration on a trial; continue with the remaining
                # destinations and degrade silently (trial card still shows
                # without travel info).
                log.info("Routes API failed for %s: %s", dest, exc, exc=ChatHealthyException(
                                                                      mode="routes_api_failed",
                                                                      message=f"Routes API failed for {dest}: {exc}",
                                                                      component="ClinicalTrialsService",
                                                                      exception=exc,
                                                                  ))
        return results

    def search(self, condition: str, location: str = "", user_location: str = "",
               max_results: int = 5) -> dict:
        """Search ClinicalTrials.gov for recruiting trials."""
        params = {
            "query.cond": condition,
            "filter.overallStatus": "RECRUITING",
            "pageSize": min(int(max_results), 10),
            "format": "json",
        }
        if location:
            params["query.locn"] = location
        try:
            response = requests.get("https://clinicaltrials.gov/api/v2/studies", params=params, timeout=15)
            response.raise_for_status()
            studies = response.json().get("studies", [])
        except Exception as exc:
            # Mode 2 (REQ-B-008): the entire trials API call failed; user
            # asked for trials and gets back an error dict the tool renders
            # as a graceful "couldn't reach ClinicalTrials.gov" message.
            # Resource temporarily unavailable — operator MUST know.
            log.error("ClinicalTrials.gov search failed: %s", exc, exc=ChatHealthyException(
                                                                    mode="clinical_trials_search_failed",
                                                                    message=f"ClinicalTrials.gov search failed: {exc}",
                                                                    component="ClinicalTrialsService",
                                                                    exception=exc,
                                                                ), if_not_debug_log=True)
            return {"error": f"ClinicalTrials.gov search failed: {exc}"}

        if not studies:
            return {"trials": [], "message": "No recruiting trials found for this condition."}

        trials = []
        travel_destinations = []
        trial_location_map = {}

        for idx, study in enumerate(studies):
            ps = study.get("protocolSection", {})
            id_mod = ps.get("identificationModule", {})
            status_mod = ps.get("statusModule", {})
            desc_mod = ps.get("descriptionModule", {})
            elig_mod = ps.get("eligibilityModule", {})
            contacts_mod = ps.get("contactsLocationsModule", {})
            design_mod = ps.get("designModule", {})
            nct_id = id_mod.get("nctId", "")
            raw_locs = contacts_mod.get("locations", [])

            locs = []
            for loc in raw_locs[:5]:
                facility = loc.get("facility", "")
                city = loc.get("city", "")
                state = loc.get("state", "")
                loc_str = ", ".join(filter(None, [facility, city, state]))
                city_state = ", ".join(filter(None, [city, state]))
                loc_obj = {"display": loc_str, "city_state": city_state}
                locs.append(loc_obj)
                if city_state and city_state not in travel_destinations:
                    travel_destinations.append(city_state)

            trial_location_map[idx] = locs

            trials.append({
                "nct_id": nct_id,
                "title": id_mod.get("briefTitle", ""),
                "status": status_mod.get("overallStatus", ""),
                "phase": ", ".join(design_mod.get("phases", [])) or "N/A",
                "locations": [loc["display"] for loc in locs] or ["See ClinicalTrials.gov"],
                "summary": (desc_mod.get("briefSummary") or "")[:400],
                "eligibility": (elig_mod.get("eligibilityCriteria") or "")[:600],
                "url": f"https://clinicaltrials.gov/study/{nct_id}",
            })

        if user_location and travel_destinations:
            travel_info = self._get_travel_info(user_location, travel_destinations[:25])
            if travel_info:
                for idx, trial in enumerate(trials):
                    locs = trial_location_map.get(idx, [])
                    travel = []
                    for loc in locs:
                        cs = loc["city_state"]
                        if cs in travel_info:
                            travel.append({
                                "location": loc["display"],
                                "distance": travel_info[cs]["distance"],
                                "travel_time": travel_info[cs]["duration"],
                            })
                    if travel:
                        trial["travel_info"] = travel

        return {"trials": trials}
