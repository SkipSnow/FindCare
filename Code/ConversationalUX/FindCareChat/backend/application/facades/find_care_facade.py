# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
#
# FindCareFacade — the one surface FindCare's tools call.
#
# Wraps FindCare's own clinical-trials and provider-detail services. Nothing
# here crosses a component boundary: both services are FindCare's, and both
# capabilities are FindCare features (EPIC-006-F-004, F-005, F-002).
#
# Design: ARCH-001

from chathealthy_lib import ChatHealthyLoggingService

from ClinicalTrials.clinical_trials_service import ClinicalTrialsService
from ProviderDetail.provider_detail_service import ProviderDetailService

log = ChatHealthyLoggingService()


class FindCareFacade:
    """The one surface FindCare's tools call.

    Clinical trials (EPIC-006-F-004, F-005) and provider detail
    (EPIC-006-F-002).
    """

    def __init__(self, clinical_trials: ClinicalTrialsService,
                 provider_detail: ProviderDetailService):
        self._clinical_trials = clinical_trials
        self._provider_detail = provider_detail

    def search_clinical_trials(self, condition: str, location: str = "",
                               user_location: str = "", max_results: int = 5) -> dict:
        """UAT Feature 3: Search for recruiting clinical trials."""
        return self._clinical_trials.search(
            condition=condition,
            location=location,
            user_location=user_location,
            max_results=max_results,
        )

    def get_provider_details(self, provider_name: str, npi: str = "",
                             state: str = "", **kwargs) -> dict:
        """UAT Feature 8: Look up provider credentials and research links."""
        return self._provider_detail.lookup(
            provider_name=provider_name,
            npi=npi,
            state=state,
            **kwargs,
        )
