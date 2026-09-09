# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
#
# EvaluateCareFacade — entry point for all EvaluateCareQuality capabilities.
#
# Facade-to-facade: calls FindCareFacade for cross-domain needs (e.g., provider location).
# Never exposes internal services to other components.
#
# Design: ARCH-001

from chathealthy_lib import ChatHealthyLoggingService

from ClinicalTrials.clinical_trials_service import ClinicalTrialsService
from ProviderDetail.provider_detail_service import ProviderDetailService

log = ChatHealthyLoggingService()


class EvaluateCareFacade:
    """Public interface for the EvaluateCareQuality business component.

    UAT Features: 3 (Clinical Trials), 8 (Provider Detail)
    """

    def __init__(self, clinical_trials: ClinicalTrialsService,
                 provider_detail: ProviderDetailService,
                 find_care_facade=None):
        self._clinical_trials = clinical_trials
        self._provider_detail = provider_detail
        self._find_care = find_care_facade  # for cross-domain calls

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
