# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
#
# Clinical-trial page — utterance mining.
#
# The classifier upstream routes and holds no domain knowledge. This page
# reads the utterance itself and answers with the parameters it needs, so
# adding a page adds a prompt rather than enlarging one shared classifier.
#
# The system prompt and the model are loaded from brain/machine_artifacts/
# content/prompts.json, record
# `clinical_trial_utterance_mining_system_prompt`.

from typing import Optional

from pydantic import BaseModel

from ProviderManagement.utterance_mining import mine

MINING_RECORD_ID = "clinical_trial_utterance_mining_system_prompt"

COMPONENT = "ClinicalTrialUtterance"


class MinedClinicalTrialParameters(BaseModel):
    """What the clinical-trial page needs from an utterance, and nothing
    else."""
    condition: str = ""
    age_years: Optional[int] = None
    sex: str = ""
    united_states_only: Optional[bool] = None


def mine_clinical_trial_parameters(
        utterance: str,
        history: Optional[list] = None) -> MinedClinicalTrialParameters:
    return mine(
        MINING_RECORD_ID, MinedClinicalTrialParameters, utterance, history,
        component=COMPONENT,
        call_site="ClinicalTrialUtterance.mine_clinical_trial_parameters")
