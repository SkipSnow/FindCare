# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
#
# Individual-provider page — utterance mining.
#
# The classifier upstream routes and holds no domain knowledge. This page
# reads the utterance itself and answers with the parameters it needs, so
# adding a page adds a prompt rather than enlarging one shared classifier.
#
# The system prompt and the model are loaded from brain/machine_artifacts/
# content/prompts.json, record
# `individual_provider_utterance_mining_system_prompt`.

from typing import Optional

from pydantic import BaseModel, Field

from ProviderManagement.utterance_mining import mine

MINING_RECORD_ID = "individual_provider_utterance_mining_system_prompt"

COMPONENT = "IndividualProviderUtterance"


class MinedGeography(BaseModel):
    """Where the person wants the care giver to be."""
    state: str = ""
    city: str = ""
    county: str = ""
    zip: str = ""


class MinedProviderName(BaseModel):
    """A care giver named outright."""
    last: str = ""
    first: str = ""
    middle: str = ""


class MinedIndividualProviderParameters(BaseModel):
    """What the individual-provider page needs from an utterance, and
    nothing else. The specialty codes the search runs under are not here:
    they are resolved from the complaint, not stated by the person."""
    complaint: str = ""
    geography: MinedGeography = Field(default_factory=MinedGeography)
    provider_name: MinedProviderName = Field(
        default_factory=MinedProviderName)
    provider_sex: str = ""
    sole_proprietor: Optional[bool] = None
    insurance: str = ""


def mine_individual_provider_parameters(
        utterance: str,
        history: Optional[list] = None
) -> MinedIndividualProviderParameters:
    return mine(
        MINING_RECORD_ID, MinedIndividualProviderParameters,
        utterance, history, component=COMPONENT,
        call_site="IndividualProviderUtterance."
                  "mine_individual_provider_parameters")
