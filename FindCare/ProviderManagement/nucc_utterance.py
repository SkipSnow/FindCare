# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
#
# NUCC page — utterance mining.
#
# The classifier upstream routes and holds no domain knowledge. This page
# reads the utterance itself and answers with the parameters it needs, so
# adding a page adds a prompt rather than enlarging one shared classifier.
#
# The system prompt and the model are loaded from brain/machine_artifacts/
# content/prompts.json, record `nucc_utterance_mining_system_prompt`.

from typing import Optional

from pydantic import BaseModel

from ProviderManagement.utterance_mining import mine

MINING_RECORD_ID = "nucc_utterance_mining_system_prompt"

COMPONENT = "NuccUtterance"


class MinedNuccParameters(BaseModel):
    """What the NUCC page needs from an utterance, and nothing else. The
    specialties it offers and the codes it offers them under are resolved
    from the complaint, not stated by the person."""
    complaint: str = ""


def mine_nucc_parameters(
        utterance: str,
        history: Optional[list] = None) -> MinedNuccParameters:
    return mine(
        MINING_RECORD_ID, MinedNuccParameters, utterance, history,
        component=COMPONENT,
        call_site="NuccUtterance.mine_nucc_parameters")
