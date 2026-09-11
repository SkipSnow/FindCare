# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
"""What the external provider-lookup tool accepts.

This was called ProviderDetailInput and lived beside the clinical-trials
models. app.py imported it near the top and then imported the Provider
Detail endpoint's own ProviderDetailInput further down, which SHADOWED
it: the tool registry got this three-field shape and the endpoint got the
other one, from a single name, in a single module. They are different
contracts -- this one names the field provider_name because that is what
FindCareFacade.get_provider_details takes -- so they now have
different names.
"""
from pydantic import BaseModel, Field


class ProviderLookupInput(BaseModel):
    provider_name: str = Field(..., description="Provider name to look up")
    npi: str = Field("", description="NPI number")
    state: str = Field("", description="State code")
