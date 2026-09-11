# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
"""Resolve a token-signing credential from the registry and the vault.

A signing credential belongs to a PAIR, not to a service. SharedServices
holds one private key per destination, and each peer holds only the public
certificate for its own pair, so a token signed for FindCare does not
verify at EvaluateCare.

Which vault secret holds which half is read from
ChatHealthyConfig.CertificateRegistry, matched on
{env, identity, purpose: token_signing, peer, status: active}. The
registry names secrets and never carries key material or a URL: the vault
address comes from the environment the deploy bound, and the credential
itself never passes through the record.

A row is per environment, as ParameterDeclaration and DBVersions already
are. Local signs with local's keypair and dev with dev's, so a token from
one environment cannot be presented in another -- and CH_ENV is required
rather than defaulted, because a process that cannot say which
environment it is must not choose a credential.

Cached per process after the first read: a signature check on every
request cannot afford a database round trip and a vault round trip, and
the credential does not change inside a process lifetime.
"""
from __future__ import annotations

import os

from ..exceptions import ChatHealthyException
from ..mongo_utilities import ChatHealthyMongoUtilities

COMPONENT = "SigningCredential"

CONFIG_DATABASE = "ChatHealthyConfig"
REGISTRY = "CertificateRegistry"
CONFIG_CLUSTER = "ChatHealthyFrontEnd"

PRIVATE = "private_key_vault_key"
PUBLIC = "public_cert_vault_key"

_CACHE: dict[tuple[str, str, str], str] = {}


def _env() -> str:
    """Which environment this process is, from the build it was built as.

    Read from build_info.json, which the build writes and the image
    carries -- the same source runtime_data_collections binds its
    collections from. Not an environment variable: a process must not be
    able to be told it is a different environment than it was built as.
    """
    from ..runtime_data_collections import _read_build_info  # noqa: PLC0415
    env = str(_read_build_info().get("env", "")).strip()
    if not env:
        raise ChatHealthyException(
            mode="security_violation",
            message=("build_info.json names no env, so no environment's "
                     "signing credential can be selected. A process that "
                     "cannot say which environment it is must not choose a "
                     "credential."),
            component=COMPONENT)
    return env


def _registry_row(identity: str, reader: str) -> dict:
    env = _env()
    utilities = ChatHealthyMongoUtilities()
    client = utilities.getConnection(reader, CONFIG_CLUSTER)
    row = client[CONFIG_DATABASE][REGISTRY].find_one({
        "env": env,
        "identity": identity,
        "purpose": "token_signing",
        "status": "active",
    })
    if not row:
        raise ChatHealthyException(
            mode="security_violation",
            message=(f"{CONFIG_DATABASE}.{REGISTRY} names no active "
                     f"token_signing credential for {identity!r} in env "
                     f"{env!r}. A server with no registered credential "
                     f"cannot sign and cannot be verified."),
            component=COMPONENT,
            context={"env": env, "identity": identity})
    return row


def _vault_pem(secret_name: str, reader: str) -> str:
    vault_uri = os.environ.get("KEY_VAULT_URI", "").strip()
    if not vault_uri:
        raise ChatHealthyException(
            mode="vault_unreachable",
            message="KEY_VAULT_URI not set, so no signing credential can be read",
            component=COMPONENT)
    utilities = ChatHealthyMongoUtilities()
    token = utilities._azure_token(reader)          # noqa: SLF001
    return utilities._vault_secret(vault_uri, token, secret_name).strip()  # noqa: SLF001


def signing_key(identity: str, reader: str) -> str:
    """The private key PEM this server signs with."""
    return _credential(identity, reader, PRIVATE)


def verifying_cert(identity: str, reader: str) -> str:
    """The certificate PEM a token signed by `identity` verifies against."""
    return _credential(identity, reader, PUBLIC)


def _credential(identity: str, reader: str, half: str) -> str:
    key = (_env(), identity, half)
    cached = _CACHE.get(key)
    if cached is not None:
        return cached
    row = _registry_row(identity, reader)
    secret_name = row.get(half)
    if not secret_name:
        raise ChatHealthyException(
            mode="security_violation",
            message=(f"the registry row for {identity!r} carries no {half}, "
                     f"so the credential cannot be found"),
            component=COMPONENT,
            context={"identity": identity, "half": half})
    pem = _vault_pem(secret_name, reader)
    _CACHE[key] = pem
    return pem
