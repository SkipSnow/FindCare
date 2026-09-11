# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).

from chathealthy_lib import ChatHealthyLoggingService
import os
import uuid
from datetime import datetime, timezone


from chathealthy_lib.authentication.auth_token import AuthToken
from chathealthy_lib.authentication.nonce import Nonce
from chathealthy_lib.authentication.session_token import SessionToken

from chathealthy_lib.exceptions import ChatHealthyException

log = ChatHealthyLoggingService()


TOKEN_PREFIX = "CH"
ORIGIN = "SharedServices"


class MintableAuthToken(AuthToken):
    @classmethod
    def manufacture(cls, server_env: str, guid: str | None = None) -> AuthToken:
        """Stamp a token, resuming the session named by guid when given.

        A page that already holds a GUID passes it back, and the session it
        names is in Mongo. Minting a new one regardless is how a reload
        orphaned the session it had: the document stayed, correct and
        complete, and nothing could point at it again. The nonce is fresh
        either way -- it is per hop, and the GUID is per session.
        """
        guid = (guid or "").strip() or uuid.uuid4().hex
        nonce_field = Nonce.fresh()

        token_str = f"{TOKEN_PREFIX}{nonce_field}{guid}"

        st = SessionToken(
            origin=ORIGIN,
            token=token_str,
            signature="",
            created_at=datetime.now(timezone.utc).isoformat(),
            signed=False,
            server_env=server_env,
        )
        # The requesting server signs, and SharedServices is the one that
        # mints. The key is its own, read from the registry and the vault.
        st.sign()
        log.info("AuthToken: env=%s guid_prefix=%s", server_env, guid[:8])
        return cls(st, origin=ORIGIN)

