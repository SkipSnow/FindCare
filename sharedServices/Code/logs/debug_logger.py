# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
#
# DebugLogger — persists chat call metadata to MongoDB for debugging.
# Source: {ENV_PREFIX}_Debug.chat_calls

from chathealthy_lib import ChatHealthyLoggingService
from chathealthy_lib.exceptions import ChatHealthyException
from datetime import datetime, timezone
from typing import Optional

log = ChatHealthyLoggingService()


class DebugLogger:
    """Persists chat call metadata to MongoDB. Dev environment debugging."""

    def __init__(self, get_db_fn, env_prefix: str):
        self._get_db = get_db_fn
        self._env = env_prefix

    def log_chat(self, ip: str, message: str, history_len: int, tool_loop_iters: int,
                 tokens_in: Optional[int], tokens_out: Optional[int],
                 response_text: Optional[str], error: Optional[str],
                 history: Optional[list] = None) -> None:
        db = self._get_db()
        if db is None:
            return
        try:
            record = {
                "datetime": datetime.now(timezone.utc).isoformat(),
                "ip": ip, "message_preview": message[:200],
                "history_len": history_len, "tool_loop_iters": tool_loop_iters,
                "tokens_in": tokens_in, "tokens_out": tokens_out,
                "response_preview": response_text[:200] if response_text else None,
                "error": error,
            }
            db[f"{self._env}_Debug"]["chat_calls"].insert_one(record)
        except Exception as exc:
            log.warning("debug log failed: %s", exc, exc=ChatHealthyException(
                                                      mode="debug_log_write_failed",
                                                      message=f"debug log write failed: {exc}",
                                                      component="DebugLogger",
                                                      exception=exc,
                                                  ), if_not_debug_log=True)
