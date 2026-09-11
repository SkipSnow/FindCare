# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).

"""ChatHealthyLoggingService - canonical logger for ChatHealthy.ai code.

Realizes EPIC-008-F-002-S-011 REQs B-001..B-007 + B-009 + B-010.

Mongo records conform to ChatHealthyLogsSchema.json published at
{env}.chathealthy.ai/schemas/ChatHealthyLogsSchema.json. The schema is
closed (additionalProperties: false); this handler emits only the
fields the schema declares.
"""

from __future__ import annotations

import logging
import os
import sys
import threading
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any, Optional

from .exceptions import ChatHealthyException


# Per-request log context. ASGI middleware in each service binds the
# session_guid at request entry; every log emit on the same async task
# inherits it. Resets at request exit via the token returned from
# bind_request_context().
_log_context: ContextVar[Optional[dict]] = ContextVar("ch_log_context", default=None)


def set_run_id(run_id: Optional[str]) -> None:
    """Bind the run this work belongs to; every later record carries it.

    Without this, a shared log is a merged pile: you can see that something
    happened but not which run it belonged to. Merges into the existing
    context rather than replacing it, so a bound session_guid survives.
    """
    ctx = dict(_log_context.get(None) or {})
    ctx["run_id"] = run_id
    _log_context.set(ctx)


def set_fatal_error(flag: bool) -> None:
    """Declare whether this run is a fatal one; every later record carries it.

    The logger cannot infer this. Only the caller knows whether the work in
    flight is a fatal path, so the caller states it and every record emitted
    afterwards is labelled accordingly. An individual call can still override
    with extra={"fatal_error": ...}.
    """
    ctx = dict(_log_context.get(None) or {})
    ctx["fatal_error"] = bool(flag)
    _log_context.set(ctx)


def set_data_version(data_version: Optional[int]) -> None:
    """Bind the data generation this work is operating on.

    Without it a log cannot answer "which data was this run touching", which
    is the first question asked when investigating a data problem. The caller
    knows; nothing downstream can infer it.
    """
    ctx = dict(_log_context.get(None) or {})
    ctx["data_version"] = data_version
    _log_context.set(ctx)


def bind_request_context(*, session_guid: Optional[str] = None) -> Any:
    """Bind per-request log context. Returns a token to pass to
    clear_request_context() at request end."""
    return _log_context.set({"session_guid": session_guid})


def clear_request_context(token: Any) -> None:
    """Reset to the prior context (called from the request finally block)."""
    _log_context.reset(token)


def bind_user_object_to_log(user_object: Any) -> None:
    """Bind the user_object's GUID to the log context. After this call,
    every log emitted on the same async task carries session_guid +
    user_action=true. Framework call - handle_gate invokes this once
    the user_object exists. Developers using this library NEVER need
    to extract the GUID themselves; user_object is the source of truth.

    Idempotent - re-binding with the same async task overwrites the
    prior value. Per-request isolation is automatic because each
    asgi request runs in its own async task and contextvars are
    per-task.

    Per EPIC-002-F-003-S-003-REQ-B-001 the GUID lives at
    user_object.current_session_token.get_auth_token() (first 32 bytes
    of the assembled session token).
    """
    guid: Optional[str] = None
    try:
        ct = getattr(user_object, "current_session_token", None)
        if ct is not None and hasattr(ct, "get_auth_token"):
            g = ct.get_auth_token()
            if isinstance(g, str) and len(g) == 32:
                guid = g
    except Exception:
        guid = None
    _log_context.set({"session_guid": guid})


# Third-party libraries narrate their internals at DEBUG. None of it is a
# business event, and it belongs on no destination -- not the shared log and
# not the console. Applied to EVERY handler, because a driver monitor thread
# writing to a closed stderr during teardown is just as unwanted as it is in
# Mongo. No trailing dots: records logged under the bare name "pymongo" slip
# past a "pymongo." prefix.
_NOISE_PREFIXES = ("pymongo", "azure", "urllib3", "msal", "httpcore", "httpx", "openai")


def _noise_filter(r: logging.LogRecord) -> bool:
    n = r.name or ""
    return not any(n.startswith(p) for p in _NOISE_PREFIXES)


_FORMAT = "%(asctime)s %(levelname)s %(name)s - %(message)s"
_DATEFMT = "%Y-%m-%d %H:%M:%S"

_lock = threading.Lock()
_bound_destinations: Optional[tuple[str, ...]] = None
_bound_level: Optional[int] = None
_mongo_log_identity: Optional[str] = None

# One log for one business process. The system is distributed across pipeline
# workers, runbooks and front-end services; the business process is not. Every
# component writes to this single collection and distinguishes itself by the
# env, component and job_id FIELDS on each record.
LOG_COLLECTION = "Log"
# The log database is a DEPLOYED fact, read from the environment. There is no
# argument and no setter: a caller that could choose its own log destination
# could quietly write the record somewhere nobody reads, and the loss would be
# invisible. Absent or unreachable is fatal, never a fallback.
LOG_DB_ENV_VAR = "CH_LOG_DB"


_mongo_log_collection: str = LOG_COLLECTION


def set_mongo_log_collection(collection: str) -> None:
    """Set the collection this process's records are written to.

    Call before setting CH_LOG_DESTINATION="mongo". The database is a
    deployed fact and is not settable; the collection is the caller's,
    because the front-end application and the pipeline share this library
    and are not one stream. A hardcoded collection made them one.

    Args:
        collection: collection name (e.g. "CGFrontEndLogs")
    """
    global _mongo_log_collection
    _mongo_log_collection = collection


def set_mongo_log_identity(identity: str) -> None:
    """Set the MongoDB identity for the logging handler.

    Call this before setting CH_LOG_DESTINATION="mongo" to specify which
    MongoDB certificate identity to use for log persistence.

    Args:
        identity: MongoDB certificate name (e.g., "pipelineEditor")
    """
    global _mongo_log_identity
    _mongo_log_identity = identity


class _Formatter(logging.Formatter):
    """Overrides Formatter.formatException to render ChatHealthyException
    with every attribute and both stack traces (REQ-B-007)."""

    def formatException(self, ei):  # type: ignore[override]
        _, exc_value, _ = ei
        if not isinstance(exc_value, ChatHealthyException):
            return super().formatException(ei)
        attrs = [
            f"mode={exc_value.mode!r}",
            f"message={exc_value.message!r}",
        ]
        if exc_value.component is not None:
            attrs.append(f"component={exc_value.component!r}")
        if exc_value.server is not None:
            attrs.append(f"server={exc_value.server!r}")
        for k, v in (exc_value.context or {}).items():
            attrs.append(f"context.{k}={v!r}")
        head = "ChatHealthyException(" + ", ".join(attrs) + ")"
        # Destination (ChatHealthyException) traceback: when the exception
        # was constructed-only (log.error(exc=ChatHealthyException(...))
        # with no `raise`), Python attached no __traceback__, so
        # formatException returns just "NoneType: None". Fall back to the
        # stack we captured at construction time so the destination always
        # has a real traceback in the formatted log document.
        if exc_value.__traceback__ is not None:
            own_tb = super().formatException(ei)
        else:
            construction_stack = getattr(exc_value, "construction_stack", "") or ""
            own_tb = (
                "Traceback (most recent call last):\n"
                + construction_stack
                + type(exc_value).__name__ + ": " + str(exc_value)
            )
        wrapped = exc_value.exception
        if wrapped is None:
            return head + "\n" + own_tb
        wrapped_ei = (type(wrapped), wrapped, wrapped.__traceback__)
        wrapped_tb = super().formatException(wrapped_ei)
        return (
            head + "\n"
            + "--- ChatHealthyException traceback ---\n" + own_tb + "\n"
            + "--- wrapped " + type(wrapped).__name__ + " traceback ---\n" + wrapped_tb
        )


class _MongoLogHandler(logging.Handler):
    """Synchronous writer to Pipelines.Log_{env}. Each emit produces one
    document conforming to ChatHealthyLogsSchema.json. Libraries throw:
    any pymongo failure is propagated. Python's stdlib
    `Logger.handle()` catches handler exceptions via
    `Handler.handleError()`, which by default writes the exception to
    sys.stderr and continues — that is the standard stdlib behavior for
    a handler that fails. No process kill, no policy decision."""

    def __init__(self, env: str, target: str,
                 collection: str = LOG_COLLECTION) -> None:
        super().__init__()
        self._env = env
        self._target = target
        # Which stream this process's records belong to. The database is a
        # deployed fact and stays one; the collection is the caller's, because
        # the front-end application writes one stream and the pipeline writes
        # another, and they share this library.
        self._collection = collection
        # Deployed fact. No argument, no default: if the deployment did not
        # say where logs go, this process must not start pretending it logged.
        self._log_db = os.environ.get(LOG_DB_ENV_VAR, "").strip()
        if not self._log_db:
            raise ChatHealthyException(
                mode="log_db_not_configured",
                component="ChatHealthyLoggingService",
                message=(
                    f"{LOG_DB_ENV_VAR} is not set. The log database is a deployed "
                    "fact and has no default; a process that cannot name its log "
                    "destination MUST NOT run."
                ),
            )
        self._coll = None  # lazy - connect on first emit
        self._lock = threading.Lock()
        # Re-entrancy guard: any log records produced by code we call
        # while emitting (mongo_utilities __init__'s 'MongoClient
        # established' line, pymongo monitor thread) get DROPPED to
        # break the deadlock cycle. The file handler still gets them.
        self._in_emit = threading.local()
        # Pymongo's own records would recurse back through here via
        # the monitor thread - drop them at handler entry. Also drop
        # azure.core.pipeline.policies.http_logging_policy noise which
        # would otherwise flood Log_dev with per-HTTP-request docs.
        self.addFilter(_noise_filter)
        self._prove_writable()

    def _prove_writable(self) -> None:
        """Connect and write one real record, now, at construction.

        A log handler that cannot write is worse than no handler: the process
        runs, believes it is logging, and leaves no trace of what it did. So
        the write is proven at startup rather than discovered later. Uses the
        normal emit path so the probe record conforms to the same schema as
        every other record. Failure raises; the caller treats it as fatal.
        """
        try:
            self.emit(logging.LogRecord(
                name="ChatHealthyLoggingService", level=logging.INFO,
                # Not __file__: the build inlines this module into every
                # Automation runbook, and inlined code has no __file__. The
                # probe raised NameError there, so a runbook proving it could
                # log was the thing that stopped it running.
                pathname=globals().get("__file__", "chathealthy_lib/logging_service.py"),
                lineno=0,
                msg="log handler initialised: db=%s target=%s env=%s",
                args=(self._log_db, self._target, self._env),
                exc_info=None,
            ))
        except Exception as exc:
            raise ChatHealthyException(
                mode="log_db_unwritable",
                component="ChatHealthyLoggingService",
                message=(
                    f"Cannot write to log database {self._log_db!r}: "
                    f"{type(exc).__name__}: {exc}. A process that cannot record "
                    "what it does MUST NOT run."
                ),
                exception=exc,
            ) from exc

    def _get_coll(self):
        if self._coll is None:
            with self._lock:
                if self._coll is None:
                    # Lazy import resolves the circular with
                    # mongo_utilities (which imports this module). The
                    # raw client bypasses TimedClient because TimedClient
                    # logs every op, which would recurse here.
                    from .mongo_utilities import ChatHealthyMongoUtilities
                    # Logs are operational records: admin target, which must
                    # answer 24x7 even while the pipeline factory is down.
                    timed_client = ChatHealthyMongoUtilities().getConnection(
                        _mongo_log_identity, "ChatHealthyFrontEnd"
                    )
                    raw_client = timed_client._client
                    # ONE log. The system is distributed but the business
                    # process is single, so every component writes here and
                    # env/component/job_id are FIELDS, not collection names.
                    # A per-env collection duplicates the env field and makes
                    # "what happened during run X" unanswerable across
                    # components.
                    self._coll = raw_client[self._log_db][
                        f"{self._collection}_{self._env}"]
        return self._coll

    def emit(self, record: logging.LogRecord) -> None:
        if getattr(self._in_emit, "value", False):
            return
        self._in_emit.value = True
        try:
            ctx = _log_context.get(None) or {}
            ctx_guid = ctx.get("session_guid")
            doc: dict = {
                "timeStamp": datetime.now(timezone.utc),
                "level": record.levelname,
                # Read live rather than using the value captured when this
                # handler was built. _ensure_configured() only rebuilds on a
                # destination or level change, so a handler constructed during
                # import would otherwise label every record with whatever
                # ENV_PREFIX held at import time -- and env is now a filter on
                # a shared collection, so a stale label is a wrong answer.
                "env": os.environ.get("ENV_PREFIX", "").strip() or self._env,
                "component": self._target,
                "pipeline_name": os.environ.get("PIPELINE_NAME", "").strip() or None,
                # The run this record belongs to. Set via set_run_id(); this
                # is what makes one shared log queryable per run rather than
                # only by timestamp.
                "run_id": ctx.get("run_id"),
                # Declared by the caller via set_data_version(). Falls back
                # to the DATA_VERSION env var for runbooks that set it.
                "data_version": (
                    ctx.get("data_version")
                    if ctx.get("data_version") is not None
                    else (int(os.environ["DATA_VERSION"])
                          if os.environ.get("DATA_VERSION", "").strip().isdigit()
                          else None)
                ),
                # The rendered line, prefix and all.
                "formatted": self.format(record),
                # The message alone, with args interpolated and no timestamp
                # or level prefix. `formatted` is for reading; this is what
                # you group, match and aggregate on.
                "message": record.getMessage(),
                "pathname": record.pathname,
                # Declared by the caller via set_fatal_error(); False only
                # because nobody has said otherwise, not because we decided
                # it isn't fatal. A single call can still override below.
                "fatal_error": bool(ctx.get("fatal_error", False)),
                "user_action": bool(ctx_guid),
                # session_guid is ALWAYS present on every doc. Null when
                # there is no user session in flight (system / startup /
                # background); the GUID string when there is.
                "session_guid": ctx_guid,
            }
            # Optional source-location fields per schema.
            if record.lineno is not None:
                doc["lineno"] = record.lineno
            if record.funcName:
                doc["funcName"] = record.funcName
            # Exception type as a structured query field. The full
            # tracebacks live inside `formatted` because the _Formatter
            # renders them.
            if record.exc_info:
                exc_type = record.exc_info[0]
                if exc_type is not None:
                    doc["exc_type"] = exc_type.__name__
            # Schema-defined extras: callers pass extra={...} on log()
            # to override these defaults. Each is a direct check - no
            # abstraction, no policy.
            if hasattr(record, "fatal_error"):
                doc["fatal_error"] = record.fatal_error
            if hasattr(record, "user_action"):
                doc["user_action"] = record.user_action
            if hasattr(record, "session_guid"):
                doc["session_guid"] = record.session_guid
            if hasattr(record, "http_status"):
                doc["http_status"] = record.http_status
            if hasattr(record, "http_path"):
                doc["http_path"] = record.http_path
            self._get_coll().insert_one(doc)
        finally:
            self._in_emit.value = False


def _build_mongo_handler() -> logging.Handler:
    """Build the Mongo handler. Called ONLY when 'mongo' is in
    CH_LOG_DESTINATION. Raises ChatHealthyException if any required env
    binding or identity is missing - libraries throw, callers decide. If 'mongo' is
    NOT in CH_LOG_DESTINATION, this function is never called and
    mongo_utilities is never imported."""
    if not _mongo_log_identity:
        raise ChatHealthyException(
            mode="mongo_log_identity_not_set",
            message=(
                "ChatHealthyLoggingService cannot wire the Mongo handler "
                "because mongo_log_identity is not set. Call set_mongo_log_identity(identity) "
                "before setting CH_LOG_DESTINATION='mongo'."
            ),
            component="ChatHealthyLoggingService",
        )
    target = os.environ.get("CH_SPACE_NAME", "").strip()
    env = os.environ.get("ENV_PREFIX", "").strip()
    missing = []
    if not target:
        missing.append("CH_SPACE_NAME")
    if not env:
        missing.append("ENV_PREFIX")
    if missing:
        raise ChatHealthyException(
            mode="mongo_log_handler_env_unset",
            message=(
                "ChatHealthyLoggingService cannot wire the Mongo handler "
                "because required env binding(s) are missing: "
                f"{', '.join(missing)}. Either set them or remove 'mongo' "
                "from CH_LOG_DESTINATION."
            ),
            component="ChatHealthyLoggingService",
            missing=",".join(missing),
        )
    h = _MongoLogHandler(env=env, target=target,
                         collection=_mongo_log_collection)
    h.setFormatter(_Formatter(fmt=_FORMAT, datefmt=_DATEFMT))
    return h


def _compute_destinations() -> tuple[str, ...]:
    """Parse CH_LOG_DESTINATION as a comma-separated list of destinations.

    Each token is one of: 'stdout', 'stderr', 'mongo', or a file path.
    Default (env unset): ('./logs', 'mongo') - preserves prior wiring
    for existing runtime callers that set the Mongo env vars.
    Callers who do not want Mongo (tests, bootstrap early) set
    CH_LOG_DESTINATION explicitly WITHOUT 'mongo'.
    """
    raw = os.environ.get("CH_LOG_DESTINATION")
    if raw is None:
        # Default is stderr-only. Callers that need Mongo persistence
        # set CH_LOG_DESTINATION="stderr,mongo" explicitly AND ensure
        # CH_SPACE_NAME + ENV_PREFIX are set BEFORE their first log()
        # call (per _build_mongo_handler's missing-env raise contract).
        # No credential is among them: the handler connects with the
        # logging identity's certificate.
        return ("stderr",)
    return tuple(t.strip() for t in raw.split(",") if t.strip())


def _compute_level() -> int:
    name = os.environ.get("CH_LOG_LEVEL", "INFO").upper()
    mapped = logging.getLevelName(name)
    return mapped if isinstance(mapped, int) else logging.INFO


def _build_handler_for(destination: str) -> logging.Handler:
    if destination == "stdout":
        h: logging.Handler = logging.StreamHandler(stream=sys.stdout)
    elif destination == "stderr":
        h = logging.StreamHandler()
    elif destination == "mongo":
        return _build_mongo_handler()
    else:
        # File path. Append component name so multiple services in the
        # same directory get distinct files.
        comp = os.environ.get("CH_COMPONENT", "chathealthy")
        path = f"{destination.rstrip('/')}/{comp}.log"
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        h = logging.FileHandler(path, mode="a", encoding="utf-8")
    h.setFormatter(_Formatter(fmt=_FORMAT, datefmt=_DATEFMT))
    h.addFilter(_noise_filter)
    return h


def _ensure_configured() -> None:
    """Re-read env vars; rebind if the destination list or level changed."""
    global _bound_destinations, _bound_level
    dests = _compute_destinations()
    level = _compute_level()
    if dests == _bound_destinations and level == _bound_level:
        return
    with _lock:
        if dests == _bound_destinations and level == _bound_level:
            return
        handlers = [_build_handler_for(d) for d in dests]
        logging.basicConfig(level=level, handlers=handlers, force=True)
        _bound_destinations = dests
        _bound_level = level


class ChatHealthyLoggingService:
    """Canonical logger. Constructor takes zero arguments.

    See ChatHealthyLib/architectureAndDesign/
        EPIC-003-F-005-Manage-Logging-design-v9.docx.
    """

    def __init__(self) -> None:
        pass

    @staticmethod
    def _debug_mode_on() -> bool:
        return logging.getLogger().isEnabledFor(logging.DEBUG)

    def _emit(
        self,
        level: int,
        msg: str,
        args: tuple,
        exc: Optional[ChatHealthyException],
        if_not_debug_log: bool,
        kw: dict,
    ) -> None:
        _ensure_configured()
        if exc is not None:
            if not isinstance(exc, ChatHealthyException):
                raise ChatHealthyException(
                    mode="logger_exception_not_chathealthy",
                    message=(
                        "The exc argument passed to ChatHealthyLoggingService "
                        "log methods MUST be a ChatHealthyException; got "
                        f"{type(exc).__name__}."
                    ),
                    component="ChatHealthyLoggingService",
                )
            kw["exc_info"] = (type(exc), exc, exc.__traceback__)
        # if_not_debug_log means "this record is only interesting when
        # debugging" (EPIC-008-F-002-S-009-REQ-B-008, Mode 1: expected,
        # recovered from, user sees nothing). It was accepted by every log
        # method, threaded down here, and never read -- so 73 call sites that
        # believed they were quiet have been emitting at INFO in production.
        # Demoting to DEBUG is what makes the flag mean what it says.
        if if_not_debug_log and level > logging.DEBUG:
            level = logging.DEBUG
        kw.setdefault("stacklevel", 3)
        logging.getLogger().log(level, msg, *args, **kw)

    def debug(
        self, msg: str, *args,
        exc: Optional[ChatHealthyException] = None,
        if_not_debug_log: bool = False, **kw,
    ) -> None:
        self._emit(logging.DEBUG, msg, args, exc, if_not_debug_log, kw)

    def info(
        self, msg: str, *args,
        exc: Optional[ChatHealthyException] = None,
        if_not_debug_log: bool = False, **kw,
    ) -> None:
        self._emit(logging.INFO, msg, args, exc, if_not_debug_log, kw)

    def warning(
        self, msg: str, *args,
        exc: Optional[ChatHealthyException] = None,
        if_not_debug_log: bool = False, **kw,
    ) -> None:
        self._emit(logging.WARNING, msg, args, exc, if_not_debug_log, kw)

    def error(
        self, msg: str, *args,
        exc: Optional[ChatHealthyException] = None,
        if_not_debug_log: bool = False, **kw,
    ) -> None:
        self._emit(logging.ERROR, msg, args, exc, if_not_debug_log, kw)

    def critical(
        self, msg: str, *args,
        exc: Optional[ChatHealthyException] = None,
        if_not_debug_log: bool = False, **kw,
    ) -> None:
        self._emit(logging.CRITICAL, msg, args, exc, if_not_debug_log, kw)

    def exception(
        self, msg: str, *args,
        exc: Optional[ChatHealthyException] = None,
        if_not_debug_log: bool = False, **kw,
    ) -> None:
        if exc is None:
            kw.setdefault("exc_info", True)
        self._emit(logging.ERROR, msg, args, exc, if_not_debug_log, kw)
