# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).

from datetime import datetime, timezone

from ..exceptions import ChatHealthyException


class Nonce:
    STAMP_SIZE = 17
    SEPARATOR = 'X'
    SIZE = STAMP_SIZE + 1 + STAMP_SIZE
    _STRFTIME = '%Y%m%d%H%M%S'

    @classmethod
    def fresh(cls) -> str:
        s = cls._now_stamp()
        return f"{s}{cls.SEPARATOR}{s}"

    @classmethod
    def restamp(cls, field: str) -> str:
        cls._validate(field)
        original = field[:cls.STAMP_SIZE]
        return f"{original}{cls.SEPARATOR}{cls._now_stamp()}"

    @classmethod
    def latest_stamp(cls, field: str) -> str:
        cls._validate(field)
        return field[cls.STAMP_SIZE + 1:]

    @classmethod
    def original_stamp(cls, field: str) -> str:
        cls._validate(field)
        return field[:cls.STAMP_SIZE]

    @classmethod
    def stamped_at(cls, stamp: str) -> datetime:
        """The moment one stamp records."""
        if len(stamp) != cls.STAMP_SIZE:
            raise ChatHealthyException(
            mode="value_error",
            component="nonce",
            message=f"stamp length {len(stamp)} != {cls.STAMP_SIZE}")
        try:
            when = datetime.strptime(stamp[:14], cls._STRFTIME)
            ms = int(stamp[14:])
        except ValueError as exc:
            raise ChatHealthyException(
            mode="value_error",
            component="nonce",
            message=f"stamp {stamp!r} is not a {cls._STRFTIME} time plus milliseconds",
            exception=exc) from exc
        return when.replace(tzinfo=timezone.utc, microsecond=ms * 1000)

    @classmethod
    def age_seconds(cls, field: str) -> float:
        """Seconds since the last hop stamped this nonce.

        The latest stamp, because that is the one that moves: the original
        records when the session was issued and never changes, so it says
        nothing about whether anyone is still here.
        """
        return (datetime.now(timezone.utc)
                - cls.stamped_at(cls.latest_stamp(field))).total_seconds()

    @classmethod
    def is_expired(cls, field: str, ttl_seconds: float) -> bool:
        """Whether the last hop is further back than the window allows.
        The holder states the window."""
        return cls.age_seconds(field) > ttl_seconds

    @classmethod
    def _now_stamp(cls) -> str:
        now = datetime.now(timezone.utc)
        ms = str(now.microsecond)[:3].zfill(3)
        return now.strftime(cls._STRFTIME) + ms

    @classmethod
    def _validate(cls, field: str) -> None:
        if len(field) != cls.SIZE:
            raise ChatHealthyException(
            mode="value_error",
            component="nonce",
            message=f"nonce length {len(field)} != {cls.SIZE}")
        if field[cls.STAMP_SIZE] != cls.SEPARATOR:
            raise ChatHealthyException(
            mode="value_error",
            component="nonce",
            message=f"missing '{cls.SEPARATOR}' separator at byte {cls.STAMP_SIZE}")
