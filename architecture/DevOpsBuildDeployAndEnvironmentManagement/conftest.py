# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
#
# Failures are written out as they happen. The hook lives here because
# pytest only calls runtest hooks from a conftest.

import os
import socket
import ssl
import urllib.parse

import pytest

from chathealthy_lib import ChatHealthyLoggingService

from find_care_windows_uat_test import VIEWPORT, _triage

log = ChatHealthyLoggingService()

BASE = os.environ.get("CH_SMOKE_BASE", "https://localhost")


def _certificate_is_trusted(url: str) -> tuple[bool, str]:
    """Open one TLS connection with verification ON and report what happened.

    Every browser context in this suite sets ignore_https_errors, because the
    assertions are about the application rather than the transport. That makes
    the suite structurally blind to a certificate failure: on 2026-09-10 the
    local stack reported 15/15 green for an afternoon while no browser could
    load the site at all, because nothing here ever verified a chain.

    This does not turn verification on for the tests. It asks the question
    once, separately, so the answer is stated rather than assumed.
    """
    parts = urllib.parse.urlparse(url)
    host = parts.hostname or "localhost"
    port = parts.port or 443
    try:
        context = ssl.create_default_context()          # verification ON
        with socket.create_connection((host, port), timeout=10) as raw:
            with context.wrap_socket(raw, server_hostname=host) as tls:
                peer = tls.getpeercert()
        issuer = dict(x[0] for x in peer.get("issuer", ())).get(
            "commonName", "unknown")
        return True, f"chain verified, issued by {issuer}"
    except ssl.SSLCertVerificationError as exc:
        return False, f"{exc.verify_message or exc}"
    except OSError as exc:
        return False, f"could not connect: {type(exc).__name__}: {exc}"


@pytest.fixture(scope="session", autouse=True)
def certificate_trust_warning():
    """Say plainly, once per run, whether a real browser would accept this
    site -- because every test below is configured not to notice."""
    trusted, detail = _certificate_is_trusted(BASE)
    if not trusted:
        banner = (
            "\n"
            "  ================= SECURITY WARNING =================\n"
            f"  {BASE} DOES NOT PRESENT A TRUSTED CERTIFICATE.\n"
            f"  {detail}\n"
            "\n"
            "  Every test in this run sets ignore_https_errors, so they\n"
            "  will pass regardless. A real browser will refuse to load\n"
            "  this site. Green below does NOT mean a user can reach it.\n"
            "  ====================================================\n"
        )
        log.warning(banner)
    else:
        log.info("certificate: %s", detail)
    yield


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if report.when in ("setup", "call") and report.failed:
        _triage(item.name, VIEWPORT,
                report.longreprtext or str(report.longrepr))
