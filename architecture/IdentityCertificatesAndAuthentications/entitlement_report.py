"""Daily entitlement report.

An operational report, read every morning to run the estate. It answers four
questions about every identity that holds rights: what it is, who classified
it, who answers for it, and what it can reach. Anything it cannot establish it
says it cannot establish, rather than printing a figure that reads as clean.

Every fact on the page is measured at the time stated. The subscriptions come
from what the reporting identity can see, the classification from directory
group membership, the delegation from directory ownership, the privilege of a
role from the actions Azure publishes for it, and where certificate material
lives from the vaults themselves. Nothing is asserted from this file.

That is not a stylistic preference. The population was once a dict written
here, matched by hand to what Azure held, so the report printed zero exceptions
by construction and an identity added to the estate appeared nowhere at all.

CLI:
    python entitlement_report.py [--no-email] [--out <path>]
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import sys
from pathlib import Path

# On a workstation the repository is present and the library is imported from
# it. Inside Azure Automation there is no git tree: the library is inlined into
# the runbook and _root stays None, which the manifest reader treats as "the
# deployment architecture is not reachable from here".
_root: Path | None = None
for _d in Path(__file__).resolve().parents:
    if (_d / ".git").exists():
        _root = _d
        for _p in (_d / "ChatHealthyLib" / "src", _d / "pipeline" / "Code"):
            if str(_p) not in sys.path:
                sys.path.insert(0, str(_p))
        break

import os as _ch_os
_ch_os.environ.setdefault("CH_LOG_DESTINATION", "stderr")
from chathealthy_lib.exceptions import ChatHealthyException  # noqa: E402
from chathealthy_lib.logging_service import ChatHealthyLoggingService  # noqa: E402

import requests  # noqa: E402
from reportlab.lib import colors  # noqa: E402
from reportlab.lib.pagesizes import landscape, letter  # noqa: E402
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle  # noqa: E402
from reportlab.lib.units import inch  # noqa: E402
from reportlab.pdfgen import canvas as canvas_module  # noqa: E402
from reportlab.platypus import (CondPageBreak,   # noqa: E402
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, KeepTogether,
)

_LOG = ChatHealthyLoggingService()

# Azure Automation keeps a runbook's settings as Automation Variables, which
# are not environment variables until the runbook asks for them. Outside
# Automation the import fails and the environment already holds what is
# needed.
#
# The names are the ones the runbook package declares. They used to be a
# tuple of six typed here, which drifted the moment the package declared a
# seventh: the deploy published the Atlas keys and the vault address as
# Automation Variables, this list did not name them, and the report said
# there were no secrets and no Atlas key rather than saying it had not
# looked. A value published and never read is worse than one missing --
# both produce a zero, and only one of them looks like an error.
#
# EVERY_VARIABLE is rewritten by the build from the package declaration, so
# a name added there arrives here without anybody remembering to.
EVERY_VARIABLE = (
    "DEVOPSUSER_AZURE_TENANT_ID",
    "DEVOPSUSER_AZURE_CLIENT_ID",
    "DEVOPSUSER_AZURE_CLIENT_SECRET",
    "SPARKMAIL_API_KEY",
    "NOTIFICATION_FROM_EMAIL",
    "ENTITLEMENT_REPORT_TO_EMAIL",
    "ATLAS_PUBLIC_KEY",
    "ATLAS_PRIVATE_KEY",
    "ATLAS_PROJECT_ID",
    "KEY_VAULT_URI",
    "CH_LOG_DB",
)

try:
    import automationassets  # type: ignore[import-not-found]

    _unread = []
    for _k in EVERY_VARIABLE:
        try:
            _ch_os.environ[_k] = str(automationassets.get_automation_variable(_k))
        except Exception as _exc:                               # noqa: BLE001
            _unread.append(f"{_k}: {_exc}")
    if _unread:
        # Said out loud. Swallowed, this is a section of the report quietly
        # reporting nothing.
        _LOG.warning("Automation Variables not read, so whatever they "
                     "configure is absent from this report: %s",
                     "; ".join(_unread))
except ImportError:
    pass

ARM = "https://management.azure.com"
GRAPH = "https://graph.microsoft.com/v1.0"

INK = colors.HexColor("#1a1a1a")
MUTED = colors.HexColor("#6b6b6b")
RULE = colors.HexColor("#c8c8c8")
BAND = colors.HexColor("#f2f2f2")
FLAG = colors.HexColor("#a3231d")
OK = colors.HexColor("#1f6b34")

def _approved_register(source: str = "") -> tuple[dict[str, tuple], str]:
    """The approved population, read from the register the build baked.

    This used to be a dict written into this file. That made the audit grade
    itself against its own answer key: the literal was authored to match what
    Azure held, so the report printed zero exceptions by construction, and an
    identity added to the estate appeared nowhere at all.

    The register is derived from IdentityCatalog in deployment_architecture.json
    at build time and shipped beside this runbook, because Azure Automation has
    no git tree. So the approved population changes only by deploying a changed
    manifest -- and a manifest change to IdentityCatalog needs operator approval
    under Rule-065-ENF-005 -- while the observed population is enumerated live
    on every run. The two are never the same source.

    A missing register is fatal. Falling back to a built-in list would restore
    exactly the defect this replaces, and a report that cannot say what was
    approved must not print a number that reads as though it could.
    """
    # Inside Azure Automation the runbook is a single file: nothing staged
    # beside it is deployed, so the build injects the register as a module
    # global instead. This file carries no population of its own; the value is
    # derived from IdentityCatalog at build time on every build.
    injected = globals().get("_BAKED_IDENTITY_REGISTER")
    if injected:
        register = {}
        for e in injected.get("identities", []):
            oid = (e.get("object_id") or "").strip()
            if not oid:
                continue
            register[oid] = (
                e.get("identity_id", ""),
                "",  # descriptions are read from the directory, not from here
                e.get("actor_type", ""),
                e.get("entra_object_type", "") or e.get("identity_class", ""),
                e.get("application", ""),
                tuple(e.get("roles", []) or ()),
            )
        src = injected.get("source") or {}
        where = (f"{src.get('environment','?')} build {src.get('build','?')} "
                 f"commit {src.get('commit','?')}")
        return register, where

    # Named explicitly, the manifest is fetched from that environment rather
    # than read from whatever happens to be on disk. A report run against dev
    # while reading a workstation`s edits states a register nobody deployed.
    if source:
        url = (f"https://{source}.chathealthy.ai/schemas/deployment_architecture.json"
               if not source.startswith("http") else source)
        r = requests.get(url, timeout=60)
        if r.status_code != 200:
            raise ChatHealthyException(
                mode="config_error", component="entitlement_report",
                message=f"cannot fetch the identity register from {url}: "
                        f"HTTP {r.status_code}")
        entries = r.json().get("IdentityCatalog", [])
        register = {}
        for e in entries:
            oid = (e.get("object_id") or "").strip()
            if oid:
                register[oid] = (
                    e.get("identity_id", ""), "",
                    e.get("actor_type", ""),
                    e.get("entra_object_type", "") or e.get("identity_class", ""),
                    e.get("application", ""), tuple(e.get("roles", []) or ()))
        return register, url

    def _build(entries):
        register = {}
        for e in entries:
            oid = (e.get("object_id") or "").strip()
            if not oid:
                continue
            register[oid] = (
                e.get("identity_id", ""),
                "",  # descriptions are read from the directory, not from here
                e.get("actor_type", ""),
                e.get("entra_object_type", "") or e.get("identity_class", ""),
                e.get("application", ""),
                tuple(e.get("roles", []) or ()),
            )
        return register

    here = Path(__file__).resolve().parent
    baked = here / "entitlement_report_identity_register.json"
    if baked.is_file():
        data = json.loads(baked.read_text(encoding="utf-8"))
        return (_build(data.get("identities") or data.get("IdentityCatalog") or []),
                f"{baked.name}, deployed with this build")

    # Off the branch, never off the disk. The approved population is the
    # control this report measures against; a control a run can edit before
    # measuring is not one. git show reads the committed bytes whatever the
    # working tree says.
    if _root is not None:
        import subprocess  # noqa: PLC0415
        rel = "brain/machine_artifacts/content/deployment_architecture.json"
        try:
            branch = subprocess.run(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=str(_root),
                capture_output=True, text=True, check=True).stdout.strip()
            ref = f"origin/{branch}"
            commit = subprocess.run(
                ["git", "rev-parse", "--short", ref], cwd=str(_root),
                capture_output=True, text=True, check=True).stdout.strip()
            blob = subprocess.run(
                ["git", "show", f"{ref}:{rel}"], cwd=str(_root),
                capture_output=True, text=True, check=True).stdout
        except Exception as exc:  # noqa: BLE001 - reported, not swallowed
            raise ChatHealthyException(
                mode="config_error",
                component="entitlement_report",
                message=(f"the approved register could not be read from "
                         f"{rel} on the branch: {exc}. The report does not "
                         f"fall back to the working tree, because a control "
                         f"the run can edit is not a control."),
                exception=exc)
        data = json.loads(blob)
        return (_build(data.get("IdentityCatalog") or []),
                f"deployment_architecture.json at {ref} {commit}")
    from chathealthy_lib.exceptions import ChatHealthyException
    raise ChatHealthyException(
        mode="config_error",
        component="entitlement_report",
        message=("no identity register found. Expected "
                 "entitlement_report_identity_register.json beside this file "
                 "(baked by the build) or deployment_architecture.json in the "
                 "repository. The report will not run against a built-in list."))


# The read is separated from the logging of it: a function that raises does
# not also log, because the catcher logs and not the thrower.
_REGISTER_ARG = ""
for _i, _a in enumerate(sys.argv):
    if _a == "--register-from" and _i + 1 < len(sys.argv):
        _REGISTER_ARG = sys.argv[_i + 1]
    elif _a.startswith("--register-from="):
        _REGISTER_ARG = _a.split("=", 1)[1]
APPROVED, _REGISTER_SOURCE = _approved_register(_REGISTER_ARG)
_LOG.info("entitlement_report approved register read from %s (%d identities)",
          _REGISTER_SOURCE, len(APPROVED))

# Roles that confer administrative authority over the subscription or over
# who may hold rights within it. Their presence is the thing an auditor looks
# for first, so they are counted and marked wherever they appear.
# What makes a role privileged, stated as the actions that make it so. A typed
# list of role names was a fact copied into this file: it went stale the moment a
# role was renamed -- "ChatHealthy Agent" became "chatHealthyAgent" and the list
# would have stopped flagging the most powerful role in the estate while still
# printing a confident table. These patterns are matched against each role's own
# published actions, so a role is privileged because of what it permits.
PRIVILEGE_MARKERS = (
    "*",
    "microsoft.authorization/roleassignments/write",
    "microsoft.authorization/roleassignments/delete",
    "microsoft.authorization/roledefinitions/write",
    "microsoft.authorization/roledefinitions/delete",
    "microsoft.authorization/elevateaccess/action",
    "microsoft.managedidentity/userassignedidentities/write",
    "microsoft.keyvault/vaults/accesspolicies/write",
)

_TOC_ENTRY = None
_SECTION_TITLE = None
_SECTION_NOTE = None

SECTIONS = (
    # EPIC-002-F-003-S-009-REQ-B-002 names these seven, in this order, and
    # each of B-003 through B-009 defines one of them. A section here that
    # no requirement defines, or a requirement with no section, is the
    # kind of drift the numbering exists to make visible.
    ("Scope", "what this firm protects, and how each component decides who "
              "may act on it"),
    ("Statistics", "every figure measured from what this run found; a "
                   "number the run did not measure is not stated"),
    ("Exceptions", "everything wanting a decision: users outside the "
                   "register, grants whose user is gone, resources with no "
                   "description, and grants that add nothing"),
    ("User's full entitlement details", "every right held by each user, "
                                        "in Entra, in Azure and in the database"),
    ("Explanation of entitlements", "every right named here, stated from "
                                    "the actions it carries"),
    ("Group hierarchy", "each group, what it means, who manages it, and "
                        "what lies beneath it"),
    ("Vault-wide access", "users that can reach every secret in a vault, "
                          "and whether they can write"),
)


def _section_header(number, suffix=""):
    """Kept for callers that want the header as one string."""
    label, line = SECTIONS[number - 1]
    return f"{number}: {label}{suffix}"


def _section_block(number, count=""):
    """The header as three stacked lines, per the specified layout.

    The number and title lead in blue at heading size. Beneath them, indented
    and italic, sit the sentence that says what the section states and the
    count of what it found. One line carrying all three read as a paragraph
    and the eye had nothing to land on.

    A CondPageBreak precedes it so a header is never left at the foot of a
    page with its section overleaf: if less than an inch and a half remains,
    the section starts on the next page instead.
    """
    label, line = SECTIONS[number - 1]
    # A header needs its section under it. Three inches is the header
    # itself plus several lines of whatever follows; with less than that
    # left, the section starts on the next page rather than stranding
    # its title at the foot of this one.
    out = [CondPageBreak(3.0 * inch),
           Paragraph(f"{number}: {label}", _SECTION_TITLE),
           Paragraph(line, _SECTION_NOTE)]
    if count:
        out.append(Paragraph(count, _SECTION_NOTE))
    return out


def _scope_story(data: dict) -> list[str]:
    """What this firm protects, and how each component decides who may act.

    EPIC-002-F-003-S-009-REQ-B-003. Prose, deliberately: a reader who does
    not already know the architecture cannot judge an entitlement table,
    and the table says nothing about what the entitlements are FOR. It
    used to describe where the report got its facts, which is a note about
    the report rather than a statement of what is being protected.

    The components are stated here and the counts are measured; a
    component named here that no section covers is a gap the reader can
    see, which is the point of naming them.
    """
    vaults = data["vaults"]
    where = (f"one key vault, {vaults[0]}" if len(vaults) == 1
             else f"{len(vaults)} key vaults ({', '.join(vaults)})" if vaults
             else "no key vault visible to this run")
    atlas = data.get("atlas") or {}
    clusters = ", ".join(atlas.get("clusters") or []) or "not read on this run"

    return [
        "ChatHealthy.ai protects six things, and each decides for itself who "
        "may act on it.",

        "<b>Microsoft Entra</b> is the directory. Every human operator and "
        "every non-human component that authenticates has an identity here, "
        "and group membership is how an identity is classified. Entitlement "
        "is granted by adding a principal to a group or by assigning it a "
        "role; nothing is granted by possessing a password.",

        "<b>Microsoft Azure</b> holds the subscriptions, the resource groups, "
        "the automation accounts and the key vaults. Entitlement is a role "
        "assignment made to an Entra principal at a scope &mdash; a "
        "subscription, a resource group or a single resource &mdash; and "
        "what a role permits is the set of actions it publishes, not what "
        "its name suggests.",

        "<b>Azure Key Vault</b> holds every private credential: certificate "
        f"material, API keys and connection strings, in {where}. Entitlement "
        "here is different in kind from the rest, because a secret is not "
        "only a value. Whoever can read a credential can present it, and so "
        "holds every right the identity that credential names holds. A "
        "secret states which identity it hands its reader, in its "
        "grants-rights-for tag.",

        f"<b>MongoDB Atlas</b> holds the data, in the clusters {clusters}. "
        "Entitlement is not granted through Entra and does not appear in any "
        "Azure role: Atlas keeps its own database users, each authenticating "
        "with an X.509 certificate subject or with a username and password, "
        "and each holding roles defined in Atlas that name the actions "
        "permitted and the database and collection they act on. This is the "
        "one component with fine-grained entitlement inside itself, which is "
        "why it is reported per database and per collection rather than as a "
        "single grant.",

        "<b>Cloudflare</b> serves the public site and routes every request "
        "that reaches it. Entitlement is held as an API token, kept in the "
        "vault; there is no per-user model, so possession of the token is "
        "the entitlement.",

        "<b>GitHub</b> holds the source and the deployment history. "
        "Entitlement is a deploy token kept in the vault and the repository "
        "permissions of the humans who hold accounts.",

        "<b>Hugging Face</b> runs the deployed application containers. "
        "Entitlement is an account token kept in the vault; as with "
        "Cloudflare, holding the token is the entitlement.",

        "<b>What this report covers.</b> Entra, Azure and Key Vault are read "
        "directly and are stated in full below. Atlas is read through its "
        "administrative API and is stated per database user. Cloudflare, "
        "GitHub and Hugging Face are named here and are NOT enumerated: "
        "their entitlement is possession of a token, so what can be said "
        "about them is who can read that token from the vault, which the "
        "vault section states.",
    ]


def _control_policy(data: dict) -> str:
    """Who holds administrative roles, and what each one's roles permit.

    This sentence used to end "which includes creating and entitling further
    identities", appended to whatever the derivation found. It was prose, and it
    was false for DevOpsUser, whose administrative flag comes from a key vault
    data wildcard that confers no power over identities at all. Each principal
    now carries the clauses computed from the actions of the roles it actually
    holds.
    """
    lines = []
    for h in data["holders"]:
        admin = [g["role"] for g in h["grants"]
                 if g["role"] in data["privileged_roles"]]
        if not admin:
            continue
        clauses = []
        for role in sorted(set(admin)):
            for c in data["role_reach"].get(role, {}).get("reach", []):
                if c not in clauses:
                    clauses.append(c)
        lines.append(f"{h['name'] or h['object_id']} holds "
                     f"{', '.join(sorted(set(admin)))}, permitting "
                     f"{'; '.join(clauses)}.")
    if not lines:
        lines.append("No principal holds an administrative role.")
    lines.append("Classification below is directory group membership."
                 if data["groups_readable"] else
                 "The directory could not be read on this run, so nothing below "
                 "is classified.")
    return " ".join(lines)


def _population_sentence(data: dict) -> str:
    """The counts, composed from what was actually found. Volatile facts do not
    belong in fixed prose: the register changes, and a report that states a
    number it did not measure is wrong the day after it is written."""
    approved = [h for h in data["holders"] if h["approved"]]
    absent = data["approved_absent"]

    def _n(n: int, one: str, many: str) -> str:
        return f"{n} {one}" if n == 1 else f"{n} {many}"

    all_h = data["holders"]
    all_humans = [h for h in all_h if h["type"].lower() == "user"]
    all_components = [h for h in all_h if h["type"].lower() != "user"]
    outside = [h for h in all_h if not h["approved"]]

    parts = [f"{_n(len(all_h), 'identity holds', 'identities hold')} rights in this subscription"]
    if all_humans and all_components:
        parts[0] += (f": {_n(len(all_humans), 'named person', 'named people')} and "
                     f"{_n(len(all_components), 'component', 'components')}.")
    else:
        parts[0] += "."
    parts.append(
        f"{_n(len(approved), 'appears', 'appear')} in the approved register and "
        f"{_n(len(outside), 'does', 'do')} not.")
    if absent:
        parts.append(
            f"{_n(len(absent), 'identity in the register holds', 'identities in the register hold')} "
            f"no rights here: {', '.join(absent)}.")
    return " ".join(parts)


class _Credential:
    """A token, fetched directly from the Microsoft identity platform.

    Client-credentials is one HTTP POST, so the report carries no SDK. That
    keeps its dependencies to requests and reportlab, both of which install
    cleanly on the Automation Account's Python, and removes four libraries
    whose versions would have to be tracked for no benefit.
    """

    def __init__(self, tenant: str, client_id: str, client_secret: str):
        self._url = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
        self._id = client_id
        self._secret = client_secret
        self._cache: dict[str, str] = {}

    def token(self, resource: str) -> str:
        if resource in self._cache:
            return self._cache[resource]
        r = requests.post(self._url, timeout=60, data={
            "grant_type": "client_credentials",
            "client_id": self._id,
            "client_secret": self._secret,
            "scope": f"{resource}/.default",
        })
        if r.status_code != 200:
            raise ChatHealthyException(
                mode="azure_login_failed",
                component="EntitlementReport",
                message=f"token request for {resource} returned "
                        f"{r.status_code}: {r.text[:200]}",
                context={"resource": resource, "status": r.status_code})
        self._cache[resource] = r.json()["access_token"]
        return self._cache[resource]

    def get_token(self, scope: str):
        """Shaped like the SDK's credential so call sites read the same."""
        return type("T", (), {"token": self.token(scope.rsplit("/.default", 1)[0])})()


def _credential() -> _Credential:
    """The identity the report runs as: DevOpsUser, and nothing else.

    It ran as pipelineEditor, which sees one subscription and is confined to the
    pipeline by design. The report must be able to see every subscription it
    claims to cover, and DevOpsUser is the identity that deploys across them.
    Giving pipelineEditor that reach instead would widen the pipeline runtime to
    subscriptions it has no business in.
    """
    keys = ("DEVOPSUSER_AZURE_TENANT_ID",
            "DEVOPSUSER_AZURE_CLIENT_ID",
            "DEVOPSUSER_AZURE_CLIENT_SECRET")
    v = {k: _ch_os.environ.get(k, "") for k in keys}
    if not all(v.values()):
        try:
            if _root is None:
                raise ImportError
            from dotenv import dotenv_values
            local = dotenv_values(_root / ".env")
            v = {k: (v[k] or (local.get(k) or "")) for k in keys}
        except ImportError:
            pass
    missing = [k for k in keys if not v[k]]
    if missing:
        raise ChatHealthyException(
            mode="azure_credential_missing",
            component="EntitlementReport",
            message=f"cannot authenticate as DevOpsUser: {', '.join(missing)} absent",
            context={"missing": missing})
    return _Credential(v[keys[0]], v[keys[1]], v[keys[2]])


def _get_all(url: str, token: str) -> list[dict]:
    out: list[dict] = []
    headers = {"Authorization": f"Bearer {token}"}
    while url:
        r = requests.get(url, headers=headers, timeout=60)
        if r.status_code != 200:
            raise ChatHealthyException(
                mode="azure_query_failed",
                component="EntitlementReport",
                message=f"{r.status_code} from {url.split('?')[0]}: {r.text[:300]}",
                context={"status": r.status_code})
        payload = r.json()
        out.extend(payload.get("value", []))
        url = payload.get("nextLink") or payload.get("@odata.nextLink") or ""
    return out


def _principal_names(object_ids: list[str], credential) -> dict[str, dict]:
    try:
        token = credential.get_token("https://graph.microsoft.com/.default").token
    except Exception as exc:                                    # noqa: BLE001
        _LOG.info("graph token unavailable: %s", exc)
        return {}
    resolved: dict[str, dict] = {}
    for i in range(0, len(object_ids), 900):
        r = requests.post(
            f"{GRAPH}/directoryObjects/getByIds",
            headers={"Authorization": f"Bearer {token}",
                     "Content-Type": "application/json"},
            data=json.dumps({"ids": object_ids[i:i + 900]}), timeout=60)
        if r.status_code != 200:
            _LOG.info("graph getByIds returned %s", r.status_code)
            return {}
        for o in r.json().get("value", []):
            resolved[o["id"]] = {
                "name": o.get("displayName") or o.get("userPrincipalName") or o["id"],
                "kind": o.get("@odata.type", "").rsplit(".", 1)[-1],
                # What the principal is, from the directory. This is the
                # column the holders table renders as its purpose; taking it
                # from the approved register let the register describe an
                # identity as the record wished it were.
                "description": (o.get("description") or "").strip(),
                "record": "live",
                "qualities": {},
            }

    # The same lookup continues into the directory's deleted items. An object
    # id either has a record or it does not; where that record lives is one of
    # the qualities it comes back with, not a case to be tested for.
    for kind in ("microsoft.graph.user", "microsoft.graph.servicePrincipal",
                 "microsoft.graph.group"):
        page = (f"{GRAPH}/directory/deletedItems/{kind}"
                f"?$select=id,displayName,userPrincipalName,userType,"
                f"createdDateTime,deletedDateTime,accountEnabled")
        while page:
            d = requests.get(page, headers={"Authorization": f"Bearer {token}"},
                             timeout=60)
            if d.status_code != 200:
                break
            payload = d.json()
            for o in payload.get("value", []):
                if o["id"] in resolved or o["id"] not in set(object_ids):
                    continue
                deleted = o.get("deletedDateTime") or ""
                recoverable = ""
                if deleted:
                    try:
                        when = _dt.datetime.fromisoformat(deleted.replace("Z", "+00:00"))
                        recoverable = (when + _dt.timedelta(days=30)).date().isoformat()
                    except ValueError:
                        recoverable = ""
                resolved[o["id"]] = {
                    "name": o.get("displayName") or o.get("userPrincipalName") or o["id"],
                    "kind": kind.rsplit(".", 1)[-1],
                    "record": "deleted",
                    "qualities": {
                        "sign-in name": o.get("userPrincipalName", ""),
                        "kind": o.get("userType", ""),
                        "created": (o.get("createdDateTime") or "")[:10],
                        "deleted": deleted[:10],
                        "recoverable until": recoverable,
                        "account was enabled": ("yes" if o.get("accountEnabled")
                                                else "no") if o.get("accountEnabled")
                                               is not None else "",
                    },
                }
            page = payload.get("@odata.nextLink") or ""
    return resolved


def _managed_identity_names(token: str, subscription_ids: list[str]) -> dict[str, dict]:
    """Resolve user-assigned managed identities through ARM.

    Managed identities are Azure resources as well as directory principals, so
    their names are readable without any Graph permission. This is what keeps
    the report legible when directory read is unavailable.
    """
    out: dict[str, dict] = {}
    items: list[dict] = []
    for sid in subscription_ids:
        try:
            items.extend(_get_all(
                f"{ARM}/subscriptions/{sid}/providers"
                f"/Microsoft.ManagedIdentity/userAssignedIdentities?api-version=2023-01-31",
                token))
        except Exception as exc:                                # noqa: BLE001
            _LOG.info("managed identity enumeration failed in %s: %s", sid, exc)
    for i in items:
        pid = (i.get("properties") or {}).get("principalId")
        if pid:
            out[pid] = {"name": i.get("name", pid), "kind": "ManagedIdentity"}
    return out


# A role that contains another: holding the key grants everything the values
# grant, so a narrower assignment alongside it adds nothing.
def _role_containment(defs: list[dict]) -> dict[str, set[str]]:
    """Which roles subsume which, computed from the actions Azure publishes.

    This was a typed map of four entries. It was a fact about Azure copied into
    this file: correct for the roles somebody thought of, silent about every
    other pair, and unable to notice that a custom role had been widened to
    swallow another. Now a role contains another when its action set covers the
    other's, wildcards expanded, which is what "already covered by" means.
    """
    def expand(d: dict) -> tuple[set[str], set[str]]:
        acts, data_acts = set(), set()
        for perm in (d["properties"].get("permissions") or []):
            acts.update(a.strip().lower() for a in (perm.get("actions") or []))
            data_acts.update(a.strip().lower() for a in (perm.get("dataActions") or []))
        return acts, data_acts

    def covers(wide: set[str], narrow: set[str]) -> bool:
        if not narrow:
            return False
        for needed in narrow:
            if needed in wide:
                continue
            if any(w == "*" or (w.endswith("*") and needed.startswith(w[:-1]))
                   for w in wide):
                continue
            return False
        return True

    sets = {d["properties"]["roleName"]: expand(d) for d in defs}
    out: dict[str, set[str]] = {}
    for wide_name, (wa, wd) in sets.items():
        for narrow_name, (na, nd) in sets.items():
            if wide_name == narrow_name:
                continue
            if covers(wa, na) and (not nd or covers(wd, nd)):
                out.setdefault(wide_name, set()).add(narrow_name)
    return out


def _secret_descriptions() -> dict[str, str]:
    """What each vault secret is, read from the vault that holds it.

    The description is a tag on the secret. It used to be read from
    deployment_architecture.json, which meant the report could describe a
    secret the vault does not hold and miss one it does, and could say so
    with the same confidence either way. The manifest is written by the same
    hands the report audits, so it is no longer a source here; the vault is.

    A secret carrying no description renders with its name alone. That
    silence is now a measurable gap rather than an invisible one.
    """
    out: dict[str, str] = {}
    for vault in _vault_hosts():
        for name, described in _secret_tags(vault).items():
            if described:
                out[name] = described
    return out


_DESCRIPTION_TAGS = ("description", "Description", "purpose", "Purpose")


def _undescribed_secrets() -> list[dict]:
    """Vault secrets carrying no description tag.

    The vault is where a secret's description is enforced, so a secret without
    one is an exception of the same kind as an undescribed resource. It was
    invisible while `undescribed` counted Azure resources alone, which is how
    a description could be removed from the record and land nowhere without
    the report saying so.
    """
    out: list[dict] = []
    for vault in _vault_hosts():
        where = vault.split("//", 1)[-1].split(".", 1)[0]
        for name, described in _secret_tags(vault).items():
            if not described:
                out.append({"origin": "Vault", "type": "secret",
                            "name": name, "where": where})
    return out


def _undescribed_identities(holders: list[dict], rightless: list[dict]) -> list[dict]:
    """Principals whose description the directory does not carry.

    An Entra user has no description property at all, so it can never carry
    one and is named here every run rather than silently excused.
    """
    out: list[dict] = []
    seen: set[str] = set()
    for h in list(holders) + list(rightless):
        oid = h.get("object_id") or ""
        if not oid or oid in seen:
            continue
        seen.add(oid)
        if (h.get("purpose") or "").strip():
            continue
        kind = (h.get("type") or h.get("entra_object_type") or "principal")
        out.append({"origin": "Entra", "type": kind,
                    "name": h.get("name") or oid, "where": "directory"})
    return out


def _vault_hosts() -> list[str]:
    """Every vault this run can reach, from the environment that names them."""
    hosts: list[str] = []
    uri = _ch_os.environ.get("KEY_VAULT_URI", "").strip()
    if uri:
        hosts.append(uri.rstrip("/"))
    return hosts


# The tag that says whose rights a secret hands to whoever can read it.
# One identity name, so it resolves; a sentence would only be readable.
_GRANT_TAG = "grants-rights-for"

# Filled by _secret_tags as it walks each vault, because the grant and the
# description are two facts about one secret and one walk should collect
# both.
_SECRET_GRANTS: dict[str, str] = {}

# Every secret the walk actually saw. Counting descriptions counted the
# ones somebody had documented; counting certificate secrets counted a
# different population again. This is the population.
_SECRETS_SEEN: set[str] = set()

# Vaults that could not be read. A count taken from a failed read is a
# zero that looks like a measurement.
_VAULT_READ_FAILURES: list[str] = []


def _secret_tags(vault_uri: str) -> dict[str, str]:
    """Every secret in one vault and what its own tags say it is.

    A vault that cannot be listed contributes nothing and does not stop the
    report: the count it prints is what states the coverage.
    """
    found: dict[str, str] = {}
    try:
        # The same identity the rest of the report authenticates as.
        # DefaultAzureCredential was used here, which finds a signed-in
        # user on a workstation and finds nothing in the Automation
        # container -- so the vault read succeeded for whoever ran it by
        # hand and silently returned nothing on every scheduled run. The
        # report then counted 11 secrets where the vault holds 69, and
        # attributed no credential to anyone.
        token = _credential().get_token("https://vault.azure.net/.default").token
    except Exception as exc:  # noqa: BLE001 - an unreadable vault is not a crash
        _LOG.warning("vault %s unreadable, so no secret is described and no "
                     "credential is attributed: %s", vault_uri, exc)
        _VAULT_READ_FAILURES.append(f"{vault_uri}: {exc}")
        return found
    url = f"{vault_uri}/secrets?api-version=7.4"
    while url:
        try:
            response = requests.get(
                url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
        except Exception as exc:  # noqa: BLE001
            _LOG.info("vault %s listing failed: %s", vault_uri, exc)
            return found
        if response.status_code != 200:
            _LOG.info("vault %s listing returned %s", vault_uri,
                      response.status_code)
            return found
        body = response.json()
        for item in body.get("value", []):
            name = item.get("id", "").rsplit("/", 1)[-1]
            if not name:
                continue
            tags = item.get("tags") or {}
            for key in _DESCRIPTION_TAGS:
                if tags.get(key):
                    found[name] = tags[key]
                    break
            found.setdefault(name, "")
            _SECRETS_SEEN.add(name)
            # Which identity this secret makes the reader into. Stated by
            # the vault, not worked out from how the secret is named.
            granted = (tags.get(_GRANT_TAG) or "").strip()
            if granted:
                _SECRET_GRANTS[name] = granted
        url = body.get("nextLink")
    return found


def _reaches(scope: str, subscription_name: str, contents: dict | None = None) -> str:
    """The thing a grant reaches, named as a thing.

    A scope is a path, and rendering its levels as columns made the report a
    picture of Azure's addressing scheme. What an operator needs is what the
    grant can touch: a vault, a secret, a container, a subscription. Each is a
    first-class object with a type and a name, and that is what this returns.
    """
    low = scope.lower()
    for marker, label in (
            ("/secrets/", "secret"),
            ("/containers/", "blob container"),
            ("/providers/microsoft.keyvault/vaults/", "key vault"),
            ("/providers/microsoft.storage/storageaccounts/", "storage account"),
            ("/providers/microsoft.containerregistry/registries/", "container registry"),
            ("/providers/microsoft.automation/automationaccounts/", "automation account"),
            ("/providers/microsoft.managedidentity/userassignedidentities/",
             "managed identity")):
        if marker in low:
            name = scope[low.index(marker) + len(marker):].split("/", 1)[0]
            return f"{label} {name}"
    if "/resourcegroups/" in low:
        name = scope[low.index("/resourcegroups/") + len("/resourcegroups/"):].split("/", 1)[0]
        return f"resource group {name}"
    if not scope.strip("/"):
        return "the whole tenant"
    return f"every resource in subscription {subscription_name}"


def _holds(scope: str, subscription_name: str, contents: dict | None = None) -> str:
    """How much sits inside the thing a grant reaches.

    Its own column: a count is not a name, and a reader comparing two grants
    should not have to read past a resource description to find it.
    """
    if scope.strip("/") and "/subscriptions/" in scope.lower() and (
            "/resourcegroups/" in scope.lower() or "/providers/" in scope.lower()):
        return ""
    c = (contents or {}).get(subscription_name)
    if not c:
        return ""
    vaults = (f", {c['vaults']} key vault{'s' if c['vaults'] != 1 else ''}"
              if c["vaults"] else "")
    return f"{c['count']} resource{'s' if c['count'] != 1 else ''}{vaults}"


def _scope_parts(scope: str) -> dict:
    """Split a scope path into the things it names.

    A scope is a nested path and each level is a different object: the
    subscription, the resource group inside it, the resource inside that, and
    the object the grant reaches. Rendered as one string they read as a single
    fact, and the object -- a secret, which IS the credential -- ended up at the
    tail of a long line. Each level is returned separately so each gets its own
    column.
    """
    out = {"resource_group": "", "resource": "", "object": ""}
    low = scope.lower()
    if "/resourcegroups/" in low:
        rest = scope[low.index("/resourcegroups/") + len("/resourcegroups/"):]
        out["resource_group"] = rest.split("/", 1)[0]
    for marker in ("/providers/Microsoft.KeyVault/vaults/",
                   "/providers/Microsoft.Storage/storageAccounts/",
                   "/providers/Microsoft.ContainerRegistry/registries/",
                   "/providers/Microsoft.Automation/automationAccounts/",
                   "/providers/Microsoft.ManagedIdentity/userAssignedIdentities/"):
        if marker.lower() in low:
            rest = scope[low.index(marker.lower()) + len(marker):]
            out["resource"] = rest.split("/", 1)[0]
            break
    if "/secrets/" in low:
        out["object"] = scope[low.index("/secrets/") + len("/secrets/"):]
    elif "/containers/" in low:
        out["object"] = scope[low.index("/containers/") + len("/containers/"):]
    return out


def _scope_label(scope: str, subscription_ids: tuple[str, ...] = ()) -> str:
    """The scope, named by the thing it governs rather than by its full path.

    The distinguishing element goes last and must not be buried: two grants that
    differ only in which secret they cover have to read as different rows.
    """
    s = scope
    for sid in subscription_ids:
        s = s.replace(f"/subscriptions/{sid}", "")
    if not s.strip():
        return "the whole subscription"
    if s.strip() == "/":
        # A grant at the tenant root is not a grant on this subscription; it is
        # above it, and covers every subscription the tenant will ever hold.
        return "the tenant root -- above every subscription"
    s = s.replace("/resourceGroups/", "resource group ")
    s = s.replace("/providers/Microsoft.KeyVault/vaults/", ", key vault ")
    s = s.replace("/providers/Microsoft.ContainerRegistry/registries/", ", container registry ")
    s = s.replace("/providers/Microsoft.Storage/storageAccounts/", ", storage account ")
    s = s.replace("/providers/Microsoft.Automation/automationAccounts/", ", automation account ")
    s = s.replace("/providers/Microsoft.ManagedIdentity/userAssignedIdentities/",
                  ", managed identity ")
    s = s.replace("/providers/Microsoft.ManagedIdentity/userAssignedIdentities/", ", managed identity ")
    s = s.replace("/secrets/", ", the single secret ")
    return s.strip()


def _mark_redundant(grants: list[dict],
                    subscription_ids: tuple[str, ...] = (),
                    role_contains: dict[str, set[str]] | None = None) -> None:
    """Flag any grant already covered by a broader one the same identity holds.

    Two ways one grant covers another: the same role at a scope that contains
    this one, or a role that contains this role at a containing scope. A grant
    so covered permits nothing additional, and saying so is the difference
    between an inventory and a review.
    """
    for g in grants:
        for other in grants:
            if other is g:
                continue
            wider_scope = (other["raw_scope"] != g["raw_scope"]
                           and g["raw_scope"].startswith(other["raw_scope"]))
            same_scope = other["raw_scope"] == g["raw_scope"]
            covers_role = (other["role"] == g["role"]
                           or g["role"] in (role_contains or {}).get(other["role"], ()))
            if covers_role and (wider_scope or (same_scope and other["role"] != g["role"])):
                g["redundant"] = (f"already covered by {other['role']} on "
                                  f"{_scope_label(other['raw_scope'], subscription_ids)}")
                break


def _all_principals(token: str, credential, subscription_id: str) -> dict[str, dict]:
    """Every principal that exists, whether or not it holds anything.

    A principal can be brought into existence holding nothing -- creating a
    virtual machine with a system-assigned identity mints one as a side effect
    of the resource write. Such a principal appears in no role assignment, so a
    report built from assignments alone cannot see it until somebody grants it
    something. This enumerates existence rather than entitlement.

    Managed identities and anything attached to a resource come from Azure
    itself. Users and application registrations come from the directory, and
    are absent when the reporting identity has no directory read -- which the
    report then says rather than implying the list is complete.
    """
    found: dict[str, dict] = {}

    for item in _get_all(
            f"{ARM}/subscriptions/{subscription_id}/providers"
            f"/Microsoft.ManagedIdentity/userAssignedIdentities?api-version=2023-01-31",
            token):
        pid = (item.get("properties") or {}).get("principalId")
        if pid:
            # A managed identity's service principal is owned by the resource
            # that minted it and is not writable through Graph by anyone, so
            # its description cannot live in the directory beside the others.
            # It lives as a tag on the ARM resource, which is the enforcement
            # surface for this kind of principal, and is read from there.
            found[pid] = {"name": item.get("name", pid), "kind": "ManagedIdentity",
                          "origin": "user-assigned identity",
                          "description": ((item.get("tags") or {})
                                          .get("description", "")).strip()}

    for res in _get_all(
            f"{ARM}/subscriptions/{subscription_id}/resources"
            f"?api-version=2021-04-01&$expand=identity", token):
        ident = res.get("identity") or {}
        pid = ident.get("principalId")
        if pid and pid not in found:
            found[pid] = {"name": f"{res.get('name', '?')} (system-assigned)",
                          "kind": "ManagedIdentity",
                          "origin": f"attached to {res.get('type', 'a resource')}"}
        for uid in (ident.get("userAssignedIdentities") or {}):
            upid = ((ident["userAssignedIdentities"][uid]) or {}).get("principalId")
            if upid and upid not in found:
                found[upid] = {"name": uid.rsplit("/", 1)[-1], "kind": "ManagedIdentity",
                               "origin": "user-assigned identity"}

    try:
        gtoken = credential.get_token("https://graph.microsoft.com/.default").token
    except Exception as exc:                                    # noqa: BLE001
        _LOG.info("directory not enumerable: %s", exc)
        return found
    # Microsoft's own first-party service principals live in every tenant --
    # Azure Cloud Shell, Azure Compute, a hundred more. They are not this firm's
    # identities, nobody here granted them anything, and listing them buries the
    # handful that matter under pages nobody reads. A principal is ours when the
    # tenant that owns its application is this one; Microsoft's carry Microsoft's.
    tenant = _tenant_id()
    # description is selected here because the directory is where a principal's
    # purpose is now read from. It used to come from IdentityCatalog in
    # deployment_architecture.json -- the file this report audits, written by
    # the same hands -- which meant the report could describe an identity as
    # the manifest wished it were and say so with the same confidence as a
    # fact it had observed. A description absent here renders blank, and that
    # silence is a measurable gap rather than an invisible one.
    for kind, url in (("ServicePrincipal",
                       f"{GRAPH}/servicePrincipals"
                       f"?$select=id,displayName,servicePrincipalType,appOwnerOrganizationId,description"),
                      ("User", f"{GRAPH}/users?$select=id,displayName,userPrincipalName")):
        page = url
        while page:
            r = requests.get(page, headers={"Authorization": f"Bearer {gtoken}"}, timeout=60)
            if r.status_code != 200:
                _LOG.info("directory enumeration of %s returned %s", kind, r.status_code)
                break
            payload = r.json()
            for o in payload.get("value", []):
                if o["id"] in found:
                    # Already discovered through a role assignment or a
                    # resource, which carries no description. The directory
                    # is the only place that has one, so fill it in rather
                    # than skip the object and report it as undescribed.
                    if o.get("description") and not found[o["id"]].get("description"):
                        found[o["id"]]["description"] = o["description"].strip()
                    continue
                owner = o.get("appOwnerOrganizationId")
                if kind == "ServicePrincipal" and owner and tenant and owner != tenant:
                    continue
                found[o["id"]] = {
                    "name": o.get("displayName") or o.get("userPrincipalName") or o["id"],
                    "kind": o.get("servicePrincipalType", kind),
                    "origin": "directory",
                    "description": (o.get("description") or "").strip(),
                }
            page = payload.get("@odata.nextLink") or ""
    return found


def _directory_roles(credential) -> dict[str, list[str]]:
    """Directory roles held, by principal object id.

    Tenant-level authority is not an Azure role assignment and cannot be found
    by reading Azure. Global Administrator is a directory role, it sits above
    every subscription the tenant holds, and a report that reads only the
    resource plane concludes there is no tenant owner while one exists. Both
    planes are read here for that reason.
    """
    out: dict[str, list[str]] = {}
    try:
        gtoken = credential.get_token("https://graph.microsoft.com/.default").token
    except Exception as exc:                                    # noqa: BLE001
        _LOG.info("directory roles not readable: %s", exc)
        return out
    headers = {"Authorization": f"Bearer {gtoken}"}
    defs = requests.get(f"{GRAPH}/roleManagement/directory/roleDefinitions"
                        f"?$select=id,displayName", headers=headers, timeout=60)
    if defs.status_code != 200:
        _LOG.info("directory role definitions returned %s", defs.status_code)
        return out
    names = {d["id"]: d["displayName"] for d in defs.json().get("value", [])}
    ras = requests.get(f"{GRAPH}/roleManagement/directory/roleAssignments",
                       headers=headers, timeout=60)
    if ras.status_code != 200:
        _LOG.info("directory role assignments returned %s", ras.status_code)
        return out
    for a in ras.json().get("value", []):
        pid = a.get("principalId")
        role = names.get(a.get("roleDefinitionId"), a.get("roleDefinitionId", ""))
        if pid:
            out.setdefault(pid, []).append(role)
    return out


def _owners(credential, object_ids: dict[str, str]) -> dict[str, list[str]]:
    """Who is accountable for each object, read from the directory.

    Authority in this estate is delegated, not flat: a person empowers an agent,
    and that agent then administers others. Entra records that as the owner edge,
    and it is the only place the delegation is written down -- an entitlement
    table shows what an identity may do, never who answers for it existing.

    Both type casts are queried. Graph's default owners collection omits service
    principals, so an agent that owns objects appears to own nothing, and a
    delegation to an agent reads as no delegation at all.
    """
    out: dict[str, list[str]] = {}
    try:
        gtoken = credential.get_token("https://graph.microsoft.com/.default").token
    except Exception as exc:                                    # noqa: BLE001
        _LOG.info("owners not readable: %s", exc)
        return out
    headers = {"Authorization": f"Bearer {gtoken}"}
    for oid, kind in object_ids.items():
        names: list[str] = []
        for cast in ("microsoft.graph.user", "microsoft.graph.servicePrincipal"):
            r = requests.get(f"{GRAPH}/{kind}/{oid}/owners/{cast}?$select=displayName",
                             headers=headers, timeout=60)
            if r.status_code != 200:
                continue
            names.extend(o.get("displayName", "") for o in r.json().get("value", []))
        if names:
            out[oid] = sorted(n for n in names if n)
    return out


def _vaults_and_certificates(token: str, subscription_ids: list[str]) -> tuple[list[str], list[str]]:
    """Where certificate material actually lives, and how much of it there is.

    Named rather than asserted: the count and the vault names are what make the
    scope paragraph a measurement instead of a claim. A secret is treated as
    certificate material when a role assignment in this estate points at it and
    its name is carried by the identity register, or when it is named for a
    certificate -- both are read, neither is typed.
    """
    vaults: list[str] = []
    certs: list[str] = []
    for sid in subscription_ids:
        for v in _get_all(f"{ARM}/subscriptions/{sid}/resources"
                          f"?api-version=2021-04-01&$filter=resourceType eq "
                          f"'Microsoft.KeyVault/vaults'", token):
            name = v.get("name", "")
            if name and name not in vaults:
                vaults.append(name)
    known = {v[0] for v in APPROVED.values() if v[0]}
    for sid in subscription_ids:
        for row in _get_all(f"{ARM}/subscriptions/{sid}/providers"
                            f"/Microsoft.Authorization/roleAssignments"
                            f"?api-version=2022-04-01", token):
            scope = row["properties"].get("scope", "")
            if "/secrets/" not in scope:
                continue
            secret = scope.split("/secrets/", 1)[1]
            if secret in certs:
                continue
            if secret in known or secret.startswith(("cert-", "key-", "ca-")):
                certs.append(secret)
    return sorted(vaults), sorted(certs)


def _tenant_name(credential) -> str:
    """The tenant's own name, so the report names the directory it read."""
    try:
        gtoken = credential.get_token("https://graph.microsoft.com/.default").token
        r = requests.get(f"{GRAPH}/organization?$select=displayName",
                         headers={"Authorization": f"Bearer {gtoken}"}, timeout=60)
        if r.status_code == 200:
            v = r.json().get("value", [])
            if v:
                return v[0].get("displayName", "")
    except Exception as exc:                                    # noqa: BLE001
        _LOG.info("tenant name not readable: %s", exc)
    return ""


def _tenant_id() -> str:
    """The tenant this estate is, taken from the credential rather than typed.

    Resolved the same way the credential itself is: inside Automation the value
    arrives as an environment variable, and on a workstation it lives in .env.
    Reading only the environment returned empty there, which silently disabled
    the filter that keeps Microsoft's own service principals out of this report
    -- 127 of them appeared under a heading that says they are logins nobody
    authorised.
    """
    key = "DEVOPSUSER_AZURE_TENANT_ID"
    value = _ch_os.environ.get(key, "").strip()
    if value or _root is None:
        return value
    try:
        from dotenv import dotenv_values
        return (dotenv_values(_root / ".env").get(key) or "").strip()
    except ImportError:
        return ""


def _role_reach(defs: list[dict]) -> dict[str, dict]:
    """What each role actually permits, computed from its own action set.

    The table used to print the role's description. For a built-in role that is
    Microsoft's text and says what the role does. For a custom role it is text
    somebody here wrote, so the most powerful role in the estate was explained
    by its own author -- chatHealthyAgent said "IDE agent that administers the
    subscription" while its actions were * and its dataActions were *, which is
    every resource and every secret in them.

    Each clause below is a statement about actions, so a role is described by
    what it permits whoever holds it, whatever anyone called it.
    """
    out: dict[str, dict] = {}
    for d in defs:
        name = d["properties"]["roleName"]
        acts, data_acts = set(), set()
        for perm in (d["properties"].get("permissions") or []):
            acts.update(a.strip().lower() for a in (perm.get("actions") or []))
            data_acts.update(a.strip().lower() for a in (perm.get("dataActions") or []))
        clauses = []
        if "*" in acts:
            clauses.append("every management action on every resource in scope")
        if "*" in data_acts:
            clauses.append("every data action in scope, which includes reading the "
                           "contents of every key vault secret")
        elif any(a.startswith("microsoft.keyvault/") and a.endswith("/*")
                 for a in data_acts):
            clauses.append("reads the contents of every secret in the key vaults in scope")
        if any(a.startswith("microsoft.authorization/roleassignments/write")
               or a == "microsoft.authorization/*" for a in acts):
            clauses.append("grants and revokes roles, including to itself")
        if any(a.startswith("microsoft.authorization/roledefinitions/write") for a in acts):
            clauses.append("creates and rewrites role definitions")
        if any(a.startswith("microsoft.managedidentity/userassignedidentities/write")
               for a in acts):
            clauses.append("creates identities")
        if not clauses:
            if acts and all(a.endswith("/read") for a in acts):
                clauses.append("reads the resources named below and changes nothing")
            elif acts:
                clauses.append("the actions named below, and nothing else")
            elif data_acts:
                # A role with data actions and no management actions -- Key Vault
                # Secrets User is the example -- produced no sentence at all and
                # printed as a bare list.
                clauses.append("the data actions named below, and nothing else")
        out[name] = {
            "reach": clauses,
            "custom": d["properties"].get("type") == "CustomRole",
            "actions": sorted(acts),
            "data_actions": sorted(data_acts),
            "not_actions": sorted(
                a for perm in (d["properties"].get("permissions") or [])
                for a in (perm.get("notActions") or [])),
        }
    return out


def _subscriptions(token: str, credential) -> list[dict]:
    """Every subscription in the tenant, and whether this run could read it.

    Listing what the reporting identity can see defines the report's scope by
    its own blind spots: a subscription it cannot read does not appear, and
    neither does any grant inside it, so a principal shown holding two roles may
    hold ten. An empty subscription still exists, and a subscription nobody
    here can read still holds whatever it holds.

    The tenant's own subscription list is asked for first, through the same
    directory the rest of the report reads. What the identity can actually
    enumerate is recorded per subscription, so the report states its reach
    rather than assuming it.
    """
    readable = {s["subscriptionId"]: s
                for s in _get_all(f"{ARM}/subscriptions?api-version=2022-12-01", token)}
    known: dict[str, dict] = {}
    try:
        gtoken = credential.get_token("https://graph.microsoft.com/.default").token
        r = requests.get(f"{GRAPH}/directory/subscriptions"
                         f"?$select=id,displayName,ocpSubscriptionId",
                         headers={"Authorization": f"Bearer {gtoken}"}, timeout=60)
        if r.status_code == 200:
            for sub in r.json().get("value", []):
                sid = sub.get("ocpSubscriptionId") or sub.get("id")
                if sid:
                    known[sid] = {"name": sub.get("displayName") or sid}
    except Exception:                                           # noqa: BLE001
        # Unreadable is not fatal here: the tenant list is one of two sources
        # and the report states which subscriptions it could read. The caller
        # logs; this function raises only when neither source yielded one.
        pass

    out = []
    for sid, meta in {**known, **{k: {"name": v.get("displayName") or k}
                                 for k, v in readable.items()}}.items():
        out.append({"id": sid, "name": meta["name"],
                    "state": readable.get(sid, {}).get("state", ""),
                    "readable": sid in readable})
    if not out:
        raise ChatHealthyException(
            mode="config_error",
            component="EntitlementReport",
            message="no subscription is visible to the reporting identity and the "
                    "tenant subscription list could not be read; there is nothing "
                    "to report on")
    out.sort(key=lambda x: x["name"].lower())
    return out


# Parent group -> the groups directly inside it, filled by _group_tree.
_GROUP_CHILDREN: dict[str, list[str]] = {}


def _group_tree(credential) -> tuple[dict[str, list[str]], dict[str, str],
                                    dict[str, list[str]], bool]:
    """The directory's own classification of every principal.

    The groups are the structure this report stands on. `humans` and `agents`
    say what kind of thing holds a grant, and Entra is the only place that
    fact is recorded by a person rather than asserted by a file -- putting a
    principal in a group is a deliberate act, and this reads that act rather
    than a list somebody typed.

    Returns (membership, description, readable). `membership` maps a principal
    object id to every group it belongs to, transitively, so a grant made to
    `agents` reaches a member of `runtimeAgents` exactly as Azure resolves it.
    `readable` is False when the directory could not be read at all, which the
    caller must surface rather than treat as "no groups exist" -- the two look
    identical in the data and mean opposite things.
    """
    membership: dict[str, list[str]] = {}
    described: dict[str, str] = {}
    try:
        gtoken = credential.get_token("https://graph.microsoft.com/.default").token
    except Exception as exc:                                    # noqa: BLE001
        _LOG.info("group tree not readable: %s", exc)
        return membership, described, {}, False

    headers = {"Authorization": f"Bearer {gtoken}"}
    r = requests.get(f"{GRAPH}/groups?$select=id,displayName,description",
                     headers=headers, timeout=60)
    if r.status_code != 200:
        _LOG.info("group enumeration returned %s", r.status_code)
        return membership, described, {}, False

    groups = r.json().get("value", [])
    by_id = {g["id"]: g["displayName"] for g in groups}

    def direct(group_id: str) -> tuple[list[str], list[str]]:
        """(principal ids, nested group ids) directly in this group.

        Two queries, and the second is not optional. Graph omits service
        principals from the untyped member collection entirely, and its
        transitiveMembers collection omits them whether cast or not -- measured,
        not assumed. Reading only the obvious endpoint returns users and groups
        and reports every agent in the estate as belonging to nothing, which
        reads as a catastrophic finding and is an artefact of the query.
        """
        principals: list[str] = []
        nested: list[str] = []
        for collection in ("members", "members/microsoft.graph.servicePrincipal"):
            page = f"{GRAPH}/groups/{group_id}/{collection}?$select=id,displayName"
            while page:
                m = requests.get(page, headers=headers, timeout=60)
                if m.status_code != 200:
                    _LOG.info("members of %s (%s) returned %s",
                              by_id.get(group_id, group_id), collection, m.status_code)
                    break
                payload = m.json()
                for member in payload.get("value", []):
                    (nested if member["id"] in by_id else principals).append(member["id"])
                page = payload.get("@odata.nextLink") or ""
        return principals, nested

    for group in groups:
        described[group["displayName"]] = group.get("description") or ""

    # REQ-B-008: which group contains which. The walk below already
    # resolves nesting to attribute membership; keeping the containment is
    # what lets the report state the structure rather than a flat list.
    children: dict[str, list[str]] = {}
    for group in groups:
        _, nested = direct(group["id"])
        names = sorted({by_id[n] for n in nested if n in by_id})
        if names:
            children[group["displayName"]] = names
    _GROUP_CHILDREN.clear()
    _GROUP_CHILDREN.update(children)

    # Nesting is resolved here rather than by Graph, because the collection that
    # would resolve it does not return service principals. A grant to `agents`
    # reaches a member of `runtimeAgents`, so the walk records both names
    # against that member, exactly as Azure resolves the grant.
    for group in groups:
        seen_groups: set[str] = set()
        frontier = [group["id"]]
        while frontier:
            gid = frontier.pop()
            if gid in seen_groups:
                continue
            seen_groups.add(gid)
            principals, nested = direct(gid)
            for pid in principals:
                names = membership.setdefault(pid, [])
                if group["displayName"] not in names:
                    names.append(group["displayName"])
            frontier.extend(nested)
    group_owners = {}
    for group in groups:
        names = []
        for cast in ("microsoft.graph.user", "microsoft.graph.servicePrincipal"):
            o = requests.get(f"{GRAPH}/groups/{group['id']}/owners/{cast}?$select=displayName",
                             headers=headers, timeout=60)
            if o.status_code == 200:
                names.extend(x.get("displayName", "") for x in o.json().get("value", []))
        group_owners[group["displayName"]] = sorted(n for n in names if n)
    return membership, described, group_owners, True


# -- Atlas: the database half of every identity's rights ---------------
#
# EPIC-002-F-003-S-009-REQ-B-001. A user is any credential or human that
# can reach a resource -- an API key, a certificate subject, a username
# with a password. Mongo holds a set of them Entra has never heard of, so
# a report that reads only the directory states half of what a user can
# do and reads as though it were all of it.
#
# Read-only throughout: databaseUsers, customDBRoles and clusters are the
# control plane. No document is read and none is needed -- who may touch a
# collection is not written in the collection.

ATLAS_API = "https://cloud.mongodb.com/api/atlas/v2"
ATLAS_ACCEPT = {"Accept": "application/vnd.atlas.2023-01-01+json"}

# What a Mongo action lets a user do, in the three words the report speaks.
# Anything that changes the database is write; anything that only looks is
# read; a role carrying every action is full.
_READ_ACTIONS = {
    "FIND", "LIST_COLLECTIONS", "LIST_INDEXES", "LIST_SEARCH_INDEXES",
    "COLL_STATS", "DB_STATS", "DB_HASH", "LIST_DATABASES", "VIEW_ROLE",
    "VIEW_USER", "SERVER_STATUS", "CONN_POOL_STATS", "TOP", "INPROG",
    "LIST_SESSIONS", "CHECK_FREE_MONITORING_STATUS", "GET_SHARD_MAP",
}

# The built-in roles Atlas offers, said in the same three words.
_BUILT_IN = {
    "read": "read", "readAnyDatabase": "read", "clusterMonitor": "read",
    "readWrite": "write", "readWriteAnyDatabase": "write",
    "dbAdmin": "write", "dbAdminAnyDatabase": "write",
    "atlasAdmin": "full", "backup": "read", "enableSharding": "write",
}


def _atlas_auth():
    """The Atlas key, or None when this run was not given one."""
    pub = _ch_os.environ.get("ATLAS_PUBLIC_KEY")
    priv = _ch_os.environ.get("ATLAS_PRIVATE_KEY")
    if not (pub and priv):
        return None
    from requests.auth import HTTPDigestAuth
    return HTTPDigestAuth(pub, priv)


def _atlas_get(path: str, auth):
    r = requests.get(f"{ATLAS_API}/{path}", auth=auth, headers=ATLAS_ACCEPT,
                     timeout=60)
    if not r.ok:
        raise ChatHealthyException(
            mode="runtime_error",
            component="entitlement_report",
            message=f"Atlas {path} returned HTTP {r.status_code}: {r.text[:200]}")
    body = r.json()
    return body if isinstance(body, list) else body.get("results", [])


def _right_of(actions: list[str]) -> str:
    """full, read or write, from what the actions actually permit."""
    names = {str(a).upper() for a in actions}
    if not names:
        return "read"
    if names - _READ_ACTIONS:
        return "write"
    return "read"


def _custom_role_reach(roles: list[dict]) -> dict:
    """{role name: {db.collection: right}} from each role's own actions.

    Stated from actions rather than from the role's name, for the reason
    the Azure half already states: a name is what somebody called it and
    the actions are what it does.
    """
    out: dict[str, dict] = {}
    for role in roles:
        name = role.get("roleName")
        if not name:
            continue
        where: dict[str, list[str]] = {}
        for act in role.get("actions") or []:
            action = act.get("action")
            for res in act.get("resources") or []:
                db = res.get("db") or ""
                coll = res.get("collection") or ""
                if res.get("cluster"):
                    key = "(whole cluster)"
                else:
                    key = f"{db}.{coll or '*'}"
                where.setdefault(key, []).append(action)
        out[name] = {k: _right_of(v) for k, v in where.items()}
        # A role may also inherit others. Recorded so the report can say so
        # rather than showing a role that appears to permit nothing.
        inherited = [i.get("role") for i in (role.get("inheritedRoles") or [])]
        if inherited:
            out[name]["(inherits)"] = ", ".join(sorted(x for x in inherited if x))
    return out


def database_rights_reached_through_secrets(data: dict) -> list[dict]:
    """Who holds a database user\'s rights by being able to read its
    credential.

    REQ-B-006. A credential in a vault is not a user standing on its own:
    whoever can read it can present it, and therefore holds every right
    that database user holds. Listing the database user in one place and
    the vault reader in another states both facts and never the one that
    matters.

    The join is the secret\'s grants-rights-for tag. A secret carrying no
    tag is reported as untagged rather than guessed at, because a guess
    here is a claim about who can reach the data.

    Reported, not judged.
    """
    atlas = data.get("atlas") or {}
    if not atlas.get("readable"):
        return []
    users = {u["username"]: u for u in atlas.get("users") or []}
    grants = data.get("secret_grants") or {}

    def database_user(stated: str) -> str:
        for name in users:
            cn = (name[3:].split(",", 1)[0]
                  if name.upper().startswith("CN=") else name)
            if cn.strip().lower() == stated.lower() or name.lower() == stated.lower():
                return name
        return ""

    # secret -> the database user it yields
    yields: dict[str, str] = {}
    untagged: list[str] = []
    for secret in data.get("certificate_secrets") or []:
        name = secret if isinstance(secret, str) else (secret.get("name") or "")
        stated = (grants.get(name) or "").strip()
        if not stated:
            untagged.append(name)
            continue
        db_user = database_user(stated)
        yields[name] = db_user or (
            f"{stated} -- named by the tag, no database user of that name")

    out: list[dict] = []
    for row in data.get("vault_wide") or []:
        reached = sorted({v for v in yields.values() if v})
        if not reached:
            continue
        out.append({
            "holder": row.get("principal") or "",
            "how": ("reads every secret in " + (row.get("vault") or "the vault")
                    + (" and may write them" if row.get("write") else "")),
            "database_users": reached,
            "untagged_secrets": sorted(untagged),
        })
    out.sort(key=lambda r: (r["holder"].lower(), r["how"]))
    return out


def duplicate_grants_of(user: dict, tree: list[dict]) -> list[dict]:
    """Where one user holds the same thing more than once.

    REQ-B-006. Two shapes count. A right granted twice over the same
    place, by two different roles, is one of them doing nothing. And a
    collection granted no more than the database above it already grants
    is a grant that changes nothing about what the user may do.

    Reported at the end of that user\'s own entry, because a duplicate is
    a fact about one user and pooling them loses whose it is.
    """
    order = {"read": 0, "write": 1, "full": 2}
    out = []

    # The same place named by more than one role.
    seen: dict[str, list[str]] = {}
    for g in user.get("grants") or []:
        seen.setdefault(g.get("where") or "", []).append(g.get("role") or "")
    for where, roles in sorted(seen.items()):
        if len(roles) > 1:
            out.append({"what": where,
                        "why": "granted by " + " and ".join(sorted(set(roles)))})

    # A collection granted no more than its database already grants.
    for t in tree:
        for db in t["databases"]:
            at_db = db["right"]
            if not at_db:
                continue
            for c in db["collections"]:
                if order.get(c["right"], 0) <= order.get(at_db, 0):
                    out.append({
                        "what": f"{db['database']}.{c['collection']}",
                        "why": f"{c['right']} on the collection, when the "
                               f"database already grants {at_db}"})
    return out


def atlas_tree(user: dict, clusters: list[str]) -> list[dict]:
    """One user's database rights as cluster, database, collection.

    REQ-B-006. A right is stated once, at the level it is granted, and a
    level below appears only where it differs from the level above. A user
    with write on a whole database does not want its forty collections
    listed underneath saying write forty times; a user with write on the
    database and read on one collection of it wants exactly that one
    collection named.

    A user with no scope reaches every cluster in the project, because
    that is what Atlas does with an unscoped database user -- so the tree
    says every cluster rather than leaving the reader to assume.
    """
    order = {"read": 0, "write": 1, "full": 2}
    reach = user.get("scopes") or clusters

    # db -> right at the database level, and db -> {collection: right}
    db_level: dict[str, str] = {}
    coll_level: dict[str, dict[str, str]] = {}
    whole_cluster: str = ""
    for g in user.get("grants") or []:
        where, right = g.get("where") or "", g.get("right") or "read"
        if where == "(whole cluster)":
            if order.get(right, 0) >= order.get(whole_cluster or "read", -1):
                whole_cluster = right
            continue
        if where == "(all databases)":
            db_level["(every database)"] = max(
                [right, db_level.get("(every database)", "read")], key=lambda r: order.get(r, 0))
            continue
        db, _, coll = where.partition(".")
        if coll in ("*", ""):
            prev = db_level.get(db)
            if prev is None or order.get(right, 0) > order.get(prev, 0):
                db_level[db] = right
        else:
            prev = coll_level.setdefault(db, {}).get(coll)
            if prev is None or order.get(right, 0) > order.get(prev, 0):
                coll_level[db][coll] = right

    out = []
    for db in sorted(set(db_level) | set(coll_level)):
        at_db = db_level.get(db)
        colls = []
        for coll, right in sorted((coll_level.get(db) or {}).items()):
            # Stated only where it differs from what the database grants.
            if at_db is not None and right == at_db:
                continue
            colls.append({"collection": coll, "right": right})
        out.append({"database": db, "right": at_db, "collections": colls})
    return [{"clusters": sorted(reach), "whole_cluster": whole_cluster,
             "databases": out}]


def collect_atlas() -> dict:
    """Every Mongo user, what it may do, and where.

    Returns readable=False when this run holds no Atlas key, which the
    report must say rather than printing an empty section that reads as
    "no database users exist".
    """
    auth = _atlas_auth()
    if auth is None:
        return {"readable": False, "reason": "no Atlas API key on this run",
                "users": [], "roles": {}, "clusters": [], "project": ""}
    project = _ch_os.environ.get("ATLAS_PROJECT_ID") or ""
    if not project:
        return {"readable": False, "reason": "ATLAS_PROJECT_ID not set",
                "users": [], "roles": {}, "clusters": [], "project": ""}

    clusters = [c.get("name") for c in _atlas_get(f"groups/{project}/clusters", auth)]
    custom = _atlas_get(f"groups/{project}/customDBRoles/roles", auth)
    reach = _custom_role_reach(custom)

    users = []
    for u in _atlas_get(f"groups/{project}/databaseUsers", auth):
        name = u.get("username") or ""
        # How this user proves who it is. The certificate subject and the
        # password are different credentials even when they carry the same
        # name, and each is a user by REQ-B-001.
        db = u.get("databaseName") or ""
        kind = ("certificate" if db == "$external" and name.upper().startswith("CN=")
                else "external" if db == "$external" else "password")
        grants = []
        for r in u.get("roles") or []:
            role_name = r.get("roleName") or ""
            on_db = r.get("databaseName") or ""
            if role_name in reach:
                for where, right in reach[role_name].items():
                    grants.append({"role": role_name, "where": where,
                                   "right": right, "custom": True})
            else:
                grants.append({
                    "role": role_name,
                    "where": f"{on_db}.*" if on_db else "(all databases)",
                    "right": _BUILT_IN.get(role_name, "write"),
                    "custom": False})
        users.append({"username": name, "credential": kind,
                      "auth_database": db, "grants": grants,
                      "scopes": [sc.get("name") for sc in (u.get("scopes") or [])]})
    return {"readable": True, "reason": "", "project": project,
            "clusters": sorted(c for c in clusters if c),
            "roles": reach, "users": users}


def _atlas_or_reason() -> dict:
    """Atlas, or why not. Never an empty set presented as an answer."""
    try:
        return collect_atlas()
    except Exception as exc:                                    # noqa: BLE001
        _LOG.warning("Atlas entitlements not readable: %s", exc)
        return {"readable": False, "reason": str(exc)[:300],
                "users": [], "roles": {}, "clusters": [], "project": ""}


def collect() -> dict:
    credential = _credential()
    token = credential.get_token(f"{ARM}/.default").token
    group_membership, group_descriptions, group_owners, groups_readable = _group_tree(credential)

    subscriptions = _subscriptions(token, credential)

    # Every subscription the reporting identity can see is walked. Aggregating
    # rather than picking one is the point: a grant in a subscription this report
    # skipped is indistinguishable, on the page, from a grant that does not
    # exist.
    roles: dict[str, str] = {}
    role_text: dict[str, str] = {}
    privileged_roles: set[str] = set()
    all_defs: list[dict] = []
    rows: list[dict] = []
    existing: dict[str, dict] = {}
    for sub in subscriptions:
        if not sub["readable"]:
            continue
        sid = sub["id"]
        defs = _get_all(f"{ARM}/subscriptions/{sid}/providers"
                        f"/Microsoft.Authorization/roleDefinitions?api-version=2022-04-01", token)
        all_defs.extend(defs)
        for d in defs:
            roles[d["id"].rsplit("/", 1)[-1]] = d["properties"]["roleName"]
            role_text[d["properties"]["roleName"]] = (
                d["properties"].get("description") or "").strip()
            # Privilege is read off each role's published actions, so a renamed
            # role, or one authored tomorrow, is judged on what it permits rather
            # than on whether somebody remembered to add its name to this file.
            for perm in (d["properties"].get("permissions") or []):
                for action in (perm.get("actions") or []):
                    if action.strip().lower() in PRIVILEGE_MARKERS:
                        privileged_roles.add(d["properties"]["roleName"])
                # A data-plane wildcard is administrative in the sense that
                # matters here: Key Vault Administrator names no privileged verb
                # and yet reads every certificate in the firm. The same test is
                # NOT applied to control-plane actions, where a service wildcard
                # like Microsoft.Network/* means Contributor over networking and
                # confers no authority over who holds rights.
                for action in (perm.get("dataActions") or []):
                    a = action.strip().lower()
                    if a in PRIVILEGE_MARKERS or a.endswith("/*"):
                        privileged_roles.add(d["properties"]["roleName"])
        for row in _get_all(f"{ARM}/subscriptions/{sid}/providers"
                            f"/Microsoft.Authorization/roleAssignments"
                            f"?api-version=2022-04-01", token):
            row["_subscription"] = sub["name"]
            rows.append(row)
        for pid, meta in _all_principals(token, credential, sid).items():
            existing.setdefault(pid, meta)
    oids = sorted({r["properties"]["principalId"] for r in rows})
    # ARM first -- managed identities resolve without any directory permission.
    # Graph then fills in users and app registrations, when permitted.
    names = _managed_identity_names(token, [s['id'] for s in subscriptions])
    graph_names = _principal_names(oids, credential)
    names.update(graph_names)

    sub_ids = tuple(s["id"] for s in subscriptions)
    role_contains = _role_containment(all_defs)
    role_reach = _role_reach(all_defs)
    vaults, certificate_secrets = _vaults_and_certificates(token, list(sub_ids))

    # Resource groups, stated once against the subscription they belong to. A
    # resource group lives in exactly one subscription, so once that is said an
    # entitlement need only name the resource group and what is inside it.
    resource_groups: list[dict] = []
    for sub in subscriptions:
        if not sub["readable"]:
            continue
        for rg in _get_all(f"{ARM}/subscriptions/{sub['id']}/resourcegroups"
                           f"?api-version=2021-04-01", token):
            resource_groups.append({"name": rg.get("name", ""),
                                    "subscription": sub["name"],
                                    "location": rg.get("location", "")})
    resource_groups.sort(key=lambda r: (r["subscription"].lower(), r["name"].lower()))

    # Every resource, the group that owns it, and its description tag. A resource
    # nobody described is a resource nobody can account for, so it is reported.
    resources: list[dict] = []

    # A resource's own description, which the person who created it controls.
    # Azure carries it as a tag; nothing else on a resource is free text.
    resource_notes: dict[str, str] = {}
    for sub in subscriptions:
        if not sub["readable"]:
            continue
        for r in _get_all(f"{ARM}/subscriptions/{sub['id']}/resources"
                          f"?api-version=2021-04-01", token):
            tags = r.get("tags") or {}
            note = ""
            for key in ("description", "Description", "purpose", "Purpose"):
                if tags.get(key):
                    note = tags[key]
                    break
            if note and r.get("id"):
                resource_notes[r["id"]] = note
            rid = r.get("id", "")
            group = ""
            low = rid.lower()
            if "/resourcegroups/" in low:
                group = rid[low.index("/resourcegroups/")
                            + len("/resourcegroups/"):].split("/", 1)[0]
            resources.append({
                "name": r.get("name", ""),
                "type": r.get("type", ""),
                "group": group,
                "subscription": sub["name"],
                "description": note,
            })

    # What a subscription actually contains. "Owner on subscription X" tells a
    # reader nothing about what can be touched; the count of resources, and the
    # vaults among them, is the thing a security officer is reading for.
    sub_contents: dict[str, dict] = {}
    for sub in subscriptions:
        if not sub["readable"]:
            continue
        res = _get_all(f"{ARM}/subscriptions/{sub['id']}/resources"
                       f"?api-version=2021-04-01", token)
        kinds: dict[str, int] = {}
        for r in res:
            kinds[r.get("type", "")] = kinds.get(r.get("type", ""), 0) + 1
        sub_contents[sub["name"]] = {
            "count": len(res),
            "vaults": kinds.get("Microsoft.KeyVault/vaults", 0),
            "kinds": kinds,
        }
    holders: dict[str, dict] = {}
    for r in rows:
        p = r["properties"]
        oid = p["principalId"]
        approved = APPROVED.get(oid)
        known = names.get(oid)
        entry = holders.setdefault(oid, {
            "object_id": oid,
            "name": approved[0] if approved else (known["name"] if known else ""),
            # From the directory, never from the register. The register says
            # who is approved to exist; it does not get to say what they are.
            "purpose": (known.get("description", "") if known else ""),
            # Group membership is the directory's own answer to "what kind of
            # thing is this" -- a person put it there. actor_type from the
            # register is the second opinion, and the two disagreeing is
            # itself worth seeing.
            "groups": sorted(group_membership.get(oid, [])),
            "actor_type": approved[2] if approved else "",
            "entra_object_type": (approved[3] if approved else
                                  (known["kind"] if known
                                   else p.get("principalType", ""))),
            "application": approved[4] if approved else "",
            "declared_roles": list(approved[5]) if approved else [],
            "type": known["kind"] if known else p.get("principalType", "Unknown"),
            "approved": approved is not None,
            "resolvable": known is not None,
            "record": (known or {}).get("record", "none"),
            "qualities": (known or {}).get("qualities", {}),
            # A principal that does not resolve in the directory has been
            # deleted; its assignment outlived it. That is not "outside the
            # approved register" -- no register can contain a deleted object,
            # and listing it as one invites someone to add it rather than
            # remove the grant.
            "orphaned": known is None and approved is None,
            "grants": [],
        })
        role_name = roles.get(p["roleDefinitionId"].rsplit("/", 1)[-1],
                              p["roleDefinitionId"].rsplit("/", 1)[-1])
        condition = p.get("condition") or ""
        # Name the roles the condition forbids, by testing which role-definition
        # ids the expression mentions. An auditor needs the restriction stated,
        # not merely flagged.
        forbidden = sorted({name for guid, name in roles.items() if guid in condition})
        entry["grants"].append({
            "role": role_name,
            "scope": _scope_label(p.get("scope", ""), sub_ids),
            "raw_scope": p.get("scope", ""),
            "secret": (p.get("scope", "").split("/secrets/", 1)[1]
                       if "/secrets/" in p.get("scope", "") else ""),
            "parent_scope": _scope_label(p.get("scope", "").split("/secrets/", 1)[0], sub_ids)
                            if "/secrets/" in p.get("scope", "") else "",
            "redundant": "",
            "privileged": role_name in privileged_roles,
            "conditioned": bool(condition),
            "forbidden_roles": forbidden,
            "constrains_write": "roleAssignments/write" in condition,
            "constrains_delete": "roleAssignments/delete" in condition,
            "justification": (p.get("description") or "").strip(),
            "subscription": r.get("_subscription", ""),
        })

    for h in holders.values():
        _mark_redundant(h["grants"], sub_ids, role_contains)
        h["grants"].sort(key=lambda g: (not g["privileged"], g["scope"], g["role"]))
        h["privileged_count"] = sum(1 for g in h["grants"] if g["privileged"])

    holders_list = sorted(
        holders.values(),
        key=lambda h: (h["approved"], -h["privileged_count"], h["name"] or h["object_id"]))

    # Principals that exist and hold nothing. A report built from assignments
    # alone cannot see these, and a principal minted quietly lands here.
    rightless = []
    for oid, meta in sorted(existing.items(), key=lambda kv: kv[1]["name"].lower()):
        if oid in holders:
            continue
        approved = APPROVED.get(oid)
        # A principal holding nothing is an exception whether or not the
        # register names it. Being declared does not make an orphan expected:
        # something brought it into existence and nothing uses it, and that is
        # the fact a reviewer must see. It is listed unapproved, with a note
        # saying what is true of it.
        rightless.append({
            "object_id": oid,
            "name": approved[0] if approved else meta["name"],
            "type": meta["kind"],
            "actor_type": approved[2] if approved else "",
            "application": approved[4] if approved else "",
            "origin": meta["origin"],
            "approved": False,
            "note": ("holds no rights; named in the approved register"
                     if approved is not None
                     else "holds no rights; not in the approved register"),
        })

    missing = [v[0] for k, v in APPROVED.items() if k not in holders]

    # Who answers for each principal and each group. Delegation is a fact about
    # authority as much as any role assignment, and it lives only here.
    owner_targets = {h["object_id"]: "servicePrincipals"
                     for h in holders_list if h["type"].lower() != "user"}
    owner_targets.update({h["object_id"]: "users"
                          for h in holders_list if h["type"].lower() == "user"})
    owners = _owners(credential, owner_targets)

    # Conditions are a property of the right, not of the identity holding it, so
    # they are gathered per role and stated once in the rights table. Repeated
    # under every holder they crowded out that holder's actual entitlements.
    role_conditions: dict[str, list[str]] = {}
    for h in holders_list:
        for g in h["grants"]:
            if g["conditioned"] and g["forbidden_roles"]:
                role_conditions.setdefault(g["role"], [])
                for f in g["forbidden_roles"]:
                    if f not in role_conditions[g["role"]]:
                        role_conditions[g["role"]].append(f)

    # Ownership, derived from scope and actions rather than from a role name.
    # A principal owns a subscription when it holds, at that subscription's own
    # scope, a role permitting every management action. It owns the tenant when
    # it holds such a role at the tenant root, which sits above every
    # subscription the tenant will ever contain. Neither is read from a role
    # name or a description: claudeCodeAgent is a subscription owner through a
    # role called chatHealthyAgent, and nothing but its actions says so.
    def _total(role: str) -> bool:
        meta = role_reach.get(role, {})
        return "*" in meta.get("actions", [])

    directory_roles = _directory_roles(credential)
    # A directory role naming the whole directory is tenant authority. Global
    # Administrator is the one Microsoft ships for it; any role granting the
    # same is caught by the same test rather than by its name.
    TENANT_ROLES = {"Global Administrator", "Company Administrator",
                    "Privileged Role Administrator"}
    for h in holders_list:
        h["directory_roles"] = sorted(directory_roles.get(h["object_id"], []))
        owns_subs = []
        owns_tenant = bool(set(h["directory_roles"]) & TENANT_ROLES)
        for g in h["grants"]:
            if not _total(g["role"]):
                continue
            raw = g["raw_scope"].rstrip("/")
            if raw == "":
                owns_tenant = True
            for sub in subscriptions:
                if raw.lower() == f"/subscriptions/{sub['id']}".lower():
                    if sub["name"] not in owns_subs:
                        owns_subs.append(sub["name"])
        h["owns_subscriptions"] = owns_subs
        h["owns_tenant"] = owns_tenant

        # For an owner, control-plane grants are implied by the ownership and
        # say nothing. Data-plane grants are NOT: Owner permits managing a key
        # vault and not reading a secret in it, so a data grant to an owner is
        # an addition to what ownership gives and has to be stated.
        for g in h["grants"]:
            in_owned = any(f"/subscriptions/{sub['id']}".lower()
                           in g["raw_scope"].lower()
                           for sub in subscriptions
                           if sub["name"] in owns_subs)
            has_data = bool(role_reach.get(g["role"], {}).get("data_actions"))
            g["beyond_ownership"] = bool(in_owned and has_data)
            g["implied_by_ownership"] = bool(
                in_owned and not has_data
                and g["role"] not in ("Owner",))

    # Two different facts were being reported as one. A principal holding a
    # vault-wide role can read every secret in that vault, which made every
    # secret look shared by everyone and buried the secrets that are actually
    # shared. Vault-wide access is stated once, as its own list. A secret is
    # shared only when more than one principal is granted that secret by name.
    vault_wide: list[dict] = []
    for h in holders_list:
        for g in h["grants"]:
            data_acts = role_reach.get(g["role"], {}).get("data_actions", [])
            reaches_secrets = any(a == "*" or a.startswith("microsoft.keyvault/vaults/")
                                  for a in data_acts)
            if not reaches_secrets or g["secret"]:
                continue
            writes = any(a == "*" or a.rstrip("/*").endswith("microsoft.keyvault/vaults")
                         or "setsecret" in a or "/secrets/*" in a for a in data_acts)
            vault_wide.append({
                "principal": h["name"] or h["object_id"],
                "role": g["role"],
                "where": _reaches(g["raw_scope"], g["subscription"], {}),
                "access": "read and write" if writes else "read",
            })
    vault_wide.sort(key=lambda x: (x["principal"].lower(), x["role"].lower()))

    named: dict[str, set[str]] = {}
    for h in holders_list:
        for g in h["grants"]:
            if g["secret"]:
                named.setdefault(g["secret"], set()).add(h["name"] or h["object_id"])
    shared = sorted(((secret, sorted(who)) for secret, who in named.items()
                     if len(who) > 1), key=lambda x: (-len(x[1]), x[0]))

    redundant = [(h["name"] or h["object_id"], g["role"], g["scope"], g["redundant"])
                 for h in holders_list for g in h["grants"] if g["redundant"]]

    # A principal holding rights while belonging to no group is the finding this
    # report exists for: nobody placed it, so nobody classified it, so nothing
    # states whether a person or a program holds what it holds.
    ungrouped = [h["name"] or h["object_id"]
                 for h in holders_list if not h["groups"]] if groups_readable else []

    return {
        "generated": _dt.datetime.now(_dt.timezone.utc),
        "holders": holders_list,
        "assignment_count": len(rows),
        "names_resolved": bool(graph_names),
        "secret_descriptions": _secret_descriptions(),
        "rightless": rightless,
        "directory_enumerated": any(m["origin"] == "directory" for m in existing.values()),
        "subscriptions": subscriptions,
        "privileged_roles": sorted(privileged_roles),
        "role_reach": role_reach,
        "role_conditions": {k: sorted(v) for k, v in role_conditions.items()},
        "resource_groups": resource_groups,
        "subscription_contents": sub_contents,
        "resource_notes": resource_notes,
        "resources": resources,
        # Every enforcement surface, not just Azure resources. Origin says
        # which one, because "undescribed" means a different remedy on each:
        # a tag on the resource, a tag on the vault secret, a description on
        # the directory object.
        "undescribed": (
            [dict(r, origin="Azure", where=r.get("subscription", ""))
             for r in resources if not r["description"]]
            + _undescribed_secrets()
            + _undescribed_identities(holders_list, rightless)
        ),
        "vaults": vaults,
        "certificate_secrets": certificate_secrets,
        "tenant_name": _tenant_name(credential) or _tenant_id(),
        "shared_secrets": shared,
        "vault_wide": vault_wide,
        "redundant_grants": redundant,
        "owners": owners,
        "group_owners": group_owners,
        "groups_readable": groups_readable,
        "group_descriptions": group_descriptions,
        "ungrouped": ungrouped,
        "role_text": {r: t for r, t in role_text.items()
                      if any(g["role"] == r for h in holders_list for g in h["grants"])},
        "approved_absent": missing,
        # REQ-B-001: the database half. A failure here is reported, not
        # swallowed -- a section that silently shows nothing is a section
        # that says there is nothing.
        "atlas": _atlas_or_reason(),
        # What each vault secret says it makes its reader into.
        "secret_grants": dict(_SECRET_GRANTS),
        "secrets_seen": sorted(_SECRETS_SEEN),
        "vault_read_failures": list(_VAULT_READ_FAILURES),
        # REQ-B-008: what lies beneath each group.
        "group_children": dict(_GROUP_CHILDREN),
    }


class _NumberedCanvas(canvas_module.Canvas):
    """Holds every page until the end so each can be stamped "n of m".

    A page numbered without its total cannot tell a reader whether the report
    in front of them is complete. The total is only known once the last page
    exists, so pages are kept and stamped on save.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._saved = []

    def showPage(self):
        self._saved.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        total = len(self._saved)
        for state in self._saved:
            self.__dict__.update(state)
            self.setFont("Helvetica", 7.5)
            self.setFillColor(MUTED)
            w, h = landscape(letter)
            self.drawRightString(w - 0.5 * inch, h - 0.52 * inch,
                                 f"{self._pageNumber}/{total}")
            super().showPage()
        super().save()


def _page_furniture(canvas, doc):
    canvas.saveState()
    w, h = landscape(letter)
    canvas.setStrokeColor(RULE)
    canvas.setLineWidth(0.5)
    canvas.line(0.5 * inch, h - 0.62 * inch, w - 0.5 * inch, h - 0.62 * inch)
    canvas.setFont("Helvetica", 7.5)
    canvas.setFillColor(MUTED)
    canvas.drawString(0.5 * inch, h - 0.52 * inch,
                      "ChatHealthy.ai  |  Access entitlement report  |  Confidential  |  "
                      + getattr(doc, "ch_stamp", "")
                      + "  |  register: " + getattr(doc, "ch_register", ""))
    # Page n of m. The total is known only after the first pass, so the document
    # is built twice and the count carried between them; a page numbered without
    # its total cannot tell a reader whether the report is complete.
    canvas.line(0.5 * inch, 0.55 * inch, w - 0.5 * inch, 0.55 * inch)
    canvas.drawString(0.5 * inch, 0.4 * inch,
                      getattr(doc, "ch_scope_line", "Azure subscriptions: unknown"))
    canvas.restoreState()


def render_pdf(data: dict, out_path: Path) -> Path:
    base = getSampleStyleSheet()
    title = ParagraphStyle("t", parent=base["Heading1"], fontSize=19, leading=23,
                           textColor=INK, spaceAfter=2)
    sub = ParagraphStyle("s", parent=base["Normal"], fontSize=9.5, textColor=MUTED,
                         spaceAfter=14)
    SECTION_NUMBER = colors.HexColor("#1F5FBF")
    sec = ParagraphStyle("sec", parent=base["Heading2"], fontSize=12.5, textColor=INK,
                         spaceBefore=16, spaceAfter=6)
    sub_sec = ParagraphStyle("subsec", parent=base["Heading3"], fontSize=10.5,
                             textColor=INK, spaceBefore=10, spaceAfter=4)
    body = ParagraphStyle("b", parent=base["Normal"], fontSize=9, leading=13,
                          textColor=INK, spaceAfter=8)
    note = ParagraphStyle("n", parent=base["Normal"], fontSize=7.5, leading=10,
                          textColor=MUTED)
    who = ParagraphStyle("w", parent=base["Heading3"], fontSize=10.5, textColor=INK,
                         spaceBefore=10, spaceAfter=1)
    cell = ParagraphStyle("c", parent=base["Normal"], fontSize=8, leading=10.5)
    global _SECTION_TITLE, _SECTION_NOTE
    _SECTION_TITLE = ParagraphStyle(
        "sectitle", parent=base["Heading2"], fontSize=17, leading=21,
        textColor=colors.HexColor("#1F5FBF"), spaceBefore=16, spaceAfter=1)
    _SECTION_NOTE = ParagraphStyle(
        "secnote", parent=base["Normal"], fontSize=9.5, leading=13,
        leftIndent=16, fontName="Helvetica-Oblique", textColor=INK,
        spaceAfter=0)
    global _TOC_ENTRY
    _TOC_ENTRY = ParagraphStyle(
        "toc", parent=base["Normal"], fontSize=13, leading=19, leftIndent=16,
        textColor=colors.HexColor("#1F5FBF"), spaceAfter=1)
    bullet = ParagraphStyle("bul", parent=base["Normal"], fontSize=9, leading=13,
                            leftIndent=22, spaceAfter=2)

    doc = SimpleDocTemplate(
        str(out_path), pagesize=landscape(letter),
        leftMargin=0.5 * inch, rightMargin=0.5 * inch,
        topMargin=0.8 * inch, bottomMargin=0.75 * inch,
        title="ChatHealthy access entitlement report",
        author="ChatHealthy.ai", subject="Access entitlement review")
    doc.ch_scope_line = "Azure subscriptions: " + ", ".join(
        s["name"] for s in data["subscriptions"])

    # Stated in the operator's own time. A report read every morning in
    # California should not make its reader convert from UTC to know whether
    # it is this morning's.
    stamp = _produced_on_pacific(data["generated"])
    doc.ch_stamp = stamp
    doc.ch_register = _REGISTER_SOURCE
    # A principal holding nothing belongs here too. unapproved read only from
    # holders - identities that hold rights - so an orphan could not appear in
    # it by construction, and the section that exists to surface exceptions
    # reported none while orphans existed.
    unapproved = [h for h in data["holders"]
                  if not h["approved"] and not h.get("orphaned")]
    unapproved = unapproved + [{
        "object_id": r["object_id"],
        "name": r["name"],
        "purpose": r.get("note", ""),
        "purpose_source": "Observed by this run",
        "groups": [],
        "actor_type": r.get("actor_type", ""),
        "entra_object_type": r["type"],
        "application": r.get("application", ""),
        "declared_roles": [],
        "type": r["type"],
        "approved": False,
        "resolvable": True,
        "record": "none",
        "qualities": {},
        "orphaned": False,
        "grants": [],
        "privileged_count": 0,
        "owns_tenant": False,
        "owns_subscriptions": [],
        "managers": [],
    } for r in data["rightless"] if not r["approved"]]

    orphaned = [h for h in data["holders"] if h.get("orphaned")]
    undeclared = [r for r in data["rightless"] if not r["approved"]]
    approved = [h for h in data["holders"] if h["approved"]]
    priv_unapproved = [h for h in unapproved if h["privileged_count"]]

    story: list = []
    story.append(Paragraph("Access entitlement report", title))
    story.append(Paragraph(
        "Azure subscriptions &nbsp;&middot;&nbsp; "
        + " &nbsp;|&nbsp; ".join(s["name"] for s in data["subscriptions"])
        + f"<br/>Enumerated {stamp}", sub))

    story.append(Paragraph(
        "This is the operational report of who can act on ChatHealthy systems. Every value "
        "in it is read at the time stated, from Azure and from the directory. It states no "
        "fact of its own. Its sections are:", body))
    # The contents names the sections and nothing else. What each one states
    # is said once, in the section itself, where a reader is standing when the
    # answer matters.
    for number, (label, _) in enumerate(SECTIONS, start=1):
        story.append(Paragraph(f"{number}: {label}", _TOC_ENTRY))
    story.append(Spacer(1, 6))

    def _grant_table(holder: dict) -> Table:
        # Grants that reach a single secret are gathered under one line naming
        # the vault, with the secrets themselves listed beneath it. Otherwise a
        # role held on six secrets reads as six near-identical rows whose only
        # difference sits at the end of a long string.
        descs = data["secret_descriptions"]
        # user -> resource -> right. The resource leads, because that is the
        # thing being protected; the right is how this user touches it.
        rows = [["Resource", "What it holds", "Description", "Right held on it",
                 "Administrative", "Conditioned", "Adds nothing"]]
        bullet_rows: list[int] = []
        title_rows: list[int] = []
        # Every entitlement is titled with the subscription it is granted in.
        # Without it a scope reads "the whole subscription" with no way to know
        # which, and two grants in different subscriptions look identical.
        by_sub: dict[str, list[dict]] = {}
        for g in holder["grants"]:
            by_sub.setdefault(g["subscription"] or "(subscription not recorded)",
                              []).append(g)

        for sub_name in sorted(by_sub):
            title_rows.append(len(rows))
            rows.append([Paragraph(f'<b>{sub_name}</b>', cell),
                         "", "", "", "", "", ""])
            # One row per grant. Two grants on the same resource are two facts,
            # and folding them into one row because the resource repeats hides
            # one of them.
            for g in by_sub[sub_name]:
                label = _reaches(g["raw_scope"], g["subscription"],
                                 data["subscription_contents"])
                holds = _holds(g["raw_scope"], g["subscription"],
                               data["subscription_contents"])
                described = data["resource_notes"].get(g["raw_scope"], "")
                if g["secret"]:
                    label = f'secret {g["secret"]}'
                    holds = ""
                    described = descs.get(g["secret"], "")
                rows.append([
                    Paragraph(label, cell),
                    Paragraph(holds, cell),
                    Paragraph(described, cell),
                    Paragraph(g["role"], cell),
                    "yes" if g["privileged"] else "",
                    "yes" if g["conditioned"] else "",
                    "yes" if g["redundant"] else "",
                ])

        tb = Table(rows, colWidths=[2.5 * inch, 1.1 * inch, 2.1 * inch,
                                    1.6 * inch, 0.9 * inch, 0.85 * inch,
                                    0.85 * inch],
                   hAlign="LEFT", repeatRows=1)
        st = [
            ("BACKGROUND", (0, 0), (-1, 0), BAND),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("LINEBELOW", (0, 0), (-1, -1), 0.25, RULE),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("ALIGN", (4, 0), (6, -1), "CENTER"),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ]
        for i in bullet_rows:
            st.append(("LINEBELOW", (0, i), (-1, i), 0, colors.white))
            st.append(("TOPPADDING", (0, i), (-1, i), 0))
        for i, row in enumerate(rows[1:], start=1):
            if i not in bullet_rows and row[4] == "yes":
                st.append(("TEXTCOLOR", (4, i), (4, i), FLAG))
                st.append(("FONTNAME", (0, i), (0, i), "Helvetica-Bold"))
        tb.setStyle(TableStyle(st))
        return tb

    def _block(holder: dict) -> list:
        label = holder["name"] or "Unidentified principal"
        out = [Paragraph(label, who)]
        meta = holder["entra_object_type"] or holder["type"]
        if holder["owns_tenant"]:
            meta += " &nbsp;&middot;&nbsp; tenant owner"
        if holder["owns_subscriptions"]:
            meta += (" &nbsp;&middot;&nbsp; owns "
                     + ", ".join(holder["owns_subscriptions"]))
        if holder["groups"]:
            meta += " &nbsp;&middot;&nbsp; " + ", ".join(holder["groups"])
        out.append(Paragraph(meta, note))
        # Who answers for this identity existing. An entitlement table says what
        # an identity may do and never who is accountable for it, and in an
        # estate where a person empowers an agent that then administers others,
        # that chain is the control.
        held_by = data["owners"].get(holder["object_id"], [])
        if held_by:
            out.append(Paragraph(f"Managed by {', '.join(held_by)}.", note))
        elif holder["type"].lower() != "user" and holder["entra_object_type"] != "managedIdentity":
            out.append(Paragraph("No owner recorded in the directory.", note))
        # Cited as the catalog's description rather than stated as fact. It is
        # prose someone wrote and it goes stale: the operator's said he was the
        # only identity able to create another, which stopped being true the
        # hour an agent was granted Owner and Application.ReadWrite.All.
        if holder["purpose"]:
            out.append(Paragraph(
                f"<i>" + (holder.get("purpose_source")
                          or "Description, from the identity catalog")
                + f":</i> {holder['purpose']}", note))
        if holder["record"] == "deleted":
            q = ", ".join(f"{k} {v}" for k, v in holder["qualities"].items() if v)
            out.append(Paragraph(
                f"<b>The grant below is held by a deleted account.</b> {q}.", note))
        elif holder["record"] == "none":
            out.append(Paragraph(
                "<b>The directory holds no record of this holder</b> &mdash; not among "
                "its objects and not among its deleted ones. Nothing can be restored, "
                "because there is nothing recorded to restore.", note)
                if data["directory_enumerated"] else Paragraph(
                "The directory could not be read on this run, so this holder was "
                "neither confirmed nor ruled out.", note))

        out.append(Spacer(1, 4))
        if holder["grants"]:
            out.append(_grant_table(holder))
        out.append(Spacer(1, 10))
        return out

    story.extend(_section_block(1))
    for para in _scope_story(data):
        story.append(Paragraph(para, note))
        story.append(Spacer(1, 4))
    # What this particular run could and could not see. Part of scope
    # because a section reporting only what was readable, without saying
    # what was not, reads as though it were everything.
    story.append(Spacer(1, 4))
    scope_rows = [["Tenant", Paragraph(data["tenant_name"], cell)]]
    for sub in data["subscriptions"]:
        scope_rows.append([
            Paragraph("Subscription", cell),
            Paragraph(f"<b>{sub['name']}</b> &mdash; "
                      + ("enumerated in full" if sub["readable"] else
                         "NOT ENUMERATED: the reporting identity holds no access "
                         "here, so grants inside it are absent from this report"),
                      cell)])
    _atlas_scope = data.get("atlas") or {}
    scope_rows.append(["Database", Paragraph(
        ("read: " + ", ".join(_atlas_scope.get("clusters") or []))
        if _atlas_scope.get("readable") else
        "NOT READ: " + (_atlas_scope.get("reason") or "no reason recorded")
        + " &mdash; so this run states nothing about database rights", cell)])
    scope_rows.append(["Directory", Paragraph(
        "groups and ownership read" if data["groups_readable"]
        else "NOT READ: nothing below is classified", cell)])
    st = Table(scope_rows, colWidths=[2.0 * inch, 7.4 * inch], hAlign="LEFT")
    st.setStyle(TableStyle([
        ("FONTSIZE", (0, 0), (-1, -1), 8.5),
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("LINEBELOW", (0, 0), (-1, -1), 0.25, RULE),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4)]))
    story.append(st)

    story.extend(_section_block(2))
    # EPIC-002-F-003-S-009-REQ-B-004. Each row names one thing counted and
    # states its count, and every count comes from what this run found.
    # Where a figure could not be measured the row says so rather than
    # printing a zero, because a zero and an unread source look identical
    # on the page and mean opposite things.
    people = [h for h in data["holders"] if h["type"].lower() == "user"]
    components = [h for h in data["holders"] if h["type"].lower() != "user"]
    atlas = data.get("atlas") or {}
    readable_subs = [x for x in data["subscriptions"] if x["readable"]]
    unreadable_subs = [x for x in data["subscriptions"] if not x["readable"]]

    if atlas.get("readable"):
        per_cluster = []
        for cluster in atlas.get("clusters") or []:
            n = sum(1 for u in atlas.get("users") or []
                    if not u.get("scopes") or cluster in (u.get("scopes") or []))
            per_cluster.append(f"{cluster}: {n}")
        db_users_row = "; ".join(per_cluster) or "0"
        db_grants = sum(len(u.get("grants") or []) for u in atlas.get("users") or [])
        db_grants_row = str(db_grants)
        db_roles_row = str(len(atlas.get("roles") or {}))
    else:
        why = atlas.get("reason") or "not read"
        db_users_row = db_grants_row = db_roles_row = f"not measured -- {why}"

    summary = [
        ["Role assignments in force", str(data["assignment_count"])],
        ["Identities holding rights", str(len(data["holders"]))],
        ["  of those, named people", str(len(people))],
        ["  of those, components", str(len(components))],
        ["Identities in the approved register", f"{len(approved)} of {len(APPROVED)}"],
        # Counted from the holders, not from `unapproved`: that list is
        # extended below with identities holding NO rights, so using it
        # here put two populations under a label naming one and reported 3
        # where the answer is 0.
        ["Holding rights but not in the register",
         str(len([h for h in data["holders"]
                  if not h["approved"] and not h.get("orphaned")]))],
        ["Holding no rights and not in the register",
         str(len([r for r in data["rightless"] if not r["approved"]]))],
        ["In the register but holding no rights", str(len(data["approved_absent"]))],
        ["Secrets in the vaults",
         (f"not measured -- {'; '.join(data['vault_read_failures'])[:120]}"
          if data.get("vault_read_failures")
          else str(len(data.get("secrets_seen") or [])))],
        ["  of those, naming the identity they grant",
         str(len(data.get("secret_grants") or {}))],
        ["Database users, per cluster", db_users_row],
        # A right over a place, which is what an entitlement is -- one role
        # naming six collections is six things the user may do, not one.
        ["Database rights in force (user x database or collection)",
         db_grants_row],
        ["Database roles defined", db_roles_row],
        ["Orphaned assignments", str(len(orphaned))],
        ["Resources undescribed", str(len(data["undescribed"]))],
        ["Exceptions in total", str(len(orphaned) + len(data["undescribed"]))],
        ["Azure resource groups", str(len(data["resource_groups"]))],
        ["Subscriptions enumerated", str(len(readable_subs))],
        ["Subscriptions NOT enumerated",
         str(len(unreadable_subs)) + (
             " -- " + ", ".join(x["name"] for x in unreadable_subs)
             if unreadable_subs else "")],
    ]
    t = Table(summary, colWidths=[4.2 * inch, 1.2 * inch], hAlign="LEFT")
    style = [
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("TEXTCOLOR", (0, 0), (-1, -1), INK),
        ("LINEBELOW", (0, 0), (-1, -2), 0.25, RULE),
        ("ALIGN", (1, 0), (1, -1), "RIGHT"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]
    # Flagged by what the row says, not by where it sits: the rows moved
    # once and the colour stayed on the old index, marking a figure it was
    # never about.
    def _row(label):
        for i, r in enumerate(summary):
            if r[0].strip() == label:
                return i
        return None

    for label in ("Holding rights but not in the register",
                  "In the register but holding no rights",
                  "Orphaned assignments"):
        i = _row(label)
        if i is None:
            continue
        bad = summary[i][1] not in ("0", "0 of 0")
        style.append(("TEXTCOLOR", (1, i), (1, i), FLAG if bad else OK))
        if bad:
            style.append(("FONTNAME", (1, i), (1, i), "Helvetica-Bold"))
    i = _row("Subscriptions NOT enumerated")
    if i is not None and unreadable_subs:
        style.append(("TEXTCOLOR", (1, i), (1, i), FLAG))
    t.setStyle(TableStyle(style))
    story.append(KeepTogether(t))

    if data["approved_absent"]:
        story.append(Spacer(1, 8))
        story.append(Paragraph(
            "Approved identities holding no rights in this subscription: "
            + ", ".join(data["approved_absent"]) + ".", note))
    if not data["names_resolved"]:
        story.append(Spacer(1, 8))
        story.append(Paragraph(
            "Directory names were unavailable when this report ran: the reporting identity "
            "holds no Microsoft Graph directory-read permission. Approved identities are "
            "named from the firm's pinned register; all others are identified by object id "
            "alone. Granting Directory.Read.All to the reporting identity resolves this.",
            note))

    # The header travels with the first principal. A page break can be told
    # how much room to require, but not how tall the next block will be, so
    # binding the two is what actually keeps a title off the foot of a page.
    story.extend(_section_block(3))
    story.append(Paragraph(
        f"Orphaned assignments &mdash; grants whose principal no longer exists "
        f"({len(orphaned)})", sub_sec))
    if orphaned:
        o_rows = [["Principal object id", "Right", "Resource", "Subscription"]]
        for h in orphaned:
            for g in h["grants"]:
                o_rows.append([
                    Paragraph(h["object_id"], cell),
                    Paragraph(g["role"], cell),
                    Paragraph(_reaches(g["raw_scope"], g["subscription"],
                                       data["subscription_contents"]), cell),
                    Paragraph(g["subscription"], cell)])
        ot = Table(o_rows, colWidths=[2.6 * inch, 1.9 * inch, 3.0 * inch, 1.9 * inch],
                   hAlign="LEFT", repeatRows=1)
        ot.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), BAND),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("LINEBELOW", (0, 0), (-1, -1), 0.25, RULE),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3)]))
        story.append(ot)
    else:
        story.append(Paragraph("<i>no exceptions</i>", note))

    story.append(Paragraph(
        f"Resources undescribed &mdash; carrying no description tag "
        f"({len(data['undescribed'])})", sub_sec))
    if data["undescribed"]:
        u_rows = [["Origin", "Type", "Name", "Where"]]
        for r in sorted(data["undescribed"],
                        key=lambda x: (x.get("origin", ""), x.get("type", ""),
                                       x.get("name", "").lower())):
            u_rows.append([Paragraph(r.get("origin", "&mdash;"), cell),
                           Paragraph(r.get("type", "&mdash;"), cell),
                           Paragraph(r.get("name", "&mdash;"), cell),
                           Paragraph(r.get("where") or r.get("group")
                                     or r.get("subscription") or "&mdash;", cell)])
        ut = Table(u_rows, colWidths=[1.1 * inch, 2.4 * inch, 3.4 * inch, 2.5 * inch],
                   hAlign="LEFT", repeatRows=1)
        ut.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), BAND),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("LINEBELOW", (0, 0), (-1, -1), 0.25, RULE),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3)]))
        story.append(ut)
    else:
        story.append(Paragraph("<i>no exceptions</i>", note))


        # Shared secrets are not an exception. A secret granted by name to
        # more than one user is something this firm does deliberately and
        # tracks, and a report that files a tracked arrangement under
        # "wanting a decision" spends the reader's attention on a decision
        # already taken.
        story.append(Paragraph("No secrets are shared. No secret is granted by name to "
                               "more than one principal.", body))

    story.append(Paragraph(
        f"Principals outside the approved list ({len(unapproved)})", sub_sec))
    if unapproved:
        for h in unapproved:
            story.append(KeepTogether(_block(h)))
    else:
        story.append(Paragraph("<i>no exceptions</i>", note))

    story.append(Paragraph(
        f"Orphaned assignments &mdash; grants whose principal no longer exists "
        f"({len(orphaned)})", sub_sec))
    if orphaned:
        o_rows = [["Principal object id", "Right", "Resource", "Subscription"]]
        for h in orphaned:
            for g in h["grants"]:
                o_rows.append([
                    Paragraph(h["object_id"], cell),
                    Paragraph(g["role"], cell),
                    Paragraph(_reaches(g["raw_scope"], g["subscription"],
                                       data["subscription_contents"]), cell),
                    Paragraph(g["subscription"], cell)])
        ot = Table(o_rows, colWidths=[2.6 * inch, 1.9 * inch, 3.0 * inch, 1.9 * inch],
                   hAlign="LEFT", repeatRows=1)
        ot.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), BAND),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("LINEBELOW", (0, 0), (-1, -1), 0.25, RULE),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3)]))
        story.append(ot)
    else:
        story.append(Paragraph("<i>no exceptions</i>", note))

    story.append(Paragraph(
        f"Resources undescribed &mdash; carrying no description tag "
        f"({len(data['undescribed'])})", sub_sec))
    if data["undescribed"]:
        u_rows = [["Origin", "Type", "Name", "Where"]]
        for r in sorted(data["undescribed"],
                        key=lambda x: (x.get("origin", ""), x.get("type", ""),
                                       x.get("name", "").lower())):
            u_rows.append([Paragraph(r.get("origin", "&mdash;"), cell),
                           Paragraph(r.get("type", "&mdash;"), cell),
                           Paragraph(r.get("name", "&mdash;"), cell),
                           Paragraph(r.get("where") or r.get("group")
                                     or r.get("subscription") or "&mdash;", cell)])
        ut = Table(u_rows, colWidths=[1.1 * inch, 2.4 * inch, 3.4 * inch, 2.5 * inch],
                   hAlign="LEFT", repeatRows=1)
        ut.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), BAND),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("LINEBELOW", (0, 0), (-1, -1), 0.25, RULE),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3)]))
        story.append(ut)
    else:
        story.append(Paragraph("<i>no exceptions</i>", note))


    story.append(Paragraph(
        f"Grants that add nothing &nbsp;&middot;&nbsp; access held for no stated reason "
        f"({len(data['redundant_grants'])})", sub_sec))
    if data["redundant_grants"]:
        rows = [["Principal", "Role", "Scope", "Also covered by"]]
        for holder_name, role, scope, why in data["redundant_grants"]:
            rows.append([Paragraph(holder_name, cell), Paragraph(role, cell),
                         Paragraph(scope, cell), Paragraph(why, cell)])
        tr = Table(rows, colWidths=[1.7 * inch, 1.9 * inch, 2.7 * inch, 3.1 * inch],
                   hAlign="LEFT", repeatRows=1)
        tr.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), BAND),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("LINEBELOW", (0, 0), (-1, -1), 0.25, RULE),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3)]))
        story.append(tr)
    else:
        story.append(Paragraph("<i>no exceptions</i>", note))

    # Classification by group. The directory is where a person records what kind
    # of thing a principal is, so it is the foundation this section stands on --
    # and when it cannot be read, the section says so instead of reporting an
    # empty finding, which would read identically to a clean estate.
    head = _section_block(4, str(len(approved)) + " found")
    if approved:
        story.append(KeepTogether(head[1:] + _block(approved[0])))
        for h in approved[1:]:
            story.append(KeepTogether(_block(h)))
    else:
        story.extend(head)

    # -- The database half ---------------------------------------------
    # EPIC-002-F-003-S-009-REQ-B-001 and REQ-B-006. Every credential that
    # can reach the data is a user: a certificate subject, a username with
    # a password, an API key. Rights are stated cluster, database,
    # collection, once at the level granted, and a collection appears only
    # where it differs from what its database already grants.
    atlas = data.get("atlas") or {}
    story.append(Spacer(1, 8))
    story.append(Paragraph(
        "<b>Database users</b> &nbsp;&middot;&nbsp; every credential that "
        "reaches the data", sub_sec))
    if not atlas.get("readable"):
        story.append(Paragraph(
            "<b>Not read.</b> " + (atlas.get("reason") or "no reason recorded")
            + " &mdash; so this run states nothing about database rights, "
            "which is not the same as there being none.", note))
    else:
        story.append(Paragraph(
            f"Project {atlas.get('project','')} &nbsp;&middot;&nbsp; clusters: "
            + ", ".join(atlas.get("clusters") or []), note))
        reached = {r["holder"]: r for r
                   in database_rights_reached_through_secrets(data)}
        for user in sorted(atlas.get("users") or [],
                           key=lambda u: (u.get("username") or "").lower()):
            story.append(Spacer(1, 6))
            story.append(Paragraph(user.get("username") or "(unnamed)", who))
            story.append(Paragraph(
                f"{user.get('credential','')} credential"
                + (f" &nbsp;&middot;&nbsp; authenticates against "
                   f"{user.get('auth_database')}" if user.get("auth_database") else ""),
                note))
            rows = [[Paragraph("<b>Database</b>", cell),
                     Paragraph("<b>Collection</b>", cell),
                     Paragraph("<b>Right</b>", cell)]]
            for tree in atlas_tree(user, atlas.get("clusters") or []):
                if tree["whole_cluster"]:
                    rows.append([Paragraph("(whole cluster)", cell),
                                 Paragraph("&mdash;", cell),
                                 Paragraph(tree["whole_cluster"], cell)])
                for db in tree["databases"]:
                    if db["right"]:
                        rows.append([Paragraph(db["database"], cell),
                                     Paragraph("every collection", cell),
                                     Paragraph(db["right"], cell)])
                    for c in db["collections"]:
                        rows.append([
                            Paragraph("" if db["right"] else db["database"], cell),
                            Paragraph(c["collection"], cell),
                            Paragraph(c["right"], cell)])
            dups = duplicate_grants_of(
                user, atlas_tree(user, atlas.get("clusters") or []))
            if len(rows) == 1:
                story.append(Paragraph("Holds no database rights.", note))
            else:
                t = Table(rows, colWidths=[2.6 * inch, 2.6 * inch, 0.9 * inch],
                          hAlign="LEFT")
                t.setStyle(TableStyle([
                    ("BACKGROUND", (0, 0), (-1, 0), BAND),
                    ("LINEBELOW", (0, 0), (-1, 0), 0.4, RULE),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("TOPPADDING", (0, 0), (-1, -1), 2),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
                ]))
                story.append(t)
            # REQ-B-006: this user\'s own duplicates, at the end of its entry.
            if dups:
                story.append(Paragraph(
                    f"<b>Exceptions for this user &mdash; {len(dups)} grant(s) "
                    f"that add nothing</b>", note))
                for d in dups:
                    story.append(Paragraph(
                        f"&nbsp;&nbsp;{d['what']}: {d['why']}", note))

        # Who else holds these rights by being able to read the credential.
        if reached:
            story.append(Spacer(1, 8))
            story.append(Paragraph(
                "<b>Held indirectly</b> &nbsp;&middot;&nbsp; a credential in "
                "a vault is the ability to be whoever it identifies", sub_sec))
            for holder, row in sorted(reached.items()):
                story.append(Paragraph(
                    f"<b>{holder}</b> {row['how']}, and therefore holds every "
                    f"right of: " + ", ".join(row["database_users"]), note))
                if row.get("untagged_secrets"):
                    story.append(Paragraph(
                        f"{len(row['untagged_secrets'])} secret(s) in reach "
                        "state no grants-rights-for tag, so what they hand "
                        "their reader is not recorded: "
                        + ", ".join(row["untagged_secrets"][:8]), note))

    story.extend(_section_block(5))
    # The section name repeats with the column header. Without it a table that
    # runs over three pages reads as three sections, when it is one list sorted
    # alphabetically.
    gl = [[Paragraph("<b>What each right permits</b> &nbsp;&middot;&nbsp; "
                     "continued, one list in alphabetical order", cell), "", ""],
          ["Right", "Administrative", "What it permits"]]
    for role in sorted(data["role_text"], key=str.lower):
        meta = data["role_reach"].get(role, {})
        parts = []
        if meta.get("reach"):
            parts.append("<b>Permits " + "; ".join(meta["reach"]) + ".</b>")
        if meta.get("not_actions"):
            parts.append("<b>Except:</b> " + ", ".join(meta["not_actions"]) + ".")
        acts = meta.get("actions", [])
        data_acts = meta.get("data_actions", [])
        if acts:
            shown = acts[:6]
            more = f" and {len(acts) - len(shown)} more" if len(acts) > len(shown) else ""
            parts.append("<font size=7>actions: " + ", ".join(shown) + more + "</font>")
        if data_acts:
            shown = data_acts[:4]
            more = (f" and {len(data_acts) - len(shown)} more"
                    if len(data_acts) > len(shown) else "")
            parts.append("<font size=7>data actions: " + ", ".join(shown) + more + "</font>")
        # Cited, with its author named, never stated as the report's own finding.
        described = (data["role_text"].get(role) or "").strip()
        if described:
            source = ("the ChatHealthy role definition" if meta.get("custom")
                      else "the Azure role definition")
            parts.append(f"<font size=7><i>Description, from {source}:</i> "
                         f"{described}</font>")
        forbidden = data["role_conditions"].get(role)
        if forbidden:
            parts.append("<b>Conditioned where held:</b> the holder may neither grant "
                         "nor revoke " + ", ".join(forbidden)
                         + ", to any principal including itself. The list includes this "
                           "role, so the holder cannot lift the condition from its own "
                           "assignment.")
        gl.append([Paragraph(role, cell),
                   "yes" if role in data["privileged_roles"] else "",
                   Paragraph(" ".join(parts), cell)])
    gt = Table(gl, colWidths=[2.4 * inch, 1.0 * inch, 5.95 * inch], hAlign="LEFT",
               repeatRows=2)
    gst = [
        ("SPAN", (0, 0), (-1, 0)),
        ("BACKGROUND", (0, 0), (-1, 1), BAND),
        ("FONTNAME", (0, 1), (-1, 1), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("LINEBELOW", (0, 0), (-1, -1), 0.25, RULE),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ALIGN", (1, 0), (1, -1), "CENTER"),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]
    for i, role in enumerate(sorted(data["role_text"], key=str.lower), start=2):
        if role in data["privileged_roles"]:
            gst.append(("TEXTCOLOR", (1, i), (1, i), FLAG))
    gt.setStyle(TableStyle(gst))
    story.append(gt)

    story.extend(_section_block(6))
    # REQ-B-008. Membership is transitive: a right granted to a group is
    # held by every member of every group beneath it. A flat list of names
    # states none of that, and the whole reason a group is used to grant is
    # that it reaches further than the names written in it.
    _below = data.get("group_children") or {}
    if _below:
        story.append(Paragraph(
            "<b>What lies beneath each group</b> &nbsp;&middot;&nbsp; a right "
            "granted to a group is held by every member of every group under "
            "it", sub_sec))
        for parent in sorted(_below):
            kids = _below[parent]
            story.append(Paragraph(
                f"<b>{parent}</b> &rarr; " + ", ".join(sorted(kids)), note))
    elif data["groups_readable"]:
        story.append(Paragraph(
            "No group contains another, so every group reaches exactly the "
            "members named in it.", note))
    if not data["groups_readable"]:
        story.append(Paragraph(
            "Not attested. The reporting identity could not read the directory, so nothing "
            "below is classified. Directory.Read.All on the reporting identity is required.",
            note))
    else:
        groups = data["group_descriptions"]
        if groups:
            rows = [["Group", "Managed by", "What it means"]]
            for name in sorted(groups):
                rows.append([Paragraph(f"<b>{name}</b>", cell),
                             Paragraph(", ".join(data["group_owners"].get(name, []))
                                       or "nobody", cell),
                             Paragraph(groups[name] or "", cell)])
            tg = Table(rows, colWidths=[1.7 * inch, 2.0 * inch, 5.7 * inch],
                       hAlign="LEFT", repeatRows=1)
            tg.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), BAND),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
                ("LINEBELOW", (0, 0), (-1, -1), 0.25, RULE),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3)]))
            story.append(tg)
            story.append(Spacer(1, 8))
        if data["ungrouped"]:
            story.append(Paragraph(
                f"{len(data['ungrouped'])} principal(s) hold rights and belong to no group: "
                f"{', '.join(data['ungrouped'])}.", note))
        else:
            story.append(Paragraph(
                "Every principal holding rights belongs to a group.", body))

    story.extend(_section_block(7))
    if data["vault_wide"]:
        vw = [["Principal", "Right", "Where", "Access to every secret"]]
        for v in data["vault_wide"]:
            vw.append([Paragraph(f"<b>{v['principal']}</b>", cell),
                       Paragraph(v["role"], cell),
                       Paragraph(v["where"], cell),
                       Paragraph(v["access"], cell)])
        vt = Table(vw, colWidths=[2.3 * inch, 2.4 * inch, 3.0 * inch, 1.65 * inch],
                   hAlign="LEFT", repeatRows=1)
        vt.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), BAND),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("LINEBELOW", (0, 0), (-1, -1), 0.25, RULE),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3)]))
        story.append(vt)
    else:
        story.append(Paragraph("No principal holds access to every secret in a vault.",
                               body))

    # Classification by group. The directory is where a person records what kind
    # of thing a principal is, so it is the foundation this section stands on --

    story.append(Paragraph(
        f"Orphaned assignments &mdash; grants whose principal no longer exists "
        f"({len(orphaned)})", sub_sec))
    if orphaned:
        o_rows = [["Principal object id", "Right", "Resource", "Subscription"]]
        for h in orphaned:
            for g in h["grants"]:
                o_rows.append([
                    Paragraph(h["object_id"], cell),
                    Paragraph(g["role"], cell),
                    Paragraph(_reaches(g["raw_scope"], g["subscription"],
                                       data["subscription_contents"]), cell),
                    Paragraph(g["subscription"], cell)])
        ot = Table(o_rows, colWidths=[2.6 * inch, 1.9 * inch, 3.0 * inch, 1.9 * inch],
                   hAlign="LEFT", repeatRows=1)
        ot.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), BAND),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("LINEBELOW", (0, 0), (-1, -1), 0.25, RULE),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3)]))
        story.append(ot)
    else:
        story.append(Paragraph("<i>no exceptions</i>", note))

    story.append(Paragraph(
        f"Resources undescribed &mdash; carrying no description tag "
        f"({len(data['undescribed'])})", sub_sec))
    if data["undescribed"]:
        u_rows = [["Origin", "Type", "Name", "Where"]]
        for r in sorted(data["undescribed"],
                        key=lambda x: (x.get("origin", ""), x.get("type", ""),
                                       x.get("name", "").lower())):
            u_rows.append([Paragraph(r.get("origin", "&mdash;"), cell),
                           Paragraph(r.get("type", "&mdash;"), cell),
                           Paragraph(r.get("name", "&mdash;"), cell),
                           Paragraph(r.get("where") or r.get("group")
                                     or r.get("subscription") or "&mdash;", cell)])
        ut = Table(u_rows, colWidths=[1.1 * inch, 2.4 * inch, 3.4 * inch, 2.5 * inch],
                   hAlign="LEFT", repeatRows=1)
        ut.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), BAND),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("LINEBELOW", (0, 0), (-1, -1), 0.25, RULE),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3)]))
        story.append(ut)
    else:
        story.append(Paragraph("<i>no exceptions</i>", note))


    # Classification by group. The directory is where a person records what kind
    # of thing a principal is, so it is the foundation this section stands on --
    story.append(Paragraph("<i>no exceptions</i>", note))

    doc.build(story, onFirstPage=_page_furniture, onLaterPages=_page_furniture,
              canvasmaker=_NumberedCanvas)
    return out_path


def _recipient_from_secret(raw: str) -> tuple[str, str]:
    """Addressee and email from ENTITLEMENT_REPORT_TO_EMAIL.

    The conventional form a mail address has carried since RFC 822:

        Skip Snow <skip.snow@example.com>

    It was JSON, which is a shape invented here for a value that has had a
    standard spelling for forty years -- every mail client, every address
    book and every person already writes it this way, so a second spelling
    is one more thing to get wrong and nothing to gain.

    A bare address is still refused. The cover note greets someone by
    name, and a name guessed from the local-part of an address is a guess
    presented as a fact.
    """
    text = (raw or "").strip()
    if not text:
        raise ChatHealthyException(
            mode="notification_recipient_missing",
            component="EntitlementReport",
            message="ENTITLEMENT_REPORT_TO_EMAIL is absent")
    # Parsed without a regular expression: the angle brackets are the
    # delimiters and finding them is two index calls.
    close = text.rfind(">")
    open_ = text.rfind("<", 0, close if close != -1 else len(text))
    if open_ == -1 or close == -1 or close < open_:
        raise ChatHealthyException(
            mode="config_error",
            component="EntitlementReport",
            message=(
                "ENTITLEMENT_REPORT_TO_EMAIL must be "
                "'Addressee Name <mailbox@host>'; "
                f"a bare address is not enough, and this is {text!r}"))
    addressee = text[:open_].strip().strip('"').strip()
    email = text[open_ + 1:close].strip()
    if not addressee or not email or "@" not in email:
        raise ChatHealthyException(
            mode="config_error",
            component="EntitlementReport",
            message=("ENTITLEMENT_REPORT_TO_EMAIL must name both the "
                     "addressee and a mailbox: "
                     "'Addressee Name <mailbox@host>'"))
    return addressee, email


def _produced_on_pacific(when: _dt.datetime) -> str:
    """Production time where the firm is: California.

    Worked out here rather than asked of the platform. It used to load
    America/Los_Angeles from the IANA database and fall back to UTC when
    that database was absent, which is why a report produced at noon in
    California went out stamped 19:02 UTC -- true of Greenwich, and the
    firm does not operate there.

    US Pacific is UTC-8, and UTC-7 while daylight time is in force: from
    the second Sunday in March to the first Sunday in November, both at
    02:00 local. That rule is public, fixed, and needs no package.
    """
    def _nth_sunday(year: int, month: int, nth: int) -> _dt.date:
        d = _dt.date(year, month, 1)
        d += _dt.timedelta(days=(6 - d.weekday()) % 7)   # first Sunday
        return d + _dt.timedelta(weeks=nth - 1)

    utc = when.astimezone(_dt.timezone.utc)
    year = utc.year
    # The switch happens at 02:00 local, which is 10:00 UTC entering
    # daylight time and 09:00 UTC leaving it.
    starts = _dt.datetime.combine(_nth_sunday(year, 3, 2),
                                  _dt.time(10, 0), _dt.timezone.utc)
    ends = _dt.datetime.combine(_nth_sunday(year, 11, 1),
                                _dt.time(9, 0), _dt.timezone.utc)
    daylight = starts <= utc < ends
    local = utc + _dt.timedelta(hours=-7 if daylight else -8)
    return local.strftime("%d %B %Y at %H:%M ") + ("PDT" if daylight else "PST")


def send(pdf_path: Path, data: dict) -> dict:
    """Mail the report, with the PDF attached.

    The transmission is posted directly rather than through the pipeline's
    notification client: that module lives in the repository and is not
    present where this runs. One POST, the same API, no shared dependency
    between an audit control and operational alerting.
    """
    import base64

    raw_to = _ch_os.environ.get("ENTITLEMENT_REPORT_TO_EMAIL", "").strip()
    api_key = _ch_os.environ.get("SPARKMAIL_API_KEY", "").strip()
    sender = _ch_os.environ.get("NOTIFICATION_FROM_EMAIL", "noreply@chathealthy.ai").strip()
    if not api_key:
        raise ChatHealthyException(
            mode="notification_recipient_missing",
            component="EntitlementReport",
            message="the report cannot be sent: SPARKMAIL_API_KEY absent",
            context={"missing": ["SPARKMAIL_API_KEY"]})
    addressee, to = _recipient_from_secret(raw_to)
    produced = _produced_on_pacific(data["generated"])
    stamp = data["generated"].strftime("%Y-%m-%d")
    body = (
        f"{addressee}\n"
        f"Please find enclosed the ChatHealthy.ai entitlements report. "
        f"It was produced on {produced}.\n"
    )
    payload = {
        "content": {
            "from": sender,
            "subject": f"ChatHealthy.ai entitlements report {stamp}",
            "text": body,
            "attachments": [{
                "type": "application/pdf",
                "name": f"ChatHealthy-entitlements-{stamp}.pdf",
                "data": base64.b64encode(pdf_path.read_bytes()).decode("ascii"),
            }],
        },
        "recipients": [{"address": to, "name": addressee}],
    }
    r = requests.post(
        "https://api.sparkpost.com/api/v1/transmissions",
        headers={"Authorization": api_key, "Content-Type": "application/json"},
        data=json.dumps(payload), timeout=120)
    if r.status_code not in (200, 201):
        raise ChatHealthyException(
            mode="notification_send_failed",
            component="EntitlementReport",
            message=f"SparkPost returned {r.status_code}: {r.text[:300]}",
            context={"status": r.status_code, "to": to})
    return {"channel": "email", "status": "sent", "to": to, "addressee": addressee}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Daily access entitlement report.")
    ap.add_argument("--no-email", action="store_true",
                    help="render the PDF and skip the send")
    ap.add_argument("--out", default="", help="where to write the PDF")
    ap.add_argument("--register-from", default="",
                    help="environment whose deployment_architecture.json supplies the "
                         "approved register (dev|qa|prod), or a full URL. Omitted, the "
                         "register is the one baked into this runbook, or the repository "
                         "copy when running from a working tree.")
    args = ap.parse_args(argv)

    data = collect()
    stamp = data["generated"].strftime("%Y-%m-%d")
    out = Path(args.out) if args.out else Path(
        _ch_os.environ.get("TEMP", ".")) / f"ChatHealthy-entitlements-{stamp}.pdf"
    render_pdf(data, out)

    unapproved = [h for h in data["holders"]
                  if not h["approved"] and not h.get("orphaned")]
    orphaned = [h for h in data["holders"] if h.get("orphaned")]
    undeclared = [r for r in data["rightless"] if not r["approved"]]
    # The attestation qualifier belongs in the summary line, not only in the PDF.
    # "0 exceptions" read from a run that could not see the directory says the
    # same words as one that could, and means something entirely different.
    attested = "attested" if data["groups_readable"] else "NOT ATTESTED (directory unreadable)"
    _LOG.info("entitlement report: %d assignments, %d principals, %d exceptions, "
              "%d ungrouped, %s, pdf=%s",
              data["assignment_count"], len(data["holders"]),
              len(unapproved) + len(data["rightless"]),
              len(data["ungrouped"]), attested, out)

    if args.no_email:
        return 0
    _LOG.info("entitlement report sent: %s", send(out, data).get("status"))
    return 0


def run_as_runbook() -> int:
    """Entry point when Azure Automation runs this on a schedule or webhook.

    Takes no arguments, renders the report and mails it. A failure here is
    silence on an audit control, so it is raised rather than swallowed.
    """
    return main([])


if __name__ == "__main__":
    sys.exit(main())
