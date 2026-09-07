"""Typed data classes for one Deployment record.

Mirrors the canonical schema at
`Website/schemas/ChatHealthyDeploymentArchitectureSchema.json`
($id `https://dev.chathealthy.ai/schemas/ChatHealthyDeploymentArchitectureSchema.json`).

One `TargetRecord` is one deployment target's full definition across every
environment in which that target is realized, plus the files that compose
the target. A `DeploymentCollection` is the set of all `TargetRecord`s for
a deployment.

There is no `metadata` field anywhere. Processing/transformation logic
lives in software, not in this data shape.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterator
import sys as _ch_sys, pathlib as _ch_pl  # noqa: E402
for _ch_d in _ch_pl.Path(__file__).resolve().parents:
    if (_ch_d / '.git').exists():
        _ch_lib = _ch_d / 'ChatHealthyLib' / 'src'
        if str(_ch_lib) not in _ch_sys.path:
            _ch_sys.path.insert(0, str(_ch_lib))
        break
from chathealthy_lib.exceptions import ChatHealthyException  # noqa: E402
def _ch_exc():
    """ChatHealthyException without assuming the library is installed.
    These modules run as bare scripts in the devops chain."""
    import sys as _s, pathlib as _p
    for _d in _p.Path(__file__).resolve().parents:
        if (_d / ".git").exists():
            _l = _d / "ChatHealthyLib" / "src"
            if str(_l) not in _s.path:
                _s.path.insert(0, str(_l))
            break
    from chathealthy_lib.exceptions import ChatHealthyException
    return ChatHealthyException



# Which facts name a collection. A collection is the singular fact repeated,
# so one occurrence and one-of-many are written identically; what tells them
# apart is knowing the fact is a collection, and that knowledge belongs to the
# code that reads the platform rather than to the schema. Everything absent
# from this set is a single value or a group.
_COLLECTIONS: dict[str, str] = {
    "subnet": "subnets",
    "private_endpoint": "private_endpoints",
    "private_dns_zone": "private_dns_zones",
    "blob_container": "blob_containers",
    "path_prefix": "path_prefixes",
    "runtime_consumer": "runtime_consumers",
    "entry": "entries",
    "product": "products",
    "phase": "phases",
    "access_rule": "accepted_access",
    "firewall_rule": "firewall_rules",
}

# Facts whose value is a boolean rather than text. A fact value is written as
# text, so the type belongs to the code that reads the platform, in the same
# way the arity of a collection does.
_BOOLEAN_FACTS: frozenset[str] = frozenset({"enabled"})


@dataclass(slots=True)

class EnvironmentBinding:
    """One environment binding for a TargetRecord."""

    env_binding: str  # closed enum: local|dev|qa|prod
    node_address: str  # addressable location; {env} substitution allowed
    azure: dict | None = None
    azure_container_app: dict | None = None
    azure_container_apps_environment: dict | None = None
    azure_container_app_job: dict | None = None
    azure_automation: dict | None = None
    azure_automation_account: dict | None = None
    azure_container_registry: dict | None = None
    azure_key_vault: dict | None = None
    azure_storage_account: dict | None = None
    azure_vnet: dict | None = None
    azure_resource_group: dict | None = None
    identity: dict | None = None
    huggingface_space: dict | None = None
    cloudflare_pages: dict | None = None
    # Windows service registration for a host_os_process target.
    # Present when the service manager supervises the process.
    windows_service: dict | None = None
    cloudflare_firewall_rules: list | None = None
    branch: str | None = None
    # The platform that runs this target here. Derived from platforms[]:
    # the entry whose facts carry role=host. Every other entry configures
    # the target rather than hosting it.
    host: str | None = None
    # Consolidated Atlas sub-block: project + cluster + accepted_access +
    # private_endpoint_service under one substrate target. Deploy handler
    # provisions everything Atlas-side from this one block.
    atlas: dict | None = None
    # The collections on this cluster that the manifest wholly governs for
    # this environment. Read straight off the binding rather than out of a
    # platform's facts: a record body is arbitrary JSON, and the fact
    # encoding carries a name and a value, not a document.
    config_collections: list | None = None
    # Package lists — child artifacts that get deployed TO this target's
    # host. runbooks[] is populated when the parent target is an
    # azure_automation_account; jobs[] when the parent is an
    # azure_container_apps_environment. Dispatch iterates these lists so
    # a runbook or job is a PACKAGE under its host target, not a target
    # in its own right.
    runbooks: list | None = None
    jobs: list | None = None
    # Generic packages[] passthrough — carried on the loaded binding so
    # non-runbook/non-job package kinds (e.g. secret_from_file under a KV
    # target) are visible to their host's deploy handler. Not synthesized
    # into per-package targets; the host handler iterates and dispatches.
    packages: list | None = None

    def to_dict(self) -> dict:
        out: dict = {"env_binding": self.env_binding, "node_address": self.node_address}
        for key in (
            "azure",
            "azure_container_app",
            "azure_container_apps_environment",
            "azure_container_app_job",
            "azure_automation",
            "azure_automation_account",
            "azure_container_registry",
            "azure_key_vault",
            "azure_storage_account",
            "azure_vnet",
            "azure_resource_group",
            "identity",
            "huggingface_space",
            "cloudflare_pages",
            "windows_service",
            "cloudflare_firewall_rules",
            "branch",
            "atlas",
            "config_collections",
            "runbooks",
            "jobs",
            "packages",
        ):
            val = getattr(self, key)
            if val is not None:
                out[key] = val
        return out

    @classmethod
    def _from_facts(cls, facts: list) -> dict:
        """The value a platform's facts describe.

        A fact carries a name and either a value or facts of its own. A name
        in _COLLECTIONS accumulates into a list even when it appears once.
        """
        out: dict = {}
        for f in facts or ():
            name = f["name"]
            if "value" in f:
                value = f["value"]
                if name in _BOOLEAN_FACTS:
                    value = value == "true"
            else:
                value = cls._from_facts(f["facts"])
            plural = _COLLECTIONS.get(name)
            if plural is not None:
                out.setdefault(plural, []).append(value)
            else:
                out[name] = value
        return out

    @classmethod
    def _platform(cls, d: dict, name: str):
        """The named platform's facts, as the value that platform's handler
        reads. Returns None when this binding declares no such platform."""
        for p in d.get("platforms") or ():
            if p.get("name") == name:
                built = cls._from_facts(p.get("facts"))
                # A platform whose facts are one repeated collection IS that
                # collection: cloudflare_firewall_rules is a list of rules,
                # not an object holding a list.
                if len(built) == 1:
                    key, only = next(iter(built.items()))
                    if isinstance(only, list) and key in _COLLECTIONS.values():
                        return only
                return built
        return None

    @staticmethod
    def _host_of(d: dict) -> str | None:
        for p in d.get("platforms") or ():
            for f in p.get("facts") or ():
                if f.get("name") == "role" and f.get("value") == "host":
                    return p.get("name")
        return None

    @classmethod
    def from_dict(cls, d: dict) -> "EnvironmentBinding":
        return cls(
            env_binding=d["env_binding"],
            node_address=d["node_address"],
            azure=d.get("azure"),
            azure_container_app=d.get("azure_container_app"),
            azure_container_apps_environment=d.get("azure_container_apps_environment"),
            azure_container_app_job=d.get("azure_container_app_job"),
            azure_automation=d.get("azure_automation"),
            azure_automation_account=cls._platform(d, "azure_automation_account"),
            azure_container_registry=cls._platform(d, "azure_container_registry"),
            azure_key_vault=cls._platform(d, "azure_key_vault"),
            azure_storage_account=cls._platform(d, "azure_storage_account"),
            azure_vnet=cls._platform(d, "azure_vnet"),
            azure_resource_group=cls._platform(d, "azure_resource_group"),
            identity=d.get("identity"),
            huggingface_space=cls._platform(d, "hf_space"),
            cloudflare_pages=cls._platform(d, "cloudflare_pages_project"),
            windows_service=d.get("windows_service"),
            cloudflare_firewall_rules=cls._platform(d, "cloudflare_firewall_rules"),
            branch=d.get("branch"),
            host=cls._host_of(d),
            atlas=cls._platform(d, "atlas"),
            config_collections=d.get("config_collections"),
            runbooks=d.get("runbooks"),
            jobs=d.get("jobs"),
            packages=d.get("packages"),
        )


@dataclass(slots=True)
class FileComposition:
    """One file that composes a TargetRecord.

    disposition decides who owns the bytes:
      - 'managed': JSON owns the bytes via embedded_content. Crosswalk
        requires byte-equality with the repo tree. Extractor materializes.
      - 'referenced': only source_location is declared. embedded_content
        is None. Deploy copies bytes from origin/<branch>'s tree at
        deploy time. Crosswalk validates presence only.
    """

    feature_id: str  # regex-shape ref into agile_backlog
    source_location: str  # repo-relative path; forward slashes
    handler_type: str  # closed enum: python_source|...|cert_or_key
    disposition: str  # closed enum: managed|referenced
    embedded_content: str | None = None  # present iff disposition='managed'
    # layout: structured spec from which the Builder generates the
    # file's embedded_content. Required when disposition='managed' AND
    # handler_type == 'dockerfile' (instruction list).
    layout: object | None = None
    # content_hash: sha256 hex digest of the file's bytes at deploy
    # time. Builder populates for disposition='referenced' (managed
    # files are protected by embedded_content). Crosswalk validates
    # the disk content still hashes to this value, catching cases
    # where on-disk content drifts from the committed/JSON state.
    content_hash: str | None = None
    # package: the package_id this file belongs to. A package is a logical
    # set of capabilities; the build stages each package into its own
    # subdirectory so a file's path in the build tree names the capability
    # it serves. Dropping this on load is what made the build unable to
    # honour the packages the manifest declares.
    package: str | None = None

    def to_dict(self) -> dict[str, object]:
        out: dict[str, object] = {
            "feature_id": self.feature_id,
            "source_location": self.source_location,
            "handler_type": self.handler_type,
            "disposition": self.disposition,
        }
        if self.package:
            out["package"] = self.package
        if self.disposition == "managed":
            if self.embedded_content is None:
                raise ChatHealthyException(
            mode="value_error",
            component="target_record",
            message=f"FileComposition {self.source_location!r}: disposition="
                    f"'managed' requires embedded_content")
            out["embedded_content"] = self.embedded_content
            if self.handler_type == "dockerfile":
                if self.layout is None:
                    raise ChatHealthyException(
            mode="value_error",
            component="target_record",
            message=f"FileComposition {self.source_location!r}: "
                        f"managed {self.handler_type} requires layout")
                out["layout"] = self.layout
            elif self.layout is not None:
                out["layout"] = self.layout
        elif self.disposition == "referenced":
            if self.embedded_content is not None:
                raise ChatHealthyException(
            mode="value_error",
            component="target_record",
            message=f"FileComposition {self.source_location!r}: disposition="
                    f"'referenced' forbids embedded_content")
            if self.layout is not None:
                raise ChatHealthyException(
            mode="value_error",
            component="target_record",
            message=f"FileComposition {self.source_location!r}: disposition="
                    f"'referenced' forbids layout")
            if self.content_hash is not None:
                out["content_hash"] = self.content_hash
        else:
            raise ChatHealthyException(
            mode="value_error",
            component="target_record",
            message=f"FileComposition {self.source_location!r}: unknown "
                f"disposition {self.disposition!r}")
        return out

    @classmethod
    def from_dict(cls, d: dict[str, object]) -> "FileComposition":
        return cls(
            feature_id=str(d["feature_id"]),
            source_location=str(d["source_location"]),
            handler_type=str(d["handler_type"]),
            disposition=str(d["disposition"]),
            embedded_content=d.get("embedded_content"),  # type: ignore[arg-type]
            layout=d.get("layout"),
            content_hash=d.get("content_hash"),  # type: ignore[arg-type]
            package=d.get("package"),  # type: ignore[arg-type]
        )


@dataclass(slots=True)
class TargetRecord:
    """One Deployment target's full record."""

    target_id: str  # opaque; pattern ^target_[a-z0-9_]{1,64}$
    target_kind: str  # closed enum: hf_space|cloudflare_pages_project|...
    environments: list[EnvironmentBinding]
    files: list[FileComposition]
    secrets: dict | None = None  # name -> store_id; same keys apply across all envs;
                                  # per-env values resolved via SecretsResolver at deploy time
    variables: dict | None = None  # name -> source_qualifier; deploy-computed values
                                    # pushed as HF Space variables (visible config, not
                                    # secrets). Each value is a source-qualifier string;
                                    # the deploy script dispatches on the qualifier
                                    # prefix. Supported qualifiers (data-driven, NO
                                    # target-specific knowledge in the deploy code):
                                    #   "env_name"                    — value = env name (dev/qa/prod)
                                    #   "local_cert_file:<rel_path>"  — value = base64 of file content
                                    #   "peer_url:<target_id>"        — value = peer HF Space URL
                                    #   "rename_from:<other_entry>"   — value = resolved value of another entry, pushed under THIS name
    promote_chain_bound: bool = True
    # True (default): target rides the dev->qa->prod promote chain; local
    # branch must match env_binding.branch for the requested env. False:
    # single-environment shared infrastructure (the one pipeline FA, the
    # one durable router ACA, the one Automation Account + its runbooks)
    # deployed from any branch, typically dev. The branch-vs-env guards
    # in deploy_chathealthy.py skip records with this flag set False.
    allowed_roles: list | None = None
    # The set of Azure RBAC roles this target will accept role-assignments
    # for. Deploy chain intersects `IdentityCatalog[*].roles` with each
    # target's allowed_roles to compute the concrete role-assignment set.
    # Data-driven; no role names appear in the deploy scripts themselves.
    permissions: list | None = None
    # Explicit self-contained per-target grants: each entry {object_id, role}
    # is applied at this target's Azure scope. Independent of the intersection
    # model above; used for USER / SERVICE_PRINCIPAL principals whose grants
    # do not fit the identity_catalog roles[] x allowed_roles[] pattern.
    peers: list | None = None
    # Cross-substrate handshake declarations. Each entry names a peer
    # target and the kind of handshake this target participates in with
    # it (e.g., Atlas target declares peers=[{target_id: <vnet>, kind:
    # private_endpoint_service}]). Deploy uses this for sequencing the
    # two-sided provisioning; it is documentary + coordination.

    def env_binding_set(self) -> set[str]:
        return {e.env_binding for e in self.environments}

    def file_by_source_location(self, source_location: str) -> FileComposition | None:
        for f in self.files:
            if f.source_location == source_location:
                return f
        return None

    def to_dict(self) -> dict[str, object]:
        out: dict[str, object] = {
            "target_id": self.target_id,
            "target_kind": self.target_kind,
        }
        if not self.promote_chain_bound:
            out["promote_chain_bound"] = False
        out["environments"] = [e.to_dict() for e in self.environments]
        out["files"] = [f.to_dict() for f in self.files]
        if self.secrets:
            out["secrets"] = self.secrets
        if self.variables:
            out["variables"] = self.variables
        if self.allowed_roles:
            out["allowed_roles"] = self.allowed_roles
        if self.permissions:
            out["permissions"] = self.permissions
        if self.peers:
            out["peers"] = self.peers
        return out

    @classmethod
    def from_dict(cls, d: dict[str, object]) -> "TargetRecord":
        envs_raw = d["environments"]
        files_raw = d["files"]
        if not isinstance(envs_raw, list) or not isinstance(files_raw, list):
            raise ChatHealthyException(
            mode="type_error",
            component="target_record",
            message="environments and files must be lists")
        return cls(
            target_id=str(d["target_id"]),
            target_kind=str(d["target_kind"]),
            environments=[EnvironmentBinding.from_dict(e) for e in envs_raw],
            files=[FileComposition.from_dict(f) for f in files_raw],
            secrets=d.get("secrets"),  # type: ignore[arg-type]
            variables=d.get("variables"),  # type: ignore[arg-type]
            promote_chain_bound=bool(d.get("promote_chain_bound", True)),
            allowed_roles=d.get("allowed_roles"),  # type: ignore[arg-type]
            permissions=d.get("permissions"),  # type: ignore[arg-type]
            peers=d.get("peers"),  # type: ignore[arg-type]
        )


@dataclass(slots=True)
class DeploymentCollection:
    """The set of TargetRecords that constitutes one deployment.

    Persisted to `brain/machine_artifacts/content/deployment_architecture.json`
    as a JSON array of TargetRecord documents. Also carries the raw
    IdentityCatalog and CustomRoleCatalog envelope entries so the deploy
    chain can iterate identity+role facts from the manifest instead of
    from hardcoded code paths.
    """

    records: list[TargetRecord] = field(default_factory=list)
    identity_catalog: list = field(default_factory=list)
    custom_role_catalog: list = field(default_factory=list)

    def __iter__(self) -> Iterator[TargetRecord]:
        return iter(self.records)

    def __len__(self) -> int:
        return len(self.records)

    def add(self, record: TargetRecord) -> None:
        for existing in self.records:
            if existing.target_id == record.target_id:
                raise ChatHealthyException(
            mode="value_error",
            component="target_record",
            message=f"target_id collision: {record.target_id!r} already in collection")
        self.records.append(record)

    def by_target_id(self, target_id: str) -> TargetRecord | None:
        for r in self.records:
            if r.target_id == target_id:
                return r
        return None

    def all_feature_ids(self) -> set[str]:
        return {f.feature_id for r in self.records for f in r.files}

    def all_source_locations(self) -> set[str]:
        return {f.source_location for r in self.records for f in r.files}

    def to_list(self) -> list[dict[str, object]]:
        return [r.to_dict() for r in self.records]

    @classmethod
    def from_list(cls, data: list[dict[str, object]]) -> "DeploymentCollection":
        # Package expansion (interim while full package-native dispatch
        # is deferred). Any target env_binding carrying packages[] produces
        # a synthetic per-package TargetRecord tagged with the package's
        # kind (runbook, job, ...) so downstream build/deploy code that
        # still dispatches per target_kind sees the packages as individual
        # targets. Each synthetic target has a `parent_target_id` field
        # linking it back to its host. The parent target is retained.
        # Runbook packages are NOT expanded into targets. There is one
        # destination -- the Automation Account -- and ten capabilities
        # inside it, so ten synthetic targets described one place ten
        # times and staged every runbook's bytes twice. The build and the
        # deploy read the packages directly; _synth_runbook_package
        # remains as the function that derives a package's azure_automation
        # block, which is what it was always really doing.
        expanded: list[dict[str, object]] = []
        for d in data:
            expanded.append(d)
            for eb in d.get("environments", []) or []:
                for pkg in eb.get("packages", []) or []:
                    if pkg.get("kind", "") == "job":
                        expanded.append(_synth_job_package(d, eb, pkg))
        coll = cls()
        for d in expanded:
            coll.add(TargetRecord.from_dict(d))
        return coll


def _parse_aa_node_address(node_address: str) -> tuple[str, str]:
    if not node_address:
        return "", ""
    parts = node_address.split("/")
    rg = ""
    aa = ""
    for i, p in enumerate(parts):
        if p.lower() == "resourcegroups" and i + 1 < len(parts):
            rg = parts[i + 1]
        elif p.lower() == "automationaccounts" and i + 1 < len(parts):
            aa = parts[i + 1]
    return rg, aa


def _synth_runbook_package(aa_record: dict, eb: dict, pkg: dict) -> dict:
    env = eb.get("env_binding")
    parent_node = eb.get("node_address", "")
    rg, aa_name = _parse_aa_node_address(parent_node)
    pkg_id = pkg.get("package_id", "")
    cfg = pkg.get("config") or {}
    runbook_name = cfg.get("runbook_name") or pkg_id
    aa_block = {
        "resource_group": rg,
        "automation_account": aa_name,
        "runbook_name": runbook_name,
        "schedule_names": cfg.get("schedule_names", []),
        "python_packages": cfg.get("python_packages", []),
        "automation_variables": cfg.get("automation_variables", []),
    }
    # Optional per-package webhook configs — carried through to the
    # deploy handler which owns the mint/reuse flow.
    if cfg.get("webhook"):
        aa_block["webhook"] = cfg["webhook"]
    if cfg.get("webhook_to_kv"):
        aa_block["webhook_to_kv"] = cfg["webhook_to_kv"]
    return {
        "target_id": f"target_azure_automation_runbook_{pkg_id}",
        "target_kind": "azure_automation_runbook",
        "promote_chain_bound": bool(aa_record.get("promote_chain_bound", True)),
        "environments": [{
            "env_binding": env,
            "node_address": cfg.get("node_address")
                or f"{parent_node}/runbooks/{runbook_name}",
            "azure_automation": aa_block,
        }],
        "files": pkg.get("files", []),
        "secrets": pkg.get("secrets", {}),
    }


def _resolve_apps_env_vars(sibling_packages: list, role: str, env: str) -> dict:
    """Merge default_apps_environment env_vars with {role}_apps_environment
    env_vars. Later overrides earlier. Templates {role} and {env} are
    substituted at synth time so ensure_aca_job can pass literal values
    to the container."""
    merged: dict = {}
    for pkg in sibling_packages or []:
        pid = pkg.get("package_id", "")
        if pid == "default_apps_environment":
            merged.update((pkg.get("config") or {}).get("env_vars", {}))
    for pkg in sibling_packages or []:
        pid = pkg.get("package_id", "")
        if pid == f"{role}_apps_environment":
            merged.update((pkg.get("config") or {}).get("env_vars", {}))
    # Substitute {role} and {env} templates.
    return {
        k: (str(v).replace("{role}", role).replace("{env}", env)
            if isinstance(v, str) else v)
        for k, v in merged.items()
    }


def _synth_job_package(env_record: dict, eb: dict, pkg: dict) -> dict:
    env = eb.get("env_binding")
    env_block = eb.get("azure_container_apps_environment") or {}
    rg = env_block.get("resource_group", "")
    env_name = env_block.get("environment_name", "")
    pkg_id = pkg.get("package_id", "")
    cfg = pkg.get("config") or {}
    role = cfg.get("role", "")
    baked_env_vars = _resolve_apps_env_vars(
        eb.get("packages") or [], role, env,
    )
    return {
        "target_id": f"target_azure_container_app_job_{pkg_id}",
        "target_kind": "azure_container_app_job",
        "provisioning": "deploy_provisioned",
        "promote_chain_bound": bool(env_record.get("promote_chain_bound", True)),
        "environments": [{
            "env_binding": env,
            "node_address": cfg.get("node_address") or cfg.get("job_name", pkg_id),
            "azure_container_app_job": {
                "resource_group": rg,
                "job_name": cfg.get("job_name"),
                "environment_name": env_name,
                "registry_name": cfg.get("registry_name") or "chpipelinedevacr",
                "image_repository": cfg.get("image_repository"),
                "dockerfile": cfg.get("dockerfile"),
                "role": role,
                "trigger_type": cfg.get("trigger_type", "Manual"),
                "cpu": cfg.get("cpu"),
                "memory": cfg.get("memory"),
                "parallelism": cfg.get("parallelism", 1),
                "replica_timeout": cfg.get("replica_timeout", 7200),
                "node_identity": cfg.get("node_identity"),
                "managed_identity_name": cfg.get("managed_identity_name"),
                "key_vault_uri": cfg.get("key_vault_uri"),
                "container_env_vars": baked_env_vars,
            },
        }],
        "files": pkg.get("files", []),
        "secrets": pkg.get("secrets", {}),
    }
