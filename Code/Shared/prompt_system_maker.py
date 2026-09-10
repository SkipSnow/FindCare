# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
#
# PromptSystemMaker — builds runtime configuration from brain artifacts.
#
# Loads system prompt rules, tool definitions, and emergency keywords
# from JSON files. main.py consumes the output — no config in code.
#
# Source: brain/machine_artifacts/ (JSON files)
# Changed: extracted from main.py inline strings

import json
import os
from pathlib import Path

import sys as _sys, pathlib as _pl
for _d in _pl.Path(__file__).resolve().parents:
    if (_d / ".git").exists():
        _lib = _d / "ChatHealthyLib" / "src"
        if str(_lib) not in _sys.path:
            _sys.path.insert(0, str(_lib))
        break
from chathealthy_lib.logging_service import ChatHealthyLoggingService

_log = ChatHealthyLoggingService()


class PromptSystemMaker:
    """Builds runtime configuration from brain artifacts and environment.

    Responsible for:
    - System prompt assembly from rules
    - Tool definitions (Anthropic schema)
    - Emergency keywords
    - Welcome message
    """

    def __init__(self, brain_dir: str = None, env_prefix: str = "dev"):
        self._brain = Path(brain_dir) if brain_dir else self._find_brain()
        self._env = env_prefix
        self._prompt_config = None
        self._tools = None
        self._emergency_keywords = None

    @staticmethod
    def _find_brain() -> Path:
        """Walk up from this file to find the brain directory."""
        d = Path(__file__).parent
        for _ in range(5):
            brain = d / "brain"
            if brain.is_dir():
                return brain
            d = d.parent
        return Path("brain")

    # ------------------------------------------------------------------
    # Brain reader — reads records from collection JSONs
    # Source: brain/machine_artifacts/{collection}.json -> records[]
    # ------------------------------------------------------------------
    def read_collection(self, collection: str) -> list[dict]:
        """Read all records from a brain collection JSON."""
        path = self._brain / "machine_artifacts" / "content" / f"{collection}.json"
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data.get("records", [])
        except Exception as exc:
            _log.warning("Failed to read collection '%s': %s", collection, exc)
            return []

    def read_record(self, collection: str, record_id: str) -> dict:
        """Read a specific record from a brain collection by _record_id."""
        for record in self.read_collection(collection):
            if record.get("_record_id") == record_id:
                return record
        _log.warning("Record '%s' not found in collection '%s'", record_id, collection)
        return {}

    # Emergency keywords — CV-006 in controlled_vocabularies.json only.
    # ------------------------------------------------------------------
    def load_emergency_keywords(self) -> list[str]:
        """Load flat list of emergency keywords from controlled vocabulary CV-006."""
        if self._emergency_keywords is not None:
            return self._emergency_keywords

        cv_path = self._brain / "machine_artifacts" / "content" / "controlled_vocabularies.json"
        try:
            with open(cv_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            for vocab in data.get("vocabularies", []):
                if vocab.get("vocabulary_id") == "CV-006" or vocab.get("name") == "emergency_keywords":
                    keywords = [m["value"] for m in vocab.get("members", []) if m.get("value")]
                    self._emergency_keywords = keywords
                    _log.info("Loaded %d emergency keywords from CV-006 in %s", len(keywords), cv_path)
                    return keywords
        except Exception as exc:
            _log.warning("Failed to load CV-006 from controlled_vocabularies: %s", exc)

        _log.warning("Using built-in emergency keyword defaults")
        self._emergency_keywords = [
            "chest pain", "heart attack", "can't breathe", "stroke",
            "severe bleeding", "unconscious", "overdose", "suicide",
            "seizure", "anaphylaxis", "choking",
        ]
        return self._emergency_keywords

    # ------------------------------------------------------------------
    # Tool definitions — Anthropic format
    # Source: brain/machine_artifacts/anthropic_tools.json (or built inline)
    # ------------------------------------------------------------------
    def load_tool_definitions(self) -> list[dict]:
        """Load Anthropic tool schemas. Tries brain artifact first, falls back to built-in."""
        if self._tools is not None:
            return self._tools

        path = self._brain / "machine_artifacts" / "anthropic_tools.json"
        if path.exists():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    self._tools = json.load(f)
                _log.info("Loaded %d tool definitions from %s", len(self._tools), path)
                return self._tools
            except Exception as exc:
                _log.warning("Failed to load tool definitions: %s — using built-in", exc)

        self._tools = self._build_tool_definitions()
        return self._tools

    def _build_tool_definitions(self) -> list[dict]:
        """Built-in tool definitions. Fallback if brain artifact missing."""
        return [
            {
                "name": "find_providers",
                "description": "Search for healthcare providers in a specific US state. FindCare covers DE, MS, VA.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "specialty_query": {"type": "string"},
                        "state": {"type": "string"},
                        "city": {"type": "string"},
                        "county": {"type": "string"},
                        "limit": {"type": "integer"},
                    },
                    "required": ["specialty_query", "state"],
                },
            },
            {
                "name": "find_specialty_codes",
                "description": "Look up medical specialty taxonomy codes (NUCC).",
                "input_schema": {
                    "type": "object",
                    "properties": {"query": {"type": "string"}},
                    "required": ["query"],
                },
            },
            {
                "name": "search_clinical_trials",
                "description": "Search for actively recruiting clinical trials on ClinicalTrials.gov.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "condition": {"type": "string"},
                        "location": {"type": "string"},
                        "user_location": {"type": "string", "description": "User location for travel time — any location worldwide."},
                        "max_results": {"type": "integer"},
                    },
                    "required": ["condition"],
                },
            },
            {
                "name": "get_skip_snow_context",
                "description": "Return Skip Snow's professional background, career summary, and LinkedIn profile.",
                "input_schema": {"type": "object", "properties": {}},
            },
            {
                "name": "get_chathealthy_context",
                "description": "Return ChatHealthy.ai company context: business plan and operating principles.",
                "input_schema": {"type": "object", "properties": {}},
            },
            {
                "name": "lookup_provider_external",
                "description": "Look up provider details from NPI Registry and research links.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "provider_name": {"type": "string"},
                        "npi": {"type": "string"},
                        "state": {"type": "string"},
                    },
                    "required": ["provider_name"],
                },
            },
        ]

    # ------------------------------------------------------------------
    # System prompt — assembled from rules
    # ------------------------------------------------------------------
    def build_system_prompt(self, name: str = "Skip Snow", website: str = "ChatHealthy.AI",
                            emergency_response: str = "", follow_up_check: bool = False) -> str:
        """Build the system prompt from rules. No hardcoded strings in main.py."""
        rules = [
            f"You are acting as {name} on {website}'s website. "
            f"Represent {name} and {website} faithfully. Be professional and engaging.",

            f"RULE 0 — EMERGENCY: If the user describes ANY medical emergency, "
            f"STOP. Do not use any tool. Respond ONLY with: '{emergency_response}'",

            f"RULE 1 — CONTEXT TOOLS: "
            f"Call get_skip_snow_context for {name}'s background. "
            f"Call get_chathealthy_context for {website}'s mission. "
            f"Only when explicitly asked. Never for greetings. Answer ONLY from tool results. "
            f"CRITICAL: Output the 'connect' field EXACTLY as-is — it contains pre-formatted markdown links.",

            f"RULE 2 — UNANSWERABLE: You know ONLY: {name}'s background, {website}'s platform, "
            f"providers in DE/MS/VA, specialties, clinical trials. NOTHING else. "
            f"For anything outside these sources, say so plainly and do NOT "
            f"answer from training data.",

            f"RULE 3 — MEDICAL ADVICE: When declining medical advice, you MUST say exactly: "
            f"'I do not give medical advice.' Then explain you CAN help navigate: find providers, "
            f"specialists, clinical trials. Never prescribe, diagnose, or recommend treatment.",

            f"RULE 4 — PROVIDER FORMAT: Always bullet list, never markdown table. "
            f"Format: - **Name**\\n  - Address\\n  - County\\n  - Phone\\n  - NPI: number",

            f"RULE 5 — PROVIDER DETAIL: When user asks about a specific provider, "
            f"call lookup_provider_external.",

            f"RULE 6 — CLINICAL TRIAL TRAVEL: After first results, ask about travel distance. "
            f"User location can be ANYWHERE worldwide. Google Routes handles international. "
            f"Do NOT assume US-only. Do NOT include travel on first call.",

            f"RULE 8 — INCLUSIVITY AND SENSITIVITY: Treat ALL healthcare queries with equal respect "
            f"regardless of gender identity, sexual orientation, race, religion, or any personal characteristic. "
            f"When a user asks about gender-affirming care, LGBT-friendly providers, reproductive health, "
            f"mental health, or any sensitive topic — search immediately. Do NOT ask unnecessary clarifying "
            f"questions that could feel intrusive or judgmental. Never express surprise, hesitation, or "
            f"moral commentary. The user's healthcare needs are valid. Search first, support always.",

            f"RULE 9 — PAGINATION: The find_providers tool returns total_count and a page of results. "
            f"ALWAYS tell the user: 'Here are the first N of TOTAL providers.' Then ask: "
            f"'Would you like to see more?' NEVER dump all results. NEVER set a high limit. "
            f"The system handles pagination — your job is to present the first page and ask. "
            f"Do NOT suggest external directories. The data is in our system.",
        ]

        return "\n\n## STRICT ANSWER RULES — NO EXCEPTIONS\n" + "\n".join(rules)

    # ------------------------------------------------------------------
    # ME context — founder/company content from PDF + text files
    # Source: me/ directory (PDF, txt files)
    # ------------------------------------------------------------------
    def load_me_context(self, me_dir: str) -> dict:
        """Load founder and company context from files."""
        import os
        ctx = {}
        try:
            from pypdf import PdfReader
            reader = PdfReader(os.path.join(me_dir, "SkipSnowLinkedInProfile.pdf"))
            ctx["linkedin"] = "".join(p.extract_text() or "" for p in reader.pages)
        except Exception:
            ctx["linkedin"] = ""
        try:
            with open(os.path.join(me_dir, "anthropic_principles.txt"), "r", encoding="utf-8") as f:
                ctx["anthropic_principles"] = f.read()
        except Exception:
            ctx["anthropic_principles"] = ""
        try:
            with open(os.path.join(me_dir, "summary.txt"), "r", encoding="utf-8") as f:
                ctx["summary"] = f.read()
        except Exception:
            ctx["summary"] = ""
        try:
            from pypdf import PdfReader
            reader = PdfReader(os.path.join(me_dir, "chatHealthy_ai_business_plan.pdf"))
            ctx["business_plan"] = "".join(p.extract_text() or "" for p in reader.pages)
        except Exception:
            ctx["business_plan"] = ""
        _log.info("ME context loaded: %d fields", sum(1 for v in ctx.values() if v))
        return ctx

    # ------------------------------------------------------------------
    # Build number — from MongoDB
    # The canonical source is the latest
    # record in frontEndAdmin.BuildVersions (single global collection). env_prefix is
    # ignored; kept in the signature for backward compatibility.
    # ------------------------------------------------------------------
    @staticmethod
    def get_build_number(get_db_fn, env_prefix: str) -> str:
        """Read latest build number from frontEndAdmin.BuildVersions. Read once at startup."""
        try:
            db = get_db_fn()
            if db is None:
                return "?"
            record = db["frontEndAdmin"]["BuildVersions"].find_one(sort=[("from", -1)])
            return str(record["build"]) if record else "0"
        except Exception:
            return "?"

    # ------------------------------------------------------------------
    # Welcome message
    # ------------------------------------------------------------------
    @staticmethod
    def build_welcome_message() -> str:
        return (
            '<div style="margin-top:10vh;">'
            '<p style="text-align:center;'
            "font-family:Georgia,'Times New Roman',serif;"
            'font-size:2em;line-height:1.3;margin:0;">'
            'Find care with Caregivers, or at a facility in the United States, or find a clinical trial anywhere in the world.<br>'
            "Let's talk about it."
            '</p>'
            '</div>'
        )

    # ------------------------------------------------------------------
    # Text utility
    # ------------------------------------------------------------------
    @staticmethod
    def trim(text: str, max_chars: int) -> str:
        """Trim text with truncation note."""
        if len(text) <= max_chars:
            return text
        return text[:max_chars] + f"\n[... truncated at {max_chars} chars ...]"
