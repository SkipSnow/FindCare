"""Boot race-condition probe.

Wired to SessionStart and UserPromptSubmit hooks. Captures when each hook
fires, what context it receives, and whether it can reach stdout (visible to
Claude) and stderr (visible in hook debug). Reading this log after a fresh
login tells us if SessionStart fires before or after OAuth completes.

Log: %TEMP%/chathealthy_boot_probe.log (JSONL, one entry per hook fire)
"""

import argparse
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import sys as _ch_sys, pathlib as _ch_pl
for _ch_d in _ch_pl.Path(__file__).resolve().parents:
    if (_ch_d / ".git").exists():
        _ch_lib = _ch_d / "ChatHealthyLib" / "src"
        if str(_ch_lib) not in _ch_sys.path:
            _ch_sys.path.insert(0, str(_ch_lib))
        break
# The chain materialises the application .env, which sets
# CH_LOG_DESTINATION=mongo and CH_LOG_DB=pipelineAdmin. Those are the
# deployed application's facts, not this tool's: devops tooling runs on
# a workstation and its log is the operator's terminal. Inheriting them
# made a build depend on a Mongo write it has no grant for.
import os as _ch_os
_ch_os.environ["CH_LOG_DESTINATION"] = "stderr"
from chathealthy_lib.logging_service import ChatHealthyLoggingService
_CH_LOG = ChatHealthyLoggingService()

LOG_PATH = Path(tempfile.gettempdir()) / "chathealthy_boot_probe.log"
BRAIN_DIR = Path(__file__).resolve().parents[4] / "brain" / "machine_artifacts" / "content"


def _probe(parser):
    """Probe the boot surface and report."""
    parser.add_argument("--event", required=True,
                        choices=["SessionStart", "UserPromptSubmit", "Stop", "SessionEnd"])
    args = parser.parse_args()

    now = datetime.now(timezone.utc).isoformat()

    stdin_data = ""
    try:
        if not sys.stdin.isatty():
            stdin_data = sys.stdin.read()
    except Exception as e:
        stdin_data = f"<stdin read error: {e}>"

    stdin_parsed = None
    try:
        stdin_parsed = json.loads(stdin_data) if stdin_data else None
    except Exception:
        pass

    # Canary read: confirm the brain content directory is readable. Uses
    # engineering_rules.json (always present, large enough to fail loudly if
    # the content dir is unreachable). Was version.json; swapped per
    # a deleted file.
    brain_read_ok = False
    brain_read_err = ""
    try:
        with open(BRAIN_DIR / "engineering_rules.json", encoding="utf-8") as f:
            json.load(f)
        brain_read_ok = True
    except Exception as e:
        brain_read_err = str(e)

    _SECRET_HINTS = ("KEY", "TOKEN", "SECRET", "PASSWORD", "AUTH", "CREDENTIAL")

    def _redact(name: str, value: str) -> str:
        if any(h in name.upper() for h in _SECRET_HINTS):
            return f"<redacted len={len(value)}>"
        return value

    env_keys_of_interest = {
        k: _redact(k, v) for k, v in os.environ.items()
        if "CLAUDE" in k.upper() or "ANTHROPIC" in k.upper() or "CURSOR" in k.upper()
        or k in ("PWD", "USERNAME", "USERPROFILE", "TEMP")
    }

    entry = {
        "timestamp": now,
        "event": args.event,
        "pid": os.getpid(),
        "ppid": os.getppid() if hasattr(os, "getppid") else None,
        "cwd": os.getcwd(),
        "stdin_len": len(stdin_data),
        "stdin_keys": list(stdin_parsed.keys()) if isinstance(stdin_parsed, dict) else None,
        "session_id": (stdin_parsed or {}).get("session_id") if isinstance(stdin_parsed, dict) else None,
        "transcript_path": (stdin_parsed or {}).get("transcript_path") if isinstance(stdin_parsed, dict) else None,
        "source": (stdin_parsed or {}).get("source") if isinstance(stdin_parsed, dict) else None,
        "brain_read_ok": brain_read_ok,
        "brain_read_err": brain_read_err,
        "env": env_keys_of_interest,
    }

    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, default=str) + "\n")
    except Exception as e:
        _CH_LOG.info(f"PROBE: log write failed: {e}")

    # Stderr visibility test — shows up in hook debug output
    _CH_LOG.info(f"PROBE[{args.event}] fired at {now} pid={os.getpid()} brain_ok={brain_read_ok}")

    # Stdout visibility test — additionalContext is shown to Claude.
    # Only SessionStart emits it; UserPromptSubmit stays silent to avoid
    # conflicting with the existing boot hook's own hookSpecificOutput.
    if args.event == "SessionStart":
        output = {
            "hookSpecificOutput": {
                "hookEventName": "SessionStart",
                "additionalContext": (
                    f"BOOT-PROBE: SessionStart hook ran at {now}. "
                    f"brain_read_ok={brain_read_ok}. "
                    f"If you (Claude) see this line in your context, SessionStart output "
                    f"reached you successfully."
                ),
            }
        }
        json.dump(output, sys.stdout)

    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    return _probe(parser)


if __name__ == "__main__":
    sys.exit(main())
