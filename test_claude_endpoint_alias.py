"""Claude wrapper rows must not present as a second provider.

`claude-cli/X` and `claude/X` are the same Anthropic model reached by two log
sources (OpenClaw wrapper counters vs Claude's own transcript). The yellow
provider column on /cost renders `model_reference.endpoint`, so an unmapped
`claude-cli` prefix split one provider into two: OpenClaw-owned work appeared
as both CLAUDE and CLAUDE-CLI.

Run: /opt/homebrew/bin/python3 test_claude_endpoint_alias.py
"""

import importlib.util
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent

spec = importlib.util.spec_from_file_location("migrate_sessions", SCRIPT_DIR / "migrate_sessions.py")
ms = importlib.util.module_from_spec(spec)
sys.modules["migrate_sessions"] = ms
spec.loader.exec_module(ms)


def test_wrapper_and_transcript_share_an_endpoint():
    """The two log sources for one model must agree on endpoint and author."""
    for model in ("claude-opus-5", "claude-sonnet-4-6", "claude-fable-5-1"):
        cli_ep, cli_author, cli_model, _ = ms.infer_model_reference(f"claude-cli/{model}")
        tr_ep, tr_author, tr_model, _ = ms.infer_model_reference(f"claude/{model}")
        assert cli_ep == tr_ep, f"{model}: endpoint {cli_ep!r} != {tr_ep!r}"
        assert cli_author == tr_author == "Anthropic", f"{model}: author {cli_author!r}/{tr_author!r}"
        assert cli_model == tr_model, f"{model}: model alias {cli_model!r} != {tr_model!r}"


def test_endpoint_is_claude():
    """Anthropic-via-Claude is one endpoint name, so billing pools don't split."""
    assert ms.infer_model_reference("claude-cli/claude-opus-5")[0] == "claude"


def test_bracketed_variant_survives():
    """claude-cli/claude-opus-5[1m] is a real row in the live DB."""
    ep, author, _, _ = ms.infer_model_reference("claude-cli/claude-opus-5[1m]")
    assert (ep, author) == ("claude", "Anthropic")


def test_unrelated_providers_unchanged():
    """Control: the fix must not widen to anything else."""
    assert ms.infer_model_reference("codex/gpt-5")[0] == "codex"
    assert ms.infer_model_reference("anthropic/claude-opus-5")[0] == "anthropic"
    assert ms.infer_model_reference("openai/gpt-5")[0] == "openai"


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if not name.startswith("test_") or not callable(fn):
            continue
        try:
            fn()
            print(f"ok   {name}")
        except AssertionError as exc:
            failures += 1
            print(f"FAIL {name}: {exc}")
    print(f"\n{'FAILED' if failures else 'OK'} ({failures} failures)")
    sys.exit(1 if failures else 0)
