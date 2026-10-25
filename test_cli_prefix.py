#!/usr/bin/env python3
"""cli-<x>/ ownership prefix: rewrite rules and ratecard resolution."""
import migrate_sessions as ms


def test_rewrite():
    f = ms.cli_prefixed_model_raw
    # CLI-owned rows gain the ownership prefix.
    assert f("claude/claude-opus-5", "cli") == "cli-claude/claude-opus-5"
    assert f("codex/gpt-5.6-luna", "cli") == "cli-codex/gpt-5.6-luna"
    assert f("kimi-code/k3-256k", "cli") == "cli-kimi/k3-256k"
    # OpenClaw-owned rows are untouched -- claude/ is a genuinely mixed prefix.
    assert f("claude/claude-opus-5", "openclaw") == "anthropic-c/claude-opus-5"
    assert f("codex/gpt-5.6-luna", "openclaw") == "codex/gpt-5.6-luna"
    # Non-CLI providers never gain a prefix, whatever the session kind.
    assert f("openai/gpt-5.6-sol", "cli") == "openai/gpt-5.6-sol"
    assert f("zai/glm-5.3", "cli") == "zai/glm-5.3"
    # Idempotent: a second pass must not produce cli-cli-claude/.
    assert f(f("claude/claude-opus-5", "cli"), "cli") == "cli-claude/claude-opus-5"
    assert f("", "cli") == ""


def test_ratecard_resolves():
    """cli- rows must price identically to their pre-rename names."""
    usage = {"input_tokens": 1_000_000, "output_tokens": 1_000_000,
             "cache_read": 1_000_000, "cache_write": 1_000_000}
    for old, new in (("claude/claude-opus-5", "cli-claude/claude-opus-5"),
                     ("codex_cli/gpt-5.6-luna", "cli-codex/gpt-5.6-luna"),
                     ("kimi-code/k3-256k", "cli-kimi/k3-256k")):
        before = ms.calculate_payg_equivalent_cost(old, usage)
        after = ms.calculate_payg_equivalent_cost(new, usage)
        assert before is not None, f"no ratecard for {old}"
        assert after == before, f"{new} priced differently from {old}: {after} vs {before}"


def test_gate_admits_cli_prefixes():
    """maybe_calculate_cli_cost must not skip cli- rows with zero source cost."""
    usage = {"input_tokens": 1_000_000, "output_tokens": 0,
             "cache_read": 0, "cache_write": 0}
    for m in ("cli-claude/claude-opus-5", "cli-codex/gpt-5.6-luna", "cli-kimi/k3-256k"):
        cost = ms.maybe_calculate_cli_cost(m, usage, None)
        assert cost.get("total"), f"{m} priced at zero"


def test_endpoint_column_unchanged():
    """cli- is an ownership label, not a new provider column value."""
    for m, endpoint, author in (
        ("cli-claude/claude-opus-5", "claude", "Anthropic"),
        ("cli-codex/gpt-5.6-luna", "codex", "OpenAI"),
        ("cli-kimi/k3-256k", "kimi-code", "Moonshot"),
    ):
        got_endpoint, got_author, _, _ = ms.infer_model_reference(m)
        assert got_endpoint == endpoint, f"{m}: endpoint {got_endpoint} != {endpoint}"
        assert got_author == author, f"{m}: author {got_author} != {author}"


if __name__ == "__main__":
    test_rewrite()
    test_ratecard_resolves()
    test_gate_admits_cli_prefixes()
    test_endpoint_column_unchanged()
    print("ok")


import unittest

def load_tests(loader, tests, pattern):
    for name, fn in sorted(globals().items()):
        if name.startswith('test_') and callable(fn):
            tests.addTest(unittest.FunctionTestCase(fn))
    return tests
