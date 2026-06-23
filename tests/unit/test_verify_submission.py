from __future__ import annotations

import json
import importlib.util
import sys
from pathlib import Path

import pytest


_SPEC = importlib.util.spec_from_file_location(
    "verify_submission", Path("scripts/verify_submission.py")
)
assert _SPEC is not None and _SPEC.loader is not None
verify_submission = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = verify_submission
_SPEC.loader.exec_module(verify_submission)


def test_accepted_chase_and_twap_testnet_manifests_pass(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(verify_submission, "ROOT", tmp_path)
    write_manifest(tmp_path, "chase", algorithm="CHASE", environment="testnet")
    write_manifest(
        tmp_path, "twap", algorithm="TWAP", environment="Environment.TESTNET"
    )

    assert verify_submission.testnet_evidence_failures() == []


def test_wrong_environment_does_not_count_as_accepted_testnet_evidence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(verify_submission, "ROOT", tmp_path)
    write_manifest(tmp_path, "chase", algorithm="CHASE", environment="simulation")
    write_manifest(tmp_path, "twap", algorithm="TWAP", environment="testnet")

    failures = verify_submission.testnet_evidence_failures()

    assert len(failures) == 1
    assert failures[0].is_bypassable is True
    assert "CHASE" in failures[0].message
    assert "exchange_order_ids" in failures[0].message


@pytest.mark.parametrize("content", ["{", "[]"])
def test_malformed_or_non_object_manifest_is_not_bypassable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    content: str,
) -> None:
    monkeypatch_verifier_gate(monkeypatch, tmp_path)
    manifest_path = evidence_root(tmp_path) / "bad" / "evidence_manifest.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(content)
    write_manifest(tmp_path, "chase", algorithm="CHASE", environment="testnet")
    write_manifest(tmp_path, "twap", algorithm="TWAP", environment="testnet")

    assert (
        verify_submission.main(
            ["--allow-missing-final-report", "--allow-missing-testnet-evidence"]
        )
        == 1
    )


def test_missing_algorithm_is_bypassable_only_with_testnet_evidence_flag(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch_verifier_gate(monkeypatch, tmp_path)
    make_final_report(tmp_path)
    write_manifest(tmp_path, "chase", algorithm="CHASE", environment="testnet")

    assert verify_submission.main([]) == 1
    assert verify_submission.main(["--allow-missing-testnet-evidence"]) == 0


def test_missing_final_report_is_bypassable_only_with_final_report_flag(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch_verifier_gate(monkeypatch, tmp_path)
    write_manifest(tmp_path, "chase", algorithm="CHASE", environment="testnet")
    write_manifest(tmp_path, "twap", algorithm="TWAP", environment="testnet")

    assert verify_submission.main([]) == 1
    assert verify_submission.main(["--allow-missing-final-report"]) == 0


def test_default_verifier_runs_non_live_tests_only(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    commands: list[list[str]] = []
    monkeypatch_verifier_gate(monkeypatch, tmp_path, commands=commands)

    assert (
        verify_submission.main(
            ["--allow-missing-final-report", "--allow-missing-testnet-evidence"]
        )
        == 0
    )

    assert ["uv", "run", "pytest", "-q", "tests/unit", "tests/simulation"] in commands
    assert ["uv", "run", "pytest", "-q"] not in commands
    assert not any("tests/integration" in command for command in commands)


def test_include_live_integration_runs_integration_tests_after_non_live_tests(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    commands: list[list[str]] = []
    monkeypatch_verifier_gate(monkeypatch, tmp_path, commands=commands)

    assert (
        verify_submission.main(
            [
                "--allow-missing-final-report",
                "--allow-missing-testnet-evidence",
                "--include-live-integration",
            ]
        )
        == 0
    )

    assert commands[:3] == [
        ["uv", "run", "pytest", "-q", "tests/unit", "tests/simulation"],
        ["uv", "run", "pytest", "-q", "tests/integration"],
        ["uv", "run", "ruff", "check", "."],
    ]


def monkeypatch_verifier_gate(
    monkeypatch: pytest.MonkeyPatch,
    root: Path,
    *,
    commands: list[list[str]] | None = None,
) -> None:
    monkeypatch.setattr(verify_submission, "ROOT", root)
    if commands is None:
        monkeypatch.setattr(verify_submission, "run", lambda _cmd: None)
    else:
        monkeypatch.setattr(verify_submission, "run", lambda cmd: commands.append(cmd))
    monkeypatch.setattr(
        verify_submission, "build_wheel", lambda _dist_dir: root / "test.whl"
    )
    monkeypatch.setattr(verify_submission, "verify_wheel", lambda _wheel_path: None)


def write_manifest(
    root: Path, run_name: str, *, algorithm: str, environment: str
) -> Path:
    manifest_path = evidence_root(root) / run_name / "evidence_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "algorithm": algorithm,
                "environment": environment,
                "accepted_exchange_order_evidence": True,
                "exchange_order_ids": ["12345"],
            }
        )
    )
    return manifest_path


def evidence_root(root: Path) -> Path:
    return root / "reports" / "evidence" / "testnet"


def make_final_report(root: Path) -> None:
    report_path = root / "reports" / "latex" / "report.pdf"
    report_path.parent.mkdir(parents=True)
    report_path.write_bytes(b"%PDF-1.4\n")
