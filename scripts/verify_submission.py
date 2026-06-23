from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import os
import shlex
import subprocess
import sys
import tempfile
import textwrap
import zipfile
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
REQUIRED_EVIDENCE_ALGORITHMS = ("CHASE", "TWAP")
MISSING_FINAL_REPORT_BYPASS = "missing_final_report"
MISSING_TESTNET_EVIDENCE_BYPASS = "missing_testnet_evidence"
NON_LIVE_TEST_COMMAND = ["uv", "run", "pytest", "-q", "tests/unit", "tests/simulation"]
LIVE_INTEGRATION_TEST_COMMAND = ["uv", "run", "pytest", "-q", "tests/integration"]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the final submission verification gate."
    )
    parser.add_argument(
        "--allow-missing-final-report",
        action="store_true",
        help="Warn instead of failing when reports/latex/report.pdf is absent.",
    )
    parser.add_argument(
        "--allow-missing-testnet-evidence",
        action="store_true",
        help="Warn instead of failing when accepted Testnet evidence is absent or incomplete.",
    )
    parser.add_argument(
        "--include-live-integration",
        action="store_true",
        help=(
            "Also run tests/integration; these tests may use Binance Testnet credentials "
            "and network access."
        ),
    )
    args = parser.parse_args(argv)

    try:
        run(NON_LIVE_TEST_COMMAND)
        if args.include_live_integration:
            run(LIVE_INTEGRATION_TEST_COMMAND)
        run(["uv", "run", "ruff", "check", "."])
        with tempfile.TemporaryDirectory(prefix="submission-dist-") as dist_tmp:
            wheel_path = build_wheel(Path(dist_tmp))
            verify_wheel(wheel_path)
    except subprocess.CalledProcessError as exc:
        error(f"command failed with exit code {exc.returncode}: {shlex.join(exc.cmd)}")
        return exc.returncode
    except VerificationFailure as exc:
        error(str(exc))
        return 1

    issues = []
    if missing_report := final_report_failure():
        issues.append(missing_report)
    issues.extend(testnet_evidence_failures())

    if issues:
        strict_issues = []
        for issue in issues:
            if issue_is_bypassed(issue, args):
                warning(issue.message)
            else:
                strict_issues.append(issue)
        if strict_issues:
            for issue in strict_issues:
                error(issue.message)
            return 1

    print("submission_verification=ok")
    return 0


def run(cmd: list[str]) -> None:
    print(f"+ {shlex.join(cmd)}", flush=True)
    subprocess.run(cmd, cwd=ROOT, check=True)


def build_wheel(dist_dir: Path) -> Path:
    run(
        [
            "uv",
            "run",
            "python",
            "-m",
            "build",
            "--no-isolation",
            "--wheel",
            "--outdir",
            str(dist_dir),
        ]
    )
    wheels = sorted(dist_dir.glob("*.whl"))
    if len(wheels) != 1:
        raise VerificationFailure(
            f"expected one built wheel in {dist_dir}, found {len(wheels)}"
        )
    return wheels[0]


def verify_wheel(wheel_path: Path) -> None:
    with zipfile.ZipFile(wheel_path) as wheel:
        wheel_members = set(wheel.namelist())

    missing_members = sorted({"config.py", "api/runtime.py"} - wheel_members)
    if missing_members:
        raise VerificationFailure(
            f"wheel is missing required importable files: {', '.join(missing_members)}"
        )

    import_check = textwrap.dedent(
        """
        import importlib
        import sys
        from pathlib import Path

        wheel_path = Path(sys.argv[1]).resolve()
        repo_root = Path(sys.argv[2]).resolve()
        repo_src = repo_root / "src"

        sys.path[:] = [
            str(wheel_path),
            *[
                entry
                for entry in sys.path
                if entry
                and Path(entry).resolve() not in {repo_root, repo_src}
                and entry != str(wheel_path)
            ],
        ]

        for module_name in ("config", "api.runtime"):
            module = importlib.import_module(module_name)
            module_file = str(getattr(module, "__file__", ""))
            if not module_file.startswith(str(wheel_path)):
                raise SystemExit(f"{module_name} imported from {module_file}")
        """
    )
    import_env = os.environ.copy()
    import_env.pop("PYTHONPATH", None)
    with tempfile.TemporaryDirectory(prefix="submission-import-") as import_tmp:
        print(
            f"+ {shlex.join([sys.executable, '-c', '<wheel import check>', str(wheel_path)])}",
            flush=True,
        )
        subprocess.run(
            [sys.executable, "-c", import_check, str(wheel_path), str(ROOT)],
            cwd=import_tmp,
            env=import_env,
            check=True,
        )


def final_report_failure() -> VerificationIssue | None:
    report_path = ROOT / "reports" / "latex" / "report.pdf"
    if report_path.is_file():
        return None
    return VerificationIssue(
        "missing final report: reports/latex/report.pdf is required",
        bypass_kind=MISSING_FINAL_REPORT_BYPASS,
    )


def testnet_evidence_failures() -> list[VerificationIssue]:
    evidence_root = ROOT / "reports" / "evidence" / "testnet"
    manifests = sorted(evidence_root.glob("**/evidence_manifest.json"))
    if not manifests:
        return [
            VerificationIssue(
                "missing accepted Testnet evidence: no reports/evidence/testnet/**/"
                "evidence_manifest.json files found",
                bypass_kind=MISSING_TESTNET_EVIDENCE_BYPASS,
            )
        ]

    accepted: dict[str, list[Path]] = {
        algorithm: [] for algorithm in REQUIRED_EVIDENCE_ALGORITHMS
    }
    invalid_manifests: list[VerificationIssue] = []
    for manifest_path in manifests:
        try:
            payload = json.loads(manifest_path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            invalid_manifests.append(
                VerificationIssue(
                    f"invalid Testnet evidence manifest: {manifest_path.relative_to(ROOT)} ({exc})"
                )
            )
            continue
        if not isinstance(payload, dict):
            invalid_manifests.append(
                VerificationIssue(
                    f"invalid Testnet evidence manifest: {manifest_path.relative_to(ROOT)} "
                    "(manifest is not an object)"
                )
            )
            continue

        algorithm = _normalized_manifest_value(payload.get("algorithm")).upper()
        if algorithm not in accepted:
            continue
        if is_accepted_exchange_order_manifest(payload):
            accepted[algorithm].append(manifest_path)

    failures = [
        VerificationIssue(
            f"missing accepted Testnet evidence: {algorithm} manifest must have "
            "environment=testnet, accepted_exchange_order_evidence=true, and a non-empty "
            "exchange_order_ids entry",
            bypass_kind=MISSING_TESTNET_EVIDENCE_BYPASS,
        )
        for algorithm, paths in accepted.items()
        if not paths
    ]
    failures.extend(invalid_manifests)
    return failures


def is_accepted_exchange_order_manifest(payload: dict[str, Any]) -> bool:
    if _normalized_manifest_value(payload.get("environment")) != "testnet":
        return False
    if payload.get("accepted_exchange_order_evidence") is not True:
        return False
    exchange_order_ids = payload.get("exchange_order_ids")
    if not isinstance(exchange_order_ids, list):
        return False
    return any(
        isinstance(exchange_order_id, str) and bool(exchange_order_id.strip())
        for exchange_order_id in exchange_order_ids
    )


def _normalized_manifest_value(value: Any) -> str:
    normalized = str(value or "").strip().casefold()
    return normalized.rsplit(".", maxsplit=1)[-1]


def issue_is_bypassed(issue: VerificationIssue, args: argparse.Namespace) -> bool:
    if issue.bypass_kind == MISSING_FINAL_REPORT_BYPASS:
        return bool(args.allow_missing_final_report)
    if issue.bypass_kind == MISSING_TESTNET_EVIDENCE_BYPASS:
        return bool(args.allow_missing_testnet_evidence)
    return False


def warning(message: str) -> None:
    print(f"WARNING: {message}", file=sys.stderr)


def error(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)


class VerificationFailure(RuntimeError):
    pass


@dataclass(frozen=True)
class VerificationIssue:
    message: str
    bypass_kind: str | None = None

    @property
    def is_bypassable(self) -> bool:
        return self.bypass_kind is not None


if __name__ == "__main__":
    raise SystemExit(main())
