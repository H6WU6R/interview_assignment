from __future__ import annotations

import argparse
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the final submission verification gate.")
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
    args = parser.parse_args(argv)

    try:
        run(["uv", "run", "pytest", "-q"])
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

    failures = []
    if missing_report := final_report_failure():
        failures.append(missing_report)
    failures.extend(testnet_evidence_failures())

    if failures:
        if args.allow_missing_final_report and args.allow_missing_testnet_evidence:
            for failure in failures:
                warning(failure)
        else:
            strict_failures = [
                failure
                for failure in failures
                if not (
                    args.allow_missing_final_report
                    and failure.startswith("missing final report:")
                )
                and not (
                    args.allow_missing_testnet_evidence
                    and failure.startswith("missing accepted Testnet evidence:")
                )
            ]
            warning_failures = [failure for failure in failures if failure not in strict_failures]
            for failure in warning_failures:
                warning(failure)
            if strict_failures:
                for failure in strict_failures:
                    error(failure)
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
        raise VerificationFailure(f"expected one built wheel in {dist_dir}, found {len(wheels)}")
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


def final_report_failure() -> str | None:
    report_path = ROOT / "reports" / "latex" / "report.pdf"
    if report_path.is_file():
        return None
    return "missing final report: reports/latex/report.pdf is required"


def testnet_evidence_failures() -> list[str]:
    evidence_root = ROOT / "reports" / "evidence" / "testnet"
    manifests = sorted(evidence_root.glob("**/evidence_manifest.json"))
    if not manifests:
        return [
            "missing accepted Testnet evidence: no reports/evidence/testnet/**/"
            "evidence_manifest.json files found"
        ]

    accepted: dict[str, list[Path]] = {algorithm: [] for algorithm in REQUIRED_EVIDENCE_ALGORITHMS}
    invalid_manifests: list[str] = []
    for manifest_path in manifests:
        try:
            payload = json.loads(manifest_path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            invalid_manifests.append(f"{manifest_path.relative_to(ROOT)} ({exc})")
            continue
        if not isinstance(payload, dict):
            invalid_manifests.append(f"{manifest_path.relative_to(ROOT)} (manifest is not an object)")
            continue

        algorithm = str(payload.get("algorithm", "")).upper()
        if algorithm not in accepted:
            continue
        if is_accepted_exchange_order_manifest(payload):
            accepted[algorithm].append(manifest_path)

    failures = [
        f"missing accepted Testnet evidence: {algorithm} manifest must have "
        "accepted_exchange_order_evidence=true and a non-empty exchange_order_id"
        for algorithm, paths in accepted.items()
        if not paths
    ]
    failures.extend(
        f"missing accepted Testnet evidence: unreadable or invalid manifest {detail}"
        for detail in invalid_manifests
    )
    return failures


def is_accepted_exchange_order_manifest(payload: dict[str, Any]) -> bool:
    if payload.get("accepted_exchange_order_evidence") is not True:
        return False
    exchange_order_ids = payload.get("exchange_order_ids")
    if not isinstance(exchange_order_ids, list):
        return False
    return any(
        isinstance(exchange_order_id, str) and bool(exchange_order_id.strip())
        for exchange_order_id in exchange_order_ids
    )


def warning(message: str) -> None:
    print(f"WARNING: {message}", file=sys.stderr)


def error(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)


class VerificationFailure(RuntimeError):
    pass


if __name__ == "__main__":
    raise SystemExit(main())
