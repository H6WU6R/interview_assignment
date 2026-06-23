from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import zipfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_built_wheel_contains_and_imports_top_level_config(tmp_path: Path) -> None:
    dist_dir = tmp_path / "dist"
    dist_dir.mkdir()

    build_result = subprocess.run(
        [
            sys.executable,
            "-m",
            "build",
            "--no-isolation",
            "--wheel",
            "--outdir",
            str(dist_dir),
        ],
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    assert build_result.returncode == 0, build_result.stdout

    wheels = sorted(dist_dir.glob("*.whl"))
    assert len(wheels) == 1
    wheel_path = wheels[0]

    with zipfile.ZipFile(wheel_path) as wheel:
        wheel_members = set(wheel.namelist())
    assert "config.py" in wheel_members
    assert "api/runtime.py" in wheel_members

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

        imported = [
            importlib.import_module("config"),
            importlib.import_module("api.runtime"),
        ]
        for module in imported:
            module_file = str(getattr(module, "__file__", ""))
            if not module_file.startswith(str(wheel_path)):
                raise SystemExit(f"{module.__name__} imported from {module_file}")
        """
    )
    import_env = os.environ.copy()
    import_env.pop("PYTHONPATH", None)
    import_result = subprocess.run(
        [sys.executable, "-c", import_check, str(wheel_path), str(REPO_ROOT)],
        cwd=tmp_path,
        env=import_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    assert import_result.returncode == 0, import_result.stdout
