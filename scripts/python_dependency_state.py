from __future__ import annotations

import argparse
import hashlib
import importlib.metadata as metadata
import sys
import tomllib
from pathlib import Path

from pip._vendor.packaging.markers import default_environment
from pip._vendor.packaging.requirements import Requirement


REPO_ROOT = Path(__file__).resolve().parent.parent


def _iter_manifest_files() -> list[Path]:
    return [
        path
        for path in (
            REPO_ROOT / "pyproject.toml",
            REPO_ROOT / "setup.py",
            REPO_ROOT / "requirements.txt",
        )
        if path.exists()
    ]


def _load_declared_requirements() -> list[str]:
    requirements: list[str] = []

    pyproject_path = REPO_ROOT / "pyproject.toml"
    if pyproject_path.exists():
        pyproject = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
        project = pyproject.get("project", {}) or {}
        requirements.extend(project.get("dependencies", []) or [])
        optional = project.get("optional-dependencies", {}) or {}
        requirements.extend(optional.get("desktop", []) or [])

    requirements_path = REPO_ROOT / "requirements.txt"
    if requirements_path.exists():
        for raw_line in requirements_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.split("#", 1)[0].strip()
            if not line or line.startswith("-"):
                continue
            requirements.append(line)

    return requirements


def compute_fingerprint() -> str:
    entries = []
    for path in _iter_manifest_files():
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        entries.append(f"{path.resolve()}:{digest}")
    payload = "|".join(entries).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def validate_installed_packages() -> list[str]:
    env = default_environment()
    seen: set[str] = set()
    problems: list[str] = []

    for raw in _load_declared_requirements():
        key = raw.strip()
        if not key or key in seen:
            continue
        seen.add(key)

        try:
            req = Requirement(key)
        except Exception as exc:
            problems.append(f"UNPARSEABLE {key}: {exc}")
            continue

        if req.marker and not req.marker.evaluate(env):
            continue

        try:
            installed_version = metadata.version(req.name)
        except metadata.PackageNotFoundError:
            problems.append(f"MISSING {req.name} required by {key}")
            continue

        if req.specifier and not req.specifier.contains(installed_version, prereleases=True):
            problems.append(
                f"VERSION_MISMATCH {req.name} installed={installed_version} required={req.specifier}"
            )

    return problems


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fingerprint", action="store_true")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    if args.fingerprint:
        print(compute_fingerprint())
        return 0

    if args.check:
        problems = validate_installed_packages()
        if problems:
            for problem in problems:
                print(problem)
            return 1
        print("OK")
        return 0

    parser.error("Specify --fingerprint or --check")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())