"""Assemble only manifest-verified source and explicit temporary-lab files.

No downloads, imports of product code, build, browser, service, or Git writes.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil

SOURCE_MANIFESTS = (
    ("source-manifest.json", "", "sha", "git"),
    ("app-test-source-manifest.json", "", "sha", "git"),
    ("sql-source-manifest.json", "app/", "blob", "git"),
    ("collector-tests-source-manifest.json", "collector/", "blob", "git"),
    ("collector-runtime-manifest.json", "", "sha256", "sha256"),
)
PACKAGING_REQUIRED = (
    "Dockerfile", "render.yaml", ".dockerignore", "worker_idle.py",
    "requirements.txt", "base-images.json", "ASSEMBLY.md",
    "assemble_context.py", "validate_packaging.py",
    "node-dependencies/package.json", "node-dependencies/package-lock.json",
    "native_serial_lab.py", "test_native_serial_lab.py", "NATIVE_SERIAL_README.md",
    "native_serial_manifest.json", "session_handoff.py", "test_session_handoff.py",
    "HANDOFF_CONTRACT.md", "collector_browser_fixture.py", "collector_matrix.py",
    "test_setup_failure_contract.py", "test_live_guard_contract.py", "test_live_supervisor_contract.py",
    "COLLECTOR_LAB_RUN.md", "session_handoff_local_test_result.json",
    "serial_fixture_guard/sitecustomize.py", "serial_node_guard.cjs",
)
REMOTE_PREFIX = "experiments/coinglass-session-recovery-lab"
SECRET_PATTERNS = {
    "private_key_pem": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----"),
    "openai_key_shape": re.compile(r"\bsk-(?:proj-|svcacct-)?[A-Za-z0-9_-]{30,}"),
    "github_token_shape": re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{40,})"),
    "aws_access_key_shape": re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
}


def secret_pattern_findings(files):
    # A bounded high-confidence pattern check, not a claim that every possible
    # secret format is detected. Never return matched values.
    findings = []
    for path, raw in files.items():
        text = raw.decode("utf-8")
        for rule, pattern in SECRET_PATTERNS.items():
            match = pattern.search(text)
            if match:
                findings.append({"path": path, "rule": rule,
                                 "line": text.count("\n", 0, match.start()) + 1})
    return findings


def safe_relative(value):
    p = Path(value)
    if p.is_absolute() or ".." in p.parts or not p.parts:
        raise ValueError("Unsafe manifest path")
    if any(part in {".git", "node_modules", "__pycache__", ".env", "evidence", "diagnostics"}
           for part in p.parts):
        raise ValueError("Excluded manifest path")
    return p


def digest(data, kind):
    if kind == "git":
        return hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()
    return hashlib.sha256(data).hexdigest()


def collect(lab, packaging):
    result = {}
    roles = {}
    for name, prefix, expected_key, kind in SOURCE_MANIFESTS:
        source = lab / name
        manifest = json.loads(source.read_text())
        result["manifests/" + name] = source.read_bytes()
        roles["manifests/" + name] = "source_provenance"
        for item in manifest["files"]:
            relative = safe_relative(prefix + item["path"])
            path = lab / relative
            if path.is_symlink() or not path.is_file():
                raise ValueError("Source must be a regular file: " + str(relative))
            data = path.read_bytes()
            if item.get("verified") is not True or digest(data, kind) != item[expected_key]:
                raise ValueError("Source hash mismatch: " + str(relative))
            key = relative.as_posix()
            if key in result and result[key] != data:
                raise ValueError("Conflicting source manifests: " + key)
            result[key] = data
            roles[key] = "unchanged_source_or_reconstructed_runtime"
    for name in PACKAGING_REQUIRED:
        relative = safe_relative(name)
        path = packaging / relative
        if path.is_symlink() or not path.is_file():
            raise ValueError("Required lab file missing: " + name)
        data = path.read_bytes()
        key = "packaging/" + relative.as_posix()
        result[key] = data
        roles[key] = "temporary_fixture_packaging"
    # Docker evaluates .dockerignore at context root; Render reads this reviewed
    # Blueprint at the nested path selected by the operator.
    for name in (".dockerignore", "render.yaml"):
        result[name] = result["packaging/" + name]
        roles[name] = "temporary_fixture_packaging"
    for key, value in result.items():
        value.decode("utf-8")  # Fail before materialization on binary payloads.
    return result, roles


def assemble(lab, packaging, destination):
    data, roles = collect(lab, packaging)
    findings = secret_pattern_findings(data)
    if findings:
        raise ValueError("Secret-shaped text requires review; values suppressed: " + json.dumps(findings))
    if destination.exists():
        raise ValueError("Destination must not exist; preserve the reviewable previous bundle")
    destination.mkdir(parents=True)
    records = []
    try:
        for relative, content in sorted(data.items()):
            path = destination / safe_relative(relative)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
            records.append({
                "relative_path": relative,
                "remote_path": REMOTE_PREFIX + "/" + relative,
                "local_path": str(path.resolve()),
                "bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
                "utf8": True,
                "role": roles[relative],
            })
        manifest = {
            "status": "assembled_not_built_or_deployed",
            "branch": "lab/coinglass-session-recovery-20261107",
            "base_commit": "33bad18f8be322009108584a6b71f3d65fa9b351",
            "remote_prefix": REMOTE_PREFIX,
            "files": records,
            "file_count": len(records),
            "total_bytes": sum(x["bytes"] for x in records),
            "largest_file_bytes": max(x["bytes"] for x in records),
            "source_bytes_verified": True,
            "credential_inputs_collected": False,
            "credential_files_packaged": False,
            "secret_pattern_scan": {
                "rules": sorted(SECRET_PATTERNS), "findings": 0,
                "scope": "allowlisted UTF-8 files only; heuristic, not exhaustive secret attestation",
            },
            "network_or_product_execution": False,
        }
        receipt = destination.parent / (destination.name + "-publish-manifest.json")
        receipt.write_text(json.dumps(manifest, indent=2) + "\n")
        return {k: v for k, v in manifest.items() if k != "files"} | {"publish_manifest": str(receipt.resolve())}
    except BaseException:
        # Only a directory created in this invocation is removed on incomplete
        # assembly. Existing bundles and source trees are never removed.
        shutil.rmtree(destination)
        raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--lab", type=Path, required=True)
    parser.add_argument("--packaging", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(assemble(args.lab.resolve(), args.packaging.resolve(), args.out.resolve()), indent=2))


if __name__ == "__main__":
    main()
