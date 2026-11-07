"""Offline review checks only; never builds an image or starts a service."""
import argparse
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import shutil
import tempfile
from unittest import mock

import jsonschema
import yaml


def load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def validate(directory, schema_path):
    checks = []
    blueprint = yaml.safe_load((directory / "render.yaml").read_text())
    schema = json.loads(schema_path.read_text())
    jsonschema.Draft202012Validator(schema).validate(blueprint)
    checks.append("current_render_json_schema")
    service, = blueprint["services"]
    assert service["type"] == "worker" and service["runtime"] == "docker"
    assert service["name"] == "coinglass-session-recovery-lab"
    assert service["plan"] == "1c-2g" and service["numInstances"] == 1
    assert service["autoDeployTrigger"] == "off"
    assert service["branch"] == "lab/coinglass-session-recovery-20261107"
    assert service["repo"] == "https://github.com/hadarkamer/crypto-intelligence-platform"
    prefix = "./experiments/coinglass-session-recovery-lab"
    assert service["dockerContext"] == prefix
    assert service["dockerfilePath"] == prefix + "/packaging/Dockerfile"
    assert not {"envVars", "envVarGroups", "disk", "scaling", "healthCheckPath",
                "schedule", "preDeployCommand", "initialDeployHook"} & service.keys()
    assert not {"databases", "envVarGroups", "projects"} & blueprint.keys()
    checks.append("one_inert_worker_no_database_disk_envgroups_or_autodeploy")
    docker = (directory / "Dockerfile").read_text()
    instructions = [line.strip() for line in docker.splitlines()
                    if line.strip() and not line.lstrip().startswith("#")]
    assert instructions[-1] == 'CMD ["python", "/lab/packaging/worker_idle.py"]'
    assert 'ENTRYPOINT ["/usr/bin/tini", "--"]' in instructions
    assert 'USER lab:lab' in instructions
    assert not any(line.startswith(("EXPOSE ", "VOLUME ", "HEALTHCHECK ")) for line in instructions)
    assert "chmod 0700 /home/lab/.ssh" in docker
    assert "--uid 10001" in docker
    assert "playwright install --with-deps chromium" in docker
    assert "CMD [\"python\", \"standalone_collector/app.py\"]" not in docker
    assert "prepare.py" not in docker
    checks.append("nonroot_inert_entrypoint_no_product_execution_or_ports")
    assert "COPY --from=postgres_native /usr/share/postgresql/ /usr/share/postgresql/" in docker
    assert "test -r /usr/share/postgresql/16/postgresql.conf.sample" in docker
    assert "test -r /usr/share/postgresql/16/postgres.bki" in docker
    with tempfile.TemporaryDirectory(prefix="sample-copy-test-", dir=directory) as temp:
        root = Path(temp)
        source = root / "source"
        (source / "16").mkdir(parents=True)
        (source / "postgresql.conf.sample").write_text("# public packaging fixture\n")
        (source / "16" / "postgresql.conf.sample").symlink_to("../postgresql.conf.sample")
        shutil.copytree(source / "16", root / "broken" / "16", symlinks=True)
        assert not (root / "broken" / "16" / "postgresql.conf.sample").exists()
        shutil.copytree(source, root / "fixed", symlinks=True)
        assert (root / "fixed" / "16" / "postgresql.conf.sample").read_text() == "# public packaging fixture\n"
    checks.append("postgres_parent_sample_symlink_regression_without_database_execution")
    bases = json.loads((directory / "base-images.json").read_text())
    for base in bases:
        assert len(base["amd64"]) == 1
        assert base["image"] + "@" + base["amd64"][0] in docker
        assert base["index_digest"] == "sha256:" + base["computed_sha256"]
    checks.append("three_official_base_images_digest_pinned")

    idle = load_module(directory / "worker_idle.py", "fixture_idle_review")
    for uid, environment in [(0, {}), (10001, {"COINGLASS_COOKIE_HEADER": "DUMMY_DENIED_INPUT"}),
                             (10001, {"OPENAI_API_KEY": "DUMMY_DENIED_INPUT"})]:
        output = io.StringIO()
        with mock.patch.object(idle.os, "getuid", return_value=uid), \
             mock.patch.dict(idle.os.environ, environment, clear=True), \
             contextlib.redirect_stdout(output):
            assert idle.main() == 2
        assert "DUMMY_DENIED_INPUT" not in output.getvalue()
        assert json.loads(output.getvalue())["status"] == "unsafe_configuration"
    checks.append("idle_rejects_root_and_credentials_without_disclosure")
    event = mock.Mock()
    event.wait.return_value = None
    output = io.StringIO()
    with mock.patch.object(idle.os, "getuid", return_value=10001), \
         mock.patch.dict(idle.os.environ, {}, clear=True), \
         mock.patch.object(idle.threading, "Event", return_value=event), \
         mock.patch.object(idle.signal, "signal"), contextlib.redirect_stdout(output):
        assert idle.main() == 0
    lines = [json.loads(line) for line in output.getvalue().splitlines()]
    assert [line["status"] for line in lines] == ["ready", "stopped"]
    assert lines[0]["tests_started"] == 0
    event.wait.assert_called_once()
    checks.append("idle_start_stop_is_wait_only")

    assembly = load_module(directory / "assemble_context.py", "fixture_assembly_review")
    for unsafe in ("../secret", "/etc/passwd", "app/.env", "app/node_modules/x"):
        try:
            assembly.safe_relative(unsafe)
        except ValueError:
            pass
        else:
            raise AssertionError("unsafe assembly path accepted")
    checks.append("assembly_path_escape_and_credential_cache_exclusions")
    samples = {
        "dummy-openai.txt": ("sk-proj-" + "X" * 40).encode(),
        "dummy-github.txt": ("ghp_" + "X" * 40).encode(),
        "dummy-aws.txt": ("AKIA" + "X" * 16).encode(),
        "dummy-key.txt": ("-----BEGIN " + "PRIVATE KEY-----").encode(),
    }
    findings = assembly.secret_pattern_findings(samples)
    assert len(findings) == 4
    assert all(set(item) == {"path", "rule", "line"} for item in findings)
    assert assembly.secret_pattern_findings({"safe.txt": b"fixture=public-dummy"}) == []
    checks.append("secret_pattern_scan_uses_synthetic_values_and_emits_metadata_only")
    return {"status": "passed", "scope": "offline packaging checks only", "checks": checks,
            "docker_image_built": False, "postgres_started": False, "browser_started": False,
            "render_deployed": False, "live_source_or_ai_requests": 0}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument("--schema", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = validate(args.directory, args.schema)
    encoded = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.write_text(encoded)
    print(encoded, end="")


if __name__ == "__main__":
    main()
