"""Safe extraction, immutable provenance, atomic install, and startup verification."""
from contextlib import contextmanager
import fcntl
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import zipfile

import research_no_horizon_worker_bootstrap as bootstrap


def dependency_tar(*,symlink="../pkg/server.js",extra=None):
    output=io.BytesIO()
    with tarfile.open(fileobj=output,mode="w:gz") as archive:
        raw=b"export const frozen = true;\n"
        item=tarfile.TarInfo("node_modules/pkg/server.js"); item.size=len(raw)
        archive.addfile(item,io.BytesIO(raw))
        link=tarfile.TarInfo("node_modules/.bin/server"); link.type=tarfile.SYMTYPE; link.linkname=symlink
        archive.addfile(link)
        if extra is not None:
            archive.addfile(extra,io.BytesIO(b"x"*extra.size) if extra.isfile() else None)
    return output.getvalue()


@contextmanager
def fixtures(root,*,deps=None,baseline_extra=None):
    source=root/"source"; source.mkdir()
    baseline=source/"baseline.zip"; integration=source/"integration.zip"
    with zipfile.ZipFile(baseline,"w") as archive:
        archive.writestr(bootstrap.DEPENDENCIES,deps if deps is not None else dependency_tar())
        archive.writestr("prospective_discovery/actual_registration_summary.json",b'{"original":8}\n')
        archive.writestr("prospective_discovery/registry_snapshot.tar.gz",b"unchanged registry fixture")
        if baseline_extra is not None:
            archive.writestr(*baseline_extra)
    with zipfile.ZipFile(integration,"w") as archive:
        archive.writestr("oct6_future_execution/queue_driver.py",b"# unchanged frozen adapter\n")
        archive.writestr("oct6_future_execution/runtime/package.json",b'{}\n')
    mapping={"baseline":("original-baseline.zip",hashlib.sha256(baseline.read_bytes()).hexdigest()),
             "integration":("original-integration.zip",hashlib.sha256(integration.read_bytes()).hexdigest())}
    with patch.object(bootstrap,"BUNDLES",mapping):
        yield root/"private",baseline,integration


class BootstrapTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)

    def test_private_atomic_stage_exact_repeat_and_independent_verification(self):
        with fixtures(self.root) as (private,baseline,integration):
            report=bootstrap.stage_inputs(private,baseline,integration)
            self.assertTrue(report["inputs_verified"])
            self.assertFalse(report["research_state_initialized"])
            self.assertFalse((private/"work").exists())
            summary=Path(report["checkpoint"])/"actual_registration_summary.json"
            self.assertEqual(summary.read_bytes(),b'{"original":8}\n')
            self.assertEqual(stat.S_IMODE(summary.stat().st_mode),0o600)
            self.assertEqual(stat.S_IMODE((private/"inputs").stat().st_mode),0o700)
            linked=Path(report["adapter_dir"])/"runtime/node_modules/.bin/server"
            self.assertEqual(linked.read_bytes(),b"export const frozen = true;\n")
            inode=(private/"inputs").stat().st_ino
            self.assertEqual(report,bootstrap.verify_inputs(private))
            self.assertEqual(report,bootstrap.stage_inputs(private,baseline,integration))
            self.assertEqual(inode,(private/"inputs").stat().st_ino)

    def test_modified_incoming_archive_fails_before_install(self):
        with fixtures(self.root) as (private,baseline,integration):
            with baseline.open("ab") as handle:handle.write(b"tamper")
            with self.assertRaisesRegex(ValueError,"SHA256_MISMATCH"):
                bootstrap.stage_inputs(private,baseline,integration)
            self.assertFalse((private/"inputs").exists())

    def test_file_and_manifest_joint_edit_does_not_bypass_pinned_original(self):
        with fixtures(self.root) as (private,baseline,integration):
            report=bootstrap.stage_inputs(private,baseline,integration)
            (Path(report["adapter_dir"])/"queue_driver.py").write_text("# changed")
            manifest=Path(report["manifest"])
            value=json.loads(manifest.read_bytes())
            value["content_inventory_sha256"]=hashlib.sha256(bootstrap._json(bootstrap._actual_contents(private/"inputs"))).hexdigest()
            manifest.write_bytes(bootstrap._json(value))
            with self.assertRaisesRegex(ValueError,"INPUTS_CHANGED"):
                bootstrap.verify_inputs(private)

    def test_extra_empty_directory_and_changed_retained_archive_are_rejected(self):
        with fixtures(self.root) as (private,baseline,integration):
            bootstrap.stage_inputs(private,baseline,integration)
            (private/"inputs/extra").mkdir()
            with self.assertRaisesRegex(ValueError,"INPUTS_CHANGED"):
                bootstrap.verify_inputs(private)
            (private/"inputs/extra").rmdir()
            retained=private/"inputs/evidence/original-baseline.zip"
            retained.write_bytes(retained.read_bytes()+b"changed")
            with self.assertRaisesRegex(ValueError,"SHA256_MISMATCH"):
                bootstrap.verify_inputs(private)

    def test_zip_traversal_is_rejected_without_visible_partial_install(self):
        with fixtures(self.root,baseline_extra=("../escaped",b"bad")) as (private,baseline,integration):
            with self.assertRaisesRegex(ValueError,"UNSAFE_ARCHIVE_PATH"):
                bootstrap.stage_inputs(private,baseline,integration)
            self.assertFalse((private/"inputs").exists())
            self.assertFalse((private/"escaped").exists())
            self.assertEqual(list(private.glob(".inputs-stage-*")),[])

    def test_dependency_links_cannot_escape_or_target_other_links(self):
        for index,target in enumerate(("../../../outside","/tmp/outside","../.bin/server")):
            directory=self.root/str(index); directory.mkdir()
            with fixtures(directory,deps=dependency_tar(symlink=target)) as (private,baseline,integration):
                with self.assertRaises(ValueError):
                    bootstrap.stage_inputs(private,baseline,integration)
                self.assertFalse((private/"inputs").exists())

    def test_hardlinks_and_device_members_rejected(self):
        for index,kind in enumerate((tarfile.LNKTYPE,tarfile.CHRTYPE)):
            directory=self.root/str(index); directory.mkdir()
            item=tarfile.TarInfo("node_modules/danger"); item.type=kind; item.linkname="node_modules/pkg/server.js"
            with fixtures(directory,deps=dependency_tar(extra=item)) as (private,baseline,integration):
                with self.assertRaisesRegex(ValueError,"TAR_BUDGET_DUPLICATE_OR_TYPE"):
                    bootstrap.stage_inputs(private,baseline,integration)

    def test_zip_member_and_total_expansion_limits(self):
        with fixtures(self.root) as (private,baseline,integration):
            with patch.object(bootstrap,"MAX_MEMBER_BYTES",1):
                with self.assertRaisesRegex(ValueError,"SIZE"):
                    bootstrap.stage_inputs(private,baseline,integration)
            with patch.object(bootstrap,"MAX_EXPANDED_BYTES",1):
                with self.assertRaisesRegex(ValueError,"EXPANSION_BUDGET"):
                    bootstrap.stage_inputs(private,baseline,integration)
            self.assertFalse((private/"inputs").exists())

    def test_symlink_root_and_input_and_url_paths_rejected(self):
        with fixtures(self.root) as (private,baseline,integration):
            for label,target in (("input-link",baseline),("root-link",self.root)):
                link=self.root/label; link.symlink_to(target)
                with self.assertRaisesRegex(ValueError,"SYMLINK"):
                    if label=="input-link":bootstrap.stage_inputs(private,link,integration)
                    else:bootstrap.stage_inputs(link/"private",baseline,integration)
            with self.assertRaisesRegex(ValueError,"LOCAL_PATH_REQUIRED"):
                bootstrap.stage_inputs(private,"https://example.invalid/bundle.zip",integration)

    def test_work_state_is_never_modified_or_recreated(self):
        with fixtures(self.root) as (private,baseline,integration):
            work=private/"work"; work.mkdir(parents=True)
            marker=work/"native_transport_identity.json"; marker.write_bytes(b"existing identity")
            bootstrap.stage_inputs(private,baseline,integration)
            self.assertEqual(marker.read_bytes(),b"existing identity")
            self.assertEqual(list(work.iterdir()),[marker])

    def test_concurrent_bootstrap_fails_before_extraction(self):
        with fixtures(self.root) as (private,baseline,integration):
            private.mkdir()
            with (private/".bootstrap.lock").open("wb") as lock:
                fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
                with self.assertRaisesRegex(RuntimeError,"ALREADY_ACTIVE"):
                    bootstrap.stage_inputs(private,baseline,integration)
            self.assertFalse((private/"inputs").exists())

    def test_injected_extraction_failure_cleans_only_its_own_staging_tree(self):
        with fixtures(self.root) as (private,baseline,integration):
            private.mkdir(); keep=private/"keep.txt"; keep.write_text("keep")
            with patch.object(bootstrap,"_tar_contents",side_effect=RuntimeError("interrupted fixture")):
                with self.assertRaises(RuntimeError):
                    bootstrap.stage_inputs(private,baseline,integration)
            self.assertFalse((private/"inputs").exists())
            self.assertEqual(list(private.glob(".inputs-stage-*")),[])
            self.assertEqual(keep.read_text(),"keep")


if __name__=="__main__":
    unittest.main()
