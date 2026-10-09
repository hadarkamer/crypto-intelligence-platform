"""Privately stage the two exact original evidence bundles on a durable volume.

Only explicit local ZIP paths are accepted. This module neither starts a worker
nor restores/registers research state, fetches a URL, or opens a source database.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import tarfile
import uuid
import zipfile

VERSION = "original-eight-private-input-bootstrap-v1"
BUNDLES = {
    "baseline": ("crypto_prospective_discovery_evidence_2026-10-06.zip",
                 "0fa6e922a5241484611cd8b88b0853e10e69a0ec4984480cd39ba517f15c2ed8"),
    "integration": ("crypto_pipeline_integration_evidence_2026-10-06.zip",
                    "41d13c14ff190f4e6514f79af88af7d3e75c61ecd712aacbeedc858c97314049"),
}
DEPENDENCIES = "prospective_discovery/runtime_dependencies.tar.gz"
MAX_ARCHIVE_BYTES = 64 * 1024 * 1024
MAX_MEMBER_BYTES = 64 * 1024 * 1024
MAX_EXPANDED_BYTES = 256 * 1024 * 1024
MAX_MEMBERS = 10000
CHUNK = 1024 * 1024
MANIFEST = "bootstrap_manifest.json"


def _json(value):
    return (json.dumps(value,sort_keys=True,separators=(",",":"),allow_nan=False)+"\n").encode()


def _hash(handle):
    result=hashlib.sha256()
    while chunk:=handle.read(CHUNK):
        result.update(chunk)
    return result.hexdigest()


def _local_path(value):
    text=os.fspath(value)
    if "://" in text or "\x00" in text:
        raise ValueError("BOOTSTRAP_LOCAL_PATH_REQUIRED")
    path=Path(os.path.abspath(text))
    for part in (path,*path.parents):
        if part.is_symlink():
            raise ValueError("BOOTSTRAP_SYMLINK_PATH_REJECTED")
    return path


@contextmanager
def _pinned_archive(path,expected):
    path=_local_path(path)
    descriptor=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
    with os.fdopen(descriptor,"rb") as handle:
        info=os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode) or not 0<info.st_size<=MAX_ARCHIVE_BYTES:
            raise ValueError("BOOTSTRAP_ARCHIVE_SIZE_OR_TYPE_INVALID")
        if _hash(handle)!=expected:
            raise ValueError("BOOTSTRAP_ORIGINAL_ZIP_SHA256_MISMATCH")
        handle.seek(0)
        yield handle


def _member_path(name):
    if (not isinstance(name,str) or not name or "\\" in name or "\x00" in name
            or name.startswith("/") or ":" in name):
        raise ValueError("BOOTSTRAP_UNSAFE_ARCHIVE_PATH")
    normalized=name[:-1] if name.endswith("/") else name
    pieces=normalized.split("/")
    if any(piece in ("",".","..") for piece in pieces):
        raise ValueError("BOOTSTRAP_UNSAFE_ARCHIVE_PATH")
    return PurePosixPath(normalized)


def _bounded_copy(source,destination,size):
    if size<0 or size>MAX_MEMBER_BYTES:
        raise ValueError("BOOTSTRAP_MEMBER_SIZE_EXCEEDED")
    hasher=hashlib.sha256()
    count=0
    while chunk:=source.read(min(CHUNK,size-count+1)):
        count+=len(chunk)
        if count>size:
            raise ValueError("BOOTSTRAP_MEMBER_LENGTH_MISMATCH")
        hasher.update(chunk)
        if destination is not None:
            destination.write(chunk)
    if count!=size:
        raise ValueError("BOOTSTRAP_MEMBER_LENGTH_MISMATCH")
    return {"kind":"file","bytes":count,"sha256":hasher.hexdigest()}


def _mkdir(path):
    if path.is_symlink() or (path.exists() and not path.is_dir()):
        raise ValueError("BOOTSTRAP_DIRECTORY_COLLISION")
    if not path.exists():
        _mkdir(path.parent)
        path.mkdir(mode=0o700)


def _file_target(root,relative):
    path=root.joinpath(*relative.parts)
    _mkdir(path.parent)
    if path.exists() or path.is_symlink():
        raise ValueError("BOOTSTRAP_ARCHIVE_PATH_COLLISION")
    return path


def _zip_contents(handle,*,destination=None):
    """Validate every entry before extracting; inventory uses content hashes."""
    result={}
    with zipfile.ZipFile(handle) as archive:
        entries=archive.infolist()
        if len(entries)>MAX_MEMBERS or sum(e.file_size for e in entries)>MAX_EXPANDED_BYTES:
            raise ValueError("BOOTSTRAP_ZIP_EXPANSION_BUDGET_EXCEEDED")
        seen=set()
        for entry in entries:
            relative=_member_path(entry.filename)
            name=str(relative)
            kind=stat.S_IFMT(entry.external_attr>>16)
            if (name in seen or kind not in (0,stat.S_IFREG,stat.S_IFDIR)
                    or entry.flag_bits&1 or entry.file_size>MAX_MEMBER_BYTES):
                raise ValueError("BOOTSTRAP_ZIP_DUPLICATE_TYPE_ENCRYPTION_OR_SIZE")
            if entry.is_dir() != (kind==stat.S_IFDIR) and kind!=0:
                raise ValueError("BOOTSTRAP_ZIP_DIRECTORY_TYPE_MISMATCH")
            seen.add(name)
        for entry in entries:
            relative=_member_path(entry.filename)
            if entry.is_dir():
                if destination is not None:
                    _mkdir(destination.joinpath(*relative.parts))
                result[str(relative)]={"kind":"directory"}
                continue
            with archive.open(entry) as source:
                if destination is None:
                    record=_bounded_copy(source,None,entry.file_size)
                else:
                    target=_file_target(destination,relative)
                    descriptor=os.open(target,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
                    with os.fdopen(descriptor,"wb") as output:
                        record=_bounded_copy(source,output,entry.file_size)
                        output.flush(); os.fsync(output.fileno())
            result[str(relative)]=record
    return result


def _link_target(relative,target):
    if not target or target.startswith("/") or "\\" in target or ":" in target or "\x00" in target:
        raise ValueError("BOOTSTRAP_UNSAFE_DEPENDENCY_LINK")
    parts=list(relative.parent.parts)
    for component in target.split("/"):
        if component=="..":
            if not parts:
                raise ValueError("BOOTSTRAP_DEPENDENCY_LINK_ESCAPES_ROOT")
            parts.pop()
        elif component not in ("", "."):
            parts.append(component)
    if not parts or parts[0]!="node_modules":
        raise ValueError("BOOTSTRAP_DEPENDENCY_LINK_ESCAPES_ROOT")
    return PurePosixPath(*parts)


def _tar_contents(handle,*,destination=None):
    """Only regular files/directories plus internal file symlinks are admitted."""
    result={}; members=[]; seen=set(); expanded=0
    with tarfile.open(fileobj=handle,mode="r:gz") as archive:
        for member in archive:
            relative=_member_path(member.name)
            expanded+=member.size
            if (len(members)>=MAX_MEMBERS or expanded>MAX_EXPANDED_BYTES or member.size>MAX_MEMBER_BYTES
                    or str(relative) in seen or relative.parts[0]!="node_modules"
                    or not (member.isfile() or member.isdir() or member.issym())):
                raise ValueError("BOOTSTRAP_TAR_BUDGET_DUPLICATE_OR_TYPE")
            if member.issym():
                _link_target(relative,member.linkname)
            seen.add(str(relative)); members.append((member,relative))
        # Reject descendants of files or links before writing anything.
        nondirs={str(relative) for member,relative in members if not member.isdir()}
        for member,relative in members:
            if any(str(parent) in nondirs for parent in relative.parents):
                raise ValueError("BOOTSTRAP_TAR_ANCESTOR_COLLISION")
        for member,relative in members:
            if member.isdir():
                if destination is not None:
                    _mkdir(destination.joinpath(*relative.parts))
                result[str(relative)]={"kind":"directory"}
                continue
            if member.issym():
                result[str(relative)]={"kind":"symlink","target":member.linkname}
                continue
            with archive.extractfile(member) as source:
                if destination is None:
                    record=_bounded_copy(source,None,member.size)
                else:
                    target=_file_target(destination,relative)
                    descriptor=os.open(target,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
                    with os.fdopen(descriptor,"wb") as output:
                        record=_bounded_copy(source,output,member.size)
                        output.flush(); os.fsync(output.fileno())
            result[str(relative)]=record
        for member,relative in members:
            if member.issym():
                target=_link_target(relative,member.linkname)
                if result.get(str(target),{}).get("kind")!="file":
                    raise ValueError("BOOTSTRAP_DEPENDENCY_LINK_REQUIRES_REGULAR_TARGET")
                if destination is not None:
                    os.symlink(member.linkname,_file_target(destination,relative))
    return result


def _expected_contents(inputs):
    expected={}
    for role,(filename,digest) in BUNDLES.items():
        original=inputs/"evidence"/filename
        with _pinned_archive(original,digest) as handle:
            info=os.fstat(handle.fileno())
            expected["evidence/"+filename]={"kind":"file","bytes":info.st_size,"sha256":digest}
            for name,record in _zip_contents(handle).items():
                expected[role+"/"+name]=record
    baseline=inputs/"evidence"/BUNDLES["baseline"][0]
    with zipfile.ZipFile(baseline) as archive:
        raw=archive.read(DEPENDENCIES)
    for name,record in _tar_contents(io.BytesIO(raw)).items():
        expected["integration/oct6_future_execution/runtime/"+name]=record
    for name in list(expected):
        for parent in PurePosixPath(name).parents:
            if str(parent)!=".":
                expected.setdefault(str(parent),{"kind":"directory"})
    return expected


def _actual_contents(inputs):
    result={}; count=0; total=0
    for directory,dirs,files in os.walk(inputs,followlinks=False):
        for name in list(dirs)+files:
            path=Path(directory)/name
            relative=path.relative_to(inputs).as_posix()
            if relative==MANIFEST:
                continue
            info=path.lstat()
            count+=1; total+=info.st_size
            if count>MAX_MEMBERS or total>3*MAX_EXPANDED_BYTES:
                raise ValueError("BOOTSTRAP_INSTALLED_INPUT_BUDGET_EXCEEDED")
            if stat.S_ISDIR(info.st_mode):
                result[relative]={"kind":"directory"}
            elif stat.S_ISLNK(info.st_mode):
                result[relative]={"kind":"symlink","target":os.readlink(path)}
            elif stat.S_ISREG(info.st_mode):
                descriptor=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
                with os.fdopen(descriptor,"rb") as handle:
                    result[relative]={"kind":"file","bytes":info.st_size,"sha256":_hash(handle)}
            else:
                raise ValueError("BOOTSTRAP_INSTALLED_SPECIAL_FILE_REJECTED")
    return result


def _manifest(expected):
    return {"version":VERSION,"original_bundle_sha256":{role:value[1] for role,value in BUNDLES.items()},
        "content_inventory_sha256":hashlib.sha256(_json(expected)).hexdigest(),
        "inventory_entry_count":len(expected),
        "file_and_link_count":sum(record["kind"]!="directory" for record in expected.values()),
        "research_state_initialized":False,
        "source_connection_opened":False,"telegram_authorized":False,"trading_authorized":False}


def paths(persistent_root):
    root=_local_path(persistent_root)
    return {"persistent_root":str(root),"checkpoint":str(root/"inputs/baseline/prospective_discovery"),
        "adapter_dir":str(root/"inputs/integration/oct6_future_execution"),"work_dir":str(root/"work"),
        "manifest":str(root/"inputs"/MANIFEST)}


def verify_inputs(persistent_root):
    root=_local_path(persistent_root); inputs=_local_path(root/"inputs")
    manifest_path=_local_path(inputs/MANIFEST)
    if not inputs.is_dir() or not manifest_path.is_file() or manifest_path.stat().st_size>16384:
        raise ValueError("BOOTSTRAP_COMPLETE_INPUTS_REQUIRED")
    expected=_expected_contents(inputs)
    if _actual_contents(inputs)!=expected or json.loads(manifest_path.read_bytes())!=_manifest(expected):
        raise ValueError("BOOTSTRAP_INSTALLED_INPUTS_CHANGED")
    return {**paths(root),**_manifest(expected),"inputs_verified":True}


def stage_inputs(persistent_root,baseline_zip,integration_zip):
    root=_local_path(persistent_root)
    if root==Path(root.anchor):
        raise ValueError("BOOTSTRAP_EXPLICIT_PRIVATE_ROOT_REQUIRED")
    _mkdir(root)
    descriptor=os.open(root/".bootstrap.lock",os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    staging=None
    try:
        try:
            fcntl.flock(descriptor,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("BOOTSTRAP_ALREADY_ACTIVE") from None
        # Verify both explicit paths before touching an existing installation.
        originals={"baseline":baseline_zip,"integration":integration_zip}
        for role,path in originals.items():
            with _pinned_archive(path,BUNDLES[role][1]):
                pass
        if (root/"inputs").exists() or (root/"inputs").is_symlink():
            return verify_inputs(root)
        staging=root/(".inputs-stage-"+uuid.uuid4().hex)
        staging.mkdir(mode=0o700)
        (staging/"evidence").mkdir(mode=0o700)
        for role,source_path in originals.items():
            filename,expected_hash=BUNDLES[role]
            copied=staging/"evidence"/filename
            with _pinned_archive(source_path,expected_hash) as source:
                with copied.open("xb") as output:
                    os.chmod(copied,0o600)
                    shutil.copyfileobj(source,output,CHUNK)
                    output.flush(); os.fsync(output.fileno())
            (staging/role).mkdir(mode=0o700)
            with _pinned_archive(copied,expected_hash) as source:
                _zip_contents(source,destination=staging/role)
        dependency_tar=staging/"baseline"/DEPENDENCIES
        with dependency_tar.open("rb") as source:
            _tar_contents(source,destination=staging/"integration/oct6_future_execution/runtime")
        expected=_expected_contents(staging)
        if _actual_contents(staging)!=expected:
            raise ValueError("BOOTSTRAP_STAGED_CONTENT_MISMATCH")
        manifest=staging/MANIFEST
        with manifest.open("xb") as handle:
            os.chmod(manifest,0o600)
            handle.write(_json(_manifest(expected))); handle.flush(); os.fsync(handle.fileno())
        # Flush directory entries bottom-up before the one visible rename.
        for directory,_,_ in os.walk(staging,topdown=False,followlinks=False):
            fd=os.open(directory,os.O_RDONLY|os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        os.rename(staging,root/"inputs")
        staging=None
        fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        return {**paths(root),**_manifest(expected),"inputs_verified":True}
    finally:
        if staging is not None:
            shutil.rmtree(staging)
        os.close(descriptor)


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command",choices=("stage","verify"))
    parser.add_argument("--persistent-root",required=True,type=Path)
    parser.add_argument("--baseline-zip",type=Path)
    parser.add_argument("--integration-zip",type=Path)
    args=parser.parse_args(argv)
    if args.command=="stage":
        if args.baseline_zip is None or args.integration_zip is None:
            parser.error("stage requires both explicit original local ZIP paths")
        result=stage_inputs(args.persistent_root,args.baseline_zip,args.integration_zip)
    else:
        if args.baseline_zip is not None or args.integration_zip is not None:
            parser.error("verify uses only the privately retained original inputs")
        result=verify_inputs(args.persistent_root)
    print(json.dumps(result,sort_keys=True))


if __name__=="__main__":
    main()
