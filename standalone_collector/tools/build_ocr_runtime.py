"""Build the collector's relocatable Debian 13 amd64 Tesseract distribution.

Run on the reviewed Debian amd64 build host with tesseract-ocr 5.5.0 and
tesseract-ocr-eng installed. The release is a normal tar.xz of files, without
symlinks or system installation hooks. Reports within it identify their origin.
"""
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
import platform
import re
import subprocess
import sys
import tarfile


# These Debian binary packages provide the SONAMEs used by this release.
PACKAGES = """
libtesseract.so.5:libtesseract5 libleptonica.so.6:libleptonica6
libarchive.so.13:libarchive13t64 libcurl.so.4:libcurl4t64 libstdc++.so.6:libstdc++6
libm.so.6:libc6 libgcc_s.so.1:libgcc-s1 libc.so.6:libc6 libgomp.so.1:libgomp1
libpng16.so.16:libpng16-16t64 libjpeg.so.62:libjpeg62-turbo libgif.so.7:libgif7
libtiff.so.6:libtiff6 libwebp.so.7:libwebp7 libwebpmux.so.3:libwebpmux3
libopenjp2.so.7:libopenjp2-7 libz.so.1:zlib1g libnettle.so.8:libnettle8t64
libacl.so.1:libacl1 liblzma.so.5:liblzma5 libzstd.so.1:libzstd1 liblz4.so.1:liblz4-1
libbz2.so.1.0:libbz2-1.0 libxml2.so.2:libxml2 libnghttp3.so.9:libnghttp3-9
libnghttp2.so.14:libnghttp2-14 libidn2.so.0:libidn2-0 librtmp.so.1:librtmp1
libssh2.so.1:libssh2-1t64 libpsl.so.5:libpsl5t64 libssl.so.3:libssl3t64
libcrypto.so.3:libssl3t64 libgssapi_krb5.so.2:libgssapi-krb5-2 libldap.so.2:libldap2
liblber.so.2:libldap2 libbrotlidec.so.1:libbrotli1 libLerc.so.4:liblerc4
libjbig.so.0:libjbig0 libdeflate.so.0:libdeflate0 libsharpyuv.so.0:libsharpyuv0
libxxhash.so.0:libxxhash0 libunistring.so.5:libunistring5 libgnutls.so.30:libgnutls30t64
libhogweed.so.6:libhogweed6t64 libgmp.so.10:libgmp10 libkrb5.so.3:libkrb5-3
libk5crypto.so.3:libk5crypto3 libcom_err.so.2:libcom-err2 libkrb5support.so.0:libkrb5support0
libsasl2.so.2:libsasl2-2 libbrotlicommon.so.1:libbrotli1 libp11-kit.so.0:libp11-kit0
libtasn1.so.6:libtasn1-6 libkeyutils.so.1:libkeyutils1 libresolv.so.2:libc6 libffi.so.8:libffi8
"""

LAUNCHER = b'''#!/bin/sh
set -eu
root=$(CDPATH= cd -- "${0%/*}" && pwd -P)
unset LD_PRELOAD LD_AUDIT LD_LIBRARY_PATH
export TESSDATA_PREFIX="$root/tessdata" OMP_THREAD_LIMIT=1 LC_ALL=C
exec "$root/lib/ld-linux-x86-64.so.2" --inhibit-cache --library-path "$root/lib" "$root/bin/tesseract" "$@"
'''


def build(dest):
    os_info = platform.freedesktop_os_release()
    if (platform.system(), platform.machine(), os_info.get("ID"), os_info.get("VERSION_ID")) != (
            "Linux", "x86_64", "debian", "13"):
        raise SystemExit("Use the Debian 13 amd64 release build host")
    versions = subprocess.check_output(["/usr/bin/tesseract", "--version"], text=True).splitlines()
    if versions[0] != "tesseract 5.5.0":
        raise SystemExit("Wrong Tesseract version")
    clean = {"PATH": "/usr/bin:/bin", "LC_ALL": "C"}
    linkage = subprocess.check_output(["/usr/bin/ldd", "/usr/bin/tesseract"], text=True, env=clean)
    resolved = dict(re.findall(r"^\s*(\S+)\s+=>\s+(/\S+)", linkage, re.M))
    owners = dict(entry.split(":") for entry in PACKAGES.split())
    if set(resolved) != set(owners):
        raise SystemExit("Unreviewed Tesseract dependency closure")
    files = {"tesseract": (LAUNCHER, 0o755)}
    origins = {}

    def add(name, filename, owner):
        path = Path(filename)
        data = path.read_bytes()
        mode = 0o755 if name.startswith(("bin/", "lib/ld-linux")) else 0o644
        files[name] = (data, mode)
        origins[name] = {"debian_package": owner, "source_path": str(path.resolve()),
                         "sha256": sha256(data).hexdigest(), "bytes": len(data)}

    add("bin/tesseract", "/usr/bin/tesseract", "tesseract-ocr")
    add("lib/ld-linux-x86-64.so.2", "/lib64/ld-linux-x86-64.so.2", "libc6")
    for name, origin in resolved.items():
        add(f"lib/{name}", origin, owners[name])
    base = Path("/usr/share/tesseract-ocr/5/tessdata")
    add("tessdata/eng.traineddata", base / "eng.traineddata", "tesseract-ocr-eng")
    add("tessdata/configs/tsv", base / "configs/tsv", "tesseract-ocr")
    for owner in sorted(set(owners.values()) | {"tesseract-ocr", "tesseract-ocr-eng"}):
        add(f"licenses/debian/{owner}", f"/usr/share/doc/{owner}/copyright", owner)
    for p in sorted(Path("/usr/share/common-licenses").iterdir()):
        if p.is_file():
            add(f"licenses/common/{p.name}", p, "base-files")
    meta = {"tesseract": versions[0], "leptonica": versions[1].strip(), "architecture": "linux-amd64",
            "build_os": os_info.get("PRETTY_NAME"), "files": origins}
    files["contents.json"] = (json.dumps(meta, indent=2, sort_keys=True).encode()+b"\n", 0o644)
    with tarfile.open(dest, "w:xz", format=tarfile.USTAR_FORMAT, preset=6) as tar:
        for name, (data, mode) in sorted(files.items()):
            info = tarfile.TarInfo(name)
            info.size, info.mode, info.mtime = len(data), mode, 0
            tar.addfile(info, BytesIO(data))
    print(json.dumps({"archive": str(dest), "sha256": sha256(dest.read_bytes()).hexdigest(),
                      "bytes": dest.stat().st_size, "installed_bytes": sum(len(d) for d,_ in files.values()),
                      "shared_objects": len(resolved)+1, "files": len(files)}))


if __name__ == "__main__":
    build(Path(sys.argv[1]))
