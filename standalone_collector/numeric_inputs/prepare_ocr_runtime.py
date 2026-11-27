"""Install the collector's pinned, relocatable OCR engine during its ordinary build."""
from argparse import ArgumentParser
from hashlib import sha256
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
import json
import platform
import shutil
import subprocess
import tarfile

HERE = Path(__file__).resolve().parent
ARCHIVE = HERE / "tesseract-5.5.0-linux-amd64.tar.xz"
SHA256 = "519c5c84f19de3f6b49d45bb50389b9aee1aeb1b5282fd3bf5de71303ec95352"
MAX_EXTRACTED = 48 * 1024 * 1024


def check_ready(path):
    executable = path / "tesseract"
    eng = path / "tessdata/eng.traineddata"
    if sha256(eng.read_bytes()).hexdigest() != "7d4322bd2a7749724879683fc3912cb542f19906c83bcc1a52132556427170b2":
        raise RuntimeError("Packaged English LSTM model changed")
    env = {"PATH": "/usr/bin:/bin", "LC_ALL": "C"}
    def read(*args):
        return subprocess.check_output([str(executable), *args], text=True, env=env,
                                       timeout=6, stderr=subprocess.STDOUT).splitlines()
    versions = read("--version")
    if not versions or versions[0] != "tesseract 5.5.0":
        raise RuntimeError("Packaged OCR version is unavailable")
    if read("--list-langs")[1:] != ["eng"]:
        raise RuntimeError("Packaged English OCR data is unavailable")
    return executable


def install(destination):
    destination = Path(destination).absolute()
    if (platform.system(), platform.machine()) != ("Linux", "x86_64"):
        raise RuntimeError("Packaged CoinGlass OCR requires Linux amd64")
    if sha256(ARCHIVE.read_bytes()).hexdigest() != SHA256:
        raise RuntimeError("CoinGlass OCR distribution digest mismatch")
    marker = destination / ".archive-sha256"
    if destination.exists():
        if destination.is_symlink() or not marker.is_file() or marker.read_text().strip() != SHA256:
            raise RuntimeError("Existing OCR directory does not match the release")
        return check_ready(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=".ocr-build-", dir=destination.parent) as tmp:
        root = Path(tmp) / "runtime"
        root.mkdir()
        total = 0
        with tarfile.open(ARCHIVE, "r|xz") as archive:
            for entry in archive:
                relative = PurePosixPath(entry.name)
                if (not entry.isreg() or relative.is_absolute() or ".." in relative.parts
                        or str(relative) != entry.name or entry.mode not in (0o644, 0o755)):
                    raise RuntimeError("Invalid OCR release member")
                total += entry.size
                if total > MAX_EXTRACTED:
                    raise RuntimeError("Unexpected OCR release size")
                target = root / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                with archive.extractfile(entry) as src, target.open("xb") as dst:
                    shutil.copyfileobj(src, dst)
                target.chmod(entry.mode)
        check_ready(root)
        (root / marker.name).write_text(SHA256 + "\n")
        root.rename(destination)
    return destination / "tesseract"


if __name__ == "__main__":
    cli = ArgumentParser(description=__doc__)
    cli.add_argument("--destination", type=Path, default=HERE / "ocr_runtime")
    path = install(cli.parse_args().destination)
    print(json.dumps({"version": "5.5.0", "language": "eng", "release_sha256": SHA256,
                      "binary": str(path)}))
