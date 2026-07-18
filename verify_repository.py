from __future__ import annotations

from pathlib import Path
import sys

EXPECTED = [
    "aiohttp==3.11.11",
    "python-dotenv==1.0.1",
    "python-telegram-bot==21.9",
    "tabulate==0.9.0",
    "requests==2.32.3",
    "pycryptodome==3.21.0",
    "psycopg[binary]==3.2.3",
    "playwright==1.49.1",
]

errors: list[str] = []
for name in ("main.py", "alert_engine.py", "live_price_provider.py", "coinglass_dom_reader.py"):
    path = Path(name)
    if not path.exists() or path.stat().st_size < 100:
        errors.append(f"missing or suspiciously small: {name}")

canonical = Path("requirements.canonical.txt").read_text(encoding="utf-8").splitlines()
if canonical != EXPECTED:
    errors.append("requirements.canonical.txt differs from the expected dependency list")

if errors:
    print("Repository verification FAILED:")
    for error in errors:
        print(f"- {error}")
    sys.exit(1)

print("Repository verification OK")
