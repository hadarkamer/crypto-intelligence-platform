"""Keep the installed migration sequence explicit, ordered, and collision-free."""
from __future__ import annotations

import re

import research_formula_schema_admin as schema_admin


_MIGRATION_NAME = re.compile(r"^(\d{3})_[a-z0-9_]+\.sql$")


def run() -> None:
    paths = schema_admin.MIGRATION_PATHS
    assert paths, "the schema installer must expose at least one migration"
    assert len(paths) == len(set(paths)), "a migration path is installed more than once"

    matches = [_MIGRATION_NAME.fullmatch(path.name) for path in paths]
    assert all(matches), "every installed migration needs a three-digit ordinal"
    ordinals = [int(match.group(1)) for match in matches if match is not None]
    assert ordinals == sorted(ordinals), "migration ordinals must be increasing"
    assert len(ordinals) == len(set(ordinals)), "migration ordinals must be unique"

    print("schema migration order selftest: PASS")


if __name__ == "__main__":
    run()
