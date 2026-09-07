"""Check targeted schema timeout boundaries without contacting a database."""
from contextlib import redirect_stdout
import io
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import research_formula_schema_admin as admin


def run():
    by_name = {path.name: path for path in admin.MIGRATION_PATHS}
    index = by_name['025_ordered_first_touch_sync_claim_queue.sql']
    fresh_delivery = by_name['026_research_sheet_fresh_delivery.sql']
    outcome_fresh_delivery = by_name['039_ordered_first_touch_fresh_delivery.sql']
    next_migration = by_name['027_ordered_formula_research_periods.sql']
    archive = by_name['028_telegram_archive_source_staging.sql']

    def exercise(paths, *, failure=None, targeted=True):
        events = []
        connection_options = []

        class Connection:
            def __init__(self):
                self.timeout = '15000' if targeted else 'server-default'
            def __enter__(self):
                return self
            def __exit__(self, error_type, error, traceback):
                events.append(('ROLLBACK' if error else 'CLOSE', self.timeout))
                # Transaction-local settings end on either commit or rollback.
                self.timeout = 'server-default'
                return False
            def execute(self, sql, params=()):
                if 'pg_advisory_xact_lock' in sql:
                    events.append(('LOCK', self.timeout))
                    return
                if 'set_config' in sql:
                    if sql == "SELECT set_config('statement_timeout', '15000', true)":
                        assert not params
                        self.timeout = '15000'
                    else:
                        assert sql == "SELECT set_config('statement_timeout', %s, true)", sql
                        assert len(params) == 1 and params[0] in ('15000', '60000')
                        self.timeout = params[0]
                    events.append(('TIMEOUT', self.timeout))
                    return
                path = next(path for path in paths if sql == path.read_text(encoding='utf-8'))
                events.append((path.name, self.timeout))
                if path == failure:
                    raise TimeoutError('simulated statement timeout')
            def commit(self):
                events.append(('COMMIT', self.timeout))
                self.timeout = 'server-default'

        def connect(url, **options):
            assert url == 'postgresql://local-only-selftest'
            connection_options.append(options)
            return Connection()

        with patch.dict(os.environ, {
            'FORMULA_SCHEMA_APPLY': '1',
            'FORMULA_SCHEMA_APPLY_ONLY': ','.join(path.name for path in paths) if targeted else '',
            'RESEARCH_DATABASE_URL': 'postgresql://local-only-selftest',
        }), patch.object(admin, 'psycopg', SimpleNamespace(connect=connect)), \
             patch.object(admin, '_selected_migration_paths', return_value=tuple(paths)), \
             redirect_stdout(io.StringIO()) as output:
            try:
                admin.apply_schema()
            except TimeoutError:
                assert failure is not None
            else:
                assert failure is None
        options = connection_options[0]
        assert options['connect_timeout'] == 5
        if targeted:
            assert options['options'] == '-c statement_timeout=15000 -c lock_timeout=1000'
            assert events[0] == ('LOCK', '15000'), 'lock acquisition must not inherit index timeout'
        else:
            assert 'options' not in options
        for path in paths:
            if any(kind == path.name for kind, _ in events):
                assert f'applying migration={path.name}' in output.getvalue()
        return events

    events = exercise([index, fresh_delivery, next_migration, archive])
    for path in (index, fresh_delivery):
        assert (path.name, '60000') in events
        assert events[events.index((path.name, '60000')) + 1] == ('TIMEOUT', '15000')
    for path in (next_migration, archive):
        assert (path.name, '15000') in events
    assert ('COMMIT', '15000') in events
    assert [value for kind, value in events if kind == 'TIMEOUT'] == [
        '60000', '15000', '60000', '15000', '15000', '15000', '15000', '15000']

    # Failure must propagate, roll back all changes, and prevent the next file.
    events = exercise([index, next_migration], failure=index)
    assert not any(kind in ('COMMIT', next_migration.name) for kind, _ in events)
    assert events[-1] == ('ROLLBACK', '60000')
    events = exercise([index, fresh_delivery, next_migration, archive], failure=fresh_delivery)
    assert not any(kind in ('COMMIT', next_migration.name, archive.name) for kind, _ in events)
    assert events[-1] == ('ROLLBACK', '60000')

    events = exercise([next_migration])
    assert not any(value == '60000' for _, value in events)
    events = exercise([index, fresh_delivery, next_migration, archive], targeted=False)
    assert not any(kind == 'TIMEOUT' for kind, _ in events), 'full installer behavior must remain unchanged'
    assert admin._migration_statement_timeout_ms(Path('025_other.sql')) == 15000
    assert admin._migration_statement_timeout_ms(Path(index.name + '.backup')) == 15000
    assert admin._migration_statement_timeout_ms(Path('026_other.sql')) == 15000
    assert admin._migration_statement_timeout_ms(Path(fresh_delivery.name + '.backup')) == 15000
    events = exercise([outcome_fresh_delivery, next_migration])
    assert (outcome_fresh_delivery.name, '60000') in events
    assert events[events.index((outcome_fresh_delivery.name, '60000')) + 1] == ('TIMEOUT', '15000')
    assert (next_migration.name, '15000') in events
    events = exercise([outcome_fresh_delivery, next_migration], failure=outcome_fresh_delivery)
    assert not any(kind in ('COMMIT', next_migration.name) for kind, _ in events)
    assert events[-1] == ('ROLLBACK', '60000')
    assert admin._migration_statement_timeout_ms(Path(outcome_fresh_delivery.name + '.backup')) == 15000
    print('targeted schema migration timeout selftest: PASS')


if __name__ == '__main__':
    run()
