"""QA input adapter for the accepted, owner-only account snapshot.

The snapshot is provisional input to the normal private session transport. It
does not bootstrap a ready session or manufacture a protected-map receipt.
Only the unchanged browser guard/checkpoint can persist verified cookies.
"""
from dataclasses import replace
import hmac


def install_provisional_snapshot(state, slot, emit):
    import modern_map_cycle
    from session_runtime import ManagedSession
    from session_state import SessionStateError, validate_state

    pending = validate_state(state)

    class RestoredQASession(ManagedSession):
        async def begin(self):
            nonlocal pending
            await super().begin()
            if pending is None:
                return
            # This adapter is permitted only for the first, empty, locally
            # owned lease. No existing session, revision or incident is reset.
            if (not hmac.compare_digest(self.store.slot, slot)
                    or self.lease.revision != 1
                    or self.lease.state != {'cookies': [], 'origins': []}
                    or self.lease.read_only):
                raise SessionStateError('qa_restore_requires_fresh_owned_probe')
            with self.connection_guard:
                with self.store._atomic():
                    self.store._owner(self.lease)
            self.lease = replace(self.lease, state=pending)
            pending = None
            emit('provisional_account_input', persisted_as_verified=False,
                 private_transport=True, protected_checkpoint_required=True)

    modern_map_cycle.ManagedSession = RestoredQASession
