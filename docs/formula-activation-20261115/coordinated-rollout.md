# Coordinated publication of the three-formula roster

This is a proposed deployment procedure, not evidence of production changes.
The user must approve the exact tested source/manager commits and the brief
notification pause described below. No new HYPE authorization is included.

1. Read and retain the current non-secret source values of
   `ALERT_DELIVERY_PROFILE` and `EXPERIMENTAL_EXECUTION_FORWARD_MODE`.
   Keep Watch's persisted desired state, all secrets and automatic deployment
   settings unchanged. Verify the active manager and both deployment identities.
2. Deploy the exact tested **new three-formula source commit**, with both existing
   controls set to `OFF`. Do not deploy an arbitrary branch head. The source's
   retirement policy is independent of the delivery profile: it retires only
   CT009, OCONF and old HYPE, and does not claim retained pending notices as
   UNKNOWN when no Telegram send happened. Normal expiry continues; no clock is
   extended to recover a missed signal.
3. Verify this quiet source process and the termination of **every** previous
   source instance. During ordinary rolling overlap the previous instance can
   still approve signals; the suspension cutover must not be declared complete
   before that instance is gone. Do not advance to the manager while it remains.
4. Deploy the exact tested three-formula manager commit. Wait for the former
   manager to release ownership normally and verify the single new owner,
   continued management, three-rule admission, and targeted cancellation or
   recorded uncertainty for proved owned unfilled entry remainders. Do not
   release locks manually or resubmit an unresolved request.
5. Redeploy the **same exact source commit** with the original two values
   restored. The enabled transport mode is
   `experimental_approved_alert_transport_v2`; it must not be confused with the
   bridge mode. The outgoing quiet process cannot authorize old formulas during
   this overlap. Verify both live rosters, ordinary notifications, queues,
   preserved choices/requests, and normal cycles.

The manager keeps managing existing positions throughout the source pause. A
resting entry can fill until its targeted cancellation is proved; partial fills
retain protection and unknown outcomes remain unresolved. No global cancel,
forced position close or manual HYPE adoption is permitted.

During the quiet source stage, ordinary Watch/Magnet notifications and generation
of their notification transitions pause. Formula price completion, execution
forwarding (including source cancellations), and automatic recovery-result
notification polling pause too. Watch data collection, scoring and research
capture continue, and Telegram handlers remain available for Yoni's authenticated
choices. Pending notification identities and uncertainty remain saved. This is a
temporary operational consequence requiring approval, not a permanent change to
the ordinary-alert roster.

If a stage fails, stop advancing and report it. Keep position management running
where possible. Never resume an unverified older code version or restore entry
permission merely to clear the deployment status. Platform Suspend/Resume is
not part of this procedure; no unverified stopped-service deploy behavior is
assumed.
