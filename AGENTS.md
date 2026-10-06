# Bot change and deployment rules

User instruction, 2026-10-06:

- Every proposed bot change or fix must be tested thoroughly in the Codex
  environment before deployment. Check affected trading, protection, monitoring,
  recovery and concurrency paths as appropriate; report evidence and limitations.
- Deploy only after the user gives explicit approval for the specific tested
  change. Earlier general deployment permissions do not authorize later changes.
- Do not push to an automatically deployed branch, change live runtime settings,
  or otherwise activate a change before that approval.
- Local implementation, review and testing are authorized. Keep tests isolated
  from live exchange accounts. Do not change strategy rules or exchange request
  budgets as a side effect of diagnostics.
