# Stage 82 — Max-Pain implied price direction in Price+OI conclusion

## Change
The combined Price+OI conclusion now compares the Regime direction with the price direction implied by the Max-Pain alert label, rather than comparing it directly with that label.

- Max-Pain `LONG` means longs are the side expected to be hurt, so the implied price direction is `SHORT` / down.
- Max-Pain `SHORT` means shorts are the side expected to be hurt, so the implied price direction is `LONG` / up.

The visible Max-Pain label and the existing combined-conclusion sentence format remain unchanged. This change affects contextual wording only and does not change Max-Pain scoring, alert selection, priority, or Price+OI calculations.
