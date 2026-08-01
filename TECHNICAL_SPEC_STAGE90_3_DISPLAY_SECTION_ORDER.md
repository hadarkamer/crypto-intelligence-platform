# Stage 90.3 — Alert display section order

Display-only update. No calculation, score, selection, Alert, Watch, Max Pain, Price+OI, CVD, or confirmation logic changed.

The alert card is now separated and ordered as follows:

1. Max Pain — all Max Pain components, quality notes, opposite-direction score, and the seven-timeframe score list with its average.
2. Price + OI.
3. Futures CVD.
4. Spot CVD.
5. Combined summary of Price+OI, Futures Flow, and Spot Flow.

Removed from the combined summary:

- `כיוון מחיר נבדק...`
- `ה-Confirmation אינו משנה את Score או הדירוג.`

The ZIP remains flat and excludes runtime/cache artifacts.
