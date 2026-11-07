export type DominanceDailyMetrics = {
  dominancePercent: number;
  changes: { "1D": number; "1W": number; "1M": number };
  baselines: Record<"1D" | "1W" | "1M", { at: string; close: number }>;
  trend: "up" | "down" | "mixed";
  dailyBars: number[][];
};

/** Closure-free: the source browser and server verify the same measured OHLC.
 * Monthly change uses the same UTC calendar date one month earlier, clamped
 * to that month's last day. No missing bar is interpolated. */
export function dominanceMetricsFromBars(raw: unknown, now: number): DominanceDailyMetrics | null {
  if (!Array.isArray(raw) || raw.length < 35 || raw.length > 5000 || !Number.isFinite(now))
    return null;
  const rows = raw.slice(-40) as number[][];
  for (let index = 0; index < rows.length; index++) {
    const bar = rows[index];
    if (
      !Array.isArray(bar) ||
      bar.length !== 5 ||
      bar.some((value) => typeof value !== "number" || !Number.isFinite(value)) ||
      !Number.isInteger(bar[0]) ||
      bar[0] % 86400 !== 0 ||
      bar.slice(1).some((value) => value <= 0 || value > 100) ||
      bar[2] < Math.max(bar[1], bar[4]) ||
      bar[3] > Math.min(bar[1], bar[4]) ||
      (index > 0 && bar[0] - rows[index - 1][0] !== 86400)
    )
      return null;
  }
  const latest = rows.at(-1)!;
  if (latest[0] * 1000 > now + 60000 || now - latest[0] * 1000 > 36 * 60 * 60 * 1000) return null;
  const month = new Date(latest[0] * 1000);
  const day = month.getUTCDate();
  month.setUTCDate(1);
  month.setUTCMonth(month.getUTCMonth() - 1);
  const lastDay = new Date(
    Date.UTC(month.getUTCFullYear(), month.getUTCMonth() + 1, 0),
  ).getUTCDate();
  month.setUTCDate(Math.min(day, lastDay));
  const times = {
    "1D": latest[0] - 86400,
    "1W": latest[0] - 7 * 86400,
    "1M": month.getTime() / 1000,
  };
  const changes = {} as DominanceDailyMetrics["changes"];
  const baselines = {} as DominanceDailyMetrics["baselines"];
  for (const period of ["1D", "1W", "1M"] as const) {
    const baseline = rows.find((row) => row[0] === times[period]);
    if (!baseline) return null;
    changes[period] = (latest[4] / baseline[4] - 1) * 100;
    baselines[period] = { at: new Date(baseline[0] * 1000).toISOString(), close: baseline[4] };
  }
  const recent = rows.slice(-7).map((row) => row[4]);
  const mean = recent.reduce((sum, value) => sum + value, 0) / recent.length;
  const slope = recent.reduce((sum, value, index) => sum + (index - 3) * (value - mean), 0) / 28;
  const trend =
    slope > 0 && latest[4] > mean && changes["1D"] > 0
      ? "up"
      : slope < 0 && latest[4] < mean && changes["1D"] < 0
        ? "down"
        : "mixed";
  return { dominancePercent: latest[4], changes, baselines, trend, dailyBars: rows };
}

export function dominanceForecast(trend: DominanceDailyMetrics["trend"]): string {
  return trend === "up"
    ? "בטווח הקצר: נטייה לעלייה בדומיננס."
    : trend === "down"
      ? "בטווח הקצר: נטייה לירידה בדומיננס."
      : "בטווח הקצר: כיוון מעורב, ללא מגמה ברורה.";
}
