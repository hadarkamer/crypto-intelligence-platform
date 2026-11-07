import { dominanceForecast, dominanceMetricsFromBars } from "./collectionDominanceMetrics";
import type { VerifiedDominanceChart } from "./collectionDominanceChart";
import { escapeAutofillHtml } from "./collectionAutofill";

/** Render only recomputed, measured daily observations. The model cannot add a
 * placeholder image, empty percentages or an unsupported price prediction. */
export function dominanceChartsValue(
  readings: { chart: VerifiedDominanceChart; sourceUrl: string; imagePath?: string }[],
  previous: Record<string, unknown> = {},
  color = "rt-color-purple",
): Record<string, unknown> & { text: string } {
  if (!readings.length) throw new Error("לא אומתו נתוני גרף יומי; התיבה לא שונתה.");
  const verified = readings.map((reading) => {
    const metrics = dominanceMetricsFromBars(
      reading.chart.dailyBars,
      Date.parse(reading.chart.capturedAt),
    );
    if (!metrics || metrics.dominancePercent !== reading.chart.dominancePercent)
      throw new Error("לא אומתו אחוזי הדומיננס מהגרף היומי; התיבה לא שונתה.");
    return { ...reading, metrics };
  });
  const text = verified
    .map(({ chart, metrics, imagePath }) => {
      const level = metrics.dominancePercent.toLocaleString("en-US", { maximumFractionDigits: 4 });
      const lines = (["1D", "1W", "1M"] as const).map((period) => {
        const change = metrics.changes[period];
        const shown = Math.abs(change) < 0.005 ? 0 : change;
        const formatted = Math.abs(shown).toLocaleString("en-US", { maximumFractionDigits: 2 });
        const cls = shown > 0 ? "rt-color-green" : shown < 0 ? "rt-color-red" : color;
        return `<p dir="ltr" class="${cls}">${period}: ${shown > 0 ? "+" : shown < 0 ? "−" : ""}${formatted}%</p>`;
      });
      return (
        `<p dir="ltr" class="${color}"><strong>${escapeAutofillHtml(chart.symbol.replaceAll("+", " + "))}: ${level}%</strong></p>` +
        lines.join("") +
        `<p class="${color}">${dominanceForecast(metrics.trend)}</p>` +
        (imagePath
          ? `<img data-path="${escapeAutofillHtml(imagePath)}" alt="${escapeAutofillHtml(chart.symbol)} · 1D">`
          : "")
      );
    })
    .join("");
  return {
    ...previous,
    text,
    _autofill: {
      kind: "dominance",
      captured_at: verified[0].chart.capturedAt,
      source_url: verified[0].sourceUrl,
      dominance_percent: verified[0].metrics.dominancePercent,
      dominance_changes: verified[0].metrics.changes,
      dominance_chart: verified[0].chart,
      dominance_readings: verified.map(({ chart, metrics, sourceUrl, imagePath }) => ({
        symbol: chart.symbol,
        source_url: sourceUrl,
        image_path: imagePath ?? null,
        captured_at: chart.capturedAt,
        latest_bar_at: chart.latestBarAt,
        chart_version: chart.version,
        interval: chart.interval,
        width: chart.width,
        height: chart.height,
        ...metrics,
      })),
      methodology: "measured_daily_closes_calendar_month_and_seven_day_trend",
    },
  };
}
