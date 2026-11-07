import type { SupabaseClient } from "@supabase/supabase-js";
import { supabase } from "@/integrations/supabase/client";
import { hasMeaningfulContent } from "@/lib/content";
import { normalizePnl } from "@/lib/domain";
import { journalFieldKind } from "@/lib/journalFields";
import { effectiveTime } from "@/lib/effectiveTime";
import { fetchAllPages } from "@/lib/keysetPage";

const db = supabase as unknown as SupabaseClient;

type LegacyRow = Record<string, unknown>;

/** עמוד אחד לפי מפתח id — סדר יציב שמאפשר המשך אמין. */
function pageBy(query: unknown, afterId: string | null, limit: number) {
  const builder = query as {
    order: (column: string) => { limit: (n: number) => unknown };
    gt: (column: string, value: string) => unknown;
  };
  const scoped = afterId ? (builder.gt("id", afterId) as typeof builder) : builder;
  return scoped.order("id").limit(limit) as PromiseLike<{
    data: LegacyRow[] | null;
    error: unknown;
  }>;
}

/** סדר לפי מועד עדכון נשמר בצד הלקוח, כי הדפדוף חייב להיות לפי id. */
function sortByUpdatedAt<T extends LegacyRow>(rows: T[]): T[] {
  return [...rows].sort((a, b) => {
    const left = String(a.updated_at ?? "");
    const right = String(b.updated_at ?? "");
    if (left === right)
      return String(a.question_id ?? "").localeCompare(String(b.question_id ?? ""));
    return left < right ? -1 : 1;
  });
}

const txt = (value: unknown): string | null => (typeof value === "string" && value ? value : null);

function sortByCycleNumber<T extends LegacyRow>(rows: T[]): T[] {
  return [...rows].sort((a, b) => Number(a.cycle_number ?? 0) - Number(b.cycle_number ?? 0));
}

export type TradePnlRow = {
  project_id: string;
  project_name: string;
  cycle_id: string;
  cycle_number: number;
  symbol: string;
  direction: string;
  date: string;
  pnl: number | null;
  break_even: boolean;
  formula_name: string | null;
};

export async function fetchTradePnlRows(): Promise<TradePnlRow[]> {
  // כל הקריאות מדפדפות עד עמוד ריק כדי שדוח לא ייחתך בשקט בתקרת השירות
  const [projects, cycles, trades, questions] = await Promise.all([
    fetchAllPages<LegacyRow>((after, limit) =>
      pageBy(
        db.from("projects").select("id,name,symbol,direction,created_at,completed_at"),
        after,
        limit,
      ),
    ),
    fetchAllPages<LegacyRow>((after, limit) =>
      pageBy(
        db
          .from("project_trade_cycles")
          .select("id,project_id,cycle_number,created_at,first_pnl_at,first_close_at,formula_name"),
        after,
        limit,
      ),
    ),
    fetchAllPages<LegacyRow>((after, limit) => pageBy(db.from("trades").select("*"), after, limit)),
    fetchAllPages<LegacyRow>((after, limit) =>
      pageBy(
        db
          .from("template_questions")
          .select("id")
          .or("field_key.eq.trade_pnl,question_type.eq.pnl"),
        after,
        limit,
      ),
    ),
  ]);
  const pnlQuestionIds = questions.map((q) => String(q.id));
  const answers = pnlQuestionIds.length
    ? await fetchAllPages<LegacyRow>((after, limit) =>
        pageBy(
          db
            .from("project_answers")
            .select(
              "id,project_id,trade_cycle_id,question_id,text_value,number_value,break_even,updated_at",
            )
            .in("question_id", pnlQuestionIds),
          after,
          limit,
        ),
      )
    : [];

  const answerByCycle = new Map<
    string,
    { raw: unknown; break_even: boolean; updated_at: string | null }
  >();
  for (const a of sortByUpdatedAt(answers)) {
    const cycleId = String(a.trade_cycle_id ?? "");
    if (!cycleId) continue;
    const current = answerByCycle.get(cycleId);
    if (!current || String(a.updated_at ?? "") >= String(current.updated_at ?? "")) {
      answerByCycle.set(cycleId, {
        raw: a.number_value ?? a.text_value,
        break_even: a.break_even === true,
        updated_at: txt(a.updated_at),
      });
    }
  }
  const tradeByCycle = new Map(trades.map((t) => [String(t.trade_cycle_id), t]));
  const projectById = new Map(projects.map((p) => [String(p.id), p]));

  return sortByCycleNumber(cycles).map((cycle) => {
    const project = projectById.get(String(cycle.project_id));
    const trade = tradeByCycle.get(String(cycle.id));
    const journalPnl = answerByCycle.get(String(cycle.id));
    const pnl = journalPnl ? normalizePnl(journalPnl.raw) : normalizePnl(trade?.pnl_amount);
    return {
      project_id: String(cycle.project_id),
      project_name: String(project?.name ?? "פרויקט"),
      cycle_id: String(cycle.id),
      cycle_number: Number(cycle.cycle_number ?? 1),
      symbol: String(trade?.symbol || project?.symbol || ""),
      direction: String(trade?.direction || project?.direction || "undecided"),
      date:
        effectiveTime({
          firstCloseAt: txt(cycle.first_close_at),
          firstPnlAt: txt(cycle.first_pnl_at),
          closedAt: txt(trade?.closed_at),
          fallback: [txt(project?.completed_at), txt(cycle.created_at), txt(project?.created_at)],
        }) || "",
      pnl,
      break_even: journalPnl?.break_even === true,
      formula_name: txt(cycle.formula_name),
    };
  });
}

export type TradeJournalEntry = {
  project_id: string;
  project_name: string;
  /** Backward-compatible alias used by the existing Journal sorter. */
  created_at: string;
  project_created_at: string;
  cycle_id: string;
  cycle_number: number;
  conclusions: string | null;
  notes: string | null;
  journal_updated_at: string | null;
};

export async function fetchTradeJournalEntries(): Promise<TradeJournalEntry[]> {
  const [questionRows, projectRows, cycleRows] = await Promise.all([
    fetchAllPages<LegacyRow>((after, limit) =>
      pageBy(
        db
          .from("template_questions")
          .select("id,field_key,question_type")
          .or(
            "field_key.in.(journal_conclusions,journal_notes),question_type.in.(conclusions,notes)",
          ),
        after,
        limit,
      ),
    ),
    fetchAllPages<LegacyRow>((after, limit) =>
      pageBy(db.from("projects").select("id,name,created_at"), after, limit),
    ),
    fetchAllPages<LegacyRow>((after, limit) =>
      pageBy(db.from("project_trade_cycles").select("id,project_id,cycle_number"), after, limit),
    ),
  ]);

  const conclusionIds = new Set(
    questionRows.filter((q) => journalFieldKind(q) === "conclusions").map((q) => String(q.id)),
  );
  const noteIds = new Set(
    questionRows.filter((q) => journalFieldKind(q) === "notes").map((q) => String(q.id)),
  );
  const allIds = [...conclusionIds, ...noteIds];
  if (!allIds.length) return [];

  const journalAnswers = sortByUpdatedAt(
    await fetchAllPages<LegacyRow>((after, limit) =>
      pageBy(
        db
          .from("project_answers")
          .select("id,project_id,trade_cycle_id,question_id,text_value,updated_at")
          .in("question_id", allIds),
        after,
        limit,
      ),
    ),
  );

  const projectById = new Map(projectRows.map((p) => [String(p.id), p]));
  const cycleById = new Map(sortByCycleNumber(cycleRows).map((c) => [String(c.id), c]));
  const out = new Map<string, TradeJournalEntry>();

  for (const a of journalAnswers) {
    if (!hasMeaningfulContent(txt(a.text_value))) continue;
    const cycleId = String(a.trade_cycle_id ?? "");
    const cycle = cycleById.get(cycleId);
    if (!cycle) continue;
    const project = projectById.get(String(a.project_id));
    const projectCreatedAt = String(project?.created_at ?? "");
    const entry = out.get(cycleId) ?? {
      project_id: String(a.project_id),
      project_name: String(project?.name ?? "פרויקט"),
      created_at: projectCreatedAt,
      project_created_at: projectCreatedAt,
      cycle_id: cycleId,
      cycle_number: Number(cycle.cycle_number ?? 1),
      conclusions: null,
      notes: null,
      journal_updated_at: null,
    };
    if (conclusionIds.has(String(a.question_id)))
      entry.conclusions = [entry.conclusions, txt(a.text_value)].filter(Boolean).join("<br><br>");
    if (noteIds.has(String(a.question_id)))
      entry.notes = [entry.notes, txt(a.text_value)].filter(Boolean).join("<br><br>");
    const updatedAt = txt(a.updated_at);
    if (updatedAt && (!entry.journal_updated_at || updatedAt > entry.journal_updated_at)) {
      entry.journal_updated_at = updatedAt;
    }
    out.set(cycleId, entry);
  }

  return [...out.values()];
}

/** Project card shows the net result of all trades in that project. */
export async function fetchProjectNetPnl(): Promise<Record<string, number | null>> {
  const rows = await fetchTradePnlRows();
  const out: Record<string, number | null> = {};
  for (const row of rows) {
    if (!(row.project_id in out)) out[row.project_id] = null;
    if (row.pnl == null) continue;
    out[row.project_id] = (out[row.project_id] ?? 0) + row.pnl;
  }
  return out;
}
