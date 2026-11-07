import type { SupabaseClient } from "@supabase/supabase-js";
import { readAllPages } from "./readAllPages";
import { supabase } from "@/integrations/supabase/client";

const multiDb = supabase as unknown as SupabaseClient;
import type { Answer, Stage } from "./domain";
import { hasMeaningfulContent, isInlineImagePath } from "./content";

export async function fetchActiveVersionId(): Promise<string | null> {
  const { data, error } = await supabase
    .from("app_settings")
    .select("active_template_version_id")
    .limit(1)
    .maybeSingle();
  if (error) throw error;
  return data?.active_template_version_id ?? null;
}

export async function fetchStructure(versionId: string): Promise<Stage[]> {
  const { data: stages, error } = await supabase
    .from("template_stages")
    .select("*")
    .eq("template_version_id", versionId)
    .order("position");
  if (error) throw error;
  const stageIds = (stages ?? []).map((s) => s.id);
  if (!stageIds.length) return [];

  const { data: groups, error: groupsError } = await supabase
    .from("template_groups")
    .select("*")
    .in("stage_id", stageIds)
    .order("position");
  if (groupsError) throw groupsError;
  const groupIds = (groups ?? []).map((g) => g.id);

  const { data: questions, error: questionsError } = groupIds.length
    ? await supabase
        .from("template_questions")
        .select("*")
        .in("group_id", groupIds)
        .order("position")
    : { data: [] as never[], error: null };
  if (questionsError) throw questionsError;

  return (stages ?? []).map((s) => ({
    ...s,
    groups: (groups ?? [])
      .filter((g) => g.stage_id === s.id)
      .map((g) => ({
        ...g,
        questions: (questions ?? []).filter((q) => q.group_id === g.id),
      })),
  })) as Stage[];
}

export async function fetchProject(projectId: string) {
  const { data, error } = await supabase
    .from("projects")
    .select("*")
    .eq("id", projectId)
    .maybeSingle();
  if (error) throw error;
  return data;
}

/** Atomically copy a legacy project, including the mapping of every trade cycle. */
export async function duplicateProject(projectId: string): Promise<string> {
  const { data, error } = await multiDb.rpc("duplicate_legacy_project", {
    _project_id: projectId,
  });
  if (error) throw error;
  if (typeof data !== "string" || !data) throw new Error("שכפול הפרויקט לא הושלם");
  return data;
}

export async function fetchAnswers(projectId: string) {
  return readAllPages((from, to) =>
    supabase
      .from("project_answers")
      .select("*")
      .eq("project_id", projectId)
      .order("id")
      .range(from, to),
  );
}

export async function fetchImages(projectId: string) {
  return readAllPages((from, to) =>
    supabase
      .from("project_images")
      .select("*")
      .eq("project_id", projectId)
      .order("created_at")
      .order("id")
      .range(from, to),
  );
}

export async function fetchTrades(projectId: string) {
  const { data, error } = await multiDb.from("trades").select("*").eq("project_id", projectId);
  if (error) throw error;
  return data ?? [];
}

export async function fetchTrade(projectId: string, tradeCycleId?: string | null) {
  let query = multiDb.from("trades").select("*").eq("project_id", projectId);
  if (tradeCycleId) query = query.eq("trade_cycle_id", tradeCycleId);
  const { data, error } = await query
    .order("created_at", { ascending: true })
    .limit(1)
    .maybeSingle();
  if (error) throw error;
  return data;
}

export function imageUrl(path: string) {
  return supabase.storage.from("project-images").getPublicUrl(path).data.publicUrl;
}

export async function signedImageUrl(path: string) {
  const workspaceImage = path.startsWith("workspace-images:");
  const bucket = workspaceImage ? "workspace-images" : "project-images";
  const storagePath = workspaceImage ? path.slice("workspace-images:".length) : path;
  const { data, error } = await supabase.storage.from(bucket).createSignedUrl(storagePath, 60 * 60);
  if (error) {
    console.error("Image URL could not be signed", error);
    return "";
  }
  return data?.signedUrl ?? "";
}

/** Deep-copies a template version into a new version row (draft). */
export async function duplicateVersion(versionId: string, status: "draft" = "draft") {
  if (status !== "draft") throw new Error("ניתן לשכפל לטיוטה בלבד");
  const { data, error } = await multiDb.rpc("duplicate_legacy_template_version", {
    p_version_id: versionId,
  });
  if (error) throw error;
  const row = Array.isArray(data) ? data[0] : data;
  if (!row?.id) throw new Error("שכפול התבנית לא הושלם");
  return row as { id: string; template_id: string; version_number: number; status: "draft" };
}

/**
 * מקור האמת ל־PnL: תשובת המשתמש לשאלה עם field_key = 'trade_pnl' (גליון יומן).
 * נפילה אחורה ל־trades.pnl_amount רק כשאין תשובה כזו כלל.
 */
export async function fetchPnlByProject(): Promise<Record<string, number | null>> {
  const { fetchProjectNetPnl } = await import("./multiTradeReports");
  return fetchProjectNetPnl();
}

export type JournalEntry = {
  project_id: string;
  name: string;
  created_at: string;
  conclusions: string | null;
  notes: string | null;
  journal_updated_at: string | null;
};

export type HistoryImage = { storage_path: string; file_name: string | null };

export type HistoryItem = {
  project_id: string;
  question_id: string;
  trade_cycle_id: string | null;
  /** מיקום השאלה בתבנית שלה — נשמר ב-Pin כדי לא להסתמך על inference בניווט. */
  group_id: string | null;
  stage_id: string | null;
  author_id: string | null;
  project_name: string;
  updated_at: string | null;
  answer: Answer;
  images: HistoryImage[];
};

/**
 * היסטוריית תשובות לשאלות של גליון אחד, בקריאות מדורגות לפי סוג נתון:
 * 1) כל השאלות בכל גרסאות התבנית שחולקות את אותו question_key
 * 2) כל התשובות לשאלות אלה בפרויקטים אחרים
 * 3) שמות הפרויקטים + תמונות מצורפות
 */
export async function fetchAnswerHistory(
  questionKeys: string[],
  excludeProjectId: string,
): Promise<Record<string, HistoryItem[]>> {
  const keys = [...new Set(questionKeys.filter(Boolean))];
  if (!keys.length) return {};

  const questions = await readAllPages((from, to) =>
    supabase
      .from("template_questions")
      .select("id, question_key, group_id")
      .in("question_key", keys)
      .order("id")
      .range(from, to),
  );
  const keyByQuestionId = new Map<string, string>();
  const groupByQuestionId = new Map<string, string | null>();
  for (const q of questions ?? []) {
    if (q.question_key) keyByQuestionId.set(q.id, q.question_key);
    groupByQuestionId.set(q.id, q.group_id ?? null);
  }
  // batch יחיד למיפוי group -> stage (ללא N+1)
  const groupIds = [...new Set([...groupByQuestionId.values()].filter(Boolean))] as string[];
  const stageByGroupId = new Map<string, string | null>();
  if (groupIds.length) {
    const groups = await readAllPages((from, to) =>
      supabase
        .from("template_groups")
        .select("id, stage_id")
        .in("id", groupIds)
        .order("id")
        .range(from, to),
    );
    for (const g of groups ?? []) stageByGroupId.set(g.id, g.stage_id ?? null);
  }
  const ids = [...keyByQuestionId.keys()];
  if (!ids.length) return {};

  const answers = await readAllPages((from, to) =>
    supabase
      .from("project_answers")
      .select("*")
      .in("question_id", ids)
      .neq("project_id", excludeProjectId)
      .order("updated_at", { ascending: false })
      .order("id")
      .range(from, to),
  );
  const rows = answers ?? [];
  if (!rows.length) return {};

  const projectIds = [...new Set(rows.map((r) => r.project_id))];
  const [projects, imgs] = await Promise.all([
    readAllPages((from, to) =>
      supabase.from("projects").select("id, name").in("id", projectIds).order("id").range(from, to),
    ),
    readAllPages((from, to) =>
      supabase
        .from("project_images")
        .select("project_id, question_id, trade_cycle_id, storage_path, file_name")
        .in("question_id", ids)
        .in("project_id", projectIds)
        .order("id")
        .range(from, to),
    ),
  ]);
  const nameById = new Map((projects ?? []).map((p) => [p.id, p.name]));

  const out: Record<string, HistoryItem[]> = {};
  for (const a of rows) {
    const key = keyByQuestionId.get(a.question_id);
    if (!key) continue;
    // תמונות שהודבקו בתוך הטקסט אינן "קבצים מצורפים" ואינן מוצגות בהיסטוריה
    const images = (imgs ?? []).filter(
      (i) =>
        i.project_id === a.project_id &&
        i.question_id === a.question_id &&
        String((i as { trade_cycle_id?: string | null }).trade_cycle_id ?? "") ===
          String((a as { trade_cycle_id?: string | null }).trade_cycle_id ?? "") &&
        !isInlineImagePath(i.storage_path),
    );
    if (!hasHistoryValue(a as Answer, images.length)) continue;
    (out[key] ??= []).push({
      project_id: a.project_id,
      question_id: a.question_id,
      trade_cycle_id: (a as { trade_cycle_id?: string | null }).trade_cycle_id ?? null,
      group_id: groupByQuestionId.get(a.question_id) ?? null,
      stage_id: stageByGroupId.get(groupByQuestionId.get(a.question_id) ?? "") ?? null,
      author_id: (a as { last_edited_by?: string | null }).last_edited_by ?? null,
      project_name: nameById.get(a.project_id) ?? "פרויקט",
      updated_at: a.updated_at ?? null,
      answer: a as Answer,
      images,
    });
  }
  return out;
}

/** תשובה נחשבת להיסטוריה רק אם יש בה תוכן משמעותי. */
function hasHistoryValue(a: Answer, imageCount: number) {
  if (imageCount > 0) return true;
  if (a.boolean_value === true) return true;
  if (a.number_value !== null && a.number_value !== undefined) return true;
  if (a.date_value) return true;
  if (hasMeaningfulContent(a.text_value)) return true;
  const c = a.choice_value_json;
  if (Array.isArray(c)) return c.length > 0;
  if (c && typeof c === "object") return Object.values(c).some((v) => !!v);
  return false;
}

/* ------------------- סימונים אישיים בסקשני "נפשי" ------------------- */

export type QuestionCheck = {
  question_id: string;
  user_id: string;
  trade_cycle_id: string;
  checked: boolean;
};

/** כל הסימונים האישיים בפרויקט — שאילתה אחת לכל הגליון, ללא N+1. */
export async function fetchQuestionChecks(projectId: string): Promise<QuestionCheck[]> {
  const { data, error } = await multiDb
    .from("question_user_checks")
    .select("question_id, user_id, trade_cycle_id, checked")
    .eq("project_id", projectId);
  if (error) throw error;
  return (data ?? []).map((r) => ({
    question_id: r.question_id,
    user_id: r.user_id,
    trade_cycle_id: String((r as { trade_cycle_id?: string }).trade_cycle_id ?? ""),
    checked: !!r.checked,
  }));
}

/** סימון/ביטול אישי — המשתמש יכול לשנות את עצמו בלבד (נאכף גם ב-RLS). */
export async function setQuestionCheck(
  projectId: string,
  questionId: string,
  checked: boolean,
  tradeCycleId: string,
) {
  const { data: auth } = await supabase.auth.getUser();
  if (!auth.user) throw new Error("אין משתמש מחובר");
  const { error } = await supabase.from("question_user_checks").upsert(
    {
      project_id: projectId,
      question_id: questionId,
      user_id: auth.user.id,
      trade_cycle_id: tradeCycleId,
      checked,
      updated_at: new Date().toISOString(),
    } as never,
    { onConflict: "project_id,question_id,user_id,trade_cycle_id" },
  );
  if (error) throw error;
}

/* ------------------------- הרשאות ושמות שותפים ------------------------- */

export type ProjectRole = "owner" | "editor" | "viewer";

export type ProjectAccess = {
  userId: string | null;
  role: ProjectRole | null;
  canEdit: boolean;
};

/** תפקיד המשתמש הנוכחי בפרויקט — בעלים, עורך או צופה. */
export async function fetchMyProjectAccess(projectId: string): Promise<ProjectAccess> {
  const { data: auth } = await supabase.auth.getUser();
  const uid = auth.user?.id ?? null;
  if (!uid) return { userId: null, role: null, canEdit: false };

  const { data: project } = await supabase
    .from("projects")
    .select("user_id")
    .eq("id", projectId)
    .maybeSingle();
  if (project?.user_id === uid) return { userId: uid, role: "owner", canEdit: true };

  const { data: member } = await supabase
    .from("project_members")
    .select("role")
    .eq("project_id", projectId)
    .eq("user_id", uid)
    .maybeSingle();
  const role = (member?.role as ProjectRole | undefined) ?? null;
  return { userId: uid, role, canEdit: role === "owner" || role === "editor" };
}

/** שמות תצוגה של המשתמשים בפרויקט — למי שסימן סעיף בסקשן נפשי. */
export async function fetchProjectUserLabels(projectId: string): Promise<Record<string, string>> {
  const { data: members } = await supabase
    .from("project_members")
    .select("user_id")
    .eq("project_id", projectId);
  const { data: project } = await supabase
    .from("projects")
    .select("user_id")
    .eq("id", projectId)
    .maybeSingle();

  const ids = [
    ...new Set(
      [...(members ?? []).map((m) => m.user_id), project?.user_id].filter(Boolean) as string[],
    ),
  ];
  if (!ids.length) return {};
  const { data: profiles } = await supabase
    .from("profiles")
    .select("id, display_name, email")
    .in("id", ids);
  const out: Record<string, string> = {};
  for (const p of profiles ?? []) {
    out[p.id] = (p.display_name || p.email || "משתמש").split("@")[0];
  }
  return out;
}

/* ------------------------------- שיתוף ------------------------------- */

export type ProjectMember = {
  user_id: string;
  role: ProjectRole;
  label: string;
  email: string | null;
  isOwner: boolean;
};

/** רשימת המשתתפים בפרויקט — בעלים ואחריו שאר החברים. */
export async function fetchProjectMembers(projectId: string): Promise<ProjectMember[]> {
  const { data: project } = await supabase
    .from("projects")
    .select("user_id")
    .eq("id", projectId)
    .maybeSingle();
  const { data: members } = await supabase
    .from("project_members")
    .select("user_id, role")
    .eq("project_id", projectId);

  const ids = [
    ...new Set(
      [project?.user_id, ...(members ?? []).map((m) => m.user_id)].filter(Boolean) as string[],
    ),
  ];
  if (!ids.length) return [];
  const { data: profiles } = await supabase
    .from("profiles")
    .select("id, display_name, email")
    .in("id", ids);
  const byId = new Map((profiles ?? []).map((p) => [p.id, p]));
  const label = (id: string) => {
    const p = byId.get(id);
    return (p?.display_name || p?.email || "משתמש").split("@")[0];
  };

  const out: ProjectMember[] = [];
  if (project?.user_id) {
    out.push({
      user_id: project.user_id,
      role: "owner",
      label: label(project.user_id),
      email: byId.get(project.user_id)?.email ?? null,
      isOwner: true,
    });
  }
  for (const m of members ?? []) {
    if (m.user_id === project?.user_id) continue;
    out.push({
      user_id: m.user_id,
      role: (m.role as ProjectRole) ?? "viewer",
      label: label(m.user_id),
      email: byId.get(m.user_id)?.email ?? null,
      isOwner: false,
    });
  }
  return out;
}

/**
 * צירוף משתמש רשום לפי כתובת מייל. הבדיקה והחיפוש מתבצעים בצד השרת
 * (הלקוח אינו יכול לקרוא את טבלת המשתמשים, וזה נכון כך).
 */
export async function addMemberByEmail(
  projectId: string,
  email: string,
  role: "editor" | "viewer",
): Promise<{ ok: boolean; message?: string; label?: string }> {
  const { data, error } = await supabase.rpc("add_project_member_by_email", {
    _project_id: projectId,
    _email: email,
    _role: role,
  });
  if (error) throw error;
  return (data ?? { ok: false, message: "פעולה נכשלה" }) as {
    ok: boolean;
    message?: string;
    label?: string;
  };
}

export async function updateMemberRole(
  projectId: string,
  userId: string,
  role: "editor" | "viewer",
) {
  const { error } = await supabase
    .from("project_members")
    .update({ role })
    .eq("project_id", projectId)
    .eq("user_id", userId);
  if (error) throw error;
}

export async function removeMember(projectId: string, userId: string) {
  const { error } = await supabase
    .from("project_members")
    .delete()
    .eq("project_id", projectId)
    .eq("user_id", userId);
  if (error) throw error;
}

/* ---------------------------- תשובות נעוצות ---------------------------- */

export type PinnedAnswer = {
  id: string;
  question_key: string;
  question_type: string;
  question_title: string | null;
  source_project_id: string | null;
  source_project_name: string | null;
  source_question_id: string | null;
  source_trade_cycle_id: string | null;
  source_group_id: string | null;
  source_stage_id: string | null;
  answer_snapshot: Record<string, unknown>;
  image_paths: { storage_path: string; file_name: string | null }[];
  author_label: string | null;
  answer_updated_at: string | null;
  created_at: string;
};

function normalizePin(row: Record<string, unknown>): PinnedAnswer {
  const imgs = Array.isArray(row.image_paths) ? (row.image_paths as unknown[]) : [];
  return {
    id: String(row.id),
    question_key: String(row.question_key),
    question_type: String(row.question_type ?? "long_text"),
    question_title: (row.question_title as string | null) ?? null,
    source_project_id: (row.source_project_id as string | null) ?? null,
    source_project_name: (row.source_project_name as string | null) ?? null,
    source_question_id: (row.source_question_id as string | null) ?? null,
    source_trade_cycle_id: (row.source_trade_cycle_id as string | null) ?? null,
    source_group_id: (row.source_group_id as string | null) ?? null,
    source_stage_id: (row.source_stage_id as string | null) ?? null,
    answer_snapshot: (row.answer_snapshot ?? {}) as Record<string, unknown>,
    image_paths: imgs.map((i) => {
      const o = (i ?? {}) as Record<string, unknown>;
      return {
        storage_path: String(o.storage_path ?? ""),
        file_name: (o.file_name as string | null) ?? null,
      };
    }),
    author_label: (row.author_label as string | null) ?? null,
    answer_updated_at: (row.answer_updated_at as string | null) ?? null,
    created_at: String(row.created_at),
  };
}

/** כל הנעיצות של מפתחות השאלות בגליון — שאילתה אחת, ללא N+1. */
export async function fetchPins(questionKeys: string[]): Promise<Record<string, PinnedAnswer[]>> {
  const keys = [...new Set(questionKeys.filter(Boolean))];
  if (!keys.length) return {};
  const { data, error } = await supabase
    .from("pinned_answers")
    .select("*")
    .in("question_key", keys)
    .order("created_at", { ascending: false });
  if (error) throw error;
  const out: Record<string, PinnedAnswer[]> = {};
  for (const row of data ?? []) {
    const pin = normalizePin(row as Record<string, unknown>);
    (out[pin.question_key] ??= []).push(pin);
  }
  return out;
}

export type NewPin = {
  question_key: string;
  question_type: string;
  question_title?: string | null;
  source_project_id?: string | null;
  source_project_name?: string | null;
  source_question_id?: string | null;
  source_trade_cycle_id?: string | null;
  source_group_id?: string | null;
  source_stage_id?: string | null;
  answer_snapshot: Record<string, unknown>;
  image_paths?: { storage_path: string; file_name: string | null }[];
  author_label?: string | null;
  answer_updated_at?: string | null;
};

export async function createPin(pin: NewPin) {
  const { data: auth } = await supabase.auth.getUser();
  const { error } = await supabase.from("pinned_answers").insert({
    ...pin,
    image_paths: (pin.image_paths ?? []) as never,
    answer_snapshot: pin.answer_snapshot as never,
    pinned_by: auth.user?.id,
  } as never);
  if (error) throw error;
}

export async function deletePin(id: string) {
  const { error } = await supabase.from("pinned_answers").delete().eq("id", id);
  if (error) throw error;
}

/* ------------------- ניקוי תמונות מוטמעות יתומות ------------------- */

/**
 * מוחק תמונות שהודבקו בתוך טקסט עשיר ואינן מופיעות יותר ב-HTML שנשמר —
 * אך ורק לאחר בדיקה במסד שאין להן אף הפניה נוספת (תשובות אחרות, רעיונות
 * לתרחישים או תשובות נעוצות). הבדיקה נעשית בקריאה אחת (batch) והפעולה
 * אידמפוטנטית: הרצה חוזרת ללא יתומים אינה עושה דבר.
 *
 * חשוב: יש לקרוא לפונקציה רק לאחר שמירה מוצלחת של ה-HTML החדש.
 */
export async function cleanupInlineImages(
  projectId: string,
  questionId: string | null,
  html: string | null | undefined,
  tradeCycleId?: string | null,
) {
  const { inlineImagePathsIn } = await import("./content");
  const keep = new Set(inlineImagePathsIn(html));

  let query = multiDb
    .from("project_images")
    .select("id, storage_path")
    .eq("project_id", projectId)
    .like("storage_path", "%-inline-%");
  query = questionId ? query.eq("question_id", questionId) : query.is("question_id", null);
  if (tradeCycleId) query = query.eq("trade_cycle_id", tradeCycleId);

  const { data, error: queryError } = await query;
  if (queryError) throw queryError;
  const candidates = (data ?? []).filter((i) => !keep.has(i.storage_path));
  if (!candidates.length) return;

  // בדיקת הפניות בכל המערכת — קובץ שמופיע בתשובה אחרת או בתשובה נעוצה נשאר.
  const paths = candidates.map((c) => c.storage_path);
  const { data: refs, error: refError } = await supabase.rpc("inline_paths_referenced", {
    _project_id: projectId,
    _paths: paths,
  });
  if (refError) throw refError;
  const referenced = new Set(((refs ?? []) as unknown as string[]).map((p) => String(p)));

  const orphans = candidates.filter((c) => !referenced.has(c.storage_path));
  if (!orphans.length) return;

  const { error: delError } = await supabase
    .from("project_images")
    .delete()
    .in(
      "id",
      orphans.map((o) => o.id),
    );
  if (delError) throw delError;

  // הקובץ הפיזי נמחק רק לאחר שהוסרו ה-metadata ואין אף הפניה (כולל נעיצות).
  await removeStorageIfUnreferenced(
    projectId,
    orphans.map((o) => o.storage_path),
  );
}

/**
 * מחיקת קבצים מהאחסון רק כאשר אין להם אף הפניה נוספת במערכת
 * (תשובה אחרת, רעיונות לתרחישים או תשובה נעוצה). משתמש באותה בדיקת
 * הפניות מרכזית — כדי ש-snapshot של נעיצה לא יישבר כשמוחקים במקור.
 */
export async function removeStorageIfUnreferenced(projectId: string, paths: string[]) {
  const list = [...new Set(paths.filter(Boolean))];
  if (!list.length) return;
  const { data: refs, error } = await supabase.rpc("inline_paths_referenced", {
    _project_id: projectId,
    _paths: list,
  });
  if (error) throw error;
  const referenced = new Set(((refs ?? []) as unknown as string[]).map((p) => String(p)));
  const free = list.filter((p) => !referenced.has(p));
  if (free.length) {
    const { error: removeError } = await supabase.storage.from("project-images").remove(free);
    if (removeError) throw removeError;
  }
}

/* --------------------- תפקידים ושמות ברשימת הפרויקטים --------------------- */

/** תפקיד המשתמש הנוכחי בכל פרויקט שהוא רואה — לצורך הצגת פעולות מותרות בלבד. */
export async function fetchMyProjectRoles(): Promise<Record<string, ProjectRole>> {
  const { data: auth } = await supabase.auth.getUser();
  const uid = auth.user?.id;
  if (!uid) return {};
  const out: Record<string, ProjectRole> = {};
  const { data: mine } = await supabase.from("projects").select("id").eq("user_id", uid);
  for (const p of mine ?? []) out[p.id] = "owner";
  const { data: memberships } = await supabase
    .from("project_members")
    .select("project_id, role")
    .eq("user_id", uid);
  for (const m of memberships ?? []) {
    if (out[m.project_id] === "owner") continue;
    out[m.project_id] = (m.role as ProjectRole) ?? "viewer";
  }
  return out;
}

/** שמות תצוגה לפי מזהי משתמש — שאילתה אחת, ללא N+1. */
export async function fetchUserLabels(ids: string[]): Promise<Record<string, string>> {
  const unique = [...new Set(ids.filter(Boolean))];
  if (!unique.length) return {};
  const { data } = await supabase
    .from("profiles")
    .select("id, display_name, email")
    .in("id", unique);
  const out: Record<string, string> = {};
  for (const p of data ?? []) out[p.id] = (p.display_name || p.email || "משתמש").split("@")[0];
  return out;
}
