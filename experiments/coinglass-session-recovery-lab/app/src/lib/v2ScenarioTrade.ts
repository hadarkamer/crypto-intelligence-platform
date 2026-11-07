import type { SupabaseClient } from "@supabase/supabase-js";
import { supabase } from "@/integrations/supabase/client";
import type { V2Scenario, V2Template, V2Trade } from "@/lib/workspaceV1";
import type { ScenarioAttachments } from "@/lib/scenarioEvidence";

const db = supabase as unknown as SupabaseClient;
export type ScenarioAlternative = ScenarioAttachments & {
  id: string;
  scenario_html: string;
  reasons_html: string;
  invalidation_html: string;
  base_values?: Record<string, Record<string, unknown>>;
};
export type Scenario = V2Scenario &
  ScenarioAttachments & {
    template_id: string | null;
    author_name: string | null;
    alternatives: ScenarioAlternative[];
  };
export type Trade = V2Trade & { root_trade_id: string | null; cycle_number: number };
export type WatchLink = { label: string; href: string; mobile_href?: string };
export type AssetSettings = {
  id?: string;
  workspace_id: string;
  symbol: string;
  display_name: string | null;
  default_scenario_template_id: string | null;
  default_trade_template_id: string | null;
  watch_links: WatchLink[];
  settings: Record<string, unknown>;
};
export type EntityValue = {
  field_id: string;
  value: Record<string, unknown>;
  updated_at?: string;
  updated_by?: string | null;
};
export const BASE_SCENARIO_FIELDS = new Set([
  "scenario_body",
  "scenario_reasons",
  "scenario_invalidation",
]);
export function normalizeAsset(symbol: string) {
  const clean = symbol.trim().toUpperCase();
  if (!/^[A-Z0-9][A-Z0-9._-]{0,19}$/.test(clean)) throw new Error("יש להזין סימול מטבע תקין");
  return clean;
}
export function defaultWatchLinks(symbol: string): WatchLink[] {
  const asset = encodeURIComponent(normalizeAsset(symbol));
  return [
    {
      label: "Cryptocurrency Alerting",
      href: `https://cryptocurrencyalerting.com/coin/${asset}`,
    },
    {
      label: `TradingView · ${symbol}/USDT`,
      href: `https://www.tradingview.com/chart/?symbol=BINANCE%3A${asset}USDT`,
    },
    {
      label: `Binance Spot · ${symbol}/USDT`,
      href: `https://www.binance.com/en/trade/${asset}_USDT?type=spot`,
    },
    {
      label: `Bybit Spot · ${symbol}/USDT`,
      href: `https://www.bybit.com/en/trade/spot/${asset}/USDT`,
    },
  ];
}
export function validWatchHref(href: string, mobile = false) {
  try {
    const url = new URL(href);
    return (
      url.protocol === "https:" ||
      (mobile && ["tradingview:", "binance:", "bybit:"].includes(url.protocol))
    );
  } catch {
    return false;
  }
}
export function resolvedWatchLinks(asset: string, settings?: AssetSettings) {
  const links = settings?.watch_links?.length ? settings.watch_links : defaultWatchLinks(asset);
  const defaults = defaultWatchLinks(asset);
  const generic = new Set([
    "https://cryptocurrencyalerting.com/",
    "https://cryptocurrencyalerting.com/price-alert.html",
    "https://www.tradingview.com/",
    "https://www.binance.com/",
    "https://www.bybit.com/",
  ]);
  return links
    .map((link) =>
      generic.has(link.href)
        ? {
            ...link,
            href:
              defaults.find((item) => new URL(item.href).hostname === new URL(link.href).hostname)
                ?.href ?? link.href,
          }
        : link,
    )
    .map((link) => ({
      ...link,
      href: link.href.replaceAll("{symbol}", encodeURIComponent(asset)),
      mobile_href: link.mobile_href?.replaceAll("{symbol}", encodeURIComponent(asset)),
    }))
    .filter((link) => validWatchHref(link.href));
}
export async function listAssetSettings(workspaceId: string): Promise<AssetSettings[]> {
  const { data, error } = await db
    .from("v2_asset_settings")
    .select("*")
    .eq("workspace_id", workspaceId)
    .eq("active", true)
    .order("symbol");
  if (error) throw error;
  return (data ?? []) as AssetSettings[];
}
export async function saveAssetSettings(settings: AssetSettings) {
  const { data: auth, error: authError } = await supabase.auth.getUser();
  if (authError) throw authError;
  if (!auth.user) throw new Error("נדרשת התחברות");
  const { data, error } = await db
    .from("v2_asset_settings")
    .upsert(
      {
        ...settings,
        symbol: normalizeAsset(settings.symbol),
        updated_by: auth.user.id,
      },
      { onConflict: "workspace_id,symbol" },
    )
    .select("*")
    .single();
  if (error) throw error;
  return data as AssetSettings;
}
export function preferredTemplate(
  templates: V2Template[],
  symbol: string,
  preferred?: string | null,
) {
  return (
    templates.find((template) => template.id === preferred && template.active) ??
    templates.find((template) => template.asset_symbol === symbol && template.active) ??
    templates.find((template) => template.is_default && template.active) ??
    templates.find((template) => template.active)
  );
}
export async function listScenarioValues(id: string): Promise<EntityValue[]> {
  const { data, error } = await db.from("v2_scenario_values").select("*").eq("scenario_id", id);
  if (error) throw error;
  return (data ?? []) as EntityValue[];
}
export async function saveScenario(
  input: Partial<Scenario> & {
    id: string;
    workspace_id: string;
    title: string;
    asset_symbol: string;
  },
  values: Record<string, Record<string, unknown>>,
) {
  const { data, error } = await db.rpc("save_v2_scenario", {
    p_scenario: { ...input, asset_symbol: normalizeAsset(input.asset_symbol) },
    p_values: Object.entries(values).map(([field_id, value]) => ({ field_id, value })),
  });
  if (error) throw error;
  return data as Scenario;
}
export async function patchScenario(id: string, patch: Partial<Scenario>) {
  const { data, error } = await db
    .from("v2_scenarios")
    .update(patch)
    .eq("id", id)
    .select("*")
    .single();
  if (error) throw error;
  return data as Scenario;
}
export async function reorderScenarios(workspaceId: string, ids: string[]) {
  const { error } = await db.rpc("reorder_v2_scenarios", {
    p_workspace_id: workspaceId,
    p_ids: ids,
  });
  if (error) throw error;
}
export async function workspaceCanEdit(workspaceId: string) {
  const { data, error } = await db.rpc("v2_can_edit_workspace", { p_workspace_id: workspaceId });
  if (error) throw error;
  return data === true;
}
