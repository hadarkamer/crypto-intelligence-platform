import { compareWorksheetTimestamps } from "./worksheetClearTimestamp";

export type WorksheetSaveReceipt = {
  revision: string;
  value: Record<string, unknown>;
};

export function worksheetSaveReceipt(result: unknown): WorksheetSaveReceipt | undefined {
  if (!result || typeof result !== "object") return undefined;
  const receipt = result as Partial<WorksheetSaveReceipt>;
  return typeof receipt.revision === "string" &&
    compareWorksheetTimestamps(receipt.revision, receipt.revision) === 0 &&
    !!receipt.value &&
    typeof receipt.value === "object" &&
    !Array.isArray(receipt.value)
    ? (receipt as WorksheetSaveReceipt)
    : undefined;
}

type Change = { present: boolean; value: unknown };
type Snapshot = Map<string, Change>;
const same = (a: unknown, b: unknown) => JSON.stringify(a) === JSON.stringify(b);

/** Retains unsaved keys across row mutations and rejects older server snapshots. */
export class WorksheetDraftState {
  private server: Record<string, unknown>;
  private revision: string | null | undefined;
  private changes: Snapshot = new Map();
  private bases: Snapshot = new Map();

  constructor(value: Record<string, unknown>, revision?: string | null) {
    this.server = value;
    this.revision = revision;
  }

  private merge(snapshot: Snapshot) {
    const value = { ...this.server };
    for (const [key, change] of snapshot) {
      if (change.present) value[key] = change.value;
      else delete value[key];
    }
    return value;
  }

  get baseline() {
    return this.merge(this.bases);
  }

  get value() {
    return this.merge(this.changes);
  }

  edit(next: Record<string, unknown>) {
    const before = this.value;
    for (const key of new Set([...Object.keys(before), ...Object.keys(next)])) {
      const present = Object.hasOwn(next, key);
      if (present !== Object.hasOwn(before, key) || !same(before[key], next[key])) {
        if (!this.bases.has(key))
          this.bases.set(key, {
            present: Object.hasOwn(this.server, key),
            value: this.server[key],
          });
        this.changes.set(key, { present, value: next[key] });
      }
    }
    return this.value;
  }

  snapshot(): Snapshot {
    return new Map(this.changes);
  }

  payload(snapshot: Snapshot) {
    return this.merge(snapshot);
  }

  receive(value: Record<string, unknown>, revision?: string | null) {
    if (compareWorksheetTimestamps(revision, this.revision) === -1) return false;
    // An unversioned refresh cannot invalidate an acknowledged, versioned write.
    if (this.revision && !revision) return false;
    this.server = value;
    this.revision = revision;
    return true;
  }

  /** Collection mutations acknowledge their timestamp; older callers return only their value. */
  receiveMutation(value: Record<string, unknown>, revision?: string | null) {
    if (revision) this.receive(value, revision);
    else this.server = value;
    return this.value;
  }

  /** An explicit, acknowledged clear retires this field's former dirty keys.
   * Ordinary refreshes must continue preserving edits made after that clear. */
  replace(value: Record<string, unknown>, revision?: string | null) {
    this.server = value;
    this.revision = revision;
    this.changes.clear();
    this.bases.clear();
    return this.value;
  }

  acknowledge(snapshot: Snapshot, sent: Record<string, unknown>, receipt?: WorksheetSaveReceipt) {
    if (receipt) this.receive(receipt.value, receipt.revision);
    else this.server = sent;
    for (const [key, change] of snapshot) {
      const current = this.changes.get(key);
      if (current && current.present === change.present && same(current.value, change.value)) {
        this.changes.delete(key);
        this.bases.delete(key);
      } else if (current) {
        const acknowledged = receipt?.value ?? sent;
        this.bases.set(key, {
          present: Object.hasOwn(acknowledged, key),
          value: acknowledged[key],
        });
      }
    }
    return this.value;
  }
}

/** React Query may finish a read that started before the latest write. */
export function mergeWorksheetValueRows<T extends { field_id: string; updated_at?: string | null }>(
  previous: T[] | undefined,
  incoming: T[],
): T[] {
  const prior = new Map((previous ?? []).map((row) => [row.field_id, row]));
  const next = incoming.map((row) => {
    const old = prior.get(row.field_id);
    prior.delete(row.field_id);
    return old && compareWorksheetTimestamps(row.updated_at, old.updated_at) === -1 ? old : row;
  });
  // Values are not physically removed on clear; a pre-insert read may omit the new row.
  return [...next, ...prior.values()];
}
