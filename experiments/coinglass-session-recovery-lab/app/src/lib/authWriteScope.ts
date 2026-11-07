let generation = 0;
let draftOwner: string | null = null;

/** Only the authenticated account may recover its own local editor drafts. */
export const authDraftOwner = () => draftOwner;

export type AuthWriteScope = {
  generation: number;
  isCurrent: () => boolean;
  assertCurrent: () => void;
};

/** Invalidate pending editor work before account-change cleanup can flush it.
 * This stores an identity generation only, never a session or credential. */
export function invalidateAuthWriteScopes(owner: string | null = null) {
  generation += 1;
  draftOwner = owner;
}

export function captureAuthWriteScope(): AuthWriteScope {
  const origin = generation;
  return {
    generation: origin,
    isCurrent: () => origin === generation,
    assertCurrent: () => {
      if (origin !== generation) {
        throw Object.assign(new Error("החשבון השתנה לפני שמירת הטיוטה"), {
          code: "AUTH_ACCOUNT_CHANGED",
        });
      }
    },
  };
}
