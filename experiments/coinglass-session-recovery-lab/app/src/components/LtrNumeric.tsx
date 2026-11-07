import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

/**
 * בידוד כיווני לתוכן מספרי (מחירים וטווחים) בתוך ממשק RTL.
 * הערך עצמו אינו משתנה — רק סדר ההצגה נעול משמאל לימין,
 * כך ש-"$76,250–$76,650" מוצג עם הקצה הנמוך משמאל והגבוה מימין.
 */
export function LtrNum({ children, className }: { children: ReactNode; className?: string }) {
  return (
    <bdi dir="ltr" className={cn("inline-block [unicode-bidi:isolate]", className)}>
      {children}
    </bdi>
  );
}
