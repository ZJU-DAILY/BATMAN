"use client";

import { createSession, fetchSession, getStoredSessionId, setStoredSessionId } from "./api";
import { SessionSummary } from "./types";

type EnsureSessionSummaryOptions = {
  resetPresetDraft?: boolean;
};

export async function ensureSessionSummary(options: EnsureSessionSummaryOptions = {}): Promise<SessionSummary> {
  const stored = getStoredSessionId();
  if (stored) {
    try {
      const summary = await fetchSession(stored);
      if (
        options.resetPresetDraft &&
        (
          summary.session.target_database?.tables.some((table) => table.id === "store_daily_totals_metadata") ||
          (
            summary.session.preset_id &&
            !summary.session.candidates.length &&
            !summary.session.accepted_candidate_id &&
            summary.session.status === "ready_for_generation"
          )
        )
      ) {
        const freshSummary = await createSession();
        setStoredSessionId(freshSummary.session.id);
        return freshSummary;
      }
      return summary;
    } catch {
      // Fall back to creating a new session when the old one expired.
    }
  }

  const summary = await createSession();
  setStoredSessionId(summary.session.id);
  return summary;
}
