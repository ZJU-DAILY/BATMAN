"use client";

import {
  GenerationSettings,
  GenerationStatusResponse,
  OutputResponse,
  SessionSummary,
  SuggestionResponse,
  TargetFieldSpec
} from "./types";

const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://127.0.0.1:8001";
const SESSION_STORAGE_KEY = "adp-demo-session-id";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    ...init,
    headers: {
      ...(init?.headers ?? {})
    },
    cache: "no-store"
  });

  if (!response.ok) {
    const contentType = response.headers.get("content-type") ?? "";
    let message = "";

    try {
      if (contentType.includes("application/json")) {
        const payload = (await response.json()) as { detail?: string; message?: string };
        message = payload.detail || payload.message || JSON.stringify(payload);
      } else {
        message = await response.text();
      }
    } catch {
      message = await response.text();
    }

    throw new Error(message || `Request failed with status ${response.status}`);
  }

  if (response.status === 204) {
    return undefined as T;
  }
  return (await response.json()) as T;
}

export function getStoredSessionId(): string | null {
  if (typeof window === "undefined") return null;
  return window.localStorage.getItem(SESSION_STORAGE_KEY);
}

export function setStoredSessionId(sessionId: string) {
  if (typeof window === "undefined") return;
  window.localStorage.setItem(SESSION_STORAGE_KEY, sessionId);
}

export function clearStoredSessionId() {
  if (typeof window === "undefined") return;
  window.localStorage.removeItem(SESSION_STORAGE_KEY);
}

export async function createSession() {
  return request<SessionSummary>("/api/sessions", { method: "POST" });
}

export async function fetchSession(sessionId: string) {
  return request<SessionSummary>(`/api/sessions/${sessionId}`);
}

export async function loadPreset(presetId: string) {
  return request<SessionSummary>(`/api/presets/${presetId}/load`, { method: "POST" });
}

export async function uploadSourceTables(sessionId: string, files: File[]) {
  const payload = {
    files: await Promise.all(
      files.map(async (file) => ({
        filename: file.name,
        content: await file.text()
      }))
    )
  };
  return request<SessionSummary>(`/api/sessions/${sessionId}/sources`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
}

export async function saveTargetSchema(sessionId: string, fields: TargetFieldSpec[]) {
  return request<SessionSummary>(`/api/sessions/${sessionId}/target-schema`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields })
  });
}

export async function saveTargetSamples(sessionId: string, rows: Record<string, unknown>[]) {
  return request<SessionSummary>(`/api/sessions/${sessionId}/target-samples`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ rows })
  });
}

export async function selectTargetTable(sessionId: string, targetTableId: string) {
  return request<SessionSummary>(`/api/sessions/${sessionId}/target-table`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ target_table_id: targetTableId })
  });
}

export async function createTargetTable(
  sessionId: string,
  payload: {
    name: string;
    description: string;
    schema: TargetFieldSpec[];
    existing_rows: Record<string, unknown>[];
  }
) {
  return request<SessionSummary>(`/api/sessions/${sessionId}/target-tables`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
}

export async function updateTargetTable(
  sessionId: string,
  targetTableId: string,
  payload: {
    name: string;
    description: string;
    schema: TargetFieldSpec[];
    existing_rows: Record<string, unknown>[];
  }
) {
  return request<SessionSummary>(`/api/sessions/${sessionId}/target-tables/${targetTableId}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
}

export async function deleteTargetTable(sessionId: string, targetTableId: string) {
  return request<SessionSummary>(`/api/sessions/${sessionId}/target-tables/${targetTableId}`, {
    method: "DELETE"
  });
}

export async function saveSettings(sessionId: string, settings: GenerationSettings) {
  return request<SessionSummary>(`/api/sessions/${sessionId}/settings`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ settings })
  });
}

export async function generatePipelines(sessionId: string) {
  return request<GenerationStatusResponse>(`/api/sessions/${sessionId}/generate`, {
    method: "POST"
  });
}

export async function fetchGenerationStatus(sessionId: string) {
  return request<GenerationStatusResponse>(`/api/sessions/${sessionId}/generation-status`);
}

export async function submitFeedback(sessionId: string, candidateId: string, nodeId: string, text: string) {
  return request<SessionSummary>(`/api/sessions/${sessionId}/feedback`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ candidate_id: candidateId, node_id: nodeId, text })
  });
}

export async function fetchLiveSuggestions(sessionId: string, candidateId: string, nodeId: string, text: string) {
  return request<SuggestionResponse>(`/api/sessions/${sessionId}/live-suggestions`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ candidate_id: candidateId, node_id: nodeId, text })
  });
}

export async function acceptPipeline(sessionId: string, candidateId: string) {
  return request<SessionSummary>(`/api/sessions/${sessionId}/accept`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ candidate_id: candidateId })
  });
}

export async function fetchOutput(sessionId: string) {
  return request<OutputResponse>(`/api/sessions/${sessionId}/output`);
}

export function exportUrl(sessionId: string, format: "csv" | "python" | "target_table" | "all") {
  return `${API_BASE_URL}/api/sessions/${sessionId}/export?format=${format}`;
}
