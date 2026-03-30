"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";

import { DataTable } from "@/components/data-table";
import { exportUrl, fetchOutput } from "@/lib/api";
import { ensureSessionSummary } from "@/lib/session";
import { OutputResponse, Session } from "@/lib/types";

function statusBadgeClass(isReady: boolean) {
  return isReady ? "bg-emerald-50 text-emerald-700" : "bg-amber-50 text-amber-700";
}

function schemaStatusClass(status: string) {
  return status === "Matched"
    ? "bg-emerald-100 text-emerald-700"
    : "bg-red-100 text-red-700";
}

export default function OutputPage() {
  const router = useRouter();
  const [session, setSession] = useState<Session | null>(null);
  const [output, setOutput] = useState<OutputResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState("");
  const exportOptions = [
    {
      title: "Transformed Table",
      format: "csv" as const,
    },
    {
      title: "Add to Target",
      format: "target_table" as const,
    },
    {
      title: "Python Script",
      format: "python" as const,
    },
  ];

  useEffect(() => {
    ensureSessionSummary()
      .then(async (summary) => {
        setSession(summary.session);
        if (!summary.session.accepted_candidate_id) {
          setIsLoading(false);
          return;
        }
        const response = await fetchOutput(summary.session.id);
        setOutput(response);
        setIsLoading(false);
      })
      .catch((err) => {
        setIsLoading(false);
        setError(err instanceof Error ? err.message : "Failed to load the output page.");
      });
  }, []);

  return (
    <div className="flex h-full min-h-0 flex-col gap-4 overflow-hidden">
      <div className="flex items-start justify-between gap-4">
        <div>
          <h1 className="text-4xl font-semibold tracking-tight text-slate-900">Output</h1>
          <p className="mt-2 text-lg text-slate-600">
            Review the final result, confirm the schema match, and export the accepted output.
          </p>
        </div>
        <div className="flex items-center gap-3">
          <button className="soft-button" onClick={() => router.push("/review")}>
            Back to Review
          </button>
          {session?.accepted_candidate_id ? (
            <a className="primary-button" href={exportUrl(session.id, "csv")}>
              Download
            </a>
          ) : null}
        </div>
      </div>

      {error ? <div className="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">{error}</div> : null}

      {isLoading ? (
        <section className="card flex min-h-0 flex-1 items-center justify-center p-8">
          <div className="text-lg text-slate-500">Preparing output...</div>
        </section>
      ) : null}

      {!isLoading && session && !session.accepted_candidate_id ? (
        <section className="card flex min-h-0 flex-1 items-center justify-center overflow-hidden p-8">
          <div className="max-w-xl text-center">
            <div className="mx-auto flex h-16 w-16 items-center justify-center rounded-full bg-blue-50 text-blue-600">
              <svg width="28" height="28" viewBox="0 0 24 24" fill="none" aria-hidden="true">
                <path d="M12 5v7l4 2" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
                <circle cx="12" cy="12" r="8" stroke="currentColor" strokeWidth="1.8" />
              </svg>
            </div>
            <h2 className="mt-6 text-[2rem] font-semibold text-slate-900">No accepted pipeline yet</h2>
            <p className="mt-3 text-base leading-7 text-slate-600">
              Open Review, inspect the generated pipeline, and run the accepted candidate to unlock output and export.
            </p>
            <div className="mt-6">
              <button className="primary-button" onClick={() => router.push("/review")}>
                Go to Review
              </button>
            </div>
          </div>
        </section>
      ) : null}

      {!isLoading && output ? (
        <div className="grid min-h-0 flex-1 grid-cols-[minmax(0,1.62fr)_minmax(360px,0.98fr)] gap-4 overflow-hidden">
          <div className="grid min-h-0 gap-4 overflow-hidden grid-rows-[auto_minmax(0,1fr)_minmax(0,1fr)]">
            <section className="card p-6">
              <div className="flex items-start justify-between gap-4">
                <div>
                  <h2 className="text-[2rem] font-medium text-slate-900">Output Summary</h2>
                  <p className="mt-2 text-sm text-slate-500">
                    The accepted result is available for validation, export, and downstream use.
                  </p>
                </div>
                <div className={`rounded-full px-4 py-2 text-sm font-medium ${statusBadgeClass(output.export_ready)}`}>
                  {output.export_ready ? "Ready to export" : "Not ready"}
                </div>
              </div>

              <div className="mt-6 grid gap-4 sm:grid-cols-4">
                <div className="rounded-2xl bg-slate-50 p-4">
                  <div className="text-sm text-slate-500">Rows</div>
                  <div className="mt-1 text-3xl font-semibold text-slate-900">{output.schema_check.row_count ?? 0}</div>
                </div>
                <div className="rounded-2xl bg-slate-50 p-4">
                  <div className="text-sm text-slate-500">Columns</div>
                  <div className="mt-1 text-3xl font-semibold text-slate-900">{output.schema_check.column_count ?? 0}</div>
                </div>
                <div className="rounded-2xl bg-slate-50 p-4">
                  <div className="text-sm text-slate-500">Schema Match</div>
                  <div className="mt-1 text-3xl font-semibold text-slate-900">
                    {output.schema_check.field_checks.filter((item) => item.status === "Matched").length} / {output.schema_check.field_checks.length}
                  </div>
                </div>
                <div className="rounded-2xl bg-slate-50 p-4">
                  <div className="text-sm text-slate-500">Export Status</div>
                  <div className={`mt-1 text-3xl font-semibold ${output.export_ready ? "text-emerald-700" : "text-amber-700"}`}>
                    {output.export_ready ? "Ready" : "Not ready"}
                  </div>
                </div>
              </div>
            </section>

            <section className="card flex min-h-0 flex-col overflow-hidden p-6">
              <div className="flex items-start justify-between gap-4">
                <div>
                  <h2 className="text-[2rem] font-medium text-slate-900">Final Output Table</h2>
                  <p className="mt-2 text-sm text-slate-500">Preview of the transformed rows that match the target schema</p>
                </div>
              </div>

              <div className="mt-5 min-h-0 flex-1 overflow-auto">
                <DataTable rows={output.final_preview_rows} />
              </div>

              <div className="mt-4 border-t border-slate-200 pt-4 text-sm text-slate-500">
                The exported output will preserve this column order.
              </div>
            </section>

            <section className="card flex min-h-0 flex-col overflow-hidden p-6">
              <h2 className="text-[2rem] font-medium text-slate-900">Target Schema Check</h2>
              <p className="mt-2 text-sm text-slate-500">
                Confirm that the final output structure matches the expected target schema.
              </p>

              <div className="mt-5 min-h-0 flex-1 overflow-auto">
                <div className="table-shell h-full">
                  <table>
                    <thead>
                      <tr>
                        <th>Field Name</th>
                        <th>Type</th>
                        <th>Status</th>
                      </tr>
                    </thead>
                    <tbody>
                      {output.schema_check.field_checks.map((check) => (
                        <tr key={check.field_name}>
                          <td>{check.field_name}</td>
                          <td>{check.expected_type}</td>
                          <td>
                            <span className={`rounded-full px-3 py-1 text-xs font-semibold ${schemaStatusClass(check.status)}`}>
                              {check.status}
                            </span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </section>
          </div>

          <div className="grid min-h-0 gap-4 overflow-hidden grid-rows-[minmax(0,1.28fr)_minmax(0,0.72fr)]">
            <section className="card flex min-h-0 flex-col overflow-hidden p-6">
              <h2 className="text-[2rem] font-medium text-slate-900">Export</h2>
              <p className="mt-2 text-sm text-slate-500">Choose how to use the final result</p>

              <div className="mt-6 min-h-0 flex-1 overflow-auto">
                {session ? (
                  <div className="space-y-3">
                    {exportOptions.map((item) => (
                      <div key={item.format} className="rounded-[24px] border border-slate-200 bg-slate-50 px-5 py-5">
                        <div className="flex items-center justify-between gap-4">
                          <div className="min-w-0">
                            <div className="text-[1.15rem] font-semibold text-slate-900">{item.title}</div>
                          </div>
                          <a
                            aria-label={`Download ${item.title}`}
                            className="soft-button flex h-11 w-11 shrink-0 items-center justify-center !px-0"
                            href={exportUrl(session.id, item.format)}
                            title={`Download ${item.title}`}
                          >
                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" aria-hidden="true">
                              <path
                                d="M12 4v10m0 0 4-4m-4 4-4-4M5 19h14"
                                stroke="currentColor"
                                strokeWidth="1.8"
                                strokeLinecap="round"
                                strokeLinejoin="round"
                              />
                            </svg>
                          </a>
                        </div>
                      </div>
                    ))}
                  </div>
                ) : null}
              </div>

              <div className={`mt-4 flex gap-3 rounded-2xl px-4 py-4 text-sm ${output.export_ready ? "bg-emerald-50 text-emerald-800" : "bg-amber-50 text-amber-800"}`}>
                <span className="mt-0.5" aria-hidden="true">
                  i
                </span>
                <span>{output.export_ready ? "This accepted pipeline passed the current output checks." : "This accepted pipeline still has output warnings, but exports remain available."}</span>
              </div>
            </section>

            <section className="card flex min-h-0 flex-col overflow-hidden p-6">
              <h2 className="text-[2rem] font-medium text-slate-900">Pipeline Summary</h2>
              <p className="mt-2 text-sm text-slate-500">Concise explanation of how the accepted pipeline transforms the data</p>

              <div className="mt-5 min-h-0 flex-1 overflow-auto text-sm leading-7 text-slate-700">
                <p>{output.candidate.summary || "No pipeline summary is available yet."}</p>
              </div>

              <div className="mt-5 border-t border-slate-200 pt-4 text-sm text-slate-500">
                This summary is for review only and is not part of the exported data.
              </div>
            </section>
          </div>
        </div>
      ) : null}
    </div>
  );
}
