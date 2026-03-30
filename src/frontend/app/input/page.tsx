"use client";

import { ChangeEvent, useEffect, useMemo, useRef, useState, useTransition } from "react";
import { useRouter } from "next/navigation";

import { DataTable } from "@/components/data-table";
import {
  createTargetTable,
  deleteTargetTable,
  generatePipelines,
  loadPreset,
  selectTargetTable,
  setStoredSessionId,
  uploadSourceTables,
  updateTargetTable
} from "@/lib/api";
import { ensureSessionSummary } from "@/lib/session";
import {
  DataType,
  PresetSummary,
  Session,
  SessionSummary,
  SourceTableSpec,
  TargetFieldSpec,
  TargetTableSpec
} from "@/lib/types";

const DATA_TYPE_OPTIONS: DataType[] = ["STRING", "INTEGER", "FLOAT", "DATE", "DATETIME", "BOOLEAN"];
type TableModalMode = "create" | "edit";

function emptyTargetField(): TargetFieldSpec {
  return {
    name: "",
    type: "STRING",
    description: "",
    required: true
  };
}

function normalizeSchema(schema: TargetFieldSpec[]): TargetFieldSpec[] {
  return schema
    .map((field) => ({
      name: field.name.trim(),
      type: field.type,
      description: field.description.trim(),
      required: field.required
    }))
    .filter((field) => field.name);
}

function emptyExampleRow(schema: TargetFieldSpec[]): Record<string, string> {
  return Object.fromEntries(schema.map((field) => [field.name, ""]));
}

function normalizeRows(schema: TargetFieldSpec[], rows: Record<string, string>[]) {
  const fieldNames = schema.map((field) => field.name);
  return rows
    .map((row) => Object.fromEntries(fieldNames.map((fieldName) => [fieldName, row[fieldName] ?? ""])))
    .filter((row) => fieldNames.some((fieldName) => String(row[fieldName] ?? "").trim() !== ""));
}

function trimStoredRows(rows: Record<string, unknown>[], schema: TargetFieldSpec[]): Record<string, string>[] {
  const fieldNames = schema.map((field) => field.name);
  return rows.map((row) =>
    Object.fromEntries(fieldNames.map((fieldName) => [fieldName, String(row[fieldName] ?? "")]))
  );
}

function parseCsvText(text: string): string[][] {
  const rows: string[][] = [];
  let currentRow: string[] = [];
  let currentCell = "";
  let inQuotes = false;
  const normalized = text.replace(/^\uFEFF/, "");

  for (let index = 0; index < normalized.length; index += 1) {
    const character = normalized[index];

    if (character === "\"") {
      if (inQuotes && normalized[index + 1] === "\"") {
        currentCell += "\"";
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (character === "," && !inQuotes) {
      currentRow.push(currentCell);
      currentCell = "";
      continue;
    }

    if ((character === "\n" || character === "\r") && !inQuotes) {
      if (character === "\r" && normalized[index + 1] === "\n") {
        index += 1;
      }
      currentRow.push(currentCell);
      rows.push(currentRow);
      currentRow = [];
      currentCell = "";
      continue;
    }

    currentCell += character;
  }

  if (currentCell || currentRow.length) {
    currentRow.push(currentCell);
    rows.push(currentRow);
  }

  return rows.filter((row) => row.some((cell) => cell.trim() !== ""));
}

function normalizeCsvHeader(header: string, index: number) {
  const trimmed = header.trim();
  return trimmed || `column_${index + 1}`;
}

function filenameToTableName(filename: string) {
  const base = filename.replace(/\.[^.]+$/, "").trim() || "uploaded_target_table";
  return base.replace(/[^a-zA-Z0-9]+/g, "_").replace(/^_+|_+$/g, "").toLowerCase() || "uploaded_target_table";
}

function inferUploadedType(values: string[]): DataType {
  const normalizedValues = values.map((value) => value.trim()).filter(Boolean);
  if (!normalizedValues.length) return "STRING";

  const isBoolean = normalizedValues.every((value) => /^(true|false|yes|no)$/i.test(value));
  if (isBoolean) return "BOOLEAN";

  const isInteger = normalizedValues.every((value) => /^-?\d+$/.test(value));
  if (isInteger) return "INTEGER";

  const isFloat = normalizedValues.every((value) => /^-?\d+(\.\d+)?$/.test(value));
  if (isFloat) return "FLOAT";

  const isDate = normalizedValues.every((value) => /^\d{4}[-/]\d{2}[-/]\d{2}$/.test(value));
  if (isDate) return "DATE";

  const isDateTime = normalizedValues.every((value) =>
    /^\d{4}[-/]\d{2}[-/]\d{2}[ T]\d{2}:\d{2}(:\d{2})?$/.test(value)
  );
  if (isDateTime) return "DATETIME";

  return "STRING";
}

function buildDraftFromCsv(filename: string, text: string) {
  const rows = parseCsvText(text);
  if (!rows.length) {
    throw new Error("The CSV file is empty.");
  }

  const headers = rows[0].map((header, index) => normalizeCsvHeader(header, index));
  if (!headers.length) {
    throw new Error("The CSV file needs at least one column.");
  }

  const existingRows = rows.slice(1).map((row) =>
    Object.fromEntries(headers.map((header, index) => [header, row[index] ?? ""]))
  );
  const schema = headers.map((header) => ({
    name: header,
    type: inferUploadedType(existingRows.map((row) => String(row[header] ?? ""))),
    description: "",
    required: true
  })) satisfies TargetFieldSpec[];

  return {
    name: filenameToTableName(filename),
    description: "",
    schema: schema.length ? schema : [emptyTargetField()],
    rows: existingRows
  };
}

function TrashButton({ label, onClick }: { label: string; onClick: () => void }) {
  return (
    <button
      aria-label={label}
      className="inline-flex h-10 w-10 items-center justify-center rounded-full border border-rose-200 bg-rose-50 text-rose-600 transition hover:bg-rose-100 hover:text-rose-700"
      onClick={onClick}
      type="button"
    >
      <svg aria-hidden="true" fill="none" height="18" viewBox="0 0 24 24" width="18">
        <path
          d="M4 7h16M9.5 4h5M10 11v5m4-5v5M7.5 7l.7 10.1A2 2 0 0 0 10.2 19h3.6a2 2 0 0 0 2-1.9L16.5 7"
          stroke="currentColor"
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth="1.8"
        />
      </svg>
    </button>
  );
}

function PencilButton({ label, onClick, disabled = false }: { label: string; onClick: () => void; disabled?: boolean }) {
  return (
    <button
      aria-label={label}
      className="inline-flex h-12 w-12 items-center justify-center rounded-full border border-slate-200 bg-white text-slate-700 transition hover:border-slate-300 hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50"
      disabled={disabled}
      onClick={onClick}
      type="button"
    >
      <svg aria-hidden="true" fill="none" height="22" viewBox="0 0 24 24" width="22">
        <path
          d="M4 20h4.25L18.8 9.45a1.5 1.5 0 0 0 0-2.12l-2.13-2.12a1.5 1.5 0 0 0-2.12 0L4 15.75V20Z"
          stroke="currentColor"
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth="1.8"
        />
        <path d="m12.5 7.5 4 4" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="1.8" />
      </svg>
    </button>
  );
}

function DeleteIconButton({ label, onClick, disabled = false }: { label: string; onClick: () => void; disabled?: boolean }) {
  return (
    <button
      aria-label={label}
      className="inline-flex h-12 w-12 items-center justify-center rounded-full border border-slate-200 bg-white text-slate-700 transition hover:border-slate-300 hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50"
      disabled={disabled}
      onClick={onClick}
      type="button"
    >
      <svg aria-hidden="true" fill="none" height="22" viewBox="0 0 24 24" width="22">
        <path
          d="M4 7h16M9.5 4h5M10 11v5m4-5v5M7.5 7l.7 10.1A2 2 0 0 0 10.2 19h3.6a2 2 0 0 0 2-1.9L16.5 7"
          stroke="currentColor"
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth="1.8"
        />
      </svg>
    </button>
  );
}

function ReadOnlySchemaTable({ targetTable }: { targetTable: TargetTableSpec | null }) {
  if (!targetTable) {
    return <div className="rounded-2xl border border-dashed border-slate-200 p-6 text-sm text-slate-500">No target table selected.</div>;
  }

  return (
    <div className="table-shell h-full">
      <table>
        <thead>
          <tr>
            <th>Field Name</th>
            <th>Type</th>
            <th>Description</th>
            <th>Required</th>
          </tr>
        </thead>
        <tbody>
          {targetTable.schema.map((field) => (
            <tr key={field.name}>
              <td>{field.name}</td>
              <td>{field.type}</td>
              <td>{field.description || "No description"}</td>
              <td>{field.required ? "Required" : "Optional"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function InputPage() {
  const router = useRouter();
  const [session, setSession] = useState<Session | null>(null);
  const [presets, setPresets] = useState<PresetSummary[]>([]);
  const [selectedSourceId, setSelectedSourceId] = useState<string | null>(null);
  const [, setStatus] = useState("");
  const [error, setError] = useState("");
  const [isPresetPickerOpen, setIsPresetPickerOpen] = useState(false);
  const [isAddTableOpen, setIsAddTableOpen] = useState(false);
  const [tableModalMode, setTableModalMode] = useState<TableModalMode>("create");
  const [editingTargetTableId, setEditingTargetTableId] = useState<string | null>(null);
  const [draftTableName, setDraftTableName] = useState("");
  const [draftTableDescription, setDraftTableDescription] = useState("");
  const [draftSchema, setDraftSchema] = useState<TargetFieldSpec[]>([emptyTargetField(), emptyTargetField()]);
  const [draftRows, setDraftRows] = useState<Record<string, string>[]>([]);
  const [draftError, setDraftError] = useState("");
  const sourceUploadInputRef = useRef<HTMLInputElement | null>(null);
  const targetUploadInputRef = useRef<HTMLInputElement | null>(null);
  const [isPending, startTransition] = useTransition();

  const applySummary = (summary: SessionSummary) => {
    setStoredSessionId(summary.session.id);
    setSession(summary.session);
    setPresets(summary.available_presets);
    setSelectedSourceId(summary.session.source_tables[0]?.id ?? null);
    setError("");
  };

  useEffect(() => {
    ensureSessionSummary({ resetPresetDraft: true })
      .then((summary) => applySummary(summary))
      .catch((err) => setError(err instanceof Error ? err.message : "Failed to load the current session."));
  }, []);

  const selectedSource = useMemo<SourceTableSpec | null>(
    () => session?.source_tables.find((table) => table.id === selectedSourceId) ?? session?.source_tables[0] ?? null,
    [session, selectedSourceId]
  );
  const targetTables = session?.target_database?.tables ?? [];
  const selectedTargetTable = useMemo<TargetTableSpec | null>(
    () => targetTables.find((table) => table.id === session?.selected_target_table_id) ?? targetTables[0] ?? null,
    [session?.selected_target_table_id, targetTables]
  );
  const normalizedDraftSchema = useMemo(() => normalizeSchema(draftSchema), [draftSchema]);

  const resetAddTableDraft = () => {
    setDraftTableName("");
    setDraftTableDescription("");
    setDraftSchema([emptyTargetField(), emptyTargetField()]);
    setDraftRows([]);
    setDraftError("");
    setEditingTargetTableId(null);
    setTableModalMode("create");
  };

  const closeAddTableModal = () => {
    setIsAddTableOpen(false);
    resetAddTableDraft();
  };

  const openCreateTableModal = () => {
    resetAddTableDraft();
    setTableModalMode("create");
    setIsAddTableOpen(true);
  };

  const openEditTableModal = () => {
    if (!selectedTargetTable) return;
    setTableModalMode("edit");
    setEditingTargetTableId(selectedTargetTable.id);
    setDraftTableName(selectedTargetTable.name);
    setDraftTableDescription(selectedTargetTable.description ?? "");
    setDraftSchema(
      selectedTargetTable.schema.length
        ? selectedTargetTable.schema.map((field) => ({ ...field }))
        : [emptyTargetField(), emptyTargetField()]
    );
    setDraftRows(trimStoredRows(selectedTargetTable.existing_rows, selectedTargetTable.schema));
    setDraftError("");
    setIsAddTableOpen(true);
  };

  const handleLoadPreset = (presetId: string, presetName: string) => {
    startTransition(async () => {
      try {
        setStatus("Loading example...");
        setError("");
        const summary = await loadPreset(presetId);
        applySummary(summary);
        setIsPresetPickerOpen(false);
        setStatus(`${presetName} loaded.`);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to load the selected example.");
      }
    });
  };

  const handleSourceUploadClick = () => {
    sourceUploadInputRef.current?.click();
  };

  const handleSourceUpload = (event: ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(event.target.files ?? []);
    event.target.value = "";
    if (!files.length || !session) return;

    startTransition(async () => {
      try {
        setStatus(`Uploading ${files.length} source file${files.length > 1 ? "s" : ""}...`);
        setError("");
        const summary = await uploadSourceTables(session.id, files);
        applySummary(summary);
        setStatus(`${files.length} source file${files.length > 1 ? "s" : ""} uploaded.`);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to upload source files.");
      }
    });
  };

  const handleTargetTableChange = (targetTableId: string) => {
    if (!session || targetTableId === session.selected_target_table_id) return;
    startTransition(async () => {
      try {
        setStatus("Switching target table...");
        setError("");
        const summary = await selectTargetTable(session.id, targetTableId);
        setSession(summary.session);
        setStatus(
          `Target table switched to ${
            summary.session.target_database?.tables.find((table) => table.id === targetTableId)?.name ?? "the selected table"
          }.`
        );
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to switch target table.");
      }
    });
  };

  const handleContinue = () => {
    if (!session) return;
    startTransition(async () => {
      try {
        setError("");
        if (!session.source_tables.length) {
          setStatus("");
          setError("Load an example before continuing.");
          return;
        }
        if (!session.target_schema.length) {
          setStatus("");
          setError("Select a target table before continuing.");
          return;
        }
        setStatus("Preparing pipeline generation...");
        await generatePipelines(session.id);
        setSession((previous) =>
          previous
            ? { ...previous, status: "generating", status_message: "Generating pipeline...", last_error: "" }
            : previous
        );
        router.push("/review");
      } catch (err) {
        setError(err instanceof Error ? err.message : "Unable to continue to Review.");
      }
    });
  };

  const handleDraftFieldChange = (index: number, key: keyof TargetFieldSpec, value: string | boolean) => {
    setDraftError("");
    const previousName = draftSchema[index]?.name.trim() ?? "";
    const nextName = key === "name" ? String(value).trim() : previousName;

    setDraftSchema((current) =>
      current.map((field, fieldIndex) => (fieldIndex === index ? { ...field, [key]: value } : field))
    );

    if (key === "name") {
      setDraftRows((current) =>
        current.map((row) => {
          const nextRow = { ...row };
          const existingValue = previousName ? nextRow[previousName] ?? "" : "";
          if (previousName && previousName !== nextName) {
            delete nextRow[previousName];
          }
          if (nextName) {
            nextRow[nextName] = existingValue;
          }
          return nextRow;
        })
      );
    }
  };

  const handleRemoveDraftField = (index: number) => {
    const removedName = draftSchema[index]?.name.trim() ?? "";
    setDraftError("");
    setDraftSchema((current) => current.filter((_, fieldIndex) => fieldIndex !== index));
    if (removedName) {
      setDraftRows((current) =>
        current.map((row) => {
          const nextRow = { ...row };
          delete nextRow[removedName];
          return nextRow;
        })
      );
    }
  };

  const handleAddDraftField = () => {
    setDraftError("");
    setDraftSchema((current) => [...current, emptyTargetField()]);
  };

  const handleAddDraftRow = () => {
    if (!normalizedDraftSchema.length) {
      setDraftError("Add at least one named target schema field before adding example rows.");
      return;
    }
    setDraftError("");
    setDraftRows((current) => [...current, emptyExampleRow(normalizedDraftSchema)]);
  };

  const handleDraftRowValueChange = (rowIndex: number, columnName: string, value: string) => {
    setDraftError("");
    setDraftRows((current) =>
      current.map((row, index) => (index === rowIndex ? { ...row, [columnName]: value } : row))
    );
  };

  const handleRemoveDraftRow = (rowIndex: number) => {
    setDraftError("");
    setDraftRows((current) => current.filter((_, index) => index !== rowIndex));
  };

  const handleSaveTargetTable = () => {
    if (!session) return;

    const tableName = draftTableName.trim();
    const schema = normalizeSchema(draftSchema);
    if (!tableName) {
      setDraftError("Enter a target table name.");
      return;
    }
    if (!schema.length) {
      setDraftError("Add at least one target schema field.");
      return;
    }
    if (new Set(schema.map((field) => field.name)).size !== schema.length) {
      setDraftError("Field names must be unique.");
      return;
    }

    const rows = normalizeRows(schema, draftRows);

    startTransition(async () => {
      try {
        setStatus(tableModalMode === "edit" ? "Updating target table..." : "Adding target table...");
        setError("");
        setDraftError("");
        const payload = {
          name: tableName,
          description: draftTableDescription.trim(),
          schema,
          existing_rows: rows
        };
        const summary =
          tableModalMode === "edit" && editingTargetTableId
            ? await updateTargetTable(session.id, editingTargetTableId, payload)
            : await createTargetTable(session.id, payload);
        applySummary(summary);
        setStatus(tableModalMode === "edit" ? `${tableName} updated.` : `${tableName} added.`);
        closeAddTableModal();
      } catch (err) {
        setDraftError(err instanceof Error ? err.message : "Failed to save the target table.");
      }
    });
  };

  const handleTargetUploadClick = () => {
    targetUploadInputRef.current?.click();
  };

  const handleTargetUpload = (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    event.target.value = "";
    if (!file) return;

    startTransition(async () => {
      try {
        const content = await file.text();
        const draft = buildDraftFromCsv(file.name, content);
        setTableModalMode("create");
        setEditingTargetTableId(null);
        setDraftTableName(draft.name);
        setDraftTableDescription(draft.description);
        setDraftSchema(draft.schema);
        setDraftRows(draft.rows);
        setDraftError("");
        setError("");
        setStatus(`${file.name} loaded into a new target table draft.`);
        setIsAddTableOpen(true);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to read the CSV file.");
      }
    });
  };

  const handleDeleteTargetTable = () => {
    if (!session || !selectedTargetTable) return;
    const confirmed = window.confirm(`Delete target table "${selectedTargetTable.name}" from this session?`);
    if (!confirmed) return;

    startTransition(async () => {
      try {
        setStatus("Deleting target table...");
        setError("");
        const summary = await deleteTargetTable(session.id, selectedTargetTable.id);
        applySummary(summary);
        setStatus(`${selectedTargetTable.name} deleted.`);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to delete the target table.");
      }
    });
  };

  const uploadedCount = session?.source_tables.length ?? 0;
  const typeFooter =
    selectedSource?.columns.map((column) => selectedSource.inferred_types[column].toLowerCase()).join(", ") ?? "No file loaded yet";
  const selectedSourceRows = selectedSource?.rows?.length ? selectedSource.rows : selectedSource?.preview_rows ?? [];

  return (
    <div className="flex h-full min-h-0 flex-col gap-4 overflow-hidden">
      <div className="flex items-start justify-between gap-4">
        <div>
          <h1 className="text-4xl font-semibold tracking-tight text-slate-900">Input</h1>
          <p className="mt-2 text-lg text-slate-600">Choose source tables on the left and the target table on the right.</p>
        </div>
        <div className="flex items-center gap-3">
          <button className="primary-button" disabled={isPending} onClick={handleContinue}>
            Continue
          </button>
        </div>
      </div>

      {error ? <div className="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">{error}</div> : null}

      <div className="grid min-h-0 flex-1 gap-4 overflow-hidden xl:grid-cols-[minmax(0,1.02fr)_minmax(0,0.98fr)]">
        <section className="card flex min-h-0 flex-col overflow-hidden p-6">
          <div className="flex items-center justify-between gap-4">
            <div>
              <h2 className="text-[2rem] font-medium text-slate-900">Source Table</h2>
              <p className="mt-2 text-sm text-slate-500">Load a prepared example scenario and inspect the source tables it provides.</p>
            </div>
            <div className="flex shrink-0 items-center gap-3">
              <button className="soft-button shrink-0" disabled={isPending || !session} onClick={handleSourceUploadClick} type="button">
                Upload
              </button>
              <button className="primary-button shrink-0" onClick={() => setIsPresetPickerOpen(true)} type="button">
                Try Example
              </button>
            </div>
          </div>
          <input
            accept=".csv,text/csv"
            className="hidden"
            multiple
            onChange={handleSourceUpload}
            ref={sourceUploadInputRef}
            type="file"
          />

          <div className="mt-5 rounded-2xl border border-dashed border-slate-300 bg-slate-50 px-6 py-8 text-center">
            <div className="mx-auto flex h-12 w-12 items-center justify-center rounded-full bg-white text-slate-500 shadow-sm">
              <svg width="22" height="22" viewBox="0 0 24 24" fill="none" aria-hidden="true">
                <path d="M7 17.5h10a3.5 3.5 0 0 0 .7-6.93A5.5 5.5 0 0 0 7.5 8a4.5 4.5 0 0 0-.5 9Z" stroke="#64748b" strokeWidth="1.7" />
                <path d="M12 8.5v7m0-7 3 3m-3-3-3 3" stroke="#64748b" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" />
              </svg>
            </div>
            <div className="mt-4 text-lg text-slate-600">Start from a prepared example and inspect the source tables it loads.</div>
          </div>

          <div className="mt-4 flex flex-wrap items-center gap-3">
            {session?.source_tables.map((table) => (
              <button
                key={table.id}
                className={`rounded-full px-3 py-1.5 text-sm font-medium transition ${
                  table.id === selectedSourceId
                    ? "bg-blue-100 text-blue-700"
                    : "border border-slate-200 bg-slate-50 text-slate-600 hover:border-blue-200"
                }`}
                onClick={() => setSelectedSourceId(table.id)}
                type="button"
              >
                {table.filename}
              </button>
            ))}
            {uploadedCount ? (
              <div className="ml-auto flex items-center gap-2 text-sm font-medium text-emerald-700">
                <span className="flex h-5 w-5 items-center justify-center rounded-full border border-emerald-200 bg-emerald-50 text-[10px]">
                  OK
                </span>
                {uploadedCount} file{uploadedCount > 1 ? "s" : ""} loaded
              </div>
            ) : null}
          </div>

          <div className="mt-4 text-sm font-medium text-slate-500">Source Data</div>
          <div className="mt-2 min-h-0 flex-1 overflow-auto">
            <DataTable rows={selectedSourceRows} emptyMessage="Load a prepared example to display the source table." />
          </div>

          <div className="mt-4 border-t border-slate-200 pt-4 text-sm text-slate-500">Detected types: {typeFooter}</div>
        </section>

        <section className="card flex min-h-0 flex-col overflow-hidden p-6">
          <div className="flex items-start justify-between gap-4">
            <div>
              <h2 className="text-[2rem] font-medium text-slate-900">Target</h2>
              <p className="mt-2 text-sm text-slate-500">Select an existing target table or add a new one for this session.</p>
            </div>
            <div className="flex shrink-0 items-center gap-3">
              <button className="soft-button shrink-0" disabled={isPending || !session} onClick={handleTargetUploadClick} type="button">
                Upload
              </button>
              <button className="soft-button shrink-0" disabled={isPending} onClick={openCreateTableModal} type="button">
                Add Table
              </button>
            </div>
          </div>
          <input accept=".csv,text/csv" className="hidden" onChange={handleTargetUpload} ref={targetUploadInputRef} type="file" />

          <div className="mt-5 flex items-center gap-3 border-b border-slate-200 pb-4">
            <div className="flex min-w-0 flex-1 gap-3 overflow-x-auto">
              {targetTables.map((table) => (
                <button
                  key={table.id}
                  className={`shrink-0 rounded-full px-4 py-2 text-sm font-medium transition ${
                    table.id === session?.selected_target_table_id
                      ? "bg-slate-900 text-white"
                      : "border border-slate-200 bg-slate-50 text-slate-600 hover:border-slate-300 hover:bg-white"
                  }`}
                  onClick={() => handleTargetTableChange(table.id)}
                  type="button"
                >
                  {table.name}
                </button>
              ))}
            </div>
            <div className="flex shrink-0 items-center gap-3">
              <PencilButton disabled={isPending || !selectedTargetTable} label="Edit target table" onClick={openEditTableModal} />
              <DeleteIconButton
                disabled={isPending || !selectedTargetTable}
                label="Delete target table"
                onClick={handleDeleteTargetTable}
              />
            </div>
          </div>

          <div className="mt-5 text-sm text-slate-500">
            {selectedTargetTable ? selectedTargetTable.description || "No description provided for this target table." : "No target table selected."}
          </div>

          <div className="mt-5 grid min-h-0 flex-1 gap-4 overflow-hidden xl:grid-rows-[minmax(0,0.52fr)_minmax(0,0.48fr)]">
            <div className="flex min-h-0 flex-col overflow-hidden rounded-[24px] border border-slate-200 bg-slate-50 p-4">
              <div className="mb-3 text-base font-semibold text-slate-900">Target Schema</div>
              <div className="min-h-0 flex-1 overflow-hidden">
                <ReadOnlySchemaTable targetTable={selectedTargetTable} />
              </div>
            </div>

            <div className="min-h-0 overflow-hidden rounded-[24px] border border-slate-200 bg-slate-50 p-4">
              <div className="mb-3 flex items-center justify-between gap-3">
                <div>
                  <div className="text-base font-semibold text-slate-900">Existing Target Rows</div>
                  <div className="mt-1 text-sm text-slate-500">These rows are treated as current target table data and will be used for similarity checks.</div>
                </div>
                <div className="shrink-0 rounded-full bg-slate-100 px-4 py-2 text-sm text-slate-600">
                  {selectedTargetTable?.existing_rows.length ? `${selectedTargetTable.existing_rows.length} existing row(s)` : "Schema only"}
                </div>
              </div>
              <div className="min-h-0 overflow-auto">
                <DataTable
                  rows={selectedTargetTable?.existing_rows ?? []}
                  emptyMessage="This target table currently has no existing rows. Generation will rely on schema-only validation."
                />
              </div>
            </div>
          </div>
        </section>
      </div>

      {isPresetPickerOpen ? (
        <div className="fixed inset-0 z-[120] flex items-center justify-center bg-slate-950/25 px-6 backdrop-blur-[2px]">
          <div className="w-full max-w-2xl rounded-[30px] border border-slate-200 bg-white p-6 shadow-[0_24px_64px_rgba(15,23,42,0.18)]">
            <div className="flex items-start justify-between gap-4">
              <div>
                <h2 className="text-[2rem] font-medium text-slate-900">Try Example</h2>
                <p className="mt-2 text-sm text-slate-500">Load a prepared BAT scenario into the current Input page.</p>
              </div>
              <button className="soft-button" onClick={() => setIsPresetPickerOpen(false)} type="button">
                Close
              </button>
            </div>

            <div className="mt-5 grid gap-3">
              {presets.map((preset) => (
                <button
                  key={preset.id}
                  className="rounded-2xl border border-slate-200 bg-slate-50 p-5 text-left transition hover:border-blue-200 hover:bg-blue-50"
                  onClick={() => handleLoadPreset(preset.id, preset.name)}
                  type="button"
                >
                  <div className="text-lg font-semibold text-slate-900">{preset.name}</div>
                  <p className="mt-2 text-sm leading-6 text-slate-600">{preset.description}</p>
                </button>
              ))}
            </div>
          </div>
        </div>
      ) : null}

      {isAddTableOpen ? (
        <div className="fixed inset-0 z-[120] flex items-center justify-center bg-slate-950/25 px-6 backdrop-blur-[2px]">
          <div className="flex max-h-[92vh] w-full max-w-6xl flex-col overflow-hidden rounded-[30px] border border-slate-200 bg-white p-6 shadow-[0_24px_64px_rgba(15,23,42,0.18)]">
            <div className="flex items-start justify-between gap-4">
              <div>
                <h2 className="text-[2rem] font-medium text-slate-900">
                  {tableModalMode === "edit" ? "Edit Target Table" : "Add Target Table"}
                </h2>
                <p className="mt-2 text-sm text-slate-500">Define the target schema and optional example rows for this session.</p>
              </div>
              <div className="flex items-center gap-3">
                <button className="soft-button" onClick={closeAddTableModal} type="button">
                  Cancel
                </button>
                <button className="primary-button" disabled={isPending} onClick={handleSaveTargetTable} type="button">
                  Save Table
                </button>
              </div>
            </div>

            {draftError ? <div className="mt-4 rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">{draftError}</div> : null}

            <div className="mt-5 min-h-0 flex-1 overflow-auto">
              <section className="rounded-[24px] border border-slate-200 bg-slate-50 p-5">
                <div className="mb-2 text-sm font-medium text-slate-600">Table Name</div>
                <input
                  className="field"
                  placeholder="e.g. store_weekly_totals"
                  value={draftTableName}
                  onChange={(event) => {
                    setDraftTableName(event.target.value);
                    setDraftError("");
                  }}
                />
              </section>

              <section className="mt-4 rounded-[24px] border border-slate-200 bg-slate-50 p-5">
                <div className="flex items-center justify-between gap-3">
                  <div className="text-base font-semibold text-slate-900">Schema</div>
                  <button className="soft-button" onClick={handleAddDraftField} type="button">
                    Add Column
                  </button>
                </div>
                <div className="mt-4 overflow-auto">
                  <div className="table-shell">
                    <table>
                      <thead>
                        <tr>
                          <th className="min-w-[220px]">Field Name</th>
                          <th className="min-w-[160px]">Type</th>
                          <th className="min-w-[280px]">Description</th>
                          <th className="min-w-[120px]">Required</th>
                          <th className="w-[84px]" />
                        </tr>
                      </thead>
                      <tbody>
                        {draftSchema.map((field, index) => (
                          <tr key={`draft-field-${index}`}>
                            <td>
                              <input
                                className="field"
                                placeholder="Field name"
                                value={field.name}
                                onChange={(event) => handleDraftFieldChange(index, "name", event.target.value)}
                              />
                            </td>
                            <td>
                              <select
                                className="field"
                                value={field.type}
                                onChange={(event) => handleDraftFieldChange(index, "type", event.target.value)}
                              >
                                {DATA_TYPE_OPTIONS.map((option) => (
                                  <option key={option} value={option}>
                                    {option}
                                  </option>
                                ))}
                              </select>
                            </td>
                            <td>
                              <input
                                className="field"
                                placeholder="Description"
                                value={field.description}
                                onChange={(event) => handleDraftFieldChange(index, "description", event.target.value)}
                              />
                            </td>
                            <td>
                              <div className="flex justify-center">
                                <input
                                  checked={field.required}
                                  onChange={(event) => handleDraftFieldChange(index, "required", event.target.checked)}
                                  type="checkbox"
                                />
                              </div>
                            </td>
                            <td>
                              <div className="flex justify-center">
                                <TrashButton label={`Remove ${field.name || `field ${index + 1}`}`} onClick={() => handleRemoveDraftField(index)} />
                              </div>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              </section>

              <section className="mt-4 rounded-[24px] border border-slate-200 bg-slate-50 p-5">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <div className="text-base font-semibold text-slate-900">Existing Target Rows</div>
                    <div className="mt-1 text-sm text-slate-500">Optional example rows that help similarity checks understand the target style.</div>
                  </div>
                  <button className="soft-button" onClick={handleAddDraftRow} type="button">
                    Add Example Row
                  </button>
                </div>

                {normalizedDraftSchema.length ? (
                  <div className="mt-4 overflow-auto">
                    <div className="table-shell">
                      <table>
                        <thead>
                          <tr>
                            {normalizedDraftSchema.map((field) => (
                              <th key={field.name}>{field.name}</th>
                            ))}
                            <th />
                          </tr>
                        </thead>
                        <tbody>
                          {draftRows.length ? (
                            draftRows.map((row, rowIndex) => (
                              <tr key={`draft-row-${rowIndex}`}>
                                {normalizedDraftSchema.map((field) => (
                                  <td key={`${rowIndex}-${field.name}`}>
                                    <input
                                      className="field"
                                      placeholder={field.name}
                                      value={row[field.name] ?? ""}
                                      onChange={(event) => handleDraftRowValueChange(rowIndex, field.name, event.target.value)}
                                    />
                                  </td>
                                ))}
                                <td>
                                  <div className="flex justify-center">
                                    <TrashButton label={`Remove example row ${rowIndex + 1}`} onClick={() => handleRemoveDraftRow(rowIndex)} />
                                  </div>
                                </td>
                              </tr>
                            ))
                          ) : (
                            <tr>
                              <td className="text-sm text-slate-500" colSpan={normalizedDraftSchema.length + 1}>
                                No example rows yet. You can leave this empty if you only want to define the schema.
                              </td>
                            </tr>
                          )}
                        </tbody>
                      </table>
                    </div>
                  </div>
                ) : (
                  <div className="mt-4 rounded-2xl border border-dashed border-slate-200 px-4 py-6 text-sm text-slate-500">
                    Add at least one named schema field before entering existing target rows.
                  </div>
                )}
              </section>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
