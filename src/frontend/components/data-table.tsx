"use client";

export function DataTable({
  rows,
  emptyMessage = "No rows to display."
}: {
  rows: Record<string, unknown>[];
  emptyMessage?: string;
}) {
  if (!rows.length) {
    return <div className="rounded-2xl border border-dashed border-slate-200 p-6 text-sm text-slate-500">{emptyMessage}</div>;
  }

  const columns = Object.keys(rows[0]);

  return (
    <div className="table-shell">
      <table>
        <thead>
          <tr>
            {columns.map((column) => (
              <th key={column}>{column}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, index) => (
            <tr key={index}>
              {columns.map((column) => (
                <td key={column}>{String(row[column] ?? "")}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
