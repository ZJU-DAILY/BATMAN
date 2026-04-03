import {
  CandidatePipeline,
  NodeAssessment,
  PipelineStep,
  Session,
  SourceTableSpec,
  StepPreview
} from "@/lib/types";

export type ReviewDisplayNode = {
  id: string;
  kind: "input" | "step" | "output";
  label: string;
  subtitle: string;
  statsLine: string;
  status: "ok" | "issue";
  explanation: string;
  assessmentReason: string;
  previewRows: Record<string, unknown>[];
  editableStepId: string | null;
  coveredStepIds: string[];
  previewStepId: string | null;
  detailLines: string[];
  sourceTable?: SourceTableSpec;
};

export type ReviewDisplayEdge = {
  id: string;
  from: string;
  to: string;
};

export type ReviewDisplayGraph = {
  nodes: ReviewDisplayNode[];
  edges: ReviewDisplayEdge[];
  displayNodeIdByRawNodeId: Record<string, string>;
};

type ReviewGraphContext = {
  candidate: CandidatePipeline;
  session: Session;
  orderedSteps: PipelineStep[];
  stepById: Map<string, PipelineStep>;
  stepPreviewById: Map<string, StepPreview>;
  sourceTableByName: Map<string, SourceTableSpec>;
  sourceTableById: Map<string, SourceTableSpec>;
  rawParentsByStepId: Map<string, string[]>;
  rawChildrenByStepId: Map<string, string[]>;
  directSourceTableNamesByStepId: Map<string, string[]>;
  rawIndexByStepId: Map<string, number>;
  assessmentByNodeId: Map<string, NodeAssessment>;
  finalProducerId: string | null;
};

function asRecord(value: unknown): Record<string, unknown> {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  return {};
}

function asStringValue(value: unknown): string | null {
  if (typeof value === "string") {
    const trimmed = value.trim();
    return trimmed || null;
  }
  if (value === null || value === undefined) {
    return null;
  }
  const rendered = String(value).trim();
  return rendered || null;
}

function asStringList(value: unknown): string[] {
  if (Array.isArray(value)) {
    return value.map((item) => String(item)).filter(Boolean);
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    return trimmed ? [trimmed] : [];
  }
  return [];
}

function normalizeIdentifier(value: string | null | undefined) {
  return (value ?? "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function quote(value: string) {
  return `"${value}"`;
}

function formatList(values: string[], limit = 4) {
  if (!values.length) return "nothing";
  const visible = values.slice(0, limit).map(quote).join(", ");
  if (values.length > limit) {
    return `${visible} +${values.length - limit} more`;
  }
  return visible;
}

function pluralize(count: number | null | undefined, singular: string, plural = `${singular}s`) {
  const safeCount = typeof count === "number" && Number.isFinite(count) ? count : 0;
  return `${safeCount} ${safeCount === 1 ? singular : plural}`;
}


function displayTableKind(kind: "input" | "step" | "output") {
  if (kind === "input") {
    return "Source table";
  }
  if (kind === "output") {
    return "Final table";
  }
  return "Intermediate table";
}

function displayStatsLine(rowCount?: number | null, columnCount?: number | null) {
  const parts: string[] = [];
  if (typeof rowCount === "number") {
    parts.push(pluralize(rowCount, "row"));
  }
  if (typeof columnCount === "number") {
    parts.push(pluralize(columnCount, "column"));
  }
  return parts.join(", ");
}

function aggregationLabel(func: string | null | undefined) {
  switch ((func ?? "").trim().toLowerCase()) {
    case "sum":
      return "sum";
    case "avg":
    case "mean":
      return "average";
    case "count":
    case "size":
      return "count";
    case "nunique":
      return "unique count";
    case "min":
      return "minimum";
    case "max":
      return "maximum";
    case "first":
      return "first value";
    case "last":
      return "last value";
    default: {
      const normalized = (func ?? "").trim().replace(/_/g, " ");
      return normalized || "values";
    }
  }
}

function groupbyLabel(step: PipelineStep) {
  const aggregations = Array.isArray(step.params.aggregations) ? step.params.aggregations : [];
  const uniqueLabels = Array.from(
    new Set(
      aggregations
        .map((aggregation) => aggregationLabel(asStringValue(asRecord(aggregation).func)))
        .filter(Boolean)
    )
  );
  if (uniqueLabels.length === 1) {
    return `Calculate ${uniqueLabels[0]}`;
  }
  return "Calculate values";
}

function groupbySummaryLine(keys: string[], summaryLabel: string) {
  if (keys.length) {
    if (summaryLabel === "values") {
      return `Calculate values for each group defined by ${formatList(keys)}.`;
    }
    return `Calculate one ${summaryLabel} for each group defined by ${formatList(keys)}.`;
  }
  return summaryLabel === "values" ? "Calculate values at the needed level." : `Calculate ${summaryLabel} at the needed level.`;
}

function aggregationVerb(func: string | null | undefined, source: string) {
  switch ((func ?? "").trim().toLowerCase()) {
    case "sum":
      return `by adding up ${quote(source)}`;
    case "avg":
    case "mean":
      return `by averaging ${quote(source)}`;
    case "count":
    case "size":
      return `by counting ${quote(source)}`;
    case "nunique":
      return `by counting unique values in ${quote(source)}`;
    case "min":
      return `by taking the smallest value from ${quote(source)}`;
    case "max":
      return `by taking the largest value from ${quote(source)}`;
    case "first":
      return `by taking the first value from ${quote(source)}`;
    case "last":
      return `by taking the last value from ${quote(source)}`;
    default: {
      const label = aggregationLabel(func);
      return `with ${label} from ${quote(source)}`;
    }
  }
}

function displayLabelForStep(step: PipelineStep) {
  switch (step.operator) {
    case "rename":
      return "Rename columns";
    case "drop_columns":
      return "Remove columns";
    case "date_formatting":
      return "Change date format";
    case "groupby":
      return groupbyLabel(step);
    case "join":
      return "Match tables";
    case "union":
      return "Combine tables";
    case "pivot":
      return "Create columns";
    case "unpivot":
      return "Create rows";
    case "column_arithmetic":
      return "Calculate value";
    case "add_columns":
      return "Add column";
    default:
      return step.operator.replace(/_/g, " ");
  }
}

function fallbackExplanation(kind: "input" | "step" | "output", step: PipelineStep | null, label: string) {
  if (kind === "input") {
    return "This source table feeds records into the flow shown here.";
  }
  if (kind === "output") {
    return "This is the final table that will be checked and exported.";
  }
  switch (step?.operator) {
    case "rename":
      return "This step renames columns so later steps can use the expected names.";
    case "drop_columns":
      return "This step keeps only the columns that still matter downstream.";
    case "date_formatting":
      return "This step rewrites date values into the expected format.";
    case "groupby":
      return "This step calculates grouped values and keeps one result row for each group.";
    case "join":
      return "This step matches rows across tables and brings in extra information.";
    case "union":
      return "This step combines compatible rows from more than one table.";
    case "pivot":
      return "This step spreads repeated values into separate columns.";
    case "unpivot":
      return "This step turns selected columns into rows.";
    case "column_arithmetic":
      return "This step calculates a value from other columns in the same row.";
    case "add_columns":
      return "This step adds a new column or updates an existing one.";
    default:
      return `This step updates the working table during ${label.toLowerCase()}.`;
  }
}

function friendlyOkReason(kind: "input" | "step" | "output", label: string) {
  if (kind === "input") {
    return "This source table is ready for the steps that depend on it.";
  }
  if (kind === "output") {
    return "The final table is ready to review.";
  }
  if (label === "Match tables") {
    return "The matched table looks consistent after bringing in the extra information.";
  }
  return "This step looks consistent with the current result.";
}

function arraysEqual(left: string[], right: string[]) {
  if (left.length !== right.length) return false;
  return left.every((value, index) => value === right[index]);
}

function buildRawContext(session: Session, candidate: CandidatePipeline): ReviewGraphContext {
  const allSteps = candidate.pipeline_spec.steps;
  const rawParentsAll = new Map<string, string[]>();
  const directSourceTableNamesAll = new Map<string, string[]>();
  const latestProducerByOutput = new Map<string, string>();
  const sourceTableNames = new Set(session.source_tables.map((table) => table.name));

  allSteps.forEach((step) => {
    const parents: string[] = [];
    const directSources: string[] = [];
    step.inputs.forEach((input) => {
      const producerId = latestProducerByOutput.get(input);
      if (producerId) {
        parents.push(producerId);
      } else if (sourceTableNames.has(input)) {
        directSources.push(input);
      }
    });
    rawParentsAll.set(step.step_id, Array.from(new Set(parents)));
    directSourceTableNamesAll.set(step.step_id, Array.from(new Set(directSources)));
    latestProducerByOutput.set(step.output, step.step_id);
  });

  const finalProducerId = latestProducerByOutput.get(candidate.pipeline_spec.final_table) ?? allSteps[allSteps.length - 1]?.step_id ?? null;
  const usedStepIds = new Set<string>();
  if (finalProducerId) {
    const stack = [finalProducerId];
    while (stack.length) {
      const current = stack.pop();
      if (!current || usedStepIds.has(current)) continue;
      usedStepIds.add(current);
      (rawParentsAll.get(current) ?? []).forEach((parentId) => stack.push(parentId));
    }
  } else {
    allSteps.forEach((step) => usedStepIds.add(step.step_id));
  }

  const orderedSteps = allSteps.filter((step) => usedStepIds.has(step.step_id));
  const rawParentsByStepId = new Map<string, string[]>();
  const rawChildrenByStepId = new Map<string, string[]>();
  const directSourceTableNamesByStepId = new Map<string, string[]>();

  orderedSteps.forEach((step) => {
    rawChildrenByStepId.set(step.step_id, []);
  });

  orderedSteps.forEach((step) => {
    const parents = (rawParentsAll.get(step.step_id) ?? []).filter((parentId) => usedStepIds.has(parentId));
    rawParentsByStepId.set(step.step_id, parents);
    directSourceTableNamesByStepId.set(step.step_id, directSourceTableNamesAll.get(step.step_id) ?? []);
    parents.forEach((parentId) => {
      const children = rawChildrenByStepId.get(parentId) ?? [];
      children.push(step.step_id);
      rawChildrenByStepId.set(parentId, children);
    });
  });

  return {
    candidate,
    session,
    orderedSteps,
    stepById: new Map(orderedSteps.map((step) => [step.step_id, step] as const)),
    stepPreviewById: new Map(candidate.step_previews.map((preview) => [preview.step_id, preview] as const)),
    sourceTableByName: new Map(session.source_tables.map((table) => [table.name, table] as const)),
    sourceTableById: new Map(session.source_tables.map((table) => [table.id, table] as const)),
    rawParentsByStepId,
    rawChildrenByStepId,
    directSourceTableNamesByStepId,
    rawIndexByStepId: new Map(orderedSteps.map((step, index) => [step.step_id, index] as const)),
    assessmentByNodeId: new Map((candidate.node_assessments ?? []).map((item) => [item.node_id, item] as const)),
    finalProducerId
  };
}

function inputColumnsForStep(step: PipelineStep, context: ReviewGraphContext) {
  const parentId = (context.rawParentsByStepId.get(step.step_id) ?? [])[0];
  if (!parentId) {
    const sourceName = (context.directSourceTableNamesByStepId.get(step.step_id) ?? [])[0];
    if (!sourceName) {
      return [];
    }
    return context.sourceTableByName.get(sourceName)?.columns ?? [];
  }
  const parentPreview = context.stepPreviewById.get(parentId);
  if (parentPreview?.columns?.length) {
    return parentPreview.columns;
  }
  const parentStep = context.stepById.get(parentId);
  if (!parentStep || parentStep.operator !== "source_table") {
    return [];
  }
  const sourceName = asStringValue(parentStep.params.source_table) ?? parentStep.output;
  const sourceTable = context.sourceTableByName.get(sourceName) ?? context.sourceTableById.get(parentStep.output);
  return sourceTable?.columns ?? [];
}

function isNoopDropColumns(step: PipelineStep, context: ReviewGraphContext) {
  if (step.operator !== "drop_columns") {
    return false;
  }
  const preview = context.stepPreviewById.get(step.step_id);
  if (!preview) {
    return false;
  }
  const beforeColumns = inputColumnsForStep(step, context);
  if (!beforeColumns.length) {
    return false;
  }
  return arraysEqual(beforeColumns, preview.columns);
}

function isJoinHelperAddColumns(step: PipelineStep) {
  if (step.operator !== "add_columns") {
    return false;
  }
  const mappings = Array.isArray(step.params.mappings) ? step.params.mappings : [];
  if (!mappings.length) {
    return false;
  }
  return mappings.every((mapping) => {
    const record = asRecord(mapping);
    const kind = asStringValue(record.kind) ?? "constant";
    const output = normalizeIdentifier(asStringValue(record.output));
    return kind === "constant" && output === "join_key";
  });
}

function sortStepIds(stepIds: string[], rawIndexByStepId: Map<string, number>) {
  return [...stepIds].sort((left, right) => (rawIndexByStepId.get(left) ?? 0) - (rawIndexByStepId.get(right) ?? 0));
}

function joinHelpersByJoinStep(context: ReviewGraphContext) {
  const absorbedByJoinStep = new Map<string, string[]>();
  const joinStepByHelperStep = new Map<string, string>();

  context.orderedSteps.forEach((step) => {
    if (step.operator !== "join") {
      return;
    }
    const covered = new Set<string>();
    (context.rawParentsByStepId.get(step.step_id) ?? []).forEach((parentId) => {
      let cursor = parentId;
      while (cursor) {
        const parentStep = context.stepById.get(cursor);
        if (!parentStep || !isJoinHelperAddColumns(parentStep)) {
          break;
        }
        covered.add(cursor);
        joinStepByHelperStep.set(cursor, step.step_id);
        const nextCursor = (context.rawParentsByStepId.get(cursor) ?? [])[0];
        if (!nextCursor) {
          break;
        }
        const nextStep = context.stepById.get(nextCursor);
        if (!nextStep || !isJoinHelperAddColumns(nextStep)) {
          break;
        }
        cursor = nextCursor;
      }
    });
    absorbedByJoinStep.set(step.step_id, sortStepIds(Array.from(covered), context.rawIndexByStepId));
  });

  return { absorbedByJoinStep, joinStepByHelperStep };
}

function explanationForNode(
  kind: "input" | "step" | "output",
  step: PipelineStep | null,
  label: string,
  candidate: CandidatePipeline,
  preferredNodeId: string | null
) {
  if (preferredNodeId) {
    const explanation = candidate.node_explanations[preferredNodeId];
    if (explanation) {
      return explanation;
    }
  }
  return fallbackExplanation(kind, step, label);
}

function detailsForInput(sourceName: string, sourceTable: SourceTableSpec | undefined, statsLine: string) {
  const details = [`Starting table: ${quote(sourceName)}`];
  if (statsLine) {
    details.push(`Current shape: ${statsLine}`);
  }
  if (sourceTable?.columns?.length) {
    details.push(`Columns: ${formatList(sourceTable.columns)}`);
  }
  return details;
}

function detailsForOutput(statsLine: string) {
  const details = ["Final result"];
  if (statsLine) {
    details.push(`Current shape: ${statsLine}`);
  }
  return details;
}

function renameDetails(step: PipelineStep, preview: StepPreview | undefined) {
  const renamed = Object.entries(preview?.renamed_columns ?? {})
    .filter(([source, target]) => Boolean(source) && Boolean(target))
    .slice(0, 3);
  if (renamed.length) {
    const lines = renamed.map(([source, target]) => `${quote(source)} -> ${quote(target)}`);
    const hiddenCount = Object.keys(preview?.renamed_columns ?? {}).length - renamed.length;
    if (hiddenCount > 0) {
      lines.push(`+${hiddenCount} more renamed columns`);
    }
    return lines;
  }
  const mapping = Object.entries(asRecord(step.params.mapping))
    .filter(([source, target]) => Boolean(source) && Boolean(target))
    .slice(0, 3)
    .map(([source, target]) => `${quote(source)} -> ${quote(String(target))}`);
  return mapping.length ? mapping : ["Rename the columns needed downstream."];
}

function dropColumnDetails(step: PipelineStep, preview: StepPreview | undefined, context: ReviewGraphContext) {
  const removed = preview?.removed_columns?.length
    ? preview.removed_columns
    : inputColumnsForStep(step, context).filter((column) => !preview?.columns.includes(column));
  if (removed.length) {
    return [`Remove ${formatList(removed)}.`];
  }
  const keep = asStringList(step.params.keep);
  if (keep.length) {
    return [`Keep only ${formatList(keep)}.`];
  }
  return ["Keep only the columns needed later in the flow."];
}

function dateDetails(step: PipelineStep) {
  const column = asStringValue(step.params.column) ?? "Date";
  const sourceColumn = asStringValue(step.params.source_column) ?? column;
  const outputFormat = asStringValue(step.params.output_format) ?? "%Y-%m-%d";
  if (sourceColumn !== column) {
    return [`Create ${quote(column)} from ${quote(sourceColumn)}.`, `Write the result in ${quote(outputFormat)} format.`];
  }
  return [`Rewrite ${quote(column)} in ${quote(outputFormat)} format.`];
}

function summarizeDetails(step: PipelineStep) {
  const keys = asStringList(step.params.keys);
  const aggregations = Array.isArray(step.params.aggregations) ? step.params.aggregations : [];
  const uniqueLabels = Array.from(
    new Set(
      aggregations
        .map((aggregation) => aggregationLabel(asStringValue(asRecord(aggregation).func)))
        .filter(Boolean)
    )
  );
  const summaryLabel = uniqueLabels.length === 1 ? uniqueLabels[0] : "values";
  const lines = [groupbySummaryLine(keys, summaryLabel)];
  aggregations.slice(0, 2).forEach((aggregation) => {
    const record = asRecord(aggregation);
    const output = asStringValue(record.output) ?? "a summary value";
    const source = asStringValue(record.source) ?? asStringValue(record.column) ?? "another column";
    const func = (asStringValue(record.func) ?? "combine").toLowerCase();
    lines.push(`Create ${quote(output)} ${aggregationVerb(func, source)}.`);
  });
  if (aggregations.length > 2) {
    lines.push(`+${aggregations.length - 2} more summary values`);
  }
  return lines;
}

function joinDetails(step: PipelineStep, preview: StepPreview | undefined) {
  const leftKeys = asStringList(step.params.left_on ?? step.params.on).filter((value) => normalizeIdentifier(value) !== "join_key");
  const rightKeys = asStringList(step.params.right_on ?? step.params.on).filter((value) => normalizeIdentifier(value) !== "join_key");
  const addedColumns = (preview?.added_columns ?? []).filter((value) => normalizeIdentifier(value) !== "join_key");
  const lines = [
    leftKeys.length || rightKeys.length
      ? `Match rows using ${formatList(leftKeys.length ? leftKeys : rightKeys)} and bring in extra information.`
      : "Match rows with another table and bring in extra information."
  ];
  if (addedColumns.length) {
    lines.push(`Added columns: ${formatList(addedColumns)}.`);
  }
  return lines;
}

function unionDetails(step: PipelineStep) {
  return [`Combine rows from ${formatList(step.inputs)}.`];
}

function pivotDetails(step: PipelineStep) {
  const values = asStringList(step.params.values);
  const columns = asStringList(step.params.columns);
  const lines = ["Turn repeated row values into separate columns."];
  if (columns.length) {
    lines.push(`New columns come from ${formatList(columns)}.`);
  }
  if (values.length) {
    lines.push(`Filled values come from ${formatList(values)}.`);
  }
  return lines;
}

function unpivotDetails(step: PipelineStep) {
  const values = asStringList(step.params.value_vars);
  return values.length ? [`Turn ${formatList(values)} into rows.`] : ["Turn selected columns into rows."];
}

function computeValueDetails(step: PipelineStep) {
  const outputColumn = asStringValue(step.params.output_column);
  return [outputColumn ? `Calculate ${quote(outputColumn)} from other columns.` : "Calculate a value from other columns."];
}

function addColumnDetails(step: PipelineStep) {
  const mappings = Array.isArray(step.params.mappings) ? step.params.mappings : [];
  if (!mappings.length) {
    return ["Add or update a column for later steps."];
  }
  const lines = mappings.slice(0, 2).map((mapping) => {
    const record = asRecord(mapping);
    const output = asStringValue(record.output) ?? "a new column";
    const kind = asStringValue(record.kind) ?? "constant";
    if (kind === "copy") {
      const source = asStringValue(record.source) ?? "another column";
      return `Copy ${quote(source)} into ${quote(output)}.`;
    }
    if (kind === "constant") {
      return `Fill ${quote(output)} with the same value in each row.`;
    }
    return `Create ${quote(output)} from other values in the row.`;
  });
  if (mappings.length > 2) {
    lines.push(`+${mappings.length - 2} more updated columns`);
  }
  return lines;
}

function detailsForStep(step: PipelineStep, preview: StepPreview | undefined, context: ReviewGraphContext) {
  switch (step.operator) {
    case "rename":
      return renameDetails(step, preview);
    case "drop_columns":
      return dropColumnDetails(step, preview, context);
    case "date_formatting":
      return dateDetails(step);
    case "groupby":
      return summarizeDetails(step);
    case "join":
      return joinDetails(step, preview);
    case "union":
      return unionDetails(step);
    case "pivot":
      return pivotDetails(step);
    case "unpivot":
      return unpivotDetails(step);
    case "column_arithmetic":
      return computeValueDetails(step);
    case "add_columns":
      return addColumnDetails(step);
    default:
      return ["Details unavailable for this step."];
  }
}

function statusAndReason(
  kind: "input" | "step" | "output",
  label: string,
  coveredNodeIds: string[],
  context: ReviewGraphContext
) {
  const issueAssessment = coveredNodeIds
    .map((nodeId) => context.assessmentByNodeId.get(nodeId))
    .find((item) => item?.status === "issue");
  if (issueAssessment?.reason) {
    return { status: "issue" as const, reason: issueAssessment.reason };
  }

  const previewWarning = coveredNodeIds
    .filter((nodeId) => nodeId !== "node_output")
    .flatMap((nodeId) => context.stepPreviewById.get(nodeId)?.warnings ?? [])
    .find(Boolean);
  if (previewWarning) {
    return { status: "issue" as const, reason: previewWarning };
  }

  return { status: "ok" as const, reason: friendlyOkReason(kind, label) };
}

function nearestDisplayNodeId(
  rawStepId: string,
  displayNodeIdByRawNodeId: Record<string, string>,
  context: ReviewGraphContext
) {
  const direct = displayNodeIdByRawNodeId[rawStepId];
  if (direct) {
    return direct;
  }

  const childQueue = [...(context.rawChildrenByStepId.get(rawStepId) ?? [])];
  const visitedChildren = new Set<string>();
  while (childQueue.length) {
    const current = childQueue.shift();
    if (!current || visitedChildren.has(current)) continue;
    visitedChildren.add(current);
    const mapped = displayNodeIdByRawNodeId[current];
    if (mapped) {
      return mapped;
    }
    (context.rawChildrenByStepId.get(current) ?? []).forEach((childId) => childQueue.push(childId));
  }

  const parentQueue = [...(context.rawParentsByStepId.get(rawStepId) ?? [])];
  const visitedParents = new Set<string>();
  while (parentQueue.length) {
    const current = parentQueue.shift();
    if (!current || visitedParents.has(current)) continue;
    visitedParents.add(current);
    const mapped = displayNodeIdByRawNodeId[current];
    if (mapped) {
      return mapped;
    }
    (context.rawParentsByStepId.get(current) ?? []).forEach((parentId) => parentQueue.push(parentId));
  }

  return null;
}

function collectParentDisplayIds(
  rawStepId: string,
  targetDisplayId: string,
  displayNodeIdByRawNodeId: Record<string, string>,
  context: ReviewGraphContext
) {
  const results = new Set<string>();

  const visit = (candidateId: string) => {
    const mappedDisplayId = displayNodeIdByRawNodeId[candidateId];
    if (mappedDisplayId && mappedDisplayId !== targetDisplayId) {
      results.add(mappedDisplayId);
      return;
    }
    (context.rawParentsByStepId.get(candidateId) ?? []).forEach((parentId) => visit(parentId));
  };

  (context.rawParentsByStepId.get(rawStepId) ?? []).forEach((parentId) => visit(parentId));
  return Array.from(results);
}

function syntheticSourceNodeId(sourceTable: SourceTableSpec | undefined, sourceName: string) {
  return `node_source_${normalizeIdentifier(sourceTable?.id ?? sourceName)}`;
}

export function buildReviewDisplayGraph(session: Session | null, candidate: CandidatePipeline | null): ReviewDisplayGraph {
  if (!session || !candidate) {
    return { nodes: [], edges: [], displayNodeIdByRawNodeId: {} };
  }

  const context = buildRawContext(session, candidate);
  const { absorbedByJoinStep, joinStepByHelperStep } = joinHelpersByJoinStep(context);

  const visibleStepIds = new Set<string>();
  context.orderedSteps.forEach((step) => {
    if (step.operator === "source_table") {
      visibleStepIds.add(step.step_id);
      return;
    }
    if (joinStepByHelperStep.has(step.step_id)) {
      return;
    }
    if (isNoopDropColumns(step, context)) {
      return;
    }
    visibleStepIds.add(step.step_id);
  });

  const displayNodeIdByRawNodeId: Record<string, string> = {};
  context.orderedSteps.forEach((step) => {
    if (visibleStepIds.has(step.step_id)) {
      displayNodeIdByRawNodeId[step.step_id] = step.step_id;
    }
  });
  joinStepByHelperStep.forEach((joinStepId, helperStepId) => {
    displayNodeIdByRawNodeId[helperStepId] = joinStepId;
  });

  context.orderedSteps.forEach((step) => {
    if (!displayNodeIdByRawNodeId[step.step_id]) {
      const mapped = nearestDisplayNodeId(step.step_id, displayNodeIdByRawNodeId, context);
      if (mapped) {
        displayNodeIdByRawNodeId[step.step_id] = mapped;
      }
    }
  });

  const nodes: ReviewDisplayNode[] = [];
  const sourceDisplayNodeIdByName = new Map<string, string>();
  const explicitSourceStepIdByName = new Map<string, string>();

  context.orderedSteps.forEach((step) => {
    if (!visibleStepIds.has(step.step_id) || step.operator !== "source_table") {
      return;
    }
    const sourceName = asStringValue(step.params.source_table) ?? step.output;
    if (sourceName) {
      explicitSourceStepIdByName.set(sourceName, step.step_id);
    }
  });

  const syntheticSourceEntries = Array.from(
    context.orderedSteps.reduce((accumulator, step) => {
      const targetDisplayId = displayNodeIdByRawNodeId[step.step_id];
      if (!targetDisplayId) {
        return accumulator;
      }
      (context.directSourceTableNamesByStepId.get(step.step_id) ?? []).forEach((sourceName) => {
        if (explicitSourceStepIdByName.has(sourceName)) {
          return;
        }
        const sourceTable = context.sourceTableByName.get(sourceName);
        if (!sourceTable) {
          return;
        }
        const existing = accumulator.get(sourceName);
        const stepIndex = context.rawIndexByStepId.get(step.step_id) ?? Number.MAX_SAFE_INTEGER;
        if (!existing || stepIndex < existing.firstConsumerIndex) {
          accumulator.set(sourceName, { sourceTable, firstConsumerIndex: stepIndex });
        }
      });
      return accumulator;
    }, new Map<string, { sourceTable: SourceTableSpec; firstConsumerIndex: number }>())
  ).sort((left, right) => left[1].firstConsumerIndex - right[1].firstConsumerIndex);

  context.orderedSteps.forEach((step) => {
    if (!visibleStepIds.has(step.step_id)) {
      return;
    }

    if (step.operator === "source_table") {
      const sourceName = asStringValue(step.params.source_table) ?? step.output;
      const sourceTable = context.sourceTableByName.get(sourceName) ?? context.sourceTableById.get(step.output);
      const preview = context.stepPreviewById.get(step.step_id);
      const subtitle = displayTableKind("input");
      const statsLine = displayStatsLine(
        sourceTable?.row_count ?? preview?.row_count,
        sourceTable?.columns.length ?? preview?.columns.length
      );
      const label = sourceTable?.name ?? step.title ?? step.output;
      const coveredStepIds = [step.step_id];
      const { status, reason } = statusAndReason("input", label, coveredStepIds, context);
      nodes.push({
        id: step.step_id,
        kind: "input",
        label,
        subtitle,
        statsLine,
        status,
        explanation: explanationForNode("input", step, label, candidate, step.step_id),
        assessmentReason: reason,
        previewRows: sourceTable?.preview_rows ?? preview?.preview_rows ?? [],
        editableStepId: null,
        coveredStepIds,
        previewStepId: step.step_id,
        detailLines: detailsForInput(sourceName, sourceTable, statsLine),
        sourceTable
      });
      sourceDisplayNodeIdByName.set(sourceName, step.step_id);
      return;
    }
  });

  syntheticSourceEntries.forEach(([sourceName, entry]) => {
    const sourceTable = entry.sourceTable;
    const label = sourceTable.name;
    const subtitle = displayTableKind("input");
    const statsLine = displayStatsLine(sourceTable.row_count, sourceTable.columns.length);
    const { status, reason } = statusAndReason("input", label, [], context);
    const nodeId = syntheticSourceNodeId(sourceTable, sourceName);
    nodes.push({
      id: nodeId,
      kind: "input",
      label,
      subtitle,
      statsLine,
      status,
      explanation: fallbackExplanation("input", null, label),
      assessmentReason: reason,
      previewRows: sourceTable.preview_rows ?? [],
      editableStepId: null,
      coveredStepIds: [],
      previewStepId: null,
      detailLines: detailsForInput(sourceName, sourceTable, statsLine),
      sourceTable
    });
    sourceDisplayNodeIdByName.set(sourceName, nodeId);
  });

  context.orderedSteps.forEach((step) => {
    if (!visibleStepIds.has(step.step_id) || step.operator === "source_table") {
      return;
    }

    const preview = context.stepPreviewById.get(step.step_id);
    const coveredStepIds = step.operator === "join"
      ? sortStepIds([...(absorbedByJoinStep.get(step.step_id) ?? []), step.step_id], context.rawIndexByStepId)
      : [step.step_id];
    const label = displayLabelForStep(step);
    const subtitle = displayTableKind("step");
    const statsLine = displayStatsLine(preview?.row_count, preview?.columns.length);
    const { status, reason } = statusAndReason("step", label, coveredStepIds, context);

    nodes.push({
      id: step.step_id,
      kind: "step",
      label,
      subtitle,
      statsLine,
      status,
      explanation: explanationForNode("step", step, label, candidate, step.step_id),
      assessmentReason: reason,
      previewRows: preview?.preview_rows ?? [],
      editableStepId: step.step_id,
      coveredStepIds,
      previewStepId: step.step_id,
      detailLines: detailsForStep(step, preview, context)
    });
  });

  const outputCoveredNodeIds = context.finalProducerId ? [context.finalProducerId, "node_output"] : ["node_output"];
  const outputLabel = "Output";
  const outputSubtitle = displayTableKind("output");
  const outputStatsLine = displayStatsLine(
    candidate.validation_summary.row_count ?? candidate.final_preview_rows.length,
    candidate.validation_summary.column_count ?? Object.keys(candidate.final_preview_rows[0] ?? {}).length
  );
  const outputStatus = statusAndReason("output", outputLabel, outputCoveredNodeIds, context);
  nodes.push({
    id: "node_output",
    kind: "output",
    label: outputLabel,
    subtitle: outputSubtitle,
    statsLine: outputStatsLine,
    status: outputStatus.status,
    explanation: explanationForNode("output", null, outputLabel, candidate, "node_output"),
    assessmentReason: outputStatus.reason,
    previewRows: candidate.final_preview_rows,
    editableStepId: null,
    coveredStepIds: outputCoveredNodeIds,
    previewStepId: context.finalProducerId,
    detailLines: detailsForOutput(outputStatsLine)
  });
  displayNodeIdByRawNodeId.node_output = "node_output";

  const edges: ReviewDisplayEdge[] = [];
  const seenEdgeIds = new Set<string>();
  const addEdge = (from: string, to: string) => {
    const edgeId = `${from}->${to}`;
    if (from === to || seenEdgeIds.has(edgeId)) {
      return;
    }
    seenEdgeIds.add(edgeId);
    edges.push({
      id: edgeId,
      from,
      to
    });
  };

  context.orderedSteps.forEach((step) => {
    const targetDisplayId = displayNodeIdByRawNodeId[step.step_id];
    if (!targetDisplayId) {
      return;
    }
    (context.directSourceTableNamesByStepId.get(step.step_id) ?? []).forEach((sourceName) => {
      const sourceDisplayId = sourceDisplayNodeIdByName.get(sourceName);
      if (sourceDisplayId) {
        addEdge(sourceDisplayId, targetDisplayId);
      }
    });
  });

  nodes.forEach((node) => {
    if (node.kind === "input") {
      return;
    }

    const parentDisplayIds = node.kind === "output"
      ? (context.finalProducerId ? collectParentDisplayIds(context.finalProducerId, node.id, displayNodeIdByRawNodeId, context) : [])
      : collectParentDisplayIds(node.id, node.id, displayNodeIdByRawNodeId, context);

    if (node.kind === "output" && context.finalProducerId) {
      const directProducerDisplayId = displayNodeIdByRawNodeId[context.finalProducerId];
      if (directProducerDisplayId && directProducerDisplayId !== node.id) {
        parentDisplayIds.push(directProducerDisplayId);
      }
    }

    Array.from(new Set(parentDisplayIds)).forEach((parentDisplayId, index) => {
      void index;
      addEdge(parentDisplayId, node.id);
    });
  });

  return { nodes, edges, displayNodeIdByRawNodeId };
}
