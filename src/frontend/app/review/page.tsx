"use client";

import {
  MouseEvent as ReactMouseEvent,
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  useTransition
} from "react";
import { createPortal } from "react-dom";
import { useRouter } from "next/navigation";

import { DataTable } from "@/components/data-table";
import {
  acceptPipeline,
  fetchGenerationStatus,
  fetchLiveSuggestions,
  fetchSession,
  generatePipelines,
  submitFeedback
} from "@/lib/api";
import { buildReviewDisplayGraph, type ReviewDisplayNode } from "@/lib/review-graph";
import { ensureSessionSummary } from "@/lib/session";
import { Session } from "@/lib/types";


const POPOVER_WIDTH = 430;
const POPOVER_HEIGHT = 640;
const NODE_MIN_WIDTH = 240;
const NODE_MAX_WIDTH = 420;
const NODE_HEIGHT = 160;
const COLUMN_GAP = 100;
const ROW_GAP = 76;
const SCENE_PADDING_X = 96;
const SCENE_PADDING_Y = 96;
const CANVAS_MIN_SCALE = 0.45;
const CANVAS_MAX_SCALE = 1.8;
const FIT_PADDING = 56;
const GENERATE_LOADING_LABELS = ["Parsing...", "Aligning...", "Synthesizing...", "Validating..."];
const REVISE_LOADING_LABELS = ["Rewriting...", "Recomputing...", "Revalidating...", "Finalizing..."];
const LOADING_LABEL_DURATION_MS = 1400;
const LOADING_TRANSITION_DURATION_MS = 300;
const LOADING_DOT_DELAYS_MS = [0, 120, 240];

type DisplayNode = ReviewDisplayNode;
type DockTab = "preview" | "warning" | "summary";

type CanvasNodeItem = {
  id: string;
  label: string;
  subtitle: string;
  statsLine?: string;
  status: "ok" | "issue";
  placeholder?: boolean;
};

type CanvasEdge = {
  id: string;
  from: string;
  to: string;
};

type CanvasNodePosition = {
  left: number;
  top: number;
  width: number;
};

type CanvasEdgePath = {
  id: string;
  from: string;
  to: string;
  path: string;
};

type CanvasPanState = {
  startClientX: number;
  startClientY: number;
  originX: number;
  originY: number;
};

type CanvasTransform = {
  scale: number;
  translateX: number;
  translateY: number;
};

type ViewportSize = {
  width: number;
  height: number;
};


function preferredNodeId(nodes: DisplayNode[]) {
  return nodes.find((node) => node.kind === "step")?.id ?? nodes.find((node) => node.kind === "input")?.id ?? nodes[0]?.id ?? null;
}


function reserveRow(preferredRow: number, usedRows: Set<number>) {
  if (!usedRows.has(preferredRow)) {
    usedRows.add(preferredRow);
    return preferredRow;
  }
  for (let delta = 1; delta < 200; delta += 1) {
    const lower = preferredRow - delta;
    if (lower >= 0 && !usedRows.has(lower)) {
      usedRows.add(lower);
      return lower;
    }
    const higher = preferredRow + delta;
    if (!usedRows.has(higher)) {
      usedRows.add(higher);
      return higher;
    }
  }
  const fallback = preferredRow + usedRows.size + 1;
  usedRows.add(fallback);
  return fallback;
}


function clampScale(scale: number) {
  return Math.min(CANVAS_MAX_SCALE, Math.max(CANVAS_MIN_SCALE, scale));
}


function estimateNodeWidth(label: string) {
  const estimated = Math.ceil(label.length * 15 + 90);
  return Math.min(NODE_MAX_WIDTH, Math.max(NODE_MIN_WIDTH, estimated));
}

function buildCanvasMetrics(nodes: CanvasNodeItem[], edges: CanvasEdge[]) {
  if (!nodes.length) {
    return {
      sceneWidth: SCENE_PADDING_X * 2 + NODE_MIN_WIDTH,
      sceneHeight: SCENE_PADDING_Y * 2 + NODE_HEIGHT,
      positions: {} as Record<string, CanvasNodePosition>,
      edgePaths: [] as CanvasEdgePath[]
    };
  }

  const incoming = new Map<string, string[]>();
  nodes.forEach((node) => {
    incoming.set(node.id, []);
  });
  edges.forEach((edge) => {
    const bucket = incoming.get(edge.to);
    if (bucket) {
      bucket.push(edge.from);
    }
  });

  const widths = new Map(nodes.map((node) => [node.id, estimateNodeWidth(node.label)] as const));
  const depthById = new Map<string, number>();
  const rowById = new Map<string, number>();
  const usedRowsByDepth = new Map<number, Set<number>>();
  let nextSourceRow = 0;
  let maxDepth = 0;
  let maxRow = 0;

  nodes.forEach((node) => {
    const parents = incoming.get(node.id) ?? [];
    const depth = parents.length ? Math.max(...parents.map((parentId) => depthById.get(parentId) ?? 0)) + 1 : 0;
    depthById.set(node.id, depth);
    maxDepth = Math.max(maxDepth, depth);

    const usedRows = usedRowsByDepth.get(depth) ?? new Set<number>();
    let preferredRow = 0;
    if (!parents.length) {
      preferredRow = nextSourceRow;
      nextSourceRow += 1;
    } else if (parents.length === 1) {
      preferredRow = rowById.get(parents[0]) ?? nextSourceRow;
    } else {
      const parentRows = parents.map((parentId) => rowById.get(parentId) ?? 0);
      preferredRow = Math.round(parentRows.reduce((sum, row) => sum + row, 0) / parentRows.length);
    }

    const row = reserveRow(preferredRow, usedRows);
    usedRowsByDepth.set(depth, usedRows);
    rowById.set(node.id, row);
    maxRow = Math.max(maxRow, row);
  });

  const columnWidths = new Map<number, number>();
  nodes.forEach((node) => {
    const depth = depthById.get(node.id) ?? 0;
    const nextWidth = Math.max(columnWidths.get(depth) ?? NODE_MIN_WIDTH, widths.get(node.id) ?? NODE_MIN_WIDTH);
    columnWidths.set(depth, nextWidth);
  });

  const columnLeftByDepth = new Map<number, number>();
  let cursor = SCENE_PADDING_X;
  for (let depth = 0; depth <= maxDepth; depth += 1) {
    columnLeftByDepth.set(depth, cursor);
    cursor += (columnWidths.get(depth) ?? NODE_MIN_WIDTH) + COLUMN_GAP;
  }

  const positions: Record<string, CanvasNodePosition> = {};
  nodes.forEach((node) => {
    const depth = depthById.get(node.id) ?? 0;
    const row = rowById.get(node.id) ?? 0;
    const width = widths.get(node.id) ?? NODE_MIN_WIDTH;
    const columnWidth = columnWidths.get(depth) ?? width;
    positions[node.id] = {
      left: (columnLeftByDepth.get(depth) ?? SCENE_PADDING_X) + (columnWidth - width) / 2,
      top: SCENE_PADDING_Y + row * (NODE_HEIGHT + ROW_GAP),
      width
    };
  });

  const sceneHeight = SCENE_PADDING_Y * 2 + (maxRow + 1) * NODE_HEIGHT + maxRow * ROW_GAP;
  const sceneWidth = cursor - COLUMN_GAP + SCENE_PADDING_X;
  const edgePaths: CanvasEdgePath[] = edges.flatMap((edge) => {
    const from = positions[edge.from];
    const to = positions[edge.to];
    if (!from || !to) return [];

    const startX = from.left + from.width;
    const startY = from.top + NODE_HEIGHT / 2;
    const endX = to.left;
    const endY = to.top + NODE_HEIGHT / 2;
    const deltaX = Math.max(52, (endX - startX) * 0.45);

    return [{
      id: edge.id,
      from: edge.from,
      to: edge.to,
      path: `M ${startX} ${startY} C ${startX + deltaX} ${startY}, ${endX - deltaX} ${endY}, ${endX} ${endY}`
    }];
  });

  return {
    sceneWidth,
    sceneHeight,
    positions,
    edgePaths
  };
}


function fitTransform(viewport: ViewportSize, sceneWidth: number, sceneHeight: number): CanvasTransform {
  const availableWidth = Math.max(viewport.width - FIT_PADDING * 2, 1);
  const availableHeight = Math.max(viewport.height - FIT_PADDING * 2, 1);
  const scale = clampScale(Math.min(availableWidth / sceneWidth, availableHeight / sceneHeight));

  return {
    scale,
    translateX: (viewport.width - sceneWidth * scale) / 2,
    translateY: (viewport.height - sceneHeight * scale) / 2
  };
}


function clampPopoverPosition(x: number, y: number) {
  const padding = 20;
  const left = Math.min(Math.max(padding, x + 16), window.innerWidth - POPOVER_WIDTH - padding);
  const top = Math.min(Math.max(96, y + 16), window.innerHeight - POPOVER_HEIGHT - padding);
  return { left, top };
}


function nodeAccentClass(status: "ok" | "issue") {
  return status === "issue"
    ? "bg-[linear-gradient(90deg,#dc2626_0%,#fb7185_100%)]"
    : "bg-[linear-gradient(90deg,#16a34a_0%,#34d399_100%)]";
}


function nodeCardClass(status: "ok" | "issue", selected: boolean) {
  const palette =
    status === "issue"
      ? "border-red-200 shadow-[0_10px_28px_rgba(239,68,68,0.08)]"
      : "border-emerald-200 shadow-[0_10px_28px_rgba(16,185,129,0.08)]";
  if (selected) {
    return `${palette} ring-4 ring-blue-100`;
  }
  return `${palette} hover:border-slate-300`;
}


function completionSuffix(prefix: string, suggestion: string) {
  if (!suggestion) return "";
  if (!prefix) return suggestion;
  if (suggestion.toLowerCase().startsWith(prefix.toLowerCase())) {
    return suggestion.slice(prefix.length);
  }
  return suggestion;
}

function warningSourceLabel(source: string) {
  if (source === "ambiguity") return "Needs confirmation";
  return "Check";
}


function splitLoadingLabel(label: string) {
  return {
    word: label.replace(/\.+$/, ""),
    dots: "...",
  };
}


function CanvasControls({ onFit }: { onFit: () => void }) {
  return (
    <button
      className="absolute right-4 top-4 z-20 inline-flex items-center gap-2 rounded-full border border-slate-200 bg-white/95 px-4 py-2 text-sm font-medium text-slate-700 shadow-sm backdrop-blur transition hover:border-slate-300 hover:bg-white"
      data-canvas-control="true"
      onClick={onFit}
      type="button"
    >
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" aria-hidden="true">
        <path d="M7 4H4v3M17 4h3v3M20 17v3h-3M4 17v3h3" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" />
        <path d="m9 9-2-2M15 9l2-2M15 15l2 2M9 15l-2 2" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" />
      </svg>
      Fit pipeline
    </button>
  );
}

function PipelineCanvas({
  nodes,
  edges,
  visibleNodeCount,
  selectedNodeId,
  onNodeClick,
  fitKey
}: {
  nodes: CanvasNodeItem[];
  edges: CanvasEdge[];
  visibleNodeCount: number;
  selectedNodeId: string | null;
  onNodeClick?: (nodeId: string, event: ReactMouseEvent<HTMLButtonElement>) => void;
  fitKey: string;
}) {
  const viewportRef = useRef<HTMLDivElement | null>(null);
  const panStateRef = useRef<CanvasPanState | null>(null);
  const transformRef = useRef<CanvasTransform>({ scale: 1, translateX: 0, translateY: 0 });
  const [viewport, setViewport] = useState<ViewportSize>({ width: 0, height: 0 });
  const [transform, setTransform] = useState<CanvasTransform>({ scale: 1, translateX: 0, translateY: 0 });
  const [isPanning, setIsPanning] = useState(false);

  const metrics = useMemo(() => buildCanvasMetrics(nodes, edges), [edges, nodes]);
  const visibleNodeIds = useMemo(() => new Set(nodes.slice(0, visibleNodeCount).map((node) => node.id)), [nodes, visibleNodeCount]);

  useEffect(() => {
    transformRef.current = transform;
  }, [transform]);

  useEffect(() => {
    const viewportElement = viewportRef.current;
    if (!viewportElement) return;

    const updateViewport = () => {
      const rect = viewportElement.getBoundingClientRect();
      setViewport({ width: rect.width, height: rect.height });
    };

    updateViewport();
    const observer = new ResizeObserver((entries) => {
      const rect = entries[0]?.contentRect;
      if (!rect) return;
      setViewport({ width: rect.width, height: rect.height });
    });
    observer.observe(viewportElement);

    return () => observer.disconnect();
  }, []);

  const fitPipelineToViewport = useCallback(() => {
    if (!viewport.width || !viewport.height) return;
    setTransform(fitTransform(viewport, metrics.sceneWidth, metrics.sceneHeight));
  }, [metrics.sceneHeight, metrics.sceneWidth, viewport]);

  useLayoutEffect(() => {
    if (!nodes.length || !viewport.width || !viewport.height) return;
    fitPipelineToViewport();
  }, [fitKey, fitPipelineToViewport, nodes.length, viewport.height, viewport.width]);

  useEffect(() => {
    const handleMouseMove = (event: globalThis.MouseEvent) => {
      if (!panStateRef.current) return;
      const nextX = panStateRef.current.originX + (event.clientX - panStateRef.current.startClientX);
      const nextY = panStateRef.current.originY + (event.clientY - panStateRef.current.startClientY);
      setTransform((current) => ({ ...current, translateX: nextX, translateY: nextY }));
    };

    const stopPanning = () => {
      panStateRef.current = null;
      setIsPanning(false);
    };

    window.addEventListener("mousemove", handleMouseMove);
    window.addEventListener("mouseup", stopPanning);

    return () => {
      window.removeEventListener("mousemove", handleMouseMove);
      window.removeEventListener("mouseup", stopPanning);
    };
  }, []);

  const beginPanning = (event: ReactMouseEvent<HTMLDivElement>) => {
    if (event.button !== 0) return;
    const target = event.target as HTMLElement;
    if (target.closest("[data-canvas-node='true']") || target.closest("[data-canvas-control='true']")) {
      return;
    }

    panStateRef.current = {
      startClientX: event.clientX,
      startClientY: event.clientY,
      originX: transformRef.current.translateX,
      originY: transformRef.current.translateY
    };
    setIsPanning(true);
    event.preventDefault();
  };

  const zoomAtPointer = useCallback((clientX: number, clientY: number, nextScale: number) => {
    const viewportElement = viewportRef.current;
    if (!viewportElement) return;

    const rect = viewportElement.getBoundingClientRect();
    const pointerX = clientX - rect.left;
    const pointerY = clientY - rect.top;

    setTransform((current) => {
      const clampedScale = clampScale(nextScale);
      const sceneX = (pointerX - current.translateX) / current.scale;
      const sceneY = (pointerY - current.translateY) / current.scale;

      return {
        scale: clampedScale,
        translateX: pointerX - sceneX * clampedScale,
        translateY: pointerY - sceneY * clampedScale
      };
    });
  }, []);

  useEffect(() => {
    const viewportElement = viewportRef.current;
    if (!viewportElement) return;

    const handleWheel = (event: WheelEvent) => {
      event.preventDefault();
      const zoomFactor = Math.exp(-event.deltaY * 0.0014);
      const nextScale = clampScale(transformRef.current.scale * zoomFactor);
      zoomAtPointer(event.clientX, event.clientY, nextScale);
    };

    viewportElement.addEventListener("wheel", handleWheel, { passive: false });
    return () => viewportElement.removeEventListener("wheel", handleWheel);
  }, [zoomAtPointer]);

  return (
    <div className="mt-5 min-h-0 min-w-0 flex-1 overflow-hidden rounded-[28px] border border-slate-200 bg-slate-50 p-4">
      <div
        ref={viewportRef}
        className={`relative h-full min-h-0 w-full overflow-hidden rounded-[24px] border border-slate-200 bg-[#f7f9fc] ${
          isPanning ? "cursor-grabbing select-none" : "cursor-grab"
        }`}
        onMouseDown={beginPanning}
      >
        <div
          className="pointer-events-none absolute inset-0"
          style={{
            backgroundImage: "radial-gradient(circle, rgba(148,163,184,0.16) 1px, transparent 1px)",
            backgroundSize: "14px 14px"
          }}
        />
        <CanvasControls onFit={fitPipelineToViewport} />
        <div
          className="absolute left-0 top-0 z-10"
          style={{
            width: metrics.sceneWidth,
            height: metrics.sceneHeight,
            transform: `translate(${transform.translateX}px, ${transform.translateY}px) scale(${transform.scale})`,
            transformOrigin: "0 0"
          }}
        >
          <div className="relative h-full w-full">
            <svg className="pointer-events-none absolute inset-0 h-full w-full overflow-visible" aria-hidden="true">
              <defs>
                <marker id="pipeline-arrowhead" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
                  <path d="M 0 0 L 10 5 L 0 10 z" fill="#94a3b8" />
                </marker>
              </defs>
              {metrics.edgePaths.map((edge) => {
                const visible = visibleNodeIds.has(edge.from) && visibleNodeIds.has(edge.to);
                return (
                  <path
                    key={edge.id}
                    d={edge.path}
                    fill="none"
                    markerEnd="url(#pipeline-arrowhead)"
                    stroke="#94a3b8"
                    strokeLinecap="round"
                    strokeWidth="1.8"
                    className={`transition-opacity duration-500 ${visible ? "opacity-100" : "opacity-0"}`}
                  />
                );
              })}
            </svg>
            {nodes.map((node, index) => {
              const position = metrics.positions[node.id];
              if (!position) return null;
              const visible = index < visibleNodeCount;

              return (
                <div key={node.id}>
                  <button
                    className={`absolute overflow-hidden rounded-[30px] border bg-white px-5 pb-5 pt-7 text-left transition-all duration-500 ${
                      node.placeholder
                        ? "animate-pulse border-slate-200 shadow-[0_8px_24px_rgba(15,23,42,0.05)]"
                        : `${nodeCardClass(node.status, node.id === selectedNodeId)}`
                    } ${
                      visible ? "translate-y-0 scale-100 opacity-100" : "pointer-events-none translate-y-6 scale-95 opacity-0"
                    }`}
                    data-canvas-node="true"
                    disabled={!onNodeClick || node.placeholder}
                    onClick={onNodeClick ? (event) => onNodeClick(node.id, event) : undefined}
                    style={{
                      left: position.left,
                      top: position.top,
                      width: position.width,
                      minHeight: NODE_HEIGHT,
                      transitionDelay: `${Math.min(index, visibleNodeCount) * 40}ms`
                    }}
                    type="button"
                  >
                    <div className={`pointer-events-none absolute inset-x-0 top-0 h-[12px] ${nodeAccentClass(node.status)}`} />
                    <div
                      className={`overflow-hidden text-2xl font-semibold leading-[1.25] ${node.placeholder ? "text-slate-300" : "text-slate-900"}`}
                      style={{
                        display: "-webkit-box",
                        WebkitLineClamp: 2,
                        WebkitBoxOrient: "vertical",
                        wordBreak: "break-word",
                      }}
                    >
                      {node.label}
                    </div>
                    <div className={`mt-2 text-lg font-medium ${node.placeholder ? "text-slate-200" : "text-slate-500"}`}>{node.subtitle}</div>
                    {node.statsLine ? (
                      <div className={`mt-1.5 text-base ${node.placeholder ? "text-slate-200" : "text-slate-500"}`}>{node.statsLine}</div>
                    ) : null}
                  </button>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
}


function ClaudeStyleLoading({ mode, resetKey }: { mode: "generate" | "revise"; resetKey: string }) {
  const labels = mode === "revise" ? REVISE_LOADING_LABELS : GENERATE_LOADING_LABELS;
  const [activeIndex, setActiveIndex] = useState(0);
  const [previousIndex, setPreviousIndex] = useState<number | null>(null);
  const clearPreviousTimerRef = useRef<number | null>(null);
  const activeIndexRef = useRef(0);
  const maxLabelLength = useMemo(
    () => labels.reduce((longest, label) => Math.max(longest, splitLoadingLabel(label).word.length), 0),
    [labels]
  );
  const activeLabel = splitLoadingLabel(labels[activeIndex] ?? labels[0] ?? "Loading...");

  useEffect(() => {
    activeIndexRef.current = activeIndex;
  }, [activeIndex]);

  useEffect(() => {
    setActiveIndex(0);
    activeIndexRef.current = 0;
    setPreviousIndex(null);
    if (clearPreviousTimerRef.current) {
      window.clearTimeout(clearPreviousTimerRef.current);
      clearPreviousTimerRef.current = null;
    }
  }, [resetKey]);

  useEffect(() => {
    const labelTimer = window.setInterval(() => {
      const current = activeIndexRef.current;
      const next = (current + 1) % labels.length;
      setPreviousIndex(current);
      setActiveIndex(next);
      activeIndexRef.current = next;

      if (clearPreviousTimerRef.current) {
        window.clearTimeout(clearPreviousTimerRef.current);
      }
      clearPreviousTimerRef.current = window.setTimeout(() => {
        setPreviousIndex(null);
        clearPreviousTimerRef.current = null;
      }, LOADING_TRANSITION_DURATION_MS);
    }, LOADING_LABEL_DURATION_MS);

    return () => {
      window.clearInterval(labelTimer);
      if (clearPreviousTimerRef.current) {
        window.clearTimeout(clearPreviousTimerRef.current);
        clearPreviousTimerRef.current = null;
      }
    };
  }, [labels]);

  return (
    <div
      aria-atomic="true"
      aria-live="polite"
      className="review-loading-text font-mono text-[1.15rem] font-medium tracking-tight text-slate-700"
      style={{ minWidth: `${maxLabelLength + 4}ch` }}
    >
      {previousIndex !== null ? (
        <span className="review-loading-text-layer review-loading-text-exit" aria-hidden="true">
          <span className="review-loading-word">{splitLoadingLabel(labels[previousIndex] ?? labels[0]).word}</span>
          <span className="review-loading-dots">
            {LOADING_DOT_DELAYS_MS.map((delay, index) => (
              <span key={`exit-dot-${index}`} className="review-loading-dot" style={{ animationDelay: `${delay}ms` }}>
                .
              </span>
            ))}
          </span>
        </span>
      ) : null}

      <span key={`${resetKey}-${activeIndex}`} className="review-loading-text-layer review-loading-text-enter" aria-hidden="true">
        <span className="review-loading-word">{activeLabel.word}</span>
        <span className="review-loading-dots">
          {LOADING_DOT_DELAYS_MS.map((delay, index) => (
            <span key={`enter-dot-${index}`} className="review-loading-dot" style={{ animationDelay: `${delay}ms` }}>
              .
            </span>
          ))}
        </span>
      </span>

      <span className="sr-only">{`${activeLabel.word}${activeLabel.dots}`}</span>
      <span aria-hidden="true" className="invisible">
        {`${"W".repeat(maxLabelLength)}...`}
      </span>
    </div>
  );
}


function GeneratingPipelineScene({
  mode,
  fitKey
}: {
  mode: "generate" | "revise";
  fitKey: string;
}) {
  return (
    <div className="min-h-0 min-w-0 flex-1">
      <section className="card flex h-full min-h-0 min-w-0 flex-col overflow-hidden p-6">
        <h2 className="text-[2rem] font-medium text-slate-900">Pipeline Overview</h2>
        <div className="mt-5 min-h-0 min-w-0 flex-1 overflow-hidden rounded-[28px] border border-slate-200 bg-slate-50">
          <div className="flex h-full min-h-0 items-center justify-center bg-[radial-gradient(circle_at_center,rgba(59,130,246,0.06),transparent_45%)] px-6 text-center">
            <ClaudeStyleLoading mode={mode} resetKey={fitKey} />
          </div>
        </div>
      </section>
    </div>
  );
}


export default function ReviewPage() {
  const router = useRouter();
  const [session, setSession] = useState<Session | null>(null);
  const [feedbackText, setFeedbackText] = useState("");
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [activeDockTab, setActiveDockTab] = useState<DockTab>("preview");
  const [popoverPosition, setPopoverPosition] = useState<{ left: number; top: number } | null>(null);
  const [status, setStatus] = useState("Preparing review...");
  const [error, setError] = useState("");
  const [isPending, startTransition] = useTransition();
  const [isFeedbackFocused, setIsFeedbackFocused] = useState(false);
  const [liveSuggestions, setLiveSuggestions] = useState<string[]>([]);
  const [suggestionStatus, setSuggestionStatus] = useState<"idle" | "loading" | "ready">("idle");
  const [isRefreshingPreview, setIsRefreshingPreview] = useState(false);
  const [visibleNodeCount, setVisibleNodeCount] = useState(0);
  const popoverRef = useRef<HTMLDivElement | null>(null);
  const pendingRevealRef = useRef(false);
  const lastAnimatedCandidateRef = useRef<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    ensureSessionSummary()
      .then((summary) => {
        if (cancelled) return;
        setSession(summary.session);
        setStatus(summary.session.status_message || "Review ready.");
        setIsRefreshingPreview(summary.session.status === "generating" || summary.session.status === "revising");
      })
      .catch((err) => setError(err instanceof Error ? err.message : "Failed to load Review."));

    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!session?.id) return;
    if (!isRefreshingPreview && session.status !== "generating" && session.status !== "revising") return;

    let cancelled = false;
    let timer: number | null = null;

    const poll = async () => {
      try {
        const generation = await fetchGenerationStatus(session.id);
        if (cancelled) return;

        setStatus(generation.message || "Generating pipeline...");

        if (generation.status === "review_ready" || generation.status === "export_ready") {
          const refreshed = await fetchSession(session.id);
          if (cancelled) return;
          pendingRevealRef.current = true;
          setSession(refreshed.session);
          setIsRefreshingPreview(false);
          setError("");
          setStatus(refreshed.session.status_message || generation.message || "Pipeline ready for review.");
          return;
        }

        if (generation.status === "error") {
          const refreshed = await fetchSession(session.id);
          if (cancelled) return;
          const nextError = refreshed.session.last_error || generation.message || "Pipeline generation failed.";
          setSession(refreshed.session);
          setIsRefreshingPreview(false);
          setError(refreshed.session.last_error ? "" : nextError);
          return;
        }

        timer = window.setTimeout(poll, 1200);
      } catch (err) {
        if (cancelled) return;
        setIsRefreshingPreview(false);
        setError(err instanceof Error ? err.message : "Unable to track pipeline generation.");
      }
    };

    void poll();

    return () => {
      cancelled = true;
      if (timer) window.clearTimeout(timer);
    };
  }, [isRefreshingPreview, session?.id, session?.status]);

  const selectedCandidate = useMemo(
    () => session?.candidates.find((candidate) => candidate.id === session.selected_candidate_id) ?? session?.candidates[0] ?? null,
    [session]
  );

  const graph = useMemo(() => buildReviewDisplayGraph(session, selectedCandidate), [session, selectedCandidate]);
  const displayNodes = graph.nodes;
  const displayEdges = graph.edges;
  const displayNodeById = useMemo(
    () => new Map(displayNodes.map((node) => [node.id, node] as const)),
    [displayNodes]
  );
  const canvasNodes = useMemo<CanvasNodeItem[]>(
    () =>
      displayNodes.map((node) => ({
        id: node.id,
        label: node.label,
        subtitle: node.subtitle,
        statsLine: node.statsLine,
        status: node.status
      })),
    [displayNodes]
  );
  const canvasFitKey = useMemo(() => {
    if (selectedCandidate) {
      return `${selectedCandidate.id}-${selectedCandidate.created_at}`;
    }
      return `${session?.id ?? "review"}-${session?.status ?? "idle"}-${status}`;
  }, [selectedCandidate, session?.id, session?.status, status]);

  useEffect(() => {
    if (!selectedCandidate || !displayNodes.length) {
      setVisibleNodeCount(0);
      return;
    }

    if (!pendingRevealRef.current || lastAnimatedCandidateRef.current === selectedCandidate.id) {
      lastAnimatedCandidateRef.current = selectedCandidate.id;
      setVisibleNodeCount(displayNodes.length);
      return;
    }

    pendingRevealRef.current = false;
    lastAnimatedCandidateRef.current = selectedCandidate.id;
    setSelectedNodeId(preferredNodeId(displayNodes));
    setPopoverPosition(null);
    setVisibleNodeCount(1);

    let currentCount = 1;
    const timer = window.setInterval(() => {
      currentCount += 1;
      if (currentCount >= displayNodes.length) {
        setVisibleNodeCount(displayNodes.length);
        window.clearInterval(timer);
        return;
      }
      setVisibleNodeCount(currentCount);
    }, 180);

    return () => window.clearInterval(timer);
  }, [displayNodes.length, selectedCandidate]);

  const renderedNodes = useMemo(() => {
    if (!displayNodes.length || !visibleNodeCount) return [];
    return displayNodes.slice(0, visibleNodeCount);
  }, [displayNodes, visibleNodeCount]);

  useEffect(() => {
    if (!renderedNodes.length) return;
    setSelectedNodeId((current) => {
      if (current && renderedNodes.some((node) => node.id === current)) {
        return current;
      }
      return preferredNodeId(renderedNodes) ?? renderedNodes[0].id;
    });
  }, [renderedNodes]);

  useEffect(() => {
    const closeOnOutsideClick = (event: globalThis.MouseEvent) => {
      if (!popoverRef.current) return;
      if (popoverRef.current.contains(event.target as Node)) return;
      setPopoverPosition(null);
      setIsFeedbackFocused(false);
    };

    window.addEventListener("mousedown", closeOnOutsideClick);
    return () => window.removeEventListener("mousedown", closeOnOutsideClick);
  }, []);

  const selectedNode = useMemo(
    () => renderedNodes.find((node) => node.id === selectedNodeId) ?? renderedNodes[renderedNodes.length - 1] ?? null,
    [renderedNodes, selectedNodeId]
  );
  const warningItems = selectedCandidate?.warning_items ?? [];
  const selectedNodeCoveredIds = useMemo(
    () => new Set(selectedNode?.coveredStepIds ?? (selectedNode ? [selectedNode.id] : [])),
    [selectedNode]
  );
  const selectedNodeWarnings = useMemo(
    () => warningItems.filter((item) => item.node_ids.some((nodeId) => selectedNodeCoveredIds.has(nodeId))),
    [selectedNodeCoveredIds, warningItems]
  );
  const selectedNodeEditableStepId = selectedNode?.kind === "step" ? selectedNode.editableStepId : null;

  useEffect(() => {
    if (
      !session ||
      !selectedCandidate ||
      !selectedNodeEditableStepId ||
      !feedbackText.trim() ||
      !isFeedbackFocused ||
      isRefreshingPreview
    ) {
      setLiveSuggestions([]);
      setSuggestionStatus("idle");
      return;
    }

    setSuggestionStatus("loading");
    const handle = window.setTimeout(async () => {
      try {
        const response = await fetchLiveSuggestions(
          session.id,
          selectedCandidate.id,
          selectedNodeEditableStepId,
          feedbackText
        );
        setLiveSuggestions(response.suggestions);
        setSuggestionStatus("ready");
      } catch {
        setLiveSuggestions([]);
        setSuggestionStatus("ready");
      }
    }, 3000);

    return () => window.clearTimeout(handle);
  }, [feedbackText, isFeedbackFocused, isRefreshingPreview, selectedCandidate, selectedNodeEditableStepId, session]);

  const suggestionCompletions = useMemo(
    () => liveSuggestions.map((item) => completionSuffix(feedbackText, item)).filter((item) => item.trim().length > 0),
    [feedbackText, liveSuggestions]
  );

  const showSuggestionMenu = Boolean(selectedNodeEditableStepId) && isFeedbackFocused && feedbackText.trim().length > 0;
  const showGeneratingScene =
    isRefreshingPreview || (((session?.status === "generating" || session?.status === "revising") && !selectedCandidate));
  const loadingMode: "generate" | "revise" = session?.status === "revising" ? "revise" : "generate";
  const displayedError = session?.last_error || error;
  const pipelineSummary = selectedCandidate?.summary?.trim() || "No pipeline summary is available yet.";

  const triggerGeneration = () => {
    if (!session) return;
    startTransition(async () => {
      try {
        setError("");
        setStatus("Generating pipeline...");
        setPopoverPosition(null);
        setIsRefreshingPreview(true);
        setSession((current) =>
          current ? { ...current, status: "generating", status_message: "Generating pipeline...", last_error: "" } : current
        );
        await generatePipelines(session.id);
      } catch (err) {
        setIsRefreshingPreview(false);
        setError(err instanceof Error ? err.message : "Failed to start generation.");
      }
    });
  };

  const updatePipeline = () => {
    if (!session || !selectedCandidate || !selectedNodeEditableStepId || !feedbackText.trim()) return;
    startTransition(async () => {
      try {
        setError("");
        setStatus("Revising pipeline from your feedback...");
        setPopoverPosition(null);
        setIsFeedbackFocused(false);
        setLiveSuggestions([]);
        setSuggestionStatus("idle");
        setIsRefreshingPreview(true);
        setSession((current) =>
          current ? { ...current, status: "revising", status_message: "Revising pipeline from your feedback.", last_error: "" } : current
        );
        const summary = await submitFeedback(
          session.id,
          selectedCandidate.id,
          selectedNodeEditableStepId,
          feedbackText
        );
        setSession(summary.session);
        setFeedbackText("");
        setStatus(summary.session.status_message || "Revising pipeline from your feedback.");
      } catch (err) {
        setIsRefreshingPreview(false);
        setSession((current) => (current ? { ...current, status: "review_ready", status_message: "Pipeline ready for review." } : current));
        setError(err instanceof Error ? err.message : "Failed to update the pipeline.");
      }
    });
  };

  const runPipeline = () => {
    if (!session || !selectedCandidate) return;
    startTransition(async () => {
      try {
        const summary = await acceptPipeline(session.id, selectedCandidate.id);
        setSession(summary.session);
        router.push("/output");
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to run the accepted pipeline.");
      }
    });
  };

  const handleNodeClick = (node: DisplayNode, event: ReactMouseEvent<HTMLButtonElement>) => {
    if (showGeneratingScene) return;
    setSelectedNodeId(node.id);
    setActiveDockTab("preview");
    setPopoverPosition(clampPopoverPosition(event.clientX, event.clientY));
  };

  const popover = popoverPosition && selectedNode && !showGeneratingScene
    ? createPortal(
        <div className="pointer-events-none fixed inset-0 z-[120]" aria-hidden="true">
          <div
            ref={popoverRef}
            className="pointer-events-auto fixed w-[460px] overflow-hidden rounded-[28px] border border-slate-200 bg-white shadow-[0_20px_60px_rgba(15,23,42,0.18)]"
            style={{ left: popoverPosition.left, top: popoverPosition.top }}
          >
            <div className="max-h-[78vh] overflow-y-auto">
              <div className="flex items-start justify-between gap-4 border-b border-slate-200 px-5 py-4">
                <div>
                  <div className="text-[1.15rem] font-semibold text-slate-900">{selectedNode.label}</div>
                  <div className="mt-2 text-sm font-medium text-slate-500">{selectedNode.subtitle}</div>
                  {selectedNode.statsLine ? (
                    <div className="mt-1 text-sm text-slate-500">{selectedNode.statsLine}</div>
                  ) : null}
                </div>
                <button
                  className="rounded-full border border-slate-200 px-2.5 py-1 text-sm text-slate-500 transition hover:border-slate-300 hover:text-slate-700"
                  onClick={() => setPopoverPosition(null)}
                  type="button"
                >
                  Close
                </button>
              </div>

              <div className="border-b border-slate-200 px-5 py-4 text-[15px] leading-7 text-slate-700">
                {selectedNode.explanation}
              </div>

              {selectedNode.assessmentReason ? (
                <div
                  className={`border-b px-5 py-3 text-sm ${
                    selectedNode.status === "issue"
                      ? "border-red-200 bg-red-50 text-red-700"
                      : "border-emerald-200 bg-emerald-50 text-emerald-700"
                  }`}
                >
                  {selectedNode.assessmentReason}
                </div>
              ) : null}

              {selectedNodeEditableStepId ? (
                <div className="px-5 py-4">
                  <div className="text-sm font-semibold text-slate-900">Suggest a change</div>
                  <p className="mt-2 text-sm leading-6 text-slate-500">
                    Describe the correction in everyday language and the system will revise this step from here.
                  </p>

                  <div className="relative mt-4">
                    <div className="flex items-center gap-3">
                      <input
                        className="field flex-1 rounded-full"
                        placeholder="Describe what to change in this step"
                        disabled={isPending || isRefreshingPreview}
                        value={feedbackText}
                        onBlur={() => window.setTimeout(() => setIsFeedbackFocused(false), 120)}
                        onChange={(event) => {
                          setFeedbackText(event.target.value);
                          setIsFeedbackFocused(true);
                          setSuggestionStatus("idle");
                        }}
                        onFocus={() => setIsFeedbackFocused(true)}
                      />
                      <button className="primary-button" disabled={isPending || isRefreshingPreview || !feedbackText.trim()} onClick={updatePipeline}>
                        Update
                      </button>
                    </div>

                    {showSuggestionMenu ? (
                      <div className="mt-3 overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm">
                        {suggestionStatus === "loading" ? (
                          <div className="px-4 py-3 text-sm text-slate-500">Generating completions...</div>
                        ) : suggestionCompletions.length ? (
                          suggestionCompletions.map((item) => (
                            <button
                              key={`popover-${feedbackText}-${item}`}
                              className="block w-full border-b border-slate-200 px-4 py-3 text-left text-sm text-slate-700 transition last:border-b-0 hover:bg-slate-50"
                              onMouseDown={(event) => event.preventDefault()}
                              onClick={() => {
                                setFeedbackText((current) => `${current}${item}`);
                                setIsFeedbackFocused(true);
                                setLiveSuggestions([]);
                                setSuggestionStatus("idle");
                              }}
                              type="button"
                            >
                              <span className="text-slate-400">{feedbackText}</span>
                              <span>{item}</span>
                            </button>
                          ))
                        ) : (
                          <div className="px-4 py-3 text-sm text-slate-500">Pause typing for 3 seconds to generate completions.</div>
                        )}
                      </div>
                    ) : null}
                  </div>
                </div>
              ) : (
                <div className="px-5 py-4 text-sm leading-6 text-slate-500">
                  This node is read-only. You can inspect its preview and explanation, but only transformation steps can be revised.
                </div>
              )}
            </div>
          </div>
        </div>,
        document.body
      )
    : null;

  return (
    <div className="flex h-full min-h-0 min-w-0 flex-col gap-4 overflow-hidden">
      <div className="flex items-start justify-between gap-4">
        <div>
          <h1 className="text-4xl font-semibold tracking-tight text-slate-900">Review</h1>
          <p className="mt-2 text-lg text-slate-600">Inspect each step, preview the table at every stage, and describe any change in plain language.</p>
        </div>
        <div className="flex items-center gap-3">
          <button className="soft-button" onClick={() => router.push("/input")}>
            Back to Input
          </button>
          <button
            className="primary-button"
            disabled={showGeneratingScene || isPending || (!selectedCandidate && session?.status !== "ready_for_generation")}
            onClick={selectedCandidate ? runPipeline : triggerGeneration}
          >
            {showGeneratingScene ? (loadingMode === "revise" ? "Revising..." : "Generating...") : selectedCandidate ? "Run Pipeline" : "Generate Pipeline"}
          </button>
        </div>
      </div>

      {!selectedCandidate && !showGeneratingScene && session?.status === "ready_for_generation" ? (
          <section className="card p-8">
          <h2 className="text-2xl font-medium text-slate-900">Pipeline Overview</h2>
          <p className="mt-2 text-sm text-slate-600">Generate the first pipeline candidate to inspect and refine it here.</p>
          <div className="mt-6">
            <button className="primary-button" onClick={triggerGeneration}>
              Generate Pipeline
            </button>
          </div>
        </section>
      ) : null}

      {showGeneratingScene ? (
        <GeneratingPipelineScene
          mode={loadingMode}
          fitKey={`generating-${canvasFitKey}`}
        />
      ) : null}

      {!showGeneratingScene && selectedCandidate && selectedNode ? (
        <div className="grid min-h-0 min-w-0 flex-1 grid-rows-[minmax(0,1.12fr)_minmax(0,0.88fr)] gap-4 overflow-hidden">
          <section className="card flex min-h-0 min-w-0 flex-col overflow-hidden p-6">
            <h2 className="text-[2rem] font-medium text-slate-900">Pipeline Overview</h2>
            <PipelineCanvas
              fitKey={canvasFitKey}
              edges={displayEdges}
              nodes={canvasNodes}
              onNodeClick={(nodeId, event) => {
                const node = displayNodes.find((item) => item.id === nodeId);
                if (!node) return;
                handleNodeClick(node, event);
              }}
              selectedNodeId={selectedNode.id}
              visibleNodeCount={visibleNodeCount}
            />
          </section>

          <section className="card flex min-h-0 min-w-0 flex-col overflow-hidden p-0">
            <div className="border-b border-slate-200 bg-white px-6">
              <div className="flex items-center gap-8 overflow-x-auto">
                {[
                  { id: "preview" as const, label: "Intermediate table preview" },
                  { id: "warning" as const, label: "Warning" },
                  { id: "summary" as const, label: "Summary" }
                ].map((tab) => (
                  <button
                    key={tab.id}
                    className={`shrink-0 border-b-2 py-4 text-[15px] leading-none transition ${
                      activeDockTab === tab.id
                        ? "border-slate-900 font-medium text-slate-900"
                        : "border-transparent font-normal text-slate-400 hover:text-slate-700"
                    }`}
                    onClick={() => setActiveDockTab(tab.id)}
                    type="button"
                  >
                    {tab.label}
                  </button>
                ))}
              </div>
            </div>

            <div className="min-h-0 min-w-0 flex-1 overflow-auto bg-white px-6 py-5">
              {activeDockTab === "preview" ? (
                <div className="flex h-full min-h-0 flex-col">
                  <div className="mb-4 text-sm text-slate-500">Showing the table after: {selectedNode.label}</div>
                  <DataTable rows={selectedNode.previewRows} emptyMessage="No intermediate preview is available for the selected node." />
                </div>
              ) : null}

              {activeDockTab === "warning" ? (
                <div className="space-y-3">
                  {warningItems.length ? (
                    warningItems.map((item) => {
                      const focusNodeId =
                        item.node_ids
                          .map((nodeId) => graph.displayNodeIdByRawNodeId[nodeId])
                          .find(Boolean) ??
                        "node_output";
                      const isActive = selectedNodeWarnings.some((warning) => warning.id === item.id);
                      const warningLabels = Array.from(
                        new Set(
                          item.node_ids
                            .map((nodeId) => graph.displayNodeIdByRawNodeId[nodeId])
                            .map((nodeId) => (nodeId ? displayNodeById.get(nodeId)?.label : null))
                            .filter((label): label is string => Boolean(label))
                        )
                      );
                      return (
                        <button
                          key={item.id}
                          className={`block w-full rounded-[24px] border px-5 py-4 text-left transition ${
                            isActive
                              ? "border-red-200 bg-red-50"
                              : "border-slate-200 bg-slate-50 hover:border-slate-300 hover:bg-white"
                          }`}
                          onClick={() => {
                            setSelectedNodeId(focusNodeId);
                            setActiveDockTab("warning");
                            setPopoverPosition(null);
                          }}
                          type="button"
                        >
                          <div className="flex items-start justify-between gap-4">
                            <div>
                              <div className="text-base font-semibold text-slate-900">{item.title}</div>
                              <p className="mt-2 text-sm leading-6 text-slate-600">{item.detail || "No additional detail."}</p>
                            </div>
                            <span className="rounded-full bg-white px-3 py-1 text-xs font-semibold uppercase tracking-[0.12em] text-slate-400">
                              {warningSourceLabel(item.source)}
                            </span>
                          </div>
                          <div className="mt-3 flex flex-wrap gap-2">
                            {warningLabels.map((label) => (
                              <span key={`${item.id}-${label}`} className="rounded-full bg-white px-3 py-1 text-xs font-medium text-slate-500">
                                {label}
                              </span>
                            ))}
                          </div>
                        </button>
                      );
                    })
                  ) : (
                    <div className="rounded-2xl border border-emerald-200 bg-emerald-50 px-4 py-4 text-sm text-emerald-700">
                      No warnings were produced for this candidate. The current pipeline passed the overall checks.
                    </div>
                  )}
                </div>
              ) : null}

              {activeDockTab === "summary" ? (
                <div className="flex h-full min-h-0 flex-col">
                  <div className="mb-4 text-sm text-slate-500">Pipeline summary</div>
                  <div className="rounded-[24px] border border-slate-200 bg-slate-50 px-5 py-5 text-sm leading-7 text-slate-700">
                    <p className="whitespace-pre-wrap">{pipelineSummary}</p>
                  </div>
                </div>
              ) : null}
            </div>
          </section>
        </div>
      ) : null}

      {status && !showGeneratingScene && !selectedCandidate ? (
        <div className="rounded-2xl border border-blue-200 bg-blue-50 px-4 py-3 text-sm text-blue-700">{status}</div>
      ) : null}
      {displayedError ? <div className="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">{displayedError}</div> : null}
      {popover}
    </div>
  );
}
