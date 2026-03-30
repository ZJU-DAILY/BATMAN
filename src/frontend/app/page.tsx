"use client";

import { useState, useTransition } from "react";
import { useRouter } from "next/navigation";

import { createSession, setStoredSessionId } from "@/lib/api";


function PanelHeader({
  title,
  accentClass,
  titleClass,
}: {
  title: string;
  accentClass: string;
  titleClass: string;
}) {
  return (
    <div className="space-y-3">
      <div className={`h-1 w-20 rounded-full ${accentClass}`} />
      <div className={`text-sm font-semibold uppercase tracking-[0.22em] ${titleClass}`}>{title}</div>
    </div>
  );
}


function FlowArrow() {
  return (
    <div className="relative flex h-full min-h-0 items-center justify-center">
      <div className="absolute inset-y-5 left-1/2 w-px -translate-x-1/2 bg-slate-200" />
      <div className="relative flex items-center justify-center rounded-full border border-slate-200 bg-white px-3 py-2 text-slate-400 shadow-sm">
        <svg width="42" height="18" viewBox="0 0 42 18" fill="none" aria-hidden="true">
          <path d="M5 9h22" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
          <path d="m22 4 8 5-8 5" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      </div>
    </div>
  );
}


function SkeletonBar({
  widthClass,
  toneClass = "bg-slate-200",
}: {
  widthClass: string;
  toneClass?: string;
}) {
  return <div className={`h-2.5 rounded-full ${toneClass} ${widthClass}`} />;
}


function FlowModule({
  label,
  children,
  className = "",
  labelClassName = "text-slate-500",
}: {
  label: string;
  children: React.ReactNode;
  className?: string;
  labelClassName?: string;
}) {
  return (
    <div className={`rounded-[22px] border border-slate-200 bg-white/88 p-4 ${className}`}>
      <div className={`text-xs font-semibold uppercase tracking-[0.18em] ${labelClassName}`}>{label}</div>
      <div className="mt-4 min-h-0">{children}</div>
    </div>
  );
}


function TableGhost({ accentClass }: { accentClass: string }) {
  return (
    <div className="rounded-[18px] border border-slate-200 bg-white/92 p-3.5">
      <div className={`h-2.5 w-16 rounded-full ${accentClass}`} />
      <div className="mt-4 grid grid-cols-4 gap-2">
        {Array.from({ length: 4 }).map((_, index) => (
          <SkeletonBar key={`head-${index}`} widthClass="w-full" toneClass="bg-slate-200" />
        ))}
      </div>
      <div className="mt-3 space-y-2.5">
        <div className="grid grid-cols-4 gap-2">
          {Array.from({ length: 4 }).map((_, index) => (
            <SkeletonBar key={`row-a-${index}`} widthClass="w-full" toneClass="bg-slate-100" />
          ))}
        </div>
        <div className="grid grid-cols-4 gap-2">
          {Array.from({ length: 4 }).map((_, index) => (
            <SkeletonBar key={`row-b-${index}`} widthClass="w-full" toneClass="bg-slate-100" />
          ))}
        </div>
        <div className="grid grid-cols-4 gap-2">
          {Array.from({ length: 4 }).map((_, index) => (
            <SkeletonBar key={`row-c-${index}`} widthClass="w-full" toneClass="bg-slate-100" />
          ))}
        </div>
      </div>
    </div>
  );
}


function SchemaGhost() {
  return (
    <div className="rounded-[18px] border border-blue-200/80 bg-blue-50/90 p-4">
      <div className="grid grid-cols-[minmax(0,1.4fr)_minmax(0,0.9fr)_minmax(0,1fr)] gap-2">
        <SkeletonBar widthClass="w-full" toneClass="bg-blue-200" />
        <SkeletonBar widthClass="w-full" toneClass="bg-blue-200" />
        <SkeletonBar widthClass="w-full" toneClass="bg-blue-200" />
      </div>
      <div className="mt-4 space-y-2.5">
        <div className="grid grid-cols-[minmax(0,1.4fr)_minmax(0,0.9fr)_minmax(0,1fr)] gap-2">
          <SkeletonBar widthClass="w-[86%]" toneClass="bg-white" />
          <SkeletonBar widthClass="w-[70%]" toneClass="bg-white" />
          <SkeletonBar widthClass="w-full" toneClass="bg-white" />
        </div>
        <div className="grid grid-cols-[minmax(0,1.4fr)_minmax(0,0.9fr)_minmax(0,1fr)] gap-2">
          <SkeletonBar widthClass="w-[78%]" toneClass="bg-white" />
          <SkeletonBar widthClass="w-[64%]" toneClass="bg-white" />
          <SkeletonBar widthClass="w-[92%]" toneClass="bg-white" />
        </div>
        <div className="grid grid-cols-[minmax(0,1.4fr)_minmax(0,0.9fr)_minmax(0,1fr)] gap-2">
          <SkeletonBar widthClass="w-[82%]" toneClass="bg-white" />
          <SkeletonBar widthClass="w-[68%]" toneClass="bg-white" />
          <SkeletonBar widthClass="w-[84%]" toneClass="bg-white" />
        </div>
      </div>
    </div>
  );
}


function PipelineStepsGhost() {
  return (
    <div className="grid min-w-0 grid-cols-[minmax(0,1fr)_12px_minmax(0,1fr)_12px_minmax(0,1fr)] items-center gap-1.5">
      {Array.from({ length: 3 }).map((_, index) => (
        <div key={index} className="contents">
          <div className="min-w-0 overflow-hidden rounded-[18px] border border-emerald-200/80 bg-emerald-50/45 px-2.5 py-3">
            <div className="h-2.5 w-[72%] max-w-full rounded-full bg-emerald-300" />
            <div className="mt-3 space-y-2">
              <SkeletonBar widthClass="w-[72%]" toneClass="bg-white" />
              <SkeletonBar widthClass="w-[50%]" toneClass="bg-white" />
            </div>
          </div>
          {index < 2 ? <div className="h-px min-w-0 w-full bg-slate-300" /> : null}
        </div>
      ))}
    </div>
  );
}


function ExplanationGhost() {
  return (
    <div className="space-y-3">
      <SkeletonBar widthClass="w-40" toneClass="bg-emerald-200" />
      <div className="space-y-2.5">
        <SkeletonBar widthClass="w-full" />
        <SkeletonBar widthClass="w-[86%]" />
        <SkeletonBar widthClass="w-[68%]" />
        <SkeletonBar widthClass="w-[93%]" />
      </div>
    </div>
  );
}


function TargetTableGhost() {
  return (
    <div className="space-y-3">
      <div className="grid grid-cols-4 gap-2">
        {Array.from({ length: 4 }).map((_, index) => (
          <SkeletonBar key={`target-head-${index}`} widthClass="w-full" toneClass="bg-slate-200" />
        ))}
      </div>
      <div className="grid grid-cols-4 gap-2">
        {Array.from({ length: 8 }).map((_, index) => (
          <SkeletonBar key={`target-row-a-${index}`} widthClass="w-full" toneClass="bg-emerald-100" />
        ))}
      </div>
      <div className="grid grid-cols-4 gap-2">
        {Array.from({ length: 8 }).map((_, index) => (
          <SkeletonBar key={`target-row-b-${index}`} widthClass="w-full" toneClass="bg-slate-100" />
        ))}
      </div>
    </div>
  );
}


function ExportGhost() {
  return (
    <div className="grid grid-cols-4 gap-2">
      {Array.from({ length: 4 }).map((_, index) => (
        <div key={index} className="rounded-[16px] border border-slate-200 bg-slate-50 px-2.5 py-3">
          <div className="mx-auto h-2.5 w-10 rounded-full bg-slate-300" />
        </div>
      ))}
    </div>
  );
}


function FlowSegment({
  title,
  accentClass,
  titleClass,
  children,
}: {
  title: string;
  accentClass: string;
  titleClass: string;
  children: React.ReactNode;
}) {
  return (
    <div className="flex min-h-0 min-w-0 flex-col gap-4 px-1 py-1">
      <PanelHeader accentClass={accentClass} title={title} titleClass={titleClass} />
      <div className="grid min-h-0 flex-1 gap-4">{children}</div>
    </div>
  );
}


function InputSegment() {
  return (
    <FlowSegment
      accentClass="bg-[linear-gradient(90deg,#2563eb_0%,#60a5fa_100%)]"
      title="Input"
      titleClass="text-blue-700"
    >
      <FlowModule label="Input Tables">
        <div className="grid grid-cols-2 gap-3">
          <TableGhost accentClass="bg-blue-200" />
          <TableGhost accentClass="bg-blue-200" />
        </div>
      </FlowModule>

      <FlowModule
        className="border-blue-200/90 bg-blue-50/75"
        label="Target Schema"
        labelClassName="text-blue-700"
      >
        <SchemaGhost />
      </FlowModule>
    </FlowSegment>
  );
}


function ReviewSegment() {
  return (
    <FlowSegment
      accentClass="bg-[linear-gradient(90deg,#16a34a_0%,#34d399_100%)]"
      title="Review"
      titleClass="text-emerald-700"
    >
      <FlowModule label="Pipeline Steps">
        <PipelineStepsGhost />
      </FlowModule>

      <FlowModule label="Plain-language Explanation">
        <ExplanationGhost />
      </FlowModule>
    </FlowSegment>
  );
}


function OutputSegment() {
  return (
    <FlowSegment
      accentClass="bg-[linear-gradient(90deg,#0f172a_0%,#475569_100%)]"
      title="Output"
      titleClass="text-slate-700"
    >
      <FlowModule label="Target Table">
        <TargetTableGhost />
      </FlowModule>

      <FlowModule label="Export">
        <ExportGhost />
      </FlowModule>
    </FlowSegment>
  );
}


export default function HomePage() {
  const router = useRouter();
  const [error, setError] = useState("");
  const [status, setStatus] = useState("");
  const [isPending, startTransition] = useTransition();

  const handleStart = () => {
    startTransition(async () => {
      try {
        setStatus("Opening a fresh input session...");
        setError("");
        const summary = await createSession();
        setStoredSessionId(summary.session.id);
        router.push("/input");
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to open a new session.");
      }
    });
  };

  return (
    <div className="flex h-full min-h-0 flex-col gap-4 overflow-hidden">
      <section className="card relative flex min-h-0 flex-1 overflow-hidden px-6 py-6 md:px-8 md:py-7">
        <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_12%_16%,rgba(59,130,246,0.12),transparent_24%),radial-gradient(circle_at_88%_14%,rgba(16,185,129,0.09),transparent_20%),linear-gradient(180deg,#ffffff_0%,#f5f8fc_100%)]" />

        <div className="relative mx-auto flex h-full w-full max-w-[1600px] flex-col justify-center">
          <div className="flex flex-[0_0_35%] items-center justify-center">
            <div className="flex w-full max-w-[860px] flex-col items-center text-center">
              <div className="inline-flex rounded-full border border-blue-100 bg-white/86 px-4 py-2 text-sm font-semibold uppercase tracking-[0.24em] text-slate-600 shadow-sm">
                Automatic Data Preparation
              </div>
              <h1 className="mt-4 text-[clamp(4.4rem,7vw,7.3rem)] font-semibold leading-[0.9] tracking-tight text-slate-950">
                BATMAN
              </h1>
              <button className="primary-button mt-4 px-8 py-3 text-base" disabled={isPending} onClick={handleStart}>
                START
              </button>
              <p className="mt-3 text-[1rem] text-slate-500">From raw tables to ready-to-export output.</p>
            </div>
          </div>

          <div className="flex flex-[0_0_49%] items-center justify-center">
            <div className="w-full rounded-[36px] border border-slate-200/90 bg-white/88 p-5 shadow-[0_24px_70px_rgba(15,23,42,0.06)]">
              <div className="grid min-h-[380px] grid-cols-[minmax(0,0.98fr)_80px_minmax(0,1.1fr)_80px_minmax(0,0.98fr)] items-stretch gap-0">
                <InputSegment />
                <FlowArrow />
                <ReviewSegment />
                <FlowArrow />
                <OutputSegment />
              </div>
            </div>
          </div>
        </div>
      </section>

      {status ? <div className="rounded-2xl border border-blue-200 bg-blue-50 px-4 py-3 text-sm text-blue-700">{status}</div> : null}
      {error ? <div className="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">{error}</div> : null}
    </div>
  );
}
