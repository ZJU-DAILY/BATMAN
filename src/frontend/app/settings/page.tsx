"use client";

import { ChangeEvent, useEffect, useState, useTransition } from "react";

import { saveSettings } from "@/lib/api";
import { ensureSessionSummary } from "@/lib/session";
import { GenerationSettings, Session } from "@/lib/types";


export default function SettingsPage() {
  const [session, setSession] = useState<Session | null>(null);
  const [settings, setSettings] = useState<GenerationSettings | null>(null);
  const [status, setStatus] = useState("");
  const [error, setError] = useState("");
  const [isPending, startTransition] = useTransition();

  useEffect(() => {
    ensureSessionSummary()
      .then((summary) => {
        setSession(summary.session);
        setSettings(summary.session.settings);
      })
      .catch((err) => setError(err instanceof Error ? err.message : "Failed to load Settings."));
  }, []);

  const updateSettings = <K extends keyof GenerationSettings>(key: K, value: GenerationSettings[K]) => {
    if (!settings) return;
    setSettings({ ...settings, [key]: value });
  };

  const updateNumberSetting = <K extends keyof GenerationSettings>(key: K, parser: (value: string) => GenerationSettings[K]) =>
    (event: ChangeEvent<HTMLInputElement>) => {
      updateSettings(key, parser(event.target.value));
    };

  const persist = () => {
    if (!session || !settings) return;
    startTransition(async () => {
      try {
        const summary = await saveSettings(session.id, settings);
        setSession(summary.session);
        setStatus("Settings updated for the current session.");
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to save Settings.");
      }
    });
  };

  return (
    <div className="flex h-full min-h-0 flex-col gap-4 overflow-hidden">
      <div className="flex items-start justify-between gap-4">
        <div>
          <h1 className="text-4xl font-semibold tracking-tight text-slate-900">Settings</h1>
          <p className="mt-2 text-lg text-slate-600">Configure BAT search parameters for the current session.</p>
        </div>
        <div className="flex items-center gap-3">
          <button className="primary-button" disabled={isPending || !settings} onClick={persist}>
            Save Settings
          </button>
        </div>
      </div>

      <section className="card p-6">
        <div className="grid gap-6 xl:grid-cols-[7fr_5fr]">
          <div>
            <h2 className="text-[2rem] font-medium text-slate-900">BAT Parameters</h2>
            <p className="mt-2 max-w-3xl text-sm leading-7 text-slate-600">
              These defaults follow the BAT reference setup: rollout steps <span className="font-semibold text-slate-900">10</span>,
              max depth <span className="font-semibold text-slate-900">5</span>, exploration constant <span className="font-semibold text-slate-900">1.0</span>,
              temperature <span className="font-semibold text-slate-900">0.1</span>, and top-p <span className="font-semibold text-slate-900">0.8</span>.
            </p>

            <div className="mt-6 grid gap-4 md:grid-cols-3">
              <div className="rounded-2xl bg-slate-50 p-5">
                <div className="text-sm text-slate-500">Rollout steps</div>
                <div className="mt-2 text-xl font-semibold text-slate-900">{settings?.bat_max_rollout_steps ?? 10}</div>
              </div>
              <div className="rounded-2xl bg-slate-50 p-5">
                <div className="text-sm text-slate-500">Max depth</div>
                <div className="mt-2 text-xl font-semibold text-slate-900">{settings?.bat_max_depth ?? 5}</div>
              </div>
              <div className="rounded-2xl bg-slate-50 p-5">
                <div className="text-sm text-slate-500">Exploration constant</div>
                <div className="mt-2 text-xl font-semibold text-slate-900">{settings?.bat_exploration_constant ?? 1}</div>
              </div>
            </div>
          </div>

          <div className="rounded-2xl bg-slate-50 p-5">
            <div className="text-sm font-semibold uppercase tracking-[0.18em] text-slate-500">What changes here</div>
            <div className="mt-4 space-y-3">
              {[
                "Rollout Steps controls how many search rollouts BAT can perform before stopping.",
                "Max Depth controls how deep the search can go while building a candidate pipeline.",
                "Exploration Constant controls how strongly BAT explores less-visited branches.",
                "Temperature and Top-p control generation randomness for BAT prompts."
              ].map((line) => (
                <div key={line} className="rounded-2xl border border-slate-200 bg-white px-4 py-3 text-sm text-slate-700">
                  {line}
                </div>
              ))}
            </div>
          </div>
        </div>
      </section>

      <section className="grid min-h-0 flex-1 gap-4 overflow-hidden xl:grid-cols-3">
        <div className="card p-6">
          <h2 className="text-[1.65rem] font-medium text-slate-900">Search Budget</h2>
          <div className="mt-5 space-y-4">
            <label className="block">
              <div className="text-sm font-medium text-slate-700">Max rollout steps</div>
              <input
                className="field mt-2"
                min={1}
                max={30}
                type="number"
                value={settings?.bat_max_rollout_steps ?? 10}
                onChange={updateNumberSetting("bat_max_rollout_steps", (value) => Math.max(1, Math.min(30, Number(value) || 10)))}
              />
            </label>
            <label className="block">
              <div className="text-sm font-medium text-slate-700">Max depth</div>
              <input
                className="field mt-2"
                min={1}
                max={10}
                type="number"
                value={settings?.bat_max_depth ?? 5}
                onChange={updateNumberSetting("bat_max_depth", (value) => Math.max(1, Math.min(10, Number(value) || 5)))}
              />
            </label>
            <label className="block">
              <div className="text-sm font-medium text-slate-700">Exploration constant</div>
              <input
                className="field mt-2"
                min={0.1}
                max={3}
                step={0.1}
                type="number"
                value={settings?.bat_exploration_constant ?? 1}
                onChange={updateNumberSetting("bat_exploration_constant", (value) => Math.max(0.1, Math.min(3, Number(value) || 1)))}
              />
            </label>
          </div>
        </div>

        <div className="card p-6">
          <h2 className="text-[1.65rem] font-medium text-slate-900">LLM Sampling</h2>
          <div className="mt-5 space-y-4">
            <label className="block">
              <div className="text-sm font-medium text-slate-700">Temperature</div>
              <input
                className="field mt-2"
                min={0}
                max={1}
                step={0.05}
                type="number"
                value={settings?.bat_temperature ?? 0.1}
                onChange={updateNumberSetting("bat_temperature", (value) => Math.max(0, Math.min(1, Number(value) || 0.1)))}
              />
            </label>
            <label className="block">
              <div className="text-sm font-medium text-slate-700">Top-p</div>
              <input
                className="field mt-2"
                min={0.1}
                max={1}
                step={0.05}
                type="number"
                value={settings?.bat_top_p ?? 0.8}
                onChange={updateNumberSetting("bat_top_p", (value) => Math.max(0.1, Math.min(1, Number(value) || 0.8)))}
              />
            </label>
          </div>
        </div>

        <div className="card p-6">
          <div className="flex items-center justify-between gap-3">
            <h2 className="text-[1.65rem] font-medium text-slate-900">LLM Config</h2>
            <span className="rounded-full bg-slate-100 px-3 py-1 text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">
              Coming soon
            </span>
          </div>
          <div className="mt-5 space-y-4">
            <label className="block">
              <div className="text-sm font-medium text-slate-700">Base URL</div>
              <input
                className="field mt-2 cursor-not-allowed border-slate-200 bg-slate-50 text-slate-400"
                disabled
                placeholder="https://api.example.com/v1"
                value=""
                readOnly
              />
            </label>
            <label className="block">
              <div className="text-sm font-medium text-slate-700">API Key</div>
              <input
                className="field mt-2 cursor-not-allowed border-slate-200 bg-slate-50 text-slate-400"
                disabled
                placeholder="sk-..."
                value=""
                readOnly
                type="password"
              />
            </label>
          </div>
          <div className="mt-5 rounded-2xl border border-slate-200 bg-slate-50 px-4 py-4 text-sm text-slate-500">
            This section is a visual placeholder for future model endpoint configuration.
          </div>
        </div>
      </section>

      {status ? <div className="rounded-2xl border border-blue-200 bg-blue-50 px-4 py-3 text-sm text-blue-700">{status}</div> : null}
      {error ? <div className="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">{error}</div> : null}
    </div>
  );
}
