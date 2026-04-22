/*
 * SPDX-License-Identifier: MIT
 *
 * Copyright (c) 2025 Jonas Waldeck
 */

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { MapView } from "./MapView";

type Taxon = {
  taxon_id: number;
  swedish_name: string;
  scientific_name: string;
  observations_count: number;
};

type JobSnapshot = {
  job_id: string;
  kind: string;
  status: "queued" | "running" | "done" | "failed" | "cancelled";
  phase: string;
  current_step: string;
  total_steps: number;
  completed_steps: number;
  progress_pct: number;
  eta_seconds: number | null;
  created_at: string | null;
  started_at: string | null;
  finished_at: string | null;
  updated_at: string | null;
  error: string | null;
  traceback_text?: string | null;
  warnings: string[];
  summary: Record<string, unknown>;
  spec: Record<string, unknown>;
  cancel_requested: boolean;
};

type JobsStatusResponse = {
  ok: boolean;
  busy: boolean;
  current_job: JobSnapshot | null;
  last_job: JobSnapshot | null;
};

type JobStartResponse = {
  ok: boolean;
  job_id?: string;
  status?: string;
  error?: string;
  current_job?: JobSnapshot | null;
};

// In dev: prefer proxy (/api -> vite proxy -> backend)
// Only set VITE_API_BASE if you explicitly want direct mode.
const API_BASE = import.meta.env.VITE_API_BASE ? String(import.meta.env.VITE_API_BASE) : "";

function normalizeSlot(s: number) {
  // slots 1..48 wrap
  if (s < 1) return 48 + (s % 48);
  if (s > 48) return ((s - 1) % 48) + 1;
  return s;
}

function makeWindow(center: number, radius: number): number[] {
  if (center === 0) return [0];
  const out: number[] = [];
  for (let d = -radius; d <= radius; d++) out.push(normalizeSlot(center + d));
  return Array.from(new Set(out));
}

function formatEta(seconds: number | null): string {
  if (seconds == null || !Number.isFinite(seconds) || seconds < 0) return "—";
  if (seconds < 60) return `${seconds}s`;
  const mins = Math.floor(seconds / 60);
  const secs = seconds % 60;
  if (mins < 60) return `${mins}m ${secs}s`;
  const hours = Math.floor(mins / 60);
  const remMins = mins % 60;
  return `${hours}h ${remMins}m`;
}

export default function App() {
  // Build/view controls
  const [slotId, setSlotId] = useState(0);
  const [zooms, setZooms] = useState("15,14,13");
  const [zoom, setZoom] = useState(15);
  const [n, setN] = useState(5);
  const [alpha, setAlpha] = useState(2.0);
  const [beta, setBeta] = useState(0.5);

  // Upstream mature controls
  const [forceRebuild, setForceRebuild] = useState(false);
  const [autoFit, setAutoFit] = useState(true);
  const [fitRequestId, setFitRequestId] = useState(0);

  // Window selection
  const [slotCenter, setSlotCenter] = useState<number>(0); // 0 means “all-time”
  const [slotRadius, setSlotRadius] = useState<number>(1); // ±1 quartile default
  const [useWindow, setUseWindow] = useState<boolean>(true);

  const [status, setStatus] = useState<string>("");

  // Jobs state
  const [jobsBusy, setJobsBusy] = useState(false);
  const [currentJob, setCurrentJob] = useState<JobSnapshot | null>(null);
  const [lastJob, setLastJob] = useState<JobSnapshot | null>(null);
  const [activeJobId, setActiveJobId] = useState<string | null>(null);

  // Clicked cell + taxa
  const [clicked, setClicked] = useState<{ x: number; y: number } | null>(null);
  const [taxa, setTaxa] = useState<Taxon[]>([]);
  const [taxaLoading, setTaxaLoading] = useState(false);

  const apiUrl = useCallback((path: string) => (API_BASE ? `${API_BASE}${path}` : path), []);

  // Default slot on first page load: today's month.quartile
  useEffect(() => {
    const now = new Date();
    const month = now.getMonth() + 1; // 1..12
    const day = now.getDate(); // 1..31
    const q = day <= 7 ? 1 : day <= 14 ? 2 : day <= 21 ? 3 : 4;
    const todaySlot = (month - 1) * 4 + q; // 1..48

    setSlotCenter(todaySlot);
    setSlotRadius(1);
    setUseWindow(true);
    setSlotId(todaySlot);
  }, []);

  const slotIdsForView = useMemo(() => {
    if (!useWindow) return [slotCenter];
    return makeWindow(slotCenter, slotRadius);
  }, [slotCenter, slotRadius, useWindow]);

  const parsedZooms = useMemo(() => {
    const zs = zooms
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean)
      .map((s) => Number(s))
      .filter((z) => Number.isFinite(z))
      .map((z) => Math.trunc(z))
      .filter((z) => z >= 1 && z <= 21);

    return Array.from(new Set(zs)).sort((a, b) => b - a);
  }, [zooms]);

  const refreshJobStatus = useCallback(async () => {
    try {
      const res = await fetch(apiUrl("/api/jobs/status"));
      const j = (await res.json()) as JobsStatusResponse;
      if (!res.ok || !j.ok) {
        throw new Error("Failed to fetch job status");
      }
      setJobsBusy(j.busy);
      setCurrentJob(j.current_job);
      setLastJob(j.last_job);
    } catch (err) {
      console.error("Failed to refresh job status", err);
    }
  }, [apiUrl]);

  useEffect(() => {
    void refreshJobStatus();
    const id = window.setInterval(() => {
      void refreshJobStatus();
    }, 1500);
    return () => window.clearInterval(id);
  }, [refreshJobStatus]);

  const prevBusyRef = useRef(false);
  useEffect(() => {
    if (prevBusyRef.current && !jobsBusy) {
      if (autoFit) {
        setFitRequestId((v) => v + 1);
      }
      if (lastJob?.status === "done") {
        setStatus(`Done: ${lastJob.job_id}`);
      } else if (lastJob?.status === "failed") {
        setStatus(`Failed: ${lastJob.error || lastJob.job_id}`);
      } else if (lastJob?.status === "cancelled") {
        setStatus(`Cancelled: ${lastJob.job_id}`);
      }
    }
    prevBusyRef.current = jobsBusy;
  }, [jobsBusy, lastJob, autoFit]);

  const build = useCallback(async () => {
    setStatus("Starting rebuild…");
    setTaxa([]);
    setClicked(null);

    try {
      if (parsedZooms.length === 0) {
        throw new Error("No valid zooms. Example: 15,14,13");
      }

      const slotsToBuild =
        slotId === 0 ? Array.from({ length: 48 }, (_, i) => i + 1) : [slotId];

      const res = await fetch(apiUrl("/api/jobs/rebuild"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          slot_ids: slotsToBuild,
          zooms: parsedZooms,
          n,
          alpha,
          beta,
          force: forceRebuild,
          year_from: 2000,
          year_to: new Date().getFullYear(),
          include_slot0: true,
          include_all_years: true,
        }),
      });

      const text = await res.text();
      let j: JobStartResponse = { ok: false };
      try {
        j = JSON.parse(text) as JobStartResponse;
      } catch {
        // keep fallback below
      }

      if (!res.ok || !j.ok) {
        console.error("Build failed", { status: res.status, body: text });
        throw new Error(j?.error || `HTTP ${res.status}: ${text.slice(0, 200)}`);
      }

      setActiveJobId(j.job_id ?? null);
      setZoom(parsedZooms[0]);
      setStatus(`Job queued: ${j.job_id}`);
      await refreshJobStatus();
    } catch (e: any) {
      setStatus(`Error: ${e?.message || String(e)}`);
    }
  }, [apiUrl, alpha, beta, forceRebuild, n, parsedZooms, refreshJobStatus, slotId]);

  const cancelJob = useCallback(async () => {
    if (!currentJob) return;
    try {
      const res = await fetch(apiUrl(`/api/jobs/${currentJob.job_id}/cancel`), {
        method: "POST",
      });
      const j = await res.json();
      if (!res.ok || !j.ok) {
        throw new Error(j?.error || `HTTP ${res.status}`);
      }
      setStatus(`Cancelling job ${currentJob.job_id}…`);
      await refreshJobStatus();
    } catch (e: any) {
      setStatus(`Cancel error: ${e?.message || String(e)}`);
    }
  }, [apiUrl, currentJob, refreshJobStatus]);

  const onCellClick = useCallback(
    async (p: { x: number; y: number; zoom: number; slotId: number }) => {
      setClicked({ x: p.x, y: p.y });
      setTaxa([]);
      setTaxaLoading(true);

      try {
        const usingWindow =
          Array.isArray(slotIdsForView) && slotIdsForView.length > 0 && !slotIdsForView.includes(0);

        const u = new URL(
          apiUrl(usingWindow ? "/api/cell/taxa_window" : "/api/cell/taxa"),
          window.location.origin
        );

        u.searchParams.set("zoom", String(p.zoom));
        u.searchParams.set("x", String(p.x));
        u.searchParams.set("y", String(p.y));
        u.searchParams.set("limit", "200");

        if (usingWindow) {
          u.searchParams.set("slot_ids", slotIdsForView.join(","));
        } else {
          u.searchParams.set("slot_id", String(p.slotId));
        }

        const res = await fetch(u.toString());
        const j = await res.json();

        if (!res.ok) throw new Error(j?.error || `HTTP ${res.status}`);
        setTaxa(j as Taxon[]);
      } catch (err) {
        console.error("Failed to fetch taxa", err);
        setTaxa([]);
      } finally {
        setTaxaLoading(false);
      }
    },
    [apiUrl, slotIdsForView]
  );

  const displayedJob = currentJob ?? lastJob;

  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "360px minmax(0, 1fr)",
        height: "100vh",
        width: "100vw",
      }}
    >
      <div style={{ padding: 12, borderRight: "1px solid #ddd", overflow: "auto" }}>
        <h2>Geomap</h2>

        <div>
          <label>Build slot</label>
          <input
            type="number"
            value={slotId}
            min={0}
            max={48}
            onChange={(e) => setSlotId(Number(e.target.value))}
            disabled={jobsBusy}
          />
          <div style={{ fontSize: 12, opacity: 0.7 }}>
            slot_id: 0 = build all 1..48 and derive slot 0, 1..48 = specific seasonal slot
          </div>
        </div>

        <div>
          <label>Zooms</label>
          <input value={zooms} onChange={(e) => setZooms(e.target.value)} disabled={jobsBusy} />
          <div style={{ fontSize: 12, opacity: 0.7 }}>Example: 15,14,13</div>
        </div>

        <div>
          <label>N species (0 = all)</label>
          <input
            type="number"
            value={n}
            min={0}
            onChange={(e) => setN(Number(e.target.value))}
            disabled={jobsBusy}
          />
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
          <div>
            <label>alpha</label>
            <input
              type="number"
              value={alpha}
              onChange={(e) => setAlpha(Number(e.target.value))}
              disabled={jobsBusy}
            />
          </div>
          <div>
            <label>beta</label>
            <input
              type="number"
              value={beta}
              onChange={(e) => setBeta(Number(e.target.value))}
              disabled={jobsBusy}
            />
          </div>
        </div>

        {status && (
          <div style={{ marginTop: 8, fontSize: 12, whiteSpace: "pre-wrap" }}>
            {status}
          </div>
        )}
        {(currentJob || lastJob) && (
          <div
            style={{
              marginTop: 10,
              padding: 10,
              border: "1px solid #ddd",
              borderRadius: 6,
              background: currentJob ? "#fffbe6" : "#f7f7f7",
              fontSize: 12,
            }}
          >
            <div>
              <b>{currentJob ? "Job running" : "Last job"}</b>
              {displayedJob?.job_id ? `: ${displayedJob.job_id}` : ""}              
            </div>
            {displayedJob && (
              <>
                <div>Status: {displayedJob.status}</div>
                <div>Phase: {displayedJob.phase}</div>
                <div>Step: {displayedJob.current_step || "—"}</div>
                <div>
                  Progress: {displayedJob.completed_steps} / {displayedJob.total_steps} ({displayedJob.progress_pct}%)
                </div>
                <div>ETA: {formatEta(displayedJob.eta_seconds)}</div>
                {activeJobId && displayedJob.job_id === activeJobId && (
                  <div style={{ opacity: 0.7 }}>Tracking active job</div>
                )}
                {displayedJob.error && <div style={{ color: "crimson" }}>Error: {displayedJob.error}</div>}
                {displayedJob.warnings.length > 0 && (
                  <div style={{ color: "#8a6d3b" }}>Warnings: {displayedJob.warnings.length}</div>
                )}
              </>
            )}
          </div>
        )}
        <hr />
        <div style={{ display: "grid", gap: 8 }}>
          <div>
            <label>Center slot</label>
            <input
              type="number"
              value={slotCenter}
              min={0}
              max={48}
              onChange={(e) => setSlotCenter(Number(e.target.value))}
            />
          </div>
          <label style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <input
              type="checkbox"
              checked={useWindow}
              onChange={(e) => setUseWindow(e.target.checked)}
            />
            Use window
          </label>

          <div>
            <label>Radius (quartiles)</label>
            <input
              type="number"
              value={slotRadius}
              min={0}
              max={24}
              onChange={(e) => setSlotRadius(Number(e.target.value))}
              disabled={!useWindow}
            />
            <div style={{ fontSize: 12, opacity: 0.7 }}>Window slots: {slotIdsForView.join(",")}</div>
          </div>

          <div style={{ display: "flex", gap: 8 }}>
            <button type="button" onClick={() => setSlotCenter((v) => normalizeSlot(v - 1))} style={{ flex: 1 }}>
              ◀ Prev
            </button>
            <button type="button" onClick={() => setSlotCenter((v) => normalizeSlot(v + 1))} style={{ flex: 1 }}>
              Next ▶
            </button>
          </div>
        </div>

        <hr />

        <div style={{ marginTop: 8 }}>
          <label style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <input type="checkbox" checked={autoFit} onChange={(e) => setAutoFit(e.target.checked)} />
            Auto-fit to hotmap after build
          </label>
        </div>

        <div style={{ marginTop: 8 }}>
          <label style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <input
              type="checkbox"
              checked={forceRebuild}
              onChange={(e) => setForceRebuild(e.target.checked)}
              disabled={jobsBusy}
            />
            Force rebuild
          </label>
        </div>

        <div style={{ display: "flex", gap: 8, marginTop: 10 }}>
          <button type="button" onClick={() => setFitRequestId((v) => v + 1)} style={{ flex: 1 }}>
            Fit now
          </button>

          <button disabled={jobsBusy} onClick={build} style={{ flex: 1 }}>
            {jobsBusy ? "Job running…" : "Build / Refresh"}
          </button>
        </div>

        {currentJob && (
          <div style={{ display: "flex", gap: 8, marginTop: 8 }}>
            <button type="button" onClick={cancelJob} style={{ flex: 1 }}>
              Cancel job
            </button>
          </div>
        )}

        <hr />

        <div>
          <label>View zoom</label>
          <input type="number" value={zoom} onChange={(e) => setZoom(Number(e.target.value))} />
        </div>

        <hr />

        <h3>Clicked cell</h3>

        {!clicked && <div style={{ opacity: 0.7 }}>Click a cell on the map</div>}

        {clicked && (
          <div>
            <div>
                   x={clicked.x} y={clicked.y}
            </div>

            <div style={{ marginTop: 8 }}>
              {taxaLoading && <div style={{ opacity: 0.7 }}>Loading taxa…</div>}

              {!taxaLoading && taxa.length === 0 && <div style={{ opacity: 0.7 }}>No taxa</div>}

              {!taxaLoading && taxa.length > 0 && (
                <ul style={{ paddingLeft: 18 }}>
                  {taxa.slice(0, 80).map((t) => (
                    <li key={t.taxon_id}>
                      <b>{t.swedish_name || t.scientific_name || t.taxon_id}</b> ({t.observations_count})
                    </li>
                  ))}
                </ul>
              )}
            </div>
          </div>
        )}
	    </div>

      <div style={{ position: "relative", height: "100%", minWidth: 0 }}>
        <MapView
          apiBase={API_BASE}
          zoom={zoom}
          slotId={slotId}
          slotIds={slotIdsForView}
          selected={clicked}
          fitRequestId={fitRequestId}
          onCellClick={onCellClick}
        />
      </div>
    </div>
  );
}
