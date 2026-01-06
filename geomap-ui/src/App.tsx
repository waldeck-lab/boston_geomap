/*
 * SPDX-License-Identifier: MIT
 *
 * Copyright (c) 2025 Jonas Waldeck
 */

import { useCallback, useEffect, useMemo, useState } from "react";
import { MapView } from "./MapView";

type Taxon = {
    taxon_id: number;
    swedish_name: string;
    scientific_name: string;
    observations_count: number;
};

const API_BASE = import.meta.env.VITE_API_BASE ?? "";

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

export default function App() {
    // Build/view controls
    const [slotId, setSlotId] = useState(0);
    const [zooms, setZooms] = useState("15,14,13");
    const [zoom, setZoom] = useState(15);
    const [n, setN] = useState(100);
    const [alpha, setAlpha] = useState(2.0);
    const [beta, setBeta] = useState(0.5);
    const [busy, setBusy] = useState(false);

    // Upstream mature controls
    const [forceRebuild, setForceRebuild] = useState(false);
    const [autoFit, setAutoFit] = useState(true);
    const [fitRequestId, setFitRequestId] = useState(0);

    // Window selection (used once backend window endpoints exist)
    const [slotCenter, setSlotCenter] = useState<number>(0); // 0 means “all-time”
    const [slotRadius, setSlotRadius] = useState<number>(1); // ±1 quartile default
    const [useWindow, setUseWindow] = useState<boolean>(true);

    const [status, setStatus] = useState<string>("");

    // Clicked cell + taxa
    const [clicked, setClicked] = useState<{ x: number; y: number } | null>(null);
    const [taxa, setTaxa] = useState<Taxon[]>([]);
    const [taxaLoading, setTaxaLoading] = useState(false);

    const apiUrl = useCallback(
	(path: string) => (API_BASE ? `${API_BASE}${path}` : path),
	[]
    );

    // Default slot on first page load: today's month.quartile
    useEffect(() => {
	const now = new Date();
	const month = now.getMonth() + 1; // 1..12
	const day = now.getDate(); // 1..31
	const q = day <= 7 ? 1 : day <= 14 ? 2 : day <= 21 ? 3 : 4;
	const todaySlot = (month - 1) * 4 + q; // 1..48

	setSlotId(todaySlot);
	setSlotCenter(todaySlot);
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

    const build = useCallback(async () => {
	setBusy(true);
	setStatus("Building…");
	setTaxa([]);
	setClicked(null);

	try {
	    if (parsedZooms.length === 0) {
		throw new Error("No valid zooms. Example: 15,14,13");
	    }

	    const res = await fetch(apiUrl("/api/pipeline/build"), {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify({
		    slot_id: slotId,
		    zooms: parsedZooms,
		    n,
		    alpha,
		    beta,
		    force: forceRebuild,
		}),
	    });

	    const j = await res.json().catch(() => ({} as any));
	    if (!res.ok || !j.ok) throw new Error(j.error || `HTTP ${res.status}`);

	    setZoom(parsedZooms[0]); // highest zoom
	    setStatus(
		`OK. Built slot ${slotId}, zooms ${parsedZooms.join(",")}${forceRebuild ? " (forced)" : ""}`
	    );

	    // Trigger a fit request AFTER the zoom is set
	    if (autoFit) setFitRequestId((v) => v + 1);
	} catch (e: any) {
	    setStatus(`Error: ${e?.message || String(e)}`);
	} finally {
	    setBusy(false);
	}
    }, [apiUrl, alpha, beta, n, parsedZooms, slotId, forceRebuild, autoFit]);

    const onCellClick = useCallback(
	async (p: { x: number; y: number; zoom: number; slotId: number }) => {
	    setClicked({ x: p.x, y: p.y });
	    setTaxa([]);
	    setTaxaLoading(true);

	    try {
		const useSlotWindow = Array.isArray(slotIdsForView) && slotIdsForView.length > 1;

		const path = useSlotWindow ? "/api/cell/taxa_window" : "/api/cell/taxa";
		const u = new URL(apiUrl(path), window.location.origin);

		u.searchParams.set("zoom", String(p.zoom));
		u.searchParams.set("x", String(p.x));
		u.searchParams.set("y", String(p.y));
		u.searchParams.set("limit", "200");

		if (useSlotWindow) {
		    // pass the *view window*, not only the clicked feature slot
		    u.searchParams.set("slot_ids", slotIdsForView.join(","));
		} else {
		    u.searchParams.set("slot_id", String(p.slotId));
		}

		const res = await fetch(u.toString());
		const j = await res.json();

		if (!res.ok) {
		    throw new Error(j?.error || `HTTP ${res.status}`);
		}

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
            <label>Slot</label>
            <input
		type="number"
		value={slotId}
		min={0}
		max={48}
		onChange={(e) => setSlotId(Number(e.target.value))}
            />
            <div style={{ fontSize: 12, opacity: 0.7 }}>
		slot_id: 0 = all-time, 1..48 = month.quartile
            </div>
        </div>

        <div>
            <label>Zooms</label>
            <input value={zooms} onChange={(e) => setZooms(e.target.value)} />
            <div style={{ fontSize: 12, opacity: 0.7 }}>Example: 15,14,13</div>
        </div>

        <div>
            <label>N species (0 = all)</label>
            <input type="number" value={n} min={0} onChange={(e) => setN(Number(e.target.value))} />
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
            <div>
		<label>alpha</label>
		<input type="number" value={alpha} onChange={(e) => setAlpha(Number(e.target.value))} />
            </div>
            <div>
		<label>beta</label>
		<input type="number" value={beta} onChange={(e) => setBeta(Number(e.target.value))} />
            </div>
        </div>

        {status && (
            <div style={{ marginTop: 8, fontSize: 12, whiteSpace: "pre-wrap" }}>
		{status}
            </div>
        )}

        {/* Window controls (will be used once we add /api/hotmap_window etc.) */}
        <hr />
        <h3 style={{ marginBottom: 6 }}>Time window (preview)</h3>
        <div style={{ fontSize: 12, opacity: 0.7, marginBottom: 6 }}>
            This will control multi-slot viewing once window endpoints are added.
        </div>
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
		    max={8}
		    onChange={(e) => setSlotRadius(Number(e.target.value))}
		    disabled={!useWindow}
		/>
		<div style={{ fontSize: 12, opacity: 0.7 }}>
		    slotsForView: {slotIdsForView.join(",")}
		</div>
            </div>

            <div style={{ display: "flex", gap: 8 }}>
		<button type="button" onClick={() => setSlotCenter((v) => normalizeSlot(v - 1))} style={{ flex: 1 }}>
		    ◀ Prev
		</button>
		<button type="button" onClick={() => setSlotCenter((v) => normalizeSlot(v + 1))} style={{ flex: 1 }}>
		    Next ▶
		</button>
            </div>

            <div style={{ fontSize: 12, opacity: 0.7 }}>
		Note: build uses Slot above (slotId). Window viewing will use Center/Radius later.
            </div>
        </div>

        <hr />

        {/* Upstream controls */}
        <div style={{ marginTop: 8 }}>
            <label style={{ display: "flex", alignItems: "center", gap: 8 }}>
		<input
		    type="checkbox"
		    checked={autoFit}
		    onChange={(e) => setAutoFit(e.target.checked)}
		/>
		Auto-fit to hotmap after build
            </label>
        </div>

        <div style={{ marginTop: 8 }}>
            <label style={{ display: "flex", alignItems: "center", gap: 8 }}>
		<input
		    type="checkbox"
		    checked={forceRebuild}
		    onChange={(e) => setForceRebuild(e.target.checked)}
		/>
		Force rebuild
            </label>
        </div>

        <div style={{ display: "flex", gap: 8, marginTop: 10 }}>
            <button type="button" onClick={() => setFitRequestId((v) => v + 1)} style={{ flex: 1 }}>
		Fit now
            </button>

            <button disabled={busy} onClick={build} style={{ flex: 1 }}>
		{busy ? "Building…" : "Build / Refresh"}
            </button>
        </div>

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
