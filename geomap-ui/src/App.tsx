/*
 * SPDX-License-Identifier: MIT
 *
 * Copyright (c) 2025 Jonas Waldeck
 */


import { useCallback, useMemo, useState } from "react";
import { MapView } from "./MapView";

type Taxon = {
  taxon_id: number;
  swedish_name: string;
  scientific_name: string;
  observations_count: number;
};

const API_BASE = "http://localhost:8088";

export default function App() {
  const [slotId, setSlotId] = useState(0);
  const [zooms, setZooms] = useState("15,14,13");
  const [zoom, setZoom] = useState(15);
  const [n, setN] = useState(100);
  const [alpha, setAlpha] = useState(2.0);
  const [beta, setBeta] = useState(0.5);
  const [busy, setBusy] = useState(false);

  const [clicked, setClicked] = useState<{ x: number; y: number } | null>(null);
  const [taxa, setTaxa] = useState<Taxon[]>([]);

  const parsedZooms = useMemo(() => {
    const zs = zooms
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean)
      .map((s) => Number(s))
      .filter((z) => Number.isFinite(z))
      .map((z) => Math.trunc(z))
      .filter((z) => z >= 1 && z <= 21);
  
    // unique + sort descending (highest zoom = most detailed)
    return Array.from(new Set(zs)).sort((a, b) => b - a);
  }, [zooms]);
  
  /* const baseZoom = useMemo(() => (parsedZooms.length ? parsedZooms[0] : zoom), [parsedZooms, zoom]);*/
  
  const [status, setStatus] = useState<string>("");

  const [taxaLoading, setTaxaLoading] = useState(false);


  const build = useCallback(async () => {
    setBusy(true);
    setStatus("Building…");
    setTaxa([]);
    setClicked(null);
  
    try {
      if (parsedZooms.length === 0) {
        throw new Error("No valid zooms. Example: 15,14,13");
      }
  
      const res = await fetch(`${API_BASE}/api/pipeline/build`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          slot_id: slotId,
          zooms: parsedZooms,
          n,
          alpha,
          beta,
          force: false,
        }),
      });
  
      const j = await res.json().catch(() => ({} as any));
      if (!res.ok || !j.ok) throw new Error(j.error || `HTTP ${res.status}`);
  
      setZoom(parsedZooms[0]); // highest zoom
      setStatus(`OK. Built slot ${slotId}, zooms ${parsedZooms.join(",")}`);
    } catch (e: any) {
      setStatus(`Error: ${e?.message || String(e)}`);
    } finally {
      setBusy(false);
    }
  }, [alpha, beta, n, parsedZooms, slotId]);
  

  const onCellClick = useCallback(
    async (p: { x: number; y: number; zoom: number; slotId: number }) => {
      setClicked({ x: p.x, y: p.y });
      setTaxa([]);
      setTaxaLoading(true);
  
      try {
        const u = new URL(`${API_BASE}/api/cell/taxa`);
        u.searchParams.set("zoom", String(p.zoom));
        u.searchParams.set("slot_id", String(p.slotId));
        u.searchParams.set("x", String(p.x));
        u.searchParams.set("y", String(p.y));
        u.searchParams.set("limit", "200");
  
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
    []
  );
  

  return (
    <div style={{ display: "grid", gridTemplateColumns: "360px 1fr", height: "100vh" }}>
      <div style={{ padding: 12, borderRight: "1px solid #ddd", overflow: "auto" }}>
        <h2>Geomap</h2>

        <div>
          <label>Slot</label>
          <input type="number" value={slotId} min={0} max={47} onChange={(e) => setSlotId(Number(e.target.value))} />
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

        {status && <div style={{ marginTop: 8, fontSize: 12, whiteSpace: "pre-wrap" }}>{status}</div>}

        <button disabled={busy} onClick={build} style={{ marginTop: 10 }}>
          {busy ? "Building…" : "Build / Refresh"}
        </button>

        <hr />

        <div>
          <label>View zoom</label>
          <input type="number" value={zoom} onChange={(e) => setZoom(Number(e.target.value))} />
        </div>

        <hr />

        <h3>Clicked cell</h3>

        {!clicked && (
          <div style={{ opacity: 0.7 }}>Click a cell on the map</div>
        )}

        {clicked && (
          <div>
            <div>
              x={clicked.x} y={clicked.y}
            </div>

            <div style={{ marginTop: 8 }}>
              {taxaLoading && (
                <div style={{ opacity: 0.7 }}>Loading taxa…</div>
              )}

              {!taxaLoading && taxa.length === 0 && (
                <div style={{ opacity: 0.7 }}>No taxa</div>
              )}

              {!taxaLoading && taxa.length > 0 && (
                <ul style={{ paddingLeft: 18 }}>
                  {taxa.slice(0, 80).map((t) => (
                    <li key={t.taxon_id}>
                      <b>{t.swedish_name || t.scientific_name || t.taxon_id}</b>{" "}
                      ({t.observations_count})
                    </li>
                  ))}
                </ul>
              )}
            </div>
          </div>
        )}
      </div>

      <div style={{ position: "relative" }}>
        <MapView apiBase={API_BASE} zoom={zoom} slotId={slotId} onCellClick={onCellClick} />
      </div>
    </div>
  );
}