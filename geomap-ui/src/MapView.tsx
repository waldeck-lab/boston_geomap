/*
 * SPDX-License-Identifier: MIT
 *
 * Copyright (c) 2025 Jonas Waldeck
 */

import { useEffect, useMemo, useRef } from "react";
import maplibregl, { Map, GeoJSONSource, MapMouseEvent, Popup } from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

type Props = {
    apiBase: string;
    zoom: number; // server zoom parameter (grid resolution)
    slotId: number;
    selected?: { x: number; y: number } | null;
    fitRequestId: number;    
    onCellClick: (p: { x: number; y: number; zoom: number; slotId: number }) => void;
};

function apiUrl(apiBase: string, path: string) {
    const base = apiBase && apiBase.length ? apiBase : window.location.origin;
    return new URL(path, base).toString();
}

export function MapView({ apiBase, zoom, slotId, selected, fitRequestId, onCellClick }: Props) {
    const mapRef = useRef<Map | null>(null);
    const containerRef = useRef<HTMLDivElement | null>(null);

    const onCellClickRef = useRef(onCellClick);
    const zoomRef = useRef(zoom);
    const slotIdRef = useRef(slotId);

    const popupRef = useRef<Popup | null>(null);
    const loadedRef = useRef(false);
    const lastGeoRef = useRef<any>(null);

    function fitToGeoJson(map: Map, geo: any) {
	const features = geo?.features;
	if (!Array.isArray(features) || features.length === 0) return;

	let minLon = Infinity, minLat = Infinity, maxLon = -Infinity, maxLat = -Infinity;

	for (const f of features) {
	    const ring = f?.geometry?.coordinates?.[0];
	    if (!Array.isArray(ring)) continue;

	    for (const pt of ring) {
		const lon = Number(pt?.[0]);
		const lat = Number(pt?.[1]);
		if (!Number.isFinite(lon) || !Number.isFinite(lat)) continue;
		minLon = Math.min(minLon, lon);
		minLat = Math.min(minLat, lat);
		maxLon = Math.max(maxLon, lon);
		maxLat = Math.max(maxLat, lat);
	    }
	}

	if (!Number.isFinite(minLon) || !Number.isFinite(minLat)) return;

	map.fitBounds(
	    [
		[minLon, minLat],
		[maxLon, maxLat],
	    ],
	    { padding: 40, animate: true }
	);
    }
    
    // Keep latest props for event handlers
    useEffect(() => {
	onCellClickRef.current = onCellClick;
    }, [onCellClick]);

    useEffect(() => {
	zoomRef.current = zoom;
	slotIdRef.current = slotId;
    }, [zoom, slotId]);

    const sourceId = "hotmap";
    const layerFill = "hotmap-fill";
    const layerLine = "hotmap-line";
    const layerSelected = "hotmap-selected";

    const hotmapUrl = useMemo(() => {
	const u = new URL(apiUrl(apiBase, "/api/hotmap"));
	u.searchParams.set("zoom", String(zoom));
	u.searchParams.set("slot_id", String(slotId));
	return u.toString();
    }, [apiBase, zoom, slotId]);

    // Create map once
    useEffect(() => {
	const el = containerRef.current;
	if (!el) return;
	if (mapRef.current) return;

	const map = new maplibregl.Map({
	    container: el,
	    style: {
		version: 8,
		sources: {
		    osm: {
			type: "raster",
			tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
			tileSize: 256,
			attribution: "© OpenStreetMap contributors",
		    },
		},
		layers: [{ id: "osm", type: "raster", source: "osm" }],
	    },
	    center: [13.35, 55.667],
	    zoom: 7,
	});

	mapRef.current = map;

	const onResize = () => map.resize();
	window.addEventListener("resize", onResize);

	map.on("error", (e) => {
	    console.error("MapLibre error:", (e as any)?.error || e);
	});

	map.on("load", () => {
	    loadedRef.current = true;

	    map.addControl(new maplibregl.NavigationControl(), "top-right");

	    // Add hotmap source
	    if (!map.getSource(sourceId)) {
		map.addSource(sourceId, {
		    type: "geojson",
		    data: { type: "FeatureCollection", features: [] },
		});
	    }

	    // Fill polygons colored by score
	    if (!map.getLayer(layerFill)) {
		map.addLayer({
		    id: layerFill,
		    type: "fill",
		    source: sourceId,
		    paint: {
			"fill-opacity": 0.45,
			"fill-color": [
			    "interpolate",
			    ["linear"],
			    ["coalesce", ["to-number", ["get", "score"]], 0],
			    0,
			    "#2c7bb6",
			    10,
			    "#abd9e9",
			    30,
			    "#ffffbf",
			    60,
			    "#fdae61",
			    120,
			    "#d7191c",
			],
		    },
		});
	    }

	    // Outline
	    if (!map.getLayer(layerLine)) {
		map.addLayer({
		    id: layerLine,
		    type: "line",
		    source: sourceId,
		    paint: {
			"line-width": 1,
			"line-opacity": 0.7,
		    },
		});
	    }

	    // Selected outline (filter updated by effect below)
	    if (!map.getLayer(layerSelected)) {
		map.addLayer({
		    id: layerSelected,
		    type: "line",
		    source: sourceId,
		    paint: {
			"line-width": 3,
		    },
		    filter: ["==", ["get", "x"], -999999],
		});
	    }

	    // Hover popup
	    popupRef.current = new maplibregl.Popup({
		closeButton: false,
		closeOnClick: false,
		offset: 10,
	    });

	    map.on("mousemove", layerFill, (e: MapMouseEvent) => {
		map.getCanvas().style.cursor = "pointer";
		const f = e.features?.[0] as any;
		if (!f || !popupRef.current) return;

		const p = f.properties || {};
		const coverage = Number(p.coverage ?? 0);
		const score = Number(p.score ?? 0);
		const x = Number(p.x);
		const y = Number(p.y);

		popupRef.current
			.setLngLat(e.lngLat)
			.setHTML(
			    `<div style="font-size:12px">
              <div><b>Cell</b> x=${x} y=${y}</div>
              <div>coverage: ${coverage}</div>
              <div>score: ${score.toFixed(2)}</div>
            </div>`
			)
			.addTo(map);
	    });

	    map.on("mouseleave", layerFill, () => {
		map.getCanvas().style.cursor = "";
		popupRef.current?.remove();
	    });

	    // Click → inform App
	    map.on("click", layerFill, (e: MapMouseEvent) => {
		const f = e.features?.[0] as any;
		if (!f) return;
		const p = f.properties || {};
		if (p.x == null || p.y == null) return;

		onCellClickRef.current({
		    x: Number(p.x),
		    y: Number(p.y),
		    zoom: Number(p.zoom ?? zoomRef.current),
		    slotId: Number(p.slot_id ?? slotIdRef.current),
		});
	    });

	    // Important: fetch hotmap immediately after layers exist
	    // (prevents "nothing shows until I change a control" feeling)
	    void (async () => {
		try {
		    const res = await fetch(hotmapUrl);
		    const geo = await res.json();
		    if (!res.ok) return;
		    const src = map.getSource(sourceId) as GeoJSONSource | undefined;
		    if (src) src.setData(geo);
		    lastGeoRef.current = geo;
		} catch {
		    // ignore
		}
	    })();

	    // Ensure correct sizing
	    setTimeout(() => map.resize(), 0);
	});

	return () => {
	    window.removeEventListener("resize", onResize);
	    popupRef.current?.remove();
	    popupRef.current = null;
	    loadedRef.current = false;
	    map.remove();
	    mapRef.current = null;
	};
    }, [apiBase]);

    // Refresh hotmap whenever zoom/slot changes
    useEffect(() => {
	const map = mapRef.current;
	if (!map) return;
	if (!loadedRef.current) return;

	const src = map.getSource(sourceId) as GeoJSONSource | undefined;
	if (!src) return;

	const ac = new AbortController();

	(async () => {
	    try {
		const res = await fetch(hotmapUrl, { signal: ac.signal });
		const geo = await res.json();
		if (!res.ok) return;
		src.setData(geo);
		lastGeoRef.current = geo;
		console.log("Hotmap loaded", { zoom, slotId, features: geo?.features?.length });
		
	    } catch {
		// ignore aborts/errors
	    }
	})();

	return () => ac.abort();
    }, [hotmapUrl]);

    // Update selected highlight filter
    useEffect(() => {
	const map = mapRef.current;
	if (!map) return;
	if (!map.getLayer(layerSelected)) return;

	if (!selected) {
	    map.setFilter(layerSelected, ["==", ["get", "x"], -999999]);
	    return;
	}

	map.setFilter(layerSelected, [
	    "all",
	    ["==", ["to-number", ["get", "x"]], selected.x],
	    ["==", ["to-number", ["get", "y"]], selected.y],
	]);
    }, [selected, zoom, slotId]);

    // Fit map when reference changes
    useEffect(() => {
	const map = mapRef.current;
	if (!map) return;
	const geo = lastGeoRef.current;
	if (!geo) return;
	
	fitToGeoJson(map, geo);
    }, [fitRequestId]);
    
    return (
	<div
	    ref={containerRef}
	    style={{
		width: "100%",
		height: "100%",
	    }}
	/>
    );
}
