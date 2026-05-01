/*
 * SPDX-License-Identifier: MIT
 *
 * Copyright (c) 2025 Jonas Waldeck
 */

import { useEffect, useMemo, useRef } from "react";
import maplibregl, {
  GeoJSONSource,
  Map,
  Popup,
  type ExpressionSpecification,
  type MapLayerMouseEvent,
} from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

type Props = {
  apiBase: string;
  zoom: number;
  slotId: number;
  slotIds?: number[];
  selected?: { x: number; y: number } | null;
  fitRequestId: number;
  autoFit?: boolean;
  fitOnFirstLoad?: boolean;
  onCellClick: (p: { x: number; y: number; zoom: number; slotId: number }) => void;
  yearFrom: number;
  yearTo: number;
  speciesStops: number[];
};

function apiUrl(apiBase: string, path: string) {
  const base = apiBase && apiBase.length ? apiBase : window.location.origin;
  return new URL(path, base).toString();
}

function buildSpeciesColorExpression(stops: number[]): ExpressionSpecification {
  const colors = [
    "#eef8ff", // 0–1, nästan vitblå
    "#cfeeff", // 2
    "#a8dcff", // 3
    "#7cc4ff", // 4–5
    "#4aa3ff", // 6–8
    "#1f78ff", // 9–13
    "#7bd88f", // 14–21
    "#ffe45c", // 22–34
    "#ffad33", // 35–55
    "#ff4d2e", // 56–89
    "#8b0000", // >89
  ];
  const safeStops = (stops.length ? stops : [1, 2, 5, 10, 20, 30, 40, 50])
    .filter((x) => Number.isFinite(x) && x > 0)
    .sort((a, b) => a - b);

  const expr: unknown[] = [
    "step",
    ["coalesce", ["to-number", ["get", "coverage"]], 0],
    "#00000000",
  ];

  safeStops.forEach((stop, i) => {
    expr.push(stop);
    expr.push(colors[Math.min(i, colors.length - 2)]);
  });

  expr.push(safeStops[safeStops.length - 1] + 1);
  expr.push(colors[colors.length - 1]);
  return expr as ExpressionSpecification;
}

function fitToGeoJson(map: Map, geo: any) {
  const features = geo?.features;
  if (!Array.isArray(features) || features.length === 0) return;

  let minLon = Infinity;
  let minLat = Infinity;
  let maxLon = -Infinity;
  let maxLat = -Infinity;

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

export function MapView({
  apiBase,
  zoom,
  yearFrom,
  yearTo,
  slotId,
  slotIds,
  selected,
  fitRequestId,
  autoFit = false,
  fitOnFirstLoad = false,
  onCellClick,
  speciesStops,
}: Props) {
  const mapRef = useRef<Map | null>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);

  const popupRef = useRef<Popup | null>(null);
  const loadedRef = useRef(false);
  const lastGeoRef = useRef<any>(null);
  const didInitialFitRef = useRef(false);

  const onCellClickRef = useRef(onCellClick);
  const zoomRef = useRef(zoom);
  const slotIdRef = useRef(slotId);

  const sourceId = "hotmap";
  const layerFill = "hotmap-fill";
  const layerLine = "hotmap-line";
  const layerSelected = "hotmap-selected";

  useEffect(() => {
    onCellClickRef.current = onCellClick;
  }, [onCellClick]);

  useEffect(() => {
    zoomRef.current = zoom;
    slotIdRef.current = slotId;
  }, [zoom, slotId]);

  const hotmapUrl = useMemo(() => {
    const hasWindow = Array.isArray(slotIds) && slotIds.length > 0;

    if (hasWindow) {
      const u = new URL(apiUrl(apiBase, "/api/hotmap_window"));
      u.searchParams.set("zoom", String(zoom));
      u.searchParams.set("year_from", String(yearFrom));
      u.searchParams.set("year_to", String(yearTo));
      u.searchParams.set("slot_ids", slotIds.join(","));
      return u.toString();
    }

    const u = new URL(apiUrl(apiBase, "/api/hotmap"));
    u.searchParams.set("zoom", String(zoom));
    u.searchParams.set("year_from", String(yearFrom));
    u.searchParams.set("year_to", String(yearTo));
    u.searchParams.set("slot_id", String(slotId));
    return u.toString();
  }, [apiBase, zoom, yearFrom, yearTo, slotId, slotIds]);

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

      if (!map.getSource(sourceId)) {
        map.addSource(sourceId, {
          type: "geojson",
          data: { type: "FeatureCollection", features: [] },
        });
      }

      if (!map.getLayer(layerFill)) {
        map.addLayer({
          id: layerFill,
          type: "fill",
          source: sourceId,
          paint: {
            "fill-opacity": 0.55,
            "fill-color": buildSpeciesColorExpression(speciesStops),
          },
        });
      }

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

      popupRef.current = new maplibregl.Popup({
        closeButton: false,
        closeOnClick: false,
        offset: 10,
      });

      map.on("mousemove", layerFill, (e: MapLayerMouseEvent) => {
        map.getCanvas().style.cursor = "pointer";
        const f = e.features?.[0] as any;
        if (!f || !popupRef.current) return;

        const p = f.properties || {};
        const coverage = Number(p.coverage ?? 0);
        const x = Number(p.x);
        const y = Number(p.y);

        popupRef.current
          .setLngLat(e.lngLat)
          .setHTML(
            `<div style="font-size:12px">
              <div><b>Cell</b> x=${x} y=${y}</div>
              <div>species: ${coverage}</div>
            </div>`
          )
          .addTo(map);
      });

      map.on("mouseleave", layerFill, () => {
        map.getCanvas().style.cursor = "";
        popupRef.current?.remove();
      });

      map.on("click", layerFill, (e: MapLayerMouseEvent) => {
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
  }, []);

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
        const text = await res.text();

        if (!res.ok) {
          throw new Error(`Hotmap HTTP ${res.status}: ${text.slice(0, 300)}`);
        }

        const geo = JSON.parse(text);
        src.setData(geo);
        lastGeoRef.current = geo;

        if (fitOnFirstLoad && !didInitialFitRef.current) {
          fitToGeoJson(map, geo);
          didInitialFitRef.current = true;
        }

        console.log("Hotmap loaded", {
          zoom,
          yearFrom,
          yearTo,
          slotId,
          slotIds,
          features: geo?.features?.length,
        });
      } catch (err) {
        if (!ac.signal.aborted) {
          console.error("Failed to load hotmap", err);
        }
      }
    })();

    return () => ac.abort();
  }, [hotmapUrl, fitOnFirstLoad, zoom, yearFrom, yearTo, slotId, slotIds]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    if (!loadedRef.current) return;
    if (!map.getLayer(layerFill)) return;

    map.setPaintProperty(
      layerFill,
      "fill-color",
      buildSpeciesColorExpression(speciesStops) as any
    );
  }, [speciesStops]);

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
  }, [selected]);

  useEffect(() => {
    if (!autoFit) return;

    const map = mapRef.current;
    if (!map) return;

    const geo = lastGeoRef.current;
    if (!geo) return;

    fitToGeoJson(map, geo);
  }, [fitRequestId, autoFit]);

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

// /*
//  * SPDX-License-Identifier: MIT
//  *
//  * Copyright (c) 2025 Jonas Waldeck
//  */

// import { useEffect, useMemo, useRef } from "react";
// import maplibregl, { GeoJSONSource, Map, MapMouseEvent, Popup } from "maplibre-gl";
// import "maplibre-gl/dist/maplibre-gl.css";

// type Props = {
//   apiBase: string;
//   zoom: number; // server zoom parameter (grid resolution)

//   // Single-slot (legacy)
//   slotId: number;

//   // Window support. If provided and length > 0, MapView prefers this.
//   slotIds?: number[];

//   // UI features
//   selected?: { x: number; y: number } | null;
//   fitRequestId: number;

//   // New behavior toggles
//   autoFit?: boolean;         // fit whenever fitRequestId changes
//   fitOnFirstLoad?: boolean;  // fit once on first successful geo load

//   onCellClick: (p: { x: number; y: number; zoom: number; slotId: number }) => void;

//   // Year handling 
//   yearFrom: number;
//   yearTo: number;

//   // Hotmap coloring
//   speciesStops: number[];

// };

// function apiUrl(apiBase: string, path: string) {
//   const base = apiBase && apiBase.length ? apiBase : window.location.origin;
//   return new URL(path, base).toString();
// }

// function fitToGeoJson(map: Map, geo: any) {
//   const features = geo?.features;
//   if (!Array.isArray(features) || features.length === 0) return;

//   let minLon = Infinity;
//   let minLat = Infinity;
//   let maxLon = -Infinity;
//   let maxLat = -Infinity;

//   for (const f of features) {
//     const ring = f?.geometry?.coordinates?.[0];
//     if (!Array.isArray(ring)) continue;

//     for (const pt of ring) {
//       const lon = Number(pt?.[0]);
//       const lat = Number(pt?.[1]);
//       if (!Number.isFinite(lon) || !Number.isFinite(lat)) continue;

//       minLon = Math.min(minLon, lon);
//       minLat = Math.min(minLat, lat);
//       maxLon = Math.max(maxLon, lon);
//       maxLat = Math.max(maxLat, lat);
//     }
//   }

//   if (!Number.isFinite(minLon) || !Number.isFinite(minLat)) return;

//   map.fitBounds(
//     [
//       [minLon, minLat],
//       [maxLon, maxLat],
//     ],
//     { padding: 40, animate: true }
//   );
// }


// export function MapView({
//   apiBase,
//   zoom,
//   yearFrom,
//   yearTo,
//   slotId,
//   slotIds,
//   selected,
//   fitRequestId,
//   autoFit = false,
//   fitOnFirstLoad = false,
//   onCellClick,
//   speciesStops,
// }: Props) {
//   const mapRef = useRef<Map | null>(null);
//   const containerRef = useRef<HTMLDivElement | null>(null);

//   const popupRef = useRef<Popup | null>(null);
//   const loadedRef = useRef(false);
//   const lastGeoRef = useRef<any>(null);
//   const didInitialFitRef = useRef(false);

//   const onCellClickRef = useRef(onCellClick);
//   const zoomRef = useRef(zoom);
//   const slotIdRef = useRef(slotId);

//   const sourceId = "hotmap";
//   const layerFill = "hotmap-fill";
//   const layerLine = "hotmap-line";
//   const layerSelected = "hotmap-selected";

//   const SPECIES_COLORS = [
//     "#dbeafe", // <= first stop
//     "#bfdbfe",
//     "#93c5fd",
//     "#60a5fa",
//     "#34d399",
//     "#facc15",
//     "#fb923c",
//     "#ef4444",
//     "#991b1b", // above last stop
//   ];


//   function bucketFor(value: number, stops: number[]): number {
//     if (!Number.isFinite(value) || value <= 0) return -1;

//     for (let i = 0; i < stops.length; i++) {
//       if (value <= stops[i]) return i;
//     }

//     return stops.length;
//   }

//   function colorForSpeciesCoverage(coverage: number, stops: number[]): string {
//     const bucket = bucketFor(coverage, stops);

//     if (bucket < 0) return "#00000000";

//     return SPECIES_COLORS[Math.min(bucket, SPECIES_COLORS.length - 1)];
//   }

//   function buildSpeciesColorExpression(stops: number[]) {
//     const colors = SPECIES_COLORS;
//     const safeStops = stops.length ? stops : [1, 2, 5, 10, 20, 30, 40, 50];

//     const expr: any[] = [
//       "step",
//       ["coalesce", ["to-number", ["get", "coverage"]], 0],
//       "#00000000",
//     ];

//     safeStops.forEach((stop, i) => {
//       expr.push(stop);
//       expr.push(colors[Math.min(i, colors.length - 1)]);
//     });

//     expr.push(safeStops[safeStops.length - 1] + 1);
//     expr.push(colors[colors.length - 1]);

//     return expr;
//   }



//   useEffect(() => {
//     onCellClickRef.current = onCellClick;
//   }, [onCellClick]);

//   useEffect(() => {
//     zoomRef.current = zoom;
//     slotIdRef.current = slotId;
//   }, [zoom, slotId]);

//   const hotmapUrl = useMemo(() => {
//     const hasWindow = Array.isArray(slotIds) && slotIds.length > 0;

//     if (hasWindow) {
//       const u = new URL(apiUrl(apiBase, "/api/hotmap_window"));
//       u.searchParams.set("zoom", String(zoom));
//       u.searchParams.set("year_from", String(yearFrom));
//       u.searchParams.set("year_to", String(yearTo));
//       u.searchParams.set("slot_ids", slotIds.join(","));
//       return u.toString();
//     }

//     const u = new URL(apiUrl(apiBase, "/api/hotmap"));
//     u.searchParams.set("zoom", String(zoom));
//     u.searchParams.set("year_from", String(yearFrom));
//     u.searchParams.set("year_to", String(yearTo));
//     u.searchParams.set("slot_id", String(slotId));
//     return u.toString();
//   }, [apiBase, zoom, yearFrom, yearTo, slotId, slotIds]);

//   // Init map exactly once
//   useEffect(() => {
//     const el = containerRef.current;
//     if (!el) return;
//     if (mapRef.current) return;

//     const map = new maplibregl.Map({
//       container: el,
//       style: {
//         version: 8,
//         sources: {
//           osm: {
//             type: "raster",
//             tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
//             tileSize: 256,
//             attribution: "© OpenStreetMap contributors",
//           },
//         },
//         layers: [{ id: "osm", type: "raster", source: "osm" }],
//       },
//       center: [13.35, 55.667],
//       zoom: 7,
//     });

//     mapRef.current = map;

//     const onResize = () => map.resize();
//     window.addEventListener("resize", onResize);

//     map.on("error", (e) => {
//       console.error("MapLibre error:", (e as any)?.error || e);
//     });

//     map.on("load", () => {
//       loadedRef.current = true;

//       map.addControl(new maplibregl.NavigationControl(), "top-right");

//       if (!map.getSource(sourceId)) {
//         map.addSource(sourceId, {
//           type: "geojson",
//           data: { type: "FeatureCollection", features: [] },
//         });
//       }

//       if (!map.getLayer(layerFill)) {
//         map.addLayer({
//           id: layerFill,
//           type: "fill",
//           source: sourceId,
//           paint: {
//             "fill-opacity": 0.45,
//             "fill-color": buildSpeciesColorExpression(speciesStops),
//           },
//         });
//       }

//       if (!map.getLayer(layerLine)) {
//         map.addLayer({
//           id: layerLine,
//           type: "line",
//           source: sourceId,
//           paint: {
//             "line-width": 1,
//             "line-opacity": 0.7,
//           },
//         });
//       }

//       if (!map.getLayer(layerSelected)) {
//         map.addLayer({
//           id: layerSelected,
//           type: "line",
//           source: sourceId,
//           paint: {
//             "line-width": 3,
//           },
//           filter: ["==", ["get", "x"], -999999],
//         });
//       }

//       popupRef.current = new maplibregl.Popup({
//         closeButton: false,
//         closeOnClick: false,
//         offset: 10,
//       });

//       map.on("mousemove", layerFill, (e: MapMouseEvent) => {
//         map.getCanvas().style.cursor = "pointer";
//         const f = e.features?.[0] as any;
//         if (!f || !popupRef.current) return;

//         const p = f.properties || {};
//         const coverage = Number(p.coverage ?? 0);
//         const x = Number(p.x);
//         const y = Number(p.y);

//         popupRef.current
//           .setLngLat(e.lngLat)

//           .setHTML(
//             `<div style="font-size:12px">
//             <div><b>Cell</b> x=${x} y=${y}</div>
//             <div>species: ${coverage}</div>
//             </div>`
//           )
//           .addTo(map);
//       });

//       map.on("mouseleave", layerFill, () => {
//         map.getCanvas().style.cursor = "";
//         popupRef.current?.remove();
//       });

//       map.on("click", layerFill, (e: MapMouseEvent) => {
//         const f = e.features?.[0] as any;
//         if (!f) return;

//         const p = f.properties || {};
//         if (p.x == null || p.y == null) return;

//         onCellClickRef.current({
//           x: Number(p.x),
//           y: Number(p.y),
//           zoom: Number(p.zoom ?? zoomRef.current),
//           slotId: Number(p.slot_id ?? slotIdRef.current),
//         });
//       });

//       setTimeout(() => map.resize(), 0);
//     });

//     return () => {
//       window.removeEventListener("resize", onResize);
//       popupRef.current?.remove();
//       popupRef.current = null;
//       loadedRef.current = false;
//       map.remove();
//       mapRef.current = null;
//     };
//   }, []);


//   // Update color scale when speciesStops changes
//   useEffect(() => {
//     const map = mapRef.current;
//     if (!map) return;
//     if (!loadedRef.current) return;
//     if (!map.getLayer(layerFill)) return;

//     map.setPaintProperty(
//       layerFill,
//       "fill-color",
//       buildSpeciesColorExpression(speciesStops)
//     );
//   }, [speciesStops]);

//   // Reload geojson when request params change
//   useEffect(() => {
//     const map = mapRef.current;
//     if (!map) return;
//     if (!loadedRef.current) return;

//     const src = map.getSource(sourceId) as GeoJSONSource | undefined;
//     if (!src) return;

//     const ac = new AbortController();

//     (async () => {
//       try {
//         const res = await fetch(hotmapUrl, { signal: ac.signal });
//         const text = await res.text();

//         if (!res.ok) {
//           throw new Error(`Hotmap HTTP ${res.status}: ${text.slice(0, 300)}`);
//         }

//         const geo = JSON.parse(text);
//         src.setData(geo);
//         lastGeoRef.current = geo;

//         if (fitOnFirstLoad && !didInitialFitRef.current) {
//           fitToGeoJson(map, geo);
//           didInitialFitRef.current = true;
//         }

//         console.log("Hotmap loaded", {
//           zoom,
//           slotId,
//           slotIds,
//           features: geo?.features?.length,
//         });
//       } catch (err) {
//         if (!ac.signal.aborted) {
//           console.error("Failed to load hotmap", err);
//         }
//       }
//     })();

//     return () => ac.abort();
//   }, [hotmapUrl, fitOnFirstLoad, zoom, slotId, slotIds]);

//   // Selected highlight only
//   useEffect(() => {
//     const map = mapRef.current;
//     if (!map) return;
//     if (!map.getLayer(layerSelected)) return;

//     if (!selected) {
//       map.setFilter(layerSelected, ["==", ["get", "x"], -999999]);
//       return;
//     }

//     map.setFilter(layerSelected, [
//       "all",
//       ["==", ["to-number", ["get", "x"]], selected.x],
//       ["==", ["to-number", ["get", "y"]], selected.y],
//     ]);
//   }, [selected]);

//   // Explicit fit trigger only when enabled
//   useEffect(() => {
//     if (!autoFit) return;

//     const map = mapRef.current;
//     if (!map) return;

//     const geo = lastGeoRef.current;
//     if (!geo) return;

//     fitToGeoJson(map, geo);
//   }, [fitRequestId, autoFit]);


//   return (
//     <div
//       ref={containerRef}
//       style={{
//         width: "100%",
//         height: "100%",
//       }}
//     />
//   );
// }
