/*
 * SPDX-License-Identifier: MIT
 *
 * Copyright (c) 2025 Jonas Waldeck
 */

import { useEffect, useMemo, useRef } from "react";
import maplibregl, { Map, GeoJSONSource, MapMouseEvent } from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

type Props = {
  apiBase: string;
  zoom: number;
  slotId: number;
  onCellClick: (p: { x: number; y: number; zoom: number; slotId: number }) => void;
};

export function MapView({ apiBase, zoom, slotId, onCellClick }: Props) {
  const mapRef = useRef<Map | null>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);

  // keep latest props for event handlers
  const onCellClickRef = useRef(onCellClick);
  const zoomRef = useRef(zoom);
  const slotIdRef = useRef(slotId);

  useEffect(() => {
    onCellClickRef.current = onCellClick;
  }, [onCellClick]);

  useEffect(() => {
    zoomRef.current = zoom;
    slotIdRef.current = slotId;
  }, [zoom, slotId]);

  const sourceId = "hotmap";
  const layerIdFill = "hotmap-fill";
  const layerIdLine = "hotmap-line";

  const hotmapUrl = useMemo(() => {
    const u = new URL(`${apiBase}/api/hotmap`);
    u.searchParams.set("zoom", String(zoom));
    u.searchParams.set("slot_id", String(slotId));
    return u.toString();
  }, [apiBase, zoom, slotId]);

  // Create the map once
  useEffect(() => {
    if (!containerRef.current) return;
    if (mapRef.current) return;

    const map = new maplibregl.Map({
      container: containerRef.current,
      style: "https://demotiles.maplibre.org/style.json",
      center: [13.35, 55.667],
      zoom: 7,
    });

    map.addControl(new maplibregl.NavigationControl(), "top-right");

    map.on("load", () => {
      if (!map.getSource(sourceId)) {
        map.addSource(sourceId, {
          type: "geojson",
          data: hotmapUrl, // initial
        });
      }

      if (!map.getLayer(layerIdFill)) {
        map.addLayer({
          id: layerIdFill,
          type: "fill",
          source: sourceId,
          paint: {
            "fill-opacity": 0.35,
          },
        });
      }

      if (!map.getLayer(layerIdLine)) {
        map.addLayer({
          id: layerIdLine,
          type: "line",
          source: sourceId,
          paint: {
            "line-width": 1,
          },
        });
      }

      map.on("click", layerIdFill, (e: MapMouseEvent) => {
       const features = map.queryRenderedFeatures(e.point, { layers: [layerIdFill] });
       const f = features[0];
       if (!f) return;

       const p: any = (f as any).properties || {};
       if (p.x == null || p.y == null) return;

       onCellClickRef.current({
	x: Number(p.x),
    	y: Number(p.y),
    	zoom: Number(p.zoom ?? zoomRef.current),
    	slotId: Number(p.slot_id ?? slotIdRef.current),
      });
    });

      map.on("mouseenter", layerIdFill, () => {
        map.getCanvas().style.cursor = "pointer";
      });
      map.on("mouseleave", layerIdFill, () => {
        map.getCanvas().style.cursor = "";
      });
    });

    mapRef.current = map;

    return () => {
      map.remove();
      mapRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [apiBase]); // create once (apiBase should be stable)

  // Update data when zoom/slot changes
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    const src = map.getSource(sourceId) as GeoJSONSource | undefined;
    if (!src) return;
    src.setData(hotmapUrl);
  }, [hotmapUrl]);

  return <div ref={containerRef} style={{ width: "100%", height: "100%" }} />;
}
