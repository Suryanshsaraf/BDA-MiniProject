"""
Interactive Folium Geospatial Map for India Crime Hotspots.

This module generates an interactive Leaflet/Folium map centered on India:
- HeatMap layer based on crime density.
- MarkerCluster layer for Top 50 high-crime districts/hotspots with detailed tooltips.
- Visual risk categorization (CRITICAL: Red, HIGH: Orange, MODERATE: Blue).
"""

import sys
import os
import json
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import (
    ANALYSIS_RESULTS_DIR,
    INDIA_CENTER_LAT,
    INDIA_CENTER_LON,
    DEFAULT_MAP_ZOOM,
    setup_logging
)

logger = setup_logging("FoliumMap")


def create_crime_map(hotspots_data: list, crime_filter: str = "ALL", min_crime: int = 0) -> str:
    """
    Generate an interactive Folium map object or self-contained HTML representation.
    
    Args:
        hotspots_data (list): List of hotspot dictionaries with lat, lon, crime stats.
        crime_filter (str): Filter for crime type or risk level.
        min_crime (int): Minimum total crime threshold.
        
    Returns:
        folium.Map or str: Folium map instance, or HTML string representation.
    """
    logger.info(f"Generating Folium map for {len(hotspots_data)} hotspots (Filter: {crime_filter})...")
    
    try:
        import folium
        from folium.plugins import HeatMap, MarkerCluster

        m = folium.Map(
            location=[INDIA_CENTER_LAT, INDIA_CENTER_LON],
            zoom_start=DEFAULT_MAP_ZOOM,
            tiles="CartoDB positron",
            control_scale=True
        )

        # 1. Filter points
        filtered_points = []
        heat_data = []

        for h in hotspots_data:
            if h["total_crimes"] < min_crime:
                continue
            if crime_filter != "ALL" and h.get("risk_level") != crime_filter:
                continue

            lat = h["lat"]
            lon = h["lon"]
            weight = min(h["total_crimes"] / 10000.0, 10.0)
            heat_data.append([lat, lon, weight])
            filtered_points.append(h)

        # 2. Add HeatMap Layer
        if heat_data:
            HeatMap(
                heat_data,
                radius=18,
                blur=15,
                max_zoom=10,
                name="Crime Density Heatmap"
            ).add_to(m)

        # 3. Add MarkerCluster for Hotspots
        marker_cluster = MarkerCluster(name="Top Danger Zones").add_to(m)

        for h in filtered_points:
            color = "red" if h.get("risk_level") == "CRITICAL" else "orange"
            icon_name = "exclamation-triangle" if color == "red" else "info-sign"

            popup_html = f"""
            <div style="font-family: Arial, sans-serif; min-width: 200px;">
                <h4 style="margin: 0 0 5px 0; color: #d9534f;">{h['district']}</h4>
                <b>State:</b> {h['state']}<br/>
                <b>Total IPC Crimes:</b> {h['total_crimes']:,}<br/>
                <b>Violent Crimes:</b> {h['violent_crimes']:,} ({h.get('violent_percentage', 0)}%)<br/>
                <b>Crimes Against Women:</b> {h.get('women_crimes', 0):,} ({h.get('women_crime_pct', 0)}%)<br/>
                <b>Risk Category:</b> <span style="color: {'red' if color == 'red' else '#f0ad4e'}; font-weight: bold;">{h.get('risk_level', 'HIGH')}</span>
            </div>
            """

            folium.Marker(
                location=[h["lat"], h["lon"]],
                popup=folium.Popup(popup_html, max_width=300),
                tooltip=f"{h['district']}, {h['state']}: {h['total_crimes']:,} crimes",
                icon=folium.Icon(color=color, icon=icon_name, prefix="fa" if icon_name == "exclamation-triangle" else "glyphicon")
            ).add_to(marker_cluster)

        folium.LayerControl().add_to(m)
        return m

    except ImportError:
        logger.warning("Folium library not imported. Returning lightweight self-contained Leaflet HTML.")
        return generate_leaflet_html(hotspots_data, crime_filter, min_crime)


def generate_leaflet_html(hotspots_data: list, crime_filter: str = "ALL", min_crime: int = 0) -> str:
    """
    Generate an offline standalone HTML Leaflet map when folium library is unavailable.
    
    Args:
        hotspots_data (list): List of district hotspot dictionaries.
        crime_filter (str): Filter value.
        min_crime (int): Minimum crime filter.
        
    Returns:
        str: Self-contained HTML string with Leaflet CDN.
    """
    filtered = [h for h in hotspots_data if h["total_crimes"] >= min_crime]
    if crime_filter != "ALL":
        filtered = [h for h in filtered if h.get("risk_level") == crime_filter]

    markers_js = []
    for h in filtered:
        col = "#e74c3c" if h.get("risk_level") == "CRITICAL" else "#f39c12"
        popup = f"<b>{h['district']}</b> ({h['state']})<br>Total Crimes: {h['total_crimes']:,}<br>Violent: {h['violent_crimes']:,}<br>Risk: {h.get('risk_level', 'HIGH')}"
        markers_js.append(f"""
        L.circleMarker([{h['lat']}, {h['lon']}], {{
            radius: {min(max(h['total_crimes'] / 20000, 6), 18)},
            fillColor: '{col}',
            color: '#ffffff',
            weight: 2,
            opacity: 1,
            fillOpacity: 0.8
        }}).addTo(map).bindPopup("{popup}");
        """)

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8" />
        <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
        <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
        <style>
            #map {{ height: 550px; width: 100%; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
            body {{ margin: 0; padding: 0; }}
        </style>
    </head>
    <body>
        <div id="map"></div>
        <script>
            var map = L.map('map').setView([{INDIA_CENTER_LAT}, {INDIA_CENTER_LON}], {DEFAULT_MAP_ZOOM});
            L.tileLayer('https://{{s}}.basemaps.cartocdn.com/rastertiles/voyager/{{z}}/{{x}}/{{y}}{{r}}.png', {{
                attribution: '&copy; <a href="https://carto.com/">CARTO</a> &copy; <a href="https://openstreetmap.org">OSM</a>'
            }}).addTo(map);
            {''.join(markers_js)}
        </script>
    </body>
    </html>
    """
    return html_content


def save_standalone_map(output_path: Path):
    """Render and save standalone HTML map for review."""
    hotspots_file = ANALYSIS_RESULTS_DIR / "hotspots.json"
    if not hotspots_file.exists():
        logger.error(f"{hotspots_file} not found.")
        return

    with open(hotspots_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    hotspots = data.get("top_50_hotspots", [])
    m = create_crime_map(hotspots)

    if hasattr(m, "save"):
        m.save(str(output_path))
    else:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(m)

    logger.info(f"Interactive crime map saved to {output_path}.")


if __name__ == "__main__":
    out_html = ANALYSIS_RESULTS_DIR / "india_crime_hotspots_map.html"
    save_standalone_map(out_html)
