"""
Web Server for India Crime React GIS Dashboard.

Serves the React + Leaflet + ArcGIS dashboard on http://localhost:3000.
"""

import os
import sys
import http.server
import socketserver
from pathlib import Path

PORT = int(os.environ.get("PORT", 3000))
REACT_DIR = Path(__file__).resolve().parent / "react-dashboard"


class CrimeGISRequestHandler(http.server.SimpleHTTPRequestHandler):
    """Custom HTTP handler with CORS, cache control, and correct MIME types."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(REACT_DIR), **kwargs)

    extensions_map = {
        **http.server.SimpleHTTPRequestHandler.extensions_map,
        '.jsx': 'text/javascript',
        '.json': 'application/json',
        '.geojson': 'application/json'
    }

    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-cache")
        super().end_headers()


def run_server():
    """Start the React GIS dashboard HTTP server."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    
    target_dir = REACT_DIR if REACT_DIR.exists() else Path(__file__).resolve().parent
    # Launch background live stream service
    try:
        from ingestion.live_stream_service import start_background_stream
        start_background_stream()
        print(" [STREAMING] Live Kafka Stream & Anomaly Engine initialized.")
    except Exception as e:
        print(f" [STREAMING] Notice: background stream fallback ({e})")

    print("=" * 75)
    print(" [INDIA CRIME] GIS INTELLIGENCE DASHBOARD (REACT + ARCGIS)")
    print("=" * 75)
    print(f" * Dashboard URL   : http://localhost:{PORT}")
    print(f" * Region Filters  : North, South, West, East, Central, North-East")
    print(f" * Map Engine      : Leaflet + Esri ArcGIS Tiles + GeoJSON Choropleth")
    print(f" * ML Predictor    : Random Forest Inference Engine (Accuracy: 76.5%)")
    print("=" * 75)
    print(f"Serving files from {target_dir}. Press Ctrl+C to stop.\n")

    try:
        with socketserver.TCPServer(("", PORT), CrimeGISRequestHandler) as httpd:
            httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server...")


if __name__ == "__main__":
    run_server()
