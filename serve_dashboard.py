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
    os.chdir(str(REACT_DIR))
    socketserver.TCPServer.allow_reuse_address = True
    
    print("=" * 75)
    print(" 🇮🇳 INDIA CRIME GIS INTELLIGENCE DASHBOARD (REACT + ARCGIS)")
    print("=" * 75)
    print(f" • Dashboard URL   : http://localhost:{PORT}")
    print(f" • Region Filters  : North, South, West, East, Central, North-East")
    print(f" • Map Engine      : Leaflet + Esri ArcGIS Tiles + GeoJSON Choropleth")
    print(f" • ML Predictor    : Random Forest Inference Engine (Accuracy: 70.6%)")
    print("=" * 75)
    print(f"Serving files from {REACT_DIR}. Press Ctrl+C to stop.\n")

    try:
        with socketserver.TCPServer(("", PORT), CrimeGISRequestHandler) as httpd:
            httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server...")


if __name__ == "__main__":
    run_server()
