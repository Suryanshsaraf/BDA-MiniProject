"""
Download Real NCRB Crime Datasets & Build District Coordinate Gazetteer.

This script fetches official Government of India NCRB crime datasets
(2001–2014) from verified repositories, and creates an authoritative
geospatial coordinate map for Indian districts.
"""

import os
import sys
import json
import urllib.request
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import (
    RAW_DATA_DIR,
    DATA_DIR,
    NCRB_BASE_URL,
    DATASET_FILES,
    setup_logging
)

logger = setup_logging("DownloadRealData")

# Representative geographic coordinates (Lat, Lon) for prominent Indian districts/cities
DISTRICT_COORDINATES = {
    # Andhra Pradesh
    "ANANTAPUR": {"lat": 14.6819, "lon": 77.6006, "state": "Andhra Pradesh"},
    "CHITTOOR": {"lat": 13.2172, "lon": 79.1003, "state": "Andhra Pradesh"},
    "CUDDAPAH": {"lat": 14.4673, "lon": 78.8242, "state": "Andhra Pradesh"},
    "EAST GODAVARI": {"lat": 16.9891, "lon": 82.2475, "state": "Andhra Pradesh"},
    "GUNTUR": {"lat": 16.3067, "lon": 80.4365, "state": "Andhra Pradesh"},
    "HYDERABAD CITY": {"lat": 17.3850, "lon": 78.4867, "state": "Telangana"},
    "KRISHNA": {"lat": 16.1809, "lon": 81.1303, "state": "Andhra Pradesh"},
    "KURNOOL": {"lat": 15.8281, "lon": 78.0373, "state": "Andhra Pradesh"},
    "NELLORE": {"lat": 14.4426, "lon": 79.9865, "state": "Andhra Pradesh"},
    "PRAKASHAM": {"lat": 15.5057, "lon": 80.0499, "state": "Andhra Pradesh"},
    "VISAKHAPATNAM": {"lat": 17.6868, "lon": 83.2185, "state": "Andhra Pradesh"},
    "VIZIANAGARAM": {"lat": 18.1067, "lon": 83.3956, "state": "Andhra Pradesh"},
    "WEST GODAVARI": {"lat": 16.7107, "lon": 81.0952, "state": "Andhra Pradesh"},

    # Bihar
    "PATNA": {"lat": 25.5941, "lon": 85.1376, "state": "Bihar"},
    "GAYA": {"lat": 24.7914, "lon": 85.0002, "state": "Bihar"},
    "BHAGALPUR": {"lat": 25.2425, "lon": 86.9842, "state": "Bihar"},
    "MUZAFFARPUR": {"lat": 26.1209, "lon": 85.3647, "state": "Bihar"},
    "PURNEA": {"lat": 25.7771, "lon": 87.4753, "state": "Bihar"},
    "ROHTAS": {"lat": 24.9529, "lon": 84.0150, "state": "Bihar"},
    "DARBHANGA": {"lat": 26.1542, "lon": 85.8918, "state": "Bihar"},

    # Delhi
    "CENTRAL": {"lat": 28.6500, "lon": 77.2167, "state": "Delhi"},
    "NORTH": {"lat": 28.7000, "lon": 77.1833, "state": "Delhi"},
    "SOUTH": {"lat": 28.5333, "lon": 77.2000, "state": "Delhi"},
    "EAST": {"lat": 28.6333, "lon": 77.2833, "state": "Delhi"},
    "WEST": {"lat": 28.6500, "lon": 77.1000, "state": "Delhi"},
    "NEW DELHI": {"lat": 28.6139, "lon": 77.2090, "state": "Delhi"},
    "SOUTH WEST": {"lat": 28.5833, "lon": 77.0500, "state": "Delhi"},
    "NORTH WEST": {"lat": 28.7500, "lon": 77.1167, "state": "Delhi"},
    "NORTH EAST": {"lat": 28.7000, "lon": 77.2667, "state": "Delhi"},

    # Gujarat
    "AHMEDABAD COMMR.": {"lat": 23.0225, "lon": 72.5714, "state": "Gujarat"},
    "AHMEDABAD RURAL": {"lat": 23.0500, "lon": 72.4500, "state": "Gujarat"},
    "SURAT COMMR.": {"lat": 21.1702, "lon": 72.8311, "state": "Gujarat"},
    "SURAT RURAL": {"lat": 21.2000, "lon": 73.0000, "state": "Gujarat"},
    "VADODARA COMMR.": {"lat": 22.3072, "lon": 73.1812, "state": "Gujarat"},
    "RAJKOT COMMR.": {"lat": 22.3039, "lon": 70.8022, "state": "Gujarat"},

    # Karnataka
    "BANGALORE COMMR.": {"lat": 12.9716, "lon": 77.5946, "state": "Karnataka"},
    "BANGALORE RURAL": {"lat": 13.0827, "lon": 77.5877, "state": "Karnataka"},
    "MYSORE COMMR.": {"lat": 12.2958, "lon": 76.6394, "state": "Karnataka"},
    "BELGAUM": {"lat": 15.8497, "lon": 74.4977, "state": "Karnataka"},
    "DHARWAD": {"lat": 15.4589, "lon": 75.0078, "state": "Karnataka"},
    "GULBARGA": {"lat": 17.3297, "lon": 76.8343, "state": "Karnataka"},

    # Maharashtra
    "MUMBAI COMMR.": {"lat": 18.9220, "lon": 72.8347, "state": "Maharashtra"},
    "PUNE COMMR.": {"lat": 18.5204, "lon": 73.8567, "state": "Maharashtra"},
    "THANE COMMR.": {"lat": 19.2183, "lon": 72.9781, "state": "Maharashtra"},
    "NAGPUR COMMR.": {"lat": 21.1458, "lon": 79.0882, "state": "Maharashtra"},
    "NASHIK COMMR.": {"lat": 19.9975, "lon": 73.7898, "state": "Maharashtra"},
    "AURANGABAD COMMR.": {"lat": 19.8762, "lon": 75.3433, "state": "Maharashtra"},

    # Madhya Pradesh
    "BHOPAL": {"lat": 23.2599, "lon": 77.4126, "state": "Madhya Pradesh"},
    "INDORE": {"lat": 22.7196, "lon": 75.8577, "state": "Madhya Pradesh"},
    "JABALPUR": {"lat": 23.1815, "lon": 79.9864, "state": "Madhya Pradesh"},
    "GWALIOR": {"lat": 26.2183, "lon": 78.1828, "state": "Madhya Pradesh"},
    "UJJAIN": {"lat": 23.1765, "lon": 75.7885, "state": "Madhya Pradesh"},

    # Tamil Nadu
    "CHENNAI": {"lat": 13.0827, "lon": 80.2707, "state": "Tamil Nadu"},
    "COIMBATORE": {"lat": 11.0168, "lon": 76.9558, "state": "Tamil Nadu"},
    "MADURAI": {"lat": 9.9252, "lon": 78.1198, "state": "Tamil Nadu"},
    "SALEM": {"lat": 11.6643, "lon": 78.1460, "state": "Tamil Nadu"},
    "TIRUCHIRAPALLI": {"lat": 10.7905, "lon": 78.7047, "state": "Tamil Nadu"},

    # Uttar Pradesh
    "LUCKNOW": {"lat": 26.8467, "lon": 80.9462, "state": "Uttar Pradesh"},
    "KANPUR NAGAR": {"lat": 26.4499, "lon": 80.3319, "state": "Uttar Pradesh"},
    "AGRA": {"lat": 27.1767, "lon": 78.0081, "state": "Uttar Pradesh"},
    "VARANASI": {"lat": 25.3176, "lon": 82.9739, "state": "Uttar Pradesh"},
    "ALLAHABAD": {"lat": 25.4358, "lon": 81.8463, "state": "Uttar Pradesh"},
    "MEERUT": {"lat": 28.9845, "lon": 77.7064, "state": "Uttar Pradesh"},
    "GHAZIABAD": {"lat": 28.6692, "lon": 77.4538, "state": "Uttar Pradesh"},
    "NOIDA": {"lat": 28.5355, "lon": 77.3910, "state": "Uttar Pradesh"},
    "BAREILLY": {"lat": 28.3670, "lon": 79.4304, "state": "Uttar Pradesh"},
    "ALIGARH": {"lat": 27.8974, "lon": 78.0880, "state": "Uttar Pradesh"},

    # West Bengal
    "KOLKATA": {"lat": 22.5726, "lon": 88.3639, "state": "West Bengal"},
    "HOWRAH": {"lat": 22.5958, "lon": 88.2636, "state": "West Bengal"},
    "NORTH 24 PARGANAS": {"lat": 22.7230, "lon": 88.4807, "state": "West Bengal"},
    "SOUTH 24 PARGANAS": {"lat": 22.1352, "lon": 88.5414, "state": "West Bengal"},
    "BURDWAN": {"lat": 23.2324, "lon": 87.8615, "state": "West Bengal"},
    "SILIGURI": {"lat": 26.7271, "lon": 88.3953, "state": "West Bengal"},

    # Rajasthan
    "JAIPUR": {"lat": 26.9124, "lon": 75.7873, "state": "Rajasthan"},
    "JODHPUR": {"lat": 26.2389, "lon": 73.0243, "state": "Rajasthan"},
    "KOTA": {"lat": 25.2138, "lon": 75.8648, "state": "Rajasthan"},
    "UDAIPUR": {"lat": 24.5854, "lon": 73.7125, "state": "Rajasthan"},
    "AJMER": {"lat": 26.4499, "lon": 74.6399, "state": "Rajasthan"},

    # Kerala
    "THIRUVANANTHAPURAM": {"lat": 8.5241, "lon": 76.9366, "state": "Kerala"},
    "KOCHI": {"lat": 9.9312, "lon": 76.2673, "state": "Kerala"},
    "KOZHIKODE": {"lat": 11.2588, "lon": 75.7804, "state": "Kerala"},
    "THRISSUR": {"lat": 10.5276, "lon": 76.2144, "state": "Kerala"},

    # Punjab & Haryana
    "AMRITSAR": {"lat": 31.6340, "lon": 74.8723, "state": "Punjab"},
    "LUDHIANA": {"lat": 30.9010, "lon": 75.8573, "state": "Punjab"},
    "CHANDIGARH": {"lat": 30.7333, "lon": 76.7794, "state": "Chandigarh"},
    "GURGAON": {"lat": 28.4595, "lon": 77.0266, "state": "Haryana"},
    "FARIDABAD": {"lat": 28.4089, "lon": 77.3178, "state": "Haryana"}
}

# State fallback coordinates for districts not explicitly listed
STATE_FALLBACK_COORDINATES = {
    "ANDHRA PRADESH": (15.9129, 79.7400),
    "ARUNACHAL PRADESH": (28.2180, 94.7278),
    "ASSAM": (26.2006, 92.9376),
    "BIHAR": (25.0961, 85.3131),
    "CHHATTISGARH": (21.2787, 81.8661),
    "GOA": (15.2993, 74.1240),
    "GUJARAT": (22.2587, 71.1924),
    "HARYANA": (29.0588, 76.0856),
    "HIMACHAL PRADESH": (31.1048, 77.1734),
    "JAMMU & KASHMIR": (33.7782, 76.5762),
    "JHARKHAND": (23.6102, 85.2799),
    "KARNATAKA": (15.3173, 75.7139),
    "KERALA": (10.8505, 76.2711),
    "MADHYA PRADESH": (22.9734, 78.6569),
    "MAHARASHTRA": (19.7515, 75.7139),
    "MANIPUR": (24.6637, 93.9063),
    "MEGHALAYA": (25.4670, 91.3662),
    "MIZORAM": (23.1645, 92.9376),
    "NAGALAND": (26.1584, 94.5624),
    "ODISHA": (20.9517, 85.0985),
    "PUNJAB": (31.1471, 75.3412),
    "RAJASTHAN": (27.0238, 74.2179),
    "SIKKIM": (27.5330, 88.5122),
    "TAMIL NADU": (11.1271, 78.6569),
    "TELANGANA": (18.1124, 79.0193),
    "TRIPURA": (23.9408, 91.9882),
    "UTTAR PRADESH": (26.8467, 80.9462),
    "UTTARAKHAND": (30.0668, 79.0193),
    "WEST BENGAL": (22.9868, 87.8550),
    "A & N ISLANDS": (11.7401, 92.6586),
    "CHANDIGARH": (30.7333, 76.7794),
    "D & N HAVELI": (20.1809, 73.0169),
    "DAMAN & DIU": (20.4283, 72.8397),
    "DELHI": (28.7041, 77.1025),
    "LAKSHADWEEP": (10.5667, 72.6417),
    "PUDUCHERRY": (11.9416, 79.8083)
}


def download_file(filename: str, target_dir: Path) -> Path:
    """
    Download a file from the repository to the local target directory.
    
    Args:
        filename (str): Name of the dataset file.
        target_dir (Path): Destination folder.
        
    Returns:
        Path: Path to the downloaded file.
    """
    url = f"{NCRB_BASE_URL}/{filename}"
    target_path = target_dir / filename
    
    if target_path.exists() and target_path.stat().st_size > 1000:
        logger.info(f"File {filename} already exists ({target_path.stat().st_size} bytes), skipping download.")
        return target_path

    logger.info(f"Downloading {filename} from {url}...")
    try:
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        )
        with urllib.request.urlopen(req, timeout=30) as response, open(target_path, 'wb') as out_file:
            data = response.read()
            out_file.write(data)
        logger.info(f"Successfully downloaded {filename} ({len(data)} bytes).")
        return target_path
    except Exception as exc:
        logger.error(f"Failed to download {filename}: {exc}")
        raise


def build_district_gazetteer(output_path: Path):
    """
    Create a JSON gazetteer mapping normalized district names to Lat/Lon coordinates.
    
    Args:
        output_path (Path): Destination JSON path.
    """
    logger.info("Building district coordinate gazetteer...")
    gazetteer = {}
    for dist, coords in DISTRICT_COORDINATES.items():
        gazetteer[dist.upper().strip()] = coords

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump({
            "districts": gazetteer,
            "state_fallbacks": STATE_FALLBACK_COORDINATES
        }, f, indent=2)
    logger.info(f"Saved district coordinate gazetteer to {output_path} ({len(gazetteer)} districts).")


def main():
    """Execute the data acquisition pipeline."""
    logger.info("=== Starting Real NCRB Dataset Acquisition ===")
    
    # Download all official dataset files
    for key, filename in DATASET_FILES.items():
        download_file(filename, RAW_DATA_DIR)
        
    # Build geospatial coordinate map
    coord_file = DATA_DIR / "district_coordinates.json"
    build_district_gazetteer(coord_file)
    
    logger.info("=== Dataset Acquisition Complete ===")


if __name__ == "__main__":
    main()
