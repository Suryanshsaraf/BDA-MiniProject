"""
PowerPoint Presentation Generator for India Crime Intelligence Command Center.
Generates a professional, high-resolution 16:9 widescreen presentation deck with embedded screenshots.
"""

import os
import sys
from pathlib import Path

import pptx
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN
from pptx.enum.shapes import MSO_SHAPE

# -----------------------------------------------------------------------------
# THEME PALETTE (Modern Dark Enterprise Theme)
# -----------------------------------------------------------------------------
BG_COLOR = RGBColor(11, 17, 32)        # Navy 950
CARD_BG = RGBColor(19, 28, 49)        # Navy 900
CARD_BORDER = RGBColor(51, 65, 85)    # Slate 700
TEXT_WHITE = RGBColor(248, 250, 252)  # Slate 50
TEXT_MUTED = RGBColor(148, 163, 184)  # Slate 400
TEXT_ACCENT = RGBColor(99, 102, 241)  # Indigo 500
EMERALD = RGBColor(16, 185, 129)      # Emerald 500
AMBER = RGBColor(245, 158, 11)        # Amber 500
ROSE = RGBColor(244, 63, 94)          # Rose 500
CYAN = RGBColor(14, 165, 233)         # Sky 500

PROJECT_ROOT = Path(__file__).resolve().parent
SCREENSHOTS_DIR = PROJECT_ROOT / "presentation_screenshots"
OUTPUT_PPTX = PROJECT_ROOT / "National_Crime_Intelligence_Command_Center.pptx"


def set_slide_background(slide):
    """Fill slide background with uniform dark navy color."""
    background = slide.background
    fill = background.fill
    fill.solid()
    fill.fore_color.rgb = BG_COLOR


def add_card(slide, left, top, width, height, bg_color=CARD_BG, border_color=CARD_BORDER):
    """Draw a rounded rectangular card container."""
    shape = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, left, top, width, height)
    shape.fill.solid()
    shape.fill.fore_color.rgb = bg_color
    if border_color:
        shape.line.color.rgb = border_color
        shape.line.width = Pt(1.2)
    else:
        shape.line.fill.background()
    return shape


def add_header(slide, title_text, category_badge="BIG DATA ANALYTICS (BDA) CAPSTONE"):
    """Standardized slide header banner."""
    # Category Tag
    badge_box = slide.shapes.add_textbox(Inches(0.8), Inches(0.4), Inches(11.5), Inches(0.35))
    tf_b = badge_box.text_frame
    tf_b.word_wrap = True
    p_b = tf_b.paragraphs[0]
    p_b.text = f"● {category_badge.upper()}"
    p_b.font.size = Pt(11)
    p_b.font.bold = True
    p_b.font.color.rgb = TEXT_ACCENT

    # Main Slide Title
    title_box = slide.shapes.add_textbox(Inches(0.8), Inches(0.75), Inches(11.5), Inches(0.65))
    tf_t = title_box.text_frame
    tf_t.word_wrap = True
    p_t = tf_t.paragraphs[0]
    p_t.text = title_text
    p_t.font.size = Pt(24)
    p_t.font.bold = True
    p_t.font.color.rgb = TEXT_WHITE


def add_footer(slide, current_slide, total_slides=12):
    """Slide footer with project branding and page numbers."""
    footer_box = slide.shapes.add_textbox(Inches(0.8), Inches(7.0), Inches(11.7), Inches(0.35))
    tf = footer_box.text_frame
    p = tf.paragraphs[0]
    p.text = f"SVKM's NMIMS Navi Mumbai • B.Tech AI & Data Science • Slide {current_slide} of {total_slides}"
    p.font.size = Pt(10)
    p.font.color.rgb = TEXT_MUTED


def build_deck():
    prs = pptx.Presentation()
    prs.slide_width = Inches(13.333)
    prs.slide_height = Inches(7.5)
    blank_layout = prs.slide_layouts[6]

    # =========================================================================
    # SLIDE 1: TITLE SLIDE
    # =========================================================================
    slide1 = prs.slides.add_slide(blank_layout)
    set_slide_background(slide1)

    # Accent decorative top bar
    top_bar = slide1.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(0), Inches(0), Inches(13.333), Inches(0.12))
    top_bar.fill.solid()
    top_bar.fill.fore_color.rgb = TEXT_ACCENT
    top_bar.line.fill.background()

    # Center hero card
    add_card(slide1, Inches(1.2), Inches(1.0), Inches(10.933), Inches(5.5), bg_color=CARD_BG, border_color=CARD_BORDER)

    # Project Tag Badge
    badge = slide1.shapes.add_textbox(Inches(1.8), Inches(1.4), Inches(9.5), Inches(0.4))
    tf = badge.text_frame
    p = tf.paragraphs[0]
    p.text = "🏛️ BIG DATA ANALYTICS CAPSTONE PROJECT • APACHE HADOOP & SPARK ECOSYSTEM"
    p.font.size = Pt(12)
    p.font.bold = True
    p.font.color.rgb = EMERALD

    # Main Title
    t_box = slide1.shapes.add_textbox(Inches(1.8), Inches(1.9), Inches(9.8), Inches(1.4))
    tf = t_box.text_frame
    tf.word_wrap = True
    p = tf.paragraphs[0]
    p.text = "National Crime Intelligence Command Center"
    p.font.size = Pt(36)
    p.font.bold = True
    p.font.color.rgb = TEXT_WHITE

    # Subtitle
    sub_box = slide1.shapes.add_textbox(Inches(1.8), Inches(3.2), Inches(9.8), Inches(0.8))
    tf = sub_box.text_frame
    tf.word_wrap = True
    p = tf.paragraphs[0]
    p.text = "A Distributed Lambda Architecture for Longitudinal Crime Analytics & Real-Time Anomaly Surveillance"
    p.font.size = Pt(16)
    p.font.color.rgb = CYAN

    # Divider line
    div = slide1.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(1.8), Inches(4.1), Inches(9.7), Inches(0.02))
    div.fill.solid()
    div.fill.fore_color.rgb = CARD_BORDER
    div.line.fill.background()

    # Authors & Affiliation
    meta_box = slide1.shapes.add_textbox(Inches(1.8), Inches(4.3), Inches(9.8), Inches(1.8))
    tf = meta_box.text_frame
    
    p1 = tf.paragraphs[0]
    p1.text = "Team Members: Suryansh Saraf  •  Manas Dhumal  •  Soham Hande"
    p1.font.size = Pt(14)
    p1.font.bold = True
    p1.font.color.rgb = TEXT_WHITE

    p2 = tf.add_paragraph()
    p2.text = "Department: B.Tech Artificial Intelligence & Data Science"
    p2.font.size = Pt(13)
    p2.font.color.rgb = TEXT_MUTED

    p3 = tf.add_paragraph()
    p3.text = "Institution: SVKM's NMIMS, Navi Mumbai, Maharashtra, India"
    p3.font.size = Pt(13)
    p3.font.color.rgb = TEXT_MUTED

    add_footer(slide1, 1)

    # =========================================================================
    # SLIDE 2: PROBLEM STATEMENT & MOTIVATION
    # =========================================================================
    slide2 = prs.slides.add_slide(blank_layout)
    set_slide_background(slide2)
    add_header(slide2, "Executive Problem Statement & Research Motivation", "CHALLENGES & SYSTEM GOALS")

    # Left Column Card (Challenges)
    add_card(slide2, Inches(0.8), Inches(1.6), Inches(5.6), Inches(5.1))
    c_box = slide2.shapes.add_textbox(Inches(1.1), Inches(1.8), Inches(5.0), Inches(4.6))
    tf = c_box.text_frame
    tf.word_wrap = True

    p = tf.paragraphs[0]
    p.text = "⚠️ Limitations in Traditional Crime Reporting"
    p.font.size = Pt(18)
    p.font.bold = True
    p.font.color.rgb = ROSE

    bullets_c = [
        ("High Latency of Retrospective Reports", "NCRB compendiums are published with a 12-18 month lag, precluding proactive deployment."),
        ("Multi-Decade Schema Entropy", "22 years of Indian records (2001-2022) exhibit severe boundary shifts, state reorganizations (Telangana 2014), and name variations."),
        ("Geospatial Disconnection", "Standard tables lack latitude/longitude coordinates, preventing spatial choropleth clustering."),
        ("Batch vs Real-Time Gap", "Traditional batch jobs cannot alert field units to emerging localized incident surges occurring tonight.")
    ]
    for b_title, b_desc in bullets_c:
        p_t = tf.add_paragraph()
        p_t.space_before = Pt(10)
        p_t.text = f"• {b_title}:"
        p_t.font.size = Pt(13)
        p_t.font.bold = True
        p_t.font.color.rgb = TEXT_WHITE

        p_d = tf.add_paragraph()
        p_d.text = f"   {b_desc}"
        p_d.font.size = Pt(12)
        p_d.font.color.rgb = TEXT_MUTED

    # Right Column Card (Proposed Solution)
    add_card(slide2, Inches(6.8), Inches(1.6), Inches(5.7), Inches(5.1))
    s_box = slide2.shapes.add_textbox(Inches(7.1), Inches(1.8), Inches(5.1), Inches(4.6))
    tf_s = s_box.text_frame
    tf_s.word_wrap = True

    p = tf_s.paragraphs[0]
    p.text = "🎯 Proposed Distributed BDA Solution"
    p.font.size = Pt(18)
    p.font.bold = True
    p.font.color.rgb = EMERALD

    bullets_s = [
        ("Enterprise Lambda Architecture", "Unifies massive historical HDFS batch analytics with sub-second Kafka streaming anomaly detection."),
        ("18,146 District-Year Vectors", "Cleans, deduplicates, and maps all Indian records into 36 canonical States and Union Territories."),
        ("Spatial Imputation via Gazetteer", "Broadcasts an O(1) in-memory gazetteer mapping precise centroids to every police district."),
        ("Rolling Gaussian Z-Score Engine", "Statistically flags localized surges exceeding 2.5σ over baseline mean for tactical patrol dispatch.")
    ]
    for b_title, b_desc in bullets_s:
        p_t = tf_s.add_paragraph()
        p_t.space_before = Pt(10)
        p_t.text = f"✔ {b_title}:"
        p_t.font.size = Pt(13)
        p_t.font.bold = True
        p_t.font.color.rgb = TEXT_WHITE

        p_d = tf_s.add_paragraph()
        p_d.text = f"   {b_desc}"
        p_d.font.size = Pt(12)
        p_d.font.color.rgb = TEXT_MUTED

    add_footer(slide2, 2)

    # =========================================================================
    # SLIDE 3: SYSTEM ARCHITECTURE (LAMBDA BLUEPRINT)
    # =========================================================================
    slide3 = prs.slides.add_slide(blank_layout)
    set_slide_background(slide3)
    add_header(slide3, "End-to-End Distributed Lambda Architecture", "SYSTEM DESIGN")

    cards = [
        ("1. BATCH LAYER", "Hadoop HDFS + PySpark ETL", CYAN, [
            ("Hadoop HDFS v3.2.1", "Replication R=3, Snappy Parquet storage in /data/crimes/."),
            ("PySpark 3.5 Distributed ETL", "Cleans 18,146 vectors; normalizes 36 States/UTs."),
            ("29-Dimensional Feature Space", "Violent, property, women crime ratios & demographics."),
            ("Apache Hive Data Warehouse", "External tables partitioned by YEAR & STATE_UT.")
        ]),
        ("2. SPEED LAYER", "Apache Kafka Streaming", AMBER, [
            ("Topic 'live_crimes'", "Simulates real-time police incident dispatches across India."),
            ("8 Broker Partitions", "P:0 to P:7 with deterministic hashing and offset tracking."),
            ("Sub-20ms Latency", "High-throughput publish-subscribe streaming ingest."),
            ("Rolling Z-Score Detector", "Flags statistical surges: Z = (X - μ) / σ >= 2.5σ.")
        ]),
        ("3. SERVING & ML LAYER", "Spark MLlib + React Command Center", EMERALD, [
            ("Spark MLlib KMeans (K=15)", "Spatial clustering into 15 geographic crime corridors."),
            ("Random Forest Classifier", "100 trees, 76.5% accuracy, 0.9412 AUC-ROC, 100% precision."),
            ("Tactical Leaflet GIS", "22-year time travel player (2001-2022) with danger corridors."),
            ("AI Force Multiplier Sandbox", "Simulates patrol saturation & deterrence ROI.")
        ])
    ]

    for i, (layer_title, layer_sub, accent_color, items) in enumerate(cards):
        left_pos = Inches(0.8 + i * 3.95)
        add_card(slide3, left_pos, Inches(1.6), Inches(3.75), Inches(5.1))
        
        box = slide3.shapes.add_textbox(left_pos + Inches(0.25), Inches(1.85), Inches(3.25), Inches(4.5))
        tf = box.text_frame
        tf.word_wrap = True

        p_h = tf.paragraphs[0]
        p_h.text = layer_title
        p_h.font.size = Pt(16)
        p_h.font.bold = True
        p_h.font.color.rgb = accent_color

        p_sub = tf.add_paragraph()
        p_sub.text = layer_sub
        p_sub.font.size = Pt(12)
        p_sub.font.color.rgb = TEXT_MUTED
        p_sub.space_after = Pt(12)

        for item_title, item_desc in items:
            p_it = tf.add_paragraph()
            p_it.text = f"• {item_title}"
            p_it.font.size = Pt(12)
            p_it.font.bold = True
            p_it.font.color.rgb = TEXT_WHITE
            p_it.space_before = Pt(6)

            p_id = tf.add_paragraph()
            p_id.text = f"  {item_desc}"
            p_id.font.size = Pt(11)
            p_id.font.color.rgb = TEXT_MUTED

    add_footer(slide3, 3)

    # =========================================================================
    # SLIDE 4: STORAGE & DISTRIBUTED ETL (HDFS + PYSPARK)
    # =========================================================================
    slide4 = prs.slides.add_slide(blank_layout)
    set_slide_background(slide4)
    add_header(slide4, "Distributed Storage & Data Pipeline: HDFS, PySpark & Hive", "DATA ENGINEERING")

    # HDFS Card
    add_card(slide4, Inches(0.8), Inches(1.6), Inches(3.75), Inches(5.1))
    box1 = slide4.shapes.add_textbox(Inches(1.05), Inches(1.85), Inches(3.25), Inches(4.5))
    tf1 = box1.text_frame
    tf1.word_wrap = True
    p = tf1.paragraphs[0]
    p.text = "📦 Apache Hadoop HDFS"
    p.font.size = Pt(16)
    p.font.bold = True
    p.font.color.rgb = CYAN

    hdfs_points = [
        ("Cluster Topology", "NameNode (9870/9000) & DataNodes (9864) with block replication factor R=3."),
        ("Snappy-Compressed Parquet", "Columnar binary format delivers 82% disk footprint reduction over raw CSV."),
        ("Predicate Pushdown", "Min/max metadata per column chunk allows PySpark to skip reading irrelevant blocks."),
        ("Storage URI Endpoints", "hdfs://namenode:9000/data/crimes/ partitioned by Year and State.")
    ]
    for h_t, h_d in hdfs_points:
        p_t = tf1.add_paragraph()
        p_t.space_before = Pt(8)
        p_t.text = f"• {h_t}:"
        p_t.font.size = Pt(12)
        p_t.font.bold = True
        p_t.font.color.rgb = TEXT_WHITE
        p_d = tf1.add_paragraph()
        p_d.text = f"  {h_d}"
        p_d.font.size = Pt(11)
        p_d.font.color.rgb = TEXT_MUTED

    # PySpark Card
    add_card(slide4, Inches(4.8), Inches(1.6), Inches(3.75), Inches(5.1))
    box2 = slide4.shapes.add_textbox(Inches(5.05), Inches(1.85), Inches(3.25), Inches(4.5))
    tf2 = box2.text_frame
    tf2.word_wrap = True
    p = tf2.paragraphs[0]
    p.text = "⚡ Apache PySpark 3.5 ETL"
    p.font.size = Pt(16)
    p.font.bold = True
    p.font.color.rgb = AMBER

    spark_points = [
        ("36 Canonical Entities", "Standardizes historical nomenclature shifts (Telangana, Delhi UT, Orissa)."),
        ("Subtotal Pruning", "Filters out duplicate 'TOTAL' and 'STATE TOTAL' rows to prevent double-counting."),
        ("Spatial Imputation", "Broadcasts gazetteer coordinates [Latitude, Longitude] with O(1) memory lookup."),
        ("29-Dimensional Vectors", "Computes violent, property, women, and economic ratios plus demographic density.")
    ]
    for s_t, s_d in spark_points:
        p_t = tf2.add_paragraph()
        p_t.space_before = Pt(8)
        p_t.text = f"• {s_t}:"
        p_t.font.size = Pt(12)
        p_t.font.bold = True
        p_t.font.color.rgb = TEXT_WHITE
        p_d = tf2.add_paragraph()
        p_d.text = f"  {s_d}"
        p_d.font.size = Pt(11)
        p_d.font.color.rgb = TEXT_MUTED

    # Hive Card
    add_card(slide4, Inches(8.8), Inches(1.6), Inches(3.75), Inches(5.1))
    box3 = slide4.shapes.add_textbox(Inches(9.05), Inches(1.85), Inches(3.25), Inches(4.5))
    tf3 = box3.text_frame
    tf3.word_wrap = True
    p = tf3.paragraphs[0]
    p.text = "🗄️ Apache Hive Warehouse"
    p.font.size = Pt(16)
    p.font.bold = True
    p.font.color.rgb = EMERALD

    hive_points = [
        ("Database Architecture", "Managed database 'india_crime_db' over HDFS Parquet storage."),
        ("External Table Catalog", "Tables: crimes_raw, crimes_clean, and crimes_features."),
        ("Hierarchical Partitioning", "Partitioned by (YEAR INT, STATE_UT STRING) enabling partition pruning."),
        ("OLAP Analytics", "Supports multi-dimensional SQL aggregation across time, regions, and offense types.")
    ]
    for hv_t, hv_d in hive_points:
        p_t = tf3.add_paragraph()
        p_t.space_before = Pt(8)
        p_t.text = f"• {hv_t}:"
        p_t.font.size = Pt(12)
        p_t.font.bold = True
        p_t.font.color.rgb = TEXT_WHITE
        p_d = tf3.add_paragraph()
        p_d.text = f"  {hv_d}"
        p_d.font.size = Pt(11)
        p_d.font.color.rgb = TEXT_MUTED

    add_footer(slide4, 4)

    # =========================================================================
    # SLIDE 5: TACTICAL GIS COMMAND CENTER (WITH SCREENSHOT 1)
    # =========================================================================
    slide5 = prs.slides.add_slide(blank_layout)
    set_slide_background(slide5)
    add_header(slide5, "Tactical GIS Command Center & 22-Year Temporal Player", "SERVING & VISUALIZATION LAYER")

    # Screenshot on Left
    s1_img = SCREENSHOTS_DIR / "slide1_gis_map.png"
    if s1_img.exists():
        slide5.shapes.add_picture(str(s1_img), Inches(0.8), Inches(1.6), width=Inches(7.2))

    # Details Card on Right
    add_card(slide5, Inches(8.2), Inches(1.6), Inches(4.35), Inches(5.1))
    box = slide5.shapes.add_textbox(Inches(8.45), Inches(1.85), Inches(3.85), Inches(4.6))
    tf = box.text_frame
    tf.word_wrap = True

    p = tf.paragraphs[0]
    p.text = "🗺️ Tactical GIS Interface"
    p.font.size = Pt(18)
    p.font.bold = True
    p.font.color.rgb = CYAN

    gis_points = [
        ("22-Year Time-Travel Engine", "Scrub slider dynamically updates state polygon choropleths (2001-2022) in client memory."),
        ("Leaflet + Esri ArcGIS Tiles", "Interactive zoomable map of all 36 Indian States and Union Territories."),
        ("Top 50 Danger Corridor Nodes", "Pulsing circular markers with radius proportional to local crime volume."),
        ("Live Ingestion Header", "Displays real-time Kafka throughput (3.3 ev/s), latency (12ms), and active surge alerts.")
    ]
    for g_t, g_d in gis_points:
        p_t = tf.add_paragraph()
        p_t.space_before = Pt(10)
        p_t.text = f"• {g_t}:"
        p_t.font.size = Pt(12)
        p_t.font.bold = True
        p_t.font.color.rgb = TEXT_WHITE

        p_d = tf.add_paragraph()
        p_d.text = f"   {g_d}"
        p_d.font.size = Pt(11)
        p_d.font.color.rgb = TEXT_MUTED

    add_footer(slide5, 5)

    # =========================================================================
    # SLIDE 6: REAL-TIME KAFKA STREAM & ANOMALY ENGINE (WITH SCREENSHOT 2)
    # =========================================================================
    slide6 = prs.slides.add_slide(blank_layout)
    set_slide_background(slide6)
    add_header(slide6, "Speed Layer: Real-Time Kafka Stream & Anomaly Engine", "STREAMING ARCHITECTURE")

    # Screenshot on Left
    s2_img = SCREENSHOTS_DIR / "slide2_kafka_terminal.png"
    if s2_img.exists():
        slide6.shapes.add_picture(str(s2_img), Inches(0.8), Inches(1.6), width=Inches(7.2))

    # Details Card on Right
    add_card(slide6, Inches(8.2), Inches(1.6), Inches(4.35), Inches(5.1))
    box = slide6.shapes.add_textbox(Inches(8.45), Inches(1.85), Inches(3.85), Inches(4.6))
    tf = box.text_frame
    tf.word_wrap = True

    p = tf.paragraphs[0]
    p.text = "📟 Real-Time Stream Terminal"
    p.font.size = Pt(18)
    p.font.bold = True
    p.font.color.rgb = AMBER

    kafka_points = [
        ("Topic 'live_crimes'", "Streams authentic NCRB incident vectors simulating live police dispatch feeds."),
        ("8 Distributed Partitions", "P:0 to P:7 with offset commits (e.g. Offset 149,771) ensuring total ordering."),
        ("Rolling Gaussian Z-Score", "Evaluates Z = (Current_Rate - μ) / σ against historical district variance."),
        ("Dual Alert Thresholds", "Z >= 2.5σ triggers Warning Surge; Z >= 3.0σ escalates to Critical Tactical Patrol Dispatch.")
    ]
    for k_t, k_d in kafka_points:
        p_t = tf.add_paragraph()
        p_t.space_before = Pt(10)
        p_t.text = f"• {k_t}:"
        p_t.font.size = Pt(12)
        p_t.font.bold = True
        p_t.font.color.rgb = TEXT_WHITE

        p_d = tf.add_paragraph()
        p_d.text = f"   {k_d}"
        p_d.font.size = Pt(11)
        p_d.font.color.rgb = TEXT_MUTED

    add_footer(slide6, 6)

    # =========================================================================
    # SLIDE 7: SPARK MLLIB MACHINE LEARNING
    # =========================================================================
    slide7 = prs.slides.add_slide(blank_layout)
    set_slide_background(slide7)
    add_header(slide7, "Distributed Machine Learning with Spark MLlib", "PREDICTIVE ANALYTICS")

    # Left Card: KMeans
    add_card(slide7, Inches(0.8), Inches(1.6), Inches(3.75), Inches(5.1))
    box1 = slide7.shapes.add_textbox(Inches(1.05), Inches(1.85), Inches(3.25), Inches(4.5))
    tf1 = box1.text_frame
    tf1.word_wrap = True
    p = tf1.paragraphs[0]
    p.text = "🌐 Spatial Clustering (KMeans)"
    p.font.size = Pt(16)
    p.font.bold = True
    p.font.color.rgb = CYAN

    km_points = [
        ("Hyperparameters", "K=15 clusters, maxIter=30, evaluated across coordinate space [Lat, Lon]."),
        ("WSSSE Optimization", "Minimizes Within-Cluster Sum of Squared Errors across all centroids."),
        ("Corridor Discovery", "Segments India into 15 macro crime zones (Southern corridor, Northern plains, Western ports)."),
        ("Cluster 3 Centroid", "Southern belt (AP, TN, Kerala): 10.47M crimes across 92 distinct districts.")
    ]
    for k_t, k_d in km_points:
        p_t = tf1.add_paragraph()
        p_t.space_before = Pt(8)
        p_t.text = f"• {k_t}:"
        p_t.font.size = Pt(12)
        p_t.font.bold = True
        p_t.font.color.rgb = TEXT_WHITE
        p_d = tf1.add_paragraph()
        p_d.text = f"  {k_d}"
        p_d.font.size = Pt(11)
        p_d.font.color.rgb = TEXT_MUTED

    # Middle Card: Random Forest
    add_card(slide7, Inches(4.8), Inches(1.6), Inches(3.75), Inches(5.1))
    box2 = slide7.shapes.add_textbox(Inches(5.05), Inches(1.85), Inches(3.25), Inches(4.5))
    tf2 = box2.text_frame
    tf2.word_wrap = True
    p = tf2.paragraphs[0]
    p.text = "🌲 Severity Classifier (RF)"
    p.font.size = Pt(16)
    p.font.bold = True
    p.font.color.rgb = EMERALD

    rf_points = [
        ("Model Ensemble", "100 decorrelated decision trees, max depth 10, evaluated on 80/20 train/test split."),
        ("76.53% Accuracy", "Tested against 3,630 out-of-sample district-year evaluation vectors."),
        ("0.9412 AUC-ROC", "Demonstrates exceptional discriminative capability for high-severity crime zones."),
        ("100% Precision", "Zero false positives on test split (2,684 TN, 94 TP), eliminating costly false alarms.")
    ]
    for r_t, r_d in rf_points:
        p_t = tf2.add_paragraph()
        p_t.space_before = Pt(8)
        p_t.text = f"• {r_t}:"
        p_t.font.size = Pt(12)
        p_t.font.bold = True
        p_t.font.color.rgb = TEXT_WHITE
        p_d = tf2.add_paragraph()
        p_d.text = f"  {r_d}"
        p_d.font.size = Pt(11)
        p_d.font.color.rgb = TEXT_MUTED

    # Right Card: Feature Importances
    add_card(slide7, Inches(8.8), Inches(1.6), Inches(3.75), Inches(5.1))
    box3 = slide7.shapes.add_textbox(Inches(9.05), Inches(1.85), Inches(3.25), Inches(4.5))
    tf3 = box3.text_frame
    tf3.word_wrap = True
    p = tf3.paragraphs[0]
    p.text = "📊 Feature Importances (Gini)"
    p.font.size = Pt(16)
    p.font.bold = True
    p.font.color.rgb = AMBER

    fi_points = [
        ("1. DISTRICT_RISK_SCORE", "34.86% — Historical 5-year moving average volume."),
        ("2. CRIME_RATE_PER_100K", "26.48% — Population-standardized incidence rate."),
        ("3. URBAN_PERCENTAGE", "11.08% — Urbanization concentration indicator."),
        ("4. POPULATION_DENSITY", "9.32% — Demographic exposure factor."),
        ("5. POLICE_PER_LAKH", "5.97% — Administrative law enforcement ratio.")
    ]
    for f_t, f_d in fi_points:
        p_t = tf3.add_paragraph()
        p_t.space_before = Pt(8)
        p_t.text = f"• {f_t}:"
        p_t.font.size = Pt(12)
        p_t.font.bold = True
        p_t.font.color.rgb = TEXT_WHITE
        p_d = tf3.add_paragraph()
        p_d.text = f"  {f_d}"
        p_d.font.size = Pt(11)
        p_d.font.color.rgb = TEXT_MUTED

    add_footer(slide7, 7)

    # =========================================================================
    # SLIDE 8: AI FORCE MULTIPLIER SIMULATOR (WITH SCREENSHOT 3)
    # =========================================================================
    slide8 = prs.slides.add_slide(blank_layout)
    set_slide_background(slide8)
    add_header(slide8, "AI Force Multiplier Resource Allocation Simulator", "POLICY & DECISION SUPPORT")

    # Screenshot on Left
    s3_img = SCREENSHOTS_DIR / "slide3_ai_simulator.png"
    if s3_img.exists():
        slide8.shapes.add_picture(str(s3_img), Inches(0.8), Inches(1.6), width=Inches(7.2))

    # Details Card on Right
    add_card(slide8, Inches(8.2), Inches(1.6), Inches(4.35), Inches(5.1))
    box = slide8.shapes.add_textbox(Inches(8.45), Inches(1.85), Inches(3.85), Inches(4.6))
    tf = box.text_frame
    tf.word_wrap = True

    p = tf.paragraphs[0]
    p.text = "🛡️ Predictive Policing Sandbox"
    p.font.size = Pt(18)
    p.font.bold = True
    p.font.color.rgb = EMERALD

    sim_points = [
        ("Interactive Levers", "Adjust Police Density (140/lakh), PCR Response Time (14 min), and Night Patrols (55%)."),
        ("Smart CCTV Grid Density", "Simulate surveillance sensor density (40% to 85% monitored)."),
        ("Projected Impact", "Real-time regression forecasting predicted crime deterrence (-2%) and apprehension surge (+9.1%)."),
        ("Crimes Averted Metric", "Projects ~977 offences averted annually per optimized metropolitan sector.")
    ]
    for sm_t, sm_d in sim_points:
        p_t = tf.add_paragraph()
        p_t.space_before = Pt(10)
        p_t.text = f"• {sm_t}:"
        p_t.font.size = Pt(12)
        p_t.font.bold = True
        p_t.font.color.rgb = TEXT_WHITE

        p_d = tf.add_paragraph()
        p_d.text = f"   {sm_d}"
        p_d.font.size = Pt(11)
        p_d.font.color.rgb = TEXT_MUTED

    add_footer(slide8, 8)

    # =========================================================================
    # SLIDE 9: DISTRICT CRIME DNA DUEL (WITH SCREENSHOT 4)
    # =========================================================================
    slide9 = prs.slides.add_slide(blank_layout)
    set_slide_background(slide9)
    add_header(slide9, "District Crime DNA Duel: Multi-Axis Radar Benchmark", "COMPARATIVE INTELLIGENCE")

    # Screenshot on Left
    s4_img = SCREENSHOTS_DIR / "slide4_district_duel.png"
    if s4_img.exists():
        slide9.shapes.add_picture(str(s4_img), Inches(0.8), Inches(1.6), width=Inches(7.2))

    # Details Card on Right
    add_card(slide9, Inches(8.2), Inches(1.6), Inches(4.35), Inches(5.1))
    box = slide9.shapes.add_textbox(Inches(8.45), Inches(1.85), Inches(3.85), Inches(4.6))
    tf = box.text_frame
    tf.word_wrap = True

    p = tf.paragraphs[0]
    p.text = "🥊 Head-to-Head Radar Profiling"
    p.font.size = Pt(18)
    p.font.bold = True
    p.font.color.rgb = ROSE

    duel_points = [
        ("Direct District Duel", "Benchmark any two districts (e.g. Bangalore vs. Mumbai) across 6 crime axes."),
        ("6 Evaluated Dimensions", "Violent Crimes %, Property Theft %, Women Safety %, Economic Fraud %, Cybercrime %, and Other IPC %."),
        ("Archetype Discovery", "Both metros identified as 'High-Density Property Theft Hubs' with over 53% property offenses."),
        ("Conviction & Police Comparison", "Mumbai exhibits 64.8% conviction vs. Bangalore's 42.7%; Police: 257.5 vs 202.6 per lakh.")
    ]
    for d_t, d_d in duel_points:
        p_t = tf.add_paragraph()
        p_t.space_before = Pt(10)
        p_t.text = f"• {d_t}:"
        p_t.font.size = Pt(12)
        p_t.font.bold = True
        p_t.font.color.rgb = TEXT_WHITE

        p_d = tf.add_paragraph()
        p_d.text = f"   {d_d}"
        p_d.font.size = Pt(11)
        p_d.font.color.rgb = TEXT_MUTED

    add_footer(slide9, 9)

    # =========================================================================
    # SLIDE 10: LONGITUDINAL TRENDS & DOSSIER (WITH SCREENSHOTS 5 & 6)
    # =========================================================================
    slide10 = prs.slides.add_slide(blank_layout)
    set_slide_background(slide10)
    add_header(slide10, "Longitudinal Crime Trends & National District Dossier", "HISTORICAL & OPERATIONAL DOSSIER")

    # Screenshot 5 (Top Left)
    s5_img = SCREENSHOTS_DIR / "slide5_macro_trends.png"
    if s5_img.exists():
        slide10.shapes.add_picture(str(s5_img), Inches(0.8), Inches(1.6), width=Inches(5.7))

    # Screenshot 6 (Top Right)
    s6_img = SCREENSHOTS_DIR / "slide6_dossier_matrix.png"
    if s6_img.exists():
        slide10.shapes.add_picture(str(s6_img), Inches(6.8), Inches(1.6), width=Inches(5.7))

    # Bottom summary card
    add_card(slide10, Inches(0.8), Inches(5.2), Inches(11.7), Inches(1.6))
    box = slide10.shapes.add_textbox(Inches(1.05), Inches(5.35), Inches(11.2), Inches(1.3))
    tf = box.text_frame
    tf.word_wrap = True

    p = tf.paragraphs[0]
    p.text = "📈 Key Macro & Operational Takeaways:"
    p.font.size = Pt(14)
    p.font.bold = True
    p.font.color.rgb = CYAN

    p_body = tf.add_paragraph()
    p_body.text = "• Longitudinal Analysis (2001–2022): Captures steady growth in property and cybercrime alongside clearance improvements, reflecting 2014 reporting reforms and 2020 pandemic mobility impacts.\n• National Intelligence Dossier: Searchable registry indexing 150+ districts by archetype, safety score, and police density with one-click 'Print Executive Dossier' briefing export."
    p_body.font.size = Pt(12)
    p_body.font.color.rgb = TEXT_WHITE

    add_footer(slide10, 10)

    # =========================================================================
    # SLIDE 11: TECHNICAL VERIFICATION & IEEE PAPER
    # =========================================================================
    slide11 = prs.slides.add_slide(blank_layout)
    set_slide_background(slide11)
    add_header(slide11, "Empirical Validation, Test Suite & IEEE Research Paper", "ACADEMIC & TECHNICAL DELIVERABLES")

    cards_v = [
        ("✔ 10/10 UNIT TESTS PASSING", CYAN, [
            ("test_pipeline.py", "Verifies deduplication, coordinate lookup, state mapping, and ratios."),
            ("test_live_stream.py", "Verifies authentic NCRB record loading, Kafka JSON events, and Z-score alerts."),
            ("100% Pass Rate", "Ran 10 tests in 0.134s with zero regressions.")
        ]),
        ("⚡ PRODUCTION ARCHITECTURE", AMBER, [
            ("Snappy Parquet on HDFS", "82% compression ratio over CSV with predicate pushdown."),
            ("Apache Airflow DAG", "Daily scheduled pipeline orchestrating ETL and training."),
            ("8 Kafka Broker Partitions", "Sub-20ms streaming latency and continuous rolling state flush.")
        ]),
        ("📄 IEEE RESEARCH PAPER", EMERALD, [
            ("Standard IEEE Format", "Full 424-line LaTeX document in research_paper.tex."),
            ("Mathematical Rigor", "WSSSE KMeans, Gini impurity, and Gaussian Z-scores fully formulated."),
            ("Published to GitHub", "1-Click compilation ready for Overleaf and peer review.")
        ])
    ]

    for i, (v_title, v_color, v_points) in enumerate(cards_v):
        left_pos = Inches(0.8 + i * 3.95)
        add_card(slide11, left_pos, Inches(1.6), Inches(3.75), Inches(5.1))
        
        box = slide11.shapes.add_textbox(left_pos + Inches(0.25), Inches(1.85), Inches(3.25), Inches(4.5))
        tf = box.text_frame
        tf.word_wrap = True

        p_h = tf.paragraphs[0]
        p_h.text = v_title
        p_h.font.size = Pt(15)
        p_h.font.bold = True
        p_h.font.color.rgb = v_color

        for pt_title, pt_desc in v_points:
            p_it = tf.add_paragraph()
            p_it.text = f"• {pt_title}:"
            p_it.font.size = Pt(12)
            p_it.font.bold = True
            p_it.font.color.rgb = TEXT_WHITE
            p_it.space_before = Pt(8)

            p_id = tf.add_paragraph()
            p_id.text = f"  {pt_desc}"
            p_id.font.size = Pt(11)
            p_id.font.color.rgb = TEXT_MUTED

    add_footer(slide11, 11)

    # =========================================================================
    # SLIDE 12: CONCLUSION & Q&A
    # =========================================================================
    slide12 = prs.slides.add_slide(blank_layout)
    set_slide_background(slide12)

    # Decorative top bar
    top_bar = slide12.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(0), Inches(0), Inches(13.333), Inches(0.12))
    top_bar.fill.solid()
    top_bar.fill.fore_color.rgb = EMERALD
    top_bar.line.fill.background()

    add_card(slide12, Inches(1.2), Inches(1.0), Inches(10.933), Inches(5.5), bg_color=CARD_BG, border_color=CARD_BORDER)

    box = slide12.shapes.add_textbox(Inches(1.8), Inches(1.4), Inches(9.8), Inches(4.7))
    tf = box.text_frame
    tf.word_wrap = True

    p = tf.paragraphs[0]
    p.text = "🎯 Summary of Achievements & Conclusion"
    p.font.size = Pt(28)
    p.font.bold = True
    p.font.color.rgb = TEXT_WHITE

    p_sub = tf.add_paragraph()
    p_sub.text = "Bridging Retrospective Big Data Analytics with Sub-Second Real-Time Tactical Police Dispatch"
    p_sub.font.size = Pt(15)
    p_sub.font.color.rgb = CYAN
    p_sub.space_after = Pt(14)

    summary_items = [
        "Unifies 22 years of historical records (18,146 vectors) via Hadoop HDFS and PySpark distributed ETL.",
        "Spark MLlib Random Forest delivers 76.53% accuracy and 0.9412 AUC-ROC with zero false-alarm dispatch waste.",
        "Speed Layer sustains sub-20ms streaming latency across 8 Kafka partitions with rolling Z-score anomaly alerts.",
        "Full interactive React GIS command center live at http://localhost:3000 with AI force multiplier sandbox.",
        "Complete IEEE-formatted conference paper research_paper.tex committed and ready for publication."
    ]
    for item in summary_items:
        p_item = tf.add_paragraph()
        p_item.text = f"✔  {item}"
        p_item.font.size = Pt(13)
        p_item.font.color.rgb = TEXT_WHITE
        p_item.space_before = Pt(6)

    p_qa = tf.add_paragraph()
    p_qa.space_before = Pt(20)
    p_qa.text = "THANK YOU! We welcome questions from the panel."
    p_qa.font.size = Pt(20)
    p_qa.font.bold = True
    p_qa.font.color.rgb = AMBER
    p_qa.alignment = PP_ALIGN.CENTER

    add_footer(slide12, 12)

    # Save presentation
    prs.save(str(OUTPUT_PPTX))
    print(f"Presentation saved successfully to {OUTPUT_PPTX} ({OUTPUT_PPTX.stat().st_size} bytes)")


if __name__ == "__main__":
    build_deck()
