"""SVG visualization for road anomaly detection snapshots.

Generates dashcam-style SVG thumbnails for each anomaly class when real
media is unavailable from MinIO.
"""
from __future__ import annotations


def render_road_snapshot_svg(
    id_str: str,
    severity: str,
    passes: int,
    lat: float,
    lon: float,
    confidence: float = 0.94,
    anomaly_class: str = "pothole",
) -> str:
    """Return an SVG string depicting a road-camera snapshot with AI detection overlays."""
    sev_upper = (severity or "Low").upper()
    cls_lower = (anomaly_class or "pothole").lower()

    # Custom color themes per anomaly type & severity
    class_color_map = {
        "debris": "#F59E0B",
        "road_debris": "#F59E0B",
        "object": "#F59E0B",
        "pothole": "#EF4444",
        "crack": "#F97316",
        "manhole": "#A855F7",
        "waterlogging": "#0EA5E9",
        "sewage": "#14B8A6",
        "garbage_dump": "#F43F5E",
    }
    box_color = class_color_map.get(cls_lower, "#EF4444")

    class_title_map = {
        "debris": "ROAD DEBRIS",
        "road_debris": "ROAD DEBRIS",
        "object": "ROAD OBSTACLE",
        "pothole": "POTHOLE",
        "crack": "ROAD CRACK",
        "manhole": "MANHOLE HAZARD",
        "waterlogging": "WATERLOGGING",
        "sewage": "SEWAGE OVERFLOW",
        "garbage_dump": "GARBAGE DUMP",
    }
    class_title = class_title_map.get(cls_lower, cls_lower.upper().replace("_", " "))

    # Generate custom SVG visuals based on anomaly category
    if cls_lower in ("debris", "road_debris", "object"):
        anomaly_visual = """
  <!-- Road Debris / Fallen Cargo Obstacle -->
  <defs>
    <linearGradient id="debrisGrad" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#F59E0B"/>
      <stop offset="60%" stop-color="#D97706"/>
      <stop offset="100%" stop-color="#78350F"/>
    </linearGradient>
  </defs>
  <!-- Fallen wooden pallet / debris barrier -->
  <rect x="180" y="115" width="120" height="60" rx="6" fill="url(#debrisGrad)" filter="url(#shadow)"/>
  <rect x="190" y="125" width="100" height="12" rx="2" fill="#FEF3C7" opacity="0.4"/>
  <rect x="190" y="145" width="100" height="12" rx="2" fill="#FEF3C7" opacity="0.3"/>
  <line x1="210" y1="115" x2="210" y2="175" stroke="#451A03" stroke-width="2.5"/>
  <line x1="270" y1="115" x2="270" y2="175" stroke="#451A03" stroke-width="2.5"/>
  <!-- Warning stripes -->
  <path d="M 230 115 L 245 175" stroke="#FEF08A" stroke-width="3" stroke-dasharray="6,4"/>
        """
    elif cls_lower == "waterlogging":
        anomaly_visual = """
  <!-- Waterlogging Flood Pool -->
  <defs>
    <linearGradient id="waterGrad" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#0284C7" stop-opacity="0.85"/>
      <stop offset="50%" stop-color="#0369A1" stop-opacity="0.95"/>
      <stop offset="100%" stop-color="#0C4A6E" stop-opacity="0.98"/>
    </linearGradient>
  </defs>
  <ellipse cx="240" cy="142" rx="90" ry="42" fill="url(#waterGrad)" filter="url(#shadow)"/>
  <!-- Ripple rings -->
  <ellipse cx="240" cy="142" rx="70" ry="30" fill="none" stroke="#38BDF8" stroke-width="2" opacity="0.75"/>
  <ellipse cx="240" cy="142" rx="45" ry="18" fill="none" stroke="#7DD3FC" stroke-width="1.5" opacity="0.6"/>
  <!-- Wave glints -->
  <path d="M 180 135 Q 200 130 220 135 T 260 135" stroke="#BAE6FD" stroke-width="2" fill="none" opacity="0.8"/>
  <path d="M 210 150 Q 230 145 250 150 T 290 150" stroke="#BAE6FD" stroke-width="1.5" fill="none" opacity="0.7"/>
        """
    elif cls_lower == "sewage":
        anomaly_visual = """
  <!-- Sewage Overflow Effluent -->
  <defs>
    <linearGradient id="sewageGrad" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#0D9488" stop-opacity="0.85"/>
      <stop offset="50%" stop-color="#0F766E" stop-opacity="0.95"/>
      <stop offset="100%" stop-color="#115E59" stop-opacity="0.98"/>
    </linearGradient>
  </defs>
  <path d="M 160 115 C 190 95, 270 100, 315 125 C 335 155, 290 185, 250 180 C 190 190, 140 160, 160 115 Z" fill="url(#sewageGrad)" filter="url(#shadow)"/>
  <path d="M 240 100 Q 220 140 250 170" stroke="#2DD4BF" stroke-width="3" fill="none" opacity="0.8"/>
  <circle cx="210" cy="135" r="8" fill="#14B8A6" opacity="0.6"/>
  <circle cx="265" cy="145" r="10" fill="#14B8A6" opacity="0.7"/>
  <circle cx="235" cy="155" r="6" fill="#5EEAD4" opacity="0.8"/>
        """
    elif cls_lower == "garbage_dump":
        anomaly_visual = """
  <!-- Illegal Garbage Heap Mound -->
  <defs>
    <linearGradient id="dumpGrad" x1="0%" y1="0%" x2="0%" y2="100%">
      <stop offset="0%" stop-color="#E11D48"/>
      <stop offset="60%" stop-color="#9F1239"/>
      <stop offset="100%" stop-color="#4C0519"/>
    </linearGradient>
  </defs>
  <path d="M 155 175 Q 180 105 240 98 Q 300 105 325 175 Z" fill="url(#dumpGrad)" filter="url(#shadow)"/>
  <rect x="175" y="140" width="28" height="24" rx="4" fill="#FB7185" opacity="0.85" transform="rotate(-12 189 152)"/>
  <rect x="235" y="132" width="34" height="26" rx="4" fill="#FDA4AF" opacity="0.9" transform="rotate(15 252 145)"/>
  <rect x="205" y="120" width="30" height="22" rx="3" fill="#F43F5E" opacity="0.85"/>
  <circle cx="280" cy="155" r="12" fill="#BE123C"/>
        """
    elif cls_lower == "manhole":
        anomaly_visual = """
  <!-- Displaced Manhole Cover Hazard -->
  <defs>
    <radialGradient id="manholeGrad" cx="50%" cy="50%" r="50%">
      <stop offset="0%" stop-color="#64748B"/>
      <stop offset="70%" stop-color="#334155"/>
      <stop offset="100%" stop-color="#0F172A"/>
    </radialGradient>
  </defs>
  <ellipse cx="240" cy="142" rx="65" ry="38" fill="#05070A" stroke="#C084FC" stroke-width="3" filter="url(#shadow)"/>
  <ellipse cx="260" cy="132" rx="60" ry="34" fill="url(#manholeGrad)" stroke="#A855F7" stroke-width="2"/>
  <circle cx="260" cy="132" r="14" fill="none" stroke="#E2E8F0" stroke-width="2" opacity="0.6"/>
  <line x1="220" y1="132" x2="300" y2="132" stroke="#94A3B8" stroke-width="2" opacity="0.5"/>
  <line x1="260" y1="108" x2="260" y2="156" stroke="#94A3B8" stroke-width="2" opacity="0.5"/>
        """
    elif cls_lower == "crack":
        anomaly_visual = """
  <!-- Road Surface Crack Network -->
  <path d="M 160 90 L 195 125 L 180 145 L 220 160 L 255 135 L 285 170 L 320 185" stroke="#F97316" stroke-width="5" fill="none" filter="url(#shadow)"/>
  <path d="M 195 125 L 230 115 L 250 95" stroke="#FB923C" stroke-width="3" fill="none"/>
  <path d="M 220 160 L 205 190 L 175 205" stroke="#FB923C" stroke-width="3" fill="none"/>
  <path d="M 255 135 L 290 120 L 315 130" stroke="#FDBA74" stroke-width="2.5" fill="none"/>
  <path d="M 285 170 L 270 200 L 295 215" stroke="#FDBA74" stroke-width="2.5" fill="none"/>
        """
    else:
        # Default: Pothole
        anomaly_visual = """
  <!-- Physical Pothole Cavity Geometry -->
  <path d="M 170 120 C 185 95, 290 100, 310 125 C 325 145, 305 180, 275 185 C 220 195, 155 170, 170 120 Z"
        fill="url(#potholeCavity)" stroke="#2B2118" stroke-width="3" filter="url(#shadow)"/>
  <!-- Asphalt Internal Fracture Cracks -->
  <path d="M 170 120 Q 140 105 125 110" stroke="#10141D" stroke-width="2" fill="none" opacity="0.8"/>
  <path d="M 310 125 Q 345 130 365 120" stroke="#10141D" stroke-width="2" fill="none" opacity="0.8"/>
  <path d="M 275 185 Q 285 215 300 225" stroke="#10141D" stroke-width="2" fill="none" opacity="0.8"/>
  <path d="M 210 175 Q 185 200 170 215" stroke="#10141D" stroke-width="2" fill="none" opacity="0.8"/>
        """

    return f"""<svg xmlns="http://www.w3.org/2000/svg" width="480" height="270" viewBox="0 0 480 270">
  <defs>
    <linearGradient id="asphalt" x1="0%" y1="0%" x2="0%" y2="100%">
      <stop offset="0%" stop-color="#141822"/>
      <stop offset="50%" stop-color="#1E2330"/>
      <stop offset="100%" stop-color="#10131A"/>
    </linearGradient>
    <linearGradient id="potholeCavity" x1="20%" y1="20%" x2="80%" y2="80%">
      <stop offset="0%" stop-color="#080A0E"/>
      <stop offset="60%" stop-color="#050608"/>
      <stop offset="100%" stop-color="#1A1512"/>
    </linearGradient>
    <filter id="shadow" x="-10%" y="-10%" width="120%" height="120%">
      <feDropShadow dx="0" dy="4" stdDeviation="6" flood-color="#000" flood-opacity="0.7"/>
    </filter>
  </defs>

  <!-- Asphalt Roadway Background -->
  <rect width="480" height="270" fill="url(#asphalt)"/>

  <!-- Road Texture Grid & Surface Grain -->
  <line x1="0" y1="90" x2="480" y2="90" stroke="rgba(255,255,255,0.03)" stroke-width="1"/>
  <line x1="0" y1="180" x2="480" y2="180" stroke="rgba(255,255,255,0.03)" stroke-width="1"/>

  <!-- Dashed Highway Lane Marking -->
  <line x1="240" y1="0" x2="240" y2="270" stroke="#EAB308" stroke-width="4" stroke-dasharray="24,20" opacity="0.7"/>
  <line x1="20" y1="0" x2="20" y2="270" stroke="#FFFFFF" stroke-width="3" opacity="0.4"/>
  <line x1="460" y1="0" x2="460" y2="270" stroke="#FFFFFF" stroke-width="3" opacity="0.4"/>

  {anomaly_visual}

  <!-- AI Detection Bounding Box -->
  <rect x="135" y="75" width="210" height="135" rx="6" fill="none" stroke="{box_color}" stroke-width="2.5" stroke-dasharray="6,4"/>

  <!-- Corner Crosshairs -->
  <path d="M 135 85 L 135 75 L 145 75" stroke="{box_color}" stroke-width="3" fill="none"/>
  <path d="M 335 75 L 345 75 L 345 85" stroke="{box_color}" stroke-width="3" fill="none"/>
  <path d="M 135 200 L 135 210 L 145 210" stroke="{box_color}" stroke-width="3" fill="none"/>
  <path d="M 335 210 L 345 210 L 345 200" stroke="{box_color}" stroke-width="3" fill="none"/>

  <!-- AI Classification Tag -->
  <rect x="135" y="50" width="200" height="24" rx="4" fill="{box_color}"/>
  <text x="142" y="66" fill="#FFFFFF" font-family="-apple-system, sans-serif" font-size="11" font-weight="bold" letter-spacing="0.5">
    {class_title} ({int(confidence*100)}% CONF)
  </text>

  <!-- Top Camera Telemetry Overlay -->
  <rect x="0" y="0" width="480" height="32" fill="rgba(11, 15, 23, 0.85)"/>
  <circle cx="16" cy="16" r="4" fill="#EF4444"/>
  <text x="26" y="20" fill="#E2E8F0" font-family="-apple-system, sans-serif" font-size="10" font-weight="600">LIVE SENSOR CAM // PATROL-{id_str[:4].upper()}</text>
  <text x="465" y="20" fill="#94A3B8" font-family="-apple-system, sans-serif" font-size="10" text-anchor="end">FPS: 30.0 | ISO 400</text>

  <!-- Bottom Telemetry HUD -->
  <rect x="0" y="238" width="480" height="32" fill="rgba(11, 15, 23, 0.85)"/>
  <text x="14" y="258" fill="#F8FAFC" font-family="-apple-system, sans-serif" font-size="11" font-weight="600">GPS: {lat:.4f}, {lon:.4f}</text>
  <text x="465" y="258" fill="{box_color}" font-family="-apple-system, sans-serif" font-size="11" font-weight="bold" text-anchor="end">{sev_upper} ({passes} PASS{'ES' if passes > 1 else ''})</text>
</svg>"""
