"""Seed data for development/demo. Disabled in production."""
from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from .deduplication import cluster_detection
from .models import CanonicalPothole, DetectionEvent, DetectionStatus

logger = logging.getLogger("seed")

# Format: (lat, lon, severity, confidence, device_id, time_offset_min, road_name, depth_cm, label)
SAMPLE_RECORDS: list[tuple[float, float, str, float, str, int, str, int, str]] = [
    # ── Islamabad Capital Territory & Rawalpindi ──
    (33.7198, 73.0895, "Critical", 0.98, "patrol-isb-04", 15, "Jinnah Avenue (Blue Area)", 18, "pothole"),
    (33.7200, 73.0897, "High",     0.94, "dashcam-civic-12", 90, "Jinnah Avenue (Blue Area)", 16, "pothole"),
    (33.7201, 73.0896, "Critical", 0.97, "transit-van-02", 340, "Jinnah Avenue (Blue Area)", 19, "pothole"),
    (33.6601, 73.0850, "Critical", 0.96, "fleet-patrol-09", 70, "Expressway (Faizabad)", 17, "pothole"),
    (33.6358, 73.0722, "Critical", 0.97, "rwp-surveyor-03", 25, "Murree Road (Sixth Rd)", 22, "pothole"),
    (33.6360, 73.0724, "High",     0.93, "transit-bus-81", 180, "Murree Road (Sixth Rd)", 20, "pothole"),
    (33.6359, 73.0723, "Critical", 0.95, "dashcam-civic-44", 720, "Murree Road (Sixth Rd)", 21, "pothole"),
    # Road Cracks
    (33.6844, 73.0479, "High",     0.93, "fleet-patrol-09", 45, "Srinagar Highway H-8", 14, "crack"),
    (33.6845, 73.0481, "Medium",   0.89, "dashcam-patrol-01", 600, "Srinagar Highway H-8", 11, "crack"),
    (33.6685, 73.0560, "High",     0.91, "truck-fleet-07", 400, "I-9 Industrial Avenue", 5, "crack"),
    (33.7240, 73.0610, "Medium",   0.88, "patrol-isb-01", 80, "Margalla Road (F-7)", 4, "crack"),
    (33.6420, 73.0580, "Critical", 0.95, "rwp-patrol-02", 120, "I.J.P. Principal Road", 8, "crack"),
    # Manhole Hazards
    (33.6932, 73.0118, "High",     0.92, "patrol-isb-02", 240, "F-10 Markaz Crescent", 0, "manhole"),
    (33.7110, 73.0580, "Critical", 0.97, "patrol-isb-05", 60, "Jinnah Super F-7", 0, "manhole"),
    (33.6290, 73.0640, "High",     0.94, "rwp-surveyor-01", 190, "Liaquat Bagh Intersection", 0, "manhole"),
    (33.6720, 73.0330, "Medium",   0.88, "fleet-patrol-08", 310, "H-9 Sector Inner Ring", 0, "manhole"),
    # Sewage
    (33.7295, 73.0745, "Critical", 0.96, "patrol-isb-04", 110, "School Road (F-6) Drain Overflow", 0, "sewage"),
    (33.6990, 73.0360, "High",     0.91, "dashcam-patrol-03", 420, "G-9/4 Commercial Sewer Burst", 0, "sewage"),
    (33.6810, 73.0510, "Critical", 0.95, "patrol-isb-07", 260, "Zero Point Waste Runoff", 0, "sewage"),
    (33.6210, 73.0680, "High",     0.93, "rwp-patrol-03", 140, "Raja Bazaar Main Sewage Leak", 0, "sewage"),
    # Garbage Dumps
    (33.7050, 73.0400, "High",     0.92, "patrol-isb-02", 500, "G-9 Service Road Open Dump", 0, "garbage_dump"),
    (33.7310, 73.0820, "Medium",   0.89, "patrol-isb-01", 140, "Kashmir Highway Waste Heap", 0, "garbage_dump"),
    (33.6180, 73.0790, "Critical", 0.97, "rwp-patrol-04", 95, "Rawal Road Solid Waste Dump", 0, "garbage_dump"),
    (33.6490, 73.0730, "High",     0.94, "rwp-surveyor-04", 180, "Commercial Market Waste Cluster", 0, "garbage_dump"),
    # Waterlogging
    (33.6750, 73.0690, "High",     0.91, "patrol-isb-06", 35, "I-8 Markaz Ring Road", 0, "waterlogging"),
    (33.6520, 73.0810, "Critical", 0.96, "patrol-isb-03", 50, "Faizabad Underpass Flooding", 0, "waterlogging"),
    (33.6390, 73.0680, "Medium",   0.88, "rwp-surveyor-02", 210, "Committee Chowk Murree Rd", 0, "waterlogging"),
    # ── Lahore ──
    (31.5642, 74.3125, "Critical", 0.98, "patrol-lhe-01", 30, "The Mall (Anarkali)", 20, "pothole"),
    (31.5644, 74.3127, "High",     0.94, "dashcam-lhe-88", 160, "The Mall (Anarkali)", 18, "pothole"),
    (31.6050, 74.3850, "Critical", 0.98, "highway-patrol-05", 60, "Ring Road North", 16, "pothole"),
    (31.4720, 74.4050, "Low",      0.84, "patrol-lhe-04", 450, "DHA Phase 5 Avenue", 5, "pothole"),
    (31.5204, 74.3587, "Medium",   0.89, "patrol-lhe-03", 80, "Main Blvd Gulberg (Liberty)", 3, "crack"),
    (31.5206, 74.3589, "High",     0.93, "dashcam-lhe-19", 290, "Main Blvd Gulberg (Liberty)", 4, "crack"),
    (31.5120, 74.3210, "High",     0.92, "patrol-lhe-02", 140, "Canal Bank (Muslim Town)", 5, "crack"),
    (31.4850, 74.3050, "Critical", 0.95, "transit-bus-33", 110, "Wahdat Road (Muslim Town)", 7, "crack"),
    (31.5420, 74.3310, "High",     0.93, "patrol-lhe-02", 140, "Jail Road near Services Hospital", 0, "manhole"),
    (31.5790, 74.3180, "Critical", 0.97, "patrol-lhe-06", 75, "Circular Road (Bhati Gate)", 0, "manhole"),
    (31.4680, 74.3520, "Medium",   0.88, "patrol-lhe-08", 320, "Peco Road (Kot Lakhpat)", 0, "manhole"),
    (31.5340, 74.3510, "Critical", 0.97, "dashcam-lhe-55", 380, "MM Alam Road Sewage Backflow", 0, "sewage"),
    (31.5150, 74.3450, "High",     0.92, "patrol-lhe-05", 410, "Garden Town Sewer Line Burst", 0, "sewage"),
    (31.5890, 74.3050, "Critical", 0.98, "patrol-lhe-01", 190, "Ravi Road Wastewater Spill", 0, "sewage"),
    (31.4920, 74.3910, "High",     0.93, "patrol-lhe-04", 450, "DHA Phase 3 Open Trash Heap", 0, "garbage_dump"),
    (31.5310, 74.3720, "Medium",   0.88, "patrol-lhe-09", 240, "Cavalry Ground Roadside Waste", 0, "garbage_dump"),
    (31.4550, 74.2980, "Critical", 0.96, "patrol-lhe-07", 150, "Township Sector B-1 Solid Waste Dump", 0, "garbage_dump"),
    (31.5050, 74.3350, "High",     0.94, "transit-bus-22", 210, "Ferozepur Rd (Kalma Chowk)", 0, "waterlogging"),
    (31.5580, 74.3420, "Critical", 0.98, "patrol-lhe-03", 40, "Lakshmi Chowk Junction", 0, "waterlogging"),
    (31.4780, 74.2810, "High",     0.92, "patrol-lhe-10", 130, "Thokar Niaz Baig Flyover", 0, "waterlogging"),
    # ── Karachi ──
    (24.8607, 67.0611, "Critical", 0.99, "patrol-khi-01", 10, "Shahrah-e-Faisal (Nursery)", 24, "pothole"),
    (24.8609, 67.0613, "Critical", 0.97, "dashcam-khi-09", 120, "Shahrah-e-Faisal (Nursery)", 22, "pothole"),
    (24.8350, 67.1350, "Critical", 0.97, "freight-patrol-08", 170, "Korangi Industrial Causeway", 25, "pothole"),
    (24.8650, 67.0180, "Critical", 0.96, "patrol-khi-01", 50, "M.A. Jinnah Rd (Saddar)", 19, "pothole"),
    (24.8120, 67.0310, "High",     0.92, "dashcam-khi-44", 95, "Sea View Road (Clifton)", 4, "crack"),
    (24.8122, 67.0312, "Medium",   0.88, "patrol-khi-03", 420, "Sea View Road (Clifton)", 3, "crack"),
    (24.9180, 67.0980, "High",     0.93, "patrol-khi-07", 160, "Gulshan Block 6 Rashid Minhas", 5, "crack"),
    (24.9450, 67.0350, "Critical", 0.95, "patrol-khi-05", 85, "Nazimabad 7-Number Road", 6, "crack"),
    (24.8720, 67.0250, "Critical", 0.98, "patrol-khi-02", 30, "Saddar Bohri Bazaar Loop", 0, "manhole"),
    (24.8210, 67.0580, "High",     0.91, "patrol-khi-04", 190, "Khayaban-e-Ittehad (DHA 6)", 0, "manhole"),
    (24.8890, 67.1120, "Critical", 0.96, "patrol-khi-08", 90, "Drigh Road Station Crossing", 0, "manhole"),
    (24.9210, 67.0850, "Critical", 0.98, "patrol-khi-04", 330, "University Rd Gulshan Sewer Flood", 0, "sewage"),
    (24.8450, 67.0050, "High",     0.93, "patrol-khi-09", 490, "I.I. Chundrigar Road Drainage Overflow", 0, "sewage"),
    (24.8010, 67.0420, "Critical", 0.96, "patrol-khi-03", 270, "Khayaban-e-Shamsheer Gutter Burst", 0, "sewage"),
    (24.8390, 67.0480, "Critical", 0.97, "patrol-khi-06", 220, "Gizri Boulevard Huge Trash Dump", 0, "garbage_dump"),
    (24.9310, 67.0620, "High",     0.94, "patrol-khi-07", 110, "Federal B Area Block 14 Open Garbage Dump", 0, "garbage_dump"),
    (24.8700, 67.0890, "Critical", 0.99, "patrol-khi-08", 70, "Tariq Road Commercial Dump", 0, "garbage_dump"),
    (24.9050, 67.1150, "High",     0.94, "transit-bus-45", 260, "Rashid Minhas Road", 0, "waterlogging"),
    (24.8510, 67.0120, "Critical", 0.98, "patrol-khi-01", 45, "Submarine Chowk Underpass", 0, "waterlogging"),
    (24.8810, 67.1720, "High",     0.92, "freight-patrol-02", 150, "Malir River Causeway", 0, "waterlogging"),
    # ── Additional Urban Hubs ──
    (34.0080, 71.5350, "High",     0.93, "patrol-pew-01", 150, "University Road (Peshawar)", 15, "pothole"),
    (34.0150, 71.5800, "Medium",   0.88, "patrol-pew-02", 280, "GT Road (Peshawar)", 3, "crack"),
    (34.0010, 71.5120, "Critical", 0.96, "patrol-pew-03", 70, "Hayatabad Phase 3 Commercial", 0, "manhole"),
    (34.0120, 71.5600, "Critical", 0.97, "patrol-pew-04", 90, "Khyber Bazaar Sewage Spill", 0, "sewage"),
    (34.0220, 71.5900, "High",     0.93, "patrol-pew-05", 140, "Ring Road Peshawar Illegal Dump", 0, "garbage_dump"),
    (30.2150, 71.4850, "Medium",   0.88, "patrol-mul-02", 220, "Bosan Road (Multan)", 0, "waterlogging"),
    (30.1980, 71.4420, "High",     0.92, "patrol-mul-01", 130, "Abdali Road (Multan)", 14, "pothole"),
    (30.1890, 71.4650, "Critical", 0.95, "patrol-mul-03", 85, "Chungi No 9 Sewage Flood", 0, "sewage"),
    (31.4150, 73.0950, "Critical", 0.96, "patrol-fsd-01", 310, "D-Ground Garbage Heap", 0, "garbage_dump"),
    (31.4280, 73.0780, "High",     0.93, "patrol-fsd-02", 100, "Jaranwala Road (Faisalabad)", 16, "pothole"),
    (30.1850, 67.0150, "High",     0.90, "patrol-qta-01", 190, "Zarghoon Road (Quetta)", 16, "pothole"),
    (30.1700, 66.9900, "Critical", 0.94, "patrol-qta-02", 100, "Sariab Road (Quetta)", 0, "manhole"),
    (30.1620, 67.0050, "Critical", 0.97, "patrol-qta-03", 80, "Jinnah Road Quetta Sewage Leak", 0, "sewage"),
]


async def seed_database(session: AsyncSession, *, wipe: bool = False) -> int:
    """Populate the database with realistic sample records. Returns count seeded."""
    if wipe:
        await session.execute(delete(CanonicalPothole))
        await session.execute(delete(DetectionEvent))
        await session.commit()
        logger.info("Cleared previous detection tables before seeding.")

    now = datetime.now(UTC)

    for lat, lon, sev, conf, dev_id, offset_min, road, depth, label in SAMPLE_RECORDS:
        cap_time = now - timedelta(minutes=offset_min)
        event = DetectionEvent(
            event_id=uuid.uuid4(),
            device_id=dev_id,
            captured_at=cap_time,
            received_at=cap_time + timedelta(seconds=2),
            processed_at=cap_time + timedelta(seconds=5),
            status=DetectionStatus.PROCESSED,
            media_kind="image",
            media_uri=f"minio://media/{dev_id}_{int(cap_time.timestamp())}.jpg",
            latitude=lat,
            longitude=lon,
            object_count=1,
            objects=[{
                "label": label,
                "confidence": conf,
                "bbox": [0.15, 0.2, 0.85, 0.75],
                "depth_cm": depth,
                "road_segment": road,
            }],
            metrics={
                "severity": sev,
                "depth_cm": depth,
                "road_name": road,
                "confidence": conf,
                "label": label,
            },
        )
        session.add(event)
        await cluster_detection(session, event)

    await session.commit()
    return len(SAMPLE_RECORDS)
