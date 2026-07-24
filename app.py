from __future__ import annotations

import csv
import io
import os
import re
import sqlite3
import uuid
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timedelta
from html import escape
from pathlib import Path

import pandas as pd
import streamlit as st

try:
    import openpyxl  # noqa: F401
    EXCEL_SUPPORT = True
except ModuleNotFoundError:
    EXCEL_SUPPORT = False

try:
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER
    from reportlab.lib.pagesizes import landscape, letter
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import inch
    from reportlab.platypus import PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
    PDF_SUPPORT = True
except ModuleNotFoundError:
    PDF_SUPPORT = False


# =========================================================
# MAINTAINLY - SINGLE-FILE STREAMLIT EDITION
# Save this file as app.py and run: streamlit run app.py
# =========================================================

st.set_page_config(
    page_title="Maintainly - Maintenance scheduling made clear",
    page_icon="🛠️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    :root { --ink:#15223b; --muted:#64748b; --green:#2563eb; --paper:#f4f7fb; --line:#d7e1ee; }
    .stApp { background:var(--paper); color:var(--ink); }
    .main .block-container { max-width:1500px; padding:1.3rem 2rem 3rem; }
    section[data-testid="stSidebar"] { background:#0f2a55; }
    section[data-testid="stSidebar"] * { color:#eef7f3; }
    section[data-testid="stSidebar"] div[role="radiogroup"] label { border-radius:10px; padding:.58rem .7rem; }
    section[data-testid="stSidebar"] div[role="radiogroup"] label:has(input:checked) { background:rgba(255,255,255,.14); }
    .brand { padding:.35rem .2rem 1.35rem; }
    .brand b { font-size:1.3rem; }
    .brand span { display:inline-grid; place-items:center; width:38px; height:38px; margin-right:.6rem;
        border-radius:11px; background:#dbeafe; color:#1e3a8a!important; }
    .brand small { display:block; margin:.35rem 0 0 3rem; color:#bfdbfe!important; }
    .eyebrow { color:var(--green); font-size:.72rem; font-weight:800; letter-spacing:.12em; text-transform:uppercase; }
    .copy { color:var(--muted); margin-top:-.4rem; margin-bottom:1.2rem; }
    h1,h2,h3 { color:var(--ink); letter-spacing:-.02em; }
    div[data-testid="stMetric"], div[data-testid="stForm"], div[data-testid="stExpander"] {
        background:#fff; border:1px solid var(--line); border-radius:14px; padding:.45rem .7rem;
    }
    div[data-testid="stDataFrame"] { border:1px solid var(--line); border-radius:14px; overflow:hidden; }
    .stButton button, .stDownloadButton button { border-radius:10px; font-weight:700; }
    .stButton button[kind="primary"] { background:var(--green); border-color:var(--green); }
    .board-card { background:#fff; border:1px solid var(--line); border-left:4px solid var(--green);
        border-radius:11px; padding:.7rem; margin:.45rem 0; min-height:100px; }
    .board-card small { color:var(--green); font-weight:800; }
    .board-card b { display:block; margin:.3rem 0; }
    .board-card span { color:var(--muted); font-size:.82rem; }
    </style>
    """,
    unsafe_allow_html=True,
)


DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
PRIORITIES = ["Emergency", "Critical", "Urgent", "High", "Medium", "Low", "Opportunity / Shutdown"]
JOB_STATUSES = ["Pending", "Scheduled", "Draft Scheduled", "Final Scheduled", "In progress", "Active", "On Hold", "Completed", "Overdue"]
SKILLS = ["Mechanical", "Welding", "Electrical", "HVAC", "Instrumentation", "Multi-skill", "General"]

DATA_DIR = Path(os.getenv("MAINTENANCE_DATA_DIR", "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "maintainly.db"


def now() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def uid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8].upper()}"


def safe_index(options: list[str], value: str) -> int:
    return options.index(value) if value in options else 0


def flash(message: str) -> None:
    st.session_state["flash"] = message
    st.rerun()


@contextmanager
def connection():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def rows(query: str, parameters: tuple = ()) -> list[dict]:
    with connection() as conn:
        return [dict(row) for row in conn.execute(query, parameters).fetchall()]


def initialize_database() -> None:
    with connection() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS team_members (
                id TEXT PRIMARY KEY, name TEXT NOT NULL, role TEXT NOT NULL, email TEXT UNIQUE,
                skill TEXT NOT NULL, weekly_hours REAL NOT NULL DEFAULT 40,
                availability TEXT NOT NULL DEFAULT 'Available', active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL, updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS team_crews (
                id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, members TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS assets (
                id TEXT PRIMARY KEY, asset_number TEXT NOT NULL UNIQUE, asset_name TEXT NOT NULL,
                location TEXT NOT NULL, department TEXT NOT NULL, criticality TEXT NOT NULL,
                manufacturer TEXT NOT NULL DEFAULT '', model TEXT NOT NULL DEFAULT '', active INTEGER NOT NULL DEFAULT 1,
                notes TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS work_orders (
                id TEXT PRIMARY KEY, title TEXT NOT NULL, asset TEXT NOT NULL, location TEXT NOT NULL,
                department TEXT NOT NULL, due_at TEXT NOT NULL, duration_hours REAL NOT NULL DEFAULT 1,
                priority TEXT NOT NULL DEFAULT 'Medium', priority_score INTEGER NOT NULL DEFAULT 7,
                status TEXT NOT NULL DEFAULT 'Pending', category TEXT NOT NULL DEFAULT 'Mechanical',
                crew_size INTEGER NOT NULL DEFAULT 1, mechanical_needed INTEGER NOT NULL DEFAULT 0,
                welding_needed INTEGER NOT NULL DEFAULT 0, allowed_days TEXT NOT NULL,
                preferred_day TEXT NOT NULL DEFAULT '', scope_ready INTEGER NOT NULL DEFAULT 1,
                parts_ready INTEGER NOT NULL DEFAULT 1, permits_ready INTEGER NOT NULL DEFAULT 1,
                shutdown_ready INTEGER NOT NULL DEFAULT 1, released INTEGER NOT NULL DEFAULT 1,
                notes TEXT NOT NULL DEFAULT '', completed_at TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS assignments (
                id TEXT PRIMARY KEY, work_order_id TEXT NOT NULL REFERENCES work_orders(id) ON DELETE CASCADE,
                state TEXT NOT NULL DEFAULT 'Draft', day TEXT NOT NULL, crew_label TEXT NOT NULL,
                technicians TEXT NOT NULL, hours REAL NOT NULL, status TEXT NOT NULL DEFAULT 'Scheduled',
                notes TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS schedule_history (
                id TEXT PRIMARY KEY, assignment_id TEXT NOT NULL, action TEXT NOT NULL,
                detail TEXT NOT NULL DEFAULT '', changed_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS app_migrations (
                name TEXT PRIMARY KEY, applied_at TEXT NOT NULL
            );
            """
        )
        email_column = next(
            (column for column in conn.execute("PRAGMA table_info(team_members)").fetchall() if column["name"] == "email"),
            None,
        )
        if email_column and email_column["notnull"]:
            conn.executescript(
                """
                ALTER TABLE team_members RENAME TO team_members_required_email;
                CREATE TABLE team_members (
                    id TEXT PRIMARY KEY, name TEXT NOT NULL, role TEXT NOT NULL, email TEXT UNIQUE,
                    skill TEXT NOT NULL, weekly_hours REAL NOT NULL DEFAULT 40,
                    availability TEXT NOT NULL DEFAULT 'Available', active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                INSERT INTO team_members (
                    id, name, role, email, skill, weekly_hours, availability, active, created_at, updated_at
                )
                SELECT
                    id, name, role, NULLIF(TRIM(email), ''), skill, weekly_hours,
                    availability, active, created_at, updated_at
                FROM team_members_required_email;
                DROP TABLE team_members_required_email;
                """
            )
        sample_cleanup = "remove_builtin_sample_work_orders_v1"
        if not conn.execute("SELECT 1 FROM app_migrations WHERE name=?", (sample_cleanup,)).fetchone():
            sample_filter = """
                (w.id='WO-2842' AND w.title='Generator load bank test') OR
                (w.id='WO-2848' AND w.title='Chiller water sample') OR
                (w.id='WO-2850' AND w.title='Conveyor belt alignment') OR
                (w.id='WO-2854' AND w.title='Dock frame weld repair')
            """
            conn.execute(
                f"""DELETE FROM schedule_history WHERE assignment_id IN (
                    SELECT a.id FROM assignments a
                    JOIN work_orders w ON w.id=a.work_order_id
                    WHERE {sample_filter}
                )"""
            )
            conn.execute(
                f"""DELETE FROM assignments WHERE work_order_id IN (
                    SELECT w.id FROM work_orders w WHERE {sample_filter}
                )"""
            )
            conn.execute(f"DELETE FROM work_orders AS w WHERE {sample_filter}")
            conn.execute("INSERT INTO app_migrations VALUES (?,?)", (sample_cleanup, now()))
        stamp = now()
        if conn.execute("SELECT COUNT(*) FROM team_members").fetchone()[0] == 0:
            team = [
                ("TM-MAYA", "Maya Chen", "Senior technician", "maya@maintainly.local", "Mechanical", 40, "Available", 1),
                ("TM-JORDAN", "Jordan Lee", "Electrical technician", "jordan@maintainly.local", "Electrical", 40, "Available", 1),
                ("TM-SAM", "Sam Rivera", "Maintenance technician", "sam@maintainly.local", "Multi-skill", 40, "Available", 1),
                ("TM-AMARA", "Amara Brown", "Welder", "amara@maintainly.local", "Welding", 40, "Available", 1),
            ]
            conn.executemany("INSERT INTO team_members VALUES (?,?,?,?,?,?,?,?,?,?)", [(*item, stamp, stamp) for item in team])
        if conn.execute("SELECT COUNT(*) FROM assets").fetchone()[0] == 0:
            assets = [
                ("AS-GEN02", "GEN-02", "Backup Generator 02", "Utility yard", "Utilities", "Critical", "Caterpillar", "C18", 1, "Monthly readiness testing"),
                ("AS-CH01", "CH-01", "Process Chiller 01", "Central plant", "Utilities", "Critical", "York", "YVAA", 1, "Water treatment readings required"),
                ("AS-DOCK07", "DOCK-07", "Dock Door 07", "Warehouse - Bay 7", "Warehouse", "High", "Rite-Hite", "RHH-5000", 1, "Safety interlocks installed"),
            ]
            conn.executemany("INSERT INTO assets VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", [(*item, stamp, stamp) for item in assets])


initialize_database()


def save_team(data: dict, member_id: str | None = None) -> None:
    member_id = member_id or uid("TM")
    stamp = now()
    email = str(data.get("email") or "").strip().lower() or None
    with connection() as conn:
        if conn.execute("SELECT 1 FROM team_members WHERE id=?", (member_id,)).fetchone():
            conn.execute("UPDATE team_members SET name=?,role=?,email=?,skill=?,weekly_hours=?,availability=?,active=?,updated_at=? WHERE id=?",
                         (data["name"], data["role"], email, data["skill"], data["hours"], data["availability"], int(data["active"]), stamp, member_id))
        else:
            conn.execute("INSERT INTO team_members VALUES (?,?,?,?,?,?,?,?,?,?)",
                         (member_id, data["name"], data["role"], email, data["skill"], data["hours"], data["availability"], int(data["active"]), stamp, stamp))


def save_crew(data: dict, crew_id: str | None = None) -> None:
    crew_id = crew_id or uid("CR")
    name = str(data.get("name") or "").strip()
    members = [str(member_id).strip() for member_id in data.get("members", []) if str(member_id).strip()]
    if not name:
        raise ValueError("Crew name is required.")
    if not members:
        raise ValueError("Select at least one crew member.")
    stamp = now()
    values = (name, ",".join(dict.fromkeys(members)), int(data.get("active", True)), stamp)
    with connection() as conn:
        conflicts: list[str] = []
        for crew in conn.execute("SELECT id,name,members FROM team_crews WHERE id!=?", (crew_id,)).fetchall():
            if set(members) & {value for value in crew["members"].split(",") if value}:
                conflicts.append(crew["name"])
        if conflicts:
            raise ValueError(
                "A person can only be in one crew. Remove the selected person from: "
                + ", ".join(conflicts)
            )
        if conn.execute("SELECT 1 FROM team_crews WHERE id=?", (crew_id,)).fetchone():
            conn.execute(
                "UPDATE team_crews SET name=?,members=?,active=?,updated_at=? WHERE id=?",
                (*values, crew_id),
            )
        else:
            conn.execute(
                "INSERT INTO team_crews VALUES (?,?,?,?,?,?)",
                (crew_id, *values[:-1], stamp, stamp),
            )


def delete_crew(crew_id: str) -> None:
    with connection() as conn:
        conn.execute("DELETE FROM team_crews WHERE id=?", (crew_id,))


def save_asset(data: dict, asset_id: str | None = None) -> None:
    asset_id = asset_id or uid("AS")
    stamp = now()
    values = (data["number"].upper(), data["name"], data["location"], data["department"], data["criticality"], data["manufacturer"], data["model"], int(data["active"]), data["notes"])
    with connection() as conn:
        if conn.execute("SELECT 1 FROM assets WHERE id=?", (asset_id,)).fetchone():
            conn.execute("UPDATE assets SET asset_number=?,asset_name=?,location=?,department=?,criticality=?,manufacturer=?,model=?,active=?,notes=?,updated_at=? WHERE id=?", (*values, stamp, asset_id))
        else:
            conn.execute("INSERT INTO assets VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", (asset_id, *values, stamp, stamp))


def save_job(data: dict, job_id: str | None = None) -> None:
    job_id = cell_text(job_id or data.get("work_order_id"))
    if not job_id:
        raise ValueError("Work order number is required.")
    stamp = now()
    values = (data["title"], data["asset"], data["location"], data["department"], data["due_at"], data["duration"], data["priority"], data["score"], data["status"], data["category"], data["crew"], data["mechanical"], data["welding"], ",".join(data["allowed_days"]), data["preferred_day"], int(data["scope"]), int(data["parts"]), int(data["permits"]), int(data["shutdown"]), int(data["released"]), data["notes"])
    with connection() as conn:
        if conn.execute("SELECT 1 FROM work_orders WHERE id=?", (job_id,)).fetchone():
            conn.execute("""UPDATE work_orders SET title=?,asset=?,location=?,department=?,due_at=?,duration_hours=?,priority=?,priority_score=?,status=?,category=?,crew_size=?,mechanical_needed=?,welding_needed=?,allowed_days=?,preferred_day=?,scope_ready=?,parts_ready=?,permits_ready=?,shutdown_ready=?,released=?,notes=?,updated_at=? WHERE id=?""", (*values, stamp, job_id))
        else:
            conn.execute("""INSERT INTO work_orders (id,title,asset,location,department,due_at,duration_hours,priority,priority_score,status,category,crew_size,mechanical_needed,welding_needed,allowed_days,preferred_day,scope_ready,parts_ready,permits_ready,shutdown_ready,released,notes,completed_at,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,?,?)""", (job_id, *values, stamp, stamp))


def cell_text(value, default: str = "") -> str:
    if value is None or (not isinstance(value, (list, tuple, dict)) and pd.isna(value)):
        return default
    return str(value).strip()


def cell_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def cell_int(value, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def cell_bool(value, default: bool = True) -> bool:
    if value is None or (not isinstance(value, (list, tuple, dict)) and pd.isna(value)):
        return default
    if isinstance(value, str):
        return value.strip().lower() not in ("", "0", "false", "no", "off", "n")
    return bool(value)


def cell_datetime(value) -> str:
    if value is None or (not isinstance(value, (list, tuple, dict)) and pd.isna(value)):
        return (datetime.now() + timedelta(days=2)).replace(hour=8, minute=0, second=0, microsecond=0).isoformat()
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.to_pydatetime().replace(microsecond=0).isoformat() if isinstance(value, pd.Timestamp) else value.replace(microsecond=0).isoformat()
    text = str(value).strip()
    try:
        return pd.to_datetime(text).to_pydatetime().replace(microsecond=0).isoformat()
    except Exception:
        return text


def build_schedule_pdf(assignments: list[dict], crew_name: str | None = None, report_title: str = "Final Maintenance Schedule") -> bytes:
    if not PDF_SUPPORT:
        raise RuntimeError("PDF support is not installed. Add reportlab>=4,<5 to requirements.txt and reboot the app.")
    normalized = []
    for row in assignments:
        normalized.append({
            "crew": cell_text(row.get("crew_label"), "Unassigned Crew"),
            "people": cell_text(row.get("technicians")),
            "day": cell_text(row.get("day")),
            "work_order": cell_text(row.get("work_order_id")),
            "job": cell_text(row.get("title")),
            "location": cell_text(row.get("location")),
            "hours": cell_float(row.get("hours"), 0),
            "status": cell_text(row.get("status"), "Scheduled"),
        })
    if crew_name:
        normalized = [row for row in normalized if row["crew"] == crew_name]
    day_order = {day: index for index, day in enumerate(DAYS)}
    normalized.sort(key=lambda row: (row["crew"].lower(), day_order.get(row["day"], 99), row["work_order"]))
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in normalized:
        grouped[row["crew"]].append(row)

    output = io.BytesIO()
    document = SimpleDocTemplate(
        output, pagesize=landscape(letter), rightMargin=.45 * inch, leftMargin=.45 * inch,
        topMargin=.45 * inch, bottomMargin=.45 * inch, title=report_title, author="Maintainly",
    )
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "ScheduleTitle", parent=styles["Title"], fontName="Helvetica-Bold", fontSize=20,
        leading=24, textColor=colors.HexColor("#123a70"), alignment=TA_CENTER, spaceAfter=8,
    )
    crew_style = ParagraphStyle(
        "CrewHeading", parent=styles["Heading2"], fontName="Helvetica-Bold", fontSize=14,
        leading=17, textColor=colors.HexColor("#1d4ed8"), spaceBefore=6, spaceAfter=4,
    )
    body_style = ParagraphStyle("ScheduleBody", parent=styles["BodyText"], fontName="Helvetica", fontSize=8.5, leading=11)
    small_style = ParagraphStyle("ScheduleSmall", parent=body_style, textColor=colors.HexColor("#475569"), spaceAfter=8)
    story = [
        Paragraph(escape(report_title), title_style),
        Paragraph(
            f"Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} | "
            f"{len(normalized)} scheduled job{'s' if len(normalized) != 1 else ''}",
            small_style,
        ),
    ]
    if not grouped:
        story.append(Paragraph("No final schedule assignments are available.", body_style))
    for index, (crew, crew_rows) in enumerate(grouped.items()):
        if index:
            story.append(PageBreak())
        people = []
        for row in crew_rows:
            for person in [value.strip() for value in row["people"].split(",") if value.strip()]:
                if person not in people:
                    people.append(person)
        story.extend([
            Paragraph(escape(crew), crew_style),
            Paragraph(
                f"<b>Crew size:</b> {len(people)} &nbsp;&nbsp; "
                f"<b>People:</b> {escape(', '.join(people) or 'No members listed')}",
                body_style,
            ),
            Spacer(1, 8),
        ])
        table_data = [[
            Paragraph("<b>Day</b>", body_style), Paragraph("<b>Work order</b>", body_style),
            Paragraph("<b>Job</b>", body_style), Paragraph("<b>Location</b>", body_style),
            Paragraph("<b>Hours</b>", body_style), Paragraph("<b>Status</b>", body_style),
        ]]
        for row in crew_rows:
            table_data.append([
                Paragraph(escape(row["day"]), body_style), Paragraph(escape(row["work_order"]), body_style),
                Paragraph(escape(row["job"]), body_style), Paragraph(escape(row["location"]), body_style),
                Paragraph(f"{row['hours']:.1f}", body_style), Paragraph(escape(row["status"]), body_style),
            ])
        table = Table(
            table_data,
            colWidths=[.8 * inch, 1.2 * inch, 3.1 * inch, 2.25 * inch, .65 * inch, 1.05 * inch],
            repeatRows=1,
        )
        table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#dbeafe")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#123a70")),
            ("GRID", (0, 0), (-1, -1), .4, colors.HexColor("#bfcee3")),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 6), ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 6), ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
        ]))
        story.append(table)

    def footer(canvas, doc):
        canvas.saveState()
        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(colors.HexColor("#64748b"))
        canvas.drawString(document.leftMargin, .23 * inch, "Maintainly - Crew schedule")
        canvas.drawRightString(landscape(letter)[0] - document.rightMargin, .23 * inch, f"Page {doc.page}")
        canvas.restoreState()

    document.build(story, onFirstPage=footer, onLaterPages=footer)
    return output.getvalue()


def job_data_from_row(row: dict) -> dict:
    allowed = cell_text(row.get("allowed_days"), ",".join(DAYS[:5]))
    allowed_days = [day.strip().title() for day in allowed.replace(";", ",").split(",") if day.strip().title() in DAYS]
    return {
        "title": cell_text(row.get("title")),
        "asset": cell_text(row.get("asset"), "UNASSIGNED").upper(),
        "location": cell_text(row.get("location"), "Plant"),
        "department": cell_text(row.get("department"), "Operations"),
        "due_at": cell_datetime(row.get("due_at")),
        "duration": max(.5, cell_float(row.get("duration_hours"), 1)),
        "priority": cell_text(row.get("priority"), "Medium"),
        "score": max(1, min(20, cell_int(row.get("priority_score"), 7))),
        "status": cell_text(row.get("status"), "Pending"),
        "category": cell_text(row.get("category"), "Mechanical"),
        "crew": max(1, cell_int(row.get("crew_size"), 1)),
        "mechanical": max(0, cell_int(row.get("mechanical_needed"), 0)),
        "welding": max(0, cell_int(row.get("welding_needed"), 0)),
        "allowed_days": allowed_days or DAYS[:5],
        "preferred_day": cell_text(row.get("preferred_day")),
        "scope": cell_bool(row.get("scope_ready"), True),
        "parts": cell_bool(row.get("parts_ready"), True),
        "permits": cell_bool(row.get("permits_ready"), True),
        "shutdown": cell_bool(row.get("shutdown_ready"), True),
        "released": cell_bool(row.get("released"), True),
        "notes": cell_text(row.get("notes")),
    }


def team_table_editor(team: list[dict]) -> None:
    columns = ["id", "name", "role", "email", "skill", "weekly_hours", "availability", "active"]
    frame = pd.DataFrame(team, columns=columns)
    edited = st.data_editor(
        frame,
        key="team_spreadsheet",
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        disabled=["id"],
        column_config={
            "id": st.column_config.TextColumn("ID"),
            "name": st.column_config.TextColumn("Name", required=True),
            "role": st.column_config.TextColumn("Role", required=True),
            "email": st.column_config.TextColumn("Email (optional)"),
            "skill": st.column_config.SelectboxColumn("Skill", options=SKILLS, required=True),
            "weekly_hours": st.column_config.NumberColumn("Weekly hours", min_value=1, max_value=84, step=1),
            "availability": st.column_config.SelectboxColumn("Availability", options=["Available", "Limited", "Unavailable"]),
            "active": st.column_config.CheckboxColumn("Active"),
        },
    )
    if st.button("Save team table", type="primary", use_container_width=True):
        records = edited.to_dict("records")
        names = [cell_text(record.get("name")) for record in records]
        emails = [cell_text(record.get("email")).lower() for record in records if cell_text(record.get("email"))]
        if any(not name for name in names):
            st.error("Every team member needs a name.")
        elif len(emails) != len(set(emails)):
            st.error("Email addresses must be unique when provided.")
        else:
            try:
                for record in records:
                    save_team({
                        "name": cell_text(record["name"]), "role": cell_text(record["role"], "Maintenance technician"),
                        "email": cell_text(record["email"]), "skill": cell_text(record["skill"], "Mechanical"),
                        "hours": max(1, cell_float(record["weekly_hours"], 40)),
                        "availability": cell_text(record["availability"], "Available"),
                        "active": cell_bool(record["active"], True),
                    }, cell_text(record["id"]))
                flash("Team table saved.")
            except sqlite3.IntegrityError as exc:
                st.error(f"The team table could not be saved: {exc}")


def asset_table_editor(assets: list[dict]) -> None:
    columns = ["id", "asset_number", "asset_name", "location", "department", "criticality", "manufacturer", "model", "active", "notes"]
    frame = pd.DataFrame(assets, columns=columns)
    edited = st.data_editor(
        frame,
        key="asset_spreadsheet",
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        disabled=["id"],
        column_config={
            "id": st.column_config.TextColumn("ID"),
            "asset_number": st.column_config.TextColumn("Asset number", required=True),
            "asset_name": st.column_config.TextColumn("Asset name", required=True),
            "location": st.column_config.TextColumn("Location"),
            "department": st.column_config.TextColumn("Department"),
            "criticality": st.column_config.SelectboxColumn("Criticality", options=["Critical", "High", "Normal", "Low"]),
            "active": st.column_config.CheckboxColumn("Active"),
        },
    )
    if st.button("Save asset table", type="primary", use_container_width=True):
        records = edited.to_dict("records")
        numbers = [cell_text(record.get("asset_number")).upper() for record in records]
        if any(not number for number in numbers) or any(not cell_text(record.get("asset_name")) for record in records):
            st.error("Every asset needs an asset number and name.")
        elif len(numbers) != len(set(numbers)):
            st.error("Asset numbers must be unique.")
        else:
            try:
                for record in records:
                    save_asset({
                        "number": cell_text(record["asset_number"]), "name": cell_text(record["asset_name"]),
                        "location": cell_text(record["location"]), "department": cell_text(record["department"], "Operations"),
                        "criticality": cell_text(record["criticality"], "Normal"), "manufacturer": cell_text(record["manufacturer"]),
                        "model": cell_text(record["model"]), "active": cell_bool(record["active"], True), "notes": cell_text(record["notes"]),
                    }, cell_text(record["id"]))
                flash("Asset table saved.")
            except sqlite3.IntegrityError as exc:
                st.error(f"The asset table could not be saved: {exc}")


def job_column_config() -> dict:
    return {
        "_original_id": None,
        "id": st.column_config.TextColumn("Work order number", required=True),
        "title": st.column_config.TextColumn("Job", required=True),
        "asset": st.column_config.TextColumn("Asset"),
        "location": st.column_config.TextColumn("Location"),
        "department": st.column_config.TextColumn("Department"),
        "due_at": st.column_config.TextColumn("Due date/time"),
        "duration_hours": st.column_config.NumberColumn("Duration hours", min_value=.5, step=.5),
        "priority": st.column_config.SelectboxColumn("Priority", options=PRIORITIES),
        "priority_score": st.column_config.NumberColumn("Priority score", min_value=1, max_value=20, step=1),
        "status": st.column_config.SelectboxColumn("Status", options=JOB_STATUSES),
        "category": st.column_config.TextColumn("Category"),
        "crew_size": st.column_config.NumberColumn("Crew size", min_value=1, step=1),
        "mechanical_needed": st.column_config.NumberColumn("Mechanical", min_value=0, step=1),
        "welding_needed": st.column_config.NumberColumn("Welding", min_value=0, step=1),
        "allowed_days": st.column_config.TextColumn("Allowed days"),
        "preferred_day": st.column_config.SelectboxColumn("Preferred day", options=["", *DAYS]),
        "scope_ready": st.column_config.CheckboxColumn("Scope ready"),
        "parts_ready": st.column_config.CheckboxColumn("Parts ready"),
        "permits_ready": st.column_config.CheckboxColumn("Permits ready"),
        "shutdown_ready": st.column_config.CheckboxColumn("Shutdown ready"),
        "released": st.column_config.CheckboxColumn("Release"),
        "notes": st.column_config.TextColumn("Notes"),
    }


JOB_TABLE_COLUMNS = [
    "id", "title", "asset", "location", "department", "due_at", "duration_hours", "priority",
    "priority_score", "status", "category", "crew_size", "mechanical_needed", "welding_needed",
    "allowed_days", "preferred_day", "scope_ready", "parts_ready", "permits_ready", "shutdown_ready",
    "released", "notes",
]


def job_table_editor(jobs: list[dict], key: str = "jobs_spreadsheet") -> None:
    frame = pd.DataFrame(jobs, columns=JOB_TABLE_COLUMNS)
    frame.insert(0, "_original_id", frame["id"])
    edited = st.data_editor(
        frame,
        key=key,
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic",
        disabled=["_original_id"],
        column_config=job_column_config(),
        height=520,
    )
    if st.button("Save work-order table", type="primary", use_container_width=True, key=f"{key}_save"):
        records = [
            record for record in edited.to_dict("records")
            if cell_text(record.get("id")) or cell_text(record.get("title"))
        ]
        work_order_numbers = [cell_text(record.get("id")) for record in records]
        original_ids = {cell_text(job.get("id")) for job in jobs}
        retained_original_ids = {cell_text(record.get("_original_id")) for record in records if cell_text(record.get("_original_id"))}
        changed_ids = [
            record for record in records
            if cell_text(record.get("_original_id"))
            and cell_text(record.get("_original_id")) != cell_text(record.get("id"))
        ]
        new_ids = {
            cell_text(record.get("id")) for record in records
            if not cell_text(record.get("_original_id"))
        }
        database_ids = {record["id"] for record in rows("SELECT id FROM work_orders")}
        if any(not number for number in work_order_numbers):
            st.error("Every row needs your work order number.")
        elif len(work_order_numbers) != len(set(work_order_numbers)):
            st.error("Work order numbers must be unique.")
        elif changed_ids:
            st.error("An existing work order number cannot be changed in the table. Delete that row and add a new row with the correct number.")
        elif new_ids & database_ids:
            st.error(f"These work order numbers already exist: {', '.join(sorted(new_ids & database_ids))}")
        elif any(not cell_text(record.get("title")) for record in records):
            st.error("Every work order needs a job title.")
        else:
            for record in records:
                save_job(job_data_from_row(record), cell_text(record["id"]))
            removed_ids = original_ids - retained_original_ids
            if removed_ids:
                with connection() as conn:
                    conn.executemany("DELETE FROM work_orders WHERE id=?", [(job_id,) for job_id in removed_ids])
            flash("Work-order table saved. Added, edited and removed jobs are ready for planning.")

def assignment_table_editor(state: str, assignments: list[dict]) -> None:
    crew_rows = rows("SELECT * FROM team_crews WHERE active=1 ORDER BY name")
    team_lookup = {member["id"]: member["name"] for member in rows("SELECT id,name FROM team_members")}
    crew_members = {
        crew["name"]: [
            team_lookup[member_id] for member_id in crew["members"].split(",")
            if member_id in team_lookup
        ]
        for crew in crew_rows
    }
    crew_options = list(crew_members)
    for assignment in assignments:
        if assignment["crew_label"] not in crew_options:
            crew_options.append(assignment["crew_label"])

    if state == "Draft":
        if not assignments:
            st.markdown('<div class="empty">No assignments in this schedule yet.</div>', unsafe_allow_html=True)
            return
        active_crew_names = list(crew_members)
        if not active_crew_names:
            st.error("Create an active saved crew in Team → Crews before editing the Draft table.")
            return

        widths = [1.05, 1.45, 2.0, 1.2, 2.1, .8, 1.15, 1.7]
        headers = st.columns(widths)
        for column, heading in zip(
            headers,
            ["Day", "Crew", "People in crew", "Work order", "Job", "Hours", "Status", "Notes"],
        ):
            column.markdown(f"**{heading}**")

        changes = []
        status_options = ["Scheduled", "In Progress", "Deferred", "Complete"]
        for assignment in assignments:
            with st.container(border=True):
                columns = st.columns(widths)
                day = columns[0].selectbox(
                    "Day",
                    DAYS,
                    index=safe_index(DAYS, assignment["day"]),
                    key=f"draft_grid_day_{assignment['id']}",
                    label_visibility="collapsed",
                )
                row_crew_options = list(active_crew_names)
                if assignment["crew_label"] not in row_crew_options:
                    row_crew_options.append(assignment["crew_label"])
                crew_name = columns[1].selectbox(
                    "Crew",
                    row_crew_options,
                    index=safe_index(row_crew_options, assignment["crew_label"]),
                    key=f"draft_grid_crew_{assignment['id']}",
                    label_visibility="collapsed",
                )
                people = crew_members.get(
                    crew_name,
                    [name.strip() for name in assignment["technicians"].split(",") if name.strip()],
                )
                columns[2].markdown(", ".join(people) if people else "_No saved members_")
                columns[3].markdown(str(assignment["work_order_id"]))
                columns[4].markdown(str(assignment.get("title", "")))
                hours = columns[5].number_input(
                    "Hours",
                    min_value=.5,
                    max_value=168.0,
                    step=.5,
                    value=float(assignment["hours"]),
                    key=f"draft_grid_hours_{assignment['id']}",
                    label_visibility="collapsed",
                )
                status = columns[6].selectbox(
                    "Status",
                    status_options,
                    index=safe_index(status_options, assignment["status"]),
                    key=f"draft_grid_status_{assignment['id']}",
                    label_visibility="collapsed",
                )
                notes = columns[7].text_input(
                    "Notes",
                    value=assignment["notes"],
                    key=f"draft_grid_notes_{assignment['id']}",
                    label_visibility="collapsed",
                )
                changes.append({
                    "id": assignment["id"],
                    "day": day,
                    "crew_label": crew_name,
                    "people": people,
                    "hours": hours,
                    "status": status,
                    "notes": notes,
                })

        if st.button("Save draft table", type="primary", use_container_width=True, key="save_draft_grid"):
            invalid = [
                change for change in changes
                if change["crew_label"] not in crew_members or not change["people"]
            ]
            if invalid:
                st.error("Every draft job must use an active saved crew that has members.")
                return
            with connection() as conn:
                for change in changes:
                    conn.execute(
                        """UPDATE assignments
                           SET day=?,crew_label=?,technicians=?,hours=?,status=?,notes=?,updated_at=?
                           WHERE id=? AND state='Draft'""",
                        (
                            change["day"],
                            change["crew_label"],
                            ",".join(change["people"]),
                            change["hours"],
                            change["status"],
                            change["notes"],
                            now(),
                            change["id"],
                        ),
                    )
                    history(
                        conn,
                        change["id"],
                        "Table updated",
                        f"Draft crew: {change['crew_label']}",
                    )
            flash("Draft table saved with the selected crews and their exact members.")
        return

    columns = ["id", "day", "crew_label", "work_order_id", "title", "technicians", "hours", "status", "notes"]
    frame = pd.DataFrame(assignments, columns=columns)
    disabled = ["id", "work_order_id", "title", "technicians"]
    if state != "Draft":
        disabled.append("crew_label")
    edited = st.data_editor(
        frame,
        key=f"{state.lower()}_assignment_spreadsheet",
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        disabled=disabled,
        column_config={
            "id": st.column_config.TextColumn("Assignment ID"),
            "day": st.column_config.SelectboxColumn("Day", options=DAYS, required=True),
            "crew_label": st.column_config.SelectboxColumn("Crew", options=crew_options, required=True),
            "work_order_id": st.column_config.TextColumn("Work order"),
            "title": st.column_config.TextColumn("Job"),
            "technicians": st.column_config.TextColumn("People in crew"),
            "hours": st.column_config.NumberColumn("Hours", min_value=.5, max_value=24, step=.5),
            "status": st.column_config.SelectboxColumn("Status", options=["Scheduled", "In Progress", "Deferred", "Complete"]),
            "notes": st.column_config.TextColumn("Notes"),
        },
    )
    if st.button(f"Save {state.lower()} table", type="primary", use_container_width=True, key=f"save_{state.lower()}_table"):
        records = edited.to_dict("records")
        if state == "Draft" and any(cell_text(record["crew_label"]) not in crew_members for record in records):
            st.error("Every draft job must use an active saved crew from Team > Crews.")
            return
        with connection() as conn:
            for record in records:
                crew_name = cell_text(record["crew_label"], "Crew")
                people = crew_members.get(crew_name, [name for name in cell_text(record["technicians"]).split(",") if name])
                conn.execute(
                    "UPDATE assignments SET day=?,crew_label=?,technicians=?,hours=?,status=?,notes=?,updated_at=? WHERE id=?",
                    (cell_text(record["day"], "Monday"), crew_name, ",".join(people), max(.5, cell_float(record["hours"], 1)), cell_text(record["status"], "Scheduled"), cell_text(record["notes"]), now(), cell_text(record["id"])),
                )
                history(conn, cell_text(record["id"]), "Table updated", f"{state} schedule edited")
        flash(f"{state} assignment table saved.")


def normalize_imported_jobs(source: pd.DataFrame) -> pd.DataFrame:
    aliases = {
        "work_order": "id", "work_order_number": "id", "work_order_no": "id", "wo": "id", "job_id": "id",
        "job": "title", "job_name": "title", "description": "title", "task": "title",
        "asset_number": "asset", "equipment": "asset", "equipment_number": "asset",
        "due": "due_at", "due_date": "due_at", "date_due": "due_at",
        "duration": "duration_hours", "estimated_hours": "duration_hours", "job_hours": "duration_hours",
        "crew": "crew_size", "crew_required": "crew_size", "crew_size_required": "crew_size",
        "mechanical": "mechanical_needed", "mechanical_manpower": "mechanical_needed", "mech_needed": "mechanical_needed",
        "welding": "welding_needed", "welding_manpower": "welding_needed", "weld_needed": "welding_needed",
        "priority_class": "priority", "score": "priority_score",
        "ready_to_schedule": "released", "release_to_scheduler": "released",
    }
    renamed: dict[str, str] = {}
    for column in source.columns:
        normalized = re.sub(r"[^a-z0-9]+", "_", str(column).strip().lower()).strip("_")
        renamed[column] = aliases.get(normalized, normalized)
    frame = source.rename(columns=renamed).copy()
    defaults = {
        "id": "", "title": "", "asset": "UNASSIGNED", "location": "Plant", "department": "Operations",
        "due_at": (datetime.now() + timedelta(days=2)).replace(hour=8, minute=0, second=0, microsecond=0).isoformat(),
        "duration_hours": 1.0, "priority": "Medium", "priority_score": 7, "status": "Pending",
        "category": "Mechanical", "crew_size": 1, "mechanical_needed": 0, "welding_needed": 0,
        "allowed_days": ",".join(DAYS[:5]), "preferred_day": "", "scope_ready": True, "parts_ready": True,
        "permits_ready": True, "shutdown_ready": True, "released": True, "notes": "",
    }
    for column, default in defaults.items():
        if column not in frame.columns:
            frame[column] = default
    frame = frame[JOB_TABLE_COLUMNS]
    for column in ["scope_ready", "parts_ready", "permits_ready", "shutdown_ready", "released"]:
        frame[column] = frame[column].map(lambda value: cell_bool(value, True))
    for column, default in [("duration_hours", 1.0), ("priority_score", 7), ("crew_size", 1), ("mechanical_needed", 0), ("welding_needed", 0)]:
        frame[column] = frame[column].map(lambda value, fallback=default: cell_float(value, fallback))
    frame["due_at"] = frame["due_at"].map(cell_datetime)
    frame["id"] = frame["id"].map(lambda value: cell_text(value))
    return frame


def job_template_frame() -> pd.DataFrame:
    return pd.DataFrame([{
        "work_order_number": "WO-5001", "job_name": "Inspect process pump", "asset_number": "P-101",
        "location": "Pump house", "department": "Utilities", "due_date": (datetime.now() + timedelta(days=7)).date().isoformat(),
        "duration_hours": 4, "priority": "High", "priority_score": 12, "status": "Pending",
        "category": "Mechanical", "crew_size_required": 2, "mechanical_manpower": 2,
        "welding_manpower": 0, "allowed_days": "Monday,Tuesday,Wednesday,Thursday,Friday",
        "preferred_day": "Tuesday", "scope_ready": True, "parts_ready": True, "permits_ready": True,
        "shutdown_ready": True, "ready_to_schedule": True, "notes": "Sample row - replace with your job",
    }])


def job_template_excel() -> bytes | None:
    if not EXCEL_SUPPORT:
        return None
    template = job_template_frame()
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        template.to_excel(writer, index=False, sheet_name="Jobs")
    return output.getvalue()


def excel_import_workspace(key_prefix: str) -> None:
    st.subheader("Import jobs from Excel")
    st.caption("Upload an .xlsx file, review every planning field in the table, then save the jobs to the backlog.")
    template_bytes = job_template_excel()
    if template_bytes is not None:
        st.download_button(
            "Download Excel template", template_bytes, "maintainly-job-import-template.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key=f"{key_prefix}_template",
        )
    else:
        st.warning("Excel support is not installed. Add openpyxl>=3.1,<4 to requirements.txt and reboot the app. You can use the CSV template until then.")
        st.download_button(
            "Download CSV template", job_template_frame().to_csv(index=False).encode("utf-8"),
            "maintainly-job-import-template.csv", "text/csv", key=f"{key_prefix}_csv_template",
        )
    upload_types = ["xlsx", "csv"] if EXCEL_SUPPORT else ["csv"]
    upload = st.file_uploader("Upload job workbook", type=upload_types, key=f"{key_prefix}_upload")
    if upload and st.button("Load workbook into editor", key=f"{key_prefix}_load", type="primary"):
        try:
            if upload.name.lower().endswith(".csv"):
                source_frame = pd.read_csv(upload)
            else:
                source_frame = pd.read_excel(upload, engine="openpyxl")
            st.session_state[f"{key_prefix}_frame"] = normalize_imported_jobs(source_frame)
            st.session_state[f"{key_prefix}_batch"] = uuid.uuid4().hex
            st.rerun()
        except ModuleNotFoundError:
            st.error("Excel support is missing. Add openpyxl>=3.1,<4 to requirements.txt, reboot the app, and upload the workbook again.")
        except Exception as exc:
            st.error(f"The workbook could not be read: {exc}")
    imported = st.session_state.get(f"{key_prefix}_frame")
    if isinstance(imported, pd.DataFrame):
        st.info("Edit any missing or incorrect scheduling fields below. Work order number and Job are required.")
        reviewed = st.data_editor(
            imported,
            key=f"{key_prefix}_review_{st.session_state.get(f'{key_prefix}_batch', 'current')}",
            use_container_width=True,
            hide_index=True,
            num_rows="dynamic",
            column_config=job_column_config(),
            height=520,
        )
        c1, c2 = st.columns(2)
        if c1.button("Save reviewed jobs", type="primary", use_container_width=True, key=f"{key_prefix}_commit"):
            records = reviewed.to_dict("records")
            if not records:
                st.error("The import table is empty.")
            elif any(not cell_text(record.get("id")) for record in records):
                st.error("Every imported job needs your Work order number.")
            elif any(not cell_text(record.get("title")) for record in records):
                st.error("Every imported job needs a Job value.")
            else:
                created = updated = 0
                existing_ids = {job["id"] for job in rows("SELECT id FROM work_orders")}
                for record in records:
                    job_id = cell_text(record.get("id"))
                    if job_id in existing_ids:
                        updated += 1
                    else:
                        created += 1
                        existing_ids.add(job_id)
                    save_job(job_data_from_row(record), job_id)
                st.session_state.pop(f"{key_prefix}_frame", None)
                st.session_state.pop(f"{key_prefix}_batch", None)
                flash(f"Excel import complete: {created} created and {updated} updated.")
        if c2.button("Cancel import", use_container_width=True, key=f"{key_prefix}_cancel"):
            st.session_state.pop(f"{key_prefix}_frame", None)
            st.session_state.pop(f"{key_prefix}_batch", None)
            st.rerun()


def history(conn: sqlite3.Connection, assignment_id: str, action: str, detail: str = "") -> None:
    conn.execute("INSERT INTO schedule_history VALUES (?,?,?,?,?)", (uid("SH"), assignment_id, action, detail, now()))


def generate_draft(daily_limit: float, clear_first: bool) -> tuple[int, list[str]]:
    """Build a draft using only the exact active crews saved by the user."""
    created, warnings = 0, []
    stamp = now()
    with connection() as conn:
        if clear_first:
            conn.execute("DELETE FROM assignments WHERE state='Draft'")
            conn.execute("UPDATE work_orders SET status='Pending' WHERE status='Draft Scheduled'")
        jobs = [dict(row) for row in conn.execute("SELECT * FROM work_orders WHERE status NOT IN ('Completed') AND released=1 ORDER BY priority_score DESC,due_at").fetchall()]
        members = {
            row["id"]: dict(row)
            for row in conn.execute("SELECT * FROM team_members").fetchall()
        }
        crews = []
        for row in conn.execute("SELECT * FROM team_crews WHERE active=1 ORDER BY name").fetchall():
            # Preserve the exact member order and membership saved on the crew.
            people = [members[member_id] for member_id in row["members"].split(",") if member_id in members]
            if people:
                crews.append({"id": row["id"], "name": row["name"], "people": people})
        if not crews:
            return 0, ["Create at least one active crew in Team > Crews before generating the draft schedule."]
        daily_crew_load: dict[tuple[str, str], float] = {}
        crew_load: dict[str, float] = {}
        for assignment in conn.execute("SELECT * FROM assignments WHERE status!='Complete'").fetchall():
            crew_name = assignment["crew_label"]
            daily_crew_load[(assignment["day"], crew_name)] = daily_crew_load.get((assignment["day"], crew_name), 0) + assignment["hours"]
            crew_load[crew_name] = crew_load.get(crew_name, 0) + assignment["hours"]
        for job in jobs:
            already = conn.execute("SELECT COALESCE(SUM(hours),0) FROM assignments WHERE work_order_id=? AND status!='Complete'", (job["id"],)).fetchone()[0]
            remaining = max(0.0, job["duration_hours"] - already)
            if remaining <= 0:
                continue
            days = [day for day in job["allowed_days"].split(",") if day in DAYS]
            if job["preferred_day"] in days:
                days = [job["preferred_day"], *[day for day in days if day != job["preferred_day"]]]
            if days:
                day = days[0]
                selected_crew = min(
                    crews,
                    key=lambda crew: (
                        daily_crew_load.get((day, crew["name"]), 0),
                        crew_load.get(crew["name"], 0),
                        crew["name"],
                    ),
                )
                names = [person["name"] for person in selected_crew["people"]]
                hours = remaining
                assignment_id = uid("SA")
                conn.execute("INSERT INTO assignments VALUES (?,?,?,?,?,?,?,?,?,?,?)", (assignment_id, job["id"], "Draft", day, selected_crew["name"], ",".join(names), hours, "Scheduled", job["notes"], stamp, stamp))
                history(conn, assignment_id, "Generated", f"{job['id']} assigned to {selected_crew['name']}")
                daily_crew_load[(day, selected_crew["name"])] = daily_crew_load.get((day, selected_crew["name"]), 0) + hours
                crew_load[selected_crew["name"]] = crew_load.get(selected_crew["name"], 0) + hours
                conn.execute("UPDATE work_orders SET status='Draft Scheduled',updated_at=? WHERE id=?", (stamp, job["id"]))
                created += 1
            else:
                warnings.append(f"{job['id']} - {job['title']} could not be assigned because it has no allowed scheduling day.")
    return created, warnings


def promote_all() -> int:
    with connection() as conn:
        assignments = conn.execute("SELECT * FROM assignments WHERE state='Draft'").fetchall()
        conn.execute("UPDATE assignments SET state='Final',updated_at=? WHERE state='Draft'", (now(),))
        for item in assignments:
            conn.execute("UPDATE work_orders SET status='Final Scheduled',updated_at=? WHERE id=?", (now(), item["work_order_id"]))
            history(conn, item["id"], "Promoted", "Draft to Final")
        return len(assignments)


def clear_final_schedule() -> int:
    """Remove unfinished Final assignments while preserving completed work."""
    with connection() as conn:
        assignments = conn.execute(
            "SELECT id,work_order_id FROM assignments WHERE state='Final' AND status!='Complete'"
        ).fetchall()
        if not assignments:
            return 0

        stamp = now()
        affected_work_orders = {assignment["work_order_id"] for assignment in assignments}
        for assignment in assignments:
            history(conn, assignment["id"], "Cleared", "Final schedule returned to Pending")
            conn.execute("DELETE FROM assignments WHERE id=?", (assignment["id"],))

        for work_order_id in affected_work_orders:
            remaining = conn.execute(
                "SELECT COUNT(*) FROM assignments WHERE work_order_id=? AND status!='Complete'",
                (work_order_id,),
            ).fetchone()[0]
            if remaining == 0:
                conn.execute(
                    """UPDATE work_orders
                       SET status='Pending',completed_at=NULL,updated_at=?
                       WHERE id=? AND status='Final Scheduled'""",
                    (stamp, work_order_id),
                )
        return len(assignments)


def title(text: str, copy: str) -> None:
    st.markdown('<div class="eyebrow">Operations workspace</div>', unsafe_allow_html=True)
    st.title(text)
    st.markdown(f'<p class="copy">{copy}</p>', unsafe_allow_html=True)


def team_form(prefix: str, member: dict | None = None) -> tuple[bool, dict]:
    member = member or {}
    with st.form(f"{prefix}_team"):
        c1, c2 = st.columns(2)
        name = c1.text_input("Name", member.get("name", ""))
        role = c2.text_input("Role", member.get("role", "Maintenance technician"))
        email = c1.text_input("Email (optional)", member.get("email") or "")
        skill = c2.selectbox("Skill", SKILLS, index=safe_index(SKILLS, member.get("skill", "Mechanical")))
        availability_options = ["Available", "Limited", "Unavailable"]
        availability = c1.selectbox("Availability", availability_options, index=safe_index(availability_options, member.get("availability", "Available")))
        hours = c2.number_input("Weekly hours", 1.0, 84.0, float(member.get("weekly_hours", 40)), 1.0)
        active = st.checkbox("Active", bool(member.get("active", 1)))
        submitted = st.form_submit_button("Save team member", type="primary", use_container_width=True)
    return submitted, {"name":name.strip(), "role":role.strip(), "email":email.strip(), "skill":skill, "availability":availability, "hours":hours, "active":active}


def crew_form(prefix: str, team: list[dict], crew: dict | None = None) -> tuple[bool, dict]:
    crew = crew or {}
    member_ids = [member["id"] for member in team]
    labels = {member["id"]: f"{member['name']} - {member['skill']}" for member in team}
    selected = [
        member_id for member_id in str(crew.get("members") or "").split(",")
        if member_id in labels
    ]
    with st.form(f"{prefix}_crew"):
        name = st.text_input("Crew name", crew.get("name", ""))
        members = st.multiselect(
            "Who is in this crew?",
            member_ids,
            default=selected,
            format_func=lambda member_id: labels.get(member_id, member_id),
        )
        st.info(f"Crew size: {len(members)} person{'s' if len(members) != 1 else ''}")
        active = st.checkbox("Active crew", bool(crew.get("active", 1)))
        submitted = st.form_submit_button("Save crew", type="primary", use_container_width=True)
    return submitted, {"name": name.strip(), "members": members, "active": active}


def asset_form(prefix: str, asset: dict | None = None) -> tuple[bool, dict]:
    asset = asset or {}
    with st.form(f"{prefix}_asset"):
        c1, c2 = st.columns(2)
        number = c1.text_input("Asset number", asset.get("asset_number", ""))
        name = c2.text_input("Asset name", asset.get("asset_name", ""))
        location = c1.text_input("Location", asset.get("location", ""))
        department = c2.text_input("Department", asset.get("department", "Operations"))
        criticality_options = ["Critical", "High", "Normal", "Low"]
        criticality = c1.selectbox("Criticality", criticality_options, index=safe_index(criticality_options, asset.get("criticality", "Normal")))
        manufacturer = c2.text_input("Manufacturer", asset.get("manufacturer", ""))
        model = c1.text_input("Model", asset.get("model", ""))
        active = c2.checkbox("Active asset", bool(asset.get("active", 1)))
        notes = st.text_area("Notes", asset.get("notes", ""))
        submitted = st.form_submit_button("Save asset", type="primary", use_container_width=True)
    return submitted, {"number":number.strip(), "name":name.strip(), "location":location.strip(), "department":department.strip(), "criticality":criticality, "manufacturer":manufacturer.strip(), "model":model.strip(), "active":active, "notes":notes.strip()}


def job_form(prefix: str, job: dict | None = None) -> tuple[bool, dict]:
    job = job or {}
    due = datetime.now() + timedelta(days=2)
    try:
        due = datetime.fromisoformat(job.get("due_at", ""))
    except ValueError:
        pass
    with st.form(f"{prefix}_job"):
        work_order_id = st.text_input(
            "Work order number",
            job.get("id", ""),
            disabled=bool(job.get("id")),
            help="Enter the number used by your plant or maintenance system.",
        )
        name = st.text_input("Job name", job.get("title", ""))
        c1, c2, c3 = st.columns(3)
        asset = c1.text_input("Asset / equipment (optional)", job.get("asset", ""))
        location = c2.text_input("Location", job.get("location", "Plant"))
        department = c3.text_input("Department", job.get("department", "Operations"))
        due_date = c1.date_input("Due date", due.date())
        due_time = c2.time_input("Due time", due.time().replace(second=0, microsecond=0))
        duration = c3.number_input("Duration hours", .5, 168.0, float(job.get("duration_hours", 1)), .5)
        priority = c1.selectbox("Priority", PRIORITIES, index=safe_index(PRIORITIES, job.get("priority", "Medium")))
        score = c2.number_input("Priority score", 1, 20, int(job.get("priority_score", 7)))
        status = c3.selectbox("Status", JOB_STATUSES, index=safe_index(JOB_STATUSES, job.get("status", "Pending")))
        category = c1.text_input("Category", job.get("category", "Mechanical"))
        crew = c2.number_input("Crew required", 1, 20, int(job.get("crew_size", 1)))
        preferred_options = ["No preference", *DAYS]
        preferred_value = job.get("preferred_day", "") or "No preference"
        preferred = c3.selectbox("Preferred day", preferred_options, index=safe_index(preferred_options, preferred_value))
        mechanical = c1.number_input("Mechanical manpower", 0, 20, int(job.get("mechanical_needed", 0)))
        welding = c2.number_input("Welding manpower", 0, 20, int(job.get("welding_needed", 0)))
        allowed_default = [d for d in job.get("allowed_days", ",".join(DAYS[:5])).split(",") if d in DAYS]
        allowed = st.multiselect("Allowed days", DAYS, default=allowed_default)
        r1, r2, r3, r4, r5 = st.columns(5)
        scope = r1.checkbox("Scope ready", bool(job.get("scope_ready", 1)))
        parts = r2.checkbox("Parts ready", bool(job.get("parts_ready", 1)))
        permits = r3.checkbox("Permits ready", bool(job.get("permits_ready", 1)))
        shutdown = r4.checkbox("Shutdown ready", bool(job.get("shutdown_ready", 1)))
        released = r5.checkbox("Release", bool(job.get("released", 1)))
        notes = st.text_area("Notes", job.get("notes", ""))
        submitted = st.form_submit_button("Save work order", type="primary", use_container_width=True)
    return submitted, {"work_order_id":work_order_id.strip(), "title":name.strip(), "asset":asset.strip() or "UNASSIGNED", "location":location.strip(), "department":department.strip(), "due_at":datetime.combine(due_date, due_time).isoformat(), "duration":duration, "priority":priority, "score":score, "status":status, "category":category.strip(), "crew":crew, "mechanical":mechanical, "welding":welding, "allowed_days":allowed, "preferred_day":"" if preferred == "No preference" else preferred, "scope":scope, "parts":parts, "permits":permits, "shutdown":shutdown, "released":released, "notes":notes.strip()}

def assignment_rows(state: str | None = None) -> list[dict]:
    query = """SELECT a.*,w.title,w.asset,w.location FROM assignments a LEFT JOIN work_orders w ON w.id=a.work_order_id"""
    params = ()
    if state:
        query += " WHERE a.state=? AND a.status!='Complete'"
        params = (state,)
    query += " ORDER BY CASE a.day WHEN 'Monday' THEN 1 WHEN 'Tuesday' THEN 2 WHEN 'Wednesday' THEN 3 WHEN 'Thursday' THEN 4 WHEN 'Friday' THEN 5 ELSE 6 END,a.crew_label"
    return rows(query, params)


def board(state: str) -> None:
    assignments = assignment_rows(state)
    for group in (DAYS[:5], DAYS[5:]):
        columns = st.columns(len(group))
        for column, day in zip(columns, group):
            with column:
                day_rows = [a for a in assignments if a["day"] == day]
                st.subheader(day[:3])
                st.caption(f"{sum(a['hours'] for a in day_rows):.1f}h")
                for a in day_rows:
                    st.markdown(f'<div class="board-card"><small>{a["crew_label"]}</small><b>{a["title"]}</b><span>{a["work_order_id"]} · {a["hours"]:.1f}h<br>{a["technicians"].replace(",", ", ")}</span></div>', unsafe_allow_html=True)
                if not day_rows:
                    st.caption("No work")


with st.sidebar:
    st.markdown('<div class="brand"><span>M</span><b>Maintainly</b><small>Plant maintenance</small></div>', unsafe_allow_html=True)
    page = st.radio("Navigation", ["Schedule", "Work orders", "Planning", "Team", "Reports"], label_visibility="collapsed")
    st.markdown("---")
    st.caption("Persistent Streamlit edition")

message = st.session_state.pop("flash", None)
if message:
    st.success(message)


if page == "Schedule":
    title("Schedule", "Plan the week, spot risks early and keep your team moving.")
    assignments = [a for a in assignment_rows() if a["status"] != "Complete"]
    open_jobs = rows("SELECT * FROM work_orders WHERE status!='Completed'")
    team = rows("SELECT * FROM team_members WHERE active=1")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Scheduled work", len(assignments))
    c2.metric("Planned hours", f"{sum(a['hours'] for a in assignments):.1f}h")
    c3.metric("Open backlog", len(open_jobs))
    c4.metric("Active team", len(team))
    state = st.radio("State", ["Final", "Draft"], horizontal=True, label_visibility="collapsed")
    board(state)

elif page == "Team":
    title("Team", "Add, edit and remove team members and manage weekly capacity.")
    team = rows("SELECT * FROM team_members ORDER BY name")
    crews = rows("SELECT * FROM team_crews ORDER BY name")
    c1, c2, c3, c4 = st.columns(4)
    active = [m for m in team if m["active"]]
    c1.metric("Active members", len(active))
    c2.metric("Weekly capacity", f"{sum(m['weekly_hours'] for m in active):.0f}h")
    c3.metric("Skills covered", len({m["skill"] for m in active}))
    c4.metric("Saved crews", len(crews))
    roster, add, edit, crew_tab = st.tabs(["Team table", "Add member", "Edit / delete", "Crews"])
    with roster:
        st.caption("Edit the team directly in the table, then select Save team table.")
        team_table_editor(team)
    with add:
        submitted, data = team_form("add")
        if submitted:
            if not data["name"]:
                st.error("Name is required.")
            else:
                try:
                    save_team(data)
                    flash("Team member added.")
                except sqlite3.IntegrityError:
                    st.error("That email address is already in use. Leave it blank if the member has no email.")
    with edit:
        if team:
            labels = {m["id"]: f"{m['name']} - {m['skill']}" for m in team}
            member_id = st.selectbox("Select team member", list(labels), format_func=labels.get)
            member = next(m for m in team if m["id"] == member_id)
            submitted, data = team_form(f"edit_{member_id}", member)
            if submitted:
                try:
                    save_team(data, member_id)
                    flash("Team member updated.")
                except sqlite3.IntegrityError:
                    st.error("That email address is already in use. Leave it blank if the member has no email.")
            if st.button("Delete selected team member"):
                with connection() as conn:
                    for crew in conn.execute("SELECT id,members FROM team_crews").fetchall():
                        remaining = [value for value in crew["members"].split(",") if value and value != member_id]
                        if remaining:
                            conn.execute(
                                "UPDATE team_crews SET members=?,updated_at=? WHERE id=?",
                                (",".join(remaining), now(), crew["id"]),
                            )
                        else:
                            conn.execute("DELETE FROM team_crews WHERE id=?", (crew["id"],))
                    conn.execute("DELETE FROM team_members WHERE id=?", (member_id,))
                flash("Team member deleted.")
    with crew_tab:
        member_lookup = {member["id"]: member["name"] for member in team}
        display = []
        for crew in crews:
            member_ids = [value for value in crew["members"].split(",") if value]
            display.append({
                "Crew": crew["name"],
                "Persons": len(member_ids),
                "Members": ", ".join(member_lookup.get(member_id, "Removed member") for member_id in member_ids),
                "Active": bool(crew["active"]),
            })
        if display:
            st.dataframe(pd.DataFrame(display), use_container_width=True, hide_index=True)
        else:
            st.info("No crews saved yet.")
        create_crew, manage_crew = st.tabs(["Create crew", "Edit / delete crew"])
        with create_crew:
            if not team:
                st.info("Add team members before creating a crew.")
            else:
                submitted, data = crew_form("new", team)
                if submitted:
                    try:
                        save_crew(data)
                        flash(f"{data['name']} crew saved with {len(data['members'])} person(s).")
                    except (ValueError, sqlite3.IntegrityError) as exc:
                        st.error("That crew name already exists." if isinstance(exc, sqlite3.IntegrityError) else str(exc))
        with manage_crew:
            if not crews:
                st.info("Create a crew first.")
            else:
                crew_labels = {crew["id"]: f"{crew['name']} ({len([value for value in crew['members'].split(',') if value])} persons)" for crew in crews}
                crew_id = st.selectbox("Select crew", list(crew_labels), format_func=crew_labels.get)
                selected_crew = next(crew for crew in crews if crew["id"] == crew_id)
                submitted, data = crew_form(f"edit_{crew_id}", team, selected_crew)
                if submitted:
                    try:
                        save_crew(data, crew_id)
                        flash(f"{data['name']} crew updated.")
                    except (ValueError, sqlite3.IntegrityError) as exc:
                        st.error("That crew name already exists." if isinstance(exc, sqlite3.IntegrityError) else str(exc))
                if st.button("Delete selected crew"):
                    delete_crew(crew_id)
                    flash("Crew deleted.")

elif page == "Assets":
    title("Assets", "Track asset health, criticality and service history.")
    assets = rows("SELECT * FROM assets ORDER BY asset_number")
    c1, c2, c3 = st.columns(3)
    c1.metric("Registered assets", len(assets))
    c2.metric("Active assets", len([a for a in assets if a["active"]]))
    c3.metric("Critical assets", len([a for a in assets if a["criticality"] == "Critical"]))
    register, add, edit = st.tabs(["Table editor", "Add asset", "Edit / delete"])
    with register:
        st.caption("Edit asset records directly in the table, then save all changes.")
        asset_table_editor(assets)
    with add:
        submitted, data = asset_form("add")
        if submitted:
            if not data["number"] or not data["name"]:
                st.error("Asset number and name are required.")
            else:
                try:
                    save_asset(data)
                    flash("Asset added.")
                except sqlite3.IntegrityError:
                    st.error("That asset number already exists.")
    with edit:
        if assets:
            labels = {a["id"]: f"{a['asset_number']} - {a['asset_name']}" for a in assets}
            asset_id = st.selectbox("Select asset", list(labels), format_func=labels.get)
            asset = next(a for a in assets if a["id"] == asset_id)
            submitted, data = asset_form(f"edit_{asset_id}", asset)
            if submitted:
                save_asset(data, asset_id)
                flash("Asset updated.")
            if st.button("Delete selected asset"):
                with connection() as conn:
                    conn.execute("DELETE FROM assets WHERE id=?", (asset_id,))
                flash("Asset deleted.")

elif page == "Work orders":
    title("Work orders", "Prioritize, assign and close out maintenance work.")
    jobs = rows("SELECT * FROM work_orders ORDER BY priority_score DESC,due_at")
    register, add, import_tab, edit = st.tabs(["Table editor", "Add form", "Import Excel", "Edit / delete"])
    with register:
        filter_value = st.selectbox("Filter", ["Open", "All", *JOB_STATUSES])
        filtered = jobs if filter_value == "All" else [j for j in jobs if (j["status"] != "Completed" if filter_value == "Open" else j["status"] == filter_value)]
        st.caption("Add a row, enter your work order number and job details, then save the table.")
        job_table_editor(filtered, "work_order_table")
    with add:
        submitted, data = job_form("add")
        if submitted:
            if not data["work_order_id"] or not data["title"] or not data["allowed_days"]:
                st.error("Work order number, job name and at least one allowed day are required.")
            else:
                try:
                    with connection() as conn:
                        exists = conn.execute("SELECT 1 FROM work_orders WHERE id=?", (data["work_order_id"],)).fetchone()
                    if exists:
                        st.error("That work order number already exists.")
                    else:
                        save_job(data)
                        flash("Work order created.")
                except sqlite3.IntegrityError:
                    st.error("That work order number already exists.")
    with import_tab:
        excel_import_workspace("work_orders_excel")
    with edit:
        if jobs:
            labels = {j["id"]: f"{j['id']} - {j['title']}" for j in jobs}
            job_id = st.selectbox("Select work order", list(labels), format_func=labels.get)
            job = next(j for j in jobs if j["id"] == job_id)
            submitted, data = job_form(f"edit_{job_id}", job)
            if submitted:
                save_job(data, job_id)
                flash("Work order updated.")
            if st.button("Delete selected work order"):
                with connection() as conn:
                    conn.execute("DELETE FROM work_orders WHERE id=?", (job_id,))
                flash("Work order deleted.")
            st.divider()
            st.subheader("Delete all work orders")
            st.warning("This also removes every draft and final schedule assignment linked to the work orders.")
            confirm_delete_all = st.checkbox("I understand and want to delete all work orders")
            if st.button("Delete all work orders", disabled=not confirm_delete_all, type="primary"):
                with connection() as conn:
                    count = conn.execute("SELECT COUNT(*) FROM work_orders").fetchone()[0]
                    conn.execute("DELETE FROM schedule_history")
                    conn.execute("DELETE FROM assignments")
                    conn.execute("DELETE FROM work_orders")
                flash(f"{count} work order(s) deleted.")

elif page == "Planning":
    title("Planning", "Turn the maintenance backlog into validated crews and a final weekly plan.")
    jobs = rows("SELECT * FROM work_orders ORDER BY priority_score DESC,due_at")
    open_jobs = [j for j in jobs if j["status"] != "Completed"]
    draft = assignment_rows("Draft")
    final = assignment_rows("Final")
    ready = [j for j in open_jobs if j["scope_ready"] and j["parts_ready"] and j["permits_ready"] and j["shutdown_ready"] and j["released"]]
    overview, readiness, draft_tab, final_tab, board_tab, history_tab, data_tab = st.tabs(["Overview", "Readiness", "Draft", "Final", "Board", "History", "Data"])
    with overview:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Backlog", len(open_jobs), f"{len(ready)} ready")
        c2.metric("Draft assignments", len(draft))
        c3.metric("Final assignments", len(final))
        c4.metric("Completed", len(jobs) - len(open_jobs))
    with readiness:
        st.caption("Correct readiness, labor, allowed days, duration, and priority here before generating the schedule.")
        job_table_editor(open_jobs, "planning_readiness_table")
    with draft_tab:
        st.caption(
            "Scheduling uses only the exact active crews you built in Team → Crews. "
            "It never creates or changes a crew."
        )
        c1, c2 = st.columns(2)
        clear = c1.checkbox("Clear existing draft", True)
        if c2.button("Generate draft", type="primary", use_container_width=True):
            count, warnings = generate_draft(8, clear)
            st.session_state["warnings"] = warnings
            flash(f"{count} draft assignment(s) generated.")
        for warning in st.session_state.get("warnings", []):
            st.warning(warning)
        st.caption("Change the Crew cell and the People in crew column updates immediately on that row.")
        assignment_table_editor("Draft", draft)
        if st.button("Promote all draft assignments"):
            flash(f"{promote_all()} assignment(s) promoted to Final.")
        if draft and st.button("Clear draft schedule"):
            with connection() as conn:
                conn.execute("DELETE FROM assignments WHERE state='Draft'")
                conn.execute("UPDATE work_orders SET status='Pending' WHERE status='Draft Scheduled'")
            flash("Draft schedule cleared.")
    with final_tab:
        st.caption("The final schedule is locked to saved crews. Download the complete schedule or one PDF per crew.")
        assignment_table_editor("Final", final)
        if final:
            if PDF_SUPPORT:
                st.subheader("Download final crew schedules")
                st.download_button(
                    "Download complete final schedule PDF",
                    data=build_schedule_pdf(final),
                    file_name="maintainly-final-schedule.pdf",
                    mime="application/pdf",
                    type="primary",
                    use_container_width=True,
                )
                for crew_name in sorted({assignment["crew_label"] for assignment in final}):
                    safe_name = "".join(character if character.isalnum() else "-" for character in crew_name).strip("-").lower() or "crew"
                    st.download_button(
                        f"Download {crew_name} PDF",
                        data=build_schedule_pdf(final, crew_name, f"{crew_name} - Final Schedule"),
                        file_name=f"{safe_name}-final-schedule.pdf",
                        mime="application/pdf",
                        key=f"download_{safe_name}_final_pdf",
                        use_container_width=True,
                    )
            else:
                st.warning("PDF support is not installed. Add reportlab>=4,<5 to requirements.txt and reboot the app.")
            labels = {a["id"]: f"{a['day']} - {a['crew_label']} - {a['work_order_id']}" for a in final}
            assignment_id = st.selectbox("Select final assignment", list(labels), format_func=labels.get)
            if st.button("Complete selected assignment", type="primary"):
                with connection() as conn:
                    assignment = conn.execute("SELECT * FROM assignments WHERE id=?", (assignment_id,)).fetchone()
                    conn.execute("UPDATE assignments SET status='Complete',updated_at=? WHERE id=?", (now(), assignment_id))
                    remaining = conn.execute("SELECT COUNT(*) FROM assignments WHERE work_order_id=? AND status!='Complete'", (assignment["work_order_id"],)).fetchone()[0]
                    if remaining == 0:
                        conn.execute("UPDATE work_orders SET status='Completed',completed_at=?,updated_at=? WHERE id=?", (now(), now(), assignment["work_order_id"]))
                    history(conn, assignment_id, "Completed")
                flash("Assignment completed.")
            st.divider()
            st.subheader("Clear final schedule")
            st.warning(
                "This removes unfinished Final assignments and returns their work orders "
                "to Pending. Completed work and its history are kept."
            )
            confirm_clear_final = st.checkbox(
                "I understand and want to clear the final schedule",
                key="confirm_clear_final_schedule",
            )
            if st.button(
                "Clear final schedule",
                disabled=not confirm_clear_final,
                key="clear_final_schedule",
            ):
                flash(f"{clear_final_schedule()} unfinished final assignment(s) cleared.")
    with board_tab:
        board_state = st.radio("Board state", ["Draft", "Final"], horizontal=True)
        board(board_state)
    with history_tab:
        completed = [j for j in jobs if j["status"] == "Completed"]
        st.subheader("Completed jobs")
        st.dataframe(pd.DataFrame(completed)[["id", "title", "asset", "completed_at"]] if completed else pd.DataFrame(), use_container_width=True, hide_index=True)
        st.subheader("Schedule audit trail")
        st.dataframe(pd.DataFrame(rows("SELECT * FROM schedule_history ORDER BY changed_at DESC LIMIT 100")), use_container_width=True, hide_index=True)
    with data_tab:
        export_rows = rows("SELECT * FROM work_orders ORDER BY id")
        buffer = io.StringIO()
        if export_rows:
            writer = csv.DictWriter(buffer, fieldnames=list(export_rows[0]))
            writer.writeheader(); writer.writerows(export_rows)
        st.download_button("Download work orders CSV", buffer.getvalue(), "maintainly-work-orders.csv", "text/csv", type="primary")
        st.markdown("---")
        excel_import_workspace("planning_excel")

else:
    title("Reports", "Turn maintenance activity into clear operational decisions.")
    jobs = rows("SELECT * FROM work_orders")
    assignments = rows("SELECT * FROM assignments")
    completed = [j for j in jobs if j["status"] == "Completed"]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total work orders", len(jobs))
    c2.metric("Completed", len(completed))
    c3.metric("Completion rate", f"{len(completed) / len(jobs) * 100 if jobs else 0:.0f}%")
    c4.metric("Scheduled hours", f"{sum(a['hours'] for a in assignments):.1f}h")
    left, right = st.columns(2)
    with left:
        st.subheader("Jobs by status")
        if jobs:
            st.bar_chart(pd.Series([j["status"] for j in jobs]).value_counts())
    with right:
        st.subheader("Jobs by department")
        if jobs:
            st.bar_chart(pd.Series([j["department"] for j in jobs]).value_counts())
