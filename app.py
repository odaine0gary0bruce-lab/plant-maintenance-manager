
"""
Plant Maintenance Manager V14
Single-file Streamlit app using generated work teams instead of fixed team membership.

Run:
    streamlit run app_v14_generated_teams.py
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from io import BytesIO
from typing import Optional

import pandas as pd
import streamlit as st

# ---------------------------------------------------------
# PAGE CONFIG
# ---------------------------------------------------------
st.set_page_config(page_title="Plant Maintenance Manager V14", layout="wide")

st.markdown("""
<style>
.block-container {
    padding-top: 1rem;
    padding-bottom: 2rem;
    padding-left: 1.2rem;
    padding-right: 1.2rem;
}
div[data-testid="stMetric"] {
    background-color: #f8f9fa;
    border: 1px solid #e9ecef;
    padding: 12px;
    border-radius: 12px;
}
div.stButton > button {
    border-radius: 10px;
    height: 42px;
    font-weight: 600;
}
div[data-baseweb="select"] > div {
    border-radius: 10px;
}
div[data-testid="stDataFrame"] {
    border-radius: 10px;
    overflow: hidden;
}
.small-caption {
    color: #6c757d;
    font-size: 0.9rem;
}
</style>
""", unsafe_allow_html=True)

st.title("Plant Maintenance Manager V14")
st.caption("Generated Teams • Editable Draft Schedule • Final Schedule • History")

# ---------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------
DB_PATH = "maintenance.db"

DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
PRIORITY_CLASSES = [
    "Emergency",
    "Urgent",
    "High",
    "Medium",
    "Low",
    "Opportunity / Shutdown",
]
JOB_STATUS_OPTIONS = [
    "Pending",
    "Draft Scheduled",
    "Final Scheduled",
    "Active",
    "On Hold",
    "Complete",
]
ASSIGNMENT_STATUS_OPTIONS = ["Scheduled", "In Progress", "Deferred", "Complete"]

DEFAULT_TECHS = [
    {"technician": "Tech 1", "skill": "Mechanical", "weekly_hours": 40, "active": 1},
    {"technician": "Tech 2", "skill": "Mechanical", "weekly_hours": 40, "active": 1},
    {"technician": "Tech 3", "skill": "Mechanical", "weekly_hours": 40, "active": 1},
    {"technician": "Tech 4", "skill": "Mechanical", "weekly_hours": 40, "active": 1},
    {"technician": "Tech 5", "skill": "Mechanical", "weekly_hours": 40, "active": 1},
    {"technician": "Tech 6", "skill": "Mechanical", "weekly_hours": 40, "active": 1},
    {"technician": "Tech 7", "skill": "Welding", "weekly_hours": 40, "active": 1},
    {"technician": "Tech 8", "skill": "Welding", "weekly_hours": 40, "active": 1},
    {"technician": "Tech 9", "skill": "Welding", "weekly_hours": 40, "active": 1},
    {"technician": "Tech 10", "skill": "Welding", "weekly_hours": 40, "active": 1},
]

# ---------------------------------------------------------
# DB HELPERS
# ---------------------------------------------------------
@contextmanager
def get_connection():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA busy_timeout = 30000;")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    ).fetchone()
    return row is not None


def column_exists(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    cols = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(col["name"] == column_name for col in cols)


def add_column_if_missing(conn: sqlite3.Connection, table_name: str, col_name: str, col_def: str):
    if not column_exists(conn, table_name, col_name):
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_def}")


def rows_to_df(rows) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame([dict(r) for r in rows])


def normalize_allowed_days(selected_days):
    ordered = [d for d in DAYS if d in selected_days]
    return ",".join(ordered)


def parse_allowed_days(days_text: str):
    if not days_text:
        return DAYS[:5]
    vals = [x.strip() for x in str(days_text).split(",") if x.strip()]
    return [d for d in DAYS if d in vals] or DAYS[:5]


def calculate_priority_score(safety, production, criticality, delay_risk):
    return int(safety) + int(production) + int(criticality) + int(delay_risk)


def map_score_to_priority_class(score: int) -> str:
    if score >= 17:
        return "Emergency"
    elif score >= 13:
        return "Urgent"
    elif score >= 9:
        return "High"
    elif score >= 5:
        return "Medium"
    else:
        return "Low"


def normalize_priority_class(priority_value: Optional[int]) -> str:
    if priority_value is None:
        return "Medium"
    try:
        p = int(priority_value)
    except Exception:
        return "Medium"
    if p <= 2:
        return "Emergency"
    if p <= 4:
        return "Urgent"
    if p <= 7:
        return "High"
    if p <= 12:
        return "Medium"
    return "Low"


def normalize_priority_score(priority_value: Optional[int]) -> int:
    if priority_value is None:
        return 8
    try:
        p = int(priority_value)
    except Exception:
        return 8
    if p <= 2:
        return 18
    if p <= 4:
        return 15
    if p <= 7:
        return 11
    if p <= 12:
        return 7
    return 3


def default_allowed_days(weekend_allowed: int = 0) -> str:
    return ",".join(DAYS if weekend_allowed else DAYS[:5])


# ---------------------------------------------------------
# INITIALIZE / MIGRATE
# ---------------------------------------------------------
def initialize_database():
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS app_metadata (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        ensure_technicians_table(conn)
        ensure_jobs_table(conn)
        ensure_schedule_assignments_table(conn)
        ensure_schedule_history_table(conn)
        ensure_indexes(conn)
        seed_default_technicians_if_empty(conn)
        migrate_old_manual_schedule(conn)
        migrate_old_jobs_hours(conn)

        conn.execute("""
            INSERT INTO app_metadata(key, value)
            VALUES('schema_version', '14')
            ON CONFLICT(key) DO UPDATE SET value='14'
        """)


def ensure_technicians_table(conn):
    if not table_exists(conn, "technicians"):
        conn.execute("""
            CREATE TABLE technicians (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                technician TEXT NOT NULL UNIQUE,
                skill TEXT NOT NULL,
                weekly_hours REAL NOT NULL DEFAULT 40,
                active INTEGER NOT NULL DEFAULT 1
            )
        """)
    else:
        add_column_if_missing(conn, "technicians", "active", "INTEGER NOT NULL DEFAULT 1")


def ensure_jobs_table(conn):
    if not table_exists(conn, "jobs"):
        conn.execute("""
            CREATE TABLE jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job TEXT NOT NULL,
                location TEXT,
                department TEXT,
                duration_hours REAL NOT NULL DEFAULT 1,
                mechanical_manpower INTEGER NOT NULL DEFAULT 0,
                welding_manpower INTEGER NOT NULL DEFAULT 0,
                crew_size_required INTEGER NOT NULL DEFAULT 1,
                priority_class TEXT NOT NULL DEFAULT 'Medium',
                priority_score INTEGER NOT NULL DEFAULT 8,
                allowed_days TEXT NOT NULL DEFAULT 'Monday,Tuesday,Wednesday,Thursday,Friday',
                preferred_day TEXT,
                earliest_start_day TEXT,
                latest_finish_day TEXT,
                weekend_allowed INTEGER NOT NULL DEFAULT 0,
                requires_shutdown INTEGER NOT NULL DEFAULT 0,
                fixed_day_job INTEGER NOT NULL DEFAULT 0,
                can_split_across_days INTEGER NOT NULL DEFAULT 1,
                status TEXT NOT NULL DEFAULT 'Pending',
                notes TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                completed_at TEXT
            )
        """)
        return

    old_cols = [c["name"] for c in conn.execute("PRAGMA table_info(jobs)").fetchall()]
    if "hours" in old_cols and "duration_hours" not in old_cols:
        add_column_if_missing(conn, "jobs", "duration_hours", "REAL")
    add_column_if_missing(conn, "jobs", "crew_size_required", "INTEGER NOT NULL DEFAULT 1")
    add_column_if_missing(conn, "jobs", "priority_class", "TEXT NOT NULL DEFAULT 'Medium'")
    add_column_if_missing(conn, "jobs", "priority_score", "INTEGER NOT NULL DEFAULT 8")
    add_column_if_missing(conn, "jobs", "allowed_days", "TEXT NOT NULL DEFAULT 'Monday,Tuesday,Wednesday,Thursday,Friday'")
    add_column_if_missing(conn, "jobs", "preferred_day", "TEXT")
    add_column_if_missing(conn, "jobs", "earliest_start_day", "TEXT")
    add_column_if_missing(conn, "jobs", "latest_finish_day", "TEXT")
    add_column_if_missing(conn, "jobs", "weekend_allowed", "INTEGER NOT NULL DEFAULT 0")
    add_column_if_missing(conn, "jobs", "requires_shutdown", "INTEGER NOT NULL DEFAULT 0")
    add_column_if_missing(conn, "jobs", "fixed_day_job", "INTEGER NOT NULL DEFAULT 0")
    add_column_if_missing(conn, "jobs", "can_split_across_days", "INTEGER NOT NULL DEFAULT 1")
    add_column_if_missing(conn, "jobs", "notes", "TEXT")
    add_column_if_missing(conn, "jobs", "created_at", "TEXT")
    add_column_if_missing(conn, "jobs", "completed_at", "TEXT")

    conn.execute("UPDATE jobs SET created_at = COALESCE(created_at, CURRENT_TIMESTAMP) WHERE created_at IS NULL")
    if "hours" in old_cols:
        conn.execute("UPDATE jobs SET duration_hours = COALESCE(duration_hours, hours, 1) WHERE duration_hours IS NULL")
    else:
        conn.execute("UPDATE jobs SET duration_hours = COALESCE(duration_hours, 1) WHERE duration_hours IS NULL")

    conn.execute("""
        UPDATE jobs
        SET crew_size_required = CASE
            WHEN COALESCE(mechanical_manpower, 0) + COALESCE(welding_manpower, 0) > 0
            THEN COALESCE(mechanical_manpower, 0) + COALESCE(welding_manpower, 0)
            ELSE 1
        END
        WHERE crew_size_required IS NULL OR crew_size_required < 1
    """)

    if "priority" in old_cols:
        rows = conn.execute("SELECT id, priority FROM jobs").fetchall()
        for row in rows:
            conn.execute("""
                UPDATE jobs
                SET priority_class = COALESCE(priority_class, ?),
                    priority_score = COALESCE(priority_score, ?)
                WHERE id = ?
            """, (normalize_priority_class(row["priority"]), normalize_priority_score(row["priority"]), row["id"]))

    conn.execute("UPDATE jobs SET priority_class = 'Medium' WHERE priority_class IS NULL OR TRIM(priority_class) = ''")
    conn.execute("UPDATE jobs SET priority_score = 8 WHERE priority_score IS NULL")
    rows = conn.execute("SELECT id, weekend_allowed, allowed_days FROM jobs").fetchall()
    for row in rows:
        if row["allowed_days"] is None or str(row["allowed_days"]).strip() == "":
            conn.execute("UPDATE jobs SET allowed_days = ? WHERE id = ?", (default_allowed_days(int(row["weekend_allowed"] or 0)), row["id"]))


def ensure_schedule_assignments_table(conn):
    if not table_exists(conn, "schedule_assignments"):
        conn.execute("""
            CREATE TABLE schedule_assignments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER,
                source_type TEXT NOT NULL DEFAULT 'job',
                source_reference_id INTEGER,
                schedule_state TEXT NOT NULL DEFAULT 'Draft',
                day TEXT NOT NULL,
                team_label TEXT,
                assigned_technicians TEXT,
                assigned_hours REAL NOT NULL DEFAULT 1,
                required_crew_size INTEGER NOT NULL DEFAULT 1,
                mechanical_manpower INTEGER NOT NULL DEFAULT 0,
                welding_manpower INTEGER NOT NULL DEFAULT 0,
                priority_class TEXT NOT NULL DEFAULT 'Medium',
                priority_score INTEGER NOT NULL DEFAULT 8,
                location TEXT,
                department TEXT,
                notes TEXT,
                status TEXT NOT NULL DEFAULT 'Scheduled',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE SET NULL
            )
        """)
        return

    add_column_if_missing(conn, "schedule_assignments", "team_label", "TEXT")
    add_column_if_missing(conn, "schedule_assignments", "required_crew_size", "INTEGER NOT NULL DEFAULT 1")
    add_column_if_missing(conn, "schedule_assignments", "mechanical_manpower", "INTEGER NOT NULL DEFAULT 0")
    add_column_if_missing(conn, "schedule_assignments", "welding_manpower", "INTEGER NOT NULL DEFAULT 0")
    add_column_if_missing(conn, "schedule_assignments", "priority_class", "TEXT NOT NULL DEFAULT 'Medium'")
    add_column_if_missing(conn, "schedule_assignments", "priority_score", "INTEGER NOT NULL DEFAULT 8")
    add_column_if_missing(conn, "schedule_assignments", "location", "TEXT")
    add_column_if_missing(conn, "schedule_assignments", "department", "TEXT")
    add_column_if_missing(conn, "schedule_assignments", "updated_at", "TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP")


def ensure_schedule_history_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schedule_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            assignment_id INTEGER NOT NULL,
            action_type TEXT NOT NULL,
            old_value TEXT,
            new_value TEXT,
            changed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (assignment_id) REFERENCES schedule_assignments(id) ON DELETE CASCADE
        )
    """)


def ensure_indexes(conn):
    conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_priority ON jobs(priority_class, priority_score)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_assignments_state ON schedule_assignments(schedule_state)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_assignments_job ON schedule_assignments(job_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_assignments_day ON schedule_assignments(day)")


def seed_default_technicians_if_empty(conn):
    count = conn.execute("SELECT COUNT(*) AS c FROM technicians").fetchone()["c"]
    if count == 0:
        conn.executemany("""
            INSERT INTO technicians (technician, skill, weekly_hours, active)
            VALUES (:technician, :skill, :weekly_hours, :active)
        """, DEFAULT_TECHS)


def migrate_old_jobs_hours(conn):
    if table_exists(conn, "jobs") and column_exists(conn, "jobs", "priority"):
        rows = conn.execute("SELECT id, priority FROM jobs WHERE priority_score IS NULL OR priority_class IS NULL").fetchall()
        for row in rows:
            conn.execute("""
                UPDATE jobs
                SET priority_class=?,
                    priority_score=?
                WHERE id=?
            """, (normalize_priority_class(row["priority"]), normalize_priority_score(row["priority"]), row["id"]))


def migrate_old_manual_schedule(conn):
    if not table_exists(conn, "manual_schedule"):
        return
    rows = conn.execute("SELECT * FROM manual_schedule").fetchall()
    old_cols = [c["name"] for c in conn.execute("PRAGMA table_info(manual_schedule)").fetchall()]
    for row in rows:
        old_id = row["id"] if "id" in row.keys() else None
        if old_id is None:
            continue
        exists = conn.execute("""
            SELECT id FROM schedule_assignments
            WHERE source_type='legacy_manual_schedule' AND source_reference_id=?
        """, (old_id,)).fetchone()
        if exists:
            continue
        assigned_technicians = row["assigned_technicians"] if "assigned_technicians" in old_cols else ""
        mech = int(row["mechanical_manpower"]) if "mechanical_manpower" in old_cols and row["mechanical_manpower"] is not None else 0
        weld = int(row["welding_manpower"]) if "welding_manpower" in old_cols and row["welding_manpower"] is not None else 0
        crew = mech + weld
        if crew < 1:
            crew = max(1, len([x for x in str(assigned_technicians).split(",") if x.strip()]))
        team_label = f"Team {old_id}"
        priority = row["priority"] if "priority" in old_cols else 10
        status = row["status"] if "status" in old_cols and row["status"] else "Scheduled"
        conn.execute("""
            INSERT INTO schedule_assignments (
                job_id, source_type, source_reference_id, schedule_state, day, team_label,
                assigned_technicians, assigned_hours, required_crew_size,
                mechanical_manpower, welding_manpower, priority_class, priority_score,
                location, department, notes, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            None,
            "legacy_manual_schedule",
            old_id,
            "Draft",
            row["day"] if "day" in old_cols else "Friday",
            team_label,
            assigned_technicians,
            float(row["hours"] if "hours" in old_cols and row["hours"] is not None else 1),
            crew,
            mech,
            weld,
            normalize_priority_class(priority),
            normalize_priority_score(priority),
            row["location"] if "location" in old_cols else "",
            row["department"] if "department" in old_cols else "",
            "Migrated from V12 manual_schedule",
            "Complete" if str(status).strip().lower() == "complete" else "Scheduled",
        ))


initialize_database()

# ---------------------------------------------------------
# DATA ACCESS
# ---------------------------------------------------------
def fetch_all_jobs(status: Optional[str] = None):
    with get_connection() as conn:
        if status:
            return conn.execute("""
                SELECT * FROM jobs
                WHERE status = ?
                ORDER BY
                    CASE priority_class
                        WHEN 'Emergency' THEN 1
                        WHEN 'Urgent' THEN 2
                        WHEN 'High' THEN 3
                        WHEN 'Medium' THEN 4
                        WHEN 'Low' THEN 5
                        WHEN 'Opportunity / Shutdown' THEN 6
                        ELSE 99
                    END,
                    priority_score DESC,
                    id ASC
            """, (status,)).fetchall()
        return conn.execute("""
            SELECT * FROM jobs
            ORDER BY
                CASE priority_class
                    WHEN 'Emergency' THEN 1
                    WHEN 'Urgent' THEN 2
                    WHEN 'High' THEN 3
                    WHEN 'Medium' THEN 4
                    WHEN 'Low' THEN 5
                    WHEN 'Opportunity / Shutdown' THEN 6
                    ELSE 99
                END,
                priority_score DESC,
                id ASC
        """).fetchall()


def fetch_all_technicians(active_only=True):
    with get_connection() as conn:
        if active_only:
            return conn.execute("SELECT * FROM technicians WHERE active=1 ORDER BY technician").fetchall()
        return conn.execute("SELECT * FROM technicians ORDER BY technician").fetchall()


def insert_job_v13(**kwargs):
    with get_connection() as conn:
        cur = conn.execute("""
            INSERT INTO jobs (
                job, location, department, duration_hours,
                mechanical_manpower, welding_manpower, crew_size_required,
                priority_class, priority_score, allowed_days, preferred_day,
                earliest_start_day, latest_finish_day, weekend_allowed,
                requires_shutdown, fixed_day_job, can_split_across_days,
                status, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            kwargs["job"].strip(),
            kwargs.get("location", "").strip(),
            kwargs.get("department", "").strip(),
            float(kwargs.get("duration_hours", 1.0)),
            int(kwargs.get("mechanical_manpower", 0)),
            int(kwargs.get("welding_manpower", 0)),
            int(kwargs.get("crew_size_required", 1)),
            kwargs.get("priority_class", "Medium"),
            int(kwargs.get("priority_score", 8)),
            kwargs.get("allowed_days", default_allowed_days(0)),
            kwargs.get("preferred_day"),
            kwargs.get("earliest_start_day"),
            kwargs.get("latest_finish_day"),
            int(kwargs.get("weekend_allowed", 0)),
            int(kwargs.get("requires_shutdown", 0)),
            int(kwargs.get("fixed_day_job", 0)),
            int(kwargs.get("can_split_across_days", 1)),
            kwargs.get("status", "Pending"),
            kwargs.get("notes", "").strip(),
        ))
        return cur.lastrowid


def update_job_v13(job_id, **kwargs):
    with get_connection() as conn:
        conn.execute("""
            UPDATE jobs
            SET job=?, location=?, department=?, duration_hours=?,
                mechanical_manpower=?, welding_manpower=?, crew_size_required=?,
                priority_class=?, priority_score=?, allowed_days=?, preferred_day=?,
                earliest_start_day=?, latest_finish_day=?, weekend_allowed=?,
                requires_shutdown=?, fixed_day_job=?, can_split_across_days=?,
                status=?, notes=?
            WHERE id=?
        """, (
            kwargs["job"].strip(),
            kwargs.get("location", "").strip(),
            kwargs.get("department", "").strip(),
            float(kwargs.get("duration_hours", 1.0)),
            int(kwargs.get("mechanical_manpower", 0)),
            int(kwargs.get("welding_manpower", 0)),
            int(kwargs.get("crew_size_required", 1)),
            kwargs.get("priority_class", "Medium"),
            int(kwargs.get("priority_score", 8)),
            kwargs.get("allowed_days", default_allowed_days(0)),
            kwargs.get("preferred_day"),
            kwargs.get("earliest_start_day"),
            kwargs.get("latest_finish_day"),
            int(kwargs.get("weekend_allowed", 0)),
            int(kwargs.get("requires_shutdown", 0)),
            int(kwargs.get("fixed_day_job", 0)),
            int(kwargs.get("can_split_across_days", 1)),
            kwargs.get("status", "Pending"),
            kwargs.get("notes", "").strip(),
            int(job_id),
        ))


def fetch_schedule_rows(schedule_state="Draft"):
    with get_connection() as conn:
        return conn.execute("""
            SELECT
                sa.*,
                j.job,
                j.duration_hours,
                j.allowed_days,
                j.preferred_day,
                j.weekend_allowed,
                j.requires_shutdown,
                j.fixed_day_job,
                j.can_split_across_days,
                j.status AS job_status
            FROM schedule_assignments sa
            LEFT JOIN jobs j ON sa.job_id = j.id
            WHERE sa.schedule_state = ?
            ORDER BY
                CASE sa.day
                    WHEN 'Monday' THEN 1
                    WHEN 'Tuesday' THEN 2
                    WHEN 'Wednesday' THEN 3
                    WHEN 'Thursday' THEN 4
                    WHEN 'Friday' THEN 5
                    WHEN 'Saturday' THEN 6
                    WHEN 'Sunday' THEN 7
                    ELSE 8
                END,
                sa.priority_score DESC,
                sa.id ASC
        """, (schedule_state,)).fetchall()


def insert_schedule_assignment(**kwargs):
    with get_connection() as conn:
        cur = conn.execute("""
            INSERT INTO schedule_assignments (
                job_id, source_type, source_reference_id, schedule_state, day,
                team_label, assigned_technicians, assigned_hours, required_crew_size,
                mechanical_manpower, welding_manpower, priority_class, priority_score,
                location, department, notes, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            kwargs.get("job_id"),
            kwargs.get("source_type", "job"),
            kwargs.get("source_reference_id"),
            kwargs.get("schedule_state", "Draft"),
            kwargs["day"],
            kwargs.get("team_label"),
            kwargs.get("assigned_technicians", ""),
            float(kwargs.get("assigned_hours", 1)),
            int(kwargs.get("required_crew_size", 1)),
            int(kwargs.get("mechanical_manpower", 0)),
            int(kwargs.get("welding_manpower", 0)),
            kwargs.get("priority_class", "Medium"),
            int(kwargs.get("priority_score", 8)),
            kwargs.get("location", ""),
            kwargs.get("department", ""),
            kwargs.get("notes", ""),
            kwargs.get("status", "Scheduled"),
        ))
        return cur.lastrowid


def clear_draft_schedule():
    with get_connection() as conn:
        conn.execute("DELETE FROM schedule_assignments WHERE schedule_state='Draft'")


def update_assignment(assignment_id, **kwargs):
    with get_connection() as conn:
        old = conn.execute("SELECT * FROM schedule_assignments WHERE id=?", (int(assignment_id),)).fetchone()
        conn.execute("""
            UPDATE schedule_assignments
            SET day=?, team_label=?, assigned_technicians=?, assigned_hours=?,
                priority_class=?, priority_score=?, notes=?, status=?,
                updated_at=CURRENT_TIMESTAMP
            WHERE id=?
        """, (
            kwargs["day"],
            kwargs["team_label"],
            kwargs["assigned_technicians"],
            float(kwargs["assigned_hours"]),
            kwargs["priority_class"],
            int(kwargs["priority_score"]),
            kwargs.get("notes", ""),
            kwargs.get("status", "Scheduled"),
            int(assignment_id),
        ))
        conn.execute("""
            INSERT INTO schedule_history (assignment_id, action_type, old_value, new_value)
            VALUES (?, 'UPDATE', ?, ?)
        """, (int(assignment_id), str(dict(old)) if old else "", str(kwargs)))


def delete_assignment(assignment_id):
    with get_connection() as conn:
        conn.execute("DELETE FROM schedule_assignments WHERE id=?", (int(assignment_id),))


def move_assignment_to_state(assignment_id, state):
    with get_connection() as conn:
        conn.execute("""
            UPDATE schedule_assignments
            SET schedule_state=?, updated_at=CURRENT_TIMESTAMP
            WHERE id=?
        """, (state, int(assignment_id)))


def promote_all_draft_to_final():
    with get_connection() as conn:
        conn.execute("""
            UPDATE schedule_assignments
            SET schedule_state='Final', updated_at=CURRENT_TIMESTAMP
            WHERE schedule_state='Draft'
        """)
        conn.execute("""
            UPDATE jobs
            SET status='Final Scheduled'
            WHERE id IN (
                SELECT DISTINCT job_id FROM schedule_assignments
                WHERE schedule_state='Final' AND job_id IS NOT NULL
            )
              AND status <> 'Complete'
        """)


def complete_assignment_and_job_if_finished(assignment_id, completion_note=""):
    with get_connection() as conn:
        assignment = conn.execute("SELECT * FROM schedule_assignments WHERE id=?", (int(assignment_id),)).fetchone()
        if not assignment:
            return
        note = completion_note.strip()
        existing = assignment["notes"] or ""
        if existing and note:
            merged_note = f"{existing} | {note}"
        elif existing:
            merged_note = existing
        else:
            merged_note = note
        conn.execute("""
            UPDATE schedule_assignments
            SET status='Complete', notes=?, updated_at=CURRENT_TIMESTAMP
            WHERE id=?
        """, (merged_note, int(assignment_id)))

        job_id = assignment["job_id"]
        if job_id is None:
            return
        incomplete = conn.execute("""
            SELECT COUNT(*) AS c
            FROM schedule_assignments
            WHERE job_id=? AND schedule_state='Final' AND status <> 'Complete'
        """, (int(job_id),)).fetchone()["c"]
        if int(incomplete) == 0:
            conn.execute("""
                UPDATE jobs
                SET status='Complete', completed_at=COALESCE(completed_at, CURRENT_TIMESTAMP)
                WHERE id=?
            """, (int(job_id),))
        else:
            conn.execute("""
                UPDATE jobs
                SET status='Active'
                WHERE id=? AND status <> 'Complete'
            """, (int(job_id),))


def reopen_completed_job(job_id):
    with get_connection() as conn:
        conn.execute("""
            UPDATE jobs
            SET status='Active', completed_at=NULL
            WHERE id=?
        """, (int(job_id),))
        conn.execute("""
            UPDATE schedule_assignments
            SET status='Scheduled', updated_at=CURRENT_TIMESTAMP
            WHERE job_id=? AND status='Complete'
        """, (int(job_id),))


def fetch_completed_jobs():
    with get_connection() as conn:
        return conn.execute("""
            SELECT * FROM jobs
            WHERE status='Complete'
            ORDER BY completed_at DESC, id DESC
        """).fetchall()


def fetch_completed_assignments():
    with get_connection() as conn:
        return conn.execute("""
            SELECT
                sa.*,
                j.job,
                j.completed_at
            FROM schedule_assignments sa
            LEFT JOIN jobs j ON sa.job_id = j.id
            WHERE sa.status='Complete' OR j.status='Complete'
            ORDER BY j.completed_at DESC, sa.updated_at DESC
        """).fetchall()


def mark_job_complete(job_id):
    with get_connection() as conn:
        conn.execute("""
            UPDATE jobs
            SET status='Complete', completed_at=COALESCE(completed_at, CURRENT_TIMESTAMP)
            WHERE id=?
        """, (int(job_id),))


# ---------------------------------------------------------
# TECHNICIAN HELPERS
# ---------------------------------------------------------
def get_technician_lookup(active_only=True):
    rows = fetch_all_technicians(active_only=active_only)
    return {r["technician"]: dict(r) for r in rows}


def save_technicians(df: pd.DataFrame):
    clean = df.copy()
    if "id" in clean.columns:
        clean = clean.drop(columns=["id"])
    required = ["technician", "skill", "weekly_hours", "active"]
    for col in required:
        if col not in clean.columns:
            raise ValueError(f"Missing column: {col}")
    clean["technician"] = clean["technician"].fillna("").astype(str).str.strip()
    clean["skill"] = clean["skill"].fillna("").astype(str).str.strip()
    clean["weekly_hours"] = pd.to_numeric(clean["weekly_hours"], errors="coerce").fillna(40.0)
    clean["active"] = pd.to_numeric(clean["active"], errors="coerce").fillna(1).astype(int)
    clean = clean[(clean["technician"] != "") & (clean["skill"] != "")].reset_index(drop=True)
    with get_connection() as conn:
        conn.execute("DELETE FROM technicians")
        for _, row in clean.iterrows():
            conn.execute("""
                INSERT INTO technicians (technician, skill, weekly_hours, active)
                VALUES (?, ?, ?, ?)
            """, (row["technician"], row["skill"], float(row["weekly_hours"]), int(row["active"])))


# ---------------------------------------------------------
# SCHEDULING LOGIC
# ---------------------------------------------------------
def get_existing_assignment_loads(include_draft=True, include_final=True):
    states = []
    if include_draft:
        states.append("Draft")
    if include_final:
        states.append("Final")
    tech_daily = {}
    tech_weekly = {}
    if not states:
        return tech_daily, tech_weekly
    placeholders = ",".join(["?"] * len(states))
    with get_connection() as conn:
        rows = conn.execute(f"""
            SELECT day, assigned_technicians, assigned_hours
            FROM schedule_assignments
            WHERE schedule_state IN ({placeholders})
        """, tuple(states)).fetchall()
    for row in rows:
        techs = [x.strip() for x in str(row["assigned_technicians"] or "").split(",") if x.strip()]
        hrs = float(row["assigned_hours"] or 0)
        day = row["day"]
        for tech in techs:
            tech_daily[(tech, day)] = tech_daily.get((tech, day), 0.0) + hrs
            tech_weekly[tech] = tech_weekly.get(tech, 0.0) + hrs
    return tech_daily, tech_weekly


def compute_remaining_job_hours(job_id):
    with get_connection() as conn:
        job = conn.execute("SELECT duration_hours FROM jobs WHERE id=?", (int(job_id),)).fetchone()
        scheduled = conn.execute("""
            SELECT COALESCE(SUM(assigned_hours), 0) AS total
            FROM schedule_assignments
            WHERE job_id=? AND schedule_state IN ('Draft', 'Final')
        """, (int(job_id),)).fetchone()
    total = float(job["duration_hours"] if job else 0)
    used = float(scheduled["total"] if scheduled else 0)
    return max(0.0, total - used)


def build_generated_team_label(counter):
    return f"Team {counter}"


def get_skill_eligible_technicians(job_row, technician_lookup):
    mech_needed = int(job_row.get("mechanical_manpower", 0) or 0)
    weld_needed = int(job_row.get("welding_manpower", 0) or 0)
    techs = list(technician_lookup.values())
    mechanics = [t for t in techs if str(t["skill"]).strip().lower() == "mechanical" and int(t["active"]) == 1]
    welders = [t for t in techs if str(t["skill"]).strip().lower() == "welding" and int(t["active"]) == 1]

    if mech_needed > 0 and weld_needed > 0:
        if len(mechanics) < mech_needed or len(welders) < weld_needed:
            return []
        return mechanics + welders
    if mech_needed > 0:
        return mechanics
    if weld_needed > 0:
        return welders
    return [t for t in techs if int(t["active"]) == 1]


def validate_skill_mix_for_assignment(assignment_row, selected_names, technician_lookup):
    warnings = []
    mech_needed = int(assignment_row.get("mechanical_manpower", 0) or 0)
    weld_needed = int(assignment_row.get("welding_manpower", 0) or 0)

    selected = [technician_lookup.get(name) for name in selected_names if name in technician_lookup]
    mech_count = sum(1 for t in selected if str(t["skill"]).strip().lower() == "mechanical")
    weld_count = sum(1 for t in selected if str(t["skill"]).strip().lower() == "welding")

    if mech_count < mech_needed:
        warnings.append(f"Need {mech_needed} mechanical technician(s), selected {mech_count}.")
    if weld_count < weld_needed:
        warnings.append(f"Need {weld_needed} welding technician(s), selected {weld_count}.")
    return warnings


def validate_assignment_row(assignment_row, technician_lookup, existing_daily=None, existing_weekly=None, day_limit=8.0, skip_assignment_id=None):
    warnings = []
    day = assignment_row.get("day")
    assigned_hours = float(assignment_row.get("assigned_hours", 0) or 0)
    required_crew = int(assignment_row.get("required_crew_size", 1) or 1)
    selected_names = [x.strip() for x in str(assignment_row.get("assigned_technicians", "")).split(",") if x.strip()]

    if len(selected_names) < required_crew:
        warnings.append(f"Assigned crew is short. Need {required_crew}, selected {len(selected_names)}.")

    existing_daily = existing_daily or {}
    existing_weekly = existing_weekly or {}
    for name in selected_names:
        if name not in technician_lookup:
            warnings.append(f"{name} is not an active technician.")
            continue
        tech = technician_lookup[name]
        current_day = float(existing_daily.get((name, day), 0.0))
        current_week = float(existing_weekly.get(name, 0.0))
        limit = float(tech["weekly_hours"] or 40)
        if current_day + assigned_hours > float(day_limit):
            warnings.append(f"{name} exceeds daily limit on {day}.")
        if current_week + assigned_hours > limit:
            warnings.append(f"{name} exceeds weekly limit.")
    warnings.extend(validate_skill_mix_for_assignment(assignment_row, selected_names, technician_lookup))
    return warnings


def generate_v14_draft_schedule(day_hours_limit=8.0, clear_existing=True):
    jobs_df = rows_to_df(fetch_all_jobs())
    tech_lookup = get_technician_lookup(active_only=True)

    if jobs_df.empty:
        return [], ["No jobs available to schedule."]
    if not tech_lookup:
        return [], ["No active technicians available."]

    if clear_existing:
        clear_draft_schedule()

    existing_daily, existing_weekly = get_existing_assignment_loads(include_draft=False, include_final=True)

    priority_order = {
        "Emergency": 1,
        "Urgent": 2,
        "High": 3,
        "Medium": 4,
        "Low": 5,
        "Opportunity / Shutdown": 6,
    }
    jobs_df["priority_rank"] = jobs_df["priority_class"].map(priority_order).fillna(99)
    jobs_df = jobs_df.sort_values(["priority_rank", "priority_score", "id"], ascending=[True, False, True]).reset_index(drop=True)

    notes = []
    generated = []
    team_counter = 1

    for _, job in jobs_df.iterrows():
        if str(job["status"]).strip().lower() == "complete":
            continue

        remaining_hours = float(job["duration_hours"] or 0)
        if remaining_hours <= 0:
            continue

        allowed_days = parse_allowed_days(job["allowed_days"])
        preferred_day = job["preferred_day"] if job["preferred_day"] in DAYS else None
        fixed_day = bool(job["fixed_day_job"])
        can_split = bool(job["can_split_across_days"])
        mech_needed = int(job["mechanical_manpower"] or 0)
        weld_needed = int(job["welding_manpower"] or 0)
        crew_required = int(job["crew_size_required"] or max(1, mech_needed + weld_needed))
        job_name = job["job"]

        eligible = get_skill_eligible_technicians(job, tech_lookup)
        if not eligible:
            notes.append(f"Job '{job_name}' could not be scheduled. No eligible technicians.")
            continue

        valid_days = allowed_days.copy()
        if preferred_day and preferred_day in valid_days:
            valid_days = [preferred_day] + [d for d in valid_days if d != preferred_day]
        if fixed_day:
            valid_days = [preferred_day] if preferred_day else [valid_days[0]]

        progress_made = False
        while remaining_hours > 0:
            placed = False
            for day in valid_days:
                mechanics = []
                welders = []
                others = []

                for tech in eligible:
                    name = tech["technician"]
                    skill = str(tech["skill"]).strip().lower()
                    weekly_limit = float(tech["weekly_hours"] or 40)
                    used_day = float(existing_daily.get((name, day), 0.0))
                    used_week = float(existing_weekly.get(name, 0.0))
                    remain_day = float(day_hours_limit) - used_day
                    remain_week = weekly_limit - used_week
                    if remain_day <= 0 or remain_week <= 0:
                        continue
                    record = {
                        "name": name,
                        "skill": skill,
                        "remain_day": remain_day,
                        "remain_week": remain_week,
                    }
                    if skill == "mechanical":
                        mechanics.append(record)
                    elif skill == "welding":
                        welders.append(record)
                    else:
                        others.append(record)

                mechanics = sorted(mechanics, key=lambda x: (x["remain_week"], x["remain_day"]), reverse=True)
                welders = sorted(welders, key=lambda x: (x["remain_week"], x["remain_day"]), reverse=True)
                others = sorted(others, key=lambda x: (x["remain_week"], x["remain_day"]), reverse=True)

                selected = []
                if mech_needed > 0:
                    if len(mechanics) < mech_needed:
                        continue
                    selected.extend(mechanics[:mech_needed])

                if weld_needed > 0:
                    if len(welders) < weld_needed:
                        continue
                    selected.extend(welders[:weld_needed])

                if len(selected) < crew_required:
                    pool = [r for r in (mechanics + welders + others) if r["name"] not in {s["name"] for s in selected}]
                    extra_needed = crew_required - len(selected)
                    if len(pool) < extra_needed:
                        continue
                    selected.extend(pool[:extra_needed])

                if not selected:
                    continue

                chunk = min(
                    [remaining_hours] +
                    [r["remain_day"] for r in selected] +
                    [r["remain_week"] for r in selected]
                )

                if chunk <= 0:
                    continue

                team_label = build_generated_team_label(team_counter)
                team_counter += 1

                selected_names = [r["name"] for r in selected]
                insert_schedule_assignment(
                    job_id=int(job["id"]),
                    source_type="job",
                    source_reference_id=int(job["id"]),
                    schedule_state="Draft",
                    day=day,
                    team_label=team_label,
                    assigned_technicians=", ".join(selected_names),
                    assigned_hours=round(float(chunk), 2),
                    required_crew_size=crew_required,
                    mechanical_manpower=mech_needed,
                    welding_manpower=weld_needed,
                    priority_class=job["priority_class"],
                    priority_score=int(job["priority_score"] or 8),
                    location=job["location"] or "",
                    department=job["department"] or "",
                    notes="Auto-generated draft schedule",
                    status="Scheduled",
                )

                for name in selected_names:
                    existing_daily[(name, day)] = existing_daily.get((name, day), 0.0) + chunk
                    existing_weekly[name] = existing_weekly.get(name, 0.0) + chunk

                generated.append({
                    "job_id": int(job["id"]),
                    "job": job_name,
                    "day": day,
                    "team_label": team_label,
                    "assigned_technicians": ", ".join(selected_names),
                    "assigned_hours": round(float(chunk), 2),
                })

                remaining_hours -= chunk
                progress_made = True
                placed = True

                if not can_split and remaining_hours > 0:
                    notes.append(f"Job '{job_name}' could not be fully scheduled because splitting across days is disabled.")
                    remaining_hours = 0
                    break

                if remaining_hours <= 0:
                    break

            if not placed:
                break

        if remaining_hours > 0:
            notes.append(f"Job '{job_name}' was only partially scheduled. {round(remaining_hours, 2)} hour(s) remain unscheduled.")
        elif progress_made:
            pass

    return generated, notes


# ---------------------------------------------------------
# EXPORT HELPERS
# ---------------------------------------------------------
def safe_sheet_name(name: str) -> str:
    invalid = ['\\', '/', '*', '?', ':', '[', ']']
    clean = str(name)
    for ch in invalid:
        clean = clean.replace(ch, "_")
    return clean[:31] if clean else "Sheet"


def build_simple_export(df, sheet_name="Export"):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=safe_sheet_name(sheet_name))
    output.seek(0)
    return output.getvalue()



def build_job_import_template_bytes():
    template_df = pd.DataFrame([
        {
            "Job": "Replace pump seal",
            "Location": "Pump P-101",
            "Department": "Utilities",
            "Duration Hours": 6,
            "Mechanical Manpower": 2,
            "Welding Manpower": 0,
            "Priority": 11,
            "Crew Size": 2,
            "Allowed Days": "Monday,Tuesday,Wednesday,Thursday,Friday",
            "Preferred Day": "Tuesday",
            "Notes": "Seal kit available"
        },
        {
            "Job": "Repair support bracket",
            "Location": "Line 2",
            "Department": "Production",
            "Duration Hours": 4,
            "Mechanical Manpower": 0,
            "Welding Manpower": 2,
            "Priority": 15,
            "Crew Size": 2,
            "Allowed Days": "Friday,Saturday,Sunday",
            "Preferred Day": "Saturday",
            "Notes": "Shutdown window required"
        },
    ])

    instructions_df = pd.DataFrame({
        "Field": [
            "Job",
            "Location",
            "Department",
            "Duration Hours",
            "Mechanical Manpower",
            "Welding Manpower",
            "Priority",
            "Crew Size",
            "Allowed Days",
            "Preferred Day",
            "Notes",
        ],
        "Required": [
            "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Optional", "Optional", "Optional", "Optional"
        ],
        "Description": [
            "Job title",
            "Asset, equipment, or work location",
            "Department or area",
            "Clock hours required for the job",
            "Number of mechanical technicians required",
            "Number of welding technicians required",
            "Priority score from 1 to 20; higher score means higher urgency",
            "If blank, app uses Mechanical Manpower + Welding Manpower",
            "Comma-separated days, e.g. Monday,Tuesday,Wednesday",
            "Single preferred day from Monday to Sunday",
            "Free text planner notes",
        ]
    })

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        template_df.to_excel(writer, index=False, sheet_name="Jobs Template")
        instructions_df.to_excel(writer, index=False, sheet_name="Instructions")
    output.seek(0)
    return output.getvalue()


def import_jobs_v14(import_df: pd.DataFrame):
    df = import_df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    required = [
        "Job",
        "Location",
        "Department",
        "Duration Hours",
        "Mechanical Manpower",
        "Welding Manpower",
        "Priority",
    ]

    missing = [c for c in required if c not in df.columns]
    if missing:
        return 0, f"Missing columns: {', '.join(missing)}"

    inserted = 0

    for _, row in df.iterrows():
        job = str(row.get("Job", "")).strip()
        if not job or job.lower() == 'nan':
            continue

        try:
            location = str(row.get("Location", "")).strip()
            department = str(row.get("Department", "")).strip()
            duration = float(pd.to_numeric(row.get("Duration Hours", 0), errors="coerce"))
            mech = int(pd.to_numeric(row.get("Mechanical Manpower", 0), errors="coerce") or 0)
            weld = int(pd.to_numeric(row.get("Welding Manpower", 0), errors="coerce") or 0)
            priority_score = int(pd.to_numeric(row.get("Priority", 7), errors="coerce") or 7)
        except Exception as e:
            return inserted, f"Could not parse one or more values on row for job '{job}': {e}"

        if duration <= 0:
            continue
        if mech == 0 and weld == 0:
            continue

        crew_raw = row.get("Crew Size", mech + weld)
        try:
            crew = int(pd.to_numeric(crew_raw, errors="coerce") or (mech + weld))
        except Exception:
            crew = mech + weld
        crew = max(crew, mech + weld, 1)

        allowed_days_raw = row.get("Allowed Days", default_allowed_days(0))
        allowed_days = str(allowed_days_raw).strip() if pd.notna(allowed_days_raw) and str(allowed_days_raw).strip() else default_allowed_days(0)

        preferred_day_raw = row.get("Preferred Day", None)
        preferred_day = None
        if pd.notna(preferred_day_raw):
            preferred_day = str(preferred_day_raw).strip() or None
            if preferred_day not in DAYS:
                preferred_day = None

        notes_raw = row.get("Notes", "")
        notes = "" if pd.isna(notes_raw) else str(notes_raw)

        insert_job_v13(
            job=job,
            location=location,
            department=department,
            duration_hours=duration,
            mechanical_manpower=mech,
            welding_manpower=weld,
            crew_size_required=crew,
            priority_class=map_score_to_priority_class(priority_score),
            priority_score=priority_score,
            allowed_days=allowed_days,
            preferred_day=preferred_day,
            earliest_start_day=None,
            latest_finish_day=None,
            weekend_allowed=1 if any(d in allowed_days for d in ["Saturday", "Sunday"]) else 0,
            requires_shutdown=0,
            fixed_day_job=1 if preferred_day and allowed_days == preferred_day else 0,
            can_split_across_days=1,
            status="Pending",
            notes=notes,
        )
        inserted += 1

    return inserted, None


# ---------------------------------------------------------
# LOAD BASE DATA
# ---------------------------------------------------------
jobs_df = rows_to_df(fetch_all_jobs())
technicians_df = rows_to_df(fetch_all_technicians(active_only=False))
draft_df = rows_to_df(fetch_schedule_rows("Draft"))
final_df = rows_to_df(fetch_schedule_rows("Final"))
completed_jobs_df = rows_to_df(fetch_completed_jobs())
completed_assignments_df = rows_to_df(fetch_completed_assignments())

# ---------------------------------------------------------
# DASHBOARD
# ---------------------------------------------------------
pending_jobs_count = 0 if jobs_df.empty else int((jobs_df["status"] == "Pending").sum())
draft_jobs_count = 0 if jobs_df.empty else int((jobs_df["status"] == "Draft Scheduled").sum())
final_jobs_count = 0 if jobs_df.empty else int((jobs_df["status"] == "Final Scheduled").sum())
active_jobs_count = 0 if jobs_df.empty else int((jobs_df["status"] == "Active").sum())
completed_jobs_count = 0 if jobs_df.empty else int((jobs_df["status"] == "Complete").sum())
technician_count = 0 if technicians_df.empty else int((technicians_df["active"] == 1).sum() if "active" in technicians_df.columns else len(technicians_df))
draft_hours = 0 if draft_df.empty else round(float(draft_df["assigned_hours"].sum()), 2)
final_hours = 0 if final_df.empty else round(float(final_df["assigned_hours"].sum()), 2)

m1, m2, m3, m4, m5, m6, m7 = st.columns(7)
m1.metric("Pending Jobs", pending_jobs_count)
m2.metric("Draft Scheduled", draft_jobs_count)
m3.metric("Final Scheduled", final_jobs_count)
m4.metric("Active Jobs", active_jobs_count)
m5.metric("Completed Jobs", completed_jobs_count)
m6.metric("Active Technicians", technician_count)
m7.metric("Draft Hours", draft_hours)

st.divider()

# ---------------------------------------------------------
# TABS
# ---------------------------------------------------------
tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
    "Dashboard",
    "Jobs / Backlog",
    "Draft Schedule",
    "Technicians",
    "Generated Crews",
    "Final Schedule",
    "History / Completed",
    "Import / Export",
])

# ---------------------------------------------------------
# DASHBOARD TAB
# ---------------------------------------------------------
with tab1:
    st.subheader("Dashboard")

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("##### Work Summary")
        st.write(f"Draft schedule rows: **{0 if draft_df.empty else len(draft_df)}**")
        st.write(f"Final schedule rows: **{0 if final_df.empty else len(final_df)}**")
        st.write(f"Completed assignment rows: **{0 if completed_assignments_df.empty else len(completed_assignments_df)}**")
    with c2:
        st.markdown("##### Hours Summary")
        st.write(f"Draft hours: **{draft_hours}**")
        st.write(f"Final hours: **{final_hours}**")

    if not jobs_df.empty:
        st.markdown("##### Jobs by Priority")
        priority_summary = jobs_df.groupby("priority_class", as_index=False).agg(Jobs=("id", "count"))
        st.dataframe(priority_summary, use_container_width=True)

    if not technicians_df.empty:
        st.markdown("##### Technicians")
        st.dataframe(technicians_df, use_container_width=True)

# ---------------------------------------------------------
# JOBS / BACKLOG TAB
# ---------------------------------------------------------
with tab2:
    st.subheader("Jobs / Backlog")

    with st.expander("Add New Job", expanded=False):
        with st.form("v14_add_job_form", clear_on_submit=True):
            c1, c2 = st.columns(2)
            job_name = c1.text_input("Job Name")
            location = c2.text_input("Location / Equipment")
            department = c1.text_input("Department")
            duration_hours = c2.number_input("Duration Hours", min_value=0.5, value=1.0, step=0.5)

            mech_mp = c1.number_input("Mechanical Manpower", min_value=0, value=1, step=1)
            weld_mp = c2.number_input("Welding Manpower", min_value=0, value=0, step=1)
            crew_size_required = st.number_input("Crew Size Required", min_value=1, value=1, step=1)

            st.markdown("##### Priority")
            priority_mode = st.radio("Priority Entry Method", ["Quick Select", "Score Based"], horizontal=True)

            if priority_mode == "Quick Select":
                priority_class = st.selectbox("Priority Class", PRIORITY_CLASSES, index=3)
                default_score_map = {
                    "Emergency": 18,
                    "Urgent": 15,
                    "High": 11,
                    "Medium": 7,
                    "Low": 3,
                    "Opportunity / Shutdown": 6,
                }
                priority_score = default_score_map[priority_class]
                st.caption(f"Priority Score will be saved as {priority_score}.")
            else:
                p1, p2, p3, p4 = st.columns(4)
                safety = p1.selectbox("Safety Risk", [1, 2, 3, 4, 5], index=0)
                production = p2.selectbox("Production Impact", [1, 2, 3, 4, 5], index=0)
                criticality = p3.selectbox("Asset Criticality", [1, 2, 3, 4, 5], index=0)
                delay_risk = p4.selectbox("Delay Risk", [1, 2, 3, 4, 5], index=0)
                priority_score = calculate_priority_score(safety, production, criticality, delay_risk)
                priority_class = map_score_to_priority_class(priority_score)
                st.info(f"Calculated Priority: {priority_class} | Score: {priority_score}")

            st.markdown("##### Scheduling Rules")
            allowed_days = st.multiselect("Allowed Days", DAYS, default=DAYS[:5])
            preferred_day = st.selectbox("Preferred Day", [""] + DAYS, index=0)
            earliest_start_day = st.selectbox("Earliest Start Day", [""] + DAYS, index=0)
            latest_finish_day = st.selectbox("Latest Finish Day", [""] + DAYS, index=0)

            cb1, cb2, cb3, cb4 = st.columns(4)
            weekend_allowed = cb1.checkbox("Weekend Allowed", value=False)
            requires_shutdown = cb2.checkbox("Requires Shutdown", value=False)
            fixed_day_job = cb3.checkbox("Fixed Day Job", value=False)
            can_split_across_days = cb4.checkbox("Can Split Across Days", value=True)

            status = st.selectbox("Status", JOB_STATUS_OPTIONS, index=0)
            notes = st.text_area("Notes")

            submitted = st.form_submit_button("Save Job")
            if submitted:
                if not job_name.strip():
                    st.error("Job Name is required.")
                elif len(allowed_days) == 0:
                    st.error("Please select at least one allowed day.")
                elif mech_mp == 0 and weld_mp == 0:
                    st.error("At least one manpower value must be greater than zero.")
                elif crew_size_required < (int(mech_mp) + int(weld_mp)):
                    st.error("Crew Size Required cannot be less than total manpower needed.")
                else:
                    insert_job_v13(
                        job=job_name,
                        location=location,
                        department=department,
                        duration_hours=duration_hours,
                        mechanical_manpower=mech_mp,
                        welding_manpower=weld_mp,
                        crew_size_required=crew_size_required,
                        priority_class=priority_class,
                        priority_score=priority_score,
                        allowed_days=normalize_allowed_days(allowed_days),
                        preferred_day=preferred_day or None,
                        earliest_start_day=earliest_start_day or None,
                        latest_finish_day=latest_finish_day or None,
                        weekend_allowed=1 if weekend_allowed else 0,
                        requires_shutdown=1 if requires_shutdown else 0,
                        fixed_day_job=1 if fixed_day_job else 0,
                        can_split_across_days=1 if can_split_across_days else 0,
                        status=status,
                        notes=notes,
                    )
                    st.success("Job saved.")
                    st.rerun()

    st.markdown("##### Backlog / Job Register")

    if jobs_df.empty:
        st.info("No jobs found.")
    else:
        remaining_rows = []
        for _, row in jobs_df.iterrows():
            remaining_rows.append({
                "id": row["id"],
                "remaining_hours": round(compute_remaining_job_hours(int(row["id"])), 2)
            })
        remaining_df = pd.DataFrame(remaining_rows)
        display_jobs = jobs_df.merge(remaining_df, on="id", how="left")

        f1, f2, f3 = st.columns(3)
        status_filter = f1.selectbox("Filter by Status", ["All"] + JOB_STATUS_OPTIONS)
        priority_filter = f2.selectbox("Filter by Priority Class", ["All"] + PRIORITY_CLASSES)
        dept_filter = f3.text_input("Filter by Department")

        if status_filter != "All":
            display_jobs = display_jobs[display_jobs["status"] == status_filter]
        if priority_filter != "All":
            display_jobs = display_jobs[display_jobs["priority_class"] == priority_filter]
        if dept_filter.strip():
            display_jobs = display_jobs[
                display_jobs["department"].fillna("").str.contains(dept_filter.strip(), case=False, na=False)
            ]

        st.dataframe(display_jobs[[
            "id", "job", "location", "department", "duration_hours",
            "mechanical_manpower", "welding_manpower", "crew_size_required",
            "priority_class", "priority_score", "allowed_days",
            "preferred_day", "status", "remaining_hours", "notes"
        ]], use_container_width=True)

        st.markdown("##### Quick Table Edit")
        editable_jobs_df = display_jobs[[
            "id", "job", "location", "department", "duration_hours",
            "mechanical_manpower", "welding_manpower", "crew_size_required",
            "priority_class", "priority_score", "allowed_days",
            "preferred_day", "status", "notes"
        ]].copy()

        editable_jobs_df = st.data_editor(
            editable_jobs_df,
            use_container_width=True,
            num_rows="fixed",
            key="jobs_quick_table_editor_v14",
            column_config={
                "id": st.column_config.NumberColumn("ID", disabled=True),
                "job": st.column_config.TextColumn("Job"),
                "location": st.column_config.TextColumn("Location"),
                "department": st.column_config.TextColumn("Department"),
                "duration_hours": st.column_config.NumberColumn("Duration Hours", min_value=0.5, step=0.5),
                "mechanical_manpower": st.column_config.NumberColumn("Mechanical Manpower", min_value=0, step=1),
                "welding_manpower": st.column_config.NumberColumn("Welding Manpower", min_value=0, step=1),
                "crew_size_required": st.column_config.NumberColumn("Crew Size Required", min_value=1, step=1),
                "priority_class": st.column_config.SelectboxColumn("Priority Class", options=PRIORITY_CLASSES),
                "priority_score": st.column_config.NumberColumn("Priority Score", min_value=1, max_value=20, step=1),
                "allowed_days": st.column_config.TextColumn("Allowed Days"),
                "preferred_day": st.column_config.SelectboxColumn("Preferred Day", options=[""] + DAYS),
                "status": st.column_config.SelectboxColumn("Status", options=JOB_STATUS_OPTIONS),
                "notes": st.column_config.TextColumn("Notes"),
            }
        )

        if st.button("Save Job Table Changes", use_container_width=True, key="save_job_table_changes_v14"):
            job_errors = []
            saved = 0
            for _, row in editable_jobs_df.iterrows():
                try:
                    job_id = int(row["id"])
                    job_name = str(row["job"] or "").strip()
                    location_val = str(row["location"] or "").strip()
                    dept_val = str(row["department"] or "").strip()
                    duration_val = float(row["duration_hours"] or 0)
                    mech_val = int(row["mechanical_manpower"] or 0)
                    weld_val = int(row["welding_manpower"] or 0)
                    crew_val = int(row["crew_size_required"] or 1)
                    priority_class_val = str(row["priority_class"] or "Medium")
                    priority_score_val = int(row["priority_score"] or 8)
                    allowed_days_val = str(row["allowed_days"] or "").strip()
                    preferred_day_val = str(row["preferred_day"] or "").strip()
                    status_val = str(row["status"] or "Pending")
                    notes_val = str(row["notes"] or "")

                    parsed_days = [d.strip() for d in allowed_days_val.split(",") if d.strip()]
                    parsed_days = [d for d in parsed_days if d in DAYS]

                    if not job_name:
                        job_errors.append(f"Job ID {job_id}: Job name is required.")
                        continue
                    if duration_val <= 0:
                        job_errors.append(f"Job ID {job_id}: Duration Hours must be greater than 0.")
                        continue
                    if mech_val == 0 and weld_val == 0:
                        job_errors.append(f"Job ID {job_id}: At least one manpower value must be greater than 0.")
                        continue
                    if crew_val < (mech_val + weld_val):
                        job_errors.append(f"Job ID {job_id}: Crew Size Required cannot be less than total manpower needed.")
                        continue
                    if not parsed_days:
                        parsed_days = DAYS[:5]
                    if preferred_day_val and preferred_day_val not in DAYS:
                        preferred_day_val = None
                    elif preferred_day_val == "":
                        preferred_day_val = None

                    update_job_v13(
                        job_id,
                        job=job_name,
                        location=location_val,
                        department=dept_val,
                        duration_hours=duration_val,
                        mechanical_manpower=mech_val,
                        welding_manpower=weld_val,
                        crew_size_required=crew_val,
                        priority_class=priority_class_val,
                        priority_score=priority_score_val,
                        allowed_days=",".join(parsed_days),
                        preferred_day=preferred_day_val,
                        earliest_start_day=None,
                        latest_finish_day=None,
                        weekend_allowed=1 if any(d in ["Saturday", "Sunday"] for d in parsed_days) else 0,
                        requires_shutdown=0,
                        fixed_day_job=0,
                        can_split_across_days=1,
                        status=status_val,
                        notes=notes_val,
                    )
                    saved += 1
                except Exception as e:
                    job_errors.append(f"Job ID {row.get('id', 'Unknown')}: {e}")

            if job_errors:
                st.error("Some job rows could not be saved.")
                for err in job_errors[:20]:
                    st.write(f"- {err}")
            if saved:
                st.success(f"Saved {saved} job row(s).")
                st.rerun()

        job_options = {f"{row['id']} - {row['job']}": int(row["id"]) for _, row in display_jobs.iterrows()}
        if job_options:
            selected_label = st.selectbox("Select Job to Edit", list(job_options.keys()))
            selected_id = job_options[selected_label]
            selected_job = jobs_df[jobs_df["id"] == selected_id].iloc[0]

            with st.expander("Edit Selected Job", expanded=False):
                with st.form("edit_job_form_v14"):
                    e1, e2 = st.columns(2)
                    edit_job_name = e1.text_input("Job Name", value=selected_job["job"])
                    edit_location = e2.text_input("Location / Equipment", value=selected_job["location"] or "")
                    edit_department = e1.text_input("Department", value=selected_job["department"] or "")
                    edit_duration = e2.number_input("Duration Hours", min_value=0.5, value=float(selected_job["duration_hours"] or 1.0), step=0.5)
                    edit_mech = e1.number_input("Mechanical Manpower", min_value=0, value=int(selected_job["mechanical_manpower"] or 0), step=1)
                    edit_weld = e2.number_input("Welding Manpower", min_value=0, value=int(selected_job["welding_manpower"] or 0), step=1)
                    edit_crew = st.number_input("Crew Size Required", min_value=1, value=int(selected_job["crew_size_required"] or 1), step=1)
                    edit_priority_class = st.selectbox("Priority Class", PRIORITY_CLASSES, index=PRIORITY_CLASSES.index(selected_job["priority_class"]) if selected_job["priority_class"] in PRIORITY_CLASSES else 3)
                    edit_priority_score = st.number_input("Priority Score", min_value=1, max_value=20, value=int(selected_job["priority_score"] or 8), step=1)
                    current_days = parse_allowed_days(selected_job["allowed_days"])
                    edit_allowed_days = st.multiselect("Allowed Days", DAYS, default=current_days)
                    edit_preferred_day = st.selectbox("Preferred Day", [""] + DAYS, index=([""] + DAYS).index(selected_job["preferred_day"]) if selected_job["preferred_day"] in DAYS else 0)
                    edit_earliest = st.selectbox("Earliest Start Day", [""] + DAYS, index=([""] + DAYS).index(selected_job["earliest_start_day"]) if selected_job["earliest_start_day"] in DAYS else 0)
                    edit_latest = st.selectbox("Latest Finish Day", [""] + DAYS, index=([""] + DAYS).index(selected_job["latest_finish_day"]) if selected_job["latest_finish_day"] in DAYS else 0)
                    ec1, ec2, ec3, ec4 = st.columns(4)
                    edit_weekend = ec1.checkbox("Weekend Allowed", value=bool(selected_job["weekend_allowed"]))
                    edit_shutdown = ec2.checkbox("Requires Shutdown", value=bool(selected_job["requires_shutdown"]))
                    edit_fixed = ec3.checkbox("Fixed Day Job", value=bool(selected_job["fixed_day_job"]))
                    edit_split = ec4.checkbox("Can Split Across Days", value=bool(selected_job["can_split_across_days"]))
                    edit_status = st.selectbox("Status", JOB_STATUS_OPTIONS, index=JOB_STATUS_OPTIONS.index(selected_job["status"]) if selected_job["status"] in JOB_STATUS_OPTIONS else 0)
                    edit_notes = st.text_area("Notes", value=selected_job["notes"] or "")
                    save_edit = st.form_submit_button("Update Job")
                    if save_edit:
                        if not edit_job_name.strip():
                            st.error("Job Name is required.")
                        elif len(edit_allowed_days) == 0:
                            st.error("Please select at least one allowed day.")
                        elif edit_mech == 0 and edit_weld == 0:
                            st.error("At least one manpower value must be greater than zero.")
                        elif edit_crew < (int(edit_mech) + int(edit_weld)):
                            st.error("Crew Size Required cannot be less than total manpower needed.")
                        else:
                            update_job_v13(
                                selected_id,
                                job=edit_job_name,
                                location=edit_location,
                                department=edit_department,
                                duration_hours=edit_duration,
                                mechanical_manpower=edit_mech,
                                welding_manpower=edit_weld,
                                crew_size_required=edit_crew,
                                priority_class=edit_priority_class,
                                priority_score=edit_priority_score,
                                allowed_days=normalize_allowed_days(edit_allowed_days),
                                preferred_day=edit_preferred_day or None,
                                earliest_start_day=edit_earliest or None,
                                latest_finish_day=edit_latest or None,
                                weekend_allowed=1 if edit_weekend else 0,
                                requires_shutdown=1 if edit_shutdown else 0,
                                fixed_day_job=1 if edit_fixed else 0,
                                can_split_across_days=1 if edit_split else 0,
                                status=edit_status,
                                notes=edit_notes
                            )
                            st.success("Job updated.")
                            st.rerun()

# ---------------------------------------------------------
# DRAFT SCHEDULE TAB
# ---------------------------------------------------------
with tab3:
    st.subheader("Draft Schedule")

    current_draft_df = rows_to_df(fetch_schedule_rows("Draft"))
    technician_lookup = get_technician_lookup(active_only=True)

    st.markdown("##### Generate Draft Schedule")
    g1, g2 = st.columns(2)
    day_hours_limit = g1.number_input("Daily Hours Limit Per Technician", min_value=1.0, value=8.0, step=1.0)
    clear_old = g2.checkbox("Clear Existing Draft Before Generate", value=True)

    if st.button("Generate Draft Schedule", use_container_width=True):
        generated, notes = generate_v14_draft_schedule(day_hours_limit=float(day_hours_limit), clear_existing=clear_old)
        st.session_state["generation_notes"] = notes
        if generated:
            st.success(f"Generated {len(generated)} draft assignment row(s).")
        else:
            st.warning("No draft schedule rows were generated.")
        st.rerun()

    if "generation_notes" in st.session_state and st.session_state["generation_notes"]:
        with st.expander("Generation Notes / Warnings", expanded=False):
            for note in st.session_state["generation_notes"]:
                st.write(f"- {note}")

    # Manual draft assignment
    st.markdown("##### Add Manual Draft Assignment")
    manual_jobs_df = rows_to_df(fetch_all_jobs())
    tech_names = sorted(list(technician_lookup.keys()))
    if not manual_jobs_df.empty and tech_names:
        manual_job_options = {
            f"{row['id']} - {row['job']}": int(row["id"])
            for _, row in manual_jobs_df.iterrows()
            if str(row["status"]).strip().lower() != "complete"
        }

        with st.expander("Manual Draft Assignment", expanded=False):
            with st.form("manual_draft_assignment_form", clear_on_submit=True):
                md1, md2 = st.columns(2)
                sel_job_label = md1.selectbox("Job", list(manual_job_options.keys()))
                sel_day = md2.selectbox("Day", DAYS)
                selected_job_id = manual_job_options[sel_job_label]
                selected_job = manual_jobs_df[manual_jobs_df["id"] == selected_job_id].iloc[0]
                default_label = build_generated_team_label((0 if current_draft_df.empty else int(current_draft_df["id"].max())) + 1)
                team_label = md1.text_input("Generated Team Label", value=default_label)
                assigned_hours = md2.number_input("Assigned Hours", min_value=0.5, value=1.0, step=0.5)
                assigned_techs = st.multiselect("Assigned Technicians", tech_names)
                manual_notes = st.text_area("Notes")
                manual_status = st.selectbox("Status", ASSIGNMENT_STATUS_OPTIONS[:-1], index=0)
                save_manual = st.form_submit_button("Add Manual Draft Assignment")
                if save_manual:
                    if not assigned_techs:
                        st.error("Please select at least one technician.")
                    else:
                        insert_schedule_assignment(
                            job_id=int(selected_job["id"]),
                            source_type="job",
                            source_reference_id=int(selected_job["id"]),
                            schedule_state="Draft",
                            day=sel_day,
                            team_label=team_label.strip() or default_label,
                            assigned_technicians=", ".join(assigned_techs),
                            assigned_hours=float(assigned_hours),
                            required_crew_size=int(selected_job["crew_size_required"] or 1),
                            mechanical_manpower=int(selected_job["mechanical_manpower"] or 0),
                            welding_manpower=int(selected_job["welding_manpower"] or 0),
                            priority_class=selected_job["priority_class"],
                            priority_score=int(selected_job["priority_score"] or 8),
                            location=selected_job["location"] or "",
                            department=selected_job["department"] or "",
                            notes=manual_notes,
                            status=manual_status,
                        )
                        st.success("Manual draft assignment added.")
                        st.rerun()

    st.markdown("##### Current Draft Schedule")
    if current_draft_df.empty:
        st.info("No draft schedule rows yet.")
    else:
        sc1, sc2, sc3, sc4 = st.columns(4)
        sc1.metric("Draft Rows", len(current_draft_df))
        sc2.metric("Draft Jobs", int(current_draft_df["job_id"].nunique()) if "job_id" in current_draft_df.columns else 0)
        sc3.metric("Draft Hours", round(float(current_draft_df["assigned_hours"].sum()), 2))
        sc4.metric("Generated Teams", int(current_draft_df["team_label"].nunique()) if "team_label" in current_draft_df.columns else 0)

        st.dataframe(current_draft_df[[
            "id", "day", "job", "team_label", "assigned_technicians", "assigned_hours",
            "required_crew_size", "mechanical_manpower", "welding_manpower",
            "priority_class", "priority_score", "status", "notes"
        ]], use_container_width=True)

        st.markdown("##### Quick Table Edit")
        editable_draft_df = current_draft_df[[
            "id", "day", "job", "team_label", "assigned_technicians", "assigned_hours",
            "required_crew_size", "priority_class", "priority_score", "status", "notes"
        ]].copy()

        editable_draft_df = st.data_editor(
            editable_draft_df,
            use_container_width=True,
            num_rows="fixed",
            key="draft_quick_table_editor_v14",
            column_config={
                "id": st.column_config.NumberColumn("ID", disabled=True),
                "day": st.column_config.SelectboxColumn("Day", options=DAYS),
                "job": st.column_config.TextColumn("Job", disabled=True),
                "team_label": st.column_config.TextColumn("Team Label"),
                "assigned_technicians": st.column_config.TextColumn("Assigned Technicians"),
                "assigned_hours": st.column_config.NumberColumn("Assigned Hours", min_value=0.5, step=0.5),
                "required_crew_size": st.column_config.NumberColumn("Required Crew Size", min_value=1, step=1),
                "priority_class": st.column_config.SelectboxColumn("Priority Class", options=PRIORITY_CLASSES),
                "priority_score": st.column_config.NumberColumn("Priority Score", min_value=1, max_value=20, step=1),
                "status": st.column_config.SelectboxColumn("Status", options=["Scheduled", "In Progress", "Deferred", "Complete"]),
                "notes": st.column_config.TextColumn("Notes"),
            }
        )

        if st.button("Save Draft Table Changes", use_container_width=True, key="save_draft_table_changes_v14"):
            existing_daily, existing_weekly = get_existing_assignment_loads(include_draft=True, include_final=True)
            draft_errors = []
            saved = 0
            for _, row in editable_draft_df.iterrows():
                try:
                    assignment_id = int(row["id"])
                    temp_assignment = {
                        "day": str(row["day"] or "Monday"),
                        "assigned_hours": float(row["assigned_hours"] or 0),
                        "required_crew_size": int(row["required_crew_size"] or 1),
                        "assigned_technicians": str(row["assigned_technicians"] or ""),
                    }
                    warnings = validate_assignment_row(
                        temp_assignment,
                        technician_lookup,
                        existing_daily=existing_daily,
                        existing_weekly=existing_weekly,
                        day_limit=float(day_hours_limit),
                        skip_assignment_id=assignment_id,
                    )
                    if warnings:
                        draft_errors.append(f"Assignment ID {assignment_id}: " + " | ".join(warnings))
                        continue

                    update_assignment(
                        assignment_id,
                        day=str(row["day"] or "Monday"),
                        team_label=str(row["team_label"] or "").strip(),
                        assigned_technicians=str(row["assigned_technicians"] or "").strip(),
                        assigned_hours=float(row["assigned_hours"] or 1.0),
                        priority_class=str(row["priority_class"] or "Medium"),
                        priority_score=int(row["priority_score"] or 8),
                        notes=str(row["notes"] or ""),
                        status=str(row["status"] or "Scheduled"),
                    )
                    saved += 1
                except Exception as e:
                    draft_errors.append(f"Assignment ID {row.get('id', 'Unknown')}: {e}")

            if draft_errors:
                st.error("Some draft rows could not be saved.")
                for err in draft_errors[:20]:
                    st.write(f"- {err}")
            if saved:
                st.success(f"Saved {saved} draft row(s).")
                st.rerun()

        assignment_options = {f"{row['id']} - {row['day']} - {row['job']}": int(row["id"]) for _, row in current_draft_df.iterrows()}
        selected_label = st.selectbox("Select Draft Assignment", list(assignment_options.keys()), key="draft_select")
        selected_id = assignment_options[selected_label]
        selected_assignment = current_draft_df[current_draft_df["id"] == selected_id].iloc[0]
        current_selected_techs = [x.strip() for x in str(selected_assignment["assigned_technicians"] or "").split(",") if x.strip()]

        with st.form("edit_draft_assignment_form"):
            ed1, ed2 = st.columns(2)
            edit_day = ed1.selectbox("Day", DAYS, index=DAYS.index(selected_assignment["day"]) if selected_assignment["day"] in DAYS else 0)
            edit_team_label = ed2.text_input("Generated Team Label", value=selected_assignment["team_label"] or "")
            edit_techs = st.multiselect("Assigned Technicians", tech_names, default=[x for x in current_selected_techs if x in tech_names], key=f"draft_techs_{selected_id}")
            ed3, ed4 = st.columns(2)
            edit_hours = ed3.number_input("Assigned Hours", min_value=0.5, value=float(selected_assignment["assigned_hours"] or 1.0), step=0.5)
            edit_priority_class = ed4.selectbox("Priority Class", PRIORITY_CLASSES, index=PRIORITY_CLASSES.index(selected_assignment["priority_class"]) if selected_assignment["priority_class"] in PRIORITY_CLASSES else 3)
            edit_priority_score = ed3.number_input("Priority Score", min_value=1, max_value=20, value=int(selected_assignment["priority_score"] or 8), step=1)
            edit_status = ed4.selectbox("Status", ASSIGNMENT_STATUS_OPTIONS, index=ASSIGNMENT_STATUS_OPTIONS.index(selected_assignment["status"]) if selected_assignment["status"] in ASSIGNMENT_STATUS_OPTIONS else 0)
            edit_notes = st.text_area("Notes", value=selected_assignment["notes"] or "")

            existing_daily, existing_weekly = get_existing_assignment_loads(include_draft=False, include_final=True)
            temp_assignment = {
                "day": edit_day,
                "assigned_hours": edit_hours,
                "required_crew_size": int(selected_assignment["required_crew_size"] or 1),
                "assigned_technicians": ", ".join(edit_techs),
                "mechanical_manpower": int(selected_assignment["mechanical_manpower"] or 0),
                "welding_manpower": int(selected_assignment["welding_manpower"] or 0),
            }
            warnings = validate_assignment_row(temp_assignment, technician_lookup, existing_daily, existing_weekly, float(day_hours_limit))
            if warnings:
                st.warning("Validation warnings:")
                for w in warnings:
                    st.write(f"- {w}")

            ac1, ac2, ac3 = st.columns(3)
            save_edit = ac1.form_submit_button("Update Draft")
            delete_row = ac2.form_submit_button("Delete Draft Row")
            promote_row = ac3.form_submit_button("Promote to Final")

            if save_edit:
                if not edit_techs:
                    st.error("Please select at least one technician.")
                else:
                    update_assignment(
                        selected_id,
                        day=edit_day,
                        team_label=edit_team_label.strip() or selected_assignment["team_label"],
                        assigned_technicians=", ".join(edit_techs),
                        assigned_hours=edit_hours,
                        priority_class=edit_priority_class,
                        priority_score=edit_priority_score,
                        notes=edit_notes,
                        status=edit_status,
                    )
                    st.success("Draft assignment updated.")
                    st.rerun()

            if delete_row:
                delete_assignment(selected_id)
                st.success("Draft assignment deleted.")
                st.rerun()

            if promote_row:
                move_assignment_to_state(selected_id, "Final")
                with get_connection() as conn:
                    conn.execute("""
                        UPDATE jobs
                        SET status='Final Scheduled'
                        WHERE id=? AND status <> 'Complete'
                    """, (int(selected_assignment["job_id"]),))
                st.success("Draft assignment promoted to Final.")
                st.rerun()

        st.markdown("##### Remaining Hours by Job")
        rem_rows = []
        for job_id in sorted(current_draft_df["job_id"].dropna().unique().tolist()):
            row = current_draft_df[current_draft_df["job_id"] == job_id].iloc[0]
            rem_rows.append({"job_id": int(job_id), "job": row["job"], "remaining_hours": round(compute_remaining_job_hours(int(job_id)), 2)})
        st.dataframe(pd.DataFrame(rem_rows), use_container_width=True)

# ---------------------------------------------------------
# TECHNICIANS TAB
# ---------------------------------------------------------
with tab4:
    st.subheader("Technicians")
    tech_base_df = rows_to_df(fetch_all_technicians(active_only=False))
    if tech_base_df.empty:
        tech_base_df = pd.DataFrame(DEFAULT_TECHS)
    editable = st.data_editor(
        tech_base_df[["id", "technician", "skill", "weekly_hours", "active"]] if "id" in tech_base_df.columns else tech_base_df,
        num_rows="dynamic",
        use_container_width=True,
        key="tech_editor_v14"
    )
    tc1, tc2 = st.columns(2)
    if tc1.button("Save Technicians", use_container_width=True):
        try:
            save_technicians(editable)
            st.success("Technicians saved.")
            st.rerun()
        except Exception as e:
            st.error(f"Could not save technicians: {e}")
    if tc2.button("Reset Default Technicians", use_container_width=True):
        with get_connection() as conn:
            conn.execute("DELETE FROM technicians")
            conn.executemany("""
                INSERT INTO technicians (technician, skill, weekly_hours, active)
                VALUES (:technician, :skill, :weekly_hours, :active)
            """, DEFAULT_TECHS)
        st.success("Technicians reset.")
        st.rerun()

    draft_now = rows_to_df(fetch_schedule_rows("Draft"))
    final_now = rows_to_df(fetch_schedule_rows("Final"))
    all_sched = pd.concat([draft_now, final_now], ignore_index=True) if (not draft_now.empty or not final_now.empty) else pd.DataFrame()

    if not all_sched.empty:
        tech_work = []
        active_lookup = get_technician_lookup(active_only=False)
        for tech_name, tech in active_lookup.items():
            tdf = all_sched[all_sched["assigned_technicians"].fillna("").str.contains(fr"\b{tech_name}\b", regex=True)]
            tech_work.append({
                "Technician": tech_name,
                "Skill": tech["skill"],
                "Weekly Hours": tech["weekly_hours"],
                "Assigned Hours": round(float(tdf["assigned_hours"].sum()) if not tdf.empty else 0.0, 2),
                "Assignments": int(len(tdf)),
                "Days": int(tdf["day"].nunique()) if not tdf.empty else 0,
            })
        st.markdown("##### Technician Workload")
        st.dataframe(pd.DataFrame(tech_work), use_container_width=True)

# ---------------------------------------------------------
# GENERATED CREWS TAB
# ---------------------------------------------------------
with tab5:
    st.subheader("Generated Crews")
    crew_df = rows_to_df(fetch_schedule_rows("Draft"))
    if crew_df.empty:
        st.info("No generated crews yet. Generate a draft schedule first.")
    else:
        crew_view = crew_df[[
            "id", "job", "day", "team_label", "required_crew_size",
            "mechanical_manpower", "welding_manpower",
            "assigned_technicians", "assigned_hours", "status"
        ]].copy()
        st.dataframe(crew_view, use_container_width=True)
        st.markdown("##### Crew Summary")
        summary = crew_df.groupby("team_label", as_index=False).agg(
            Jobs=("job", "nunique"),
            Hours=("assigned_hours", "sum"),
            Day=("day", "first"),
            Crew_Required=("required_crew_size", "max"),
        )
        st.dataframe(summary, use_container_width=True)

# ---------------------------------------------------------
# FINAL SCHEDULE TAB
# ---------------------------------------------------------
with tab6:
    st.subheader("Final Schedule")
    current_final_df = rows_to_df(fetch_schedule_rows("Final"))
    current_draft_df = rows_to_df(fetch_schedule_rows("Draft"))
    technician_lookup = get_technician_lookup(active_only=True)
    tech_names = sorted(list(technician_lookup.keys()))

    a1, a2 = st.columns(2)
    if a1.button("Promote All Draft Rows to Final", use_container_width=True):
        if current_draft_df.empty:
            st.warning("There are no draft rows to promote.")
        else:
            promote_all_draft_to_final()
            st.success("All draft rows promoted to Final.")
            st.rerun()

    if a2.button("Refresh Final Schedule", use_container_width=True):
        st.rerun()

    if current_final_df.empty:
        st.info("No final schedule rows yet.")
    else:
        fc1, fc2, fc3, fc4 = st.columns(4)
        fc1.metric("Final Rows", len(current_final_df))
        fc2.metric("Final Jobs", int(current_final_df["job_id"].nunique()) if "job_id" in current_final_df.columns else 0)
        fc3.metric("Final Hours", round(float(current_final_df["assigned_hours"].sum()), 2))
        fc4.metric("Generated Teams", int(current_final_df["team_label"].nunique()) if "team_label" in current_final_df.columns else 0)

        st.dataframe(current_final_df[[
            "id", "day", "job", "team_label", "assigned_technicians",
            "assigned_hours", "required_crew_size", "priority_class",
            "priority_score", "status", "notes"
        ]], use_container_width=True)

        for day in DAYS:
            day_rows = current_final_df[current_final_df["day"] == day]
            with st.expander(day, expanded=(day == "Monday")):
                if day_rows.empty:
                    st.info(f"No final schedule rows for {day}.")
                else:
                    st.dataframe(day_rows[[
                        "id", "job", "team_label", "assigned_technicians",
                        "assigned_hours", "priority_class", "status", "notes"
                    ]], use_container_width=True)

        options = {f"{row['id']} - {row['day']} - {row['job']}": int(row["id"]) for _, row in current_final_df.iterrows()}
        selected_label = st.selectbox("Select Final Assignment", list(options.keys()), key="final_select")
        selected_id = options[selected_label]
        selected = current_final_df[current_final_df["id"] == selected_id].iloc[0]
        current_selected_techs = [x.strip() for x in str(selected["assigned_technicians"] or "").split(",") if x.strip()]

        with st.form("edit_final_assignment_form"):
            ef1, ef2 = st.columns(2)
            edit_day = ef1.selectbox("Day", DAYS, index=DAYS.index(selected["day"]) if selected["day"] in DAYS else 0)
            edit_team_label = ef2.text_input("Generated Team Label", value=selected["team_label"] or "")
            edit_techs = st.multiselect("Assigned Technicians", tech_names, default=[x for x in current_selected_techs if x in tech_names], key=f"final_techs_{selected_id}")
            ef3, ef4 = st.columns(2)
            edit_hours = ef3.number_input("Assigned Hours", min_value=0.5, value=float(selected["assigned_hours"] or 1.0), step=0.5)
            edit_priority_class = ef4.selectbox("Priority Class", PRIORITY_CLASSES, index=PRIORITY_CLASSES.index(selected["priority_class"]) if selected["priority_class"] in PRIORITY_CLASSES else 3)
            edit_priority_score = ef3.number_input("Priority Score", min_value=1, max_value=20, value=int(selected["priority_score"] or 8), step=1)
            edit_status = ef4.selectbox("Status", ASSIGNMENT_STATUS_OPTIONS, index=ASSIGNMENT_STATUS_OPTIONS.index(selected["status"]) if selected["status"] in ASSIGNMENT_STATUS_OPTIONS else 0)
            edit_notes = st.text_area("Notes", value=selected["notes"] or "")

            existing_daily, existing_weekly = get_existing_assignment_loads(include_draft=False, include_final=True)
            temp_assignment = {
                "day": edit_day,
                "assigned_hours": edit_hours,
                "required_crew_size": int(selected["required_crew_size"] or 1),
                "assigned_technicians": ", ".join(edit_techs),
                "mechanical_manpower": int(selected["mechanical_manpower"] or 0),
                "welding_manpower": int(selected["welding_manpower"] or 0),
            }
            warnings = validate_assignment_row(temp_assignment, technician_lookup, existing_daily, existing_weekly, 8.0)
            if warnings:
                st.warning("Validation warnings:")
                for w in warnings:
                    st.write(f"- {w}")

            ac1, ac2, ac3 = st.columns(3)
            save_edit = ac1.form_submit_button("Update Final Assignment")
            move_back = ac2.form_submit_button("Move Back to Draft")
            mark_complete = ac3.form_submit_button("Mark Assignment Complete")

            if save_edit:
                if not edit_techs:
                    st.error("Please select at least one technician.")
                else:
                    update_assignment(
                        selected_id,
                        day=edit_day,
                        team_label=edit_team_label.strip() or selected["team_label"],
                        assigned_technicians=", ".join(edit_techs),
                        assigned_hours=edit_hours,
                        priority_class=edit_priority_class,
                        priority_score=edit_priority_score,
                        notes=edit_notes,
                        status=edit_status,
                    )
                    st.success("Final assignment updated.")
                    st.rerun()

            if move_back:
                move_assignment_to_state(selected_id, "Draft")
                st.success("Final assignment moved back to Draft.")
                st.rerun()

            if mark_complete:
                complete_assignment_and_job_if_finished(selected_id, edit_notes)
                st.success("Assignment marked complete.")
                st.rerun()

# ---------------------------------------------------------
# HISTORY / COMPLETED TAB
# ---------------------------------------------------------
with tab7:
    st.subheader("History / Completed")
    completed_jobs_df = rows_to_df(fetch_completed_jobs())
    completed_assignments_df = rows_to_df(fetch_completed_assignments())

    hc1, hc2, hc3 = st.columns(3)
    hc1.metric("Completed Jobs", 0 if completed_jobs_df.empty else len(completed_jobs_df))
    hc2.metric("Completed Assignment Rows", 0 if completed_assignments_df.empty else len(completed_assignments_df))
    hc3.metric("Completed Hours", 0 if completed_assignments_df.empty else round(float(completed_assignments_df["assigned_hours"].fillna(0).sum()), 2))

    st.markdown("##### Completed Jobs")
    if completed_jobs_df.empty:
        st.info("No completed jobs yet.")
    else:
        cj1, cj2 = st.columns(2)
        dept_filter = cj1.text_input("Filter Completed Jobs by Department")
        priority_filter = cj2.selectbox("Filter Completed Jobs by Priority Class", ["All"] + PRIORITY_CLASSES)
        filtered_jobs = completed_jobs_df.copy()
        if dept_filter.strip():
            filtered_jobs = filtered_jobs[filtered_jobs["department"].fillna("").str.contains(dept_filter.strip(), case=False, na=False)]
        if priority_filter != "All":
            filtered_jobs = filtered_jobs[filtered_jobs["priority_class"] == priority_filter]
        st.dataframe(filtered_jobs[[
            "id", "job", "location", "department", "duration_hours", "priority_class",
            "priority_score", "status", "created_at", "completed_at", "notes"
        ]], use_container_width=True)

        options = {f"{row['id']} - {row['job']}": int(row["id"]) for _, row in filtered_jobs.iterrows()}
        if options:
            selected_label = st.selectbox("Select Completed Job to Reopen", list(options.keys()))
            if st.button("Reopen Selected Job", use_container_width=True):
                reopen_completed_job(options[selected_label])
                st.success("Completed job reopened.")
                st.rerun()

    st.markdown("##### Completed Assignment History")
    if completed_assignments_df.empty:
        st.info("No completed assignment history yet.")
    else:
        ca1, ca2 = st.columns(2)
        team_filter = ca1.text_input("Filter Completed Assignments by Team Label")
        day_filter = ca2.selectbox("Filter Completed Assignments by Day", ["All"] + DAYS)
        filtered_assignments = completed_assignments_df.copy()
        if team_filter.strip():
            filtered_assignments = filtered_assignments[filtered_assignments["team_label"].fillna("").str.contains(team_filter.strip(), case=False, na=False)]
        if day_filter != "All":
            filtered_assignments = filtered_assignments[filtered_assignments["day"] == day_filter]
        st.dataframe(filtered_assignments[[
            "id", "job", "day", "team_label", "assigned_technicians", "assigned_hours",
            "priority_class", "priority_score", "status", "updated_at", "completed_at", "notes"
        ]], use_container_width=True)

        if "team_label" in filtered_assignments.columns and not filtered_assignments.empty:
            summary = filtered_assignments.groupby("team_label", as_index=False).agg(
                Total_Hours=("assigned_hours", "sum"),
                Jobs=("job", "nunique"),
                Rows=("id", "count"),
            )
            st.markdown("##### Completed Hours by Generated Team")
            st.dataframe(summary, use_container_width=True)


# ---------------------------------------------------------
# IMPORT / EXPORT TAB
# ---------------------------------------------------------
with tab8:
    st.subheader("Import / Export")

    st.markdown("##### Import Jobs")
    template_bytes = build_job_import_template_bytes()
    st.download_button(
        "Download Job Import Template",
        data=template_bytes,
        file_name="Plant_Maintenance_Manager_V14_Job_Template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

    uploaded_file = st.file_uploader("Upload Job Excel", type=["xlsx"], key="v14_job_import_uploader")
    if uploaded_file is not None:
        try:
            import_preview_df = pd.read_excel(uploaded_file, engine="openpyxl")
            import_preview_df.columns = [str(c).strip() for c in import_preview_df.columns]
            st.write("Import Preview")
            st.dataframe(import_preview_df, use_container_width=True)

            if st.button("Import Jobs", use_container_width=True):
                inserted, error = import_jobs_v14(import_preview_df)
                if error:
                    st.error(error)
                else:
                    st.success(f"{inserted} job(s) imported.")
                    st.rerun()
        except Exception as e:
            st.error(f"Could not read Excel file: {e}")

    st.markdown("##### Export Data")

    ex1, ex2 = st.columns(2)
    ex3, ex4 = st.columns(2)

    jobs_export_df = jobs_df.copy() if not jobs_df.empty else pd.DataFrame()
    draft_export_df = draft_df.copy() if not draft_df.empty else pd.DataFrame()
    final_export_df = final_df.copy() if not final_df.empty else pd.DataFrame()
    completed_export_df = completed_jobs_df.copy() if not completed_jobs_df.empty else pd.DataFrame()

    ex1.download_button(
        "Export Jobs / Backlog",
        data=build_simple_export(jobs_export_df, "Jobs_Backlog"),
        file_name="Jobs_Backlog.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
    ex2.download_button(
        "Export Draft Schedule",
        data=build_simple_export(draft_export_df, "Draft_Schedule"),
        file_name="Draft_Schedule.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
    ex3.download_button(
        "Export Final Schedule",
        data=build_simple_export(final_export_df, "Final_Schedule"),
        file_name="Final_Schedule.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
    ex4.download_button(
        "Export Completed Jobs",
        data=build_simple_export(completed_export_df, "Completed_Jobs"),
        file_name="Completed_Jobs.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
