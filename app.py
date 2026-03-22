import streamlit as st
import pandas as pd
import sqlite3
from io import BytesIO
import plotly.express as px

st.set_page_config(page_title="Plant Maintenance Manager V12", layout="wide")

# ----------------------------
# CLEANER UI STYLING
# ----------------------------
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

st.title("Plant Maintenance Manager V12")
st.caption("Cleaner UI • Same Core Scheduling Logic • Monday to Sunday Planning")

# ----------------------------
# SESSION STATE
# ----------------------------
if "auto_schedule_df" not in st.session_state:
    st.session_state.auto_schedule_df = pd.DataFrame()

if "schedule_display_df" not in st.session_state:
    st.session_state.schedule_display_df = pd.DataFrame()

if "jobs_snapshot" not in st.session_state:
    st.session_state.jobs_snapshot = pd.DataFrame()

if "unscheduled_notes" not in st.session_state:
    st.session_state.unscheduled_notes = []

if "days_snapshot" not in st.session_state:
    st.session_state.days_snapshot = []

# ----------------------------
# DATABASE CONNECTION
# ----------------------------
conn = sqlite3.connect("maintenance.db", check_same_thread=False)
c = conn.cursor()

# ----------------------------
# CONSTANTS
# ----------------------------
DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
WEEKEND_DAYS = ["Friday", "Saturday", "Sunday"]
STATUS_OPTIONS = ["Pending", "Active", "Complete"]

DEFAULT_TECHS = [
    {"Technician": "Tech 1", "Skill": "Mechanical", "Weekly Hours": 40},
    {"Technician": "Tech 2", "Skill": "Mechanical", "Weekly Hours": 40},
    {"Technician": "Tech 3", "Skill": "Mechanical", "Weekly Hours": 40},
    {"Technician": "Tech 4", "Skill": "Mechanical", "Weekly Hours": 40},
    {"Technician": "Tech 5", "Skill": "Mechanical", "Weekly Hours": 40},
    {"Technician": "Tech 6", "Skill": "Mechanical", "Weekly Hours": 40},
    {"Technician": "Tech 7", "Skill": "Welding", "Weekly Hours": 40},
    {"Technician": "Tech 8", "Skill": "Welding", "Weekly Hours": 40},
    {"Technician": "Tech 9", "Skill": "Welding", "Weekly Hours": 40},
    {"Technician": "Tech 10", "Skill": "Welding", "Weekly Hours": 40},
]

# ----------------------------
# DATABASE HELPERS
# ----------------------------
def table_exists(table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name=?
    """, (table_name,))
    return cur.fetchone() is not None

def get_table_columns(table_name: str):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    rows = cur.fetchall()
    return [row[1] for row in rows]

def safe_sheet_name(name: str) -> str:
    invalid = ['\\', '/', '*', '?', ':', '[', ']']
    clean = str(name)
    for ch in invalid:
        clean = clean.replace(ch, "_")
    return clean[:31] if clean else "Sheet"

# ----------------------------
# DATABASE SETUP / MIGRATION
# ----------------------------
def ensure_jobs_table():
    if not table_exists("jobs"):
        c.execute("""
            CREATE TABLE jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job TEXT,
                location TEXT,
                department TEXT,
                hours REAL,
                mechanical_manpower INTEGER DEFAULT 0,
                welding_manpower INTEGER DEFAULT 0,
                priority INTEGER,
                status TEXT
            )
        """)
        conn.commit()
        return

    cols = get_table_columns("jobs")
    expected_cols = {
        "id", "job", "location", "department", "hours",
        "mechanical_manpower", "welding_manpower", "priority", "status"
    }

    if expected_cols.issubset(set(cols)):
        return

    c.execute("ALTER TABLE jobs RENAME TO jobs_old")

    c.execute("""
        CREATE TABLE jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job TEXT,
            location TEXT,
            department TEXT,
            hours REAL,
            mechanical_manpower INTEGER DEFAULT 0,
            welding_manpower INTEGER DEFAULT 0,
            priority INTEGER,
            status TEXT
        )
    """)

    old_cols = get_table_columns("jobs_old")

    if {"job", "location", "department", "hours", "mechanical_manpower", "welding_manpower", "priority"}.issubset(set(old_cols)):
        status_expr = "status" if "status" in old_cols else "'Pending'"
        c.execute(f"""
            INSERT INTO jobs (
                job, location, department, hours,
                mechanical_manpower, welding_manpower, priority, status
            )
            SELECT
                job, location, department, hours,
                COALESCE(mechanical_manpower, 0),
                COALESCE(welding_manpower, 0),
                priority,
                {status_expr}
            FROM jobs_old
        """)
    elif {"job", "type", "location", "department", "hours", "manpower", "priority"}.issubset(set(old_cols)):
        status_expr = "status" if "status" in old_cols else "'Pending'"
        c.execute(f"""
            INSERT INTO jobs (
                job, location, department, hours,
                mechanical_manpower, welding_manpower, priority, status
            )
            SELECT
                job,
                location,
                department,
                hours,
                CASE WHEN LOWER(type)='mechanical' THEN COALESCE(manpower, 1) ELSE 0 END,
                CASE WHEN LOWER(type)='welding' THEN COALESCE(manpower, 1) ELSE 0 END,
                priority,
                {status_expr}
            FROM jobs_old
        """)
    else:
        available = set(old_cols)
        job_col = "job" if "job" in available else "''"
        location_col = "location" if "location" in available else "''"
        dept_col = "department" if "department" in available else "''"
        hours_col = "hours" if "hours" in available else "1"
        priority_col = "priority" if "priority" in available else "10"
        status_col = "status" if "status" in available else "'Pending'"
        mech_col = "mechanical_manpower" if "mechanical_manpower" in available else "0"
        weld_col = "welding_manpower" if "welding_manpower" in available else "0"

        c.execute(f"""
            INSERT INTO jobs (
                job, location, department, hours,
                mechanical_manpower, welding_manpower, priority, status
            )
            SELECT
                {job_col},
                {location_col},
                {dept_col},
                {hours_col},
                COALESCE({mech_col}, 0),
                COALESCE({weld_col}, 0),
                {priority_col},
                {status_col}
            FROM jobs_old
        """)

    c.execute("DROP TABLE jobs_old")
    conn.commit()

def ensure_technicians_table():
    if not table_exists("technicians"):
        c.execute("""
            CREATE TABLE technicians (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                technician TEXT,
                skill TEXT,
                weekly_hours REAL
            )
        """)
        conn.commit()
        return

    cols = get_table_columns("technicians")
    expected_cols = {"id", "technician", "skill", "weekly_hours"}
    if expected_cols.issubset(set(cols)):
        return

    c.execute("ALTER TABLE technicians RENAME TO technicians_old")

    c.execute("""
        CREATE TABLE technicians (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            technician TEXT,
            skill TEXT,
            weekly_hours REAL
        )
    """)

    old_cols = get_table_columns("technicians_old")
    tech_col = "technician" if "technician" in old_cols else None
    skill_col = "skill" if "skill" in old_cols else None
    hours_col = "weekly_hours" if "weekly_hours" in old_cols else None

    if tech_col and skill_col and hours_col:
        c.execute(f"""
            INSERT INTO technicians (technician, skill, weekly_hours)
            SELECT {tech_col}, {skill_col}, {hours_col}
            FROM technicians_old
        """)

    c.execute("DROP TABLE technicians_old")
    conn.commit()

def ensure_manual_schedule_table():
    if not table_exists("manual_schedule"):
        c.execute("""
            CREATE TABLE manual_schedule (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job TEXT,
                day TEXT,
                assigned_technicians TEXT,
                location TEXT,
                department TEXT,
                hours REAL,
                mechanical_manpower INTEGER DEFAULT 0,
                welding_manpower INTEGER DEFAULT 0,
                priority INTEGER,
                status TEXT
            )
        """)
        conn.commit()
        return

    cols = get_table_columns("manual_schedule")
    expected_cols = {
        "id", "job", "day", "assigned_technicians", "location", "department", "hours",
        "mechanical_manpower", "welding_manpower", "priority", "status"
    }
    if expected_cols.issubset(set(cols)):
        return

    c.execute("ALTER TABLE manual_schedule RENAME TO manual_schedule_old")

    c.execute("""
        CREATE TABLE manual_schedule (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job TEXT,
            day TEXT,
            assigned_technicians TEXT,
            location TEXT,
            department TEXT,
            hours REAL,
            mechanical_manpower INTEGER DEFAULT 0,
            welding_manpower INTEGER DEFAULT 0,
            priority INTEGER,
            status TEXT
        )
    """)

    old_cols = get_table_columns("manual_schedule_old")
    available = set(old_cols)

    job_col = "job" if "job" in available else "''"
    day_col = "day" if "day" in available else "''"
    techs_col = "assigned_technicians" if "assigned_technicians" in available else "''"
    loc_col = "location" if "location" in available else "''"
    dept_col = "department" if "department" in available else "''"
    hours_col = "hours" if "hours" in available else "1"
    mech_col = "mechanical_manpower" if "mechanical_manpower" in available else "0"
    weld_col = "welding_manpower" if "welding_manpower" in available else "0"
    priority_col = "priority" if "priority" in available else "10"
    status_col = "status" if "status" in available else "'Pending'"

    c.execute(f"""
        INSERT INTO manual_schedule (
            job, day, assigned_technicians, location, department, hours,
            mechanical_manpower, welding_manpower, priority, status
        )
        SELECT
            {job_col}, {day_col}, {techs_col}, {loc_col}, {dept_col}, {hours_col},
            COALESCE({mech_col}, 0), COALESCE({weld_col}, 0), {priority_col}, {status_col}
        FROM manual_schedule_old
    """)

    c.execute("DROP TABLE manual_schedule_old")
    conn.commit()

def seed_default_technicians_if_empty():
    df = pd.read_sql("SELECT * FROM technicians", conn)
    if df.empty:
        for row in DEFAULT_TECHS:
            c.execute("""
                INSERT INTO technicians (technician, skill, weekly_hours)
                VALUES (?, ?, ?)
            """, (row["Technician"], row["Skill"], row["Weekly Hours"]))
        conn.commit()

ensure_jobs_table()
ensure_technicians_table()
ensure_manual_schedule_table()
seed_default_technicians_if_empty()

# ----------------------------
# DATABASE FUNCTIONS
# ----------------------------
def load_jobs_from_db():
    return pd.read_sql("""
        SELECT
            id,
            job,
            location,
            department,
            hours,
            mechanical_manpower,
            welding_manpower,
            priority,
            status
        FROM jobs
        ORDER BY priority ASC, id ASC
    """, conn)

def load_technicians_from_db():
    df = pd.read_sql("""
        SELECT id, technician, skill, weekly_hours
        FROM technicians
        ORDER BY id ASC
    """, conn)

    if df.empty:
        return pd.DataFrame(columns=["id", "Technician", "Skill", "Weekly Hours"])

    return df.rename(columns={
        "technician": "Technician",
        "skill": "Skill",
        "weekly_hours": "Weekly Hours"
    })

def load_manual_schedule_from_db():
    return pd.read_sql("""
        SELECT
            id,
            job,
            day,
            assigned_technicians,
            location,
            department,
            hours,
            mechanical_manpower,
            welding_manpower,
            priority,
            status
        FROM manual_schedule
        ORDER BY
            CASE day
                WHEN 'Monday' THEN 1
                WHEN 'Tuesday' THEN 2
                WHEN 'Wednesday' THEN 3
                WHEN 'Thursday' THEN 4
                WHEN 'Friday' THEN 5
                WHEN 'Saturday' THEN 6
                WHEN 'Sunday' THEN 7
                ELSE 8
            END,
            priority ASC,
            id ASC
    """, conn)

def save_technicians_to_db(tech_df):
    clean_df = tech_df.copy()

    if "id" in clean_df.columns:
        clean_df = clean_df.drop(columns=["id"])

    required = ["Technician", "Skill", "Weekly Hours"]
    missing = [col for col in required if col not in clean_df.columns]
    if missing:
        raise ValueError(f"Missing columns: {', '.join(missing)}")

    clean_df["Technician"] = clean_df["Technician"].fillna("").astype(str).str.strip()
    clean_df["Skill"] = clean_df["Skill"].fillna("").astype(str).str.strip()
    clean_df["Weekly Hours"] = pd.to_numeric(clean_df["Weekly Hours"], errors="coerce").fillna(40)

    clean_df = clean_df[
        (clean_df["Technician"] != "") &
        (clean_df["Skill"] != "")
    ].reset_index(drop=True)

    c.execute("DELETE FROM technicians")

    for _, row in clean_df.iterrows():
        c.execute("""
            INSERT INTO technicians (technician, skill, weekly_hours)
            VALUES (?, ?, ?)
        """, (
            row["Technician"],
            row["Skill"],
            float(row["Weekly Hours"])
        ))

    conn.commit()

def insert_job_to_db(job, location, department, hours, mechanical_manpower, welding_manpower, priority, status="Pending"):
    c.execute("""
        INSERT INTO jobs (
            job, location, department, hours,
            mechanical_manpower, welding_manpower, priority, status
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        job, location, department, hours,
        mechanical_manpower, welding_manpower, priority, status
    ))
    conn.commit()

def update_job_status(job_id, status):
    if status == "Complete":
        c.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
    else:
        c.execute("UPDATE jobs SET status = ? WHERE id = ?", (status, job_id))
    conn.commit()

def delete_job(job_id):
    c.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
    conn.commit()

def insert_manual_schedule_to_db(
    job, day, assigned_technicians, location, department, hours,
    mechanical_manpower, welding_manpower, priority, status="Pending"
):
    c.execute("""
        INSERT INTO manual_schedule (
            job, day, assigned_technicians, location, department, hours,
            mechanical_manpower, welding_manpower, priority, status
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        job, day, assigned_technicians, location, department, hours,
        mechanical_manpower, welding_manpower, priority, status
    ))
    conn.commit()

def update_manual_schedule_status(manual_id, status):
    if status == "Complete":
        c.execute("DELETE FROM manual_schedule WHERE id = ?", (manual_id,))
    else:
        c.execute("UPDATE manual_schedule SET status = ? WHERE id = ?", (status, manual_id))
    conn.commit()

def delete_manual_schedule(manual_id):
    c.execute("DELETE FROM manual_schedule WHERE id = ?", (manual_id,))
    conn.commit()

def normalize_excel_jobs(excel_df: pd.DataFrame):
    df = excel_df.copy()
    df.columns = [str(col).strip() for col in df.columns]

    if {"Job", "Location", "Department", "Hours", "Mechanical Manpower", "Welding Manpower", "Priority"}.issubset(df.columns):
        out = pd.DataFrame()
        out["Job"] = df["Job"]
        out["Location"] = df["Location"]
        out["Department"] = df["Department"]
        out["Hours"] = df["Hours"]
        out["Mechanical Manpower"] = df["Mechanical Manpower"]
        out["Welding Manpower"] = df["Welding Manpower"]
        out["Priority"] = df["Priority"]
        out["Status"] = df["Status"] if "Status" in df.columns else "Pending"
        return out, None, False

    if {"Job", "Type", "Location", "Department", "Hours", "Manpower", "Priority"}.issubset(df.columns):
        out = pd.DataFrame()
        out["Job"] = df["Job"]
        out["Location"] = df["Location"]
        out["Department"] = df["Department"]
        out["Hours"] = df["Hours"]
        out["Type"] = df["Type"]
        out["Manpower"] = df["Manpower"]
        out["Mechanical Manpower"] = 0
        out["Welding Manpower"] = 0
        out["Priority"] = df["Priority"]
        out["Status"] = df["Status"] if "Status" in df.columns else "Pending"
        return out, "Old Excel format detected. Please assign Mechanical Manpower and Welding Manpower below before importing.", True

    return None, (
        "Excel file must contain either:\n"
        "1) Job, Location, Department, Hours, Mechanical Manpower, Welding Manpower, Priority\n"
        "or\n"
        "2) Job, Type, Location, Department, Hours, Manpower, Priority"
    ), False

def insert_excel_jobs_to_db(excel_df):
    clean_df = excel_df.copy()

    clean_df["Job"] = clean_df["Job"].fillna("").astype(str).str.strip()
    clean_df["Location"] = clean_df["Location"].fillna("").astype(str).str.strip()
    clean_df["Department"] = clean_df["Department"].fillna("").astype(str).str.strip()
    clean_df["Hours"] = pd.to_numeric(clean_df["Hours"], errors="coerce").fillna(1.0)
    clean_df["Mechanical Manpower"] = pd.to_numeric(clean_df["Mechanical Manpower"], errors="coerce").fillna(0).astype(int)
    clean_df["Welding Manpower"] = pd.to_numeric(clean_df["Welding Manpower"], errors="coerce").fillna(0).astype(int)
    clean_df["Priority"] = pd.to_numeric(clean_df["Priority"], errors="coerce").fillna(10).astype(int)
    clean_df["Status"] = clean_df["Status"].fillna("Pending").astype(str).str.strip()

    clean_df = clean_df[
        (clean_df["Job"] != "") &
        ((clean_df["Mechanical Manpower"] > 0) | (clean_df["Welding Manpower"] > 0))
    ].reset_index(drop=True)

    inserted = 0
    for _, row in clean_df.iterrows():
        c.execute("""
            INSERT INTO jobs (
                job, location, department, hours,
                mechanical_manpower, welding_manpower, priority, status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row["Job"],
            row["Location"],
            row["Department"],
            float(row["Hours"]),
            int(row["Mechanical Manpower"]),
            int(row["Welding Manpower"]),
            int(row["Priority"]),
            row["Status"]
        ))
        inserted += 1

    conn.commit()
    return inserted

# ----------------------------
# SCHEDULE / EXPORT FUNCTIONS
# ----------------------------
def build_jobs_dataframe():
    db_jobs = load_jobs_from_db()
    if db_jobs.empty:
        return pd.DataFrame()

    jobs = db_jobs.copy()
    jobs["Source"] = "Database"
    jobs["SourceID"] = jobs["id"]
    jobs["Job ID"] = "DB_" + jobs["id"].astype(str)

    jobs = jobs.rename(columns={
        "job": "Job",
        "location": "Location",
        "department": "Department",
        "hours": "Hours",
        "mechanical_manpower": "Mechanical Manpower",
        "welding_manpower": "Welding Manpower",
        "priority": "Priority",
        "status": "Status"
    })

    jobs["Job"] = jobs["Job"].fillna("").astype(str).str.strip()
    jobs["Location"] = jobs["Location"].fillna("").astype(str).str.strip()
    jobs["Department"] = jobs["Department"].fillna("").astype(str).str.strip()
    jobs["Hours"] = pd.to_numeric(jobs["Hours"], errors="coerce").fillna(1.0)
    jobs["Mechanical Manpower"] = pd.to_numeric(jobs["Mechanical Manpower"], errors="coerce").fillna(0).astype(int)
    jobs["Welding Manpower"] = pd.to_numeric(jobs["Welding Manpower"], errors="coerce").fillna(0).astype(int)
    jobs["Priority"] = pd.to_numeric(jobs["Priority"], errors="coerce").fillna(10).astype(int)
    jobs["Status"] = jobs["Status"].fillna("Pending").astype(str).str.strip()

    jobs = jobs[
        (jobs["Job"] != "") &
        ((jobs["Mechanical Manpower"] > 0) | (jobs["Welding Manpower"] > 0))
    ].copy()

    jobs = jobs[jobs["Status"] != "Complete"].copy()
    jobs = jobs.sort_values(["Priority", "Job", "Job ID"]).reset_index(drop=True)
    return jobs

def generate_auto_schedule(jobs_df, planning_days, day_hours_limit=8):
    technicians = load_technicians_from_db()

    if technicians.empty:
        return pd.DataFrame(), ["Please add at least one technician."]

    technicians["Technician"] = technicians["Technician"].fillna("").astype(str).str.strip()
    technicians["Skill"] = technicians["Skill"].fillna("").astype(str).str.strip().str.lower()
    technicians["Weekly Hours"] = pd.to_numeric(technicians["Weekly Hours"], errors="coerce").fillna(40.0)

    technicians = technicians[
        (technicians["Technician"] != "") &
        (technicians["Skill"].isin(["mechanical", "welding"]))
    ].reset_index(drop=True)

    if technicians.empty:
        return pd.DataFrame(), ["Please add valid technicians with skill 'Mechanical' or 'Welding'."]

    tech_hours = {row["Technician"]: float(row["Weekly Hours"]) for _, row in technicians.iterrows()}
    daily_hours = {(row["Technician"], day): float(day_hours_limit) for _, row in technicians.iterrows() for day in planning_days}
    tech_skill = {row["Technician"]: row["Skill"] for _, row in technicians.iterrows()}

    schedule_rows = []
    unscheduled_notes = []

    for _, job in jobs_df.iterrows():
        if str(job["Status"]).strip() == "Complete":
            continue

        job_id = job["Job ID"]
        job_name = job["Job"]
        location = job["Location"]
        department = job["Department"]
        duration_remaining = max(0.0, float(job["Hours"]))
        mech_needed = max(0, int(job["Mechanical Manpower"]))
        weld_needed = max(0, int(job["Welding Manpower"]))
        priority = int(job["Priority"])
        status = job["Status"]

        if mech_needed == 0 and weld_needed == 0:
            unscheduled_notes.append(f"Job '{job_name}' has no manpower requirement.")
            continue

        while duration_remaining > 0:
            progress_made = False

            for day in planning_days:
                mech_candidates = technicians[technicians["Skill"] == "mechanical"]["Technician"].tolist()
                weld_candidates = technicians[technicians["Skill"] == "welding"]["Technician"].tolist()

                mech_candidates = sorted(
                    mech_candidates,
                    key=lambda x: (tech_hours[x], daily_hours[(x, day)]),
                    reverse=True
                )
                weld_candidates = sorted(
                    weld_candidates,
                    key=lambda x: (tech_hours[x], daily_hours[(x, day)]),
                    reverse=True
                )

                selected_mech = [
                    t for t in mech_candidates
                    if tech_hours[t] > 0 and daily_hours[(t, day)] > 0
                ][:mech_needed]

                selected_weld = [
                    t for t in weld_candidates
                    if tech_hours[t] > 0 and daily_hours[(t, day)] > 0
                ][:weld_needed]

                if len(selected_mech) < mech_needed or len(selected_weld) < weld_needed:
                    continue

                selected_team = selected_mech + selected_weld
                if not selected_team:
                    continue

                possible_chunk = min(
                    [duration_remaining] +
                    [tech_hours[t] for t in selected_team] +
                    [daily_hours[(t, day)] for t in selected_team]
                )

                if possible_chunk <= 0:
                    continue

                assigned_mechanics = ", ".join(selected_mech)
                assigned_welders = ", ".join(selected_weld)
                assigned_team = ", ".join(selected_team)

                for tech in selected_team:
                    schedule_rows.append({
                        "Schedule Type": "Auto",
                        "Source Row ID": job_id,
                        "Day": day,
                        "Technician": tech,
                        "Technician Skill": tech_skill[tech].title(),
                        "Job": job_name,
                        "Location": location,
                        "Department": department,
                        "Hours": round(possible_chunk, 2),
                        "Mechanical Manpower": mech_needed,
                        "Welding Manpower": weld_needed,
                        "Assigned Mechanics": assigned_mechanics,
                        "Assigned Welders": assigned_welders,
                        "Assigned Team": assigned_team,
                        "Priority": priority,
                        "Status": status
                    })

                    tech_hours[tech] -= possible_chunk
                    daily_hours[(tech, day)] -= possible_chunk

                duration_remaining -= possible_chunk
                progress_made = True

                if duration_remaining <= 0:
                    break

            if not progress_made:
                break

        if duration_remaining > 0:
            missing_parts = []
            if mech_needed > 0:
                missing_parts.append(f"{mech_needed} mechanic(s)")
            if weld_needed > 0:
                missing_parts.append(f"{weld_needed} welder(s)")

            crew_text = " and ".join(missing_parts) if missing_parts else "required crew"

            unscheduled_notes.append(
                f"Job '{job_name}' could not be fully scheduled. {round(duration_remaining, 2)} hour(s) left. "
                f"Required crew: {crew_text}."
            )

    return pd.DataFrame(schedule_rows), unscheduled_notes

def build_manual_schedule_display():
    manual_df = load_manual_schedule_from_db()
    if manual_df.empty:
        return pd.DataFrame()

    manual_df = manual_df[manual_df["status"] != "Complete"].copy()

    rows = []
    tech_df = load_technicians_from_db()
    skill_map = {}
    if not tech_df.empty:
        skill_map = dict(zip(tech_df["Technician"], tech_df["Skill"]))

    for _, row in manual_df.iterrows():
        techs_raw = row["assigned_technicians"] if pd.notna(row["assigned_technicians"]) else ""
        tech_list = [t.strip() for t in str(techs_raw).split(",") if t.strip()]

        if not tech_list:
            tech_list = ["Unassigned"]

        assigned_mechanics = []
        assigned_welders = []

        for tech in tech_list:
            skill = str(skill_map.get(tech, "")).strip().lower()
            if skill == "mechanical":
                assigned_mechanics.append(tech)
            elif skill == "welding":
                assigned_welders.append(tech)

        for tech in tech_list:
            rows.append({
                "Schedule Type": "Manual",
                "Source Row ID": f"MANUAL_{row['id']}",
                "Day": row["day"],
                "Technician": tech,
                "Technician Skill": skill_map.get(tech, "Manual/Unknown"),
                "Job": row["job"],
                "Location": row["location"],
                "Department": row["department"],
                "Hours": float(row["hours"]),
                "Mechanical Manpower": int(row["mechanical_manpower"]),
                "Welding Manpower": int(row["welding_manpower"]),
                "Assigned Mechanics": ", ".join(assigned_mechanics),
                "Assigned Welders": ", ".join(assigned_welders),
                "Assigned Team": ", ".join(tech_list),
                "Priority": int(row["priority"]),
                "Status": row["status"]
            })

    return pd.DataFrame(rows)

def combine_auto_and_manual(auto_df, manual_df):
    if auto_df.empty and manual_df.empty:
        return pd.DataFrame()

    if auto_df.empty:
        combined = manual_df.copy()
    elif manual_df.empty:
        combined = auto_df.copy()
    else:
        combined = pd.concat([auto_df, manual_df], ignore_index=True)

    day_order = {day: i for i, day in enumerate(DAYS)}
    combined["Day Order"] = combined["Day"].map(day_order).fillna(99)
    combined = combined.sort_values(["Day Order", "Priority", "Job", "Technician"]).drop(columns=["Day Order"])
    return combined.reset_index(drop=True)

def build_grouped_schedule_view(schedule_df):
    if schedule_df.empty:
        return pd.DataFrame()

    grouped = schedule_df.groupby(
        [
            "Schedule Type", "Day", "Job", "Location", "Department",
            "Mechanical Manpower", "Welding Manpower", "Priority", "Status"
        ],
        as_index=False
    ).agg({
        "Assigned Mechanics": lambda x: ", ".join(sorted(set([v for v in x if str(v).strip()]))),
        "Assigned Welders": lambda x: ", ".join(sorted(set([v for v in x if str(v).strip()]))),
        "Assigned Team": lambda x: ", ".join(sorted(set([v for v in x if str(v).strip()]))),
        "Hours": "max"
    })

    day_order = {day: i for i, day in enumerate(DAYS)}
    grouped["Day Order"] = grouped["Day"].map(day_order).fillna(99)
    grouped = grouped.sort_values(["Day Order", "Priority", "Job"]).drop(columns=["Day Order"])
    return grouped.reset_index(drop=True)

def build_technician_summary(schedule_df):
    if schedule_df.empty:
        return pd.DataFrame()

    summary = schedule_df.groupby(["Technician", "Technician Skill"], as_index=False).agg(
        Total_Hours=("Hours", "sum"),
        Jobs_Assigned=("Job", "nunique"),
        Days_Assigned=("Day", "nunique")
    )

    tech_df = load_technicians_from_db()
    if not tech_df.empty:
        summary = summary.merge(
            tech_df[["Technician", "Weekly Hours"]],
            on="Technician",
            how="left"
        )
        summary["Remaining Hours"] = summary["Weekly Hours"] - summary["Total_Hours"]
        summary["Utilization %"] = ((summary["Total_Hours"] / summary["Weekly Hours"]) * 100).round(1)
    else:
        summary["Weekly Hours"] = 0
        summary["Remaining Hours"] = 0
        summary["Utilization %"] = 0

    summary["Total_Hours"] = summary["Total_Hours"].round(2)
    summary["Remaining Hours"] = summary["Remaining Hours"].round(2)
    return summary.sort_values(["Technician"]).reset_index(drop=True)

def build_simple_export(df, sheet_name="Export"):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=safe_sheet_name(sheet_name))
    output.seek(0)
    return output.getvalue()

# ----------------------------
# LOAD BASE DATA
# ----------------------------
jobs = build_jobs_dataframe()
db_jobs = load_jobs_from_db()
manual_db = load_manual_schedule_from_db()
tech_df = load_technicians_from_db()

schedule_df = st.session_state.schedule_display_df
auto_schedule_df = st.session_state.auto_schedule_df
jobs_snapshot = st.session_state.jobs_snapshot
unscheduled_notes = st.session_state.unscheduled_notes

# ----------------------------
# TOP SUMMARY BAR
# ----------------------------
pending_jobs_count = 0 if db_jobs.empty else int((db_jobs["status"] == "Pending").sum())
active_jobs_count = 0 if db_jobs.empty else int((db_jobs["status"] == "Active").sum())
manual_jobs_count = 0 if manual_db.empty else len(manual_db)
technician_count = 0 if tech_df.empty else len(tech_df)
scheduled_hours = 0 if schedule_df.empty else round(schedule_df["Hours"].sum(), 2)

m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("Pending Jobs", pending_jobs_count)
m2.metric("Active Jobs", active_jobs_count)
m3.metric("Weekend Manual Jobs", manual_jobs_count)
m4.metric("Technicians", technician_count)
m5.metric("Scheduled Hours", scheduled_hours)

st.divider()

# ----------------------------
# TABS
# ----------------------------
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Dashboard", "Jobs", "Schedule", "Technicians", "Import / Export"
])

# ----------------------------
# DASHBOARD TAB
# ----------------------------
with tab1:
    st.subheader("Dashboard")

    col_dash1, col_dash2 = st.columns([1, 1])

    with col_dash1:
        st.markdown("##### Quick Planning")
        day_hours_limit = st.number_input(
            "Daily Hours Limit Per Technician",
            min_value=1.0,
            value=8.0,
            step=1.0,
            key="dashboard_day_limit"
        )

        if st.button("Generate Schedule", use_container_width=True):
            if jobs.empty:
                st.error("No database jobs found. Add jobs or import Excel data first.")
            else:
                auto_df, notes = generate_auto_schedule(jobs, DAYS, day_hours_limit=float(day_hours_limit))
                manual_df = build_manual_schedule_display()
                combined_df = combine_auto_and_manual(auto_df, manual_df)

                st.session_state.auto_schedule_df = auto_df
                st.session_state.schedule_display_df = combined_df
                st.session_state.jobs_snapshot = jobs.copy()
                st.session_state.unscheduled_notes = notes
                st.session_state.days_snapshot = DAYS.copy()
                st.rerun()

    with col_dash2:
        st.markdown("##### Current Status")
        st.write(f"Database jobs: **{0 if db_jobs.empty else len(db_jobs)}**")
        st.write(f"Manual weekend jobs: **{0 if manual_db.empty else len(manual_db)}**")
        st.write(f"Technicians loaded: **{0 if tech_df.empty else len(tech_df)}**")

    if not schedule_df.empty:
        st.markdown("##### Weekly Schedule (Grouped View)")
        grouped_schedule = build_grouped_schedule_view(schedule_df)
        st.dataframe(grouped_schedule, use_container_width=True)

        if unscheduled_notes:
            with st.expander("Scheduling Notes / Warnings"):
                for note in unscheduled_notes:
                    st.write(f"- {note}")

        with st.expander("View Timeline"):
            gantt = schedule_df.copy()
            day_map = {d: i + 1 for i, d in enumerate(DAYS)}
            gantt["Start"] = gantt["Day"].map(day_map)
            gantt["Finish"] = gantt["Start"] + 0.4

            fig = px.timeline(
                gantt,
                x_start="Start",
                x_end="Finish",
                y="Technician",
                color="Job",
                hover_data=[
                    "Schedule Type", "Location", "Department",
                    "Mechanical Manpower", "Welding Manpower",
                    "Priority", "Hours"
                ]
            )
            st.plotly_chart(fig, use_container_width=True)

# ----------------------------
# JOBS TAB
# ----------------------------
with tab2:
    st.subheader("Jobs")

    with st.expander("Add New Job", expanded=False):
        with st.form("job_form_v12", clear_on_submit=True):
            col1, col2 = st.columns(2)

            job_name = col1.text_input("Job Name")
            location = col2.text_input("Location / Equipment")
            department = col1.text_input("Department")
            hours = col2.number_input("Hours Required", min_value=1.0, value=1.0, step=1.0)

            mech_mp = col1.number_input("Mechanical Manpower", min_value=0, value=1, step=1)
            weld_mp = col2.number_input("Welding Manpower", min_value=0, value=0, step=1)
            priority = st.slider("Priority (1 = Highest)", 1, 20, 10)
            status = st.selectbox("Status", STATUS_OPTIONS[:-1])

            submitted = st.form_submit_button("Save Job")

            if submitted:
                if not job_name.strip():
                    st.error("Job name is required.")
                elif int(mech_mp) == 0 and int(weld_mp) == 0:
                    st.error("At least one manpower value must be greater than zero.")
                else:
                    insert_job_to_db(
                        job=job_name.strip(),
                        location=location.strip(),
                        department=department.strip(),
                        hours=float(hours),
                        mechanical_manpower=int(mech_mp),
                        welding_manpower=int(weld_mp),
                        priority=int(priority),
                        status=status
                    )
                    st.success("Job saved.")
                    st.rerun()

    if db_jobs.empty:
        st.info("No jobs in the database yet.")
    else:
        display_db_jobs = db_jobs.rename(columns={
            "job": "Job",
            "location": "Location",
            "department": "Department",
            "hours": "Hours",
            "mechanical_manpower": "Mechanical Manpower",
            "welding_manpower": "Welding Manpower",
            "priority": "Priority",
            "status": "Status"
        })
        st.markdown("##### Current Jobs")
        st.dataframe(display_db_jobs, use_container_width=True)

        col_manage1, col_manage2 = st.columns(2)

        with col_manage1:
            with st.expander("Update Job Status", expanded=False):
                job_options = {
                    f"{row['id']} - {row['job']}": row["id"]
                    for _, row in db_jobs.iterrows()
                }

                selected_job_label = st.selectbox("Select Job", list(job_options.keys()), key="job_status_select_v12")
                new_status = st.selectbox("Select New Status", STATUS_OPTIONS, key="job_new_status_v12")

                if st.button("Apply Status Change", use_container_width=True):
                    update_job_status(job_options[selected_job_label], new_status)
                    if new_status == "Complete":
                        st.success("Job marked complete and removed from the program.")
                    else:
                        st.success(f"Job status updated to {new_status}.")
                    st.rerun()

        with col_manage2:
            with st.expander("Delete Job", expanded=False):
                job_options_delete = {
                    f"{row['id']} - {row['job']}": row["id"]
                    for _, row in db_jobs.iterrows()
                }

                selected_label = st.selectbox("Select Job to Delete", list(job_options_delete.keys()), key="delete_job_select_v12")
                if st.button("Delete Selected Job", use_container_width=True):
                    delete_job(job_options_delete[selected_label])
                    st.success("Job deleted.")
                    st.rerun()

    st.markdown("##### Weekend Jobs")

    with st.expander("Add Weekend Job", expanded=False):
        available_tech_names = tech_df["Technician"].tolist() if not tech_df.empty else []

        with st.form("manual_weekend_form_v12", clear_on_submit=True):
            colm1, colm2 = st.columns(2)

            manual_job_name = colm1.text_input("Job Name ")
            manual_day = colm2.selectbox("Day", WEEKEND_DAYS)
            manual_location = colm1.text_input("Location / Equipment ")
            manual_department = colm2.text_input("Department ")
            manual_hours = colm1.number_input("Hours ", min_value=1.0, value=1.0, step=1.0)
            manual_mech_mp = colm2.number_input("Mechanical Manpower ", min_value=0, value=0, step=1)
            manual_weld_mp = colm1.number_input("Welding Manpower ", min_value=0, value=0, step=1)
            manual_priority = colm2.slider("Priority (1 = Highest) ", 1, 20, 10)
            manual_status = st.selectbox("Status ", STATUS_OPTIONS[:-1])
            manual_techs = st.multiselect("Assign Technician(s)", available_tech_names)

            manual_submit = st.form_submit_button("Save Weekend Job")

            if manual_submit:
                if not manual_job_name.strip():
                    st.error("Manual job name is required.")
                elif int(manual_mech_mp) == 0 and int(manual_weld_mp) == 0:
                    st.error("At least one manpower value must be greater than zero.")
                elif not manual_techs:
                    st.error("Please assign at least one technician.")
                else:
                    insert_manual_schedule_to_db(
                        job=manual_job_name.strip(),
                        day=manual_day,
                        assigned_technicians=", ".join(manual_techs),
                        location=manual_location.strip(),
                        department=manual_department.strip(),
                        hours=float(manual_hours),
                        mechanical_manpower=int(manual_mech_mp),
                        welding_manpower=int(manual_weld_mp),
                        priority=int(manual_priority),
                        status=manual_status
                    )
                    st.success("Manual weekend job saved.")
                    st.rerun()

    if manual_db.empty:
        st.info("No manual weekend jobs saved yet.")
    else:
        st.dataframe(manual_db, use_container_width=True)

        col_manual1, col_manual2 = st.columns(2)

        with col_manual1:
            with st.expander("Update Weekend Job Status", expanded=False):
                manual_options_status = {
                    f"{row['id']} - {row['day']} - {row['job']}": row["id"]
                    for _, row in manual_db.iterrows()
                }
                selected_manual_status = st.selectbox(
                    "Select Weekend Job",
                    list(manual_options_status.keys()),
                    key="manual_status_select_v12"
                )
                new_manual_status = st.selectbox(
                    "Select New Status",
                    STATUS_OPTIONS,
                    key="manual_new_status_v12"
                )

                if st.button("Apply Weekend Status Change", use_container_width=True):
                    update_manual_schedule_status(manual_options_status[selected_manual_status], new_manual_status)
                    if new_manual_status == "Complete":
                        st.success("Manual weekend job marked complete and removed from the program.")
                    else:
                        st.success(f"Manual weekend job status updated to {new_manual_status}.")
                    st.rerun()

        with col_manual2:
            with st.expander("Delete Weekend Job", expanded=False):
                manual_options = {
                    f"{row['id']} - {row['day']} - {row['job']}": row["id"]
                    for _, row in manual_db.iterrows()
                }
                selected_manual = st.selectbox(
                    "Select Weekend Job to Delete",
                    list(manual_options.keys()),
                    key="manual_delete_select_v12"
                )
                if st.button("Delete Selected Weekend Job", use_container_width=True):
                    delete_manual_schedule(manual_options[selected_manual])
                    st.success("Manual weekend job deleted.")
                    st.rerun()

# ----------------------------
# SCHEDULE TAB
# ----------------------------
with tab3:
    st.subheader("Schedule")

    col_sched1, col_sched2 = st.columns([1, 1])

    with col_sched1:
        sched_day_limit = st.number_input(
            "Daily Hours Limit Per Technician",
            min_value=1.0,
            value=8.0,
            step=1.0,
            key="schedule_day_limit"
        )

    with col_sched2:
        st.markdown("<div class='small-caption'>This keeps the same scheduling logic as the previous version.</div>", unsafe_allow_html=True)
        if st.button("Generate Schedule Now", use_container_width=True):
            if jobs.empty:
                st.error("No database jobs found. Add jobs or import Excel data first.")
            else:
                auto_df, notes = generate_auto_schedule(jobs, DAYS, day_hours_limit=float(sched_day_limit))
                manual_df = build_manual_schedule_display()
                combined_df = combine_auto_and_manual(auto_df, manual_df)

                st.session_state.auto_schedule_df = auto_df
                st.session_state.schedule_display_df = combined_df
                st.session_state.jobs_snapshot = jobs.copy()
                st.session_state.unscheduled_notes = notes
                st.session_state.days_snapshot = DAYS.copy()
                st.rerun()

    if schedule_df.empty:
        st.info("Generate a schedule to view weekly assignments.")
    else:
        grouped_schedule = build_grouped_schedule_view(schedule_df)

        st.markdown("##### Weekly Schedule (Grouped View)")
        for day in DAYS:
            day_data = grouped_schedule[grouped_schedule["Day"] == day]
            with st.expander(day, expanded=(day == "Monday")):
                if day_data.empty:
                    st.info(f"No jobs scheduled for {day}.")
                else:
                    st.dataframe(day_data, use_container_width=True)

        with st.expander("Full Schedule Detail", expanded=False):
            st.dataframe(schedule_df, use_container_width=True)

        with st.expander("Job Backlog", expanded=False):
            if jobs_snapshot.empty:
                st.info("No jobs found.")
            else:
                scheduled_auto_job_ids = auto_schedule_df["Source Row ID"].unique().tolist() if not auto_schedule_df.empty else []
                backlog = jobs_snapshot[~jobs_snapshot["Job ID"].isin(scheduled_auto_job_ids)].copy()

                display_backlog = backlog[[
                    "Job", "Location", "Department", "Hours",
                    "Mechanical Manpower", "Welding Manpower",
                    "Priority", "Status"
                ]] if not backlog.empty else pd.DataFrame()

                if display_backlog.empty:
                    st.success("All database jobs were scheduled.")
                else:
                    st.dataframe(display_backlog, use_container_width=True)
                    critical = display_backlog[display_backlog["Priority"] <= 3]
                    if len(critical) > 0:
                        st.error(f"{len(critical)} high-priority backlog job(s) were not scheduled.")

        if unscheduled_notes:
            with st.expander("Warnings", expanded=False):
                for note in unscheduled_notes:
                    st.write(f"- {note}")

# ----------------------------
# TECHNICIANS TAB
# ----------------------------
with tab4:
    st.subheader("Technicians")

    editable_tech_df = st.data_editor(
        tech_df if not tech_df.empty else pd.DataFrame(DEFAULT_TECHS),
        num_rows="dynamic",
        use_container_width=True,
        key="tech_editor_v12"
    )

    col_tech1, col_tech2 = st.columns(2)

    if col_tech1.button("Save Technicians", use_container_width=True):
        try:
            save_technicians_to_db(editable_tech_df)
            st.success("Technicians saved.")
            st.rerun()
        except Exception as e:
            st.error(f"Could not save technicians: {e}")

    if col_tech2.button("Reset Default Technicians", use_container_width=True):
        c.execute("DELETE FROM technicians")
        for row in DEFAULT_TECHS:
            c.execute("""
                INSERT INTO technicians (technician, skill, weekly_hours)
                VALUES (?, ?, ?)
            """, (row["Technician"], row["Skill"], row["Weekly Hours"]))
        conn.commit()
        st.success("Technicians reset.")
        st.rerun()

    if not schedule_df.empty:
        st.markdown("##### Technician Workload")
        workload_summary = build_technician_summary(schedule_df)
        if workload_summary.empty:
            st.info("No technician workload available.")
        else:
            st.bar_chart(workload_summary.set_index("Technician")["Total_Hours"])
            st.dataframe(workload_summary, use_container_width=True)

        st.markdown("##### Technician Assignment View")
        technician_list = sorted(schedule_df["Technician"].dropna().astype(str).unique().tolist())
        selected_technician = st.selectbox("Select Technician", technician_list, key="selected_technician_v12")

        technician_jobs = schedule_df[schedule_df["Technician"] == selected_technician].copy()
        technician_jobs = technician_jobs.sort_values(["Day", "Priority", "Job"]).reset_index(drop=True)

        tc1, tc2, tc3 = st.columns(3)
        tc1.metric("Jobs Assigned", technician_jobs["Job"].nunique())
        tc2.metric("Hours Assigned", round(technician_jobs["Hours"].sum(), 2))
        tc3.metric("Days Assigned", technician_jobs["Day"].nunique())

        st.dataframe(technician_jobs, use_container_width=True)
    else:
        st.info("Generate a schedule to view technician workload and assignments.")

# ----------------------------
# IMPORT / EXPORT TAB
# ----------------------------
with tab5:
    st.subheader("Import / Export")

    with st.expander("Import Excel Jobs", expanded=False):
        uploaded_file = st.file_uploader("Upload Job Excel Sheet", type=["xlsx"], key="upload_v12")

        if uploaded_file:
            try:
                excel_jobs = pd.read_excel(uploaded_file, engine="openpyxl")
                excel_jobs.columns = [str(col).strip() for col in excel_jobs.columns]

                st.write("Imported Excel Preview")
                st.dataframe(excel_jobs, use_container_width=True)

                normalized_preview, preview_message, needs_manual_assignment = normalize_excel_jobs(excel_jobs)

                if normalized_preview is None:
                    st.error(preview_message)
                else:
                    if preview_message:
                        st.info(preview_message)

                    if needs_manual_assignment:
                        st.write("Assign Mechanical and Welding Manpower Before Import")
                        editable_preview = normalized_preview.copy()
                        editable_preview = st.data_editor(
                            editable_preview,
                            use_container_width=True,
                            num_rows="fixed",
                            key="excel_old_format_editor_v12"
                        )

                        if st.button("Import Edited Excel Jobs", use_container_width=True):
                            inserted_count = insert_excel_jobs_to_db(editable_preview)
                            st.success(f"{inserted_count} Excel job(s) imported to database.")
                            st.rerun()
                    else:
                        st.write("Normalized Import Preview")
                        st.dataframe(normalized_preview, use_container_width=True)

                        if st.button("Import Excel Jobs", use_container_width=True):
                            inserted_count = insert_excel_jobs_to_db(normalized_preview)
                            st.success(f"{inserted_count} Excel job(s) imported to database.")
                            st.rerun()

            except Exception as e:
                st.error(f"Could not read Excel file: {e}")

    with st.expander("Export Files", expanded=True):
        if schedule_df.empty:
            st.info("Generate a schedule first to unlock exports.")
        else:
            grouped_schedule = build_grouped_schedule_view(schedule_df)

            technician_list = sorted(schedule_df["Technician"].dropna().astype(str).unique().tolist())
            export_technician = st.selectbox("Technician for Assignment Export", technician_list, key="export_technician_v12")
            technician_jobs_export = schedule_df[schedule_df["Technician"] == export_technician].copy()
            technician_jobs_export = technician_jobs_export.sort_values(["Day", "Priority", "Job"]).reset_index(drop=True)

            col_exp_top1, col_exp_top2 = st.columns(2)

            col_exp_top1.download_button(
                label="Export Weekly Schedule (Grouped View)",
                data=build_simple_export(grouped_schedule, sheet_name="Weekly Grouped View"),
                file_name="Weekly_Schedule_Grouped_View.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            col_exp_top2.download_button(
                label=f"Export {export_technician} Assignment View",
                data=build_simple_export(technician_jobs_export, sheet_name="Technician Assignment View"),
                file_name=f"{export_technician.replace(' ', '_')}_Technician_Assignment_View.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            st.markdown("##### Weekend Exports")
            col_exp1, col_exp2, col_exp3, col_exp4 = st.columns(4)

            friday_df = grouped_schedule[grouped_schedule["Day"] == "Friday"].copy()
            saturday_df = grouped_schedule[grouped_schedule["Day"] == "Saturday"].copy()
            sunday_df = grouped_schedule[grouped_schedule["Day"] == "Sunday"].copy()
            weekend_df = grouped_schedule[grouped_schedule["Day"].isin(WEEKEND_DAYS)].copy()

            col_exp1.download_button(
                label="Export Friday",
                data=build_simple_export(friday_df, sheet_name="Friday"),
                file_name="Friday_Schedule.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            col_exp2.download_button(
                label="Export Saturday",
                data=build_simple_export(saturday_df, sheet_name="Saturday"),
                file_name="Saturday_Schedule.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            col_exp3.download_button(
                label="Export Sunday",
                data=build_simple_export(sunday_df, sheet_name="Sunday"),
                file_name="Sunday_Schedule.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            col_exp4.download_button(
                label="Export Friday-Sunday",
                data=build_simple_export(weekend_df, sheet_name="Friday-Sunday"),
                file_name="Friday_to_Sunday_Schedule.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )