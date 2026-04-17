"""
frontend/timesheet_validation.py
---------------------------------
Streamlit app for Agentic Timesheet Validation.

Features:
  - Upload Fusion XLSX + MS Weekly Hrs XLSX
  - Run validation (all 7 rules) in-browser
  - Summary metrics and error breakdown
  - Preview error table
  - Download correction_output.xlsx
  - Save run history to Oracle ADW via ORDS

Run locally:
    streamlit run frontend/timesheet_validation.py

Deploy to Streamlit Cloud:
    - Push repo to GitHub
    - Connect at share.streamlit.io
    - Set secrets: OCI_DB_USER, OCI_DB_PASSWORD
"""

import io
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import streamlit as st
import pandas as pd

# Allow imports from project root
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

# ── Page config (must be first Streamlit call) ────────────────────────────────
st.set_page_config(
    page_title="Timesheet Validation",
    page_icon="T",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS — Oracle dark theme (matches existing app.py) ─────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600;700&display=swap');
:root {
    --oracle-red:     #C74634;
    --oracle-dark:    #1A1A2E;
    --oracle-navy:    #16213E;
    --oracle-blue:    #0F3460;
    --oracle-accent:  #E94560;
    --oracle-teal:    #0D7377;
    --text-primary:   #F0F0F0;
    --text-secondary: #A0A0B0;
    --border:         #2A2A4A;
    --card-bg:        #1E1E38;
    --success:        #2ECC71;
    --warning:        #F39C12;
    --error-col:      #E74C3C;
}
.stApp {
    background: linear-gradient(135deg, #0A0A1A 0%, #1A1A2E 50%, #0A0F1A 100%);
    font-family: 'IBM Plex Sans', sans-serif;
    color: var(--text-primary);
}
#MainMenu, footer, header { visibility: hidden; }
.stDeployButton { display: none; }

.app-header {
    background: linear-gradient(90deg, var(--oracle-dark) 0%, var(--oracle-blue) 100%);
    border-bottom: 2px solid var(--oracle-red);
    padding: 1rem 2rem;
    margin: -1rem -1rem 2rem -1rem;
    display: flex; align-items: center; gap: 1rem;
}
.app-title    { font-family:'IBM Plex Mono',monospace; font-size:1.4rem; font-weight:600; color:var(--text-primary); letter-spacing:.05em; margin:0; }
.app-subtitle { font-size:.75rem; color:var(--text-secondary); font-family:'IBM Plex Mono',monospace; letter-spacing:.1em; text-transform:uppercase; margin:0; }

.metric-row  { display:flex; gap:1rem; margin-bottom:1rem; }
.metric-card { flex:1; background:var(--card-bg); border:1px solid var(--border); border-radius:6px; padding:.75rem 1rem; text-align:center; }
.metric-value { font-family:'IBM Plex Mono',monospace; font-size:1.5rem; font-weight:600; color:var(--oracle-teal); }
.metric-value.red  { color:var(--error-col); }
.metric-value.amber{ color:var(--warning); }
.metric-value.green{ color:var(--success); }
.metric-label { font-size:.7rem; color:var(--text-secondary); text-transform:uppercase; letter-spacing:.1em; }

[data-testid="stSidebar"] { background: var(--oracle-navy) !important; border-right:1px solid var(--border); }
[data-testid="stSidebar"] h3 { margin-top:.25rem !important; margin-bottom:.25rem !important; }
.sidebar-section { font-family:'IBM Plex Mono',monospace; font-size:.7rem; color:var(--text-secondary);
    text-transform:uppercase; letter-spacing:.15em; margin:.6rem 0 .25rem 0;
    padding-bottom:.2rem; border-bottom:1px solid var(--border); }
[data-testid="stSidebar"] [data-testid="stFileUploader"] { margin-bottom:0 !important; }
[data-testid="stSidebar"] [data-testid="stFileUploader"] > div { padding:.4rem !important; }
[data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] { padding:.5rem !important; min-height:auto !important; }
[data-testid="stSidebar"] section[data-testid="stSidebar"] > div { padding-top:.5rem !important; }

.stButton > button {
    background: var(--card-bg) !important; color: var(--text-primary) !important;
    border: 1px solid var(--border) !important; border-radius:4px !important;
    font-family:'IBM Plex Sans',sans-serif !important; font-size:.82rem !important;
    padding:.4rem .75rem !important; width:100% !important;
    text-align:left !important; transition:all .2s !important;
}
.stButton > button:hover { background:var(--oracle-blue) !important; border-color:var(--oracle-teal) !important; color:white !important; }
.step-box {
    background: var(--card-bg); border:1px solid var(--border); border-radius:8px;
    padding:1.25rem 1.5rem; margin-bottom:1rem;
}
.step-title { font-family:'IBM Plex Mono',monospace; font-size:.85rem; color:var(--oracle-teal);
    letter-spacing:.08em; text-transform:uppercase; margin-bottom:.5rem; }
.badge { display:inline-block; padding:.15rem .5rem; border-radius:3px;
    font-family:'IBM Plex Mono',monospace; font-size:.68rem; font-weight:600;
    letter-spacing:.05em; text-transform:uppercase; }
.badge-red    { background:rgba(199,70,52,.2);  color:#C74634; border:1px solid #C74634; }
.badge-amber  { background:rgba(243,156,18,.2); color:#F39C12; border:1px solid #F39C12; }
.badge-yellow { background:rgba(241,196,15,.2); color:#F1C40F; border:1px solid #F1C40F; }
.badge-green  { background:rgba(46,204,113,.2); color:#2ECC71; border:1px solid #2ECC71; }
.badge-gray   { background:rgba(160,160,176,.2);color:#A0A0B0; border:1px solid #A0A0B0; }
</style>
""", unsafe_allow_html=True)

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="app-header">
  <div>
    <p class="app-subtitle">Oracle Fusion HCM</p>
    <p class="app-title">Agentic Timesheet Validation</p>
  </div>
</div>
""", unsafe_allow_html=True)

# ── Session state ─────────────────────────────────────────────────────────────
for key, default in [
    ("validation_done", False),
    ("error_df", None),
    ("all_df", None),
    ("summary", {}),
    ("excel_bytes", None),
    ("run_id", None),
    ("db_saved", None),
]:
    if key not in st.session_state:
        st.session_state[key] = default


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_db():
    """Return OrdsDB if credentials available, else None."""
    try:
        from scripts.oci_db import OrdsDB
        pwd = (
            st.secrets.get("OCI_DB_PASSWORD", "")
            or os.getenv("OCI_DB_PASSWORD", "")
        )
        if not pwd:
            return None
        return OrdsDB(password=pwd)
    except Exception:
        return None


@st.cache_resource
def _load_vc():
    """Load validate_timecards module once and cache it."""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "validate_timecards", ROOT / "scripts" / "validate_timecards.py"
    )
    vc = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(vc)
    return vc


def run_validation(fusion_bytes: bytes, jira_bytes: bytes, pm_filter: set = None):
    """
    Write uploads to temp files, call vc.run(), read back results.
    Returns (error_df, all_df, summary_dict, excel_bytes).
    pm_filter: set of PM names to include, or None for all default PMs.
    """
    vc = _load_vc()

    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f1:
        f1.write(fusion_bytes)
        fusion_path = f1.name

    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f2:
        f2.write(jira_bytes)
        jira_path = f2.name

    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f3:
        output_path = f3.name

    try:
        # Run full validation (writes correction_output.xlsx to output_path)
        vc.run(fusion_path, jira_path, output_path, pm_filter=pm_filter or None)

        # Read back the output Excel to get dataframes
        all_df   = pd.read_excel(output_path, sheet_name="All Entries")
        error_df = pd.read_excel(output_path, sheet_name="Corrections Needed")

        # Read output Excel bytes for download
        with open(output_path, "rb") as fout:
            excel_bytes = fout.read()

    finally:
        for p in [fusion_path, jira_path, output_path]:
            try: os.unlink(p)
            except: pass

    # Summary
    total  = len(all_df)
    errors = len(error_df)
    summary = {
        "total_rows":   total,
        "total_errors": errors,
        "clean_rows":   total - errors,
    }
    if errors:
        summary["breakdown"] = (
            error_df["Corrections Needed"]
            .value_counts()
            .reset_index()
        )

    return error_df, all_df, summary, excel_bytes


def save_run_to_db(db, fusion_name, jira_name, summary):
    """Persist run metadata to ts_validation_runs. Returns run_id."""
    try:
        fn = fusion_name.replace("'", "''")
        jn = jira_name.replace("'", "''")
        db.execute(f"""
            INSERT INTO ts_validation_runs
                (fusion_file, jira_file, fusion_rows_in, fusion_rows_excluded,
                 total_errors, total_clean, status)
            VALUES ('{fn}', '{jn}',
                {summary.get('total_rows', 0)}, 0,
                {summary.get('total_errors', 0)}, {summary.get('clean_rows', 0)},
                'completed')
        """)
        rows = db.query("SELECT MAX(id) AS run_id FROM ts_validation_runs")
        return rows[0]["run_id"] if rows else None
    except Exception as e:
        st.warning(f"Could not save run metadata: {e}")
        return None


def _esc(val, max_len=500):
    """Escape value for inline SQL."""
    if val is None or (isinstance(val, float) and str(val) == 'nan'):
        return "NULL"
    s = str(val).strip()[:max_len].replace("'", "''")
    return f"'{s}'"


def save_results_to_db(db, run_id: int, all_df):
    """
    Insert every validated row into ts_validation_results.
    Batches 50 rows per ORDS request.
    Returns (inserted, errors).
    """
    if run_id is None:
        return 0, 0

    inserted = errors = 0
    batch = []

    for i, row in all_df.iterrows():
        has_err = 1 if str(row.get("Corrections Needed", "")).strip() else 0
        note    = _esc(row.get("Corrections Needed"), 500)
        detail  = _esc(row.get("Error Detail"), 500)
        emp_num = _esc(row.get("Employee #"), 50)
        emp     = _esc(row.get("Employee"), 200)
        email   = _esc(row.get("Email"), 200)
        proj_n  = _esc(row.get("Project #"), 100)
        proj_nm = _esc(row.get("Customer/Job"), 500)
        task    = _esc(row.get("Task Name"), 500)
        memo    = _esc(row.get("Memo"), 2000)
        ticket  = _esc(row.get("Extracted Ticket"), 50)
        suggest = _esc(row.get("Suggested Ticket"), 50)
        jira_op = _esc(row.get("Jira Oracle Project"), 500)
        pmatch  = _esc(row.get("Project Match"), 10)
        itype   = _esc(row.get("Issue Type"), 100)
        labels  = _esc(row.get("Labels"), 500)
        period  = _esc(row.get("Period"), 50)
        status  = _esc(row.get("Status"), 50)
        hours   = row.get("Actual Time") or 0
        try:
            hours = float(hours)
        except Exception:
            hours = 0

        # Parse date
        raw_date = row.get("Date")
        try:
            from datetime import datetime
            if hasattr(raw_date, 'strftime'):
                date_sql = f"TO_DATE('{raw_date.strftime('%Y-%m-%d')}','YYYY-MM-DD')"
            elif raw_date and str(raw_date).strip() not in ('', 'nan', 'None'):
                date_sql = f"TO_DATE('{str(raw_date)[:10]}','YYYY-MM-DD')"
            else:
                date_sql = "NULL"
        except Exception:
            date_sql = "NULL"

        sql = (
            f"INSERT INTO ts_validation_results "
            f"(run_id,row_num,timecard_status,project_number,project_name,task_name,"
            f"entry_date,employee_number,employee_name,email,total_hours,memo,"
            f"has_error,correction_note,error_detail,extracted_ticket,suggested_ticket,"
            f"jira_oracle_project,project_match,issue_type,jira_labels,timecard_period) "
            f"VALUES ({run_id},{i},{status},{proj_n},{proj_nm},{task},"
            f"{date_sql},{emp_num},{emp},{email},{hours},{memo},"
            f"'{has_err}',{note},{detail},{ticket},{suggest},"
            f"{jira_op},{pmatch},{itype},{labels},{period})"
        )
        batch.append(sql)

        if len(batch) == 50:
            try:
                db.execute_many(batch)
                inserted += len(batch)
            except Exception as e:
                errors += len(batch)
            batch = []

    if batch:
        try:
            db.execute_many(batch)
            inserted += len(batch)
        except Exception as e:
            errors += len(batch)

    return inserted, errors


def _render_ai_tab(db, current_run_id):
    """Render the AI Assistant chat tab."""
    from scripts.ts_agent import get_answer, QUERIES

    groq_key = st.secrets.get("GROQ_API_KEY", "") or os.getenv("GROQ_API_KEY", "")

    # Resolve run_id automatically — use current session or latest from DB
    run_id = current_run_id
    if not run_id and db:
        try:
            rows = db.query("SELECT MAX(id) AS rid FROM ts_validation_runs")
            run_id = rows[0]["rid"] if rows else 1
        except Exception:
            run_id = 1
    run_id = int(run_id) if run_id else 1

    if not db:
        st.warning("Oracle ADW not connected — results cannot be fetched.")

    # Suggested questions
    with st.expander("Suggested questions", expanded=False):
        for q in QUERIES:
            st.markdown(f'<span class="badge badge-gray">{q.name}</span> <span style="font-size:.8rem;color:#A0A0B0;">{q.description}</span>', unsafe_allow_html=True)

    # Question input
    question = st.text_input(
        "Ask a question about the validation results",
        placeholder="e.g. Who has the most errors?  |  Show entries with no memo  |  Format issues",
        key="ai_question",
    )

    if st.button("Ask", key="ai_ask_btn") and question.strip():
        sql, _ = get_answer(question.strip(), run_id, groq_api_key=groq_key or None)

        if db:
            try:
                rows = db.query(sql)
                if rows:
                    result_df = pd.DataFrame(rows)
                    st.dataframe(result_df, use_container_width=True, height=400)

                    # Excel download
                    buf = io.BytesIO()
                    result_df.to_excel(buf, index=False, engine="openpyxl")
                    buf.seek(0)
                    fname = f"ai_results_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
                    st.download_button(
                        label="Download as Excel",
                        data=buf,
                        file_name=fname,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="ai_download",
                    )
                else:
                    st.info("No results found.")
            except Exception as e:
                st.error(f"Could not fetch results: {e}")
        else:
            st.info("Connect Oracle ADW to execute queries.")


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### Timesheet Validation")
    st.markdown('<div class="sidebar-section">Step 1 — Upload Files</div>', unsafe_allow_html=True)

    fusion_file = st.file_uploader(
        "Fusion Timecard Dump (XLSX)",
        type=["xlsx"],
        key="fusion_upload",
        help="Export from Oracle Fusion HCM — Timecard Dump by Employee",
    )

    jira_file = st.file_uploader(
        "MS Weekly Hrs / Jira Workbook (XLSX)",
        type=["xlsx"],
        key="jira_upload",
        help="MS Weekly Hrs workbook with Tickets, People, Project Edits sheets",
    )

    st.markdown('<div class="sidebar-section">Step 2 — Run</div>', unsafe_allow_html=True)

    run_btn = st.button(
        "Run Validation",
        disabled=(fusion_file is None or jira_file is None),
        key="run_btn",
    )

    if fusion_file:
        st.markdown(f'<span class="badge badge-green">Fusion loaded</span> <small style="color:#606080">{fusion_file.name}</small>', unsafe_allow_html=True)
    else:
        st.markdown('<span class="badge badge-gray">Fusion — not uploaded</span>', unsafe_allow_html=True)

    if jira_file:
        st.markdown(f'<span class="badge badge-green">Jira loaded</span> <small style="color:#606080">{jira_file.name}</small>', unsafe_allow_html=True)
    else:
        st.markdown('<span class="badge badge-gray">Jira — not uploaded</span>', unsafe_allow_html=True)

    st.markdown('<div class="sidebar-section">Step 3 — Download</div>', unsafe_allow_html=True)

    if st.session_state.excel_bytes:
        fname = f"correction_output_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        st.download_button(
            label="Download Correction Excel",
            data=st.session_state.excel_bytes,
            file_name=fname,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
    else:
        st.markdown('<span style="font-size:.75rem;color:#404060;">Run validation first</span>', unsafe_allow_html=True)

    st.markdown('<div class="sidebar-section">DB Status</div>', unsafe_allow_html=True)
    db = get_db()
    if db:
        h = db.health_check()
        if h["status"] == "ok":
            st.markdown('<span class="badge badge-green">Oracle ADW Connected</span>', unsafe_allow_html=True)
        else:
            st.markdown('<span class="badge badge-amber">Oracle ADW Error</span>', unsafe_allow_html=True)
    else:
        st.markdown('<span class="badge badge-gray">Oracle ADW — no credentials</span>', unsafe_allow_html=True)


# ── Run validation ─────────────────────────────────────────────────────────────
if run_btn and fusion_file and jira_file:
    with st.spinner("Running validation — this may take 30–60 seconds..."):
        try:
            error_df, all_df, summary, excel_bytes = run_validation(
                fusion_file.read(), jira_file.read(), pm_filter=None
            )
            st.session_state.validation_done = True
            st.session_state.error_df   = error_df
            st.session_state.all_df     = all_df
            st.session_state.summary    = summary
            st.session_state.excel_bytes = excel_bytes

            # Save to DB if connected
            db = get_db()
            if db:
                with st.spinner("Saving results to Oracle ADW..."):
                    run_id = save_run_to_db(db, fusion_file.name, jira_file.name, summary)
                    st.session_state.run_id = run_id
                    if run_id:
                        ins, errs = save_results_to_db(db, run_id, all_df)
                        st.session_state.db_saved = {"inserted": ins, "errors": errs}

            st.success("Validation complete!")
        except Exception as e:
            st.error(f"Validation failed: {e}")
            import traceback
            st.code(traceback.format_exc())


# ── Main area ─────────────────────────────────────────────────────────────────
if not st.session_state.validation_done:
    st.markdown("""
    <div class="step-box">
      <div class="step-title">How to use</div>
      <ol style="color:var(--text-secondary); font-size:.88rem; line-height:1.9;">
        <li>Upload the <strong style="color:var(--text-primary)">Fusion Timecard Dump</strong> (XLSX export from Oracle Fusion)</li>
        <li>Upload the <strong style="color:var(--text-primary)">MS Weekly Hrs workbook</strong> (contains Tickets, People, Project Edits sheets)</li>
        <li>Click <strong style="color:var(--oracle-teal)">Run Validation</strong></li>
        <li>Review the error breakdown and preview table</li>
        <li>Download the <strong style="color:var(--text-primary)">Correction Excel</strong></li>
      </ol>
      <div style="margin-top:1rem; font-size:.8rem; color:#404060; font-family:'IBM Plex Mono',monospace;">
        Validation rules: blank memo · missing ticket · format issues (spaces, em-dash, multiple tickets) · not in Jira · wrong project
      </div>
    </div>
    """, unsafe_allow_html=True)

else:
    summary  = st.session_state.summary
    error_df = st.session_state.error_df
    all_df   = st.session_state.all_df

    total   = summary.get("total_rows", 0)
    errors  = summary.get("total_errors", 0)
    clean   = summary.get("clean_rows", 0)
    pct_err = round(errors / total * 100, 1) if total else 0

    # ── Metrics strip ─────────────────────────────────────────
    st.markdown(f"""
    <div class="metric-row">
      <div class="metric-card">
        <div class="metric-value">{total:,}</div>
        <div class="metric-label">Total Rows</div>
      </div>
      <div class="metric-card">
        <div class="metric-value red">{errors:,}</div>
        <div class="metric-label">Errors Found</div>
      </div>
      <div class="metric-card">
        <div class="metric-value green">{clean:,}</div>
        <div class="metric-label">Clean Rows</div>
      </div>
      <div class="metric-card">
        <div class="metric-value amber">{pct_err}%</div>
        <div class="metric-label">Error Rate</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")

    # ── Error table ───────────────────────────────────────────
    tab1, tab2 = st.tabs(["Errors Only", "All Entries"])

    with tab1:
        st.markdown(f"**{len(error_df):,} rows with issues**")

        # Filter controls
        col_f1, col_f2 = st.columns([2, 2])
        with col_f1:
            note_filter = st.selectbox(
                "Filter by issue type",
                ["All"] + sorted(error_df["Corrections Needed"].unique().tolist()),
                key="note_filter",
            )
        with col_f2:
            emp_search = st.text_input("Search employee name", key="emp_search", placeholder="e.g. Smith")

        display_df = error_df.copy()
        if note_filter != "All":
            display_df = display_df[display_df["Corrections Needed"] == note_filter]
        if emp_search:
            display_df = display_df[display_df["Employee"].str.contains(emp_search, case=False, na=False)]

        show_cols = ["Corrections Needed", "Employee", "Date", "Project #",
                     "Memo", "Extracted Ticket", "Suggested Ticket", "Project Match", "Actual Time"]
        st.dataframe(
            display_df[[c for c in show_cols if c in display_df.columns]],
            use_container_width=True,
            height=400,
        )

    with tab2:
        st.markdown(f"**{len(all_df):,} total rows** filtered from Fusion dump")
        show_all_cols = ["Corrections Needed", "Employee", "Date", "Project #", "Memo",
                         "Actual Time", "Extracted Ticket", "Project Match"]
        st.dataframe(
            all_df[[c for c in show_all_cols if c in all_df.columns]],
            use_container_width=True,
            height=400,
        )

    # ── Run ID ────────────────────────────────────────────────
    if st.session_state.run_id:
        db_msg = ""
        if st.session_state.db_saved:
            ins  = st.session_state.db_saved["inserted"]
            errs = st.session_state.db_saved["errors"]
            db_msg = f"&nbsp;|&nbsp; {ins:,} rows saved to ADW"
            if errs:
                db_msg += f" ({errs} errors)"
        st.markdown(
            f'<div style="font-family:IBM Plex Mono,monospace;font-size:.68rem;color:#404060;margin-top:.5rem;">'
            f'run_id = {st.session_state.run_id}{db_msg}'
            f'</div>',
            unsafe_allow_html=True,
        )


# ── AI Assistant — always visible ────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
st.markdown("""
<div style="
    background: linear-gradient(135deg, #0D1F2D 0%, #0A1628 100%);
    border: 1px solid #0D7377;
    border-left: 4px solid #0D7377;
    border-radius: 8px;
    padding: 1.25rem 1.5rem 0.5rem 1.5rem;
    margin-top: 0.5rem;
">
  <div style="display:flex; align-items:center; gap:.6rem; margin-bottom:.75rem;">
    <span style="font-size:1.2rem;">🤖</span>
    <span style="font-family:'IBM Plex Mono',monospace; font-size:.9rem; font-weight:600;
          color:#0D7377; text-transform:uppercase; letter-spacing:.1em;">AI Assistant</span>
    <span style="font-family:'IBM Plex Mono',monospace; font-size:.68rem; color:#404060;
          margin-left:auto;">Ask anything about the validation results</span>
  </div>
</div>
""", unsafe_allow_html=True)

with st.container():
    st.markdown('<div style="background:#0A1628;border:1px solid #0D7377;border-top:none;border-radius:0 0 8px 8px;padding:1rem 1.5rem 1.25rem 1.5rem;">', unsafe_allow_html=True)
    _render_ai_tab(get_db(), st.session_state.run_id)
    st.markdown('</div>', unsafe_allow_html=True)
