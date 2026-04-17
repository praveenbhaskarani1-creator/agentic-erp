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
.sidebar-section { font-family:'IBM Plex Mono',monospace; font-size:.7rem; color:var(--text-secondary);
    text-transform:uppercase; letter-spacing:.15em; margin:1.5rem 0 .5rem 0;
    padding-bottom:.3rem; border-bottom:1px solid var(--border); }

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


def run_validation(fusion_bytes: bytes, jira_bytes: bytes):
    """
    Write uploads to temp files, call vc.run(), read back results.
    Returns (error_df, all_df, summary_dict, excel_bytes).
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
        vc.run(fusion_path, jira_path, output_path)

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


def save_run_to_db(db, fusion_name, jira_name, summary, error_df):
    """Persist run metadata to ts_validation_runs."""
    try:
        fn  = fusion_name.replace("'", "''")
        jn  = jira_name.replace("'", "''")
        rows_in  = summary.get("total_rows", 0)
        rows_exc = 0
        errors   = summary.get("total_errors", 0)
        clean    = summary.get("clean_rows", 0)

        db.execute(f"""
            INSERT INTO ts_validation_runs
                (fusion_file, jira_file, fusion_rows_in, fusion_rows_excluded,
                 total_errors, total_clean, status)
            VALUES ('{fn}', '{jn}', {rows_in}, {rows_exc}, {errors}, {clean}, 'completed')
        """)
        # Get the new run ID
        rows = db.query("SELECT MAX(id) AS run_id FROM ts_validation_runs")
        return rows[0]["run_id"] if rows else None
    except Exception as e:
        st.warning(f"Could not save run to DB: {e}")
        return None


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
                fusion_file.read(), jira_file.read()
            )
            st.session_state.validation_done = True
            st.session_state.error_df   = error_df
            st.session_state.all_df     = all_df
            st.session_state.summary    = summary
            st.session_state.excel_bytes = excel_bytes

            # Save to DB if connected
            db = get_db()
            if db:
                run_id = save_run_to_db(db, fusion_file.name, jira_file.name, summary, error_df)
                st.session_state.run_id = run_id

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
        <li>Download the <strong style="color:var(--text-primary)">Correction Excel</strong> — same format as Alison's weekly sheets</li>
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

    # ── Error breakdown ───────────────────────────────────────
    if "breakdown" in summary:
        col1, col2 = st.columns([1, 2])

        with col1:
            st.markdown('<div class="step-title" style="font-family:\'IBM Plex Mono\',monospace;font-size:.8rem;color:#0D7377;text-transform:uppercase;letter-spacing:.08em;">Error Breakdown</div>', unsafe_allow_html=True)
            bd = summary["breakdown"].copy()
            # Colour map for badges
            def badge_for(note):
                n = str(note).lower()
                if "memo" in n:           return "badge-red"
                if "not found" in n:      return "badge-red"
                if "remove spaces" in n:  return "badge-amber"
                if "dash" in n:           return "badge-amber"
                if "one ticket" in n:     return "badge-amber"
                if "ticket is for" in n:  return "badge-yellow"
                return "badge-gray"

            for _, r in bd.iterrows():
                note  = r.iloc[0]
                count = r.iloc[1]
                badge = badge_for(note)
                st.markdown(
                    f'<div style="margin:.3rem 0;">'
                    f'<span class="badge {badge}">{count:,}</span> '
                    f'<span style="font-size:.82rem;color:var(--text-secondary);">{note}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

        with col2:
            st.markdown('<div class="step-title" style="font-family:\'IBM Plex Mono\',monospace;font-size:.8rem;color:#0D7377;text-transform:uppercase;letter-spacing:.08em;">Error Distribution</div>', unsafe_allow_html=True)
            chart_data = summary["breakdown"].set_index(summary["breakdown"].columns[0])[summary["breakdown"].columns[1]]
            st.bar_chart(chart_data, use_container_width=True, height=220)

    st.markdown("---")

    # ── Error table ───────────────────────────────────────────
    tab1, tab2 = st.tabs(["Errors Only", "All Entries"])

    with tab1:
        st.markdown(f"**{len(error_df):,} rows with issues** — same as Corrections Needed sheet")

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
        st.markdown(
            f'<div style="font-family:IBM Plex Mono,monospace;font-size:.68rem;color:#404060;margin-top:.5rem;">'
            f'Run saved to Oracle ADW &nbsp;|&nbsp; run_id = {st.session_state.run_id}'
            f'</div>',
            unsafe_allow_html=True,
        )
