"""
frontend/app.py
---------------
Streamlit frontend for Agentic Time Entry Validation.

Features:
  - Chat interface with conversation history
  - Sidebar with quick query buttons
  - System health status
  - Export to Excel download
  - Professional Oracle/enterprise aesthetic

Run locally:
    streamlit run frontend/app.py

Run on EC2 Bastion:
    streamlit run frontend/app.py --server.port 8501 --server.address 0.0.0.0
"""

import streamlit as st
import requests
import json
import html as _html
import uuid
from datetime import datetime
from typing import Optional

import boto3
from botocore.exceptions import ClientError

S3_BUCKET       = "agentic-erp-artifacts-241030170015"
S3_REGION       = "us-east-1"
PENDING_PREFIX  = "uploads/pending/"
RESULTS_PREFIX  = "uploads/results/"
URL_EXPIRES_SEC = 900

# -- Config ----------------------------------------------------
API_URL = "http://3.239.64.11:8000"   # -- your ECS task IP

# -- Page config -----------------------------------------------
st.set_page_config(
    page_title  = "Agentic Time Entry Validation",
    page_icon   = "T",
    layout      = "wide",
    initial_sidebar_state = "expanded",
)

# -- Custom CSS ------------------------------------------------
st.markdown("""
<style>
    /* Import fonts */
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600;700&display=swap');

    /* Root variables */
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
        --error:          #E74C3C;
    }

    /* Global */
    .stApp {
        background: linear-gradient(135deg, #0A0A1A 0%, #1A1A2E 50%, #0A0F1A 100%);
        font-family: 'IBM Plex Sans', sans-serif;
        color: var(--text-primary);
    }

    /* Hide Streamlit branding */
    #MainMenu, footer, header { visibility: hidden; }
    .stDeployButton { display: none; }

    /* Header */
    .app-header {
        background: linear-gradient(90deg, var(--oracle-dark) 0%, var(--oracle-blue) 100%);
        border-bottom: 2px solid var(--oracle-red);
        padding: 1rem 2rem;
        margin: -1rem -1rem 2rem -1rem;
        display: flex;
        align-items: center;
        gap: 1rem;
    }
    .app-title {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 1.4rem;
        font-weight: 600;
        color: var(--text-primary);
        letter-spacing: 0.05em;
        margin: 0;
    }
    .app-subtitle {
        font-size: 0.75rem;
        color: var(--text-secondary);
        font-family: 'IBM Plex Mono', monospace;
        letter-spacing: 0.1em;
        text-transform: uppercase;
        margin: 0;
    }

    /* Health indicator */
    .health-dot {
        width: 10px;
        height: 10px;
        border-radius: 50%;
        display: inline-block;
        margin-right: 6px;
    }
    .health-ok      { background: var(--success); box-shadow: 0 0 6px var(--success); }
    .health-error   { background: var(--error);   box-shadow: 0 0 6px var(--error); }
    .health-unknown { background: var(--warning); box-shadow: 0 0 6px var(--warning); }

    /* Chat messages */
    .chat-container {
        max-height: 520px;
        overflow-y: auto;
        padding: 1rem;
        background: var(--oracle-dark);
        border: 1px solid var(--border);
        border-radius: 8px;
        margin-bottom: 1rem;
    }
    .message-user {
        background: var(--oracle-blue);
        border-left: 3px solid var(--oracle-accent);
        padding: 0.75rem 1rem;
        border-radius: 0 8px 8px 0;
        margin: 0.5rem 0 0.5rem 2rem;
        font-size: 0.9rem;
    }
    .message-bot {
        background: var(--card-bg);
        border-left: 3px solid var(--oracle-teal);
        padding: 0.75rem 1rem;
        border-radius: 0 8px 8px 0;
        margin: 0.5rem 2rem 0.5rem 0;
        font-size: 0.9rem;
    }
    .message-meta {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 0.7rem;
        color: var(--text-secondary);
        margin-top: 0.5rem;
        padding-top: 0.5rem;
        border-top: 1px solid var(--border);
    }
    .message-label-user {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 0.7rem;
        color: var(--oracle-accent);
        text-transform: uppercase;
        letter-spacing: 0.1em;
        margin-bottom: 0.3rem;
    }
    .message-label-bot {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 0.7rem;
        color: var(--oracle-teal);
        text-transform: uppercase;
        letter-spacing: 0.1em;
        margin-bottom: 0.3rem;
    }

    /* Sidebar */
    .css-1d391kg, [data-testid="stSidebar"] {
        background: var(--oracle-navy) !important;
        border-right: 1px solid var(--border);
    }
    .sidebar-section {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 0.7rem;
        color: var(--text-secondary);
        text-transform: uppercase;
        letter-spacing: 0.15em;
        margin: 1.5rem 0 0.5rem 0;
        padding-bottom: 0.3rem;
        border-bottom: 1px solid var(--border);
    }

    /* Quick query buttons */
    .stButton > button {
        background: var(--card-bg) !important;
        color: var(--text-primary) !important;
        border: 1px solid var(--border) !important;
        border-radius: 4px !important;
        font-family: 'IBM Plex Sans', sans-serif !important;
        font-size: 0.82rem !important;
        padding: 0.4rem 0.75rem !important;
        width: 100% !important;
        text-align: left !important;
        transition: all 0.2s !important;
    }
    .stButton > button:hover {
        background: var(--oracle-blue) !important;
        border-color: var(--oracle-teal) !important;
        color: white !important;
    }

    /* Input */
    .stTextInput > div > div > input {
        background: var(--card-bg) !important;
        border: 1px solid var(--border) !important;
        border-radius: 4px !important;
        color: var(--text-primary) !important;
        font-family: 'IBM Plex Sans', sans-serif !important;
        font-size: 0.9rem !important;
    }

    /* Status badges */
    .badge {
        display: inline-block;
        padding: 0.2rem 0.6rem;
        border-radius: 3px;
        font-family: 'IBM Plex Mono', monospace;
        font-size: 0.7rem;
        font-weight: 600;
        letter-spacing: 0.05em;
        text-transform: uppercase;
    }
    .badge-keyword { background: rgba(13, 115, 119, 0.2); color: #0D7377; border: 1px solid #0D7377; }
    .badge-llm     { background: rgba(201, 70, 52, 0.2);  color: #C74634; border: 1px solid #C74634; }
    .badge-ok      { background: rgba(46, 204, 113, 0.2); color: #2ECC71; border: 1px solid #2ECC71; }
    .badge-empty   { background: rgba(243, 156, 18, 0.2); color: #F39C12; border: 1px solid #F39C12; }

    /* Metrics */
    .metric-row {
        display: flex;
        gap: 1rem;
        margin-bottom: 1rem;
    }
    .metric-card {
        flex: 1;
        background: var(--card-bg);
        border: 1px solid var(--border);
        border-radius: 6px;
        padding: 0.75rem 1rem;
        text-align: center;
    }
    .metric-value {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 1.5rem;
        font-weight: 600;
        color: var(--oracle-teal);
    }
    .metric-label {
        font-size: 0.7rem;
        color: var(--text-secondary);
        text-transform: uppercase;
        letter-spacing: 0.1em;
    }

    /* Scrollbar */
    ::-webkit-scrollbar { width: 4px; }
    ::-webkit-scrollbar-track { background: var(--oracle-dark); }
    ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 2px; }
</style>
""", unsafe_allow_html=True)


# -------------------------------------------------------------
# Session state
# -------------------------------------------------------------

if "messages" not in st.session_state:
    st.session_state.messages = []

if "health" not in st.session_state:
    st.session_state.health = None

if "quick_query" not in st.session_state:
    st.session_state.quick_query = None

if "pending_result_key" not in st.session_state:
    st.session_state.pending_result_key = None

if "upload_status_msg" not in st.session_state:
    st.session_state.upload_status_msg = None


# -------------------------------------------------------------
# API helpers
# -------------------------------------------------------------

def call_api_ask(question: str) -> dict:
    try:
        response = requests.post(
            f"{API_URL}/ask",
            json    = {"question": question, "user_id": "streamlit_user"},
            timeout = 30,
        )
        return response.json()
    except requests.exceptions.ConnectionError:
        return {"error": "Cannot connect to API. Check if ECS task is running."}
    except Exception as e:
        return {"error": str(e)}


def call_api_health() -> dict:
    try:
        response = requests.get(f"{API_URL}/health", timeout=5)
        return response.json()
    except:
        return {"status": "error", "api": "unreachable", "db": "unknown", "bedrock": "unknown"}


def call_api_export() -> Optional[bytes]:
    try:
        response = requests.get(f"{API_URL}/export/excel", timeout=60)
        if response.status_code == 200:
            return response.content
        return None
    except:
        return None


def call_api_queries() -> list:
    try:
        response = requests.get(f"{API_URL}/queries", timeout=5)
        return response.json().get("queries", [])
    except:
        return []


def _s3_client():
    return boto3.client("s3", region_name=S3_REGION)


def generate_presigned_upload(filename: str, content_type: str) -> Optional[dict]:
    safe_name  = "".join(c for c in filename if c.isalnum() or c in "._-")
    uid        = uuid.uuid4().hex[:8]
    s3_key     = f"{PENDING_PREFIX}{uid}_{safe_name}"
    result_key = f"{RESULTS_PREFIX}{uid}_{safe_name}.json"
    try:
        upload_url = _s3_client().generate_presigned_url(
            "put_object",
            Params    = {"Bucket": S3_BUCKET, "Key": s3_key, "ContentType": content_type},
            ExpiresIn = URL_EXPIRES_SEC,
        )
        return {"upload_url": upload_url, "s3_key": s3_key, "result_key": result_key}
    except ClientError:
        return None


def upload_file_to_s3(upload_url: str, file_bytes: bytes, content_type: str) -> bool:
    try:
        response = requests.put(
            upload_url,
            data    = file_bytes,
            headers = {"Content-Type": content_type},
            timeout = 120,
        )
        return response.status_code in (200, 204)
    except:
        return False


def get_upload_status(result_key: str) -> Optional[dict]:
    try:
        obj  = _s3_client().get_object(Bucket=S3_BUCKET, Key=result_key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        return None


# -------------------------------------------------------------
# Header
# -------------------------------------------------------------

st.markdown("""
<div class="app-header">
    <div>
        <p class="app-subtitle">Oracle Fusion HCM</p>
        <p class="app-title">Agentic Time Entry Validation</p>
    </div>
</div>
""", unsafe_allow_html=True)


# -------------------------------------------------------------
# Sidebar
# -------------------------------------------------------------

with st.sidebar:
    st.markdown("### Time Validation")

    # -- Upload Data -------------------------------------------
    st.markdown('<div class="sidebar-section">Upload Data</div>', unsafe_allow_html=True)

    uploaded_file = st.file_uploader(
        "Upload CSV or Excel",
        type            = ["csv", "xlsx"],
        label_visibility= "collapsed",
        key             = "upload_widget",
    )

    if uploaded_file is not None:
        if st.button("Upload to S3", key="upload_btn"):
            content_type = (
                "text/csv"
                if uploaded_file.name.endswith(".csv")
                else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            with st.spinner("Generating upload URL..."):
                url_data = generate_presigned_upload(uploaded_file.name, content_type)

            if not url_data:
                st.error("Could not generate upload URL — check AWS credentials")
            else:
                with st.spinner("Uploading to S3..."):
                    file_bytes = uploaded_file.read()
                    ok = upload_file_to_s3(url_data["upload_url"], file_bytes, content_type)

                if not ok:
                    st.error("S3 upload failed")
                else:
                    st.session_state["pending_result_key"] = url_data["result_key"]
                    st.session_state["upload_status_msg"] = None

    # Persistent status message
    if st.session_state.get("upload_status_msg"):
        msg = st.session_state["upload_status_msg"]
        if msg["type"] == "success":
            st.success(msg["text"])
        elif msg["type"] == "error":
            st.error(msg["text"])

    # Poll for processing result
    if st.session_state.get("pending_result_key"):
        result_key = st.session_state["pending_result_key"]
        st.info("File uploaded — Lambda is processing...")
        if st.button("Check Processing Status", key="check_status_btn"):
            with st.spinner("Checking..."):
                status = get_upload_status(result_key)
            if status is None:
                st.session_state["upload_status_msg"] = {
                    "type": "error",
                    "text": "Still processing — check again in a few seconds",
                }
            elif status.get("status") == "success":
                inserted = status.get("rows_inserted", 0)
                skipped  = status.get("rows_skipped", 0)
                st.session_state["upload_status_msg"] = {
                    "type": "success",
                    "text": f"Success! {inserted} rows inserted, {skipped} skipped.",
                }
                st.session_state.pop("pending_result_key", None)
            else:
                errors = status.get("errors") or []
                st.session_state["upload_status_msg"] = {
                    "type": "error",
                    "text": "Processing failed: " + " | ".join(errors[:3]),
                }
                st.session_state.pop("pending_result_key", None)
            st.rerun()

    st.markdown("""
    <div style="font-family:'IBM Plex Mono',monospace; font-size:0.65rem; color:#404060; margin-top:0.3rem;">
        Required columns: employee, date, hours<br>
        Optional: id, memo &nbsp;·&nbsp; Date format: YYYY-MM-DD
    </div>
    """, unsafe_allow_html=True)

    # -- Quick Queries -----------------------------------------
    st.markdown('<div class="sidebar-section">Quick Queries</div>', unsafe_allow_html=True)

    quick_queries = [
        ("All Entries",       "show me all time entries"),
        ("Blank Memos",       "show me entries with blank memos"),
        ("Last 7 Days",       "show me entries from the last 7 days"),
        ("Non ERP Memo",      "find entries not following ERP naming convention"),
        ("Total Count",       "how many total time entries are there"),
    ]

    for label, question in quick_queries:
        if st.button(label, key=f"quick_{label}"):
            st.session_state.messages.append({"role": "user", "content": question})
            with st.spinner("Agent thinking..."):
                result = call_api_ask(question)
            if "error" in result and not result.get("answer"):
                answer = f"⚠️ Error: {result['error']}"
                data   = None
            else:
                answer = result.get("answer", "No answer returned")
                data   = result.get("data")
            st.session_state.messages.append({"role": "assistant", "content": answer, "data": data})
            st.rerun()

    # -- Export ------------------------------------------------
    st.markdown('<div class="sidebar-section">Export</div>', unsafe_allow_html=True)

    if st.button("Download Excel Report"):
        with st.spinner("Generating Excel report..."):
            content = call_api_export()
            if content:
                filename = f"TimeEntry_Validation_{datetime.now().strftime('%Y-%m-%d')}.xlsx"
                st.download_button(
                    label    = "Save Report",
                    data     = content,
                    file_name = filename,
                    mime     = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            else:
                st.error("Export failed — check API connection")

    # -- Clear Chat --------------------------------------------
    st.markdown('<div class="sidebar-section">Actions</div>', unsafe_allow_html=True)

    if st.button("Clear Chat"):
        st.session_state.messages = []
        st.rerun()

    # -- API Info ----------------------------------------------
    st.markdown('<div class="sidebar-section">API</div>', unsafe_allow_html=True)
    st.markdown(f"""
    <div style="font-family: 'IBM Plex Mono', monospace; font-size: 0.7rem; color: #606080; word-break: break-all;">
        {API_URL}
    </div>
    """, unsafe_allow_html=True)

    # -- Health Status (bottom) --------------------------------
    st.markdown('<div class="sidebar-section">System Status</div>', unsafe_allow_html=True)

    if st.button("Refresh Status"):
        st.session_state.health = call_api_health()

    health = st.session_state.health or call_api_health()
    st.session_state.health = health

    def health_dot(status):
        if status in ("ok", "connected"):
            return '<span class="health-dot health-ok"></span>'
        elif status == "error" or "error" in str(status).lower():
            return '<span class="health-dot health-error"></span>'
        else:
            return '<span class="health-dot health-unknown"></span>'

    st.markdown(f"""
    <div style="font-size:0.82rem; line-height:2;">
        {health_dot(health.get('api',''))} API &nbsp;&nbsp;&nbsp; <code style="font-size:0.7rem;">{health.get('api','')}</code><br>
        {health_dot(health.get('db',''))} Database &nbsp; <code style="font-size:0.7rem;">{health.get('db','')}</code><br>
        {health_dot(health.get('bedrock',''))} Bedrock &nbsp;&nbsp; <code style="font-size:0.7rem;">{health.get('bedrock','')[:12] if health.get('bedrock') else ''}</code>
    </div>
    """, unsafe_allow_html=True)


# -------------------------------------------------------------
# Main — Session Stats strip (top)
# -------------------------------------------------------------

total_msgs   = len([m for m in st.session_state.messages if m["role"] == "user"])
keyword_hits = len([m for m in st.session_state.messages
                    if m.get("meta", {}) and m["meta"].get("intent_source") == "keyword"])
llm_hits     = len([m for m in st.session_state.messages
                    if m.get("meta", {}) and m["meta"].get("intent_source") == "llm"])

st.markdown(f"""
<div class="metric-row">
    <div class="metric-card">
        <div class="metric-value">{total_msgs}</div>
        <div class="metric-label">Questions</div>
    </div>
    <div class="metric-card">
        <div class="metric-value" style="color:#0D7377;">{keyword_hits}</div>
        <div class="metric-label">Keyword Hits</div>
    </div>
    <div class="metric-card">
        <div class="metric-value" style="color:#C74634;">{llm_hits}</div>
        <div class="metric-label">LLM Routed</div>
    </div>
    <div class="metric-card">
        <div class="metric-value" style="font-size:0.95rem; color:#606080;">
            {datetime.now().strftime('%Y-%m-%d %H:%M')}
        </div>
        <div class="metric-label">Last Updated</div>
    </div>
</div>
""", unsafe_allow_html=True)

# -------------------------------------------------------------
# Main — Chat Area
# -------------------------------------------------------------

if True:
    col1 = st.container()
    # -- Conversation history ----------------------------------
    if st.session_state.messages:
        for msg in st.session_state.messages:
            if msg["role"] == "user":
                st.markdown(f"""
                <div class="message-user">
                    <div class="message-label-user">You</div>
                    {_html.escape(msg["content"])}
                </div>""", unsafe_allow_html=True)
            else:
                # Bot message
                st.markdown(f"""
                <div class="message-bot">
                    <div class="message-label-bot">Agent</div>
                    {msg["content"].replace(chr(10), "<br>")}
                </div>""", unsafe_allow_html=True)
                
                # Render table if data exists
                if msg.get("data"):
                    with st.expander("View Data Table", expanded=True):
                        st.dataframe(msg["data"], use_container_width=True)
    else:
        st.markdown("""
        <div style="padding:0.6rem 1rem; background:var(--oracle-dark,#1A1A2E);
                    border:1px solid #2A2A4A; border-radius:6px; margin-bottom:0.75rem;">
            <span style="font-family:'IBM Plex Mono',monospace; font-size:0.72rem;
                         color:#404060; letter-spacing:0.08em;">
                &#9656; Ask a question or use Quick Queries in the sidebar
            </span>
        </div>
        """, unsafe_allow_html=True)

    # -- Input -------------------------------------------------
    with st.form("chat_form", clear_on_submit=True):
        col_input, col_btn = st.columns([5, 1])
        with col_input:
            user_input = st.text_input(
                "question",
                placeholder = "Ask about your timesheet data...",
                label_visibility = "collapsed",
            )
        with col_btn:
            submitted = st.form_submit_button("Ask", use_container_width=True)

    # -- Process question --------------------------------------
    if submitted and user_input.strip():
        st.session_state.messages.append({
            "role":    "user",
            "content": user_input.strip(),
        })

        with st.spinner("Agent thinking..."):
            result = call_api_ask(user_input.strip())

        if "error" in result and not result.get("answer"):
            answer = f"⚠️ Error: {result['error']}"
            meta   = None
            data   = None
        else:
            answer = result.get("answer", "No answer returned")
            meta   = {
                "intent_detected": result.get("intent_detected"),
                "intent_source":   result.get("intent_source"),
                "row_count":       result.get("row_count"),
            }
            data = result.get("data")
            
        st.session_state.messages.append({
            "role":    "assistant",
            "content": answer,
            "meta":    meta,
            "data":    data,
        })
        st.rerun()


