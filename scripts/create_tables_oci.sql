-- =======================================================================
-- create_tables_oci.sql
-- Oracle Autonomous Database DDL for Timesheet Validation System
--
-- Run in: OCI Console → Autonomous DB → Database Actions → SQL
--   OR via SQLcl:  sql ADMIN/<pwd>@<dsn> @create_tables_oci.sql
--
-- Tables created:
--   1. ts_jira_tickets     - Jira ticket lookup (from MS Weekly Hrs → Tickets sheet)
--   2. ts_people           - Employee email lookup (from MS Weekly Hrs → People sheet)
--   3. ts_project_mapping  - Oracle→Jira project name map (from Project Edits sheet)
--   4. ts_validation_runs  - One row per validation run (audit trail)
--   5. ts_validation_results - One row per timecard entry per run
--   6. upload_log          - Upload history for all loaders
-- =======================================================================

SET DEFINE OFF;

-- -----------------------------------------------------------------------
-- 1. ts_jira_tickets
--    Source: MS Weekly Hrs → Tickets sheet (26,524 rows, 7 columns)
--    Used for: Ticket# lookup, project comparison, fuzzy matching
-- -----------------------------------------------------------------------
CREATE TABLE ts_jira_tickets (
    id                  NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ticket_key          VARCHAR2(50)    NOT NULL,   -- e.g. YAKRM-219
    summary             VARCHAR2(1000),
    oracle_project_name VARCHAR2(500),              -- maps to Fusion project_number prefix
    jira_project_name   VARCHAR2(500),              -- Jira project display name
    labels              VARCHAR2(500),              -- space-separated Jira labels
    issue_type          VARCHAR2(100),              -- Story / Sub-task / Bug / Task
    parent              VARCHAR2(50),               -- parent ticket key if sub-task
    loaded_at           TIMESTAMP DEFAULT SYSTIMESTAMP,
    source_file         VARCHAR2(500),

    CONSTRAINT uq_ts_jira_tickets_key UNIQUE (ticket_key)
);

CREATE INDEX idx_tsjt_key_upper ON ts_jira_tickets (UPPER(ticket_key));
CREATE INDEX idx_tsjt_oracle_proj ON ts_jira_tickets (UPPER(oracle_project_name));
CREATE INDEX idx_tsjt_jira_proj ON ts_jira_tickets (UPPER(jira_project_name));
CREATE INDEX idx_tsjt_issue_type ON ts_jira_tickets (issue_type);


-- -----------------------------------------------------------------------
-- 2. ts_people
--    Source: MS Weekly Hrs → People sheet
--    Used for: VLOOKUP employee# → email (for Tempo load)
-- -----------------------------------------------------------------------
CREATE TABLE ts_people (
    id              NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    employee_number VARCHAR2(50)    NOT NULL,
    employee_name   VARCHAR2(200),
    email           VARCHAR2(200),
    loaded_at       TIMESTAMP DEFAULT SYSTIMESTAMP,
    source_file     VARCHAR2(500),

    CONSTRAINT uq_ts_people_empnum UNIQUE (employee_number)
);

CREATE INDEX idx_tsp_empnum ON ts_people (employee_number);
CREATE INDEX idx_tsp_name ON ts_people (UPPER(employee_name));


-- -----------------------------------------------------------------------
-- 3. ts_project_mapping
--    Source: MS Weekly Hrs → Project Edits sheet (23 mappings)
--    Used for: Map Oracle project name → expected Jira project name
--    Skip comparison for SHNBADM, OFAINT, GOLD prefixes (shared projects)
-- -----------------------------------------------------------------------
CREATE TABLE ts_project_mapping (
    id                  NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    oracle_project_name VARCHAR2(500)   NOT NULL,   -- Fusion project name
    jira_project_name   VARCHAR2(500)   NOT NULL,   -- expected Jira project
    is_shared_project   CHAR(1) DEFAULT 'N',        -- Y = skip project comparison
    loaded_at           TIMESTAMP DEFAULT SYSTIMESTAMP,
    source_file         VARCHAR2(500),

    CONSTRAINT uq_ts_projmap UNIQUE (oracle_project_name)
);

CREATE INDEX idx_tspm_oracle ON ts_project_mapping (UPPER(oracle_project_name));


-- -----------------------------------------------------------------------
-- 4. ts_validation_runs
--    One row per validation execution (audit trail + download link)
-- -----------------------------------------------------------------------
CREATE TABLE ts_validation_runs (
    id              NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_at          TIMESTAMP DEFAULT SYSTIMESTAMP,
    fusion_file     VARCHAR2(500),          -- uploaded Fusion XLSX filename
    jira_file       VARCHAR2(500),          -- uploaded MS Weekly Hrs XLSX filename
    fusion_rows_in  NUMBER,                 -- rows after PM/dept filter
    fusion_rows_excluded NUMBER,            -- rows excluded by filter
    total_errors    NUMBER,
    total_clean     NUMBER,
    output_file     VARCHAR2(500),          -- generated correction_output.xlsx path
    run_by          VARCHAR2(200),          -- user email or session id
    status          VARCHAR2(50) DEFAULT 'completed'
);

CREATE INDEX idx_tsvr_run_at ON ts_validation_runs (run_at DESC);
CREATE INDEX idx_tsvr_status ON ts_validation_runs (status);


-- -----------------------------------------------------------------------
-- 5. ts_validation_results
--    One row per timecard entry per validation run
--    Mirrors the "Corrections Needed" sheet in correction_output.xlsx
-- -----------------------------------------------------------------------
CREATE TABLE ts_validation_results (
    id                  NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id              NUMBER          NOT NULL,   -- FK → ts_validation_runs.id
    row_num             NUMBER,                     -- row number in source file

    -- From Fusion timecard
    timecard_status     VARCHAR2(50),
    project_number      VARCHAR2(100),
    project_name        VARCHAR2(500),
    task_name           VARCHAR2(500),
    entry_date          DATE,
    employee_number     VARCHAR2(50),
    employee_name       VARCHAR2(200),
    email               VARCHAR2(200),
    total_hours         NUMBER(6,2),
    memo                VARCHAR2(2000),

    -- Validation output
    has_error           CHAR(1) DEFAULT 'N',
    correction_note     VARCHAR2(500),          -- Column A: what Alison writes
    error_detail        VARCHAR2(500),          -- technical detail
    extracted_ticket    VARCHAR2(50),           -- ticket found in memo
    suggested_ticket    VARCHAR2(50),           -- fuzzy match suggestion
    jira_oracle_project VARCHAR2(500),
    project_match       VARCHAR2(10),           -- GOOD / BAD / SHARED / N/A
    issue_type          VARCHAR2(100),
    jira_labels         VARCHAR2(500),
    timecard_period     VARCHAR2(50),

    CONSTRAINT fk_tvr_run FOREIGN KEY (run_id) REFERENCES ts_validation_runs(id)
);

CREATE INDEX idx_tsvres_run ON ts_validation_results (run_id);
CREATE INDEX idx_tsvres_has_error ON ts_validation_results (run_id, has_error);
CREATE INDEX idx_tsvres_proj ON ts_validation_results (project_number);
CREATE INDEX idx_tsvres_emp ON ts_validation_results (employee_number);
CREATE INDEX idx_tsvres_date ON ts_validation_results (entry_date);
CREATE INDEX idx_tsvres_ticket ON ts_validation_results (extracted_ticket);
CREATE INDEX idx_tsvres_note ON ts_validation_results (correction_note);


-- -----------------------------------------------------------------------
-- 6. upload_log
--    Shared across all loader scripts (load_ms_lookups, upload_fusion, etc.)
-- -----------------------------------------------------------------------
CREATE TABLE upload_log (
    id                      NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    logged_at               TIMESTAMP DEFAULT SYSTIMESTAMP,
    table_name              VARCHAR2(100),
    filename                VARCHAR2(500),
    rows_attempted          NUMBER DEFAULT 0,
    rows_inserted           NUMBER DEFAULT 0,
    rows_skipped_duplicate  NUMBER DEFAULT 0,
    rows_skipped_filter     NUMBER DEFAULT 0,
    status                  VARCHAR2(50),   -- success / partial / error
    notes                   VARCHAR2(1000)
);

CREATE INDEX idx_ul_logged_at ON upload_log (logged_at DESC);
CREATE INDEX idx_ul_table ON upload_log (table_name);
CREATE INDEX idx_ul_status ON upload_log (status);

-- -----------------------------------------------------------------------
-- Verify: list all created tables
-- -----------------------------------------------------------------------
SELECT table_name, num_rows
FROM user_tables
WHERE table_name IN (
    'TS_JIRA_TICKETS', 'TS_PEOPLE', 'TS_PROJECT_MAPPING',
    'TS_VALIDATION_RUNS', 'TS_VALIDATION_RESULTS', 'UPLOAD_LOG'
)
ORDER BY table_name;
