-- ─────────────────────────────────────────────────────────────
-- seed_projects.sql
-- Assigns project_number and project_name to existing rows
-- in public.fusion_time_entries
--
-- Run after connecting via SSH tunnel:
--   psql -h localhost -p 5433 -U pgadmin -d agentdb -f seed_projects.sql
-- ─────────────────────────────────────────────────────────────

-- Project assignments by employee name
UPDATE public.fusion_time_entries
SET
    project_number = CASE
        WHEN employee IN ('Alice Johnson', 'Bob Smith', 'Carol White')
            THEN 'P-1001'
        WHEN employee IN ('David Brown', 'Emma Davis', 'Frank Miller')
            THEN 'P-1002'
        WHEN employee IN ('Grace Lee', 'Henry Wilson')
            THEN 'P-1003'
        WHEN employee IN ('Iris Chen', 'Jack Taylor')
            THEN 'P-1004'
        ELSE 'P-1001'   -- default for any other employees
    END,
    project_name = CASE
        WHEN employee IN ('Alice Johnson', 'Bob Smith', 'Carol White')
            THEN 'Oracle Fusion ERP Implementation'
        WHEN employee IN ('David Brown', 'Emma Davis', 'Frank Miller')
            THEN 'Time and Labor Module'
        WHEN employee IN ('Grace Lee', 'Henry Wilson')
            THEN 'Payroll Integration'
        WHEN employee IN ('Iris Chen', 'Jack Taylor')
            THEN 'HR Analytics Dashboard'
        ELSE 'Oracle Fusion ERP Implementation'
    END
WHERE project_number IS NULL;   -- only update rows not already set

-- Verify the result
SELECT
    project_number,
    project_name,
    COUNT(*)        AS row_count,
    COUNT(DISTINCT employee) AS employees
FROM public.fusion_time_entries
GROUP BY project_number, project_name
ORDER BY project_number;
