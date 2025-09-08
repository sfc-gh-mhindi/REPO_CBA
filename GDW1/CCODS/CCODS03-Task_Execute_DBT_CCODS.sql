-- CCODS DBT Task Creation Script
-- This task orchestrates the execution of CCODS DBT models for PLAN_BALN_SEGM_MSTR_NPW table population

-- Create the scheduled task for CCODS DBT execution
CREATE OR REPLACE TASK NPD_D12_DMN_GDWMIG.TMP.T_EXECUTE_DBT_CCODS
    WAREHOUSE = wh_usr_npd_d12_gdwmig_001
    SCHEDULE = 'USING CRON 0 4 * * * Australia/Sydney'  -- Daily at 4:00 AM (1 hour after CSEL)
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT = 'CCODS DBT execution task - Processes BCFINSG data and populates PLAN_BALN_SEGM_MSTR_NPW table'
AS
    CALL NPD_D12_DMN_GDWMIG.TMP.P_EXECUTE_DBT_CCODS();

-- Task management commands (commented out - use as needed)

-- To start the task:
-- ALTER TASK NPD_D12_DMN_GDWMIG.TMP.T_EXECUTE_DBT_CCODS RESUME;

-- To suspend the task:
-- ALTER TASK NPD_D12_DMN_GDWMIG.TMP.T_EXECUTE_DBT_CCODS SUSPEND;

-- To check task status:
-- SHOW TASKS LIKE 'T_EXECUTE_DBT_CCODS' IN SCHEMA NPD_D12_DMN_GDWMIG.TMP;

-- To view task execution history:
-- SELECT 
--     QUERY_ID, 
--     STATE, 
--     QUERY_START_TIME, 
--     QUERY_END_TIME,
--     TOTAL_ELAPSED_TIME,
--     ERROR_CODE,
--     ERROR_MESSAGE
-- FROM TABLE(NPD_D12_DMN_GDWMIG.INFORMATION_SCHEMA.TASK_HISTORY(
--     TASK_NAME => 'T_EXECUTE_DBT_CCODS'
-- )) A
-- WHERE SCHEMA_NAME = 'TMP'
-- ORDER BY A.QUERY_START_TIME DESC
-- LIMIT 10;

-- To manually execute the procedure (for testing):
-- CALL NPD_D12_DMN_GDWMIG.TMP.P_EXECUTE_DBT_CCODS();

-- To view execution results from a specific run:
-- SELECT * FROM TABLE(RESULT_SCAN('<QUERY_ID_FROM_TASK_HISTORY>')); 