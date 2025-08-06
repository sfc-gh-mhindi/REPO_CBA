WITH Transformer_6 AS (
  SELECT
    CASE WHEN DSLink2.NUM_LOAD_ERR = -1 THEN 'Y' ELSE 'N' END AS AllLoadsComplete,
    CASE WHEN DSLink2.NUM_LOAD_ERR != -1 THEN 'Project: Previous day load not complete. Current ETL load aborting.' ELSE '' END AS Subject,
    CASE WHEN DSLink2.NUM_LOAD_ERR != -1 THEN 'Project: Previous day load not complete. Current ETL load aborting. Check UTIL_PROS_ISAC table to find out which target tables have not loaded successfully. check ET and UT tables for those targets.' ELSE '' END AS Message,
    CASE WHEN DSLink2.NUM_LOAD_ERR != -1 THEN ('Project: ' || Subject || 'Message: ' || Message) ELSE 'Dont send Email' END AS SendMail,
    CASE WHEN DSLink2.NUM_LOAD_ERR != -1 THEN 'Error: ' || Message ELSE '' END AS WriteErrToLog,
    DSLink2.NUM_LOAD_ERR AS NUM_LOAD_ERR
  FROM {{ ref("util_pros_isac__utilprosisacprevloadcheck") }} AS DSLink2
)

SELECT * FROM Transformer_6