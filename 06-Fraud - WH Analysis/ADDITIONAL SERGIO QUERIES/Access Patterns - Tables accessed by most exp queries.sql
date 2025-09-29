-- FRAUMD Warehouse Analysis Queries
-- Filtered for specific FRAUMD warehouses

call pst.svcs.sp_set_account_context (50973,'AU');
USE DATABASE PST;
USE SCHEMA PST.SVCS;

--TABLES ACCESSED BY TOP 200 MOST EXPENSIVE QUERIES
WITH WAREHOUSE_SIZE AS
(
     SELECT WAREHOUSE_SIZE, NODES
       FROM (
              SELECT 'X-SMALL' AS WAREHOUSE_SIZE, 1 AS NODES
              UNION ALL
              SELECT 'SMALL' AS WAREHOUSE_SIZE, 2 AS NODES
              UNION ALL
              SELECT 'MEDIUM' AS WAREHOUSE_SIZE, 4 AS NODES
              UNION ALL
              SELECT 'LARGE' AS WAREHOUSE_SIZE, 8 AS NODES
              UNION ALL
              SELECT 'X-LARGE' AS WAREHOUSE_SIZE, 16 AS NODES
              UNION ALL
              SELECT '2X-LARGE' AS WAREHOUSE_SIZE, 32 AS NODES
              UNION ALL
              SELECT '3X-LARGE' AS WAREHOUSE_SIZE, 64 AS NODES
              UNION ALL
              SELECT '4X-LARGE' AS WAREHOUSE_SIZE, 128 AS NODES
            )
),
QH AS
(
     SELECT QH.QUERY_ID
           ,QH.QUERY_TEXT
           ,QH.USER_NAME
           ,QH.ROLE_NAME
           ,QH.EXECUTION_TIME
           ,QH.WAREHOUSE_SIZE
           ,QH.WAREHOUSE_NAME
      FROM QUERY_HISTORY QH
     WHERE warehouse_name IN ('WH_USR_PRD_P01_FRAUMD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_002', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_003') AND START_TIME > DATEADD(month,-2,CURRENT_TIMESTAMP())
),
EXPENSIVE_QRY AS
(
SELECT QH.QUERY_ID
      -- ,QH.QUERY_TEXT
      -- ,QH.USER_NAME
      -- ,QH.ROLE_NAME
      ,QH.EXECUTION_TIME as EXECUTION_TIME_MILLISECONDS
      ,(QH.EXECUTION_TIME/(1000)) as EXECUTION_TIME_SECONDS
      ,(QH.EXECUTION_TIME/(1000*60)) AS EXECUTION_TIME_MINUTES
      ,(QH.EXECUTION_TIME/(1000*60*60)) AS EXECUTION_TIME_HOURS,
      QH.WAREHOUSE_NAME
    --   ,WS.WAREHOUSE_SIZE
    --   ,WS.NODES
      ,(QH.EXECUTION_TIME/(1000*60*60))*WS.NODES as RELATIVE_PERFORMANCE_COST

FROM  QH
JOIN WAREHOUSE_SIZE WS ON WS.WAREHOUSE_SIZE = upper(QH.WAREHOUSE_SIZE)
ORDER BY RELATIVE_PERFORMANCE_COST DESC
LIMIT 200
)
-- SELECT * FROM EXPENSIVE_QRY;
SELECT
    f1.value:"objectName"::string AS table_name,
    COUNT(*) AS access_count,
    EXPENSIVE_QRY.WAREHOUSE_NAME
FROM
    PST.SVCS.access_history,
    LATERAL FLATTEN(input => base_objects_accessed) f1
    ,EXPENSIVE_QRY
WHERE
    f1.value:"objectDomain"::string = 'Table'
AND EXPENSIVE_QRY.QUERY_ID = access_history.query_id
    -- AND query_start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
    -- and query_id = '01b8d355-3203-deb8-0000-c71d0f5b5faa'
    -- AND QUERY_ID IN (
    --     SELECT DISTINCT QUERY_ID FROM EXPENSIVE_QRY
    -- )
GROUP BY
    f1.value:"objectName"::string--table_name
    ,EXPENSIVE_QRY.WAREHOUSE_NAME
ORDER BY
    access_count DESC;

/*
What to Look For:
Hot tables: Frequently accessed across multiple warehouses
Scan patterns: Large table scans vs targeted queries
Cache efficiency: Tables with low cache hit rates
Cross-warehouse access: Same tables used by different warehouses
*/
