
call pst.svcs.sp_set_account_context (50973,'AU');
USE DATABASE PST;
USE SCHEMA PST.SVCS;

--TABLES ACCESSED BY SPECIFIC WAREHOUSES
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
)
-- SELECT * FROM EXPENSIVE_QRY;
SELECT
    f1.value:"objectName"::string AS table_name,
    COUNT(*) AS access_count 
FROM
    PST.SVCS.access_history,
    LATERAL FLATTEN(input => base_objects_accessed) f1 
WHERE
    f1.value:"objectDomain"::string = 'Table'
 GROUP BY
    f1.value:"objectName"::string--table_name
 ORDER BY
    access_count DESC;

/*
What to Look For:
Hot tables: Frequently accessed across multiple warehouses
Scan patterns: Large table scans vs targeted queries
Cache efficiency: Tables with low cache hit rates
Cross-warehouse access: Same tables used by different warehouses
*/


