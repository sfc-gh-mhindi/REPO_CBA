{{ config(materialized='view', tags=['GDWUtilProcessMetaDataFL']) }}

WITH 
util_pros_isac AS (
	SELECT
	*
	FROM {{ ref("util_pros_isac")  }}),
Lkup_UTIL_PROS_ISAC AS (SELECT BTCH_KEY_I, CAST(TRGT_M AS VARCHAR(40)) AS TRGT_M, SUM(SRCE_LOAD_CNT) AS SRCE_LOAD_CNT_SUM FROM UTIL_PROS_ISAC WHERE BTCH_KEY_I = '{{ var("pODS_BATCH_ID") }}' AND TRGT_M = '{{ var("pODS_TABLE_NAME") }}' GROUP BY BTCH_KEY_I, TRGT_M)


SELECT * FROM Lkup_UTIL_PROS_ISAC