{{ config(materialized='view', tags=['LdDelFlagREJT_RT_PERC_PROD_INT_MARGDel']) }}

WITH FN_DeleteRecords AS (
	SELECT
		HL_INT_RATE_ID as HL_INT_RATE_ID
	FROM {{ ref('RenameCol') }}
	UNION ALL
	SELECT
		HL_INT_RATE_ID
	FROM {{ ref('DeleteRejtTableRecsDS') }}
)

SELECT * FROM FN_DeleteRecords