{{ config(materialized='view', tags=['LdDelFlagREJT_CPL_BUS_INT_RATE_AMT_MARGDel']) }}

WITH FN_DeleteRecords AS (
	SELECT
		PL_INT_RATE_ID as PL_INT_RATE_ID
	FROM {{ ref('RenameCol') }}
	UNION ALL
	SELECT
		PL_INT_RATE_ID
	FROM {{ ref('DeleteRejtTableRecsDS') }}
)

SELECT * FROM FN_DeleteRecords