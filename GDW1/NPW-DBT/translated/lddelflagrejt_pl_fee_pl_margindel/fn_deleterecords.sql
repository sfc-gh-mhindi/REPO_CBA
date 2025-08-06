{{ config(materialized='view', tags=['LdDelFlagREJT_PL_FEE_PL_MARGINDel']) }}

WITH FN_DeleteRecords AS (
	SELECT
		PL_FEE_ID as PL_FEE_ID
	FROM {{ ref('RenameCol') }}
	UNION ALL
	SELECT
		PL_FEE_ID
	FROM {{ ref('DeleteRejtTableRecsDS') }}
)

SELECT * FROM FN_DeleteRecords