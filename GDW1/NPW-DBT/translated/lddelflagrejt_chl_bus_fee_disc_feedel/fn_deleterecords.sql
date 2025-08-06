{{ config(materialized='view', tags=['LdDelFlagREJT_CHL_BUS_FEE_DISC_FEEDel']) }}

WITH FN_DeleteRecords AS (
	SELECT
		HL_FEE_ID as HL_FEE_ID
	FROM {{ ref('RenameCol') }}
	UNION ALL
	SELECT
		HL_FEE_ID
	FROM {{ ref('DeleteRejtTableRecsDS') }}
)

SELECT * FROM FN_DeleteRecords