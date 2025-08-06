{{ config(materialized='view', tags=['ExtCclappServCpty']) }}

WITH CpyCclAppSeq AS (
	SELECT
		CCL_APP_ID,
		CCL_APP_SERVICETST_ID,
		NET_SURPLUS_AMT,
		TOTAL_HOUSEHOLD_EXP_AMT
	FROM {{ ref('SrcCclAppSeq') }}
)

SELECT * FROM CpyCclAppSeq