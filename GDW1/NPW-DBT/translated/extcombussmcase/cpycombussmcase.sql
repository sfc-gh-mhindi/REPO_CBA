{{ config(materialized='view', tags=['ExtComBusSmCase']) }}

WITH CpyComBusSmCase AS (
	SELECT
		RECORD_TYPE,
		MOD_TIMESTAMP,
		SM_CASE_ID,
		CREATED_TIMESTAMP,
		WIM_PROCESS_ID,
		DUMMY
	FROM {{ ref('SrcComBusSmCaseSeq') }}
)

SELECT * FROM CpyComBusSmCase