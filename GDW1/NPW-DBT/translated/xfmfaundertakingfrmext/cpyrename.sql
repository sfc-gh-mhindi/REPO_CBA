{{ config(materialized='view', tags=['XfmFaUndertakingFrmExt']) }}

WITH CpyRename AS (
	SELECT
		FA_UNDERTAKING_ID,
		PLANNING_GROUP_NAME,
		COIN_ADVICE_GROUP_ID,
		ADVICE_GROUP_CORRELATION_ID,
		CREATED_DATE,
		CREATED_BY_STAFF_NUMBER,
		SM_CASE_ID,
		ORIG_ETL_D
	FROM {{ ref('SrcFAUndertakingDS') }}
)

SELECT * FROM CpyRename