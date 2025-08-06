{{ config(materialized='view', tags=['XfmFaEnvisionEventFrmExt']) }}

WITH CpyRename AS (
	SELECT
		FA_ENVISION_EVENT_ID,
		FA_UNDERTAKING_ID,
		{{ ref('SrcFAEnvisionEventDS') }}.FA_ENVISION_EVENT_CAT_ID AS FA_ENV_EVNT_CAT_ID,
		CREATED_DATE,
		CREATED_BY_STAFF_NUMBER,
		COIN_REQUEST_ID,
		ORIG_ETL_D
	FROM {{ ref('SrcFAEnvisionEventDS') }}
)

SELECT * FROM CpyRename