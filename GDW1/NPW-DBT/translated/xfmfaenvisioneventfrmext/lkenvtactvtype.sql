{{ config(materialized='view', tags=['XfmFaEnvisionEventFrmExt']) }}

WITH LkEnvtActvType AS (
	SELECT
		{{ ref('CpyRename') }}.FA_ENVISION_EVENT_ID,
		{{ ref('CpyRename') }}.FA_UNDERTAKING_ID,
		{{ ref('CpyRename') }}.FA_ENV_EVNT_CAT_ID,
		{{ ref('CpyRename') }}.CREATED_DATE,
		{{ ref('CpyRename') }}.CREATED_BY_STAFF_NUMBER,
		{{ ref('CpyRename') }}.COIN_REQUEST_ID,
		{{ ref('CpyRename') }}.ORIG_ETL_D,
		{{ ref('SrcMAP_CSE_ENV_EVNT_ACTV_TYPELks') }}.EVNT_ACTV_TYPE_C
	FROM {{ ref('CpyRename') }}
	LEFT JOIN {{ ref('SrcMAP_CSE_ENV_EVNT_ACTV_TYPELks') }} ON 
)

SELECT * FROM LkEnvtActvType