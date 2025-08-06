{{ config(materialized='view', tags=['ExtFaEnvisionEvent']) }}

WITH CpyRejtRejectOra AS (
	SELECT
		FA_ENVISION_EVENT_ID,
		{{ ref('SrcRejtRejectOra') }}.FA_UNDERTAKING_ID AS FA_UNDERTAKING_ID_R,
		{{ ref('SrcRejtRejectOra') }}.FA_ENVISION_EVENT_CAT_ID AS FA_ENVISION_EVENT_CAT_ID_R,
		{{ ref('SrcRejtRejectOra') }}.CREATED_DATE AS CREATED_DATE_R,
		{{ ref('SrcRejtRejectOra') }}.CREATED_BY_STAFF_NUMBER AS CREATED_BY_STAFF_NUMBER_R,
		{{ ref('SrcRejtRejectOra') }}.COIN_REQUEST_ID AS COIN_REQUEST_ID_R,
		{{ ref('SrcRejtRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R
	FROM {{ ref('SrcRejtRejectOra') }}
)

SELECT * FROM CpyRejtRejectOra