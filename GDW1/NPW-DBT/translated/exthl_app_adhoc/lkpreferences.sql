{{ config(materialized='view', tags=['ExtHL_APP_Adhoc']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('XfmCheckHlAppIdNulls__OutCheckHlAppIdNullsSorted') }}.HL_APP_ID,
		{{ ref('XfmCheckHlAppIdNulls__OutCheckHlAppIdNullsSorted') }}.HL_APP_PROD_ID,
		{{ ref('XfmCheckHlAppIdNulls__OutCheckHlAppIdNullsSorted') }}.PEXA_FLAG,
		{{ ref('MAP_CSE_APPT_PDCT_FEAT') }}.FEAT_VALU_C
	FROM {{ ref('XfmCheckHlAppIdNulls__OutCheckHlAppIdNullsSorted') }}
	LEFT JOIN {{ ref('MAP_CSE_APPT_PDCT_FEAT') }} ON {{ ref('XfmCheckHlAppIdNulls__OutCheckHlAppIdNullsSorted') }}.PEXA_FLAG = {{ ref('MAP_CSE_APPT_PDCT_FEAT') }}.PEXA_FLAG
)

SELECT * FROM LkpReferences