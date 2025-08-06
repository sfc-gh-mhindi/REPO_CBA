{{ config(materialized='view', tags=['XfmEvntQstrQstnResp']) }}

WITH LookUp AS (
	SELECT
		{{ ref('CSE_ONLN_BUS_OL_CLNT_RM_RATE') }}.OL_CLIENT_RM_RATING_ID,
		{{ ref('MAP_CSE_QSTR_QSTN_RESP_RM') }}.RESP_C,
		{{ ref('CSE_ONLN_BUS_OL_CLNT_RM_RATE') }}.RATING
	FROM {{ ref('CSE_ONLN_BUS_OL_CLNT_RM_RATE') }}
	LEFT JOIN {{ ref('MAP_CSE_QSTR_QSTN_RESP_RM') }} ON {{ ref('CSE_ONLN_BUS_OL_CLNT_RM_RATE') }}.RATING = {{ ref('MAP_CSE_QSTR_QSTN_RESP_RM') }}.RATING
)

SELECT * FROM LookUp