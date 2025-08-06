{{ config(materialized='view', tags=['XfmAppt_Stus']) }}

WITH Lkp_SM_CASE_STUS AS (
	SELECT
		{{ ref('Lkp') }}.APP_ID,
		{{ ref('Lkp') }}.SUBTYPE_CODE,
		{{ ref('Lkp') }}.SM_STATE_CAT_ID,
		{{ ref('Lkp') }}.START_D,
		{{ ref('Lkp') }}.END_D,
		{{ ref('Lkp') }}.CREATED_BY_STAFF_NUMBER,
		{{ ref('Lkp') }}.APPT_QLFY_C,
		{{ ref('MAP_CSE_SM_CASE_STUS') }}.STUS_C
	FROM {{ ref('Lkp') }}
	LEFT JOIN {{ ref('MAP_CSE_SM_CASE_STUS') }} ON {{ ref('Lkp') }}.SM_STATE_CAT_ID = {{ ref('MAP_CSE_SM_CASE_STUS') }}.SM_STAT_CAT_ID
)

SELECT * FROM Lkp_SM_CASE_STUS