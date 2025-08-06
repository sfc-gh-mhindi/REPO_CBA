{{ config(materialized='view', tags=['XfmAppt_Stus']) }}

WITH Lkp AS (
	SELECT
		{{ ref('TransXfm') }}.APP_ID,
		{{ ref('TransXfm') }}.SUBTYPE_CODE,
		{{ ref('TransXfm') }}.SM_STATE_CAT_ID,
		{{ ref('TransXfm') }}.START_D,
		{{ ref('TransXfm') }}.END_D,
		{{ ref('TransXfm') }}.CREATED_BY_STAFF_NUMBER,
		{{ ref('MAP_CSE_APPT_QLFY') }}.APPT_QLFY_C
	FROM {{ ref('TransXfm') }}
	LEFT JOIN {{ ref('MAP_CSE_APPT_QLFY') }} ON {{ ref('TransXfm') }}.SUBTYPE_CODE = {{ ref('MAP_CSE_APPT_QLFY') }}.SBTY_CODE
)

SELECT * FROM Lkp