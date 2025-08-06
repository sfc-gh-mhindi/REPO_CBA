{{ config(materialized='view', tags=['XfmAppt_Empl']) }}

WITH LkpApptQlfyC AS (
	SELECT
		{{ ref('Transformer') }}.SUBTYPE_CODE,
		{{ ref('Transformer') }}.APP_ID,
		{{ ref('Transformer') }}.CREATED_BY_STAFF_NUMBER,
		{{ ref('Transformer') }}.OWNED_BY_STAFF_NUMBER,
		{{ ref('MAP_CSE_APPT_QLFY') }}.APPT_QLFY_C
	FROM {{ ref('Transformer') }}
	LEFT JOIN {{ ref('MAP_CSE_APPT_QLFY') }} ON {{ ref('Transformer') }}.SUBTYPE_CODE = {{ ref('MAP_CSE_APPT_QLFY') }}.SBTY_CODE
)

SELECT * FROM LkpApptQlfyC