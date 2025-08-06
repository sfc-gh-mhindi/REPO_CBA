{{ config(materialized='view', tags=['XfmAppt_Dept']) }}

WITH LkpApptQlfyC AS (
	SELECT
		{{ ref('Transformer') }}.SUBTYPE_CODE,
		{{ ref('Transformer') }}.APP_ID,
		{{ ref('MAP_CSE_APPT_QLFY') }}.APPT_QLFY_C,
		{{ ref('Transformer') }}.GL_DEPT_NO
	FROM {{ ref('Transformer') }}
	LEFT JOIN {{ ref('MAP_CSE_APPT_QLFY') }} ON {{ ref('Transformer') }}.SUBTYPE_CODE = {{ ref('MAP_CSE_APPT_QLFY') }}.SBTY_CODE
)

SELECT * FROM LkpApptQlfyC