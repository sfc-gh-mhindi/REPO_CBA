{{ config(materialized='view', tags=['XfmAppt_Pdct_Paty']) }}

WITH Copy_of_LkpApptQlfyC AS (
	SELECT
		{{ ref('Transformer') }}.APP_PROD_ID,
		{{ ref('Transformer') }}.SUBTYPE_CODE,
		{{ ref('Transformer') }}.ROLE_CAT_ID,
		{{ ref('Transformer') }}.ROLE_CAT_ID_CHK,
		{{ ref('Transformer') }}.APP_PROD_CLIENT_ROLE_ID,
		{{ ref('Transformer') }}.PATY_ROLE_C,
		{{ ref('Copy_of_MAP_CSE_APPT_QLFY') }}.APPT_QLFY_C,
		{{ ref('Transformer') }}.CIF_CODE
	FROM {{ ref('Transformer') }}
	LEFT JOIN {{ ref('Copy_of_MAP_CSE_APPT_QLFY') }} ON {{ ref('Transformer') }}.SUBTYPE_CODE = {{ ref('Copy_of_MAP_CSE_APPT_QLFY') }}.SBTY_CODE
)

SELECT * FROM Copy_of_LkpApptQlfyC