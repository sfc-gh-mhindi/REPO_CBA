{{ config(materialized='view', tags=['XfmAppt_Pdct_Paty']) }}

WITH LkpApptQlfyC AS (
	SELECT
		{{ ref('FiltrNulAndNotnul') }}.SUBTYPE_CODE,
		{{ ref('FiltrNulAndNotnul') }}.APP_PROD_ID,
		{{ ref('FiltrNulAndNotnul') }}.ROLE_CAT_ID,
		{{ ref('FiltrNulAndNotnul') }}.APP_PROD_CLIENT_ROLE_ID,
		{{ ref('FiltrNulAndNotnul') }}.APPT_QLFY_C,
		{{ ref('MAP_CSE_APPT_PDCT_PATY_ROLE') }}.PATY_ROLE_C,
		{{ ref('FiltrNulAndNotnul') }}.CIF_CODE
	FROM {{ ref('FiltrNulAndNotnul') }}
	LEFT JOIN {{ ref('MAP_CSE_APPT_PDCT_PATY_ROLE') }} ON {{ ref('FiltrNulAndNotnul') }}.ROLE_CAT_ID = {{ ref('MAP_CSE_APPT_PDCT_PATY_ROLE') }}.ROLE_CAT_ID
	WHERE ROLE_CAT_ID_CHK = 'N'
)

SELECT * FROM LkpApptQlfyC