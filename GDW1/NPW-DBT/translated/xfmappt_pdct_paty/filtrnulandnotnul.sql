{{ config(materialized='view', tags=['XfmAppt_Pdct_Paty']) }}

WITH FiltrNulAndNotnul AS (
	SELECT
		APP_PROD_ID,
		SUBTYPE_CODE,
		ROLE_CAT_ID,
		APP_PROD_CLIENT_ROLE_ID,
		APPT_QLFY_C,
		CIF_CODE,
		APP_PROD_ID,
		SUBTYPE_CODE,
		ROLE_CAT_ID,
		APP_PROD_CLIENT_ROLE_ID,
		PATY_ROLE_C,
		APPT_QLFY_C,
		CIF_CODE
	FROM {{ ref('Copy_of_LkpApptQlfyC') }}
)

SELECT * FROM FiltrNulAndNotnul