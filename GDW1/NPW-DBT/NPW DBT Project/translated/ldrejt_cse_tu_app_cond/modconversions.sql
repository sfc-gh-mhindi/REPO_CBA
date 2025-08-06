{{ config(materialized='view', tags=['LdREJT_CSE_TU_APP_COND']) }}

WITH ModConversions AS (
	SELECT 
	--Manual
	--ETL_D= date_from_string[%yyyy%mm%dd] (ETL_D)
	--ORIG_ETL_D= date_from_string[%yyyy%mm%dd] (ORIG_ETL_D)
	--CONDITION_MET_DATE=date_from_string[%yyyy%mm%dd] (CONDITION_MET_DATE)
	--TU_APP_CONDITION_CAT_ID=COND_APPT_CAT_ID
	--SUBTYPE_CODE = SBTY_CODE
	SUBTYPE_CODE, HL_APP_PROD_ID, TU_APP_CONDITION_ID, TU_APP_CONDITION_CAT_ID, CONDITION_MET_DATE, ETL_D, ORIG_ETL_D, EROR_C 
	FROM {{ ref('TgtMapApptQfyRejectsDS1') }}
)

SELECT * FROM ModConversions