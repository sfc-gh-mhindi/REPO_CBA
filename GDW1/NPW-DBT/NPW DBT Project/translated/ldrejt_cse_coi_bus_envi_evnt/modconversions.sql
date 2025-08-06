{{ config(materialized='view', tags=['LdREJT_CSE_COI_BUS_ENVI_EVNT']) }}

WITH ModConversions AS (
	SELECT 
	--Manual
	--ETL_D= date_from_string[%yyyy%mm%dd] (ETL_D)
	--ORIG_ETL_D= date_from_string[%yyyy%mm%dd] (ORIG_ETL_D)
	FA_ENVISION_EVENT_ID, FA_UNDERTAKING_ID, FA_ENV_EVNT_CAT_ID AS FA_ENVISION_EVENT_CAT_ID, CREATED_DATE, CREATED_BY_STAFF_NUMBER, COIN_REQUEST_ID, ETL_D, ORIG_ETL_D, EROR_C 
	FROM {{ ref('FunRejectsNulls') }}
)

SELECT * FROM ModConversions