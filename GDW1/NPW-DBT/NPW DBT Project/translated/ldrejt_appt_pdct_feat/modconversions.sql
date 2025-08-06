{{ config(materialized='view', tags=['LdREJT_APPT_PDCT_FEAT']) }}

WITH ModConversions AS (
	SELECT 
	--Manual
	--ETL_D= date_from_string[%yyyy%mm%dd] (ETL_D)
	--ORIG_ETL_D= date_from_string[%yyyy%mm%dd] (ORIG_ETL_D)
	APP_PROD_ID, COM_SUBTYPE_CODE, CAMPAIGN_CAT_ID, COM_APP_ID, ETL_D, ORIG_ETL_D, EROR_C 
	FROM {{ ref('FunRejectsNulls') }}
)

SELECT * FROM ModConversions