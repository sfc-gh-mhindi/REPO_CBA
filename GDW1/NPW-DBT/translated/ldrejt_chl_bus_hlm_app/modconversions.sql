{{ config(materialized='view', tags=['LdREJT_CHL_BUS_HLM_APP']) }}

WITH ModConversions AS (
	SELECT 
	--Manual
	--ETL_D= date_from_string[%yyyy%mm%dd] (ETL_D)
	--ORIG_ETL_D= date_from_string[%yyyy%mm%dd] (ORIG_ETL_D)
	HLM_APP_ID, HLM_ACCOUNT_ID, ACCOUNT_NUMBER, CRIS_PRODUCT_ID, HLM_APP_TYPE_CAT_ID, DISCHARGE_REASON_ID, HL_APP_PROD_ID, ETL_D, ORIG_ETL_D, EROR_C 
	FROM {{ ref('Copy_89') }}
)

SELECT * FROM ModConversions