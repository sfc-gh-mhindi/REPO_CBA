{{ config(materialized='view', tags=['LdREJT_CHL_BUS_APP_PROD_PURP']) }}

WITH ModConversions AS (
	SELECT 
	--Manual
	--ETL_D= date_from_string[%yyyy%mm%dd] (ETL_D)
	--ORIG_ETL_D= date_from_string[%yyyy%mm%dd] (ORIG_ETL_D)
	HL_APP_PROD_PURPOSE_ID, HL_APP_PROD_ID, HL_LOAN_PURPOSE_CAT_ID, AMOUNT, MAIN_PURPOSE, ETL_D, ORIG_ETL_D, EROR_C 
	FROM {{ ref('FunChlBusAppProdPurpNulls') }}
)

SELECT * FROM ModConversions