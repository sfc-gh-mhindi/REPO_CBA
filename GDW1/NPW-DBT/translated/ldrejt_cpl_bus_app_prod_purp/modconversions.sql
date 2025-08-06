{{ config(materialized='view', tags=['LdREJT_CPL_BUS_APP_PROD_PURP']) }}

WITH ModConversions AS (
	SELECT 
	--Manual
	--ETL_D= date_from_string[%yyyy%mm%dd] (ETL_D)
	--ORIG_ETL_D= date_from_string[%yyyy%mm%dd] (ORIG_ETL_D)
	PL_APP_PROD_PURP_ID, PL_APP_PROD_PURP_CAT_ID, AMT, PL_APP_PROD_ID, ETL_D, ORIG_ETL_D, EROR_C 
	FROM {{ ref('FunCplBusAppProdPurpNulls') }}
)

SELECT * FROM ModConversions