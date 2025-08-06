{{ config(materialized='view', tags=['LdDelFlagREJT_PL_FEE_PL_MARGINIns']) }}

WITH ModConversions AS (
	SELECT 
	--Manual
	--ETL_D= date_from_string[%yyyy%mm%dd] (ETL_D)
	--ORIG_ETL_D= date_from_string[%yyyy%mm%dd] (ORIG_ETL_D)
	PL_FEE_ID, PL_APP_PROD_ID, PL_FEE_PL_APP_PROD_ID, PL_MARGIN_PL_MARGIN_ID, PL_MARGIN_PL_FEE_ID, PL_MARGIN_MARGIN_REASON_CAT_ID, PL_FEE_FOUND_FLAG, PL_MARGIN_FOUND_FLAG, ETL_D, ORIG_ETL_D, EROR_C 
	FROM {{ ref('InsertRejtTableRecsDS') }}
)

SELECT * FROM ModConversions