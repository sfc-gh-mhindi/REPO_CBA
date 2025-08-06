{{ config(materialized='view', tags=['LdDelFlagREJT_RT_PERC_PROD_INT_MARGIns']) }}

WITH ModConversions AS (
	SELECT 
	--Manual
	--ETL_D= date_from_string[%yyyy%mm%dd] (ETL_D)
	--ORIG_ETL_D= date_from_string[%yyyy%mm%dd] (ORIG_ETL_D)
	HL_INT_RATE_ID, MARG_HL_PROD_INT_MARGIN_ID, MARG_HL_INT_RATE_ID, MARG_HL_PROD_INT_MARGIN_CAT_ID, RATE_FOUND_FLAG, PERC_FOUND_FLAG, MARG_FOUND_FLAG, ETL_D, ORIG_ETL_D, EROR_C 
	FROM {{ ref('InsertRejtTableRecsDS') }}
)

SELECT * FROM ModConversions