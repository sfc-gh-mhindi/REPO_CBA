{{ config(materialized='view', tags=['LdDelFlagREJT_CPL_BUS_INT_RATE_AMT_MARGIns']) }}

WITH ModConversions AS (
	SELECT 
	--Manual
	--ETL_D= date_from_string[%yyyy%mm%dd] (ETL_D)
	--ORIG_ETL_D= date_from_string[%yyyy%mm%dd] (ORIG_ETL_D)
	PL_INT_RATE_ID, PL_MARGIN_PL_MARGIN_ID, PL_MARGIN_PL_INT_RATE_ID, PL_MARGIN_MARGIN_RESN_CAT_ID, PL_MARGIN_FOUND_FLAG, PL_INT_RATE_FOUND_FLAG, PL_INT_RATE_AMT_FOUND_FLAG, ETL_D, ORIG_ETL_D, EROR_C 
	FROM {{ ref('InsertRejtTableRecsDS') }}
)

SELECT * FROM ModConversions