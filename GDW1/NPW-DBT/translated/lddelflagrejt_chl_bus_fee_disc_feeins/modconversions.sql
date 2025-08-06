{{ config(materialized='view', tags=['LdDelFlagREJT_CHL_BUS_FEE_DISC_FEEIns']) }}

WITH ModConversions AS (
	SELECT 
	--Manual
	--ETL_D= date_from_string[%yyyy%mm%dd] (ETL_D)
	--ORIG_ETL_D= date_from_string[%yyyy%mm%dd] (ORIG_ETL_D)
	HL_FEE_ID, BFD_HL_FEE_DISCOUNT_ID, BFD_HL_FEE_ID, BFD_DISCOUNT_AMT, BFD_DISCOUNT_TERM, BFD_HL_FEE_DISCOUNT_CAT_ID, BF_FOUND_FLAG, BFD_FOUND_FLAG, ETL_D, ORIG_ETL_D, EROR_C 
	FROM {{ ref('InsertRejtTableRecsDS') }}
)

SELECT * FROM ModConversions