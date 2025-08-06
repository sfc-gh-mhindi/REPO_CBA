{{ config(materialized='view', tags=['LdDelFlagREJT_CHL_BUS_FEE_DISC_FEEUpd']) }}

WITH ModConversions AS (
	SELECT 
	--Manual
	--ETL_D= date_from_string[%yyyy%mm%dd] (ETL_D)
	HL_FEE_ID, BFD_DISCOUNT_REASON, BFD_DISCOUNT_CODE, BFD_DISCOUNT_AMT, BFD_DISCOUNT_TERM, BFD_HL_FEE_DISCOUNT_CAT_ID, BFD_FOUND_FLAG, ETL_D 
	FROM {{ ref('UpdateRejtTableRecsDS') }}
)

SELECT * FROM ModConversions