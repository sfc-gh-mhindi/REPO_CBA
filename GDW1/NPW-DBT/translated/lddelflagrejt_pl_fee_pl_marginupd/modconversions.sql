{{ config(materialized='view', tags=['LdDelFlagREJT_PL_FEE_PL_MARGINUpd']) }}

WITH ModConversions AS (
	SELECT 
	--Manual
	--ETL_D= date_from_string[%yyyy%mm%dd] (ETL_D)
	PL_FEE_ID, PL_MARGIN_MARGIN_AMT, PL_MARGIN_MARGIN_REASON_CAT_ID, PL_MARGIN_FOUND_FLAG, ETL_D 
	FROM {{ ref('UpdateRejtTableRecsDS') }}
)

SELECT * FROM ModConversions