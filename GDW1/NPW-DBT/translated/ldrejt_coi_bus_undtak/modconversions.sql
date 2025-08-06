{{ config(materialized='view', tags=['LdREJT_COI_BUS_UNDTAK']) }}

WITH ModConversions AS (
	SELECT 
	--Manual
	--ETL_D= date_from_string[%yyyy%mm%dd] (ETL_D)
	--ORIG_ETL_D= date_from_string[%yyyy%mm%dd] (ORIG_ETL_D)
	FA_UNDERTAKING_ID, PLANNING_GROUP_NAME, COIN_ADVICE_GROUP_ID, ADVICE_GROUP_CORRELATION_ID, CREATED_DATE, CREATED_BY_STAFF_NUMBER, SM_CASE_ID, ETL_D, ORIG_ETL_D, EROR_C 
	FROM {{ ref('SrcIdNullsDS') }}
)

SELECT * FROM ModConversions