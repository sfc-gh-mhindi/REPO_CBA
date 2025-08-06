{{ config(materialized='view', tags=['LdREJT_CCL_HL_APP']) }}

WITH ModConversions AS (
	SELECT 
	--Manual
	--ETL_D= date_from_string[%yyyy%mm%dd] (ETL_D)
	--ORIG_ETL_D= date_from_string[%yyyy%mm%dd] (ORIG_ETL_D)
	CCL_HL_APP_ID, CCL_APP_ID, HL_APP_ID, LMI_AMT, HL_PACKAGE_CAT_ID, ETL_D, ORIG_ETL_D, EROR_C 
	FROM {{ ref('RejCclHlAppDS') }}
)

SELECT * FROM ModConversions