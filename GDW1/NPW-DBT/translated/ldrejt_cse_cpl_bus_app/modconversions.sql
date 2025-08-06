{{ config(materialized='view', tags=['LdREJT_CSE_CPL_BUS_APP']) }}

WITH ModConversions AS (
	SELECT 
	--Manual
	--ETL_D= date_from_string[%yyyy%mm%dd] (ETL_D)
	--ORIG_ETL_D= date_from_string[%yyyy%mm%dd] (ORIG_ETL_D)
	PL_APP_ID, NOMINATED_BRANCH_ID, PL_PACKAGE_CAT_ID, ETL_D, ORIG_ETL_D, EROR_C 
	FROM {{ ref('FunCseCplBusNulls') }}
)

SELECT * FROM ModConversions