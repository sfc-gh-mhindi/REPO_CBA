{{ config(materialized='view', tags=['LdREJT_CSE_CPL_BUS_APP']) }}

SELECT
	PL_APP_ID
	NOMINATED_BRANCH_ID
	PL_PACKAGE_CAT_ID
	ETL_D
	ORIG_ETL_D
	EROR_C 
FROM {{ ref('ModConversions') }}