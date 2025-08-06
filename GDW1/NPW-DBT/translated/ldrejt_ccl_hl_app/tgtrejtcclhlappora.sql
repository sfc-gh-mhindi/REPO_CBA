{{ config(materialized='view', tags=['LdREJT_CCL_HL_APP']) }}

SELECT
	CCL_HL_APP_ID
	CCL_APP_ID
	HL_APP_ID
	LMI_AMT
	HL_PACKAGE_CAT_ID
	ETL_D
	ORIG_ETL_D
	EROR_C 
FROM {{ ref('ModConversions') }}