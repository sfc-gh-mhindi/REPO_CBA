{{ config(materialized='view', tags=['LdREJT_CHL_BUS_FEAT_ATTR']) }}

WITH ModConversions AS (
	SELECT 
	--Manual
	--ETL_D= date_from_string[%yyyy%mm%dd] (ETL_D)
	--ORIG_ETL_D= date_from_string[%yyyy%mm%dd] (ORIG_ETL_D)
	HL_FEATURE_ATTR_ID, HL_FEATURE_TERM, HL_FEATURE_AMOUNT, HL_FEATURE_BALANCE, HL_FEATURE_FEE, HL_FEATURE_SPEC_REPAY, HL_FEATURE_EST_INT_AMT, HL_FEATURE_DATE, HL_FEATURE_COMMENT, HL_FEATURE_CAT_ID, HL_APP_PROD_ID, ETL_D, ORIG_ETL_D, EROR_C 
	FROM {{ ref('FunJnlIdNulls') }}
)

SELECT * FROM ModConversions