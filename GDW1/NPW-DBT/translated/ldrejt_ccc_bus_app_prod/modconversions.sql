{{ config(materialized='view', tags=['LdREJT_CCC_BUS_APP_PROD']) }}

WITH ModConversions AS (
	SELECT 
	--Manual
	--ETL_D= date_from_string[%yyyy%mm%dd] (ETL_D)
	--ORIG_ETL_D= date_from_string[%yyyy%mm%dd] (ORIG_ETL_D)
	CC_APP_PROD_ID, REQUESTED_LIMIT_AMT, CC_INTEREST_OPT_CAT_ID, CBA_HOMELOAN_NO, PRE_APPRV_AMOUNT, ETL_D, ORIG_ETL_D, EROR_C, READ_COSTS_AND_RISKS_FLAG, ACCEPTS_COSTS_AND_RISKS_DATE 
	FROM {{ ref('FunCCAppProdIdNulls') }}
)

SELECT * FROM ModConversions