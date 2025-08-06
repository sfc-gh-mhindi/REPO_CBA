{{ config(materialized='view', tags=['ExtCC_APP_PROD']) }}

WITH 
rejt_ccc_bus_app_prod AS (
	SELECT
	*
	FROM {{ source("cse4_stg_dsv81test","rejt_ccc_bus_app_prod")  }}),
SrcRejtCCAppProdRejectOra AS (SELECT CC_APP_PROD_ID, REQUESTED_LIMIT_AMT, CC_INTEREST_OPT_CAT_ID, CBA_HOMELOAN_NO, PRE_APPRV_AMOUNT, CAST(ORIG_ETL_D AS TEXT) AS ORIG_ETL_D, READ_COSTS_AND_RISKS_FLAG, ACCEPTS_COSTS_AND_RISKS_DATE FROM REJT_CCC_BUS_APP_PROD WHERE EROR_C LIKE 'RPR%')


SELECT * FROM SrcRejtCCAppProdRejectOra