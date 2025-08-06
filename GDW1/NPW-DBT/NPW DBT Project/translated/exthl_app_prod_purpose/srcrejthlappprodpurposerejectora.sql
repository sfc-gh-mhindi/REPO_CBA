{{ config(materialized='view', tags=['ExtHL_APP_PROD_PURPOSE']) }}

WITH 
rejt_chl_bus_app_prod_purp AS (
	SELECT
	*
	FROM {{ source("cse4_stg","rejt_chl_bus_app_prod_purp")  }}),
SrcRejtHlAppProdPurposeRejectOra AS (SELECT HL_APP_PROD_PURPOSE_ID, HL_APP_PROD_ID, HL_LOAN_PURPOSE_CAT_ID, AMOUNT, MAIN_PURPOSE, CAST(ORIG_ETL_D AS TEXT) AS ORIG_ETL_D FROM REJT_CHL_BUS_APP_PROD_PURP WHERE EROR_C LIKE 'RPR%')


SELECT * FROM SrcRejtHlAppProdPurposeRejectOra