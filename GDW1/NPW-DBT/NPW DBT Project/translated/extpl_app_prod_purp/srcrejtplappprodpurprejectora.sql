{{ config(materialized='view', tags=['ExtPL_APP_PROD_PURP']) }}

WITH 
rejt_cpl_bus_app_prod_purp AS (
	SELECT
	*
	FROM {{ source("cse4_stg","rejt_cpl_bus_app_prod_purp")  }}),
SrcRejtPlAppProdPurpRejectOra AS (SELECT PL_APP_PROD_PURP_ID, PL_APP_PROD_PURP_CAT_ID, AMT, PL_APP_PROD_ID, CAST(ORIG_ETL_D AS TEXT) AS ORIG_ETL_D FROM REJT_CPL_BUS_APP_PROD_PURP WHERE EROR_C LIKE 'RPR%')


SELECT * FROM SrcRejtPlAppProdPurpRejectOra