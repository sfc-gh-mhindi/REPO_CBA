{{ config(materialized='view', tags=['ExtCC_APP_PROD_BAL_XFER']) }}

WITH 
rejt_ccc_bus_app_prod_bal_xfer AS (
	SELECT
	*
	FROM {{ source("cse4_stg","rejt_ccc_bus_app_prod_bal_xfer")  }}),
SrcRejtCCAppProdBalXferRejectOra AS (SELECT CC_APP_PROD_ID, CC_APP_PROD_BAL_XFER_ID, BAL_XFER_OPTION_CAT_ID, XFER_AMT, BAL_XFER_INSTITUTION_CAT_ID, CC_APP_ID, CAST(ORIG_ETL_D AS TEXT) AS ORIG_ETL_D FROM REJT_CCC_BUS_APP_PROD_BAL_XFER WHERE EROR_C LIKE 'RPR%')


SELECT * FROM SrcRejtCCAppProdBalXferRejectOra