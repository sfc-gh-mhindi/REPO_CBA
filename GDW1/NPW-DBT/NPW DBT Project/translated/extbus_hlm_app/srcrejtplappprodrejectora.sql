{{ config(materialized='view', tags=['ExtBUS_HLM_APP']) }}

WITH 
rejt_chl_bus_hlm_app AS (
	SELECT
	*
	FROM {{ source("cse4_stg_dsv81test","rejt_chl_bus_hlm_app")  }}),
SrcRejtPLAppProdRejectOra AS (SELECT HLM_APP_ID, HLM_ACCOUNT_ID, ACCOUNT_NUMBER, CRIS_PRODUCT_ID, HLM_APP_TYPE_CAT_ID, DISCHARGE_REASON_ID, HL_APP_PROD_ID, ORIG_ETL_D FROM REJT_CHL_BUS_HLM_APP)


SELECT * FROM SrcRejtPLAppProdRejectOra