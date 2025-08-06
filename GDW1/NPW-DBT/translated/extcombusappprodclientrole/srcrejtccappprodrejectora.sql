{{ config(materialized='view', tags=['ExtComBusAppProdClientRole']) }}

WITH 
rejt_com_bus_ap_prd_clnt_rl AS (
	SELECT
	*
	FROM {{ source("cse4_stg","rejt_com_bus_ap_prd_clnt_rl")  }}),
SrcRejtCCAppProdRejectOra AS (SELECT APP_PROD_CLIENT_ROLE_ID, ROLE_CAT_ID, CIF_CODE, APP_PROD_ID, SUBTYPE_CODE, CAST(ORIG_ETL_D AS TEXT) AS ORIG_ETL_D FROM REJT_COM_BUS_AP_PRD_CLNT_RL WHERE EROR_C LIKE 'RPR%')


SELECT * FROM SrcRejtCCAppProdRejectOra