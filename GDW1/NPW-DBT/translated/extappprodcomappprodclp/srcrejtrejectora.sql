{{ config(materialized='view', tags=['ExtAppProdComAppProdClp']) }}

WITH 
rejt_clp_bus_app_pdct_feat AS (
	SELECT
	*
	FROM {{ source("cse4_stg","rejt_clp_bus_app_pdct_feat")  }}),
SrcRejtRejectOra AS (SELECT APP_PROD_ID, COM_SUBTYPE_CODE, CAMPAIGN_CAT_ID, COM_APP_ID, CAST(ORIG_ETL_D AS TEXT) AS ORIG_ETL_D FROM REJT_CLP_BUS_APP_PDCT_FEAT WHERE EROR_C LIKE 'RPR%')


SELECT * FROM SrcRejtRejectOra