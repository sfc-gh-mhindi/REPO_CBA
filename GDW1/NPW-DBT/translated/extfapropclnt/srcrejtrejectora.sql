{{ config(materialized='view', tags=['ExtFaPropClnt']) }}

WITH 
rejt_cse_coi_bus_prop_clnt AS (
	SELECT
	*
	FROM {{ source("cse4_stg","rejt_cse_coi_bus_prop_clnt")  }}),
SrcRejtRejectOra AS (SELECT FA_PROPOSED_CLIENT_ID, COIN_ENTITY_ID, CLIENT_CORRELATION_ID, COIN_ENTITY_NAME, FA_ENTITY_CAT_ID, FA_UNDERTAKING_ID, FA_PROPOSED_CLIENT_CAT_ID, CAST(ORIG_ETL_D AS TEXT) AS orig_etl_d FROM REJT_CSE_COI_BUS_PROP_CLNT WHERE EROR_C LIKE 'RPR%')


SELECT * FROM SrcRejtRejectOra