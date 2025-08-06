{{ config(materialized='view', tags=['ExtFaEnvisionEvent']) }}

WITH 
rejt_cse_coi_bus_envi_evnt AS (
	SELECT
	*
	FROM {{ source("cse4_stg","rejt_cse_coi_bus_envi_evnt")  }}),
SrcRejtRejectOra AS (SELECT FA_ENVISION_EVENT_ID, FA_UNDERTAKING_ID, FA_ENVISION_EVENT_CAT_ID, CREATED_DATE, CREATED_BY_STAFF_NUMBER, COIN_REQUEST_ID, CAST(ORIG_ETL_D AS TEXT) AS orig_etl_d FROM REJT_CSE_COI_BUS_ENVI_EVNT WHERE EROR_C LIKE 'RPR%')


SELECT * FROM SrcRejtRejectOra