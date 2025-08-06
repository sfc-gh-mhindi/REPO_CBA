{{ config(materialized='view', tags=['ExtFaUndertaking']) }}

WITH 
rejt_coi_bus_undtak AS (
	SELECT
	*
	FROM {{ source("cse4_stg","rejt_coi_bus_undtak")  }}),
SrcRejtRejectOra AS (SELECT FA_UNDERTAKING_ID, PLANNING_GROUP_NAME, COIN_ADVICE_GROUP_ID, ADVICE_GROUP_CORRELATION_ID, CREATED_DATE, CREATED_BY_STAFF_NUMBER, SM_CASE_ID, CAST(ORIG_ETL_D AS TEXT) AS orig_etl_d FROM REJT_COI_BUS_UNDTAK WHERE EROR_C LIKE 'RPR%')


SELECT * FROM SrcRejtRejectOra