{{ config(materialized='view', tags=['ExtFAClientUndertaking']) }}

WITH 
rejt_cse_coi_bus_clnt_undtak AS (
	SELECT
	*
	FROM {{ source("cse4_stg","rejt_cse_coi_bus_clnt_undtak")  }}),
SrcSubjectAreaRejectOra AS (SELECT FA_CLIENT_UNDERTAKING_ID, FA_UNDERTAKING_ID, COIN_ENTITY_ID, CLIENT_CORRELATION_ID, FA_ENTITY_CAT_ID, FA_CHILD_STATUS_CAT_ID, CLIENT_RELATIONSHIP_TYPE_ID, CLIENT_POSITION, IS_PRIMARY_FLAG, CIF_CODE, CAST(ORIG_ETL_D AS TEXT) AS ORIG_ETL_D FROM REJT_CSE_COI_BUS_CLNT_UNDTAK WHERE EROR_C LIKE 'RPR%')


SELECT * FROM SrcSubjectAreaRejectOra