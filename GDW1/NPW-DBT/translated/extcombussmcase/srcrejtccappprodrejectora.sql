{{ config(materialized='view', tags=['ExtComBusSmCase']) }}

WITH 
rejt_com_bus_sm_case AS (
	SELECT
	*
	FROM {{ source("cse4_stg","rejt_com_bus_sm_case")  }}),
SrcRejtCCAppProdRejectOra AS (SELECT SM_CASE_ID, CREATED_TIMESTAMP, WIM_PROCESS_ID, CAST(ORIG_ETL_D AS TEXT) AS ORIG_ETL_D FROM REJT_COM_BUS_SM_CASE WHERE EROR_C LIKE 'RPR%')


SELECT * FROM SrcRejtCCAppProdRejectOra