{{ config(materialized='view', tags=['ExtApptQstnAns']) }}

WITH 
rejt_clp_bus_app_qstn AS (
	SELECT
	*
	FROM {{ source("cse4_stg","rejt_clp_bus_app_qstn")  }}),
SrcRejtCCAppProdBalXferRejectOra AS (SELECT APP_Id, SUBTYPE_CODE, QA_QUESTION_ID, QA_ANSWER_ID, TEXT_ANSWER, CIF_CODE, CAST(ORIG_ETL_D AS TEXT) AS ORIG_ETL_D FROM REJT_CLP_BUS_APP_QSTN WHERE eror_c LIKE 'REJ%')


SELECT * FROM SrcRejtCCAppProdBalXferRejectOra