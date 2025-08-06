{{ config(materialized='view', tags=['Ext_APP_ANS']) }}

WITH 
rejt_bus_xs_app_ans AS (
	SELECT
	*
	FROM {{ source("cse4_stg_dsv81test","rejt_bus_xs_app_ans")  }}),
SrcRejtApptRelRejectOra AS (SELECT APP_ID, QA_QUESTION_ID, QA_ANSWER_ID, TEXT_ANSWER, CIF_CODE, CBA_STAFF_NUMBER, LODGEMENT_BRANCH_ID, SBTY_CODE, CAST(MOD_TIMESTAMP AS TEXT) AS MOD_TIMESTAMP, CAST(ORIG_ETL_D AS TEXT) AS ORIG_ETL_D FROM REJT_BUS_XS_APP_ANS WHERE EROR_C LIKE 'REJ%')


SELECT * FROM SrcRejtApptRelRejectOra