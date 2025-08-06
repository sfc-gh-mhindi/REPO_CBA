{{ config(materialized='view', tags=['ExtSmCaseState']) }}

WITH 
rejt_com_bus_sm_case_state AS (
	SELECT
	*
	FROM {{ source("cse4_stg","rejt_com_bus_sm_case_state")  }}),
SrcRejtSmCaseStateReasonRejectOra AS (SELECT SM_CASE_STATE_ID, SM_CASE_ID, SM_STATE_CAT_ID, START_DATE, END_DATE, CREATED_BY_STAFF_NUMBER, STATE_CAUSED_BY_ACTION_ID, CAST(ORIG_ETL_D AS TEXT) AS ORIG_ETL_D FROM REJT_COM_BUS_SM_CASE_STATE WHERE EROR_C LIKE 'RPR%')


SELECT * FROM SrcRejtSmCaseStateReasonRejectOra