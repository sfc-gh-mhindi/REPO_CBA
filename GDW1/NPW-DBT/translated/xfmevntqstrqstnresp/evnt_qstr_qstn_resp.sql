{{ config(materialized='view', tags=['XfmEvntQstrQstnResp']) }}

WITH 
evnt_qstr_qstn_resp AS (
	SELECT
	*
	FROM {{ ref("evnt_qstr_qstn_resp")  }}),
tmp_rm_rate_evnt AS (
	SELECT
	*
	FROM {{ source("sdcseld","tmp_rm_rate_evnt")  }}),
EVNT_QSTR_QSTN_RESP AS (SELECT TGT.EVNT_I, 1 AS dummy FROM EVNT_QSTR_QSTN_RESP INNER JOIN TMP_RM_RATE_EVNT ON TGT.EVNT_I = TMP.EVNT_I WHERE TGT.QSTR_C = 'CRMR' AND TGT.QSTN_C = 'RMRT' AND TGT.EXPY_D = '9999-12-31')


SELECT * FROM EVNT_QSTR_QSTN_RESP