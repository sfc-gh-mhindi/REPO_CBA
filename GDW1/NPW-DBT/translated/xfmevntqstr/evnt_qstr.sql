{{ config(materialized='view', tags=['XfmEvntQstr']) }}

WITH 
evnt_qstr AS (
	SELECT
	*
	FROM {{ ref("evnt_qstr")  }}),
tmp_rm_rate_evnt AS (
	SELECT
	*
	FROM {{ source("sdcseld","tmp_rm_rate_evnt")  }}),
EVNT_QSTR AS (SELECT TGT.EVNT_I, 1 AS dummy FROM EVNT_QSTR INNER JOIN TMP_RM_RATE_EVNT ON TGT.EVNT_I = TMP.EVNT_I WHERE TGT.QSTR_C = 'CRMR' AND TGT.EXPY_D = '9999-12-31')


SELECT * FROM EVNT_QSTR