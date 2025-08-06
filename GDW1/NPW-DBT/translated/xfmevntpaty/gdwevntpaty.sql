{{ config(materialized='view', tags=['XfmEvntPaty']) }}

WITH 
evnt_paty AS (
	SELECT
	*
	FROM {{ ref("evnt_paty")  }}),
tmp_rm_rate_evnt AS (
	SELECT
	*
	FROM {{ source("sdcseld","tmp_rm_rate_evnt")  }}),
GdwEvntPaty AS (SELECT TGT.EVNT_I, 1 AS dummy FROM EVNT_PATY INNER JOIN TMP_RM_RATE_EVNT ON TGT.EVNT_I = TMP.EVNT_I WHERE TGT.EVNT_PATY_ROLE_TYPE_C = 'RMRA')


SELECT * FROM GdwEvntPaty