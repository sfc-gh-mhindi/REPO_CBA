{{ config(materialized='view', tags=['XfmEnvtUser']) }}

WITH 
evnt_user AS (
	SELECT
	*
	FROM {{ ref("evnt_user")  }}),
tmp_rm_rate_evnt AS (
	SELECT
	*
	FROM {{ source("sdcseld","tmp_rm_rate_evnt")  }}),
GdwEvntUser AS (SELECT TGT.EVNT_I, 1 AS dummy FROM EVNT_USER INNER JOIN TMP_RM_RATE_EVNT ON TGT.EVNT_I = TMP.EVNT_I WHERE TGT.EVNT_PATY_ROLE_C = 'RMRC' AND TGT.EXPY_D = '9999-12-31')


SELECT * FROM GdwEvntUser