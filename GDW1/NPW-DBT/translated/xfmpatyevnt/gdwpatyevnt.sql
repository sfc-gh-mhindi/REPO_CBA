{{ config(materialized='view', tags=['XfmPatyEvnt']) }}

WITH 
tmp_rm_rate_paty AS (
	SELECT
	*
	FROM {{ source("sdcseld","tmp_rm_rate_paty")  }}),
paty_evnt AS (
	SELECT
	*
	FROM {{ ref("paty_evnt")  }}),
GdwPatyEvnt AS (SELECT TGT.PATY_I, 1 AS dummy FROM PATY_EVNT INNER JOIN TMP_RM_RATE_PATY ON TGT.PATY_I = TMP.PATY_I WHERE TGT.EVNT_PATY_ROLE_TYPE_C = 'RMRA')


SELECT * FROM GdwPatyEvnt