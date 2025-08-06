{{ config(materialized='view', tags=['XfmEvnt']) }}

WITH 
evnt AS (
	SELECT
	*
	FROM {{ ref("evnt")  }}),
tmp_rm_rate_evnt AS (
	SELECT
	*
	FROM {{ source("sdcseld","tmp_rm_rate_evnt")  }}),
GdwEvnt AS (SELECT TGT.EVNT_I, 1 AS dummy FROM EVNT INNER JOIN TMP_RM_RATE_EVNT ON TGT.EVNT_I = TMP.EVNT_I WHERE SUBSTRING(TGT.EVNT_I, 1, 5) IN ('CSEA7'))


SELECT * FROM GdwEvnt