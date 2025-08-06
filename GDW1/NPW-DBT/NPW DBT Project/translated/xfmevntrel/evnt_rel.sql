{{ config(materialized='view', tags=['XfmEvntRel']) }}

WITH 
tmp_rm_rate_evnt AS (
	SELECT
	*
	FROM {{ source("sdcseld","tmp_rm_rate_evnt")  }}),
evnt_rel AS (
	SELECT
	*
	FROM {{ ref("evnt_rel")  }}),
EVNT_REL AS (SELECT TGT.EVNT_I, 1 AS dummy FROM EVNT_REL INNER JOIN TMP_RM_RATE_EVNT ON TGT.EVNT_I = TMP.EVNT_I WHERE TGT.EVNT_REL_TYPE_C = 'RMRW')


SELECT * FROM EVNT_REL