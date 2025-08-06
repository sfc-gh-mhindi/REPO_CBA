{{ config(materialized='view', tags=['XfmBusnEvnt']) }}

WITH 
tmp_rm_rate_evnt AS (
	SELECT
	*
	FROM {{ source("sdcseld","tmp_rm_rate_evnt")  }}),
busn_evnt AS (
	SELECT
	*
	FROM {{ ref("busn_evnt")  }}),
GdwBusnEvnt AS (SELECT TGT.EVNT_I, 1 AS dummy FROM BUSN_EVNT INNER JOIN TMP_RM_RATE_EVNT ON TGT.EVNT_I = TMP.EVNT_I)


SELECT * FROM GdwBusnEvnt