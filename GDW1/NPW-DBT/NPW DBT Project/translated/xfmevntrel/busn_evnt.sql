{{ config(materialized='view', tags=['XfmEvntRel']) }}

WITH 
busn_evnt AS (
	SELECT
	*
	FROM {{ ref("busn_evnt")  }}),
tmp_rm_rate_evnt AS (
	SELECT
	*
	FROM {{ source("sdcseld","tmp_rm_rate_evnt")  }}),
BUSN_EVNT AS (SELECT TGT.EVNT_I, TGT.SRCE_SYST_EVNT_I FROM BUSN_EVNT INNER JOIN TMP_RM_RATE_EVNT ON TGT.SRCE_SYST_EVNT_I = TMP.WIM_PROCESS_ID WHERE TGT.EVNT_I LIKE 'CSEPRC%')


SELECT * FROM BUSN_EVNT