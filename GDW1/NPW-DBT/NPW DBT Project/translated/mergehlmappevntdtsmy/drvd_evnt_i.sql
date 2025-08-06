{{ config(materialized='view', tags=['MergehlmappEvntDtSmy']) }}

WITH 
evnt_grup_assc AS (
	SELECT
	*
	FROM {{ source("svcseld","evnt_grup_assc")  }}),
tmp_hlm_app AS (
	SELECT
	*
	FROM {{ ref("tmp_hlm_app")  }}),
appt_evnt_grup AS (
	SELECT
	*
	FROM {{ source("svcseld","appt_evnt_grup")  }}),
DRVD_EVNT_I AS (SELECT EGA.EVNT_I AS EVNT_I, AEG_TMP.APPT_I AS APPT_I FROM EVNT_GRUP_ASSC INNER JOIN (SELECT TMP.APPT_I, AEG.EVNT_GRUP_I FROM TMP_HLM_APP INNER JOIN APPT_EVNT_GRUP ON TMP.APPT_I = AEG.APPT_I WHERE EXPY_D = '9999-12-31') AS AEG_TMP ON EGA.EVNT_GRUP_I = AEG_TMP.EVNT_GRUP_I WHERE EGA.EXPY_D = '9999-12-31')


SELECT * FROM DRVD_EVNT_I