{{ config(materialized='view', tags=['DltAPPT_EVNT_GRUPFrmTMP_APPT_EVNT_GRUP']) }}

WITH 
appt_evnt_grup AS (
	SELECT
	*
	FROM {{ source("pvtech","appt_evnt_grup")  }}),
tmp_appt_evnt_grup AS (
	SELECT
	*
	FROM {{ ref("tmp_appt_evnt_grup")  }}),
SrcTmpApptEvntGrupTera AS (SELECT a.APPT_I AS NEW_APPT_I, a.EVNT_GRUP_I AS NEW_EVNT_GRUP_I, b.APPT_I AS OLD_APPT_I, b.EVNT_GRUP_I AS OLD_EVNT_GRUP_I, b.EFFT_D AS OLD_EFFT_D FROM TMP_APPT_EVNT_GRUP LEFT OUTER JOIN APPT_EVNT_GRUP ON TRIM(a.APPT_I) = TRIM(b.APPT_I) AND b.EXPY_D = '9999-12-31' WHERE a.RUN_STRM = '{{ var("RUN_STREAM") }}')


SELECT * FROM SrcTmpApptEvntGrupTera