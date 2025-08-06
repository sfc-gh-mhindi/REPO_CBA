{{ config(materialized='view', tags=['DltAPPT_PATYFrmTMP_APPT_PATY']) }}

WITH 
appt_paty AS (
	SELECT
	*
	FROM {{ ref("appt_paty")  }}),
tmp_appt_paty AS (
	SELECT
	*
	FROM {{ ref("tmp_appt_paty")  }}),
SrcTmpApptPatyTera AS (SELECT a.APPT_I AS NEW_APPT_I, a.PATY_I AS NEW_PATY_I, a.REL_C AS NEW_REL_C, b.APPT_I AS OLD_APPT_I, b.PATY_I AS OLD_PATY_I, b.REL_C AS OLD_REL_C, b.EFFT_D AS OLD_EFFT_D FROM TMP_APPT_PATY LEFT OUTER JOIN APPT_PATY ON a.APPT_I = B.APPT_I AND a.PATY_I = b.PATY_I AND b.EXPY_D = '9999-12-31' WHERE a.RUN_STRM = '{{ var("RUN_STREAM") }}')


SELECT * FROM SrcTmpApptPatyTera