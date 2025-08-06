{{ config(materialized='view', tags=['DltAPPT_RELFrmTMP_APPT_REL_2']) }}

WITH 
tmp_appt_rel AS (
	SELECT
	*
	FROM {{ ref("tmp_appt_rel")  }}),
appt_rel AS (
	SELECT
	*
	FROM {{ ref("appt_rel")  }}),
SrcTmpApptRelTera AS (SELECT a.APPT_I AS NEW_APPT_I, a.RELD_APPT_I AS NEW_RELD_APPT_I, a.REL_TYPE_C AS NEW_REL_TYPE_C, b.APPT_I AS OLD_APPT_I, b.RELD_APPT_I AS OLD_RELD_APPT_I, b.REL_TYPE_C AS OLD_REL_TYPE_C, b.EFFT_D AS OLD_EFFT_D FROM TMP_APPT_REL LEFT OUTER JOIN APPT_REL ON TRIM(a.APPT_I) = TRIM(b.APPT_I) AND TRIM(a.REL_TYPE_C) = TRIM(b.REL_TYPE_C) AND b.EXPY_D = '9999-12-31' WHERE a.RUN_STRM = '{{ var("RUN_STREAM") }}')


SELECT * FROM SrcTmpApptRelTera