{{ config(materialized='view', tags=['DltAPPT_EMPLFrmTMP_APPT_EMPL']) }}

WITH 
tmp_appt_empl AS (
	SELECT
	*
	FROM {{ ref("tmp_appt_empl")  }}),
appt_empl AS (
	SELECT
	*
	FROM {{ ref("appt_empl")  }}),
SrcTmpApptEmplTera AS (SELECT a.APPT_I AS NEW_APPT_I, a.EMPL_ROLE_C AS NEW_EMPL_ROLE_C, a.EMPL_I AS NEW_EMPL_I, b.APPT_I AS OLD_APPT_I, b.EMPL_ROLE_C AS OLD_EMPL_ROLE_C, b.EMPL_I AS OLD_EMPL_I, b.EFFT_D AS OLD_EFFT_D FROM TMP_APPT_EMPL LEFT OUTER JOIN APPT_EMPL ON TRIM(a.APPT_I) = TRIM(b.APPT_I) AND TRIM(a.EMPL_ROLE_C) = TRIM(b.EMPL_ROLE_C) AND b.EXPY_D = '9999-12-31' WHERE a.RUN_STRM = '{{ var("RUN_STREAM") }}')


SELECT * FROM SrcTmpApptEmplTera