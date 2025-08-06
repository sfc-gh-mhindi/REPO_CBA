{{ config(materialized='view', tags=['DltAPPT_ACTVFrmTMP_APPT_ACTV']) }}

WITH 
tmp_appt_actv AS (
	SELECT
	*
	FROM {{ ref("tmp_appt_actv")  }}),
appt_actv AS (
	SELECT
	*
	FROM {{ ref("appt_actv")  }}),
SrcTmpApptActvTera AS (SELECT A.APPT_I AS NEW_APPT_I, A.APPT_ACTV_Q AS NEW_APPT_ACTV_Q, B.APPT_I AS OLD_APPT_I, B.APPT_ACTV_Q AS OLD_APPT_ACTV_Q, B.EFFT_D AS OLD_EFFT_D FROM TMP_APPT_ACTV LEFT OUTER JOIN APPT_ACTV ON a.APPT_I = b.APPT_I AND b.EXPY_D = '9999-12-31' AND b.SRCE_SYST_C = 'CSE' AND b.APPT_ACTV_TYPE_C = 'PRNT' WHERE a.RUN_STRM = '{{ var("RUN_STREAM") }}')


SELECT * FROM SrcTmpApptActvTera