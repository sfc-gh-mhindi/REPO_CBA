{{ config(materialized='view', tags=['DltAPPT_DOCU_DELY_INSSFrmTMP_APPT_DOCU_DELY_INSS']) }}

WITH 
tmp_appt_docu_dely_inss AS (
	SELECT
	*
	FROM {{ ref("tmp_appt_docu_dely_inss")  }}),
appt_docu_dely_inss AS (
	SELECT
	*
	FROM {{ ref("appt_docu_dely_inss")  }}),
SrcTmpApptDocuDelyInssTera AS (SELECT A.APPT_I AS NEW_APPT_I, A.DOCU_DELY_RECV_C AS NEW_DOCU_DELY_RECV_C, B.APPT_I AS OLD_APPT_I, B.DOCU_DELY_RECV_C AS OLD_DOCU_DELY_RECV_C, B.EFFT_D AS OLD_EFFT_D FROM TMP_APPT_DOCU_DELY_INSS LEFT OUTER JOIN APPT_DOCU_DELY_INSS ON a.APPT_I = b.APPT_I AND b.EXPY_D = '9999-12-31' AND b.SRCE_SYST_C = 'CSE' WHERE a.RUN_STRM = '{{ var("RUN_STREAM") }}')


SELECT * FROM SrcTmpApptDocuDelyInssTera