{{ config(materialized='view', tags=['DltAPPT_ASES_DETLFrmTMP_APPT_ASES_DETL']) }}

WITH 
appt_ases_detl AS (
	SELECT
	*
	FROM {{ source("tdcsad","appt_ases_detl")  }}),
tmp_appt_ases_detl AS (
	SELECT
	*
	FROM {{ ref("tmp_appt_ases_detl")  }}),
SrcTmpApptAsesDetlTera AS (SELECT a.APPT_I AS NEW_APPT_I, a.AMT_TYPE_C AS NEW_AMT_TYPE_C, a.CNCY_C AS NEW_CNCY_C, a.APPT_ASES_A AS NEW_APPT_ASES_A, b.APPT_I AS OLD_APPT_I, b.AMT_TYPE_C AS OLD_AMT_TYPE_C, b.EFFT_D AS OLD_EFFT_D, b.APPT_ASES_A AS OLD_APPT_ASES_A FROM TMP_APPT_ASES_DETL LEFT OUTER JOIN APPT_ASES_DETL ON TRIM(a.APPT_I) = TRIM(b.APPT_I) AND TRIM(a.AMT_TYPE_C) = TRIM(b.AMT_TYPE_C) AND b.EXPY_D = '9999-12-31' WHERE a.RUN_STRM = '{{ var("RUN_STREAM") }}')


SELECT * FROM SrcTmpApptAsesDetlTera