{{ config(materialized='view', tags=['DltAPPT_ASETFrmTMP_APPT_ASET']) }}

WITH 
appt_aset AS (
	SELECT
	*
	FROM {{ ref("appt_aset")  }}),
tmp_appt_aset AS (
	SELECT
	*
	FROM {{ source("tdcsdstg","tmp_appt_aset")  }}),
SrcTmpApptAsetTera AS (SELECT A.APPT_I AS NEW_APPT_I, A.ASET_I AS NEW_ASET_I, A.PRIM_SECU_F AS NEW_PRIM_SECU_F, B.APPT_I AS OLD_APPT_I, B.ASET_I AS OLD_ASET_I, B.PRIM_SECU_F AS OLD_PRIM_SECU_F, B.EFFT_D AS OLD_EFFT_D FROM TMP_APPT_ASET LEFT OUTER JOIN APPT_ASET ON a.APPT_I = b.APPT_I AND b.EXPY_D = '9999-12-31' WHERE a.RUN_STRM = '{{ var("RUN_STREAM") }}')


SELECT * FROM SrcTmpApptAsetTera