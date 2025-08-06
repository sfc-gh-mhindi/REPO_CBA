{{ config(materialized='view', tags=['DltCSE_APPT_CPGNFrmTMP_CSE_APPT_CPGN']) }}

WITH 
cse_appt_cpgn AS (
	SELECT
	*
	FROM {{ ref("cse_appt_cpgn")  }}),
tmp_cse_appt_cpgn AS (
	SELECT
	*
	FROM {{ ref("tmp_cse_appt_cpgn")  }}),
SrcTmpCseApptCpgnTera AS (SELECT A.APPT_I AS APPT_I, A.CSE_CPGN_CODE_X AS CSE_CPGN_CODE_X, (CASE WHEN B.APPT_I IS NULL THEN 'I' ELSE (CASE WHEN (B.EXPY_D = '9999-12-31' AND A.CSE_CPGN_CODE_X <> B.CSE_CPGN_CODE_X) THEN 'U' END) END) AS INSERT_UPDATE_FLAG, B.EFFT_D FROM TMP_CSE_APPT_CPGN LEFT OUTER JOIN CSE_APPT_CPGN ON A.APPT_I = B.APPT_I WHERE A.RUN_STRM = '{{ var("RUN_STREAM") }}')


SELECT * FROM SrcTmpCseApptCpgnTera