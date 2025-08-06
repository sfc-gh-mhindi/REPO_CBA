{{ config(materialized='view', tags=['DltADRSFrmTMP_ADRS']) }}

WITH 
cse4_tmp_adrs AS (
	SELECT
	*
	FROM {{ ref("cse4_tmp_adrs")  }}),
adrs AS (
	SELECT
	*
	FROM {{ ref("adrs")  }}),
SrcTmpADRSTera AS (SELECT a.ADRS_I, a.ADRS_TYPE_C, a.SRCE_SYST_C, a.ADRS_QLFY_C, a.SRCE_SYST_ADRS_I, a.SRCE_SYST_ADRS_SEQN_N FROM CSE4_TMP_ADRS LEFT OUTER JOIN ADRS ON TRIM(a.SRCE_SYST_ADRS_I) = TRIM(b.SRCE_SYST_ADRS_I) WHERE a.RUN_STRM = '{{ var("RUN_STREAM") }}' AND b.SRCE_SYST_ADRS_I IS NULL)


SELECT * FROM SrcTmpADRSTera