{{ config(materialized='view', tags=['DltASET_ADRSFrmTMP_ASET_ADRS']) }}

WITH 
aset_adrs AS (
	SELECT
	*
	FROM {{ ref("aset_adrs")  }}),
tmp_aset_adrs AS (
	SELECT
	*
	FROM {{ ref("tmp_aset_adrs")  }}),
SrcTmpAsetAdrsTera AS (SELECT A.ADRS_I AS NEW_ADRS_I, A.ASET_I AS NEW_ASET_I FROM TMP_ASET_ADRS LEFT OUTER JOIN ASET_ADRS ON a.ADRS_I = b.ADRS_I AND A.ASET_I = B.ASET_I AND b.EXPY_D = '9999-12-31' WHERE a.RUN_STRM = '{{ var("RUN_STREAM") }}' AND b.ASET_I IS NULL)


SELECT * FROM SrcTmpAsetAdrsTera