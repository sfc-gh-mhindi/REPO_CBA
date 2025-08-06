{{ config(materialized='view', tags=['DltASETFrmTMP_ASET1']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_ASET_I, {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_ASET_I) AS NEW_ASET_I,
		CASE
			WHEN {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_ASET_I IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_ASET_I IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_ASET_I = {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_ASET_I) AND ({{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_SRCE_SYST_C <> {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_SRCE_SYST_C) THEN '3'
			WHEN {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_ASET_I = {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_ASET_I AND {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_SRCE_SYST_C = {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_SRCE_SYST_C THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }} 
	ON {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_ASET_I = {{ ref('{{ ref('CpyApptAsetSetlLocn') }}') }}.NEW_ASET_I
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture