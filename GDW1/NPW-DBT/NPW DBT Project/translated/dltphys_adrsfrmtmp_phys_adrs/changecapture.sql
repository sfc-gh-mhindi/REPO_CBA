{{ config(materialized='view', tags=['DltPHYS_ADRSFrmTMP_PHYS_ADRS']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ADRS_I, {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ADRS_I) AS NEW_ADRS_I,
		COALESCE({{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_PHYS_ADRS_TYPE_C, {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_PHYS_ADRS_TYPE_C) AS NEW_PHYS_ADRS_TYPE_C,
		COALESCE({{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ADRS_LINE_1_X, {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ADRS_LINE_1_X) AS NEW_ADRS_LINE_1_X,
		COALESCE({{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ADRS_LINE_2_X, {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ADRS_LINE_2_X) AS NEW_ADRS_LINE_2_X,
		COALESCE({{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_SURB_X, {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_SURB_X) AS NEW_SURB_X,
		COALESCE({{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_CITY_X, {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_CITY_X) AS NEW_CITY_X,
		COALESCE({{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_PCOD_C, {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_PCOD_C) AS NEW_PCOD_C,
		COALESCE({{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_STAT_C, {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_STAT_C) AS NEW_STAT_C,
		COALESCE({{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ISO_CNTY_C, {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ISO_CNTY_C) AS NEW_ISO_CNTY_C,
		CASE
			WHEN {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ADRS_I IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ADRS_I IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ADRS_I = {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ADRS_I) AND ({{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_PHYS_ADRS_TYPE_C <> {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_PHYS_ADRS_TYPE_C OR {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ADRS_LINE_1_X <> {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ADRS_LINE_1_X OR {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ADRS_LINE_2_X <> {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ADRS_LINE_2_X OR {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_SURB_X <> {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_SURB_X OR {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_CITY_X <> {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_CITY_X OR {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_PCOD_C <> {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_PCOD_C OR {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_STAT_C <> {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_STAT_C OR {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ISO_CNTY_C <> {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ISO_CNTY_C) THEN '3'
			WHEN {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ADRS_I = {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ADRS_I AND {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_PHYS_ADRS_TYPE_C = {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_PHYS_ADRS_TYPE_C AND {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ADRS_LINE_1_X = {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ADRS_LINE_1_X AND {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ADRS_LINE_2_X = {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ADRS_LINE_2_X AND {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_SURB_X = {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_SURB_X AND {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_CITY_X = {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_CITY_X AND {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_PCOD_C = {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_PCOD_C AND {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_STAT_C = {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_STAT_C AND {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ISO_CNTY_C = {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ISO_CNTY_C THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyApptFeat') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyApptFeat') }}') }} 
	ON {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ADRS_I = {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ADRS_I
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture