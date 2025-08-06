{{ config(materialized='view', tags=['DltINT_GRUP_STUSFrmTMP_INT_GRUP_STUS']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_INT_GRUP_I, {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_INT_GRUP_I) AS NEW_INT_GRUP_I,
		COALESCE({{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_STRT_S, {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_STRT_S) AS NEW_STRT_S,
		COALESCE({{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_STUS_C, {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_STUS_C) AS NEW_STUS_C,
		COALESCE({{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_END_S, {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_END_S) AS NEW_END_S,
		COALESCE({{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_END_D, {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_END_D) AS NEW_END_D,
		COALESCE({{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_END_T, {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_END_T) AS NEW_END_T,
		CASE
			WHEN {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_INT_GRUP_I IS NULL AND {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_STRT_S IS NULL AND {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_STUS_C IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_INT_GRUP_I IS NULL AND {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_STRT_S IS NULL AND {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_STUS_C IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_INT_GRUP_I = {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_INT_GRUP_I AND {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_STRT_S = {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_STRT_S AND {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_STUS_C = {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_STUS_C) AND ({{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_END_S <> {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_END_S OR {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_END_D <> {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_END_D OR {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_END_T <> {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_END_T) THEN '3'
			WHEN {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_INT_GRUP_I = {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_INT_GRUP_I AND {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_STRT_S = {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_STRT_S AND {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_STUS_C = {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_STUS_C AND {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_END_S = {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_END_S AND {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_END_D = {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_END_D AND {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_END_T = {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_END_T THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyIntGrupStus') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyIntGrupStus') }}') }} 
	ON {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_INT_GRUP_I = {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_INT_GRUP_I
	AND {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_STRT_S = {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_STRT_S
	AND {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_STUS_C = {{ ref('{{ ref('CpyIntGrupStus') }}') }}.NEW_STUS_C
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture