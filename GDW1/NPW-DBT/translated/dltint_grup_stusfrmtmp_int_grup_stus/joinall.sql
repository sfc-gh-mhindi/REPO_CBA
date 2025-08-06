{{ config(materialized='view', tags=['DltINT_GRUP_STUSFrmTMP_INT_GRUP_STUS']) }}

WITH JoinAll AS (
	SELECT
		{{ ref('ChangeCapture') }}.NEW_INT_GRUP_I,
		{{ ref('ChangeCapture') }}.NEW_STRT_S,
		{{ ref('ChangeCapture') }}.NEW_STUS_C,
		{{ ref('ChangeCapture') }}.NEW_END_S,
		{{ ref('ChangeCapture') }}.NEW_END_D,
		{{ ref('ChangeCapture') }}.NEW_END_T,
		{{ ref('ChangeCapture') }}.change_code,
		{{ ref('CpyIntGrupStus') }}.NEW_STRT_D,
		{{ ref('CpyIntGrupStus') }}.NEW_STRT_T,
		{{ ref('CpyIntGrupStus') }}.NEW_EMPL_I,
		{{ ref('CpyIntGrupStus') }}.OLD_EFFT_D
	FROM {{ ref('ChangeCapture') }}
	INNER JOIN {{ ref('CpyIntGrupStus') }} ON {{ ref('ChangeCapture') }}.NEW_INT_GRUP_I = {{ ref('CpyIntGrupStus') }}.NEW_INT_GRUP_I
	AND {{ ref('ChangeCapture') }}.NEW_STRT_S = {{ ref('CpyIntGrupStus') }}.NEW_STRT_S
	AND {{ ref('ChangeCapture') }}.NEW_STUS_C = {{ ref('CpyIntGrupStus') }}.NEW_STUS_C
)

SELECT * FROM JoinAll