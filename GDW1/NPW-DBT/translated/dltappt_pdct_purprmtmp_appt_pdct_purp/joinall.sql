{{ config(materialized='view', tags=['DltAPPT_PDCT_PURPrmTMP_APPT_PDCT_PURP']) }}

WITH JoinAll AS (
	SELECT
		{{ ref('ChangeCapture') }}.NEW_APPT_PDCT_I,
		{{ ref('ChangeCapture') }}.NEW_SRCE_SYST_APPT_PDCT_PURP_I,
		{{ ref('ChangeCapture') }}.NEW_PURP_TYPE_C,
		{{ ref('ChangeCapture') }}.NEW_PURP_CLAS_C,
		{{ ref('ChangeCapture') }}.NEW_SRCE_SYST_C,
		{{ ref('ChangeCapture') }}.NEW_PURP_A,
		{{ ref('ChangeCapture') }}.NEW_CNCY_C,
		{{ ref('ChangeCapture') }}.NEW_MAIN_PURP_F,
		{{ ref('ChangeCapture') }}.change_code,
		{{ ref('CpyApptPdctPurp') }}.OLD_EFFT_D
	FROM {{ ref('ChangeCapture') }}
	INNER JOIN {{ ref('CpyApptPdctPurp') }} ON {{ ref('ChangeCapture') }}.NEW_APPT_PDCT_I = {{ ref('CpyApptPdctPurp') }}.NEW_APPT_PDCT_I
	AND {{ ref('ChangeCapture') }}.NEW_SRCE_SYST_APPT_PDCT_PURP_I = {{ ref('CpyApptPdctPurp') }}.NEW_SRCE_SYST_APPT_PDCT_PURP_I
)

SELECT * FROM JoinAll