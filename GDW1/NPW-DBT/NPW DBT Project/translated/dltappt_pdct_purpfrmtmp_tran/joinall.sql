{{ config(materialized='view', tags=['DltAppt_Pdct_PurpfrmTMP_tran']) }}

WITH JoinAll AS (
	SELECT
		{{ ref('ChangeCapture') }}.NEW_APPT_PDCT_I,
		{{ ref('ChangeCapture') }}.NEW_PURP_TYPE_C,
		{{ ref('ChangeCapture') }}.NEW_SRCE_SYST_APPT_PDCT_PURP_I,
		{{ ref('CpyApptPdctPurp') }}.NEW_PURP_CLAS_C,
		{{ ref('ChangeCapture') }}.NEW_SRCE_SYST_C,
		{{ ref('CpyApptPdctPurp') }}.NEW_PURP_A,
		{{ ref('CpyApptPdctPurp') }}.NEW_CNCY_C,
		{{ ref('CpyApptPdctPurp') }}.NEW_MAIN_PURP_F,
		{{ ref('CpyApptPdctPurp') }}.OLD_EFFT_D,
		{{ ref('ChangeCapture') }}.change_code
	FROM {{ ref('ChangeCapture') }}
	INNER JOIN {{ ref('CpyApptPdctPurp') }} ON {{ ref('ChangeCapture') }}.NEW_APPT_PDCT_I = {{ ref('CpyApptPdctPurp') }}.NEW_APPT_PDCT_I
	AND {{ ref('ChangeCapture') }}.NEW_SRCE_SYST_C = {{ ref('CpyApptPdctPurp') }}.NEW_SRCE_SYST_C
)

SELECT * FROM JoinAll