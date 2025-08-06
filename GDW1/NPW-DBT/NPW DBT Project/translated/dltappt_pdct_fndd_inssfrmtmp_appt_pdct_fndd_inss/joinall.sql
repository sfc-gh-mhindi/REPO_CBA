{{ config(materialized='view', tags=['DltAPPT_PDCT_FNDD_INSSFrmTMP_APPT_PDCT_FNDD_INSS']) }}

WITH JoinAll AS (
	SELECT
		{{ ref('ChangeCapture') }}.NEW_APPT_PDCT_I,
		{{ ref('ChangeCapture') }}.NEW_APPT_PDCT_FNDD_I,
		{{ ref('ChangeCapture') }}.NEW_APPT_PDCT_FNDD_METH_I,
		{{ ref('ChangeCapture') }}.NEW_FNDD_INSS_METH_C,
		{{ ref('ChangeCapture') }}.NEW_SRCE_SYST_FNDD_I,
		{{ ref('ChangeCapture') }}.NEW_SRCE_SYST_FNDD_METH_I,
		{{ ref('ChangeCapture') }}.NEW_SRCE_SYST_C,
		{{ ref('ChangeCapture') }}.NEW_FNDD_D,
		{{ ref('ChangeCapture') }}.NEW_FNDD_A,
		{{ ref('ChangeCapture') }}.NEW_PDCT_SYST_ACCT_N,
		{{ ref('ChangeCapture') }}.NEW_CMPE_I,
		{{ ref('ChangeCapture') }}.NEW_CMPE_ACCT_BSB_N,
		{{ ref('ChangeCapture') }}.NEW_CMPE_ACCT_N,
		{{ ref('ChangeCapture') }}.change_code,
		{{ ref('CpyApptPdctFnddInss') }}.OLD_FNDD_INSS_METH_C,
		{{ ref('CpyApptPdctFnddInss') }}.OLD_EFFT_D
	FROM {{ ref('ChangeCapture') }}
	INNER JOIN {{ ref('CpyApptPdctFnddInss') }} ON {{ ref('ChangeCapture') }}.NEW_APPT_PDCT_I = {{ ref('CpyApptPdctFnddInss') }}.NEW_APPT_PDCT_I
	AND {{ ref('ChangeCapture') }}.NEW_APPT_PDCT_FNDD_I = {{ ref('CpyApptPdctFnddInss') }}.NEW_APPT_PDCT_FNDD_I
	AND {{ ref('ChangeCapture') }}.NEW_APPT_PDCT_FNDD_METH_I = {{ ref('CpyApptPdctFnddInss') }}.NEW_APPT_PDCT_FNDD_METH_I
)

SELECT * FROM JoinAll