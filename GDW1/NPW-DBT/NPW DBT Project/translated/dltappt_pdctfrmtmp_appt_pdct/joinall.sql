{{ config(materialized='view', tags=['DltAPPT_PDCTFrmTMP_APPT_PDCT']) }}

WITH JoinAll AS (
	SELECT
		{{ ref('ChangeCapture') }}.NEW_APPT_PDCT_I,
		{{ ref('ChangeCapture') }}.NEW_DEBT_ABN_X,
		{{ ref('ChangeCapture') }}.NEW_DEBT_BUSN_M,
		{{ ref('ChangeCapture') }}.NEW_SMPL_APPT_F,
		{{ ref('ChangeCapture') }}.NEW_JOB_COMM_CATG_C,
		{{ ref('ChangeCapture') }}.NEW_APPT_QLFY_C,
		{{ ref('ChangeCapture') }}.NEW_ACQR_TYPE_C,
		{{ ref('ChangeCapture') }}.NEW_ACQR_ADHC_X,
		{{ ref('ChangeCapture') }}.NEW_ACQR_SRCE_C,
		{{ ref('ChangeCapture') }}.NEW_PDCT_N,
		{{ ref('ChangeCapture') }}.NEW_APPT_I,
		{{ ref('ChangeCapture') }}.NEW_SRCE_SYST_C,
		{{ ref('ChangeCapture') }}.NEW_SRCE_SYST_APPT_PDCT_I,
		{{ ref('ChangeCapture') }}.NEW_LOAN_FNDD_METH_C,
		{{ ref('ChangeCapture') }}.NEW_NEW_ACCT_F,
		{{ ref('ChangeCapture') }}.NEW_BROK_PATY_I,
		{{ ref('ChangeCapture') }}.NEW_COPY_FROM_OTHR_APPT_F,
		{{ ref('ChangeCapture') }}.NEW_APPT_PDCT_CATG_C,
		{{ ref('ChangeCapture') }}.NEW_APPT_PDCT_DURT_C,
		{{ ref('ChangeCapture') }}.NEW_ASES_D,
		{{ ref('CpyApptPdct') }}.OLD_APPT_I,
		{{ ref('CpyApptPdct') }}.OLD_EFFT_D,
		{{ ref('ChangeCapture') }}.change_code
	FROM {{ ref('ChangeCapture') }}
	INNER JOIN {{ ref('CpyApptPdct') }} ON {{ ref('ChangeCapture') }}.NEW_APPT_PDCT_I = {{ ref('CpyApptPdct') }}.NEW_APPT_PDCT_I
)

SELECT * FROM JoinAll