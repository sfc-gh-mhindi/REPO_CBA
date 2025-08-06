{{ config(materialized='view', tags=['DltAPPT_PDCT_ACCT_FrmTMP_APPT_PDCT_ACCT']) }}

WITH Join AS (
	SELECT
		{{ ref('CpyApptPdctAcctEntSeq') }}.NEW_APPT_PDCT_I,
		{{ ref('CpyApptPdctAcctEntSeq') }}.NEW_REL_TYPE_C,
		{{ ref('CpyApptPdctAcctEntSeq') }}.OLD_ACCT_I,
		{{ ref('CpyApptPdctAcctEntSeq') }}.OLD_EFFT_D,
		{{ ref('ChangeCapture') }}.NEW_ACCT_I,
		{{ ref('ChangeCapture') }}.change_code
	FROM {{ ref('CpyApptPdctAcctEntSeq') }}
	INNER JOIN {{ ref('ChangeCapture') }} ON {{ ref('CpyApptPdctAcctEntSeq') }}.NEW_APPT_PDCT_I = {{ ref('ChangeCapture') }}.NEW_APPT_PDCT_I
)

SELECT * FROM Join