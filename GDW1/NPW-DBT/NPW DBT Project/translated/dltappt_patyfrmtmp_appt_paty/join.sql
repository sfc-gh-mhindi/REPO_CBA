{{ config(materialized='view', tags=['DltAPPT_PATYFrmTMP_APPT_PATY']) }}

WITH Join AS (
	SELECT
		{{ ref('CpyApptPdctAcctEntSeq') }}.NEW_APPT_I,
		{{ ref('CpyApptPdctAcctEntSeq') }}.NEW_PATY_I,
		{{ ref('CpyApptPdctAcctEntSeq') }}.NEW_REL_C,
		{{ ref('CpyApptPdctAcctEntSeq') }}.OLD_EFFT_D,
		{{ ref('ChangeCapture') }}.change_code
	FROM {{ ref('CpyApptPdctAcctEntSeq') }}
	INNER JOIN {{ ref('ChangeCapture') }} ON {{ ref('CpyApptPdctAcctEntSeq') }}.NEW_APPT_I = {{ ref('ChangeCapture') }}.NEW_APPT_I
	AND {{ ref('CpyApptPdctAcctEntSeq') }}.NEW_PATY_I = {{ ref('ChangeCapture') }}.NEW_PATY_I
)

SELECT * FROM Join