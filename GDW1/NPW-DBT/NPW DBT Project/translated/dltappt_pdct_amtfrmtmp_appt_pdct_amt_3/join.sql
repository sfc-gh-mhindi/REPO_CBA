{{ config(materialized='view', tags=['DltAPPT_PDCT_AMTFrmTMP_APPT_PDCT_AMT_3']) }}

WITH Join AS (
	SELECT
		{{ ref('CpyApptPdctAmtEntSeq') }}.NEW_APPT_PDCT_I,
		{{ ref('CpyApptPdctAmtEntSeq') }}.NEW_AMT_TYPE_C,
		{{ ref('CpyApptPdctAmtEntSeq') }}.NEW_SRCE_SYST_C,
		{{ ref('ChangeCapture') }}.NEW_CNCY_C,
		{{ ref('CpyApptPdctAmtEntSeq') }}.OLD_EFFT_D,
		{{ ref('ChangeCapture') }}.NEW_APPT_PDCT_A,
		{{ ref('ChangeCapture') }}.change_code
	FROM {{ ref('CpyApptPdctAmtEntSeq') }}
	INNER JOIN {{ ref('ChangeCapture') }} ON {{ ref('CpyApptPdctAmtEntSeq') }}.NEW_APPT_PDCT_I = {{ ref('ChangeCapture') }}.NEW_APPT_PDCT_I
	AND {{ ref('CpyApptPdctAmtEntSeq') }}.NEW_AMT_TYPE_C = {{ ref('ChangeCapture') }}.NEW_AMT_TYPE_C
	AND {{ ref('CpyApptPdctAmtEntSeq') }}.NEW_SRCE_SYST_C = {{ ref('ChangeCapture') }}.NEW_SRCE_SYST_C
)

SELECT * FROM Join