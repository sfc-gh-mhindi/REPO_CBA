{{ config(materialized='view', tags=['DltAPPT_TRNF_DETLFrmTMP_APPT_TRNF_DETL']) }}

WITH Join AS (
	SELECT
		{{ ref('CpyApptTrnfDetlEntSeq') }}.NEW_APPT_I,
		{{ ref('CpyApptTrnfDetlEntSeq') }}.NEW_APPT_TRNF_I,
		{{ ref('CpyApptTrnfDetlEntSeq') }}.NEW_CNCY_C,
		{{ ref('CpyApptTrnfDetlEntSeq') }}.OLD_EFFT_D,
		{{ ref('ChangeCapture') }}.NEW_TRNF_OPTN_C,
		{{ ref('ChangeCapture') }}.NEW_TRNF_A,
		{{ ref('ChangeCapture') }}.NEW_CMPE_I,
		{{ ref('ChangeCapture') }}.change_code
	FROM {{ ref('CpyApptTrnfDetlEntSeq') }}
	INNER JOIN {{ ref('ChangeCapture') }} ON {{ ref('CpyApptTrnfDetlEntSeq') }}.NEW_APPT_TRNF_I = {{ ref('ChangeCapture') }}.NEW_APPT_TRNF_I
	AND {{ ref('CpyApptTrnfDetlEntSeq') }}.NEW_APPT_I = {{ ref('ChangeCapture') }}.NEW_APPT_I
)

SELECT * FROM Join