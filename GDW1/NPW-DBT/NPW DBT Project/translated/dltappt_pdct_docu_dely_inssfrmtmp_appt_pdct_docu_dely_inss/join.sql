{{ config(materialized='view', tags=['DltAPPT_PDCT_DOCU_DELY_INSSFrmTMP_APPT_PDCT_DOCU_DELY_INSS']) }}

WITH Join AS (
	SELECT
		{{ ref('CpyApptDeptEntSeq') }}.NEW_APPT_PDCT_I,
		{{ ref('CpyApptDeptEntSeq') }}.OLD_DOCU_DELY_METH_C,
		{{ ref('CpyApptDeptEntSeq') }}.OLD_EFFT_D,
		{{ ref('ChangeCapture') }}.NEW_DOCU_DELY_METH_C,
		{{ ref('ChangeCapture') }}.NEW_PYAD_TYPE_C,
		{{ ref('ChangeCapture') }}.NEW_ADRS_LINE_1_X,
		{{ ref('ChangeCapture') }}.NEW_ADRS_LINE_2_X,
		{{ ref('ChangeCapture') }}.NEW_SURB_X,
		{{ ref('ChangeCapture') }}.NEW_PCOD_C,
		{{ ref('ChangeCapture') }}.NEW_STAT_X,
		{{ ref('ChangeCapture') }}.NEW_ISO_CNTY_C,
		{{ ref('ChangeCapture') }}.NEW_BRCH_N,
		{{ ref('ChangeCapture') }}.change_code
	FROM {{ ref('CpyApptDeptEntSeq') }}
	INNER JOIN {{ ref('ChangeCapture') }} ON {{ ref('CpyApptDeptEntSeq') }}.NEW_APPT_PDCT_I = {{ ref('ChangeCapture') }}.NEW_APPT_PDCT_I
)

SELECT * FROM Join