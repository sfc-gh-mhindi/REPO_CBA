{{ config(materialized='view', tags=['DltAPPT_PDCT_AMTFrmTMP_APPT_PDCT_AMT_1']) }}

WITH CpyApptPdctAmtEntSeq AS (
	SELECT
		NEW_APPT_PDCT_I,
		NEW_AMT_TYPE_C,
		NEW_APPT_PDCT_A,
		{{ ref('SrcTmpApptPdctAmtTera') }}.OLD_APPT_PDCT_I AS NEW_APPT_PDCT_I,
		{{ ref('SrcTmpApptPdctAmtTera') }}.OLD_AMT_TYPE_C AS NEW_AMT_TYPE_C,
		{{ ref('SrcTmpApptPdctAmtTera') }}.OLD_APPT_PDCT_A AS NEW_APPT_PDCT_A,
		NEW_CNCY_C,
		OLD_AMT_TYPE_C,
		OLD_EFFT_D
	FROM {{ ref('SrcTmpApptPdctAmtTera') }}
)

SELECT * FROM CpyApptPdctAmtEntSeq