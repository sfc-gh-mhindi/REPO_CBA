{{ config(materialized='view', tags=['DltAPPT_TRNF_DETLFrmTMP_APPT_TRNF_DETL']) }}

WITH CpyApptTrnfDetlEntSeq AS (
	SELECT
		NEW_APPT_I,
		NEW_APPT_TRNF_I,
		NEW_TRNF_OPTN_C,
		NEW_TRNF_A,
		NEW_CMPE_I,
		{{ ref('SrcTmpApptTrnfDetlTera') }}.OLD_APPT_I AS NEW_APPT_I,
		{{ ref('SrcTmpApptTrnfDetlTera') }}.OLD_APPT_TRNF_I AS NEW_APPT_TRNF_I,
		{{ ref('SrcTmpApptTrnfDetlTera') }}.OLD_TRNF_OPTN_C AS NEW_TRNF_OPTN_C,
		{{ ref('SrcTmpApptTrnfDetlTera') }}.OLD_TRNF_A AS NEW_TRNF_A,
		{{ ref('SrcTmpApptTrnfDetlTera') }}.OLD_CMPE_I AS NEW_CMPE_I,
		NEW_CNCY_C,
		OLD_EFFT_D
	FROM {{ ref('SrcTmpApptTrnfDetlTera') }}
)

SELECT * FROM CpyApptTrnfDetlEntSeq