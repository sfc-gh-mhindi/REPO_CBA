{{ config(materialized='view', tags=['DltAPPT_PDCT_ACCTFrmTMP_APPT_PDCT_ACCT']) }}

WITH CpyApptPdctAcctEntSeq AS (
	SELECT
		NEW_APPT_PDCT_I,
		NEW_REL_TYPE_C,
		NEW_ACCT_I,
		{{ ref('SrcTmpApptPdctAcctTera') }}.OLD_APPT_PDCT_I AS NEW_APPT_PDCT_I,
		{{ ref('SrcTmpApptPdctAcctTera') }}.OLD_REL_TYPE_C AS NEW_REL_TYPE_C,
		{{ ref('SrcTmpApptPdctAcctTera') }}.OLD_ACCT_I AS NEW_ACCT_I,
		OLD_ACCT_I,
		OLD_EFFT_D
	FROM {{ ref('SrcTmpApptPdctAcctTera') }}
)

SELECT * FROM CpyApptPdctAcctEntSeq