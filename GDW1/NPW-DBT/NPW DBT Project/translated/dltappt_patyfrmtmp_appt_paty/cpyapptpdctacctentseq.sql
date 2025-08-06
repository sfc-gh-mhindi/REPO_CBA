{{ config(materialized='view', tags=['DltAPPT_PATYFrmTMP_APPT_PATY']) }}

WITH CpyApptPdctAcctEntSeq AS (
	SELECT
		NEW_APPT_I,
		NEW_PATY_I,
		NEW_REL_C,
		{{ ref('SrcTmpApptPatyTera') }}.OLD_APPT_I AS NEW_APPT_I,
		{{ ref('SrcTmpApptPatyTera') }}.OLD_PATY_I AS NEW_PATY_I,
		{{ ref('SrcTmpApptPatyTera') }}.OLD_REL_C AS NEW_REL_C,
		OLD_EFFT_D
	FROM {{ ref('SrcTmpApptPatyTera') }}
)

SELECT * FROM CpyApptPdctAcctEntSeq