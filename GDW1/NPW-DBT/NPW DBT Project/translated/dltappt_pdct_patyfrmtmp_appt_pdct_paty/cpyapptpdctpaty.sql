{{ config(materialized='view', tags=['DltAPPT_PDCT_PATYFrmTMP_APPT_PDCT_PATY']) }}

WITH CpyApptPdctPaty AS (
	SELECT
		NEW_APPT_PDCT_I,
		NEW_PATY_ROLE_C,
		NEW_PATY_I,
		{{ ref('SrcTmpApptPdctPatyTera') }}.OLD_APPT_PDCT_I AS NEW_APPT_PDCT_I,
		{{ ref('SrcTmpApptPdctPatyTera') }}.OLD_PATY_ROLE_C AS NEW_PATY_ROLE_C,
		{{ ref('SrcTmpApptPdctPatyTera') }}.OLD_PATY_I AS NEW_PATY_I,
		NEW_SRCE_SYST_C,
		NEW_SRCE_SYST_APPT_PDCT_PATY_I,
		OLD_PATY_I,
		OLD_EFFT_D
	FROM {{ ref('SrcTmpApptPdctPatyTera') }}
)

SELECT * FROM CpyApptPdctPaty