{{ config(materialized='view', tags=['DltAPPT_PDCT_PURP_rmTMP_APPT_PDCT_PURP']) }}

WITH CpyApptPdctPurp AS (
	SELECT
		NEW_APPT_PDCT_I,
		NEW_PURP_TYPE_C,
		{{ ref('SrcTmpApptPdctPurpTera') }}.OLD_APPT_PDCT_I AS NEW_APPT_PDCT_I,
		{{ ref('SrcTmpApptPdctPurpTera') }}.OLD_PURP_TYPE_C AS NEW_PURP_TYPE_C,
		NEW_SRCE_SYST_APPT_PDCT_PURP_I,
		NEW_PURP_CLAS_C,
		NEW_SRCE_SYST_C,
		NEW_PURP_A,
		NEW_CNCY_C,
		NEW_MAIN_PURP_F,
		OLD_EFFT_D
	FROM {{ ref('SrcTmpApptPdctPurpTera') }}
)

SELECT * FROM CpyApptPdctPurp