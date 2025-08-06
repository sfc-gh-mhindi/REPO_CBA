{{ config(materialized='view', tags=['DltAPPT_PDCT_RELFrmTMP_APPT_PDCT_REL']) }}

WITH CpyApptPdctRel AS (
	SELECT
		NEW_APPT_PDCT_I,
		NEW_RELD_APPT_PDCT_I,
		NEW_REL_TYPE_C,
		{{ ref('SrcTmpApptPdctRelTera') }}.OLD_APPT_PDCT_I AS NEW_APPT_PDCT_I,
		{{ ref('SrcTmpApptPdctRelTera') }}.OLD_RELD_APPT_PDCT_I AS NEW_RELD_APPT_PDCT_I,
		{{ ref('SrcTmpApptPdctRelTera') }}.OLD_REL_TYPE_C AS NEW_REL_TYPE_C,
		OLD_EFFT_D
	FROM {{ ref('SrcTmpApptPdctRelTera') }}
)

SELECT * FROM CpyApptPdctRel