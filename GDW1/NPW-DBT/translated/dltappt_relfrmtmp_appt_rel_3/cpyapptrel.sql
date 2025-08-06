{{ config(materialized='view', tags=['DltAPPT_RELFrmTMP_APPT_REL_3']) }}

WITH CpyApptRel AS (
	SELECT
		NEW_APPT_I,
		NEW_RELD_APPT_I,
		NEW_REL_TYPE_C,
		{{ ref('SrcTmpApptRelTera') }}.OLD_APPT_I AS NEW_APPT_I,
		{{ ref('SrcTmpApptRelTera') }}.OLD_RELD_APPT_I AS NEW_RELD_APPT_I,
		{{ ref('SrcTmpApptRelTera') }}.OLD_REL_TYPE_C AS NEW_REL_TYPE_C,
		OLD_EFFT_D
	FROM {{ ref('SrcTmpApptRelTera') }}
)

SELECT * FROM CpyApptRel