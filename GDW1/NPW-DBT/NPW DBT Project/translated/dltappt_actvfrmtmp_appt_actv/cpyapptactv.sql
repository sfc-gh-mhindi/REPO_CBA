{{ config(materialized='view', tags=['DltAPPT_ACTVFrmTMP_APPT_ACTV']) }}

WITH CpyApptActv AS (
	SELECT
		NEW_APPT_I,
		NEW_APPT_ACTV_Q,
		{{ ref('SrcTmpApptActvTera') }}.OLD_APPT_I AS NEW_APPT_I,
		{{ ref('SrcTmpApptActvTera') }}.OLD_APPT_ACTV_Q AS NEW_APPT_ACTV_Q,
		OLD_EFFT_D
	FROM {{ ref('SrcTmpApptActvTera') }}
)

SELECT * FROM CpyApptActv