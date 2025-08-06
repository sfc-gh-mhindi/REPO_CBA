{{ config(materialized='view', tags=['DltAPPT_EMPLFrmTMP_APPT_EMPL']) }}

WITH CpyApptEmpl AS (
	SELECT
		NEW_APPT_I,
		NEW_EMPL_ROLE_C,
		NEW_EMPL_I,
		{{ ref('SrcTmpApptEmplTera') }}.OLD_APPT_I AS NEW_APPT_I,
		{{ ref('SrcTmpApptEmplTera') }}.OLD_EMPL_ROLE_C AS NEW_EMPL_ROLE_C,
		{{ ref('SrcTmpApptEmplTera') }}.OLD_EMPL_I AS NEW_EMPL_I,
		OLD_EMPL_I,
		OLD_EFFT_D
	FROM {{ ref('SrcTmpApptEmplTera') }}
)

SELECT * FROM CpyApptEmpl