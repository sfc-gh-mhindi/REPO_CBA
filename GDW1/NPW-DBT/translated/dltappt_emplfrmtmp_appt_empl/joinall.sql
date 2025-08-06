{{ config(materialized='view', tags=['DltAPPT_EMPLFrmTMP_APPT_EMPL']) }}

WITH JoinAll AS (
	SELECT
		{{ ref('ChangeCapture') }}.NEW_APPT_I,
		{{ ref('ChangeCapture') }}.NEW_EMPL_ROLE_C,
		{{ ref('ChangeCapture') }}.NEW_EMPL_I,
		{{ ref('CpyApptEmpl') }}.OLD_EFFT_D,
		{{ ref('ChangeCapture') }}.change_code,
		{{ ref('CpyApptEmpl') }}.OLD_EMPL_I
	FROM {{ ref('ChangeCapture') }}
	INNER JOIN {{ ref('CpyApptEmpl') }} ON {{ ref('ChangeCapture') }}.NEW_APPT_I = {{ ref('CpyApptEmpl') }}.NEW_APPT_I
	AND {{ ref('ChangeCapture') }}.NEW_EMPL_ROLE_C = {{ ref('CpyApptEmpl') }}.NEW_EMPL_ROLE_C
)

SELECT * FROM JoinAll