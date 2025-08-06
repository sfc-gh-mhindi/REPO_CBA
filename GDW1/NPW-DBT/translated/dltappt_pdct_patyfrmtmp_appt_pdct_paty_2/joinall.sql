{{ config(materialized='view', tags=['DltAPPT_PDCT_PATYFrmTMP_APPT_PDCT_PATY_2']) }}

WITH JoinAll AS (
	SELECT
		{{ ref('ChangeCapture') }}.NEW_APPT_PDCT_I,
		{{ ref('ChangeCapture') }}.NEW_PATY_ROLE_C,
		{{ ref('ChangeCapture') }}.NEW_PATY_I,
		{{ ref('ChangeCapture') }}.change_code,
		{{ ref('CpyApptPdctPaty') }}.NEW_SRCE_SYST_C,
		{{ ref('CpyApptPdctPaty') }}.NEW_SRCE_SYST_APPT_PDCT_PATY_I,
		{{ ref('CpyApptPdctPaty') }}.OLD_PATY_I,
		{{ ref('CpyApptPdctPaty') }}.OLD_EFFT_D
	FROM {{ ref('ChangeCapture') }}
	INNER JOIN {{ ref('CpyApptPdctPaty') }} ON {{ ref('ChangeCapture') }}.NEW_APPT_PDCT_I = {{ ref('CpyApptPdctPaty') }}.NEW_APPT_PDCT_I
	AND {{ ref('ChangeCapture') }}.NEW_PATY_ROLE_C = {{ ref('CpyApptPdctPaty') }}.NEW_PATY_ROLE_C
	AND {{ ref('ChangeCapture') }}.NEW_PATY_I = {{ ref('CpyApptPdctPaty') }}.NEW_PATY_I
)

SELECT * FROM JoinAll