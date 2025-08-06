{{ config(materialized='view', tags=['DltAPPT_EVNT_GRUPFrmTMP_APPT_EVNT_GRUP']) }}

WITH JoinAll AS (
	SELECT
		{{ ref('ChangeCapture') }}.NEW_APPT_I,
		{{ ref('ChangeCapture') }}.NEW_EVNT_GRUP_I,
		{{ ref('ChangeCapture') }}.change_code,
		{{ ref('CpyApptEvntGrup') }}.OLD_EFFT_D
	FROM {{ ref('ChangeCapture') }}
	INNER JOIN {{ ref('CpyApptEvntGrup') }} ON {{ ref('ChangeCapture') }}.NEW_APPT_I = {{ ref('CpyApptEvntGrup') }}.NEW_APPT_I
)

SELECT * FROM JoinAll