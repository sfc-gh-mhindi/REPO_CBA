{{ config(materialized='view', tags=['DltAPPT_DOCU_DELY_INSSFrmTMP_APPT_DOCU_DELY_INSS']) }}

WITH JoinAll AS (
	SELECT
		{{ ref('ChangeCapture') }}.NEW_APPT_I,
		{{ ref('ChangeCapture') }}.change_code,
		{{ ref('CpyApptDocuDelyInss') }}.NEW_DOCU_DELY_RECV_C,
		{{ ref('CpyApptDocuDelyInss') }}.OLD_EFFT_D
	FROM {{ ref('ChangeCapture') }}
	INNER JOIN {{ ref('CpyApptDocuDelyInss') }} ON {{ ref('ChangeCapture') }}.NEW_APPT_I = {{ ref('CpyApptDocuDelyInss') }}.NEW_APPT_I
)

SELECT * FROM JoinAll