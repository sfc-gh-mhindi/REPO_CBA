{{ config(materialized='view', tags=['DltAPPT_FEAT_SHLFrmTMP_APPT_FEAT']) }}

WITH JoinAll AS (
	SELECT
		{{ ref('ChangeCapture') }}.NEW_APPT_I,
		{{ ref('ChangeCapture') }}.NEW_FEAT_I,
		{{ ref('CpyApptFeat') }}.NEW_SRCE_SYST_C,
		{{ ref('ChangeCapture') }}.NEW_SHL_F,
		{{ ref('CpyApptFeat') }}.OLD_EFFT_D,
		{{ ref('ChangeCapture') }}.change_code,
		{{ ref('CpyApptFeat') }}.NEW_CASS_WITHHOLD_RISKBANK_FLAG
	FROM {{ ref('ChangeCapture') }}
	INNER JOIN {{ ref('CpyApptFeat') }} ON {{ ref('ChangeCapture') }}.NEW_APPT_I = {{ ref('CpyApptFeat') }}.NEW_APPT_I
	AND {{ ref('ChangeCapture') }}.NEW_FEAT_I = {{ ref('CpyApptFeat') }}.NEW_FEAT_I
)

SELECT * FROM JoinAll