{{ config(materialized='view', tags=['DltAPPT_ASETFrmTMP_APPT_ASET1']) }}

WITH JoinAll AS (
	SELECT
		{{ ref('ChangeCapture') }}.NEW_APPT_I,
		{{ ref('ChangeCapture') }}.NEW_ASET_I,
		{{ ref('CpyApptFeat') }}.NEW_PRIM_SECU_F,
		{{ ref('CpyApptFeat') }}.NEW_ASET_SETL_REQD,
		{{ ref('ChangeCapture') }}.change_code,
		{{ ref('CpyApptFeat') }}.OLD_ASET_I,
		{{ ref('CpyApptFeat') }}.OLD_EFFT_D
	FROM {{ ref('ChangeCapture') }}
	INNER JOIN {{ ref('CpyApptFeat') }} ON {{ ref('ChangeCapture') }}.NEW_APPT_I = {{ ref('CpyApptFeat') }}.NEW_APPT_I
	AND {{ ref('ChangeCapture') }}.NEW_ASET_I = {{ ref('CpyApptFeat') }}.NEW_ASET_I
)

SELECT * FROM JoinAll