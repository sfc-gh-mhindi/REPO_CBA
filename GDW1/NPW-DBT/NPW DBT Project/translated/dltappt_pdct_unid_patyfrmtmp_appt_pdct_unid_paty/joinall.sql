{{ config(materialized='view', tags=['DltAPPT_PDCT_UNID_PATYFrmTMP_APPT_PDCT_UNID_PATY']) }}

WITH JoinAll AS (
	SELECT
		{{ ref('ChangeCapture') }}.NEW_APPT_PDCT_I,
		{{ ref('ChangeCapture') }}.NEW_PATY_ROLE_C,
		{{ ref('ChangeCapture') }}.NEW_UNID_PATY_CATG_C,
		{{ ref('ChangeCapture') }}.change_code,
		{{ ref('ChangeCapture') }}.NEW_SRCE_SYST_PATY_I,
		{{ ref('CpyApptPdctFeat') }}.NEW_SRCE_SYST_C,
		{{ ref('ChangeCapture') }}.NEW_PATY_M,
		{{ ref('CpyApptPdctFeat') }}.OLD_EFFT_D
	FROM {{ ref('ChangeCapture') }}
	INNER JOIN {{ ref('CpyApptPdctFeat') }} ON {{ ref('ChangeCapture') }}.NEW_APPT_PDCT_I = {{ ref('CpyApptPdctFeat') }}.NEW_APPT_PDCT_I
	AND {{ ref('ChangeCapture') }}.NEW_PATY_ROLE_C = {{ ref('CpyApptPdctFeat') }}.NEW_PATY_ROLE_C
)

SELECT * FROM JoinAll