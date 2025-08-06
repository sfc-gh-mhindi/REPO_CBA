{{ config(materialized='view', tags=['DltAPPT_PDCT_RPAYFrmTMP_APPT_PDCT_RPAY']) }}

WITH JoinAll AS (
	SELECT
		{{ ref('ChangeCapture') }}.NEW_APPT_PDCT_I,
		{{ ref('ChangeCapture') }}.NEW_RPAY_TYPE_C,
		{{ ref('ChangeCapture') }}.NEW_PAYT_FREQ_C,
		{{ ref('ChangeCapture') }}.change_code,
		{{ ref('CpyApptPdctFeat') }}.NEW_STRT_RPAY_D,
		{{ ref('CpyApptPdctFeat') }}.OLD_EFFT_D
	FROM {{ ref('ChangeCapture') }}
	INNER JOIN {{ ref('CpyApptPdctFeat') }} ON {{ ref('ChangeCapture') }}.NEW_APPT_PDCT_I = {{ ref('CpyApptPdctFeat') }}.NEW_APPT_PDCT_I
)

SELECT * FROM JoinAll