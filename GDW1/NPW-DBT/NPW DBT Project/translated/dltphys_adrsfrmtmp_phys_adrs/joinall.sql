{{ config(materialized='view', tags=['DltPHYS_ADRSFrmTMP_PHYS_ADRS']) }}

WITH JoinAll AS (
	SELECT
		{{ ref('ChangeCapture') }}.NEW_ADRS_I,
		{{ ref('ChangeCapture') }}.NEW_PHYS_ADRS_TYPE_C,
		{{ ref('ChangeCapture') }}.NEW_ADRS_LINE_1_X,
		{{ ref('ChangeCapture') }}.NEW_ADRS_LINE_2_X,
		{{ ref('ChangeCapture') }}.NEW_SURB_X,
		{{ ref('ChangeCapture') }}.NEW_CITY_X,
		{{ ref('ChangeCapture') }}.NEW_PCOD_C,
		{{ ref('ChangeCapture') }}.NEW_STAT_C,
		{{ ref('ChangeCapture') }}.NEW_ISO_CNTY_C,
		{{ ref('ChangeCapture') }}.change_code,
		{{ ref('CpyApptFeat') }}.OLD_EFFT_D
	FROM {{ ref('ChangeCapture') }}
	INNER JOIN {{ ref('CpyApptFeat') }} ON {{ ref('ChangeCapture') }}.NEW_ADRS_I = {{ ref('CpyApptFeat') }}.NEW_ADRS_I
)

SELECT * FROM JoinAll