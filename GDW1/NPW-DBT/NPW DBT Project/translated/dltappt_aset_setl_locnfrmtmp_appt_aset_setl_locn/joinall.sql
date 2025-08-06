{{ config(materialized='view', tags=['DltAPPT_ASET_SETL_LOCNFrmTMP_APPT_ASET_SETL_LOCN']) }}

WITH JoinAll AS (
	SELECT
		{{ ref('ChangeCapture') }}.NEW_APPT_I,
		{{ ref('ChangeCapture') }}.NEW_ASET_I,
		{{ ref('CpyApptAsetSetlLocn') }}.NEW_FRWD_DOCU_C,
		{{ ref('CpyApptAsetSetlLocn') }}.NEW_SETL_LOCN_X,
		{{ ref('CpyApptAsetSetlLocn') }}.NEW_SETL_CMMT_X,
		{{ ref('ChangeCapture') }}.change_code,
		{{ ref('CpyApptAsetSetlLocn') }}.OLD_ASET_I,
		{{ ref('CpyApptAsetSetlLocn') }}.OLD_EFFT_D
	FROM {{ ref('ChangeCapture') }}
	INNER JOIN {{ ref('CpyApptAsetSetlLocn') }} ON {{ ref('ChangeCapture') }}.NEW_APPT_I = {{ ref('CpyApptAsetSetlLocn') }}.NEW_APPT_I
	AND {{ ref('ChangeCapture') }}.NEW_ASET_I = {{ ref('CpyApptAsetSetlLocn') }}.NEW_ASET_I
)

SELECT * FROM JoinAll