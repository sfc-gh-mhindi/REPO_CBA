{{ config(materialized='view', tags=['DltASETFrmTMP_ASET1']) }}

WITH JoinAll AS (
	SELECT
		{{ ref('ChangeCapture') }}.NEW_ASET_I,
		{{ ref('ChangeCapture') }}.change_code,
		{{ ref('CpyApptAsetSetlLocn') }}.NEW_SECU_CODE_C,
		{{ ref('CpyApptAsetSetlLocn') }}.SECU_CATG_C,
		{{ ref('CpyApptAsetSetlLocn') }}.SRCE_SYST_ASET_I,
		{{ ref('CpyApptAsetSetlLocn') }}.NEW_SRCE_SYST_C,
		{{ ref('CpyApptAsetSetlLocn') }}.ASET_C,
		{{ ref('CpyApptAsetSetlLocn') }}.ORIG_SRCE_SYST_ASET_I,
		{{ ref('CpyApptAsetSetlLocn') }}.ORIG_SRCE_SYST_C,
		{{ ref('CpyApptAsetSetlLocn') }}.ENVT_F,
		{{ ref('CpyApptAsetSetlLocn') }}.ASET_X,
		{{ ref('CpyApptAsetSetlLocn') }}.EFFT_D,
		{{ ref('CpyApptAsetSetlLocn') }}.EXPY_D,
		{{ ref('CpyApptAsetSetlLocn') }}.EROR_SEQN_I,
		{{ ref('CpyApptAsetSetlLocn') }}.ASET_LIBL_C,
		{{ ref('CpyApptAsetSetlLocn') }}.AL_CATG_C,
		{{ ref('CpyApptAsetSetlLocn') }}.DUPL_ASET_F
	FROM {{ ref('ChangeCapture') }}
	INNER JOIN {{ ref('CpyApptAsetSetlLocn') }} ON {{ ref('ChangeCapture') }}.NEW_ASET_I = {{ ref('CpyApptAsetSetlLocn') }}.NEW_ASET_I
)

SELECT * FROM JoinAll