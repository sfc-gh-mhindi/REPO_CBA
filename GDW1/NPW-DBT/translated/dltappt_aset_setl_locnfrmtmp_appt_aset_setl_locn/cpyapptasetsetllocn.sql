{{ config(materialized='view', tags=['DltAPPT_ASET_SETL_LOCNFrmTMP_APPT_ASET_SETL_LOCN']) }}

WITH CpyApptAsetSetlLocn AS (
	SELECT
		NEW_APPT_I,
		NEW_ASET_I,
		{{ ref('SrcTmpApptAsetSetlLocnTera') }}.NEW_FRWD_DOCU_C AS FRWD_DOCU_C,
		{{ ref('SrcTmpApptAsetSetlLocnTera') }}.NEW_SETL_LOCN_X AS SETL_LOCN_X,
		{{ ref('SrcTmpApptAsetSetlLocnTera') }}.NEW_SETL_CMMT_X AS SETL_CMMT_X,
		{{ ref('SrcTmpApptAsetSetlLocnTera') }}.OLD_APPT_I AS NEW_APPT_I,
		{{ ref('SrcTmpApptAsetSetlLocnTera') }}.OLD_ASET_I AS NEW_ASET_I,
		{{ ref('SrcTmpApptAsetSetlLocnTera') }}.OLD_FRWD_DOCU_C AS FRWD_DOCU_C,
		{{ ref('SrcTmpApptAsetSetlLocnTera') }}.OLD_SETL_LOCN_X AS SETL_LOCN_X,
		{{ ref('SrcTmpApptAsetSetlLocnTera') }}.OLD_SETL_CMMT_X AS SETL_CMMT_X,
		NEW_FRWD_DOCU_C,
		NEW_SETL_LOCN_X,
		NEW_SETL_CMMT_X,
		OLD_ASET_I,
		OLD_EFFT_D
	FROM {{ ref('SrcTmpApptAsetSetlLocnTera') }}
)

SELECT * FROM CpyApptAsetSetlLocn