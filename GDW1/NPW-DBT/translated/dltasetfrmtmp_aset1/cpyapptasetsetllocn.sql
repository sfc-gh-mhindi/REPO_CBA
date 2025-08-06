{{ config(materialized='view', tags=['DltASETFrmTMP_ASET1']) }}

WITH CpyApptAsetSetlLocn AS (
	SELECT
		NEW_ASET_I,
		NEW_SRCE_SYST_C,
		{{ ref('SrcTmpASETTera') }}.OLD_ASET_I AS NEW_ASET_I,
		{{ ref('SrcTmpASETTera') }}.OLD_SRCE_SYST_C AS NEW_SRCE_SYST_C,
		NEW_SECU_CODE_C,
		SECU_CATG_C,
		SRCE_SYST_ASET_I,
		ASET_C,
		ORIG_SRCE_SYST_ASET_I,
		ORIG_SRCE_SYST_C,
		ENVT_F,
		ASET_X,
		EFFT_D,
		EXPY_D,
		EROR_SEQN_I,
		ASET_LIBL_C,
		AL_CATG_C,
		DUPL_ASET_F
	FROM {{ ref('SrcTmpASETTera') }}
)

SELECT * FROM CpyApptAsetSetlLocn