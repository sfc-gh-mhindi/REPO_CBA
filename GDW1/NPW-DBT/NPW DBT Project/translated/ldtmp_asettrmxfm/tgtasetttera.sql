{{ config(materialized='incremental', alias='cse4_tmp_aset', incremental_strategy='append', tags=['LdTMP_ASETTrmXfm']) }}

SELECT
	ASET_I
	SECU_CODE_C
	SECU_CATG_C
	SRCE_SYST_ASET_I
	SRCE_SYST_C
	ASET_C
	ORIG_SRCE_SYST_ASET_I
	ORIG_SRCE_SYST_C
	ENVT_F
	ASET_X
	ASET_LIBL_C
	AL_CATG_C
	DUPL_ASET_F
	EFFT_D
	EXPY_D
	RUN_STRM 
FROM {{ ref('TgtTmp_AsetDS') }}