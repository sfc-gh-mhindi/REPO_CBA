{{ config(materialized='incremental', alias='tmp_int_grup_stus', incremental_strategy='append', tags=['LdTMP_INT_GRUP_STUSFrmXfm']) }}

SELECT
	INT_GRUP_I
	STRT_S
	STUS_C
	STRT_D
	STRT_T
	EMPL_I
	END_S
	END_D
	END_T
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I
	RUN_STRM 
FROM {{ ref('SrcIntGrupStusDS') }}