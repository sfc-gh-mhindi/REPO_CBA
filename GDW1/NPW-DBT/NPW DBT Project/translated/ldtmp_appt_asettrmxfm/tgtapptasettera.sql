{{ config(materialized='incremental', alias='tmp_appt_dept', incremental_strategy='append', tags=['LdTMP_APPT_ASETTrmXfm']) }}

SELECT
	APPT_I
	ASET_I
	PRIM_SECU_F
	EFFT_D
	EXPY_D
	EROR_SEQN_I
	RUN_STRM
	CHL_PRCP_SCUY_FLAG 
FROM {{ ref('TgtTmp_ApptAsetDS') }}