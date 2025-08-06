{{ config(materialized='incremental', alias='tmp_appt_aset1', incremental_strategy='append', tags=['LdTMP_APPT_ASETFrmXfm1']) }}

SELECT
	APPT_I
	ASET_I
	PRIM_SECU_F
	RUN_STRM
	ASET_SETL_REQD 
FROM {{ ref('Cpy') }}