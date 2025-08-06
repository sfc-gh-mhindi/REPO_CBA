{{ config(materialized='incremental', alias='tmp_appt_aset_setl_locn', incremental_strategy='append', tags=['LdTMP_APPT_ASET_SETL_LOCNFrmXfm']) }}

SELECT
	APPT_I
	ASET_I
	FRWD_DOCU_C
	SETL_LOCN_X
	SETL_CMMT_X
	RUN_STRM 
FROM {{ ref('Cpy') }}