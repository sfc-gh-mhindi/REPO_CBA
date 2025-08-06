{{ config(materialized='incremental', alias='tmp_appt_pdct_chkl_pl', incremental_strategy='append', tags=['LdTMP_APPT_PDCT_CHKLFrmXfm']) }}

SELECT
	APPT_PDCT_I
	CHKL_ITEM_C
	STUS_D
	STUS_C
	SRCE_SYST_C
	CHKL_ITEM_X
	RUN_STRM 
FROM {{ ref('Cpy') }}