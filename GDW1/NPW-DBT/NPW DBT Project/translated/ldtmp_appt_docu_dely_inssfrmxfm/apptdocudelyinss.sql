{{ config(materialized='incremental', alias='tmp_appt_docu_dely_inss', incremental_strategy='append', tags=['LdTMP_APPT_DOCU_DELY_INSSFrmXfm']) }}

SELECT
	APPT_I
	DOCU_DELY_RECV_C
	RUN_STRM 
FROM {{ ref('Cpy') }}