{{ config(materialized='incremental', alias='tmp_appt_pdct_cond', incremental_strategy='append', tags=['LdTMP_APPT_PDCT_COND']) }}

SELECT
	APPT_PDCT_I
	COND_C
	APPT_PDCT_COND_MEET_D 
FROM {{ ref('SrcApptDeptDS') }}