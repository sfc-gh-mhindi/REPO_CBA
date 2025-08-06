{{ config(materialized='incremental', alias='_cba__app_lpxs_lpxs__dev_lookupset_map__cse__appt__qlfy__sbty__loan__code', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_APPT_QLFY_LPLkp']) }}

SELECT
	LOAN_SBTY_CODE,
	LOAN_APPT_QLFY_C 
FROM {{ ref('XfmConversions') }}