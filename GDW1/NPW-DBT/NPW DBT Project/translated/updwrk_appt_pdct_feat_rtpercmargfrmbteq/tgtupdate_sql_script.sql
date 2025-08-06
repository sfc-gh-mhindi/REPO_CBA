{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_scripts_update__cse__chl__bus__int__rt__perc__prod__int__marg__wrk__appt__pdct__feat', incremental_strategy='insert_overwrite', tags=['UpdWRK_APPT_PDCT_FEAT_RTPERCMARGFrmBTEQ']) }}

SELECT
	ROW 
FROM {{ ref('Generate_UPDATE_SQL_Script') }}