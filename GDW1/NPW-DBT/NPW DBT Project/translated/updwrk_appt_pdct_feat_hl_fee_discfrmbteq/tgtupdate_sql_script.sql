{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_scripts_update__cse__chl__bus__fee__disc__fee__wrk__appt__pdct__feat', incremental_strategy='insert_overwrite', tags=['UpdWRK_APPT_PDCT_FEAT_HL_FEE_DISCFrmBTEQ']) }}

SELECT
	ROW 
FROM {{ ref('Generate_UPDATE_SQL_Script') }}