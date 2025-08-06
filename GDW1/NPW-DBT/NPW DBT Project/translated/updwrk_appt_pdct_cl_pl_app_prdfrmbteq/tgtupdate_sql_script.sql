{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_scripts_update__cse__com__bus__ccl__chl__com__app__wrk__appt__pdct__cl__pl__app__prd', incremental_strategy='insert_overwrite', tags=['UpdWRK_APPT_PDCT_CL_PL_APP_PRDFrmBTEQ']) }}

SELECT
	ROW 
FROM {{ ref('Generate_UPDATE_SQL_Script') }}