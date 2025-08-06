{{ config(materialized='incremental', alias='_cba__app_csel4_sit_dataset_cse__chl__bus__app__comm__unid__paty__name__gnrc__i20110627', incremental_strategy='insert_overwrite', tags=['XfmUnidPatyNameGnrc']) }}

SELECT
	 
FROM {{ ref('Lk_BusRules') }}