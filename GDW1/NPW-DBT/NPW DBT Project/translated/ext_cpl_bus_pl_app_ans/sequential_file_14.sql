{{ config(materialized='incremental', alias='_cba__app_lpxs_lpxs__sit_inbound_cpl__bus__pl__app__ans', incremental_strategy='insert_overwrite', tags=['EXT_CPL_BUS_PL_APP_ANS']) }}

SELECT
	RPT_ROW 
FROM {{ ref('Transformer_9') }}