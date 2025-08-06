{{ config(materialized='incremental', alias='_cba__app_lpxs_lpxs__sit_inbound_chl__bus__hl__app__ans', incremental_strategy='insert_overwrite', tags=['EXT_CHL_BUS_HL_APP_ANS']) }}

SELECT
	RPT_ROW 
FROM {{ ref('Transformer_9') }}