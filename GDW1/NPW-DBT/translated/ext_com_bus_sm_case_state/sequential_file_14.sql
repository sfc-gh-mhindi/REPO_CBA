{{ config(materialized='incremental', alias='_cba__app_lpxs_lpxs__sit_inbound_com__bus__sm__case__state', incremental_strategy='insert_overwrite', tags=['EXT_COM_BUS_SM_CASE_STATE']) }}

SELECT
	RPT_ROW 
FROM {{ ref('Transformer_9') }}