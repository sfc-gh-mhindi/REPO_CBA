{{ config(materialized='incremental', alias='_cba__app_lpxs_lpxs__sit_inbound_clp__bus__appt__rel', incremental_strategy='insert_overwrite', tags=['EXT_CLP_APP_REL']) }}

SELECT
	RPT_ROW 
FROM {{ ref('Transformer_9') }}