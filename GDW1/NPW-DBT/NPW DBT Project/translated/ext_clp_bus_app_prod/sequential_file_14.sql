{{ config(materialized='incremental', alias='_cba__app_lpxs_lpxs__sit_inbound_clp__bus__app__prod', incremental_strategy='insert_overwrite', tags=['EXT_CLP_BUS_APP_PROD']) }}

SELECT
	RPT_ROW 
FROM {{ ref('Transformer_9') }}