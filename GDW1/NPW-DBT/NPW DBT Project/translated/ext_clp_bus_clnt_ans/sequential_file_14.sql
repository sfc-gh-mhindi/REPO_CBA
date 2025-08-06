{{ config(materialized='incremental', alias='_cba__app_lpxs_lpxs__sit_inbound_clp__bus__clnt__ans', incremental_strategy='insert_overwrite', tags=['EXT_CLP_BUS_CLNT_ANS']) }}

SELECT
	RPT_ROW 
FROM {{ ref('Transformer_9') }}