{{ config(materialized='incremental', alias='_cba__app_lpxs_lpxs__sit_inbound_com__bus__app__prod__client__role', incremental_strategy='insert_overwrite', tags=['EXT_COM_BUS_APP_PROD_CLIENT_ROLE']) }}

SELECT
	RPT_ROW 
FROM {{ ref('Transformer_9') }}