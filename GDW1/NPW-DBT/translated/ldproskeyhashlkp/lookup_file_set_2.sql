{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_pros__key__hash__conv__m', incremental_strategy='insert_overwrite', tags=['LdProsKeyHashLkp']) }}

SELECT
	CONV_M,
	PROS_KEY_I 
FROM {{ ref('SeqProsKey') }}