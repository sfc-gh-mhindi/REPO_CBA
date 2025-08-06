{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__chl__bus__fee__disc__fee', incremental_strategy='insert_overwrite', tags=['LdGRD_GNRC_MAPLkp']) }}

SELECT
	SRCE_CHAR_1_C,
	SRCE_CHAR_2_C,
	TARG_CHAR_C 
FROM {{ ref('Copy_69') }}