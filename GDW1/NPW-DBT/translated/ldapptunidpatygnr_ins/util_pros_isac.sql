{{ config(materialized='incremental', alias='tdcsodepo.util_pros_isac', incremental_strategy='append', tags=['LdApptUnidPatyGnr_Ins']) }}

SELECT
	PROS_KEY_I
	CONV_TYPE_M
	TRLR_RECD_ISRT_Q 
FROM {{ ref('SumByProcKey') }}