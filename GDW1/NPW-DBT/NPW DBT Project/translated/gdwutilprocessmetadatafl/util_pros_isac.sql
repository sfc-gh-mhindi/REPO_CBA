{{ config(materialized='incremental', alias='adccods.util_pros_isac', incremental_strategy='append', tags=['GDWUtilProcessMetaDataFL']) }}

SELECT
	PROS_KEY_I
	COMT_F
	SYST_INS_Q
	STUS_CHNG_S
	COMT_S
	STUS_C
	CONV_M
	CONV_TYPE_M
	SRCE_SYST_M
	SRCE_M
	TRGT_M
	SYST_S
	BTCH_KEY_I
	SRCE_BTCH_LOAD_CNT 
FROM {{ ref('Update_Util_Pros__Update_Util_Proc_Table') }}