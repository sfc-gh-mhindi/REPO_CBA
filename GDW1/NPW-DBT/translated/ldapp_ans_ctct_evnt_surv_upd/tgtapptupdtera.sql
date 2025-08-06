{{ config(materialized='view', alias='ctct_evnt_surv', tags=['LdAPP_ANS_CTCT_EVNT_SURV_Upd']) }}

SELECT
	EVNT_I
	QSTN_C
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtAPPT_PDCT_DOCU_DELY_INSSUpDS') }}