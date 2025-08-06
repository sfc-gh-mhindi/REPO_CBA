{{ config(materialized='view', alias='evnt_empl', tags=['LdAPP_ANS_EVNT_EMPL_Upd']) }}

SELECT
	EVNT_I
	EVNT_PATY_ROLE_TYPE_C
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtAPPT_PDCT_DOCU_DELY_INSSUpDS') }}