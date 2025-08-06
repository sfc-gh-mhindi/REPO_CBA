{{ config(materialized='incremental', alias='appt_pdct_feat', incremental_strategy='merge', tags=['LdApptPdctFeat_Upd_ONEOFF']) }}

SELECT
	APPT_PDCT_I
	FEAT_I
	SRCE_SYST_C
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtAppPdctFeatUpdateDS') }}