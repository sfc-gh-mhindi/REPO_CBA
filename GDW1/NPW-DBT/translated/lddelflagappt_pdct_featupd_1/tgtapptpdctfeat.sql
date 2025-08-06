{{ config(materialized='view', alias='appt_pdct_feat', tags=['LdDelFlagAPPT_PDCT_FEATUpd_1']) }}

SELECT
	APPT_PDCT_I
	SRCE_SYST_APPT_FEAT_I
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('CL_HL_PL_OutApptPdctFeatDS') }}