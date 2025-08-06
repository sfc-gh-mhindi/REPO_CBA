{{ config(materialized='view', tags=['DltApptPdctFeatFrmTMP']) }}

WITH gdw_cpy AS (
	SELECT
		APPT_PDCT_I,
		FEAT_I,
		ACTL_VALU_Q,
		SRCE_SYST_C,
		EFFT_D
	FROM {{ ref('Appt_Pdct_Feat_Tgt') }}
)

SELECT * FROM gdw_cpy