{{ config(materialized='view', alias='appt_pdct_rpay', tags=['LdDelFlagAPPT_PDCT_RPAYUpd']) }}

SELECT
	APPT_PDCT_I
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('SrcApptPdctRpayDS') }}