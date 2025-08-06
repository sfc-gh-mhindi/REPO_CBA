{{ config(materialized='view', alias='appt_pdct_unid_paty', tags=['LdDelFlagAPPT_PDCT_UNID_PATYUpd']) }}

SELECT
	APPT_PDCT_I
	PATY_ROLE_C
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('SrcApptPdctUnidPatyDS') }}