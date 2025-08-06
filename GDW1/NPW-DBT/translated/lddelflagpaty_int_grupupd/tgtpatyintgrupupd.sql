{{ config(materialized='view', alias='paty_int_grup', tags=['LdDelFlagPATY_INT_GRUPUpd']) }}

SELECT
	INT_GRUP_I
	SRCE_SYST_PATY_INT_GRUP_I
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('PatyIntGrupDS') }}