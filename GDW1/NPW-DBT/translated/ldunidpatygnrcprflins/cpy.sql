{{ config(materialized='view', tags=['LdUnidPatyGnrcPrflIns']) }}

WITH Cpy AS (
	SELECT
		UNID_PATY_I,
		SRCE_SYST_C,
		GRDE_C,
		SUB_GRDE_C,
		PRNT_PRVG_F,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		ROW_SECU_ACCS_C
	FROM {{ ref('TgtTmpUnidPatyGnrcDS') }}
)

SELECT * FROM Cpy