{{ config(materialized='view', tags=['DltUNID_PATY_GNRC_PRFLFrmTMP_UNID_PATY_GNRC_PRFL']) }}

WITH CopyFil AS (
	SELECT
		UNID_PATY_I,
		GRDE_C,
		SUB_GRDE_C,
		PRNT_PRVG_F,
		SRCE_SYST_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		ROW_SECU_ACCS_C
	FROM {{ ref('UnidPatyGnrcPrflDs') }}
)

SELECT * FROM CopyFil