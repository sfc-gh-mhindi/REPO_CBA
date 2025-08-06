{{ config(materialized='view', tags=['XfmUnidPatyIdnnGnrc']) }}

WITH Fnl_UtilPatyIdnnGnrc AS (
	SELECT
		SRCE_SYST_C as SRCE_SYST_C,
		IDNN_TYPE_C as IDNN_TYPE_C,
		IDNN_VALU_X as IDNN_VALU_X,
		EFFT_D as EFFT_D,
		EXPY_D as EXPY_D,
		UNID_PATY_I as UNID_PATY_I,
		PROS_KEY_EFFT_I as PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I as PROS_KEY_EXPY_I,
		ROW_SECU_ACCS_C as ROW_SECU_ACCS_C,
		RUN_STRM_C
	FROM {{ ref('Xfm_BusinRules__To_UnidPatyIdnnGnrc') }}
	UNION ALL
	SELECT
		SRCE_SYST_C,
		IDNN_TYPE_C,
		IDNN_VALU_X,
		EFFT_D,
		EXPY_D,
		UNID_PATY_I,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		ROW_SECU_ACCS_C,
		RUN_STRM_C
	FROM {{ ref('Xfm_BusinRules__To_UnidPatyIdnnGnrc1') }}
)

SELECT * FROM Fnl_UtilPatyIdnnGnrc