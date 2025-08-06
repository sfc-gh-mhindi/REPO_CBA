{{ config(materialized='view', tags=['LdUnidPatyIdnnGnrcIns']) }}

WITH Transformer__LoadRows AS (
	SELECT
		SRCE_SYST_C,
		IDNN_TYPE_C,
		IDNN_VALU_X,
		EFFT_D,
		EXPY_D,
		UNID_PATY_I,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		ROW_SECU_ACCS_C
	FROM {{ ref('UNID_PATY_IDNN_GNRC_INSERT') }}
	WHERE 
)

SELECT * FROM Transformer__LoadRows