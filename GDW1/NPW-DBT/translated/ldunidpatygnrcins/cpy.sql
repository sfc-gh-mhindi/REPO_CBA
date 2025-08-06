{{ config(materialized='view', tags=['LdUnidPatyGnrcIns']) }}

WITH Cpy AS (
	SELECT
		UNID_PATY_I,
		EFFT_D,
		PATY_TYPE_C,
		PATY_ROLE_C,
		PROS_KEY_EFFT_I,
		SRCE_SYST_C,
		PATY_QLFY_C,
		SRCE_SYST_PATY_I
	FROM {{ ref('TgTUnidPatyGnrcDS') }}
)

SELECT * FROM Cpy