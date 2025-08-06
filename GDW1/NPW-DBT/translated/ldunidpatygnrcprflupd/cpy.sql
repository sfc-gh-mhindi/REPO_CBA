{{ config(materialized='view', tags=['LdUnidPatyGnrcPrflUpd']) }}

WITH Cpy AS (
	SELECT
		UNID_PATY_I,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM {{ ref('TgtUnidPatyGnrcPrflUpdateDS') }}
)

SELECT * FROM Cpy