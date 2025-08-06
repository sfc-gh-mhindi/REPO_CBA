{{ config(materialized='view', tags=['DltUNID_PATY_GNRC_PRFLFrmTMP_UNID_PATY_GNRC_PRFL']) }}

WITH Cpy AS (
	SELECT
		UNID_PATY_I,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM {{ ref('DetermineChange__ToCpy') }}
)

SELECT * FROM Cpy