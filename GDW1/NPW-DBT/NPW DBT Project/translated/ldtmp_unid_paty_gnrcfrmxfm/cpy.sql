{{ config(materialized='view', tags=['LdTMP_UNID_PATY_GNRCFrmXfm']) }}

WITH Cpy AS (
	SELECT
		UNID_PATY_I,
		RUN_STRM,
		SRCE_SYST_PATY_I
	FROM {{ ref('TgtTmpUnidPatyGnrcDS') }}
)

SELECT * FROM Cpy