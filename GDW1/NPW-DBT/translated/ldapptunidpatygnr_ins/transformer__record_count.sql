{{ config(materialized='view', tags=['LdApptUnidPatyGnr_Ins']) }}

WITH Transformer__record_count AS (
	SELECT
		pGDW_PROS_ID AS PROS_KEY_I,
		1 AS TRLR_RECD_ISRT_Q,
		pTD_BULK_LOADTYPE AS CONV_TYPE_M
	FROM {{ ref('dsApptUnidPatyGnrc') }}
	WHERE 
)

SELECT * FROM Transformer__record_count