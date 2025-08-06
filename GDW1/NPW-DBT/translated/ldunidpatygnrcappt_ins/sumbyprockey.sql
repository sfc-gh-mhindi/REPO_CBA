{{ config(materialized='view', tags=['LdUnidPatyGnrcAppt_Ins']) }}

WITH SumByProcKey AS (
	SELECT
		PROS_KEY_I,
		CONV_TYPE_M,
		-- *SRC*: Sum(record_count.TRLR_RECD_ISRT_Q),
		SUM({{ ref('Transformer__record_count') }}.TRLR_RECD_ISRT_Q) AS TRLR_RECD_ISRT_Q 
	FROM {{ ref('Transformer__record_count') }}
	GROUP BY PROS_KEY_I, CONV_TYPE_M
)

SELECT * FROM SumByProcKey