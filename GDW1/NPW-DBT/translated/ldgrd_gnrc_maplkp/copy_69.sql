{{ config(materialized='view', tags=['LdGRD_GNRC_MAPLkp']) }}

WITH Copy_69 AS (
	SELECT
		SRCE_CHAR_1_C,
		SRCE_CHAR_2_C,
		TARG_CHAR_C
	FROM {{ ref('XfmConversions__InRemoveDups9') }}
)

SELECT * FROM Copy_69