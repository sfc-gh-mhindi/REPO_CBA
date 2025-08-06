{{ config(materialized='view', tags=['LdGRD_GNRC_MAPLkp']) }}

WITH Copy_60 AS (
	SELECT
		MAP_TYPE_C,
		TARG_CHAR_C,
		SRCE_CHAR_1_C
	FROM {{ ref('XfmConversions__InRemoveDups1') }}
)

SELECT * FROM Copy_60