{{ config(materialized='view', tags=['LdGRD_GNRC_MAPLkp']) }}

WITH Copy_65 AS (
	SELECT
		MAP_TYPE_C,
		SRCE_CHAR_1_C,
		SRCE_CHAR_2_C,
		TARG_CHAR_C
	FROM {{ ref('XfmConversions__InRemoveDups6') }}
)

SELECT * FROM Copy_65