{{ config(materialized='view', tags=['LdGRD_GNRC_MAPLkp']) }}

WITH Copy_66 AS (
	SELECT
		MAP_TYPE_C2,
		TARG_CHAR_C2,
		SRCE_CHAR_1_C2
	FROM {{ ref('XfmConversions__InRemoveDups7') }}
)

SELECT * FROM Copy_66