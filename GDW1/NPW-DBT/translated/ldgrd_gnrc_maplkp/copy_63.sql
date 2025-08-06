{{ config(materialized='view', tags=['LdGRD_GNRC_MAPLkp']) }}

WITH Copy_63 AS (
	SELECT
		MAP_TYPE_C,
		SRCE_CHAR_1_C,
		TARG_CHAR_C
	FROM {{ ref('XfmConversions__InRemoveDups4') }}
)

SELECT * FROM Copy_63