{{ config(materialized='view', tags=['LdGRD_GNRC_MAPLkp']) }}

WITH Copy_62 AS (
	SELECT
		MAP_TYPE_C,
		SRCE_NUMC_1_C,
		TARG_CHAR_C
	FROM {{ ref('XfmConversions__InRemoveDups3') }}
)

SELECT * FROM Copy_62