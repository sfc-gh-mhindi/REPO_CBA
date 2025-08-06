{{ config(materialized='view', tags=['LdGRD_GNRC_MAPLkp']) }}

WITH Copy_68 AS (
	SELECT
		MAP_TYPE_C,
		TARG_CHAR_C,
		SRCE_NUMC_1_C,
		SRCE_NUMC_2_C
	FROM {{ ref('XfmConversions__InRemoveDups8') }}
)

SELECT * FROM Copy_68