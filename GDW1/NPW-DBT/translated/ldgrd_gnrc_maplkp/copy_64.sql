{{ config(materialized='view', tags=['LdGRD_GNRC_MAPLkp']) }}

WITH Copy_64 AS (
	SELECT
		MAP_TYPE_C,
		SRCE_NUMC_1_C,
		TARG_NUMC_C
	FROM {{ ref('XfmConversions__iNRemoveDups5') }}
)

SELECT * FROM Copy_64