{{ config(materialized='view', tags=['LdGRD_GNRC_MAPLkp']) }}

WITH Copy_61 AS (
	SELECT
		MAP_TYPE_C,
		SRCE_CHAR_1_C,
		TARG_NUMC_C
	FROM {{ ref('XfmConversions__InRemoveDups2') }}
)

SELECT * FROM Copy_61