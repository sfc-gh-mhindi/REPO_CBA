{{ config(materialized='view', tags=['LdGRD_GNRC_MAPLkp']) }}

WITH XfmConversions__InRemoveDups9 AS (
	SELECT
		SRCE_CHAR_1_C,
		SRCE_CHAR_2_C,
		TARG_CHAR_C
	FROM {{ ref('SrcGRD_GNRC_MAPTera') }}
	WHERE 
)

SELECT * FROM XfmConversions__InRemoveDups9