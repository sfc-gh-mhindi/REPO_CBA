{{ config(materialized='view', tags=['LdGRD_GNRC_MAPLkp']) }}

WITH XfmConversions__InRemoveDups8 AS (
	SELECT
		-- *SRC*: trim(( IF IsNotNull((InGRD_GNRC_MAPTera.MAP_TYPE_C)) THEN (InGRD_GNRC_MAPTera.MAP_TYPE_C) ELSE "")),
		TRIM(IFF({{ ref('SrcGRD_GNRC_MAPTera') }}.MAP_TYPE_C IS NOT NULL, {{ ref('SrcGRD_GNRC_MAPTera') }}.MAP_TYPE_C, '')) AS MAP_TYPE_C,
		-- *SRC*: trim(( IF IsNotNull((InGRD_GNRC_MAPTera.TARG_CHAR_C)) THEN (InGRD_GNRC_MAPTera.TARG_CHAR_C) ELSE "")),
		TRIM(IFF({{ ref('SrcGRD_GNRC_MAPTera') }}.TARG_CHAR_C IS NOT NULL, {{ ref('SrcGRD_GNRC_MAPTera') }}.TARG_CHAR_C, '')) AS TARG_CHAR_C,
		SRCE_NUMC_1_C,
		SRCE_NUMC_2_C
	FROM {{ ref('SrcGRD_GNRC_MAPTera') }}
	WHERE 
)

SELECT * FROM XfmConversions__InRemoveDups8