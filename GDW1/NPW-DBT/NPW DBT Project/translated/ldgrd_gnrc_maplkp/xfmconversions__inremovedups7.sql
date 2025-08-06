{{ config(materialized='view', tags=['LdGRD_GNRC_MAPLkp']) }}

WITH XfmConversions__InRemoveDups7 AS (
	SELECT
		-- *SRC*: trim(( IF IsNotNull((InGRD_GNRC_MAPTera.MAP_TYPE_C)) THEN (InGRD_GNRC_MAPTera.MAP_TYPE_C) ELSE "")),
		TRIM(IFF({{ ref('SrcGRD_GNRC_MAPTera') }}.MAP_TYPE_C IS NOT NULL, {{ ref('SrcGRD_GNRC_MAPTera') }}.MAP_TYPE_C, '')) AS MAP_TYPE_C2,
		-- *SRC*: trim(( IF IsNotNull((InGRD_GNRC_MAPTera.TARG_CHAR_C)) THEN (InGRD_GNRC_MAPTera.TARG_CHAR_C) ELSE "")),
		TRIM(IFF({{ ref('SrcGRD_GNRC_MAPTera') }}.TARG_CHAR_C IS NOT NULL, {{ ref('SrcGRD_GNRC_MAPTera') }}.TARG_CHAR_C, '')) AS TARG_CHAR_C2,
		-- *SRC*: trim(( IF IsNotNull((InGRD_GNRC_MAPTera.SRCE_CHAR_1_C)) THEN (InGRD_GNRC_MAPTera.SRCE_CHAR_1_C) ELSE "")),
		TRIM(IFF({{ ref('SrcGRD_GNRC_MAPTera') }}.SRCE_CHAR_1_C IS NOT NULL, {{ ref('SrcGRD_GNRC_MAPTera') }}.SRCE_CHAR_1_C, '')) AS SRCE_CHAR_1_C2
	FROM {{ ref('SrcGRD_GNRC_MAPTera') }}
	WHERE 
)

SELECT * FROM XfmConversions__InRemoveDups7