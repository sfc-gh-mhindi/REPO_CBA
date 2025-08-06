{{ config(materialized='view', tags=['XfmHL_FEATURE_ATTRFrmExt']) }}

WITH LkpReferences1 AS (
	SELECT
		{{ ref('AddMapTypeC') }}.HL_FEATURE_ATTR_ID,
		{{ ref('AddMapTypeC') }}.HL_FEATURE_TERM,
		{{ ref('AddMapTypeC') }}.HL_FEATURE_AMOUNT,
		{{ ref('AddMapTypeC') }}.HL_FEATURE_BALANCE,
		{{ ref('AddMapTypeC') }}.HL_FEATURE_FEE,
		{{ ref('AddMapTypeC') }}.HL_FEATURE_SPEC_REPAY,
		{{ ref('AddMapTypeC') }}.HL_FEATURE_EST_INT_AMT,
		{{ ref('AddMapTypeC') }}.HL_FEATURE_DATE,
		{{ ref('AddMapTypeC') }}.HL_FEATURE_COMMENT,
		{{ ref('AddMapTypeC') }}.SRCE_CHAR_1_C AS HL_FEATURE_CAT_ID,
		{{ ref('AddMapTypeC') }}.HL_APP_PROD_ID,
		{{ ref('AddMapTypeC') }}.ORIG_ETL_D,
		{{ ref('SrcGRD_GNRC_MAP_Lks1') }}.TARG_CHAR_C AS TARG_CHAR_C_LKP1
	FROM {{ ref('AddMapTypeC') }}
	LEFT JOIN {{ ref('SrcGRD_GNRC_MAP_Lks1') }} ON 
)

SELECT * FROM LkpReferences1